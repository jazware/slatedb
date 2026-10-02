use async_trait::async_trait;

use crate::error::SlateDBError;
use crate::types::RowEntry;

#[derive(Clone, Copy, Debug)]
pub enum IterationOrder {
    Ascending,
    Descending,
}

/// Note: this is intentionally its own trait instead of an Iterator<Item=KeyValue>,
/// because next will need to be made async to support SSTs, which are loaded over
/// the network.
/// See: https://github.com/slatedb/slatedb/issues/12

#[async_trait]
pub(crate) trait RowEntryIterator: Send + Sync {
    /// Performs any expensive initialization required before regular iteration.
    ///
    /// This method should be idempotent and can be called multiple times, only
    /// the first initialization should perform expensive operations.
    async fn init(&mut self) -> Result<(), SlateDBError>;

    /// Returns the next entry in the iterator, which may be a key-value pair or
    /// a tombstone of a deleted key-value pair.
    ///
    /// Will fail with `SlateDBError::IteratorNotInitialized` if the iterator is
    /// not yet initialized.
    ///
    /// NOTE: we don't initialize the iterator when calling next and instead
    /// require the caller to explicitly initialize the iterator. This is in order
    /// to ensure that optimizations which eagerly initialize the iterator are not
    /// lost in a refactor and instead would throw errors.
    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError>;

    /// Synchronous fast path for [`Self::next`]: returns the next entry when
    /// it can be produced without awaiting (e.g. from an already loaded
    /// block), or `None` when it can't (a block fetch, an SST open, ...).
    ///
    /// `Some(result)` is exactly what `next().await` would have returned. On
    /// `None` the iterator may have made progress internally, but it has not
    /// consumed the next entry: a following `next()` (or `try_next_sync()`)
    /// returns the same entry `next()` would have returned had this not been
    /// called. The default never takes the fast path.
    ///
    /// Like [`Self::next`], this is only valid on an initialized iterator;
    /// an uninitialized one returns `None` (and `next` reports the error).
    fn try_next_sync(&mut self) -> Option<Result<Option<RowEntry>, SlateDBError>> {
        None
    }

    /// Seek to the next (inclusive) key
    ///
    /// Will fail with `SlateDBError::IteratorNotInitialized` if the iterator is
    /// not yet initialized.
    ///
    /// NOTE: we don't initialize the iterator when calling seek and instead
    /// require the caller to explicitly initialize the iterator. This is in order
    /// to ensure that optimizations which eagerly initialize the iterator are not
    /// lost in a refactor and instead would throw errors.
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError>;
}

/// Iterator trait that tracks bytes processed for progress reporting.
///
/// This extends `RowEntryIterator` with progress tracking capability.
/// Only iterators used in the compaction pipeline implement this trait.
/// The bottom-most iterator (MergeIterator) tracks actual bytes, while
/// wrapper iterators delegate to their inner iterator.
pub(crate) trait TrackedRowEntryIterator: RowEntryIterator {
    /// Returns the total bytes processed (key + value length) by this iterator.
    fn bytes_processed(&self) -> u64;
}

#[async_trait]
impl<'a> RowEntryIterator for Box<dyn RowEntryIterator + 'a> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.as_mut().init().await
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        self.as_mut().next().await
    }

    fn try_next_sync(&mut self) -> Option<Result<Option<RowEntry>, SlateDBError>> {
        self.as_mut().try_next_sync()
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.as_mut().seek(next_key).await
    }
}

#[async_trait]
impl<'a> RowEntryIterator for Box<dyn TrackedRowEntryIterator + 'a> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.as_mut().init().await
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        self.as_mut().next().await
    }

    fn try_next_sync(&mut self) -> Option<Result<Option<RowEntry>, SlateDBError>> {
        self.as_mut().try_next_sync()
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.as_mut().seek(next_key).await
    }
}

impl<'a> TrackedRowEntryIterator for Box<dyn TrackedRowEntryIterator + 'a> {
    fn bytes_processed(&self) -> u64 {
        self.as_ref().bytes_processed()
    }
}

pub(crate) struct EmptyIterator;

impl EmptyIterator {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait]
impl RowEntryIterator for EmptyIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(None)
    }

    fn try_next_sync(&mut self) -> Option<Result<Option<RowEntry>, SlateDBError>> {
        Some(Ok(None))
    }

    async fn seek(&mut self, _next_key: &[u8]) -> Result<(), SlateDBError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! `try_next_sync` against `next`: a stack of iterators driven by any mix
    //! of the two (and seeks) yields what `next` alone does.

    use super::*;
    use crate::filter_iterator::FilterIterator;
    use crate::merge_iterator::MergeIterator;
    use crate::merge_operator::{MergeOperatorIterator, MergeOperatorRequiredIterator};
    use crate::proptest_util::rng::new_test_rng;
    use crate::reader::ReadTrace;
    use crate::test_utils::StringConcatMergeOperator;
    use proptest::test_runner::TestRng;
    use rand::Rng;
    use std::collections::VecDeque;
    use std::sync::Arc;

    /// A source whose `try_next_sync` randomly "would block" (as a leaf does
    /// at a block boundary). Both paths return the same entries.
    struct GatedIterator {
        entries: VecDeque<Result<RowEntry, SlateDBError>>,
        order: IterationOrder,
        rng: Option<TestRng>,
        block_prob: f64,
    }

    impl GatedIterator {
        fn pop(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
            self.entries.pop_front().transpose()
        }
    }

    #[async_trait]
    impl RowEntryIterator for GatedIterator {
        async fn init(&mut self) -> Result<(), SlateDBError> {
            Ok(())
        }

        async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
            self.pop()
        }

        fn try_next_sync(&mut self) -> Option<Result<Option<RowEntry>, SlateDBError>> {
            // `None` for a reference stack: it never takes the fast path
            let rng = self.rng.as_mut()?;
            if rng.random_bool(self.block_prob) {
                return None;
            }
            Some(self.pop())
        }

        async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
            while let Some(Ok(entry)) = self.entries.front() {
                let before = match self.order {
                    IterationOrder::Ascending => entry.key.as_ref() < next_key,
                    IterationOrder::Descending => entry.key.as_ref() > next_key,
                };
                if !before {
                    break;
                }
                self.entries.pop_front();
            }
            Ok(())
        }
    }

    /// Sorted sources: `keys` small so keys repeat across sources and
    /// versions; unique seqs; values, tombstones and merge operands.
    fn sources(
        rng: &mut TestRng,
        order: IterationOrder,
        with_errors: bool,
    ) -> Vec<Vec<Result<RowEntry, SlateDBError>>> {
        let n = rng.random_range(1..=5);
        let keys: u8 = rng.random_range(1..=12);
        let mut seq = 0u64;
        (0..n)
            .map(|_| {
                let len = rng.random_range(0..=40);
                let mut rows: Vec<RowEntry> = (0..len)
                    .map(|_| {
                        seq += 1;
                        let key = [b'k', rng.random_range(0..keys)];
                        let value = format!("v{seq}");
                        match rng.random_range(0..10) {
                            0..=1 => RowEntry::new_tombstone(&key, seq),
                            2..=3 => RowEntry::new_merge(&key, value.as_bytes(), seq),
                            _ => RowEntry::new_value(&key, value.as_bytes(), seq),
                        }
                    })
                    .collect();
                rows.sort_by(|a, b| match order {
                    IterationOrder::Ascending => a.key.cmp(&b.key).then(b.seq.cmp(&a.seq)),
                    IterationOrder::Descending => b.key.cmp(&a.key).then(b.seq.cmp(&a.seq)),
                });
                let mut rows: Vec<_> = rows.into_iter().map(Ok).collect();
                if with_errors && !rows.is_empty() && rng.random_bool(0.2) {
                    let at = rng.random_range(0..rows.len());
                    rows.insert(at, Err(SlateDBError::InvalidDeletion));
                }
                rows
            })
            .collect()
    }

    #[derive(Clone, Copy, Debug)]
    enum Top {
        /// A deduping merge.
        Merge,
        /// The scan stack's shape: a deduping merge of an empty source, a
        /// merge of filtered sources and a filtered non-deduping merge.
        Scan,
        /// A non-deduping merge.
        Raw,
    }

    #[derive(Clone, Copy, Debug)]
    enum Wrap {
        None,
        MergeOperator,
        MergeOperatorRequired,
    }

    fn build(
        srcs: &[Vec<Result<RowEntry, SlateDBError>>],
        order: IterationOrder,
        top: Top,
        wrap: Wrap,
        max_seq: Option<u64>,
        gate: Option<(&mut TestRng, f64)>,
    ) -> Box<dyn RowEntryIterator> {
        let mut gate = gate;
        let mut leaf = |rows: &Vec<Result<RowEntry, SlateDBError>>| -> Box<dyn RowEntryIterator> {
            let (rng, block_prob) = match gate.as_mut() {
                Some((rng, p)) => (Some(new_test_rng(Some(rng.random()))), *p),
                None => (None, 0.0),
            };
            Box::new(GatedIterator {
                entries: rows.iter().cloned().collect(),
                order,
                rng,
                block_prob,
            })
        };
        let filtered = |it: Box<dyn RowEntryIterator>| -> Box<dyn RowEntryIterator> {
            Box::new(FilterIterator::new_with_max_seq(it, max_seq))
        };
        let iter: Box<dyn RowEntryIterator> = match top {
            Top::Merge => {
                Box::new(MergeIterator::new_with_order(srcs.iter().map(&mut leaf), order).unwrap())
            }
            Top::Raw => Box::new(
                MergeIterator::new_with_order(srcs.iter().map(&mut leaf), order)
                    .unwrap()
                    .with_dedup(false),
            ),
            Top::Scan => {
                let split = srcs.len() / 2;
                let mem: Vec<Box<dyn RowEntryIterator>> =
                    srcs[..split].iter().map(|s| filtered(leaf(s))).collect();
                let disk: Vec<Box<dyn RowEntryIterator>> =
                    srcs[split..].iter().map(&mut leaf).collect();
                let disk = MergeIterator::new_with_order(disk, order)
                    .unwrap()
                    .with_dedup(false);
                let arms: Vec<Box<dyn RowEntryIterator>> = vec![
                    Box::new(EmptyIterator::new()),
                    Box::new(MergeIterator::new_with_order(mem, order).unwrap()),
                    filtered(Box::new(disk)),
                ];
                Box::new(MergeIterator::new_with_order(arms, order).unwrap())
            }
        };
        match wrap {
            Wrap::None => iter,
            Wrap::MergeOperatorRequired => Box::new(MergeOperatorRequiredIterator::new(iter)),
            Wrap::MergeOperator => Box::new(MergeOperatorIterator::new(
                Arc::new(StringConcatMergeOperator),
                iter,
                true,
                None,
                ReadTrace::none(),
            )),
        }
    }

    #[derive(Clone, Debug)]
    enum Step {
        Next,
        Seek(Vec<u8>),
    }

    /// Runs `steps` on `iter`, `next` via `try_next_sync` first when
    /// `sync`, until the first error.
    async fn drive(
        iter: &mut Box<dyn RowEntryIterator>,
        steps: &[Step],
        sync: bool,
    ) -> Vec<Result<Option<RowEntry>, String>> {
        if let Err(e) = iter.init().await {
            return vec![Err(e.to_string())];
        }
        let mut out = Vec::new();
        for step in steps {
            let result = match step {
                Step::Next => {
                    let next = match sync.then(|| iter.try_next_sync()).flatten() {
                        Some(next) => next,
                        None => iter.next().await,
                    };
                    next.map_err(|e| e.to_string())
                }
                Step::Seek(key) => iter
                    .seek(key)
                    .await
                    .map(|()| None)
                    .map_err(|e| e.to_string()),
            };
            let failed = result.is_err();
            out.push(result);
            if failed {
                break;
            }
        }
        out
    }

    #[tokio::test]
    async fn test_try_next_sync_matches_next() {
        for case in 0..3000u32 {
            let mut seed = [0u8; 32];
            seed[..4].copy_from_slice(&case.to_le_bytes());
            let mut rng = new_test_rng(Some(seed));
            let order = if rng.random_bool(0.7) {
                IterationOrder::Ascending
            } else {
                IterationOrder::Descending
            };
            let with_errors = rng.random_bool(0.2);
            let srcs = sources(&mut rng, order, with_errors);
            let top = [Top::Merge, Top::Scan, Top::Raw][rng.random_range(0..3)];
            let wrap = [Wrap::None, Wrap::MergeOperator, Wrap::MergeOperatorRequired]
                [rng.random_range(0..3)];
            let max_seq = rng.random_bool(0.3).then(|| rng.random_range(0..120));
            let block_prob = [0.0, 0.05, 0.3, 0.9][rng.random_range(0..4)];
            // seeks (ascending only, as in `DbIterator`) to keys that only
            // move forward
            let mut steps = Vec::new();
            let mut seek_floor = 0u8;
            for _ in 0..rng.random_range(0..150) {
                if matches!(order, IterationOrder::Ascending) && rng.random_bool(0.03) {
                    seek_floor = seek_floor.saturating_add(rng.random_range(0..3));
                    steps.push(Step::Seek(vec![b'k', seek_floor]));
                } else {
                    steps.push(Step::Next);
                }
            }

            let mut reference = build(&srcs, order, top, wrap, max_seq, None);
            let expected = drive(&mut reference, &steps, false).await;
            let mut fast = build(
                &srcs,
                order,
                top,
                wrap,
                max_seq,
                Some((&mut rng, block_prob)),
            );
            let got = drive(&mut fast, &steps, true).await;
            assert_eq!(
                got, expected,
                "case {case}: order {order:?} top {top:?} wrap {wrap:?} max_seq {max_seq:?} block_prob {block_prob}"
            );
        }
    }
}
