# Patches

This fork's `main` is upstream SlateDB 0.17.0 (`c1e36fc`) plus the patches below, in this order.
vlpds and vlRelay pin `main`'s head commit by rev.
Each patch also has its own branch, so that it can be reported upstream later.
Add a row here with every new patch.

| # | Commit | Branch | What and why | Upstream |
|---|--------|--------|--------------|----------|
| 1 | `f246143`, `308ffa5` (tests) | `vlpds-0.17-union-l0-view-ids` | `Manifest::cloned_from_union` gives repeated L0 view ids fresh ids. A union clone of two sources that shared an L0 SST repeated its view id, and the first compaction of the merged shard dropped the keys of one half. | pending report |
| 2 | `fc2aae0` | `vlpds-0.17-batch-next` | A synchronous `next` fast path for resident rows, and `DbIterator::next_batch`. A scan paid an awaited, boxed future per iterator layer per row (about 12.5k to 3.4k instructions per row). | pending report |
| 3 | `68106cc` | `vlpds-0.17-submit-dest-guard` | `validate_compaction` fails a submitted spec that collides with a claimed job. An admin-submitted compaction skipped the destination check, and the executor's `assert!` panicked. | pending report |
| 4 | `2a79077` | `vlpds-0.17-filtercache` | `CompactionWorkerBuilder::with_db_cache` seeds a standalone worker's output SSTs into the DB cache. Without it, the first reads of a fresh SST always missed its index and filters. | pending report |
| 5 | `c7b29a0` | `fix/l0-view-merge-dup-sst` | `LsmTreeState::merge_writer_and_compactor` cuts L0 at the compacted view id, not the SST id. A union clone has one SST behind several views, and the cut dropped live views, so the next flush failed with `InvalidClockTick`. | pending report |
| 6 | `17d6844` | `vlpds-0.17-cache-first-loader` | The table store peeks the cache (new `DbCache::peek_*`) before it builds a `CacheLoader`. Every cache hit built a loader that it did not use, about 10 allocations per section read. | pending report |
