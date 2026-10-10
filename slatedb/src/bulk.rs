//! Creating a database from rows that are already sorted, without the write
//! path or the compactor: the rows become the SSTs of one sorted run, and a
//! new manifest names that run. A bulk load then costs one write of the data
//! instead of L0 flushes plus a compaction that rewrites all of it (and holds
//! both copies until GC runs).
//!
//! This is the narrow case of upstream's SST-ingestion RFC (#2103): a fresh
//! database only, keys strictly ascending, every row a plain value at one
//! sequence number. The result opens with [`crate::Db::builder`] like any
//! other database.

use crate::db_state::{SortedRun, SsTableHandle, SsTableId, SsTableView};
use crate::error::SlateDBError;
use crate::format::sst::SsTableFormat;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::manifest::{LsmTreeState, ManifestCore};
use crate::tablestore::{EncodedSsTableWriter, TableStore, TableStoreKind};
use crate::types::{RowEntry, ValueDeletable};
use crate::config::Settings;
use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb_common::clock::{DefaultSystemClock, SystemClock};
use std::sync::Arc;

/// What [`SortedRunWriter::finish`] wrote.
#[derive(Debug, Clone, Copy, Default)]
pub struct SortedRunStats {
    pub rows: u64,
    pub ssts: usize,
    /// Bytes of SST data written (after compression).
    pub bytes: u64,
}

/// Writes ascending rows as the SSTs of one sorted run, then creates a new
/// database whose only data is that run.
pub struct SortedRunWriter {
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    table_store: Arc<TableStore>,
    writer: Option<EncodedSsTableWriter>,
    max_sst_size: usize,
    sst_bytes: usize,
    ssts: Vec<SsTableHandle>,
    last_key: Option<Bytes>,
    stats: SortedRunStats,
}

/// Rows get this sequence number; the manifest's `last_l0_seq` covers it, so
/// later writes order after them.
const BULK_SEQ: u64 = 1;

impl SortedRunWriter {
    /// `settings` gives the SST format (compression codec, minimum filter
    /// keys); SSTs are cut at about `max_sst_size` bytes, as compaction
    /// output is.
    pub fn new<P: Into<Path>>(
        path: P,
        object_store: Arc<dyn ObjectStore>,
        settings: &Settings,
        max_sst_size: usize,
    ) -> Self {
        let path = path.into();
        let sst_format = SsTableFormat {
            min_filter_keys: settings.min_filter_keys,
            compression_codec: settings.compression_codec,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store.clone(),
            sst_format,
            path.clone(),
            None,
            TableStoreKind::Main,
            Default::default(),
        ));
        Self {
            path,
            object_store,
            table_store,
            writer: None,
            max_sst_size,
            sst_bytes: 0,
            ssts: Vec::new(),
            last_key: None,
            stats: SortedRunStats::default(),
        }
    }

    /// Adds a row. Keys must be strictly ascending.
    pub async fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), crate::Error> {
        if let Some(last) = &self.last_key {
            if key <= last.as_ref() {
                return Err(crate::Error::invalid("SortedRunWriter keys must be strictly ascending".to_string()));
            }
        }
        self.last_key = Some(Bytes::copy_from_slice(key));
        if self.writer.is_none() {
            self.writer = Some(self.table_store.table_writer(SsTableId::from(ulid::Ulid::new()), None));
        }
        let w = self.writer.as_mut().expect("writer");
        if let Some(n) = w.add(RowEntry::new(
            Bytes::copy_from_slice(key),
            ValueDeletable::Value(Bytes::copy_from_slice(value)),
            BULK_SEQ,
            None,
            None,
        )).await? {
            self.sst_bytes += n;
        }
        self.stats.rows += 1;
        if self.sst_bytes >= self.max_sst_size {
            self.cut().await?;
        }
        Ok(())
    }

    async fn cut(&mut self) -> Result<(), SlateDBError> {
        if let Some(w) = self.writer.take() {
            if !w.is_drained() || self.sst_bytes > 0 {
                let (sst, bytes) = w.close().await?;
                self.stats.bytes += bytes;
                self.ssts.push(sst);
            }
        }
        self.sst_bytes = 0;
        Ok(())
    }

    /// Closes the last SST and creates the database. Fails if a database
    /// already exists at the path.
    pub async fn finish(mut self) -> Result<SortedRunStats, crate::Error> {
        self.cut().await?;
        self.stats.ssts = self.ssts.len();
        let run = SortedRun::new(0, self.ssts.into_iter().map(SsTableView::identity));
        let mut core = ManifestCore::new();
        core.tree = Arc::new(LsmTreeState { compacted: vec![run], ..LsmTreeState::default() });
        core.last_l0_seq = BULK_SEQ;
        let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let store = Arc::new(ManifestStore::new(&self.path, self.object_store.clone()));
        StoredManifest::create_new_db(store, core, clock).await?;
        Ok(self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Db;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn opens_reads_and_writes_after_a_bulk_build() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut w = SortedRunWriter::new("db", os.clone(), &Settings::default(), 64 << 10);
        for i in 0..20_000u32 {
            w.add(&i.to_be_bytes(), format!("v{i}").as_bytes()).await.unwrap();
        }
        assert!(w.add(&5u32.to_be_bytes(), b"x").await.is_err(), "keys must ascend");
        let mut w = SortedRunWriter::new("db", os.clone(), &Settings::default(), 64 << 10);
        for i in 0..20_000u32 {
            w.add(&i.to_be_bytes(), format!("v{i}").as_bytes()).await.unwrap();
        }
        let st = w.finish().await.unwrap();
        assert_eq!(st.rows, 20_000);
        assert!(st.ssts > 1, "cut into several SSTs: {st:?}");

        let db = Db::builder("db", os.clone()).build().await.unwrap();
        assert_eq!(db.get(7u32.to_be_bytes()).await.unwrap().as_deref(), Some(&b"v7"[..]));
        assert_eq!(db.get(19_999u32.to_be_bytes()).await.unwrap().as_deref(), Some(&b"v19999"[..]));
        assert_eq!(db.get(20_000u32.to_be_bytes()).await.unwrap(), None);
        let mut it = db.scan(100u32.to_be_bytes()..110u32.to_be_bytes()).await.unwrap();
        let mut n = 0;
        while let Some(kv) = it.next().await.unwrap() {
            assert_eq!(kv.value.as_ref(), format!("v{}", 100 + n).as_bytes());
            n += 1;
        }
        assert_eq!(n, 10);
        // later writes win over the bulk rows
        db.put(7u32.to_be_bytes(), b"new").await.unwrap();
        db.delete(8u32.to_be_bytes()).await.unwrap();
        db.flush().await.unwrap();
        assert_eq!(db.get(7u32.to_be_bytes()).await.unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(db.get(8u32.to_be_bytes()).await.unwrap(), None);
        db.close().await.unwrap();

        // and survive a reopen
        let db = Db::builder("db", os.clone()).build().await.unwrap();
        assert_eq!(db.get(7u32.to_be_bytes()).await.unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(db.get(9u32.to_be_bytes()).await.unwrap().as_deref(), Some(&b"v9"[..]));
        db.close().await.unwrap();

        // a second build over an existing database fails
        let mut w = SortedRunWriter::new("db", os.clone(), &Settings::default(), 64 << 10);
        w.add(b"a", b"b").await.unwrap();
        assert!(w.finish().await.is_err());
    }
}
