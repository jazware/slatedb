//! Counts the object-store requests `DbBuilder::build` makes when it opens an
//! existing database. Every request waits a fixed latency, so the round trips
//! an open makes one after another show up in its wall time.

use crate::config::{CompactionWorkerOptions, CompactorOptions, Settings};
use crate::db::Db;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parking_lot::Mutex;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
struct Request {
    op: &'static str,
    path: String,
    start: Instant,
    end: Instant,
}

#[derive(Debug)]
struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    latency: Duration,
    recording: AtomicBool,
    requests: Arc<Mutex<Vec<Request>>>,
    fail_containing: Mutex<Option<String>>,
}

impl LatencyStore {
    fn new(inner: Arc<dyn ObjectStore>, latency: Duration) -> Self {
        Self {
            inner,
            latency,
            recording: AtomicBool::new(false),
            requests: Arc::new(Mutex::new(Vec::new())),
            fail_containing: Mutex::new(None),
        }
    }

    fn start(&self) {
        self.requests.lock().clear();
        self.recording.store(true, Ordering::SeqCst);
    }

    fn stop(&self) -> Vec<Request> {
        self.recording.store(false, Ordering::SeqCst);
        let mut requests = self.requests.lock().clone();
        requests.sort_by_key(|r| r.start);
        requests
    }

    async fn timed<R>(
        &self,
        op: &'static str,
        path: &Path,
        f: impl std::future::Future<Output = object_store::Result<R>>,
    ) -> object_store::Result<R> {
        let start = Instant::now();
        if let Some(fail) = self.fail_containing.lock().as_deref() {
            if path.as_ref().contains(fail) {
                return Err(object_store::Error::NotSupported {
                    source: format!("injected failure for {path}").into(),
                });
            }
        }
        tokio::time::sleep(self.latency).await;
        let result = f.await;
        if self.recording.load(Ordering::SeqCst) {
            self.requests.lock().push(Request {
                op,
                path: path.to_string(),
                start,
                end: Instant::now(),
            });
        }
        result
    }

    fn timed_list(
        &self,
        path: String,
        list: BoxStream<'static, object_store::Result<ObjectMeta>>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let latency = self.latency;
        let recording = self.recording.load(Ordering::SeqCst);
        let requests = self.requests.clone();
        stream::once(async move {
            let start = Instant::now();
            tokio::time::sleep(latency).await;
            let items: Vec<_> = list.collect().await;
            if recording {
                requests.lock().push(Request {
                    op: "LIST",
                    path,
                    start,
                    end: Instant::now(),
                });
            }
            stream::iter(items)
        })
        .flatten()
        .boxed()
    }
}

impl fmt::Display for LatencyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LatencyStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LatencyStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let op = if options.head { "HEAD" } else { "GET" };
        self.timed(op, location, self.inner.get_opts(location, options))
            .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.timed(
            "PUT",
            location,
            self.inner.put_opts(location, payload, opts),
        )
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.timed(
            "MULTIPART",
            location,
            self.inner.put_multipart_opts(location, opts),
        )
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = prefix.map(|p| p.to_string()).unwrap_or_default();
        self.timed_list(path, self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = prefix.map(|p| p.to_string()).unwrap_or_default();
        self.timed_list(path, self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let path = prefix.cloned().unwrap_or_default();
        self.timed("LIST", &path, self.inner.list_with_delimiter(prefix))
            .await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.timed("COPY", to, self.inner.copy_opts(from, to, options))
            .await
    }
}

fn settings() -> Settings {
    Settings {
        #[cfg(feature = "wal_disable")]
        wal_enabled: false,
        compactor_options: Some(CompactorOptions {
            worker: Some(CompactionWorkerOptions::default()),
            ..CompactorOptions::default()
        }),
        ..Settings::default()
    }
}

const PATH: &str = "/open_requests";

/// A database that has been opened and written a few times: several
/// generations of manifests, compactions files and SSTs.
async fn existing_db(latency: Duration) -> Arc<LatencyStore> {
    let store = Arc::new(LatencyStore::new(Arc::new(InMemory::new()), latency));
    for round in 0..3u8 {
        let db = open(&store).await;
        for i in 0..16u8 {
            db.put([round, i], [i; 64]).await.unwrap();
        }
        db.flush().await.unwrap();
        db.close().await.unwrap();
    }
    store
}

async fn open(store: &Arc<LatencyStore>) -> Db {
    Db::builder(PATH, store.clone() as Arc<dyn ObjectStore>)
        .with_settings(settings())
        .build()
        .await
        .unwrap()
}

fn latency() -> Duration {
    Duration::from_millis(
        std::env::var("SLATEDB_OPEN_LATENCY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_leaves_the_compactor_start_off_the_critical_path() {
    let latency = latency();
    let store = existing_db(latency).await;

    store.start();
    let t = Instant::now();
    let db = open(&store).await;
    let build = t.elapsed();
    let built_at = Instant::now();
    assert_eq!(
        db.get([2u8, 3]).await.unwrap().as_deref(),
        Some(&[3u8; 64][..])
    );
    // Let the compactor's start-up requests land before closing.
    tokio::time::sleep(latency * 30).await;
    db.close().await.unwrap();
    let requests = store.stop();
    let in_build: Vec<_> = requests
        .iter()
        .filter(|r| r.start < built_at)
        .cloned()
        .collect();
    let rounds = build.as_secs_f64() / latency.as_secs_f64();

    let origin = requests[0].start;
    for r in &requests {
        let at = |i: Instant| i.duration_since(origin).as_millis();
        eprintln!(
            "{:>6}ms {:>6}ms {:<9} {}",
            at(r.start),
            at(r.end),
            r.op,
            r.path
        );
    }
    eprintln!(
        "open: {} requests before build returned, {} through close; build took {:?} ({:.1} round trips) at {:?} per request",
        in_build.len(),
        requests.len(),
        build,
        rounds,
        latency,
    );

    assert!(
        requests
            .iter()
            .any(|r| r.path.contains("/compactions/") && r.end > built_at),
        "build waited on the compactor's start"
    );
    // 10 at the time of writing; 21 when build fenced the compactor itself.
    assert!(rounds < 14.0, "build took {rounds:.1} round trips");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_while_the_compactor_starts() {
    let store = existing_db(Duration::from_millis(50)).await;
    let db = open(&store).await;
    tokio::time::timeout(Duration::from_secs(10), db.close())
        .await
        .expect("close waits on the compactor's start")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compactor_that_fails_to_start_closes_the_db() {
    let store = existing_db(Duration::ZERO).await;
    *store.fail_containing.lock() = Some("/compactions/".to_string());
    // The failure can land before build returns, which then fails with it.
    let err = match Db::builder(PATH, store.clone() as Arc<dyn ObjectStore>)
        .with_settings(settings())
        .build()
        .await
    {
        Err(err) => err,
        Ok(db) => {
            tokio::time::timeout(Duration::from_secs(10), async {
                while db.inner.check_closed().is_ok() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the compactor's start-up error never reached the db");
            db.put(b"k", b"v").await.unwrap_err()
        }
    };
    assert!(
        err.to_string().contains("injected failure")
            || format!("{err:?}").contains("injected failure"),
        "{err:?}"
    );
}
