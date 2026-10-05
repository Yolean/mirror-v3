//! A store wrapper for sink tests: counts requests by kind and can be
//! switched to fail every request, as an unreachable S3 does.

#![allow(dead_code)]

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

#[derive(Default)]
pub struct Counts {
    pub puts: AtomicUsize,
    pub gets: AtomicUsize,
    pub heads: AtomicUsize,
    pub lists: AtomicUsize,
    /// Entries returned by all LIST calls together.
    pub listed: Arc<AtomicUsize>,
}

pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    pub counts: Counts,
    pub down: AtomicBool,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            counts: Counts::default(),
            down: AtomicBool::new(false),
        }
    }

    pub fn reset(&self) {
        for c in [
            &self.counts.puts,
            &self.counts.gets,
            &self.counts.heads,
            &self.counts.lists,
            &*self.counts.listed,
        ] {
            c.store(0, Ordering::SeqCst);
        }
    }

    pub fn get(&self, c: &AtomicUsize) -> usize {
        c.load(Ordering::SeqCst)
    }

    fn check(&self) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "counting",
                source: "store is down".into(),
            });
        }
        Ok(())
    }

    fn counted_list(
        &self,
        stream: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counts.lists.fetch_add(1, Ordering::SeqCst);
        if self.down.load(Ordering::SeqCst) {
            return futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "counting",
                    source: "store is down".into(),
                })
            })
            .boxed();
        }
        let listed = Arc::clone(&self.counts.listed);
        stream
            .inspect(move |_| {
                listed.fetch_add(1, Ordering::SeqCst);
            })
            .boxed()
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

impl fmt::Debug for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.counts.puts.fetch_add(1, Ordering::SeqCst);
        self.check()?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.check()?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.head {
            self.counts.heads.fetch_add(1, Ordering::SeqCst);
        } else {
            self.counts.gets.fetch_add(1, Ordering::SeqCst);
        }
        self.check()?;
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.counts.heads.fetch_add(1, Ordering::SeqCst);
        self.check()?;
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.check()?;
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counted_list(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counted_list(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.counts.lists.fetch_add(1, Ordering::SeqCst);
        self.check()?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.check()?;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.check()?;
        self.inner.copy_if_not_exists(from, to).await
    }
}
