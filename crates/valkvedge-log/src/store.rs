//! A thin wrapper over [`object_store`] exposing the conditional writes the
//! log depends on.

use crate::error::{Error, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use std::sync::Arc;

/// An object with the version tag needed for a compare-and-swap.
#[derive(Debug, Clone)]
pub struct Versioned {
    /// Object contents.
    pub bytes: Bytes,
    /// Entity tag identifying this version.
    pub etag: String,
}

/// Result of a conditional write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutOutcome {
    /// The object was written. Carries its new entity tag.
    Stored(String),
    /// The condition failed: the object exists (create) or changed (update).
    ConditionFailed,
}

/// Handle to an object store. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Store {
    inner: Arc<dyn ObjectStore>,
}

fn unavailable(e: object_store::Error) -> Error {
    Error::Unavailable(e.to_string())
}

impl Store {
    /// Wraps any [`ObjectStore`]. For Amazon S3, build it with conditional puts
    /// enabled (`aws_conditional_put = etag`).
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Store { inner }
    }

    /// An in-memory store, for tests and examples.
    pub fn memory() -> Self {
        Store::new(Arc::new(object_store::memory::InMemory::new()))
    }

    /// Reads an object. `None` when it does not exist.
    pub async fn get(&self, key: &str) -> Result<Option<Versioned>> {
        match self.inner.get(&Path::from(key)).await {
            Ok(result) => {
                let etag = result.meta.e_tag.clone();
                let bytes = result.bytes().await.map_err(unavailable)?;
                let etag =
                    etag.ok_or_else(|| Error::Unavailable("object store returned no etag".into()))?;
                Ok(Some(Versioned { bytes, etag }))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(unavailable(e)),
        }
    }

    async fn put_mode(&self, key: &str, bytes: Vec<u8>, mode: PutMode) -> Result<PutOutcome> {
        let opts = PutOptions {
            mode,
            ..PutOptions::default()
        };
        match self
            .inner
            .put_opts(&Path::from(key), PutPayload::from(bytes), opts)
            .await
        {
            Ok(result) => Ok(PutOutcome::Stored(result.e_tag.unwrap_or_default())),
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => Ok(PutOutcome::ConditionFailed),
            Err(e) => Err(unavailable(e)),
        }
    }

    /// Writes only if no object exists at `key` (`If-None-Match: *`).
    pub async fn put_if_absent(&self, key: &str, bytes: Vec<u8>) -> Result<PutOutcome> {
        self.put_mode(key, bytes, PutMode::Create).await
    }

    /// Writes only if the object still has `etag` (`If-Match`).
    pub async fn put_if_match(&self, key: &str, bytes: Vec<u8>, etag: &str) -> Result<PutOutcome> {
        let version = UpdateVersion {
            e_tag: Some(etag.to_string()),
            version: None,
        };
        self.put_mode(key, bytes, PutMode::Update(version)).await
    }

    /// Writes unconditionally.
    pub async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.put_mode(key, bytes, PutMode::Overwrite).await?;
        Ok(())
    }

    /// Lists every key under `prefix`, sorted.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let path = Path::from(prefix.trim_end_matches('/'));
        let metas: Vec<_> = self
            .inner
            .list(Some(&path))
            .try_collect()
            .await
            .map_err(unavailable)?;
        let mut keys: Vec<String> = metas.into_iter().map(|m| m.location.to_string()).collect();
        keys.sort();
        Ok(keys)
    }

    /// Deletes an object. Deleting a missing object is not an error.
    pub async fn delete(&self, key: &str) -> Result<()> {
        match self.inner.delete(&Path::from(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(unavailable(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn conditional_writes_behave_like_s3() {
        let store = Store::memory();
        assert!(store.get("a/b").await.unwrap().is_none());

        let first = store.put_if_absent("a/b", b"one".to_vec()).await.unwrap();
        assert!(matches!(first, PutOutcome::Stored(_)));
        assert_eq!(
            store.put_if_absent("a/b", b"two".to_vec()).await.unwrap(),
            PutOutcome::ConditionFailed
        );

        let current = store.get("a/b").await.unwrap().unwrap();
        assert_eq!(&current.bytes[..], b"one");
        let updated = store
            .put_if_match("a/b", b"two".to_vec(), &current.etag)
            .await
            .unwrap();
        assert!(matches!(updated, PutOutcome::Stored(_)));
        assert_eq!(
            store
                .put_if_match("a/b", b"three".to_vec(), &current.etag)
                .await
                .unwrap(),
            PutOutcome::ConditionFailed
        );
    }

    #[tokio::test]
    async fn list_is_sorted_and_scoped() {
        let store = Store::memory();
        for key in ["p/2", "p/1", "q/1"] {
            store.put(key, vec![1]).await.unwrap();
        }
        assert_eq!(store.list("p/").await.unwrap(), vec!["p/1", "p/2"]);
        store.delete("p/1").await.unwrap();
        store.delete("p/1").await.unwrap();
        assert_eq!(store.list("p/").await.unwrap(), vec!["p/2"]);
    }
}
