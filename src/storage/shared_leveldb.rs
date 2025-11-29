use rusty_leveldb::{Options, Status, WriteBatch, DB as Database};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug)]
pub enum BatchError {
    Unsupported,
    AlreadyActive,
    Commit(Status),
    LockPoisoned,
}

pub struct SharedLeveldb {
    db: Mutex<Database>,
    active_batch: Mutex<Option<WriteBatch>>,
    #[cfg(test)]
    commit_fail_status: Mutex<Option<Status>>,
}

impl SharedLeveldb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Arc<Self>, Status> {
        let opts = Options {
            create_if_missing: true,
            ..Default::default()
        };
        let db = Database::open(path, opts)?;
        Ok(Arc::new(Self {
            db: Mutex::new(db),
            active_batch: Mutex::new(None),
            #[cfg(test)]
            commit_fail_status: Mutex::new(None),
        }))
    }

    pub fn begin_batch(&self) -> Result<LeveldbBatchGuard<'_>, BatchError> {
        let mut slot = self
            .active_batch
            .lock()
            .map_err(|_| BatchError::LockPoisoned)?;
        if slot.is_some() {
            return Err(BatchError::AlreadyActive);
        }
        *slot = Some(WriteBatch::default());
        Ok(LeveldbBatchGuard {
            shared: self,
            committed: false,
        })
    }

    fn commit_batch(&self) -> Result<(), Status> {
        let mut slot = self.active_batch.lock().map_err(|_| {
            Status::new(rusty_leveldb::StatusCode::LockError, "Lock poisoned")
        })?;
        let Some(batch) = slot.take() else {
            return Ok(());
        };
        #[cfg(test)]
        if let Some(status) = self
            .commit_fail_status
            .lock()
            .ok()
            .and_then(|mut s| s.take())
        {
            return Err(status);
        }
        self.db
            .lock()
            .map_err(|_| Status::new(rusty_leveldb::StatusCode::LockError, "Lock poisoned"))?
            .write(batch, true)
    }

    fn abort_batch(&self) {
        if let Ok(mut slot) = self.active_batch.lock() {
            slot.take();
        }
    }

    pub fn with_active_batch<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut WriteBatch) -> R,
    {
        let mut slot = self.active_batch.lock().ok()?;
        slot.as_mut().map(f)
    }

    pub fn db(&self) -> MutexGuard<'_, Database> {
        self.db.lock().expect("Database lock poisoned")
    }

    pub fn try_db(&self) -> Result<MutexGuard<'_, Database>, BatchError> {
        self.db.lock().map_err(|_| BatchError::LockPoisoned)
    }
}

pub struct LeveldbBatchGuard<'a> {
    shared: &'a SharedLeveldb,
    committed: bool,
}

impl<'a> LeveldbBatchGuard<'a> {
    pub fn commit(mut self) -> Result<(), Status> {
        self.shared.commit_batch()?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for LeveldbBatchGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.shared.abort_batch();
        }
    }
}

pub trait SharedLeveldbAccess {
    fn shared_leveldb(&self) -> Option<Arc<SharedLeveldb>>;
}

#[cfg(test)]
impl SharedLeveldb {
    pub fn inject_commit_failure(&self, status: Status) {
        if let Ok(mut slot) = self.commit_fail_status.lock() {
            slot.replace(status);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn begin_batch_prevents_nested_batches() {
        let dir = tempdir().unwrap();
        let shared = SharedLeveldb::open(dir.path()).expect("open shared db");

        let guard = shared.begin_batch().expect("begin first batch");
        match shared.begin_batch() {
            Err(BatchError::AlreadyActive) => {}
            Ok(_) => panic!("expected AlreadyActive error, got Ok"),
            Err(err) => panic!("unexpected batch error: {err:?}"),
        }
        drop(guard);

        shared
            .begin_batch()
            .expect("batch should be available after guard drop")
            .commit()
            .expect("commit empty batch");
    }

    #[test]
    fn commit_batch_persists_operations() {
        let dir = tempdir().unwrap();
        let shared = SharedLeveldb::open(dir.path()).expect("open shared db");

        let guard = shared.begin_batch().expect("begin batch");
        let key = b"test-key";
        let value = b"test-value";

        let inserted = shared.with_active_batch(|batch| batch.put(key, value));
        assert!(inserted.is_some(), "expected active batch to exist");

        guard.commit().expect("commit batch");

        let stored = shared
            .db()
            .get(key)
            .expect("value should exist after commit");
        assert_eq!(stored.as_slice(), value);
    }

    #[test]
    fn dropping_guard_discards_pending_operations() {
        let dir = tempdir().unwrap();
        let shared = SharedLeveldb::open(dir.path()).expect("open shared db");
        let key = b"discard-key";
        let value = b"discard-value";

        {
            let _guard = shared.begin_batch().expect("begin batch");
            let inserted = shared.with_active_batch(|batch| batch.put(key, value));
            assert!(inserted.is_some(), "expected active batch to exist");
            // guard dropped here without commit
        }

        let result = shared.db().get(key);
        assert!(
            result.is_none(),
            "value should not be persisted when batch guard is dropped without commit"
        );
    }
}
