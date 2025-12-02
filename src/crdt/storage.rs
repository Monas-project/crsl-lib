use crate::crdt::error::{CrdtError, Result};
use crate::crdt::operation::Operation;
use crate::storage::{BatchError, LeveldbBatchGuard, SharedLeveldb, SharedLeveldbAccess};
use bincode;
use rusty_leveldb::LdbIterator;
use std::marker::PhantomData;
use std::path::Path;
use std::rc::Rc;
use ulid::Ulid;

/// Abstraction over the persistent storage used by `CrdtState`.
pub trait OperationStorage<ContentId, T> {
    fn save_operation(&self, op: &Operation<ContentId, T>) -> Result<()>;
    fn load_operations(&self, genesis: &ContentId) -> Result<Vec<Operation<ContentId, T>>>;
    fn get_operation(&self, op_id: &Ulid) -> Result<Option<Operation<ContentId, T>>>;
    fn delete_operation(&self, op_id: &Ulid) -> Result<()>;
    fn begin_batch(&self) -> std::result::Result<LeveldbBatchGuard<'_>, BatchError> {
        Err(BatchError::Unsupported)
    }
}

/// LevelDB-backed implementation of [`OperationStorage`].
#[derive(Clone)]
pub struct LeveldbStorage<ContentId, T> {
    shared: Rc<SharedLeveldb>,
    _marker: PhantomData<(ContentId, T)>,
}

impl<ContentId, T> LeveldbStorage<ContentId, T> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let shared = SharedLeveldb::open(path).map_err(CrdtError::Storage)?;
        Ok(Self::new(shared))
    }

    pub fn new(shared: Rc<SharedLeveldb>) -> Self {
        Self {
            shared,
            _marker: PhantomData,
        }
    }

    /// Builds the LevelDB key prefix used for operations (`0x01` namespace).
    fn make_key(id: &Ulid) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 16);
        key.push(0x01);
        key.extend_from_slice(id.to_bytes().as_ref());
        key
    }

    /// Serialises an operation into the binary format persisted in LevelDB.
    fn encode_operation(op: &Operation<ContentId, T>) -> Result<Vec<u8>>
    where
        ContentId: serde::Serialize,
        T: serde::Serialize,
    {
        let value = bincode::serde::encode_to_vec(op, bincode::config::standard())?;
        Ok(value)
    }

    /// Writes value bytes either to the active batch or directly to the DB.
    fn put_bytes(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self
            .shared
            .with_active_batch(|batch| batch.put(key, value))
            .is_none()
        {
            self.shared.db().borrow_mut().put(key, value)?;
        }
        Ok(())
    }

    /// Deletes the given key, respecting an active batch if present.
    fn delete_key(&self, key: &[u8]) -> Result<()> {
        if self
            .shared
            .with_active_batch(|batch| batch.delete(key))
            .is_none()
        {
            self.shared.db().borrow_mut().delete(key)?;
        }
        Ok(())
    }
}

impl<ContentId, T> SharedLeveldbAccess for LeveldbStorage<ContentId, T> {
    fn shared_leveldb(&self) -> Option<Rc<SharedLeveldb>> {
        Some(self.shared.clone())
    }
}

impl<ContentId, T> OperationStorage<ContentId, T> for LeveldbStorage<ContentId, T>
where
    ContentId: serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + std::fmt::Debug,
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + std::fmt::Debug,
{
    fn begin_batch(&self) -> std::result::Result<LeveldbBatchGuard<'_>, BatchError> {
        self.shared.begin_batch()
    }

    fn save_operation(&self, op: &Operation<ContentId, T>) -> Result<()> {
        let key = Self::make_key(&op.id);
        let value = Self::encode_operation(op)?;
        self.put_bytes(&key, &value)
    }

    fn load_operations(&self, genesis: &ContentId) -> Result<Vec<Operation<ContentId, T>>> {
        let mut result = Vec::new();
        let mut iter = self
            .shared
            .db()
            .borrow_mut()
            .new_iter()
            .map_err(CrdtError::Storage)?;
        iter.seek_to_first();

        let mut key = Vec::new();
        let mut value = Vec::new();
        while iter.valid() {
            iter.current(&mut key, &mut value);
            if let Ok((op, _)) = bincode::serde::decode_from_slice::<Operation<ContentId, T>, _>(
                &value,
                bincode::config::standard(),
            ) {
                if op.genesis == *genesis {
                    result.push(op);
                }
            }
            iter.advance();
        }

        Ok(result)
    }

    fn get_operation(&self, op_id: &Ulid) -> Result<Option<Operation<ContentId, T>>> {
        let key = Self::make_key(op_id);
        match self.shared.db().borrow_mut().get(&key) {
            Some(raw) => {
                let (op, _) = bincode::serde::decode_from_slice::<Operation<ContentId, T>, _>(
                    &raw,
                    bincode::config::standard(),
                )?;
                Ok(Some(op))
            }
            None => Ok(None),
        }
    }

    fn delete_operation(&self, op_id: &Ulid) -> Result<()> {
        let key = Self::make_key(op_id);
        self.delete_key(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::operation::OperationType;
    use crate::storage::SharedLeveldb;
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct DummyContentId(u64);

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct DummyPayload(String);

    fn make_op(id: u64, payload: &str) -> Operation<DummyContentId, DummyPayload> {
        Operation::new(
            DummyContentId(id),
            OperationType::Update(DummyPayload(payload.to_string())),
            "tester".into(),
        )
    }

    fn setup_storage() -> (
        LeveldbStorage<DummyContentId, DummyPayload>,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let shared = SharedLeveldb::open(dir.path()).unwrap();
        (LeveldbStorage::new(shared), dir)
    }

    #[test]
    fn save_and_load_roundtrip() {
        let (storage, _dir) = setup_storage();
        let op = make_op(1, "hello");
        storage.save_operation(&op).unwrap();

        let retrieved = storage
            .get_operation(&op.id)
            .unwrap()
            .expect("operation should exist");
        assert_eq!(retrieved, op);

        let all = storage.load_operations(&DummyContentId(1)).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0], op);
    }

    #[test]
    fn delete_operation_removes_entry() {
        let (storage, _dir) = setup_storage();
        let op = make_op(7, "bye");
        storage.save_operation(&op).unwrap();

        storage.delete_operation(&op.id).unwrap();
        assert!(storage.get_operation(&op.id).unwrap().is_none());
    }

    #[test]
    fn batch_commit_persists_operations() {
        let (storage, _dir) = setup_storage();

        let guard = storage.begin_batch().unwrap();
        let op_a = make_op(10, "a");
        let op_b = make_op(10, "b");

        storage.save_operation(&op_a).unwrap();
        storage.save_operation(&op_b).unwrap();

        // Operations are not visible before commit.
        assert!(storage.get_operation(&op_a.id).unwrap().is_none());

        guard.commit().unwrap();

        assert!(storage.get_operation(&op_a.id).unwrap().is_some());
        assert!(storage.get_operation(&op_b.id).unwrap().is_some());

        let all = storage.load_operations(&DummyContentId(10)).unwrap();
        assert_eq!(all.len(), 2);
        assert!(all.contains(&op_a));
        assert!(all.contains(&op_b));
    }
}
