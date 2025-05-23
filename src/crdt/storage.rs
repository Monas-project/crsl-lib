use crate::crdt::error::{CrdtError, Result};
use crate::crdt::operation::Operation;
use bincode;
use rusty_leveldb::{LdbIterator, Options, DB as Database};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::path::Path;
use ulid::Ulid;

pub trait OperationStorage<ContentId, T> {
    fn save_operation(&self, op: &Operation<ContentId, T>) -> Result<()>;
    fn load_operations(&self, content_id: &ContentId) -> Result<Vec<Operation<ContentId, T>>>;
    fn get_operation(&self, op_id: &Ulid) -> Result<Option<Operation<ContentId, T>>>;
}

pub struct LeveldbStorage<ContentId, T> {
    db: RefCell<Database>,
    _marker: PhantomData<(ContentId, T)>,
}

impl<ContentId, T> LeveldbStorage<ContentId, T> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let opts = Options {
            create_if_missing: true,
            ..Default::default()
        };
        let db = Database::open(path, opts).map_err(CrdtError::Storage)?;
        Ok(LeveldbStorage {
            db: RefCell::new(db),
            _marker: PhantomData,
        })
    }

    fn make_key(id: &Ulid) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 16);
        key.push(0x01);
        key.extend_from_slice(id.to_bytes().as_ref());
        key
    }
}

impl<ContentId, T> OperationStorage<ContentId, T> for LeveldbStorage<ContentId, T>
where
    ContentId: serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + std::fmt::Debug,
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + std::fmt::Debug,
{
    fn save_operation(&self, op: &Operation<ContentId, T>) -> Result<()> {
        let key = Self::make_key(&op.id);
        let value = bincode::serde::encode_to_vec(op, bincode::config::standard())?;
        self.db.borrow_mut().put(&key, &value)?;
        Ok(())
    }

    fn load_operations(&self, content_id: &ContentId) -> Result<Vec<Operation<ContentId, T>>> {
        let mut result = Vec::new();
        let mut iter = self
            .db
            .borrow_mut()
            .new_iter()
            .map_err(CrdtError::Storage)?;
        // todo: Implement efficient search methods
        iter.seek_to_first();
        let mut key = Vec::new();
        let mut value = Vec::new();

        while iter.valid() {
            iter.current(&mut key, &mut value);
            if let Ok((op, _)) = bincode::serde::decode_from_slice::<Operation<ContentId, T>, _>(
                &value,
                bincode::config::standard(),
            ) {
                if op.genesis == *content_id {
                    result.push(op);
                }
            }
            iter.advance();
        }

        Ok(result)
    }

    fn get_operation(&self, op_id: &Ulid) -> Result<Option<Operation<ContentId, T>>> {
        let key = Self::make_key(op_id);
        match self.db.borrow_mut().get(&key) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::operation::{Operation, OperationType};
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct DummyContentId(String);

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct DummyPayload(String);

    fn setup_test_storage() -> (
        LeveldbStorage<DummyContentId, DummyPayload>,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let storage = LeveldbStorage::open(dir.path()).unwrap();
        (storage, dir)
    }

    #[test]
    fn test_save_operation() {
        let (storage, _dir) = setup_test_storage();
        let target = DummyContentId("test".into());
        let payload = DummyPayload("test".into());
        let author = "Alice".to_string();
        let op = Operation::new(
            target.clone(),
            OperationType::Create(payload.clone()),
            author.clone(),
        );

        storage.save_operation(&op).unwrap();

        let retrieved_op = storage.get_operation(&op.id);
        assert!(retrieved_op.is_ok());
        assert_eq!(retrieved_op.unwrap(), Some(op));
    }

    #[test]
    fn test_get_operation() {
        let (storage, _dir) = setup_test_storage();
        let target = DummyContentId("test".into());
        let payload = DummyPayload("test".into());
        let author = "Alice".to_string();
        let op = Operation::new(
            target.clone(),
            OperationType::Create(payload.clone()),
            author.clone(),
        );
        storage.save_operation(&op).unwrap();

        let retrieved_op = storage.get_operation(&op.id);

        assert!(retrieved_op.is_ok());
        assert_eq!(retrieved_op.unwrap(), Some(op));
    }

    #[test]
    fn test_save_and_get_multiple_operations() {
        let (storage, _dir) = setup_test_storage();
        let target = DummyContentId("test".into());
        let payload = DummyPayload("test".into());
        let author = "Alice".to_string();
        let op1 = Operation::new(
            target.clone(),
            OperationType::Create(payload.clone()),
            author.clone(),
        );
        let op2 = Operation::new_with_genesis(
            target.clone(),
            target.clone(),
            OperationType::Update(payload.clone()),
            author.clone(),
        );
        storage.save_operation(&op1).unwrap();
        storage.save_operation(&op2).unwrap();

        let retrieved_ops = storage.get_operation(&op1.id);
        let retrieved_ops2 = storage.get_operation(&op2.id);

        assert!(retrieved_ops.is_ok());
        assert_eq!(retrieved_ops.unwrap(), Some(op1));
        assert!(retrieved_ops2.is_ok());
        assert_eq!(retrieved_ops2.unwrap(), Some(op2));
    }

    #[test]
    fn test_load_operations() {
        let (storage, _dir) = setup_test_storage();
        let target = DummyContentId("test".into());
        let target2 = DummyContentId("test2".into());
        let genesis = DummyContentId("genesis".into());
        let payload = DummyPayload("test".into());
        let author = "Alice".to_string();
        let op1 = Operation::new(
            target.clone(),
            OperationType::Create(payload.clone()),
            author.clone(),
        );
        let op2 = Operation::new_with_genesis(
            target2.clone(),
            target.clone(),
            OperationType::Update(payload.clone()),
            author.clone(),
        );
        let op3 = Operation::new_with_genesis(
            genesis.clone(),
            genesis.clone(),
            OperationType::Update(payload.clone()),
            author.clone(),
        );
        storage.save_operation(&op1).unwrap();
        storage.save_operation(&op2).unwrap();
        storage.save_operation(&op3).unwrap();

        let retrieved_ops = storage.load_operations(&target);

        assert!(retrieved_ops.is_ok());
        let ops = retrieved_ops.unwrap();
        assert_eq!(ops.len(), 2);
        assert!(ops.contains(&op1));
        assert!(ops.contains(&op2));
    }
}
