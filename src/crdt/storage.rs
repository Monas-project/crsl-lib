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
    fn load_operations_by_genesis(&self, genesis_id: &ContentId) -> Result<Vec<Operation<ContentId, T>>>;
    fn load_operations_by_target(&self, target_id: &ContentId) -> Result<Vec<Operation<ContentId, T>>>;
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

    fn load_operations_by_genesis(&self, genesis_id: &ContentId) -> Result<Vec<Operation<ContentId, T>>> {
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
                if op.genesis == *genesis_id {
                    result.push(op);
                }
            }
            iter.advance();
        }

        Ok(result)
    }

    fn load_operations_by_target(&self, target_id: &ContentId) -> Result<Vec<Operation<ContentId, T>>> {
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
                if op.target == *target_id {
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
    fn test_load_operations_by_genesis() {
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

        let retrieved_ops = storage.load_operations_by_genesis(&target);

        assert!(retrieved_ops.is_ok());
        let ops = retrieved_ops.unwrap();
        assert_eq!(ops.len(), 2);
        assert!(ops.contains(&op1));
        assert!(ops.contains(&op2));
    }

    #[test]
    fn test_load_operations_by_target() {
        let (storage, _dir) = setup_test_storage();
        let shared_target = DummyContentId("shared".into());
        let _genesis_a = DummyContentId("genesis_a".into());
        let genesis_b = DummyContentId("genesis_b".into());
        let payload = DummyPayload("test".into());
        let author = "Alice".to_string();

        // Create operation with target = genesis (series A)
        let op1 = Operation::new(
            shared_target.clone(),
            OperationType::Create(payload.clone()),
            author.clone(),
        );

        // Update operation with different genesis but same target (series B)
        let op2 = Operation::new_with_genesis(
            shared_target.clone(),
            genesis_b.clone(),
            OperationType::Update(DummyPayload("updated".into())),
            author.clone(),
        );

        storage.save_operation(&op1).unwrap();
        storage.save_operation(&op2).unwrap();

        // Load by target should return both operations
        let ops_by_target = storage.load_operations_by_target(&shared_target).unwrap();
        assert_eq!(ops_by_target.len(), 2);
        assert!(ops_by_target.contains(&op1));
        assert!(ops_by_target.contains(&op2));

        // Load by genesis should return only matching genesis
        let ops_by_genesis_a = storage.load_operations_by_genesis(&shared_target).unwrap();
        assert_eq!(ops_by_genesis_a.len(), 1);
        assert!(ops_by_genesis_a.contains(&op1));

        let ops_by_genesis_b = storage.load_operations_by_genesis(&genesis_b).unwrap();
        assert_eq!(ops_by_genesis_b.len(), 1);
        assert!(ops_by_genesis_b.contains(&op2));
    }

    /// Demonstrates that both operations are returned when querying by target,
    /// but only matching genesis when querying by genesis.
    #[test]
    fn test_same_target_different_genesis_collision() {
        let (storage, _dir) = setup_test_storage();
        let target = DummyContentId("shared".into());
        let payload = DummyPayload("one".into());
        // Create (genesis = target)
        let create = Operation::new(
            target.clone(),
            OperationType::Create(payload.clone()),
            "u1".into(),
        );
        storage.save_operation(&create).unwrap();

        // Update with DIFFERENT genesis but same target
        let different_genesis = DummyContentId("DIFF".into());
        let update = Operation::new_with_genesis(
            target.clone(),
            different_genesis.clone(),
            OperationType::Update(DummyPayload("two".into())),
            "u1".into(),
        );
        storage.save_operation(&update).unwrap();

        // Load by target should return both
        let ops_by_target = storage.load_operations_by_target(&target).unwrap();
        assert_eq!(ops_by_target.len(), 2);

        // Load by genesis should return only the create operation
        let ops_by_genesis = storage.load_operations_by_genesis(&target).unwrap();
        assert_eq!(ops_by_genesis.len(), 1);
        assert!(ops_by_genesis.contains(&create));
    }
}
