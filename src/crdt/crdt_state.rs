use crate::crdt::error::{CrdtError, Result, ValidationError};
use crate::crdt::operation::{Operation, OperationType};
use crate::crdt::reducer::Reducer;
use crate::crdt::storage::OperationStorage;
use std::fmt::Debug;
use std::marker::PhantomData;
use ulid::Ulid;
/// A generic CRDT state container that manages operations on content.
///
/// `CrdtState` provides a high-level interface for applying operations to content
/// and retrieving the current state through a reducer. It supports both raw operation
/// application and validated operation application.
///
/// # Type Parameters
///
/// * `ContentId` - The type used to identify content
/// * `T` - The payload type for operations
/// * `S` - The storage implementation for operations
/// * `R` - The reducer implementation for determining current state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrdtState<ContentId, T, S, R>
where
    S: OperationStorage<ContentId, T>,
    R: Reducer<ContentId, T>,
{
    storage: S,
    _marker: PhantomData<(T, ContentId, R)>,
}

impl<ContentId, T, S, R> CrdtState<ContentId, T, S, R>
where
    ContentId: Clone + Debug,
    T: Clone,
    S: OperationStorage<ContentId, T>,
    R: Reducer<ContentId, T>,
{
    pub fn new(storage: S) -> Self {
        CrdtState {
            storage,
            _marker: PhantomData,
        }
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }
    /// Applies an operation to the CRDT state without validation.
    ///
    /// This method directly saves the operation to storage without checking its validity.
    /// Use this method when operations have already been validated elsewhere or when
    /// performance is critical.
    ///
    /// # Parameters
    ///
    /// * `op` - The operation to apply
    pub fn apply(&self, op: Operation<ContentId, T>) -> Result<()> {
        self.storage.save_operation(&op)
    }

    /// Applies an operation to the CRDT state with validation.
    ///
    /// This method first validates the operation using `validate_operation`. If validation
    /// passes, the operation is applied; otherwise, it is rejected.
    ///
    /// Use this method to ensure operations maintain logical consistency (e.g., not updating
    /// content that doesn't exist).
    ///
    /// # Parameters
    ///
    /// * `op` - The operation to validate and potentially apply
    pub fn apply_with_validation(&self, op: Operation<ContentId, T>) -> Result<()> {
        if self.validate_operation(&op)? {
            self.apply(op)
        } else {
            Err(CrdtError::Validation(ValidationError::MissingCreate(
                format!("No create operation found for genesis: {:?}", op.genesis),
            )))
        }
    }
    pub fn get_state(&self, genesis: &ContentId) -> Option<T> {
        let ops = self.storage.load_operations(genesis).ok()?;
        R::reduce(&ops)
    }

    pub fn get_operations_by_genesis(
        &self,
        genesis: &ContentId,
    ) -> Result<Vec<Operation<ContentId, T>>> {
        self.storage.load_operations(genesis)
    }

    pub fn get_operation(&self, op_id: &Ulid) -> Result<Option<Operation<ContentId, T>>> {
        self.storage.get_operation(op_id)
    }

    pub fn delete_operation(&self, op_id: &Ulid) -> Result<()> {
        self.storage.delete_operation(op_id)
    }

    /// Validates whether an operation is logically valid to apply.
    ///
    /// This method performs the following checks:
    /// - For Update and Delete operations, ensures a Create operation exists for the target
    /// - Create operations are always considered valid
    ///
    /// # Parameters
    ///
    /// * `op` - The operation to validate
    ///
    /// # Returns
    ///
    /// * `true` - If the operation is valid to apply
    /// * `false` - If the operation would violate logical constraints
    pub fn validate_operation(&self, op: &Operation<ContentId, T>) -> Result<bool> {
        match &op.kind {
            OperationType::Update(_) | OperationType::Delete | OperationType::Merge(_) => {
                let ops = self.storage.load_operations(&op.genesis)?;
                Ok(ops
                    .iter()
                    .any(|o| matches!(o.kind, OperationType::Create(_))))
            }
            _ => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::operation::{Operation, OperationType};
    use crate::crdt::reducer::LwwReducer;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct DummyContentId(String);

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct DummyPayload(String);

    /// Helper for constructing operations with deterministic timestamps.
    fn make_op(
        id: u64,
        ts: u64,
        kind: OperationType<DummyPayload>,
    ) -> Operation<DummyContentId, DummyPayload> {
        let mut op = Operation::new(DummyContentId(id.to_string()), kind, "tester".into());
        op.timestamp = ts;
        op
    }

    #[test]
    fn test_create_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            crate::crdt::storage::LeveldbStorage::<DummyContentId, DummyPayload>::open(dir.path())
                .unwrap();
        let state: CrdtState<DummyContentId, DummyPayload, _, LwwReducer> = CrdtState::new(storage);
        let op = make_op(1, 100, OperationType::Create(DummyPayload("A".to_string())));

        state.apply(op).unwrap();

        assert_eq!(
            state.get_state(&DummyContentId("1".to_string())),
            Some(DummyPayload("A".to_string()))
        );
    }

    #[test]
    fn test_update_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            crate::crdt::storage::LeveldbStorage::<DummyContentId, DummyPayload>::open(dir.path())
                .unwrap();
        let state: CrdtState<DummyContentId, DummyPayload, _, LwwReducer> = CrdtState::new(storage);
        let op1 = make_op(1, 100, OperationType::Create(DummyPayload("A".to_string())));
        let op2 = make_op(1, 200, OperationType::Update(DummyPayload("B".to_string())));

        state.apply(op1).unwrap();
        state.apply(op2).unwrap();

        assert_eq!(
            state.get_state(&DummyContentId("1".to_string())),
            Some(DummyPayload("B".to_string()))
        );
    }

    #[test]
    fn test_delete_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            crate::crdt::storage::LeveldbStorage::<DummyContentId, DummyPayload>::open(dir.path())
                .unwrap();
        let state: CrdtState<DummyContentId, DummyPayload, _, LwwReducer> = CrdtState::new(storage);
        let op1 = make_op(1, 100, OperationType::Create(DummyPayload("A".to_string())));
        let op2 = make_op(1, 200, OperationType::Update(DummyPayload("B".to_string())));
        let op3 = make_op(1, 300, OperationType::Delete);

        state.apply(op1).unwrap();
        state.apply(op2).unwrap();
        state.apply(op3).unwrap();
        assert_eq!(state.get_state(&DummyContentId("1".to_string())), None);
    }

    #[test]
    fn test_validate_operation() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            crate::crdt::storage::LeveldbStorage::<DummyContentId, DummyPayload>::open(dir.path())
                .unwrap();
        let state: CrdtState<DummyContentId, DummyPayload, _, LwwReducer> = CrdtState::new(storage);
        let op1 = make_op(1, 100, OperationType::Create(DummyPayload("A".to_string())));
        let op2 = make_op(1, 200, OperationType::Update(DummyPayload("B".to_string())));
        state.apply(op1).unwrap();

        let result = state.validate_operation(&op2).unwrap();

        assert!(result);
    }

    #[test]
    fn test_apply_with_validation() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            crate::crdt::storage::LeveldbStorage::<DummyContentId, DummyPayload>::open(dir.path())
                .unwrap();
        let state: CrdtState<DummyContentId, DummyPayload, _, LwwReducer> = CrdtState::new(storage);
        let op1 = make_op(1, 100, OperationType::Create(DummyPayload("A".to_string())));
        let op2 = make_op(1, 200, OperationType::Update(DummyPayload("B".to_string())));
        state.apply(op1).unwrap();

        state.apply_with_validation(op2).unwrap();

        assert_eq!(
            state.get_state(&DummyContentId("1".to_string())),
            Some(DummyPayload("B".to_string()))
        );
    }

    /// This test demonstrates an edge-case where two different genesis IDs share the same
    /// `target`.  The update with a different genesis is **ignored** by `get_state`, which
    /// filters by `op.genesis == content_id`.  The correct behaviour from a user perspective
    /// would be to see the latest payload ("B") but the current implementation wrongly keeps
    /// the older payload ("A").  The assertion therefore fails and captures the bug.
    #[test]
    fn test_same_target_different_genesis_collision() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            crate::crdt::storage::LeveldbStorage::<DummyContentId, DummyPayload>::open(dir.path())
                .unwrap();
        let state: CrdtState<DummyContentId, DummyPayload, _, LwwReducer> = CrdtState::new(storage);

        // Create operation with target "X" (genesis = target)
        let create = Operation::new(
            DummyContentId("X".into()),
            OperationType::Create(DummyPayload("A".into())),
            "u1".into(),
        );
        state.apply(create.clone()).unwrap();

        // Simulate an update coming from another genesis (different series) but same target.
        let fake_genesis = DummyContentId("DIFFERENT".into());
        let update = Operation::new(
            fake_genesis,
            OperationType::Update(DummyPayload("B".into())),
            "u1".into(),
        );
        state.apply(update).unwrap();

        // Should only get operations with matching genesis, so expect "A"
        assert_eq!(
            state.get_state(&DummyContentId("X".into())),
            Some(DummyPayload("A".into()))
        );
    }

    #[test]
    fn test_delete_one_genesis_preserves_other_series() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            crate::crdt::storage::LeveldbStorage::<DummyContentId, DummyPayload>::open(dir.path())
                .unwrap();
        let state: CrdtState<DummyContentId, DummyPayload, _, LwwReducer> = CrdtState::new(storage);

        let primary_genesis = DummyContentId("X".into());
        let alt_genesis = DummyContentId("ALT".into());

        let mut primary_create = Operation::new(
            primary_genesis.clone(),
            OperationType::Create(DummyPayload("A".into())),
            "u1".into(),
        );
        primary_create.timestamp = 100;

        let mut alt_create = Operation::new(
            alt_genesis.clone(),
            OperationType::Create(DummyPayload("B".into())),
            "u2".into(),
        );
        alt_create.timestamp = 150;

        let mut alt_update = Operation::new(
            alt_genesis.clone(),
            OperationType::Update(DummyPayload("C".into())),
            "u2".into(),
        );
        alt_update.timestamp = 300;

        let mut primary_delete =
            Operation::new(primary_genesis.clone(), OperationType::Delete, "u1".into());
        primary_delete.timestamp = 400;

        state.apply(primary_create).unwrap();
        state.apply(alt_create.clone()).unwrap();
        state.apply(alt_update.clone()).unwrap();
        state.apply(primary_delete).unwrap();

        assert_eq!(state.get_state(&primary_genesis), None);

        assert_eq!(
            state.get_state(&alt_genesis),
            Some(DummyPayload("C".into()))
        );

        let operations = state.get_operations_by_genesis(&alt_genesis).unwrap();
        assert_eq!(operations.len(), 2);
        assert!(operations
            .iter()
            .any(|op| op.kind == OperationType::Create(DummyPayload("B".into()))));
        assert!(operations
            .iter()
            .any(|op| op.kind == OperationType::Update(DummyPayload("C".into()))));
    }
}
