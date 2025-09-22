use crate::crdt::error::{CrdtError, Result, ValidationError};
use crate::crdt::operation::{Operation, OperationType};
use crate::crdt::reducer::Reducer;
use crate::crdt::storage::OperationStorage;
use std::fmt::Debug;
use std::marker::PhantomData;
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
                format!("No create operation found for target: {:?}", op.target),
            )))
        }
    }
    pub fn get_state(&self, target_id: &ContentId) -> Option<T> {
        let ops = self.storage.load_operations_by_target(target_id).ok()?;
        R::reduce(&ops)
    }

    /// Get state for a specific genesis and target combination.
    /// This is useful when you need to isolate operations to a specific series.
    pub fn get_state_for_genesis(&self, genesis_id: &ContentId, target_id: &ContentId) -> Option<T>
    where
        ContentId: PartialEq,
    {
        let ops = self.storage.load_operations_by_genesis(genesis_id).ok()?;
        let filtered_ops: Vec<_> = ops
            .into_iter()
            .filter(|op| op.target == *target_id)
            .collect();
        R::reduce(&filtered_ops)
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
            OperationType::Update(_) | OperationType::Delete => {
                let ops = self.storage.load_operations_by_genesis(&op.genesis)?;
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

    fn make_op(
        id: u64,
        ts: u64,
        kind: OperationType<DummyPayload>,
    ) -> Operation<DummyContentId, DummyPayload> {
        let mut op = Operation::new_with_genesis(
            DummyContentId(id.to_string()),
            DummyContentId(id.to_string()),
            kind,
            "tester".into(),
        );
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

    /// This test demonstrates that when two different genesis IDs share the same target,
    /// get_state correctly returns the latest state across all operations for that target.
    /// With the fixed implementation, both operations are considered when determining
    /// the final state, so the LWW reducer will correctly return "B" as the latest value.
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

        // Small delay to ensure update has a later timestamp
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Simulate an update coming from another genesis (different series) but same target.
        let fake_genesis = DummyContentId("DIFFERENT".into());
        let update = Operation::new_with_genesis(
            DummyContentId("X".into()),
            fake_genesis,
            OperationType::Update(DummyPayload("B".into())),
            "u1".into(),
        );
        state.apply(update).unwrap();

        // Now both operations are considered, and LWW will return "B" as it has a later timestamp
        assert_eq!(
            state.get_state(&DummyContentId("X".into())),
            Some(DummyPayload("B".into()))
        );
    }
}
