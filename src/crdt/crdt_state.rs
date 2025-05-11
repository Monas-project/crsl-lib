use crate::crdt::operation::Operation;
use crate::crdt::storage::OperationStorage;
use crate::crdt::reducer::Reducer;
use std::marker::PhantomData;

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
    ContentId: Clone,
    T: Clone,
    S: OperationStorage<ContentId, T>,
    R: Reducer<ContentId, T>,
{
    pub fn new(storage: S) -> Self {
        CrdtState { storage, _marker: PhantomData }
    }
    pub fn apply(&self, op: Operation<ContentId, T>){
        self.storage.save_operation(&op);
    }
    pub fn get_state(&self, content_id: &ContentId) -> Option<T> {
        let ops = self.storage.load_operations(content_id);
        R::reduce(&ops)
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

    fn make_op(id: u64, ts: u64, kind: OperationType<DummyPayload>) -> Operation<DummyContentId, DummyPayload> {
        let mut op = Operation::new_with_genesis(DummyContentId(id.to_string()), DummyContentId(id.to_string()), kind, "tester".into());
        op.timestamp = ts;
        op
    }
    
    #[test]
    fn test_create_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::crdt::storage::LeveldbStorage::<DummyContentId, DummyPayload>::open(dir.path());
        let state: CrdtState<DummyContentId, DummyPayload, _, LwwReducer> = CrdtState::new(storage);
        let op = make_op(1, 100, OperationType::Create(DummyPayload("A".to_string())));

        state.apply(op);

        assert_eq!(state.get_state(&DummyContentId("1".to_string())), Some(DummyPayload("A".to_string())));
    }

    #[test]
    fn test_update_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::crdt::storage::LeveldbStorage::<DummyContentId, DummyPayload>::open(dir.path());
        let state: CrdtState<DummyContentId, DummyPayload, _, LwwReducer> = CrdtState::new(storage);
        let op1 = make_op(1, 100, OperationType::Create(DummyPayload("A".to_string())));
        let op2 = make_op(1, 200, OperationType::Update(DummyPayload("B".to_string())));

        state.apply(op1);
        state.apply(op2);

        assert_eq!(state.get_state(&DummyContentId("1".to_string())), Some(DummyPayload("B".to_string())));
    }

    #[test]
    fn test_delete_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::crdt::storage::LeveldbStorage::<DummyContentId, DummyPayload>::open(dir.path());
        let state: CrdtState<DummyContentId, DummyPayload, _, LwwReducer> = CrdtState::new(storage);
        let op1 = make_op(1, 100, OperationType::Create(DummyPayload("A".to_string())));
        let op2 = make_op(1, 200, OperationType::Update(DummyPayload("B".to_string())));
        let op3 = make_op(1, 300, OperationType::Delete);

        state.apply(op1);
        state.apply(op2);
        state.apply(op3);
        assert_eq!(state.get_state(&DummyContentId("1".to_string())), None);
    }
}