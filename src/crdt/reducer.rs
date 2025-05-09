use crate::crdt::operation::{Operation, OperationType};

pub trait Reducer<ContentId, T> {
    fn reduce(ops: &[Operation<ContentId, T>]) -> Option<T>;
}

pub struct LwwReducer;
impl<ContentId, T> Reducer<ContentId, T> for LwwReducer
where
    T: Clone,
{
    fn reduce(ops: &[Operation<ContentId, T>]) -> Option<T> {
        ops.iter()
            .max_by_key(|op| op.timestamp)
            .and_then(|op| match &op.kind {
                OperationType::Create(v) | OperationType::Update(v) => Some(v.clone()),
                OperationType::Delete => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::operation::{Operation, OperationType};
    use serde::{Deserialize, Serialize};
    use ulid::Ulid;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct DummyContentId(String);

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct DummyPayload(String);

    fn make_op(
        id: u64,
        ts: u64,
        kind: OperationType<DummyPayload>,
    ) -> Operation<DummyContentId, DummyPayload> {
        Operation {
            id: Ulid::new(),
            target: DummyContentId(id.to_string()),
            genesis: DummyContentId(id.to_string()),
            kind,
            timestamp: ts,
            author: "test".into(),
        }
    }

    #[test]
    fn lww_reducer_picks_latest_update() {
        let op1 = make_op(1, 100, OperationType::Create(DummyPayload("A".into())));
        let op2 = make_op(1, 200, OperationType::Update(DummyPayload("B".into())));
        let op3 = make_op(1, 150, OperationType::Update(DummyPayload("C".into())));
        let ops = vec![op1, op2.clone(), op3];

        let state = LwwReducer::reduce(&ops);
        println!("state: {:?}", state);

        assert_eq!(state, Some(DummyPayload("B".into())));
    }

    #[test]
    fn lww_reducer_handles_delete() {
        let op1 = make_op(1, 100, OperationType::Create(DummyPayload("A".into())));
        let del = make_op(1, 200, OperationType::Delete);
        let ops = vec![op1, del.clone()];

        let state = LwwReducer::reduce(&ops);

        assert_eq!(state, None);
    }

    #[test]
    fn lww_reducer_empty_ops() {
        let ops: Vec<Operation<DummyContentId, DummyPayload>> = vec![];

        let state = LwwReducer::reduce(&ops);

        assert_eq!(state, None);
    }

    #[test]
    fn lww_reducer_same_timestamp() {
        let op1 = make_op(1, 100, OperationType::Create(DummyPayload("A".into())));
        let op2 = make_op(1, 100, OperationType::Update(DummyPayload("B".into())));
        let op3 = make_op(1, 100, OperationType::Update(DummyPayload("C".into())));
        let ops = vec![op1, op2, op3];

        let state = LwwReducer::reduce(&ops);

        assert_eq!(state, Some(DummyPayload("C".into())));
    }
}
