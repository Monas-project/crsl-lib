use crate::crdt::operation::{Operation, OperationType};

pub trait Reducer<ContentId, T> {
    fn reduce(ops: &[Operation<ContentId, T>]) -> Option<T>;
}

/// Last-Write-Wins reducer: picks the operation with the highest timestamp,
/// breaking ties by ULID order.
pub struct LwwReducer;
impl<ContentId, T> Reducer<ContentId, T> for LwwReducer
where
    T: Clone,
{
    fn reduce(ops: &[Operation<ContentId, T>]) -> Option<T> {
        ops.iter()
            .max_by(|a, b| {
                a.timestamp
                    .cmp(&b.timestamp)
                    .then(a.id.to_bytes().cmp(&b.id.to_bytes()))
            })
            .and_then(|op| match &op.kind {
                OperationType::Create(v) | OperationType::Update(v) | OperationType::Merge(v) => {
                    Some(v.clone())
                }
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
            genesis: DummyContentId(id.to_string()),
            kind,
            timestamp: ts,
            author: "test".into(),
            parents: Vec::new(),
            node_timestamp: None,
        }
    }

    fn make_op_with_ulid(
        id: u64,
        ts: u64,
        kind: OperationType<DummyPayload>,
        ulid_str: &str,
    ) -> Operation<DummyContentId, DummyPayload> {
        let ulid = Ulid::from_string(ulid_str).unwrap();
        Operation {
            id: ulid,
            genesis: DummyContentId(id.to_string()),
            kind,
            timestamp: ts,
            author: "test".into(),
            parents: Vec::new(),
            node_timestamp: None,
        }
    }

    #[test]
    fn lww_reducer_picks_latest_update() {
        let op1 = make_op(1, 100, OperationType::Create(DummyPayload("A".into())));
        let op2 = make_op(1, 200, OperationType::Update(DummyPayload("B".into())));
        let op3 = make_op(1, 150, OperationType::Update(DummyPayload("C".into())));
        let ops = vec![op1, op2.clone(), op3];

        let state = LwwReducer::reduce(&ops);

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
        let op1 = make_op_with_ulid(
            1,
            100,
            OperationType::Create(DummyPayload("A".into())),
            "01GMTWF61FS176A96AKERBFNNX",
        );
        let op2 = make_op_with_ulid(
            1,
            100,
            OperationType::Update(DummyPayload("B".into())),
            "01GMTWF7ANPDQBCMWTKGSQG4QD",
        );
        let op3 = make_op_with_ulid(
            1,
            100,
            OperationType::Update(DummyPayload("C".into())),
            "01GMTWF9TZQ27MEKTAR4VWZCCT",
        );
        let ops = vec![op1, op2, op3];

        let state = LwwReducer::reduce(&ops);

        assert_eq!(state, Some(DummyPayload("C".into())));
    }
}
