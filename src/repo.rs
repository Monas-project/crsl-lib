use crate:: {
    crdt::{crdt_state::CrdtState, operation::{Operation, OperationType}, reducer::LwwReducer, storage::OperationStorage},
    graph::{dag::DagGraph, storage::NodeStorage},
};
use cid::Cid;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub struct Repo<OpStore, NodeStore, Payload> 
where 
    OpStore: OperationStorage<Cid, Payload>,
    NodeStore: NodeStorage<Payload, ()>,
    Payload: Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    pub state: CrdtState<Cid, Payload, OpStore, LwwReducer>,
    pub dag: DagGraph<NodeStore, Payload, ()>,
}

impl<OpStore, NodeStore, Payload> Repo<OpStore, NodeStore, Payload>
where 
    OpStore: OperationStorage<Cid, Payload>,
    NodeStore: NodeStorage<Payload, ()>,
    Payload: Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    pub fn new(state: CrdtState<Cid, Payload, OpStore, LwwReducer>,
        dag: DagGraph<NodeStore, Payload, ()>) -> Self {
        Self { state, dag }
    }

    pub fn commit_operation(&mut self, op: Operation<Cid, Payload>) -> Cid {
        self.state.apply(op.clone());
        let parents = self
            .dag
            .latest_head(&op.target)
            .into_iter()
            .collect::<Vec<_>>();
        
        let cid = match &op.kind {
            OperationType::Create(payload) | OperationType::Update(payload) => {
                self.dag
                    .add_node(payload.clone(), parents, ())
                    .expect("add node")
            }
            OperationType::Delete => {
                // For delete operations, we create a node with the last known payload
                // This ensures we maintain the DAG structure while marking the content as deleted
                let last_payload = self.state.get_state(&op.target)
                    .expect("content must exist for delete operation");
                self.dag
                    .add_node(last_payload, parents, ())
                    .expect("add node")
            }
        };
        
        self.dag.set_head(&op.target, cid);
        cid
    }

    pub fn latest(&self, target: &Cid) -> Option<Cid> {
        self.dag.latest_head(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::operation::{Operation, OperationType};
    use crate::crdt::storage::LeveldbStorage;
    use crate::graph::storage::LeveldbNodeStorage;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(transparent)]
    struct TestPayload(String);

    type TestRepo = Repo<LeveldbStorage<Cid, TestPayload>, LeveldbNodeStorage<TestPayload, ()>, TestPayload>;

    fn setup_test_repo() -> (TestRepo, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let op_storage = LeveldbStorage::open(dir.path().join("ops"));
        let node_storage = LeveldbNodeStorage::open(dir.path().join("nodes"));
        let state = CrdtState::new(op_storage);
        let dag = DagGraph::new(node_storage);
        let repo = Repo::new(state, dag);
        (repo, dir)
    }

    fn make_test_operation(target: Cid, kind: OperationType<TestPayload>) -> Operation<Cid, TestPayload> {
        Operation {
            id: Ulid::new(),
            target,
            genesis: target,
            kind,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            author: "test".to_string(),
        }
    }
    fn make_test_operation_with_genesis(target: Cid, genesis: Cid, kind: OperationType<TestPayload>) -> Operation<Cid, TestPayload> {
        Operation {
            id: Ulid::new(),
            target,
            genesis,
            kind,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            author: "test".to_string(),
        }
    }

    #[test]
    fn test_create_operation() {
        let (mut repo, _) = setup_test_repo();
        let target = Cid::new_v1(0x55, multihash::Multihash::<64>::wrap(0x12, b"test").unwrap());
        let payload = TestPayload("test content".to_string());
        let op = make_test_operation(target, OperationType::Create(payload.clone()));

        let cid = repo.commit_operation(op);

        assert!(repo.latest(&target).is_some());
        assert_eq!(repo.latest(&target).unwrap(), cid);
    }

    #[test]
    fn test_update_operation() {
        let (mut repo, _) = setup_test_repo();
        let target = Cid::new_v1(0x55, multihash::Multihash::<64>::wrap(0x12, b"test").unwrap());
        let create_op = make_test_operation(target, OperationType::Create(TestPayload("initial".to_string())));
        let create_cid = repo.commit_operation(create_op);

        let update_op = make_test_operation_with_genesis(target, create_cid, OperationType::Update(TestPayload("updated".to_string())));
        let update_cid = repo.commit_operation(update_op);

        assert!(repo.latest(&target).is_some());
        assert_eq!(repo.latest(&target).unwrap(), update_cid);
        assert_ne!(create_cid, update_cid);
    }

    #[test]
    fn test_delete_operation() {
        let (mut repo, _) = setup_test_repo();
        let target = Cid::new_v1(0x55, multihash::Multihash::<64>::wrap(0x12, b"test").unwrap());
        let create_op = make_test_operation(target, OperationType::Create(TestPayload("initial".to_string())));
        let create_cid = repo.commit_operation(create_op);

        let delete_op = make_test_operation_with_genesis(target, create_cid, OperationType::Delete);
        let delete_cid = repo.commit_operation(delete_op);

        assert!(repo.latest(&target).is_some());
        assert_eq!(repo.latest(&target).unwrap(), delete_cid);
        assert_ne!(create_cid, delete_cid);
    }

    #[test]
    fn test_multiple_targets() {
        let (mut repo, _) = setup_test_repo();
        let target1 = Cid::new_v1(0x55, multihash::Multihash::<64>::wrap(0x12, b"test1").unwrap());
        let target2 = Cid::new_v1(0x55, multihash::Multihash::<64>::wrap(0x12, b"test2").unwrap());
        
        // Create two different targets
        let create1_op = make_test_operation(target1, OperationType::Create(TestPayload("target1".to_string())));
        let create1_cid = repo.commit_operation(create1_op);

        let create2_op = make_test_operation(target2, OperationType::Create(TestPayload("target2".to_string())));
        let create2_cid = repo.commit_operation(create2_op);

        assert!(repo.latest(&target1).is_some());
        assert!(repo.latest(&target2).is_some());
        assert_eq!(repo.latest(&target1).unwrap(), create1_cid);
        assert_eq!(repo.latest(&target2).unwrap(), create2_cid);
        assert_ne!(create1_cid, create2_cid);
    }
}