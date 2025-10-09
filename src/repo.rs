use crate::crdt::error::Result;
use crate::{
    crdt::{
        crdt_state::CrdtState,
        operation::{Operation, OperationType},
        reducer::LwwReducer,
        storage::OperationStorage,
    },
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
    pub fn new(
        state: CrdtState<Cid, Payload, OpStore, LwwReducer>,
        dag: DagGraph<NodeStore, Payload, ()>,
    ) -> Self {
        Self { state, dag }
    }

    pub fn commit_operation(&mut self, mut op: Operation<Cid, Payload>) -> Result<Cid> {
        let cid = match &op.kind {
            OperationType::Create(payload) => {
                let genesis_cid = self.dag.add_genesis_node(payload.clone(), ())?;
                op.genesis = genesis_cid;
                genesis_cid
            }
            OperationType::Update(payload) => {
                let parents = self.get_latest_parents(&op.genesis);
                self.dag
                    .add_child_node(payload.clone(), parents, op.genesis, ())?
            }
            OperationType::Delete => {
                let parents = self.get_latest_parents(&op.genesis);

                // For delete operations, find the latest payload in the original genesis chain
                let ops = self
                    .state
                    .get_operations_by_genesis(&op.genesis)
                    .expect("Failed to load operations for delete");
                let last_payload = ops
                    .iter()
                    .filter(|o| o.payload().is_some())
                    .max_by_key(|o| o.timestamp)
                    .expect("content must exist for delete operation")
                    .payload()
                    .unwrap()
                    .clone();

                self.dag
                    .add_child_node(last_payload, parents, op.genesis, ())?
            }
            OperationType::Merge(payload) => {
                let parents = self.get_latest_parents(&op.genesis);
                self.dag
                    .add_child_node(payload.clone(), parents, op.genesis, ())?
            }
        };

        self.state.apply(op)?;

        Ok(cid)
    }

    pub fn latest(&self, genesis_id: &Cid) -> Option<Cid> {
        self.dag.calculate_latest(genesis_id).ok().flatten()
    }

    /// Get the complete history from genesis
    pub fn get_history(&self, genesis: &Cid) -> Result<Vec<Cid>> {
        self.dag
            .get_nodes_by_genesis(genesis)
            .map_err(crate::crdt::error::CrdtError::Graph)
    }

    /// Get genesis from any version
    pub fn get_genesis(&self, version: &Cid) -> Result<Cid> {
        self.dag
            .get_genesis(version)
            .map_err(crate::crdt::error::CrdtError::Graph)
    }

    /// Get the latest parent nodes for the given genesis
    fn get_latest_parents(&self, genesis: &Cid) -> Vec<Cid> {
        self.dag
            .calculate_latest(genesis)
            .ok()
            .flatten()
            .map(|head| vec![head])
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::operation::{Operation, OperationType};
    use crate::crdt::storage::LeveldbStorage;
    use crate::graph::storage::LeveldbNodeStorage;
    use tempfile::tempdir;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(transparent)]
    struct TestPayload(String);

    type TestRepo =
        Repo<LeveldbStorage<Cid, TestPayload>, LeveldbNodeStorage<TestPayload, ()>, TestPayload>;

    fn setup_test_repo() -> (TestRepo, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let op_storage = LeveldbStorage::open(dir.path().join("ops")).unwrap();
        let node_storage = LeveldbNodeStorage::open(dir.path().join("nodes"));
        let state = CrdtState::new(op_storage);
        let dag = DagGraph::new(node_storage);
        let repo = Repo::new(state, dag);
        (repo, dir)
    }

    fn make_test_operation(
        genesis: Cid,
        kind: OperationType<TestPayload>,
    ) -> Operation<Cid, TestPayload> {
        Operation::new(genesis, kind, "test".into())
    }

    #[test]
    fn test_create_operation() {
        let (mut repo, _) = setup_test_repo();
        let target = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"test").unwrap(),
        );
        let payload = TestPayload("test content".to_string());
        let op = make_test_operation(target, OperationType::Create(payload.clone()));

        let cid = repo.commit_operation(op).unwrap();

        assert!(repo.latest(&cid).is_some());
        assert_eq!(repo.latest(&cid).unwrap(), cid);
    }

    #[test]
    fn test_update_operation() {
        let (mut repo, _) = setup_test_repo();
        let target = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"test").unwrap(),
        );
        let create_op = make_test_operation(
            target,
            OperationType::Create(TestPayload("initial".to_string())),
        );
        let create_cid = repo.commit_operation(create_op).unwrap();

        let update_op = make_test_operation(
            create_cid,
            OperationType::Update(TestPayload("updated".to_string())),
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
        let update_cid = repo.commit_operation(update_op).unwrap();

        assert!(repo.latest(&create_cid).is_some());
        assert_eq!(repo.latest(&create_cid).unwrap(), update_cid);
        assert_ne!(create_cid, update_cid);
    }

    #[test]
    fn test_delete_operation() {
        let (mut repo, _) = setup_test_repo();
        let target = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"test").unwrap(),
        );
        let create_op = make_test_operation(
            target,
            OperationType::Create(TestPayload("initial".to_string())),
        );
        let create_cid = repo.commit_operation(create_op).unwrap();

        let delete_op = make_test_operation(create_cid, OperationType::Delete);
        std::thread::sleep(std::time::Duration::from_millis(1));
        let delete_cid = repo.commit_operation(delete_op).unwrap();

        assert!(repo.latest(&create_cid).is_some());
        assert_eq!(repo.latest(&create_cid).unwrap(), delete_cid);
        assert_ne!(create_cid, delete_cid);
    }

    #[test]
    fn test_multiple_targets() {
        let (mut repo, _) = setup_test_repo();
        let target1 = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"test1").unwrap(),
        );
        let target2 = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"test2").unwrap(),
        );

        let create1_op = make_test_operation(
            target1,
            OperationType::Create(TestPayload("target1".to_string())),
        );
        let create1_cid = repo.commit_operation(create1_op).unwrap();

        let create2_op = make_test_operation(
            target2,
            OperationType::Create(TestPayload("target2".to_string())),
        );
        let create2_cid = repo.commit_operation(create2_op).unwrap();

        assert!(repo.latest(&create1_cid).is_some());
        assert!(repo.latest(&create2_cid).is_some());
        assert_eq!(repo.latest(&create1_cid).unwrap(), create1_cid);
        assert_eq!(repo.latest(&create2_cid).unwrap(), create2_cid);
        assert_ne!(create1_cid, create2_cid);
    }

    #[test]
    fn test_update_keeps_series_isolated() {
        let (mut repo, _) = setup_test_repo();
        let shared_target = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"update_shared").unwrap(),
        );

        // Series A
        let create_a = make_test_operation(
            shared_target,
            OperationType::Create(TestPayload("A1".into())),
        );
        let genesis_a = repo.commit_operation(create_a).unwrap();

        // Series B
        let create_b = make_test_operation(
            shared_target,
            OperationType::Create(TestPayload("B1".into())),
        );
        let genesis_b = repo.commit_operation(create_b).unwrap();

        // Update only series A
        let update_a = make_test_operation(
            genesis_a,
            OperationType::Update(TestPayload("A2".into())),
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
        let latest_a = repo.commit_operation(update_a).unwrap();

        // 確認: series A の latest は更新され、series B は変わらない
        assert_eq!(repo.latest(&genesis_a).unwrap(), latest_a);
        assert_eq!(repo.latest(&genesis_b).unwrap(), genesis_b);
    }

    /// Failing test: Delete on one series still uses `target` and may fetch wrong payload.
    #[test]
    fn test_delete_mixes_series_due_to_target_lookup() {
        let (mut repo, _) = setup_test_repo();
        let shared_target = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"shared").unwrap(),
        );

        // User1: Create
        let create1 = make_test_operation(
            shared_target,
            OperationType::Create(TestPayload("u1".into())),
        );
        let cid1 = repo.commit_operation(create1).unwrap();

        // User2: parallel series
        let create2 = make_test_operation(
            shared_target,
            OperationType::Create(TestPayload("u2".into())),
        );
        let cid2 = repo.commit_operation(create2).unwrap();

        // User2 update in its own series
        let update2 = make_test_operation(
            cid2,
            OperationType::Update(TestPayload("u2_updated".into())),
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
        repo.commit_operation(update2).unwrap();

        // User1 delete
        let del_op = make_test_operation(cid1, OperationType::Delete);
        std::thread::sleep(std::time::Duration::from_millis(1));
        repo.commit_operation(del_op).unwrap();

        assert_eq!(repo.state.get_state(&cid1), None);
        assert_eq!(
            repo.state.get_state(&cid2),
            Some(TestPayload("u2_updated".into()))
        );
    }
}
