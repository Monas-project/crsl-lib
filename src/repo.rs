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

    pub fn commit_operation(&mut self, op: Operation<Cid, Payload>) -> Result<Cid> {
        // For delete operations, get the state before applying the operation
        let last_payload_for_delete = if matches!(op.kind, OperationType::Delete) {
            self.state.get_state_for_genesis(&op.genesis, &op.target)
        } else {
            None
        };

        self.state.apply(op.clone())?;

        let cid = match &op.kind {
            OperationType::Create(payload) => self.dag.add_genesis_node(payload.clone(), ())?,
            OperationType::Update(payload) => {
                let parents = self
                    .dag
                    .calculate_latest(&op.genesis)
                    .ok()
                    .flatten()
                    .map(|head| vec![head])
                    .unwrap_or_default();

                self.dag
                    .add_version_node(payload.clone(), parents, op.genesis, ())?
            }
            OperationType::Delete => {
                let parents = self
                    .dag
                    .calculate_latest(&op.genesis)
                    .ok()
                    .flatten()
                    .map(|head| vec![head])
                    .unwrap_or_default();

                let last_payload = last_payload_for_delete
                    .expect("content must exist for delete operation");

                self.dag
                    .add_version_node(last_payload, parents, op.genesis, ())?
            }
        };

        Ok(cid)
    }

    pub fn latest(&self, genesis_id: &Cid) -> Option<Cid> {
        self.dag.calculate_latest(genesis_id).ok().flatten()
    }

    /// Get the complete history from genesis
    pub fn get_history(&self, genesis: &Cid) -> Result<Vec<Cid>> {
        if let Some(latest) = self.latest(genesis) {
            self.dag
                .get_history_from_version(&latest)
                .map_err(crate::crdt::error::CrdtError::Graph)
        } else {
            Ok(vec![])
        }
    }

    /// Get genesis from any version
    pub fn get_genesis(&self, version: &Cid) -> Result<Cid> {
        self.dag
            .get_genesis(version)
            .map_err(crate::crdt::error::CrdtError::Graph)
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
        target: Cid,
        kind: OperationType<TestPayload>,
    ) -> Operation<Cid, TestPayload> {
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
    fn make_test_operation_with_genesis(
        target: Cid,
        genesis: Cid,
        kind: OperationType<TestPayload>,
    ) -> Operation<Cid, TestPayload> {
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

        let update_op = make_test_operation_with_genesis(
            target,
            create_cid,
            OperationType::Update(TestPayload("updated".to_string())),
        );
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

        let delete_op = make_test_operation_with_genesis(target, target, OperationType::Delete);
        let delete_cid = repo.commit_operation(delete_op).unwrap();

        assert!(repo.latest(&target).is_some());
        assert_eq!(repo.latest(&target).unwrap(), delete_cid);
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
        let update_a = make_test_operation_with_genesis(
            shared_target,
            genesis_a,
            OperationType::Update(TestPayload("A2".into())),
        );
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
        let genesis1 = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"genesis1").unwrap(),
        );
        let genesis2 = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"genesis2").unwrap(),
        );

        // User1: Create in series 1
        let create1 = Operation::new_with_genesis(
            shared_target,
            genesis1,
            OperationType::Create(TestPayload("u1".into())),
            "user1".to_string(),
        );
        let _cid1 = repo.commit_operation(create1).unwrap();

        // User2: Create in series 2 (different genesis, same target)
        let create2 = Operation::new_with_genesis(
            shared_target,
            genesis2,
            OperationType::Create(TestPayload("u2".into())),
            "user2".to_string(),
        );
        let _cid2 = repo.commit_operation(create2).unwrap();

        // User1 delete in series 1 (make it happen first)
        std::thread::sleep(std::time::Duration::from_millis(10));
        let del_op = Operation::new_with_genesis(
            shared_target,
            genesis1,
            OperationType::Delete,
            "user1".to_string(),
        );
        repo.commit_operation(del_op).unwrap();

        // User2 update in series 2 (make it happen after delete)
        std::thread::sleep(std::time::Duration::from_millis(10));
        let update2 = Operation::new_with_genesis(
            shared_target,
            genesis2,
            OperationType::Update(TestPayload("u2_updated".into())),
            "user2".to_string(),
        );
        repo.commit_operation(update2).unwrap();

        // After deletion in series 1, series 2 should still be visible
        // The state should show the latest update from series 2
        assert_eq!(
            repo.state.get_state(&shared_target),
            Some(TestPayload("u2_updated".into()))
        );
    }

    #[test]
    fn test_concurrent_operations_same_target_different_genesis() {
        let (mut repo, _) = setup_test_repo();
        let shared_target = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"concurrent").unwrap(),
        );
        let genesis1 = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"gen1").unwrap(),
        );
        let genesis2 = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"gen2").unwrap(),
        );

        // Create operations in parallel series
        let create1 = Operation::new_with_genesis(
            shared_target,
            genesis1,
            OperationType::Create(TestPayload("series1_v1".into())),
            "user1".to_string(),
        );
        repo.commit_operation(create1).unwrap();

        let create2 = Operation::new_with_genesis(
            shared_target,
            genesis2,
            OperationType::Create(TestPayload("series2_v1".into())),
            "user2".to_string(),
        );
        repo.commit_operation(create2).unwrap();

        // Multiple updates in both series
        std::thread::sleep(std::time::Duration::from_millis(5));
        let update1 = Operation::new_with_genesis(
            shared_target,
            genesis1,
            OperationType::Update(TestPayload("series1_v2".into())),
            "user1".to_string(),
        );
        repo.commit_operation(update1).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(5));
        let update2 = Operation::new_with_genesis(
            shared_target,
            genesis2,
            OperationType::Update(TestPayload("series2_v2".into())),
            "user2".to_string(),
        );
        repo.commit_operation(update2).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(5));
        let update3 = Operation::new_with_genesis(
            shared_target,
            genesis1,
            OperationType::Update(TestPayload("series1_v3".into())),
            "user1".to_string(),
        );
        repo.commit_operation(update3).unwrap();

        // The global state should reflect the latest operation
        assert_eq!(
            repo.state.get_state(&shared_target),
            Some(TestPayload("series1_v3".into()))
        );

        // Each genesis should have its own state
        assert_eq!(
            repo.state.get_state_for_genesis(&genesis1, &shared_target),
            Some(TestPayload("series1_v3".into()))
        );
        assert_eq!(
            repo.state.get_state_for_genesis(&genesis2, &shared_target),
            Some(TestPayload("series2_v2".into()))
        );
    }

    #[test]
    fn test_delete_in_one_series_preserves_other() {
        let (mut repo, _) = setup_test_repo();
        let shared_target = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"delete_test").unwrap(),
        );
        let genesis1 = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"g1").unwrap(),
        );
        let genesis2 = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"g2").unwrap(),
        );

        // Create in both series
        let create1 = Operation::new_with_genesis(
            shared_target,
            genesis1,
            OperationType::Create(TestPayload("data1".into())),
            "u1".to_string(),
        );
        repo.commit_operation(create1).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(5));
        let create2 = Operation::new_with_genesis(
            shared_target,
            genesis2,
            OperationType::Create(TestPayload("data2".into())),
            "u2".to_string(),
        );
        repo.commit_operation(create2).unwrap();

        // Delete in series 1
        std::thread::sleep(std::time::Duration::from_millis(5));
        let delete1 = Operation::new_with_genesis(
            shared_target,
            genesis1,
            OperationType::Delete,
            "u1".to_string(),
        );
        repo.commit_operation(delete1).unwrap();

        // Update in series 2 after delete
        std::thread::sleep(std::time::Duration::from_millis(5));
        let update2 = Operation::new_with_genesis(
            shared_target,
            genesis2,
            OperationType::Update(TestPayload("data2_updated".into())),
            "u2".to_string(),
        );
        repo.commit_operation(update2).unwrap();

        // Series 1 should be deleted
        assert_eq!(
            repo.state.get_state_for_genesis(&genesis1, &shared_target),
            None
        );

        // Series 2 should still exist with updated value
        assert_eq!(
            repo.state.get_state_for_genesis(&genesis2, &shared_target),
            Some(TestPayload("data2_updated".into()))
        );

        // Global state should show series 2 update (happened after delete)
        assert_eq!(
            repo.state.get_state(&shared_target),
            Some(TestPayload("data2_updated".into()))
        );
    }
}
