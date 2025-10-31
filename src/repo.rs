use crate::convergence::{
    metadata::ContentMetadata, policies::lww::LwwMergePolicy, policy::MergePolicy,
    resolver::ConflictResolver,
};
use crate::crdt::error::{CrdtError, Result};
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
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

pub struct Repo<OpStore, NodeStore, Payload>
where
    OpStore: OperationStorage<Cid, Payload>,
    NodeStore: NodeStorage<Payload, ContentMetadata>,
    Payload: Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    pub state: CrdtState<Cid, Payload, OpStore, LwwReducer>,
    pub dag: DagGraph<NodeStore, Payload, ContentMetadata>,
    resolver: ConflictResolver<Payload, ContentMetadata>,
}

impl<OpStore, NodeStore, Payload> Repo<OpStore, NodeStore, Payload>
where
    OpStore: OperationStorage<Cid, Payload>,
    NodeStore: NodeStorage<Payload, ContentMetadata>,
    Payload: Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    pub fn new(
        state: CrdtState<Cid, Payload, OpStore, LwwReducer>,
        dag: DagGraph<NodeStore, Payload, ContentMetadata>,
    ) -> Self {
        Self {
            state,
            dag,
            resolver: ConflictResolver::new(),
        }
    }

    pub fn commit_operation(&mut self, op: Operation<Cid, Payload>) -> Result<Cid> {
        // Recently, Merge operations cannot be manually committed
        if matches!(op.kind, OperationType::Merge(_)) {
            return Err(CrdtError::Internal(
                "Merge operations cannot be manually committed".to_string(),
            ));
        }

        self.commit_operation_internal(op, false)
    }

    fn commit_operation_internal(
        &mut self,
        op: Operation<Cid, Payload>,
        skip_auto_merge: bool,
    ) -> Result<Cid> {
        let mut op = op;

        if !skip_auto_merge {
            match &op.kind {
                OperationType::Update(_) | OperationType::Delete => {
                    if op.parents.is_empty() {
                        let merged_head = self
                            .check_and_merge(&op.genesis)?
                            .or_else(|| self.dag.calculate_latest(&op.genesis).ok().flatten())
                            .ok_or_else(|| {
                                CrdtError::Internal(format!(
                                    "No head available for genesis {} to attach operation",
                                    op.genesis
                                ))
                            })?;

                        op.parents = vec![merged_head];
                    } else {
                        self.validate_parent_genesis(&op.genesis, &op.parents)?;
                    }
                }
                OperationType::Merge(_) => {
                    if op.parents.is_empty() {
                        op.parents = self.find_heads(&op.genesis)?;
                    }
                    self.validate_parent_genesis(&op.genesis, &op.parents)?;
                }
                OperationType::Create(_) => {}
            }
        }

        let cid = match &op.kind {
            OperationType::Create(payload) => {
                let genesis_cid = self
                    .dag
                    .add_genesis_node(payload.clone(), ContentMetadata::default())?;
                op.genesis = genesis_cid;
                genesis_cid
            }
            OperationType::Update(payload) => self.dag.add_child_node(
                payload.clone(),
                op.parents.clone(),
                op.genesis,
                self.resolve_metadata_for_commit(&op.genesis, &op.parents)?,
            )?,
            OperationType::Delete => {
                let ops = self.state.get_operations_by_genesis(&op.genesis)?;
                let last_payload = ops
                    .iter()
                    .filter_map(|operation| {
                        operation
                            .payload()
                            .cloned()
                            .map(|payload| (operation.timestamp, payload))
                    })
                    .max_by_key(|(timestamp, _)| *timestamp)
                    .map(|(_, payload)| payload)
                    .ok_or_else(|| {
                        CrdtError::Internal(format!(
                            "content must exist for delete operation: {}",
                            op.genesis
                        ))
                    })?;

                self.dag.add_child_node(
                    last_payload,
                    op.parents.clone(),
                    op.genesis,
                    self.resolve_metadata_for_commit(&op.genesis, &op.parents)?,
                )?
            }
            OperationType::Merge(_) => {
                return Err(CrdtError::Internal(
                    "Merge operations must be committed via auto-merge".to_string(),
                ))
            }
        };

        self.state.apply(op)?;

        Ok(cid)
    }

    pub fn latest(&self, genesis_id: &Cid) -> Option<Cid> {
        self.dag.calculate_latest(genesis_id).ok().flatten()
    }
    /// Convenience wrapper around `DagGraph::get_genesis`
    pub fn get_genesis(&self, cid: &Cid) -> Result<Cid> {
        self.dag.get_genesis(cid).map_err(CrdtError::Graph)
    }

    pub fn get_operations_with_index(
        &self,
        genesis: &Cid,
    ) -> Result<Vec<(usize, Operation<Cid, Payload>)>> {
        let mut ops = self.state.get_operations_by_genesis(genesis)?;
        ops.sort_by_key(|op| op.timestamp);
        Ok(ops
            .into_iter()
            .enumerate()
            .map(|(idx, op)| (idx + 1, op))
            .collect())
    }

    /// Get the latest parent nodes for the given genesis
    fn validate_parent_genesis(&self, genesis: &Cid, parents: &[Cid]) -> Result<()> {
        for parent in parents {
            let parent_genesis = self.dag.get_genesis(parent).map_err(CrdtError::Graph)?;
            if &parent_genesis != genesis {
                return Err(CrdtError::Internal(format!(
                    "Parent {parent} does not belong to genesis {genesis}"
                )));
            }
        }
        Ok(())
    }

    fn check_and_merge(&mut self, genesis: &Cid) -> Result<Option<Cid>> {
        let heads = self.find_heads(genesis)?;

        if heads.len() <= 1 {
            return Ok(None);
        }

        let genesis_node = self
            .dag
            .get_node(genesis)
            .map_err(CrdtError::Graph)?
            .ok_or_else(|| CrdtError::Internal(format!("Genesis not found: {genesis}")))?;
        let policy_type = genesis_node.metadata().policy_type();
        let policy = self.create_policy(policy_type)?;

        let merge_node =
            self.resolver
                .create_merge_node(&heads, &self.dag, *genesis, policy.as_ref())?;

        self.validate_parent_genesis(genesis, &heads)?;

        let merge_cid = self.dag.add_child_node(
            merge_node.payload().clone(),
            heads.clone(),
            *genesis,
            merge_node.metadata().clone(),
        )?;

        let mut merge_op = Operation::new(
            *genesis,
            OperationType::Merge(merge_node.payload().clone()),
            "auto-merge".to_string(),
        );
        merge_op.parents = heads;
        self.state.apply(merge_op)?;

        Ok(Some(merge_cid))
    }

    fn find_heads(&self, genesis: &Cid) -> Result<Vec<Cid>> {
        let nodes = self
            .dag
            .get_nodes_by_genesis(genesis)
            .map_err(CrdtError::Graph)?;
        if nodes.is_empty() {
            return Ok(vec![]);
        }

        let node_set: HashSet<Cid> = nodes.iter().copied().collect();
        let mut parents_within = HashSet::new();

        for cid in &nodes {
            if let Some(node) = self.dag.get_node(cid).map_err(CrdtError::Graph)? {
                for parent in node.parents() {
                    if node_set.contains(parent) {
                        parents_within.insert(*parent);
                    }
                }
            }
        }

        Ok(nodes
            .into_iter()
            .filter(|cid| !parents_within.contains(cid))
            .collect())
    }

    fn create_policy(&self, policy_type: &str) -> Result<Box<dyn MergePolicy<Payload>>> {
        match policy_type {
            "lww" => Ok(Box::new(LwwMergePolicy)),
            other => Err(CrdtError::Internal(format!("Unknown policy type: {other}"))),
        }
    }

    fn resolve_metadata_for_commit(
        &self,
        genesis: &Cid,
        parents: &[Cid],
    ) -> Result<ContentMetadata> {
        if let Some(parent) = parents.first() {
            let node = self
                .dag
                .get_node(parent)
                .map_err(CrdtError::Graph)?
                .ok_or_else(|| CrdtError::Internal(format!("Parent node not found: {parent}")))?;
            Ok(node.metadata().clone())
        } else {
            let genesis_node = self
                .dag
                .get_node(genesis)
                .map_err(CrdtError::Graph)?
                .ok_or_else(|| CrdtError::Internal(format!("Genesis not found: {genesis}")))?;
            Ok(genesis_node.metadata().clone())
        }
    }
    /// Return parent -> children adjacency for the specified genesis (DAG structure).
    pub fn branching_history(&self, genesis: &Cid) -> Result<HashMap<Cid, Vec<Cid>>> {
        let nodes = self
            .dag
            .get_nodes_by_genesis(genesis)
            .map_err(CrdtError::Graph)?;

        let mut adjacency: HashMap<Cid, HashSet<Cid>> = HashMap::new();
        for &cid in &nodes {
            if let Some(node) = self.dag.get_node(&cid).map_err(CrdtError::Graph)? {
                for parent in node.parents() {
                    adjacency.entry(*parent).or_default().insert(cid);
                }
                adjacency.entry(cid).or_default();
            }
        }

        Ok(adjacency
            .into_iter()
            .map(|(cid, set)| {
                let mut children: Vec<Cid> = set.into_iter().collect();
                children.sort();
                (cid, children)
            })
            .collect())
    }

    /// Find a linear path from genesis to the latest head.
    pub fn linear_history(&self, genesis: &Cid) -> Result<Vec<Cid>> {
        let adjacency = self.branching_history(genesis)?;
        let mut path = Vec::new();
        let mut current = *genesis;
        let mut visited = HashSet::new();

        while visited.insert(current) {
            path.push(current);
            let children = adjacency.get(&current).cloned().unwrap_or_default();

            if children.is_empty() {
                break;
            }

            let mut best: Option<(Cid, (bool, u64))> = None;
            for child in children {
                let info = self.node_characteristics(&child)?;
                if let Some((_, best_info)) = &best {
                    if info > *best_info {
                        best = Some((child, info));
                    }
                } else {
                    best = Some((child, info));
                }
            }

            let Some((next, _)) = best else {
                break;
            };

            current = next;
        }

        Ok(path)
    }

    fn node_characteristics(&self, cid: &Cid) -> Result<(bool, u64)> {
        let node = self
            .dag
            .get_node(cid)
            .map_err(CrdtError::Graph)?
            .ok_or_else(|| CrdtError::Internal(format!("Node not found: {cid}")))?;
        let is_merge = node.parents().len() > 1;
        Ok((is_merge, node.timestamp()))
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

    type TestRepo = Repo<
        LeveldbStorage<Cid, TestPayload>,
        LeveldbNodeStorage<TestPayload, ContentMetadata>,
        TestPayload,
    >;

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

    fn sleep_for_ordering() {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    #[test]
    fn test_create_operation() {
        let (mut repo, _) = setup_test_repo();
        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"test").unwrap(),
        );
        let payload = TestPayload("test content".to_string());
        let op = make_test_operation(initial_genesis, OperationType::Create(payload.clone()));

        let cid = repo.commit_operation(op).unwrap();

        assert!(repo.latest(&cid).is_some());
        assert_eq!(repo.latest(&cid).unwrap(), cid);
    }

    #[test]
    fn test_update_operation() {
        let (mut repo, _) = setup_test_repo();
        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"test").unwrap(),
        );
        let create_op = make_test_operation(
            initial_genesis,
            OperationType::Create(TestPayload("initial".to_string())),
        );
        let create_cid = repo.commit_operation(create_op).unwrap();

        let update_op = make_test_operation(
            create_cid,
            OperationType::Update(TestPayload("updated".to_string())),
        );
        sleep_for_ordering();
        let update_cid = repo.commit_operation(update_op).unwrap();

        assert!(repo.latest(&create_cid).is_some());
        assert_eq!(repo.latest(&create_cid).unwrap(), update_cid);
        assert_ne!(create_cid, update_cid);
    }

    #[test]
    fn test_update_with_explicit_parent_is_respected() {
        let (mut repo, _) = setup_test_repo();
        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"explicit-parent").unwrap(),
        );
        let create_op = make_test_operation(
            initial_genesis,
            OperationType::Create(TestPayload("root".to_string())),
        );
        let genesis = repo.commit_operation(create_op).unwrap();

        let update_auto = make_test_operation(
            genesis,
            OperationType::Update(TestPayload("child-1".to_string())),
        );
        sleep_for_ordering();
        let auto_cid = repo.commit_operation(update_auto).unwrap();

        let mut update_branch = make_test_operation(
            genesis,
            OperationType::Update(TestPayload("branch-from-genesis".to_string())),
        );
        update_branch.parents.push(genesis);
        sleep_for_ordering();
        let branch_cid = repo.commit_operation(update_branch).unwrap();

        let branch_node = repo
            .dag
            .get_node(&branch_cid)
            .unwrap()
            .expect("branch node");
        assert_eq!(branch_node.parents(), &[genesis]);

        let auto_node = repo.dag.get_node(&auto_cid).unwrap().expect("auto node");
        assert_eq!(auto_node.parents(), &[genesis]);
    }

    #[test]
    fn test_update_rejects_parent_from_other_genesis() {
        let (mut repo, _) = setup_test_repo();
        let seed_a = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"genesis-a").unwrap(),
        );
        let seed_b = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"genesis-b").unwrap(),
        );

        let genesis_a = repo
            .commit_operation(make_test_operation(
                seed_a,
                OperationType::Create(TestPayload("A".into())),
            ))
            .unwrap();
        let genesis_b = repo
            .commit_operation(make_test_operation(
                seed_b,
                OperationType::Create(TestPayload("B".into())),
            ))
            .unwrap();

        let mut bad_update =
            make_test_operation(genesis_a, OperationType::Update(TestPayload("bad".into())));
        bad_update.parents.push(genesis_b);

        let err = repo.commit_operation(bad_update).unwrap_err();
        match err {
            CrdtError::Internal(message) => {
                assert!(message.contains("does not belong"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_multiple_children_from_same_parent() {
        let (mut repo, _) = setup_test_repo();
        let seed = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"shared-parent").unwrap(),
        );

        let genesis = repo
            .commit_operation(make_test_operation(
                seed,
                OperationType::Create(TestPayload("root".into())),
            ))
            .unwrap();

        let mut child_a = make_test_operation(
            genesis,
            OperationType::Update(TestPayload("child-a".into())),
        );
        child_a.parents.push(genesis);
        let child_a_cid = repo.commit_operation(child_a).unwrap();

        let mut child_b = make_test_operation(
            genesis,
            OperationType::Update(TestPayload("child-b".into())),
        );
        child_b.parents.push(genesis);
        sleep_for_ordering();
        let child_b_cid = repo.commit_operation(child_b).unwrap();

        let node_a = repo.dag.get_node(&child_a_cid).unwrap().expect("child_a");
        assert_eq!(node_a.parents(), &[genesis]);

        let node_b = repo.dag.get_node(&child_b_cid).unwrap().expect("child_b");
        assert_eq!(node_b.parents(), &[genesis]);

        let heads = repo.find_heads(&genesis).unwrap();
        assert_eq!(heads.len(), 2);
        assert!(heads.contains(&child_a_cid));
        assert!(heads.contains(&child_b_cid));
    }

    #[test]
    fn test_delete_operation() {
        let (mut repo, _) = setup_test_repo();
        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"test").unwrap(),
        );
        let create_op = make_test_operation(
            initial_genesis,
            OperationType::Create(TestPayload("initial".to_string())),
        );
        let create_cid = repo.commit_operation(create_op).unwrap();

        let delete_op = make_test_operation(create_cid, OperationType::Delete);
        sleep_for_ordering();
        let delete_cid = repo.commit_operation(delete_op).unwrap();

        assert!(repo.latest(&create_cid).is_some());
        assert_eq!(repo.latest(&create_cid).unwrap(), delete_cid);
        assert_ne!(create_cid, delete_cid);
    }

    #[test]
    fn test_multiple_genesis_entries() {
        let (mut repo, _) = setup_test_repo();
        let genesis1 = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"test1").unwrap(),
        );
        let genesis2 = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"test2").unwrap(),
        );

        let create1_op = make_test_operation(
            genesis1,
            OperationType::Create(TestPayload("entry1".to_string())),
        );
        let create1_cid = repo.commit_operation(create1_op).unwrap();

        let create2_op = make_test_operation(
            genesis2,
            OperationType::Create(TestPayload("entry2".to_string())),
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
        let placeholder_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"update_shared").unwrap(),
        );

        // Series A
        let create_a = make_test_operation(
            placeholder_genesis,
            OperationType::Create(TestPayload("A1".into())),
        );
        let genesis_a = repo.commit_operation(create_a).unwrap();

        // Series B
        let create_b = make_test_operation(
            placeholder_genesis,
            OperationType::Create(TestPayload("B1".into())),
        );
        let genesis_b = repo.commit_operation(create_b).unwrap();

        // Update only series A
        let update_a =
            make_test_operation(genesis_a, OperationType::Update(TestPayload("A2".into())));
        sleep_for_ordering();
        let latest_a = repo.commit_operation(update_a).unwrap();

        assert_eq!(repo.latest(&genesis_a).unwrap(), latest_a);
        assert_eq!(repo.latest(&genesis_b).unwrap(), genesis_b);
    }

    /// Failing test: Delete on one series still uses the legacy lookup and may fetch the wrong payload.
    #[test]
    fn test_delete_mixes_series_due_to_legacy_lookup() {
        let (mut repo, _) = setup_test_repo();
        let placeholder_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"shared").unwrap(),
        );

        // User1: Create
        let create1 = make_test_operation(
            placeholder_genesis,
            OperationType::Create(TestPayload("u1".into())),
        );
        let cid1 = repo.commit_operation(create1).unwrap();

        // User2: parallel series
        let create2 = make_test_operation(
            placeholder_genesis,
            OperationType::Create(TestPayload("u2".into())),
        );
        let cid2 = repo.commit_operation(create2).unwrap();

        // User2 update in its own series
        let update2 = make_test_operation(
            cid2,
            OperationType::Update(TestPayload("u2_updated".into())),
        );
        sleep_for_ordering();
        repo.commit_operation(update2).unwrap();

        let del_op = make_test_operation(cid1, OperationType::Delete);
        sleep_for_ordering();
        repo.commit_operation(del_op).unwrap();

        assert_eq!(repo.state.get_state(&cid1), None);
        assert_eq!(
            repo.state.get_state(&cid2),
            Some(TestPayload("u2_updated".into()))
        );
    }

    #[test]
    fn test_manual_merge_operations_are_rejected() {
        let (mut repo, _) = setup_test_repo();
        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"merge").unwrap(),
        );
        let create = make_test_operation(
            initial_genesis,
            OperationType::Create(TestPayload("base".into())),
        );
        let genesis = repo.commit_operation(create).unwrap();

        let merge_op = make_test_operation(
            genesis,
            OperationType::Merge(TestPayload("should-fail".into())),
        );

        let err = repo.commit_operation(merge_op).unwrap_err();
        match err {
            CrdtError::Internal(message) => {
                assert!(message.contains("Merge operations cannot be manually committed"))
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_auto_merge_creates_merge_operation() {
        let (mut repo, _) = setup_test_repo();
        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"autoMerge").unwrap(),
        );
        let create = make_test_operation(
            initial_genesis,
            OperationType::Create(TestPayload("root".into())),
        );
        let genesis = repo.commit_operation(create).unwrap();

        // Create two explicit branches from the same genesis using commit_operation
        let mut branch1_op = make_test_operation(
            genesis,
            OperationType::Update(TestPayload("branch-1".into())),
        );
        branch1_op.parents.push(genesis);
        let branch1_cid = repo.commit_operation(branch1_op).unwrap();
        sleep_for_ordering();
        let mut branch2_op = make_test_operation(
            genesis,
            OperationType::Update(TestPayload("branch-2".into())),
        );
        branch2_op.parents.push(genesis);
        let branch2_cid = repo.commit_operation(branch2_op).unwrap();
        sleep_for_ordering();

        // Committing a regular update should trigger auto-merge
        let update =
            make_test_operation(genesis, OperationType::Update(TestPayload("latest".into())));
        repo.commit_operation(update).unwrap();

        let ops = repo.state.get_operations_by_genesis(&genesis).unwrap();
        assert!(ops
            .iter()
            .any(|op| matches!(op.kind, OperationType::Merge(_))));

        // After auto-merge, the content should converge to a single head
        let heads_after_merge = repo.find_heads(&genesis).unwrap();
        assert_eq!(heads_after_merge.len(), 1);
        assert!(!heads_after_merge.contains(&branch1_cid));
        assert!(!heads_after_merge.contains(&branch2_cid));
    }

    #[test]
    fn test_auto_merge_from_intermediate_branch() {
        let (mut repo, _) = setup_test_repo();
        let seed = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"intermediate-merge").unwrap(),
        );

        // genesis a
        let genesis = repo
            .commit_operation(make_test_operation(
                seed,
                OperationType::Create(TestPayload("a".into())),
            ))
            .unwrap();

        // main chain: a -> b -> d -> e -> f
        let mut last_main = genesis;
        let mut d_cid = genesis;
        for label in ["b", "d", "e", "f"] {
            let mut op =
                make_test_operation(genesis, OperationType::Update(TestPayload((*label).into())));
            op.parents.push(last_main);
            sleep_for_ordering();
            let cid = repo.commit_operation(op).unwrap();
            if label == "d" {
                d_cid = cid;
            }
            last_main = cid;
        }
        let f_cid = last_main;

        // branch from d: g -> h
        let mut g_op = make_test_operation(genesis, OperationType::Update(TestPayload("g".into())));
        g_op.parents.push(d_cid);
        sleep_for_ordering();
        let g_cid = repo.commit_operation(g_op).unwrap();

        let mut h_op = make_test_operation(genesis, OperationType::Update(TestPayload("h".into())));
        h_op.parents.push(g_cid);
        sleep_for_ordering();
        let h_cid = repo.commit_operation(h_op).unwrap();

        // auto-merge will trigger when committing a new update without explicit parents
        sleep_for_ordering();

        let latest_op =
            make_test_operation(genesis, OperationType::Update(TestPayload("latest".into())));
        let latest_cid = repo.commit_operation(latest_op).unwrap();

        let heads = repo.find_heads(&genesis).unwrap();
        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0], latest_cid);

        let latest_node = repo
            .dag
            .get_node(&latest_cid)
            .unwrap()
            .expect("latest node");
        let latest_parents = latest_node.parents();
        assert_eq!(latest_parents.len(), 1);
        let merge_cid = latest_parents[0];

        let merge_node = repo.dag.get_node(&merge_cid).unwrap().expect("merge node");
        let merge_parents = merge_node.parents();
        assert_eq!(merge_parents.len(), 2);
        assert!(merge_parents.contains(&f_cid));
        assert!(merge_parents.contains(&h_cid));
    }

    #[test]
    fn test_branching_history_returns_adjacency() {
        let (mut repo, _) = setup_test_repo();
        let genesis_seed = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"branching").unwrap(),
        );
        let create = make_test_operation(
            genesis_seed,
            OperationType::Create(TestPayload("root".into())),
        );
        let genesis = repo.commit_operation(create).unwrap();

        let branch_a_payload = TestPayload("branch-a".into());
        let branch_a = repo
            .dag
            .add_child_node(
                branch_a_payload.clone(),
                vec![genesis],
                genesis,
                ContentMetadata::default(),
            )
            .unwrap();
        repo.state
            .apply(Operation::new(
                genesis,
                OperationType::Update(branch_a_payload),
                "manual".into(),
            ))
            .unwrap();

        let branch_b_payload = TestPayload("branch-b".into());
        let branch_b = repo
            .dag
            .add_child_node(
                branch_b_payload.clone(),
                vec![genesis],
                genesis,
                ContentMetadata::default(),
            )
            .unwrap();
        repo.state
            .apply(Operation::new(
                genesis,
                OperationType::Update(branch_b_payload),
                "manual".into(),
            ))
            .unwrap();

        let adjacency = repo.branching_history(&genesis).unwrap();
        let children = adjacency.get(&genesis).cloned().unwrap_or_default();

        assert!(children.contains(&branch_a));
        assert!(children.contains(&branch_b));
        assert!(adjacency.contains_key(&branch_a));
        assert!(adjacency.contains_key(&branch_b));
    }

    #[test]
    fn test_linear_history_prefers_merge_path() {
        let (mut repo, _) = setup_test_repo();
        let genesis_seed = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"linear").unwrap(),
        );
        let create = make_test_operation(
            genesis_seed,
            OperationType::Create(TestPayload("root".into())),
        );
        let genesis = repo.commit_operation(create).unwrap();

        let branch_a_payload = TestPayload("A".into());
        let branch_a = repo
            .dag
            .add_child_node(
                branch_a_payload.clone(),
                vec![genesis],
                genesis,
                ContentMetadata::default(),
            )
            .unwrap();
        repo.state
            .apply(Operation::new(
                genesis,
                OperationType::Update(branch_a_payload),
                "manual".into(),
            ))
            .unwrap();

        sleep_for_ordering();

        let branch_b_payload = TestPayload("B".into());
        let branch_b = repo
            .dag
            .add_child_node(
                branch_b_payload.clone(),
                vec![genesis],
                genesis,
                ContentMetadata::default(),
            )
            .unwrap();
        repo.state
            .apply(Operation::new(
                genesis,
                OperationType::Update(branch_b_payload),
                "manual".into(),
            ))
            .unwrap();

        let merge_payload = TestPayload("merged".into());
        let merge_cid = repo
            .dag
            .add_child_node(
                merge_payload.clone(),
                vec![branch_a, branch_b],
                genesis,
                ContentMetadata::default(),
            )
            .unwrap();
        repo.state
            .apply(Operation::new(
                genesis,
                OperationType::Merge(merge_payload.clone()),
                "auto-merge".into(),
            ))
            .unwrap();

        sleep_for_ordering();

        let latest_payload = TestPayload("latest".into());
        let latest_cid = repo
            .dag
            .add_child_node(
                latest_payload.clone(),
                vec![merge_cid],
                genesis,
                ContentMetadata::default(),
            )
            .unwrap();
        repo.state
            .apply(Operation::new(
                genesis,
                OperationType::Update(latest_payload),
                "manual".into(),
            ))
            .unwrap();

        let path = repo.linear_history(&genesis).unwrap();
        assert_eq!(path.last(), Some(&latest_cid));
        assert!(path.contains(&merge_cid));
        assert!(path.iter().any(|cid| cid == &branch_a || cid == &branch_b));
        if let (Some(branch_pos), Some(merge_pos)) = (
            path.iter()
                .position(|cid| cid == &branch_a || cid == &branch_b),
            path.iter().position(|cid| cid == &merge_cid),
        ) {
            assert!(branch_pos < merge_pos);
        } else {
            panic!("branch or merge node missing from linear history");
        }
    }
}
