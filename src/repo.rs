use crate::convergence::{
    metadata::ContentMetadata, policies::lww::LwwMergePolicy, policy::MergePolicy,
    resolver::ConflictResolver,
};
use crate::crdt::error::{CrdtError, Result};
use crate::crdt::timestamp::next_monotonic_timestamp;
use crate::storage::{BatchError, LeveldbBatchGuard, SharedLeveldb, SharedLeveldbAccess};
use crate::{
    crdt::{
        crdt_state::CrdtState,
        operation::{Operation, OperationType},
        reducer::LwwReducer,
        storage::OperationStorage,
    },
    dasl::node::Node,
    graph::{dag::DagGraph, storage::NodeStorage},
};
use cid::Cid;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

struct PendingNode {
    cid: Cid,
    parents: Vec<Cid>,
    metadata: ContentMetadata,
}

pub struct Repo<OpStore, NodeStore, Payload>
where
    OpStore: OperationStorage<Cid, Payload> + SharedLeveldbAccess,
    NodeStore: NodeStorage<Payload, ContentMetadata> + SharedLeveldbAccess,
    Payload: Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    pub state: CrdtState<Cid, Payload, OpStore, LwwReducer>,
    pub dag: DagGraph<NodeStore, Payload, ContentMetadata>,
    resolver: ConflictResolver<Payload, ContentMetadata>,
}

impl<OpStore, NodeStore, Payload> Repo<OpStore, NodeStore, Payload>
where
    OpStore: OperationStorage<Cid, Payload> + SharedLeveldbAccess,
    NodeStore: NodeStorage<Payload, ContentMetadata> + SharedLeveldbAccess,
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

    /// Commits an operation to the repository.
    ///
    /// If `op.node_timestamp` is set, the operation is treated as an import from
    /// another replica, preserving the original timestamp for CID consistency.
    /// Otherwise, the current time is used for the DAG node timestamp.
    ///
    /// # Arguments
    ///
    /// * `op` - The operation to commit
    ///
    /// # Returns
    ///
    /// The CID of the committed node
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Merge operations are attempted to be committed manually (without node_timestamp)
    /// - The operation cannot be applied
    /// - There are consistency issues with the DAG structure
    pub fn commit_operation(&mut self, op: Operation<Cid, Payload>) -> Result<Cid> {
        // Merge operations can only be committed via import (with node_timestamp) or auto-merge
        if matches!(op.kind, OperationType::Merge(_)) && op.node_timestamp.is_none() {
            return Err(CrdtError::Internal(
                "Merge operations cannot be manually committed".to_string(),
            ));
        }

        self.commit_operation_internal(op, false)
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

    fn shared_leveldb(&self) -> Result<Arc<SharedLeveldb>> {
        let op_db = self.state.storage().shared_leveldb().ok_or_else(|| {
            CrdtError::Internal("operation storage does not support batching".into())
        })?;
        let node_db =
            self.dag.storage.shared_leveldb().ok_or_else(|| {
                CrdtError::Internal("node storage does not support batching".into())
            })?;

        if !Arc::ptr_eq(&op_db, &node_db) {
            return Err(CrdtError::Internal(
                "operation and node storage must share the same LevelDB instance for transactions"
                    .into(),
            ));
        }

        Ok(op_db)
    }

    fn commit_operation_internal(
        &mut self,
        op: Operation<Cid, Payload>,
        skip_auto_merge: bool,
    ) -> Result<Cid> {
        let mut op = op;
        let shared = self.shared_leveldb()?;
        let batch_guard = Self::begin_shared_batch(&shared)?;
        let mut pending_nodes: Vec<PendingNode> = Vec::new();

        // If node_timestamp is not set, run auto-merge logic
        if !skip_auto_merge && op.node_timestamp.is_none() {
            self.ensure_parent_context(&mut op, &mut pending_nodes)?;
        }

        // Use specified timestamp or generate a new one
        let timestamp = op.node_timestamp.unwrap_or_else(next_monotonic_timestamp);

        let cid = match op.kind.clone() {
            OperationType::Create(payload) => {
                self.stage_create(payload, &mut op, timestamp, &mut pending_nodes)?
            }
            OperationType::Update(payload) => {
                self.stage_update(payload, &op, timestamp, &mut pending_nodes)?
            }
            OperationType::Delete => self.stage_delete(&op, timestamp, &mut pending_nodes)?,
            OperationType::Merge(payload) => {
                if op.node_timestamp.is_none() {
                    return Err(CrdtError::Internal(
                        "Merge operations must be committed via auto-merge".to_string(),
                    ));
                }
                self.stage_merge(payload, &op, timestamp, &mut pending_nodes)?
            }
        };

        if let Err(err) = self.state.apply(op) {
            self.rollback_pending_nodes(&pending_nodes);
            return Err(err);
        }

        if let Err(status) = batch_guard.commit() {
            self.rollback_pending_nodes(&pending_nodes);
            return Err(CrdtError::Storage(status));
        }

        Ok(cid)
    }

    fn begin_shared_batch(shared: &SharedLeveldb) -> Result<LeveldbBatchGuard<'_>> {
        shared.begin_batch().map_err(|err| match err {
            BatchError::Unsupported => CrdtError::Internal(
                "current storage backend does not support transactions".to_string(),
            ),
            BatchError::AlreadyActive => CrdtError::Internal(
                "a transaction is already active on the shared LevelDB".to_string(),
            ),
            BatchError::Commit(status) => CrdtError::Storage(status),
            BatchError::LockPoisoned => {
                CrdtError::Internal("shared LevelDB lock was poisoned".to_string())
            }
        })
    }

    fn rollback_pending_nodes(&mut self, pending: &[PendingNode]) {
        for node in pending.iter().rev() {
            self.dag.rollback_pending_node(&node.cid, &node.parents);
        }
    }

    fn ensure_parent_context(
        &mut self,
        op: &mut Operation<Cid, Payload>,
        pending_nodes: &mut Vec<PendingNode>,
    ) -> Result<()> {
        match &op.kind {
            OperationType::Update(_) | OperationType::Delete => {
                if op.parents.is_empty() {
                    let merged_head = self
                        .check_and_merge(&op.genesis, pending_nodes)?
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
        Ok(())
    }

    /// Stages a Create operation.
    ///
    /// If `op.node_timestamp` is set (import), verifies CID matches op.genesis.
    /// Otherwise, sets op.genesis to the computed CID.
    fn stage_create(
        &mut self,
        payload: Payload,
        op: &mut Operation<Cid, Payload>,
        timestamp: u64,
        pending_nodes: &mut Vec<PendingNode>,
    ) -> Result<Cid> {
        let (genesis_cid, node) =
            self.dag
                .prepare_genesis_node(payload, timestamp, ContentMetadata::default())?;

        if op.node_timestamp.is_some() {
            // Import: verify that the computed CID matches the expected genesis
            if genesis_cid != op.genesis {
                return Err(CrdtError::Internal(format!(
                    "CID mismatch during import: expected {}, got {}",
                    op.genesis, genesis_cid
                )));
            }
        } else {
            // Local create: set genesis to the computed CID
            op.genesis = genesis_cid;
        }

        self.stage_prepared_node(genesis_cid, node, pending_nodes)
    }

    /// Stages an Update operation.
    fn stage_update(
        &mut self,
        payload: Payload,
        op: &Operation<Cid, Payload>,
        timestamp: u64,
        pending_nodes: &mut Vec<PendingNode>,
    ) -> Result<Cid> {
        let lenient = op.node_timestamp.is_some();
        let metadata =
            self.resolve_metadata(&op.genesis, &op.parents, pending_nodes.as_slice(), lenient)?;
        let (cid, node) = self.dag.prepare_child_node(
            payload,
            op.parents.clone(),
            op.genesis,
            timestamp,
            metadata,
        )?;
        self.stage_prepared_node(cid, node, pending_nodes)
    }

    /// Stages a Delete operation.
    fn stage_delete(
        &mut self,
        op: &Operation<Cid, Payload>,
        timestamp: u64,
        pending_nodes: &mut Vec<PendingNode>,
    ) -> Result<Cid> {
        let ops = self.state.get_operations_by_genesis(&op.genesis)?;
        let last_payload = ops
            .iter()
            .filter_map(|operation| {
                operation
                    .payload()
                    .cloned()
                    .map(|payload| (operation.timestamp, payload))
            })
            .max_by_key(|(ts, _)| *ts)
            .map(|(_, payload)| payload)
            .ok_or_else(|| {
                CrdtError::Internal(format!(
                    "content must exist for delete operation: {}",
                    op.genesis
                ))
            })?;

        let lenient = op.node_timestamp.is_some();
        let metadata =
            self.resolve_metadata(&op.genesis, &op.parents, pending_nodes.as_slice(), lenient)?;
        let (cid, node) = self.dag.prepare_child_node(
            last_payload,
            op.parents.clone(),
            op.genesis,
            timestamp,
            metadata,
        )?;
        self.stage_prepared_node(cid, node, pending_nodes)
    }

    /// Stages a Merge operation (only for imports).
    fn stage_merge(
        &mut self,
        payload: Payload,
        op: &Operation<Cid, Payload>,
        timestamp: u64,
        pending_nodes: &mut Vec<PendingNode>,
    ) -> Result<Cid> {
        // Merge operations are always imports, so use lenient metadata resolution
        let metadata =
            self.resolve_metadata(&op.genesis, &op.parents, pending_nodes.as_slice(), true)?;
        let (cid, node) = self.dag.prepare_child_node(
            payload,
            op.parents.clone(),
            op.genesis,
            timestamp,
            metadata,
        )?;
        self.stage_prepared_node(cid, node, pending_nodes)
    }

    fn stage_prepared_node(
        &mut self,
        cid: Cid,
        node: Node<Payload, ContentMetadata>,
        pending_nodes: &mut Vec<PendingNode>,
    ) -> Result<Cid> {
        let pending = self.persist_prepared_node(cid, &node)?;
        pending_nodes.push(pending);
        Ok(cid)
    }

    fn persist_prepared_node(
        &mut self,
        cid: Cid,
        node: &Node<Payload, ContentMetadata>,
    ) -> Result<PendingNode> {
        self.dag.storage.put(node).map_err(CrdtError::Graph)?;
        self.dag
            .register_prepared_node(cid, node)
            .map_err(CrdtError::Graph)?;
        Ok(PendingNode {
            cid,
            parents: node.parents().to_vec(),
            metadata: node.metadata().clone(),
        })
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

    fn check_and_merge(
        &mut self,
        genesis: &Cid,
        pending_nodes: &mut Vec<PendingNode>,
    ) -> Result<Option<Cid>> {
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

        self.validate_parent_genesis(genesis, &heads)?;

        let merge_timestamp = next_monotonic_timestamp();
        let merge_node = self.resolver.create_merge_node(
            &heads,
            &self.dag,
            *genesis,
            merge_timestamp,
            policy.as_ref(),
        )?;

        let (merge_cid, node) = self
            .dag
            .prepare_child_node(
                merge_node.payload().clone(),
                heads.clone(),
                *genesis,
                merge_timestamp,
                merge_node.metadata().clone(),
            )
            .map_err(CrdtError::Graph)?;
        let pending = self.persist_prepared_node(merge_cid, &node)?;

        let mut merge_op = Operation::new(
            *genesis,
            OperationType::Merge(merge_node.payload().clone()),
            "auto-merge".to_string(),
        );
        merge_op.parents = heads;
        if let Err(err) = self.state.apply(merge_op) {
            self.dag
                .rollback_pending_node(&pending.cid, &pending.parents);
            return Err(err);
        }

        pending_nodes.push(pending);

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

    /// Resolves metadata for an operation.
    ///
    /// # Arguments
    /// * `genesis` - The genesis CID
    /// * `parents` - The parent CIDs
    /// * `pending_nodes` - Pending nodes that haven't been committed yet
    /// * `lenient` - If true, returns default metadata when nodes not found (for imports)
    fn resolve_metadata(
        &self,
        genesis: &Cid,
        parents: &[Cid],
        pending_nodes: &[PendingNode],
        lenient: bool,
    ) -> Result<ContentMetadata> {
        // Try to get metadata from parents first
        if let Some(parent) = parents.first() {
            if let Some(pending) = pending_nodes.iter().find(|pending| &pending.cid == parent) {
                return Ok(pending.metadata.clone());
            }
            match self.dag.get_node(parent) {
                Ok(Some(node)) => return Ok(node.metadata().clone()),
                Ok(None) if !lenient => {
                    return Err(CrdtError::Internal(format!(
                        "Parent node not found: {parent}"
                    )))
                }
                Err(e) if !lenient => return Err(CrdtError::Graph(e)),
                _ => {} // lenient mode: continue to try genesis
            }
        }

        // Try to get metadata from genesis
        if let Some(pending) = pending_nodes.iter().find(|pending| &pending.cid == genesis) {
            return Ok(pending.metadata.clone());
        }
        match self.dag.get_node(genesis) {
            Ok(Some(genesis_node)) => Ok(genesis_node.metadata().clone()),
            Ok(None) if lenient => Ok(ContentMetadata::default()),
            Ok(None) => Err(CrdtError::Internal(format!("Genesis not found: {genesis}"))),
            Err(_) if lenient => {
                // In lenient mode, return default metadata on error
                Ok(ContentMetadata::default())
            }
            Err(e) => Err(CrdtError::Graph(e)),
        }
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
    use crate::graph::error::GraphError;
    use crate::graph::storage::LeveldbNodeStorage;
    use rusty_leveldb::{Status, StatusCode};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;
    use ulid::Ulid;

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
        let shared = SharedLeveldb::open(dir.path().join("store")).unwrap();
        let op_storage = LeveldbStorage::new(shared.clone());
        let node_storage = LeveldbNodeStorage::new(shared);
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

    struct FailingOperationStorage<S> {
        inner: S,
        fail_next: AtomicBool,
    }

    impl<S> FailingOperationStorage<S> {
        fn new(inner: S) -> Self {
            Self {
                inner,
                fail_next: AtomicBool::new(false),
            }
        }

        fn fail_on_first(inner: S) -> Self {
            Self {
                inner,
                fail_next: AtomicBool::new(true),
            }
        }

        fn fail_on_next(&self) {
            self.fail_next.store(true, Ordering::SeqCst);
        }
    }

    impl<S, ContentId, T> OperationStorage<ContentId, T> for FailingOperationStorage<S>
    where
        S: OperationStorage<ContentId, T>,
        ContentId: Send + Sync,
        T: Send + Sync,
    {
        fn save_operation(&self, op: &Operation<ContentId, T>) -> crate::crdt::error::Result<()> {
            if self.fail_next.swap(false, Ordering::SeqCst) {
                Err(CrdtError::Internal(
                    "forced failure for testing".to_string(),
                ))
            } else {
                self.inner.save_operation(op)
            }
        }

        fn load_operations(
            &self,
            genesis: &ContentId,
        ) -> crate::crdt::error::Result<Vec<Operation<ContentId, T>>> {
            self.inner.load_operations(genesis)
        }

        fn get_operation(
            &self,
            op_id: &Ulid,
        ) -> crate::crdt::error::Result<Option<Operation<ContentId, T>>> {
            self.inner.get_operation(op_id)
        }

        fn delete_operation(&self, op_id: &Ulid) -> crate::crdt::error::Result<()> {
            self.inner.delete_operation(op_id)
        }
    }

    impl<S> SharedLeveldbAccess for FailingOperationStorage<S>
    where
        S: SharedLeveldbAccess,
    {
        fn shared_leveldb(&self) -> Option<Arc<SharedLeveldb>> {
            self.inner.shared_leveldb()
        }
    }

    struct FailingNodeStorage<S> {
        inner: S,
        fail_next_put: AtomicBool,
    }

    impl<S> FailingNodeStorage<S> {
        fn fail_on_first_put(inner: S) -> Self {
            Self {
                inner,
                fail_next_put: AtomicBool::new(true),
            }
        }
    }

    impl<S, P, M> NodeStorage<P, M> for FailingNodeStorage<S>
    where
        S: NodeStorage<P, M>,
        P: Send + Sync,
        M: Send + Sync,
    {
        fn get(&self, content_id: &Cid) -> crate::graph::error::Result<Option<Node<P, M>>> {
            self.inner.get(content_id)
        }

        fn put(&self, node: &Node<P, M>) -> crate::graph::error::Result<()> {
            if self.fail_next_put.swap(false, Ordering::SeqCst) {
                Err(GraphError::Internal(
                    "injected node storage failure".to_string(),
                ))
            } else {
                self.inner.put(node)
            }
        }

        fn delete(&self, content_id: &Cid) -> crate::graph::error::Result<()> {
            self.inner.delete(content_id)
        }

        fn get_node_map(&self) -> crate::graph::error::Result<HashMap<Cid, Vec<Cid>>> {
            self.inner.get_node_map()
        }
    }

    impl<S> SharedLeveldbAccess for FailingNodeStorage<S>
    where
        S: SharedLeveldbAccess,
    {
        fn shared_leveldb(&self) -> Option<Arc<SharedLeveldb>> {
            self.inner.shared_leveldb()
        }
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
    fn test_create_operation_fails_when_node_storage_errors() {
        let dir = tempdir().unwrap();
        let shared = SharedLeveldb::open(dir.path().join("store")).unwrap();
        let op_storage = LeveldbStorage::new(shared.clone());
        let node_storage =
            FailingNodeStorage::fail_on_first_put(LeveldbNodeStorage::new(shared.clone()));
        let state = CrdtState::new(op_storage);
        let dag = DagGraph::new(node_storage);
        let mut repo = Repo::new(state, dag);

        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"create-fail").unwrap(),
        );
        let op = make_test_operation(
            initial_genesis,
            OperationType::Create(TestPayload("should fail".to_string())),
        );
        let op_id = op.id;

        let err = repo.commit_operation(op).unwrap_err();
        match err {
            CrdtError::Graph(GraphError::Internal(message)) => {
                assert!(message.contains("injected node storage failure"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        assert!(
            repo.state.get_operation(&op_id).unwrap().is_none(),
            "operation should not be persisted on failure"
        );
        assert!(
            repo.dag.storage.get_node_map().unwrap().is_empty(),
            "dag should remain empty when node storage fails"
        );
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
    fn test_update_operation_without_existing_head_fails() {
        let (mut repo, _) = setup_test_repo();
        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"update-no-head").unwrap(),
        );
        let op = make_test_operation(
            initial_genesis,
            OperationType::Update(TestPayload("orphaned".to_string())),
        );

        let err = repo.commit_operation(op).unwrap_err();
        match err {
            CrdtError::Internal(message) => {
                assert!(message.contains("No head available"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let stored_ops = repo
            .state
            .get_operations_by_genesis(&initial_genesis)
            .unwrap();
        assert!(
            stored_ops.is_empty(),
            "update should not persist when no head exists"
        );
    }

    #[test]
    fn test_create_operation_rolls_back_on_state_failure() {
        let dir = tempdir().unwrap();
        let shared = SharedLeveldb::open(dir.path().join("store")).unwrap();
        let op_storage =
            FailingOperationStorage::fail_on_first(LeveldbStorage::new(shared.clone()));
        let node_storage = LeveldbNodeStorage::new(shared);
        let state = CrdtState::new(op_storage);
        let dag = DagGraph::new(node_storage);
        let mut repo = Repo::new(state, dag);

        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"rollback-test").unwrap(),
        );
        let op = make_test_operation(
            initial_genesis,
            OperationType::Create(TestPayload("should not persist".to_string())),
        );
        let op_id = op.id;

        let result = repo.commit_operation(op);
        assert!(result.is_err());

        let node_map = repo.dag.storage.get_node_map().unwrap();
        assert!(
            node_map.is_empty(),
            "expected DAG to be empty after rollback, found {node_map:?}"
        );
        assert!(
            repo.state.get_operation(&op_id).unwrap().is_none(),
            "operation was persisted despite failure"
        );
    }

    #[test]
    fn test_create_operation_rolls_back_when_batch_commit_fails() {
        let (mut repo, _) = setup_test_repo();
        let shared = repo
            .state
            .storage()
            .shared_leveldb()
            .expect("shared leveldb instance");
        shared.inject_commit_failure(Status::new(StatusCode::IOError, "forced commit failure"));

        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"batch-failure").unwrap(),
        );
        let op = make_test_operation(
            initial_genesis,
            OperationType::Create(TestPayload("batch-fail".to_string())),
        );
        let op_id = op.id;

        let err = repo.commit_operation(op).unwrap_err();
        match err {
            CrdtError::Storage(status) => {
                assert_eq!(status.code, StatusCode::IOError);
                assert!(status.err.contains("forced commit failure"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        assert!(
            repo.state.get_operation(&op_id).unwrap().is_none(),
            "operation should not persist when batch commit fails"
        );
        assert!(
            repo.dag.storage.get_node_map().unwrap().is_empty(),
            "dag should be rolled back when batch commit fails"
        );
    }

    #[test]
    fn test_rollback_pending_nodes_restores_heads_after_failure() {
        let dir = tempdir().unwrap();
        let shared = SharedLeveldb::open(dir.path().join("store")).unwrap();
        let op_storage = FailingOperationStorage::new(LeveldbStorage::new(shared.clone()));
        let node_storage = LeveldbNodeStorage::new(shared);
        let state = CrdtState::new(op_storage);
        let dag = DagGraph::new(node_storage);
        let mut repo = Repo::new(state, dag);

        let seed = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"rollback-pending").unwrap(),
        );
        let create = make_test_operation(seed, OperationType::Create(TestPayload("root".into())));
        let genesis = repo.commit_operation(create).unwrap();

        let mut branch1 = make_test_operation(
            genesis,
            OperationType::Update(TestPayload("branch-1".into())),
        );
        branch1.parents.push(genesis);
        let branch1_cid = repo.commit_operation(branch1).unwrap();
        sleep_for_ordering();

        let mut branch2 = make_test_operation(
            genesis,
            OperationType::Update(TestPayload("branch-2".into())),
        );
        branch2.parents.push(genesis);
        let branch2_cid = repo.commit_operation(branch2).unwrap();

        let original_heads = repo.find_heads(&genesis).unwrap();
        assert_eq!(original_heads.len(), 2);
        assert!(original_heads.contains(&branch1_cid));
        assert!(original_heads.contains(&branch2_cid));

        repo.state.storage().fail_on_next();

        let update = make_test_operation(
            genesis,
            OperationType::Update(TestPayload("should-rollback".into())),
        );
        let err = repo.commit_operation(update).unwrap_err();
        match err {
            CrdtError::Internal(message) => {
                assert!(message.contains("forced failure for testing"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let heads_after = repo.find_heads(&genesis).unwrap();
        assert_eq!(heads_after.len(), 2);
        assert!(heads_after.contains(&branch1_cid));
        assert!(heads_after.contains(&branch2_cid));

        let ops = repo.state.get_operations_by_genesis(&genesis).unwrap();
        assert_eq!(
            ops.len(),
            3,
            "rollback should leave only the original create and two branch updates"
        );

        let node_map = repo.dag.storage.get_node_map().unwrap();
        assert!(node_map.contains_key(&genesis));
        assert!(node_map.contains_key(&branch1_cid));
        assert!(node_map.contains_key(&branch2_cid));
        assert_eq!(
            node_map.len(),
            3,
            "no additional DAG nodes should remain after rollback"
        );
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
    fn test_delete_operation_without_existing_payload_fails() {
        let (mut repo, _) = setup_test_repo();
        let (genesis_cid, genesis_node) = repo
            .dag
            .prepare_genesis_node(
                TestPayload("dangling".to_string()),
                1000,
                ContentMetadata::default(),
            )
            .unwrap();
        repo.dag.storage.put(&genesis_node).unwrap();
        repo.dag
            .register_prepared_node(genesis_cid, &genesis_node)
            .unwrap();

        let op = make_test_operation(genesis_cid, OperationType::Delete);
        let op_id = op.id;

        let err = repo.commit_operation(op).unwrap_err();
        match err {
            CrdtError::Internal(message) => {
                assert!(message.contains("content must exist"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        assert!(
            repo.state.get_operation(&op_id).unwrap().is_none(),
            "delete operation should not be stored when payload is missing"
        );
        assert!(
            repo.state
                .get_operations_by_genesis(&genesis_cid)
                .unwrap()
                .is_empty(),
            "operation history should remain empty on failure"
        );
        assert!(
            repo.dag.get_node(&genesis_cid).unwrap().is_some(),
            "existing genesis node should remain after failed delete"
        );
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
                2000,
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
                3000,
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
                2000,
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
                3000,
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
                4000,
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
                5000,
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

    #[test]
    fn test_import_operation_preserves_cid() {
        let (mut repo1, _dir1) = setup_test_repo();
        let (mut repo2, _dir2) = setup_test_repo();

        // Create content in repo1
        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"import-test").unwrap(),
        );
        let payload = TestPayload("test content".to_string());
        let op = make_test_operation(initial_genesis, OperationType::Create(payload.clone()));

        let cid1 = repo1.commit_operation(op.clone()).unwrap();

        // Get the node timestamp from repo1
        let node = repo1.dag.get_node(&cid1).unwrap().unwrap();
        let node_timestamp = node.timestamp();

        // Create the operation with the correct genesis CID and node_timestamp for import
        let mut import_op = make_test_operation(cid1, OperationType::Create(payload));
        import_op.genesis = cid1;
        import_op.node_timestamp = Some(node_timestamp);

        // Import the operation into repo2
        let cid2 = repo2.commit_operation(import_op).unwrap();

        // CIDs should match
        assert_eq!(cid1, cid2, "CIDs should be identical after import");

        // Verify the content can be retrieved using the original CID
        assert!(
            repo2.latest(&cid1).is_some(),
            "Should be able to get latest using original CID"
        );
        assert_eq!(repo2.latest(&cid1).unwrap(), cid1);
    }

    #[test]
    fn test_import_operation_update_preserves_cid() {
        let (mut repo1, _dir1) = setup_test_repo();
        let (mut repo2, _dir2) = setup_test_repo();

        // Create initial content in repo1
        let initial_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"import-update-test").unwrap(),
        );
        let create_payload = TestPayload("initial".to_string());
        let create_op = make_test_operation(
            initial_genesis,
            OperationType::Create(create_payload.clone()),
        );
        let genesis_cid = repo1.commit_operation(create_op.clone()).unwrap();

        // Get genesis node timestamp
        let genesis_node = repo1.dag.get_node(&genesis_cid).unwrap().unwrap();
        let genesis_timestamp = genesis_node.timestamp();

        // Import genesis into repo2
        let mut import_create_op =
            make_test_operation(genesis_cid, OperationType::Create(create_payload));
        import_create_op.genesis = genesis_cid;
        import_create_op.node_timestamp = Some(genesis_timestamp);
        let imported_genesis = repo2.commit_operation(import_create_op).unwrap();
        assert_eq!(genesis_cid, imported_genesis);

        // Create update in repo1
        sleep_for_ordering();
        let update_payload = TestPayload("updated".to_string());
        let update_op =
            make_test_operation(genesis_cid, OperationType::Update(update_payload.clone()));
        let update_cid = repo1.commit_operation(update_op).unwrap();

        // Get update node info from repo1
        let update_node = repo1.dag.get_node(&update_cid).unwrap().unwrap();
        let update_timestamp = update_node.timestamp();
        let update_parents = update_node.parents().clone();

        // Import update into repo2
        let mut import_update_op =
            make_test_operation(genesis_cid, OperationType::Update(update_payload));
        import_update_op.parents = update_parents;
        import_update_op.node_timestamp = Some(update_timestamp);
        let imported_update = repo2.commit_operation(import_update_op).unwrap();

        // CIDs should match
        assert_eq!(
            update_cid, imported_update,
            "Update CIDs should be identical after import"
        );

        // Verify latest points to the update
        assert_eq!(repo2.latest(&genesis_cid).unwrap(), update_cid);
    }

    #[test]
    fn test_import_operation_rejects_cid_mismatch() {
        let (mut repo, _dir) = setup_test_repo();

        // Create an operation with a genesis CID that won't match the computed CID
        let wrong_genesis = Cid::new_v1(
            0x55,
            multihash::Multihash::<64>::wrap(0x12, b"wrong-genesis").unwrap(),
        );
        let payload = TestPayload("test content".to_string());
        let mut op = make_test_operation(wrong_genesis, OperationType::Create(payload));
        op.genesis = wrong_genesis; // This won't match the computed CID
        op.node_timestamp = Some(12345); // Set node_timestamp to trigger import path

        // Import should fail due to CID mismatch
        let result = repo.commit_operation(op);
        assert!(result.is_err());
        match result {
            Err(CrdtError::Internal(msg)) => {
                assert!(msg.contains("CID mismatch"));
            }
            other => panic!("Expected CID mismatch error, got: {:?}", other),
        }
    }
}
