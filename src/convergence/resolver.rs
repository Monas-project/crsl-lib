use crate::convergence::policy::{MergePolicy, ResolveInput};
use crate::crdt::error::{CrdtError, Result as CrdtResult};
use crate::dasl::node::Node;
use crate::graph::dag::DagGraph;
use crate::graph::storage::NodeStorage;
use cid::Cid;
use std::marker::PhantomData;

/// Responsible for orchestrating merge operations by delegating
/// DAG traversal and policy selection to dedicated components.
#[derive(Debug, Default, Clone)]
pub struct ConflictResolver<P, M> {
    _marker: PhantomData<(P, M)>,
}

impl<P, M> ConflictResolver<P, M>
where
    P: Clone,
    M: Clone,
{
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    /// Creates a merge node from the given heads.
    ///
    /// # Arguments
    ///
    /// * `heads` - The head CIDs to merge
    /// * `dag` - The DAG graph
    /// * `genesis` - The genesis CID
    /// * `timestamp` - The timestamp to use for the merge node
    /// * `policy` - The merge policy to use
    pub fn create_merge_node<S>(
        &self,
        heads: &[Cid],
        dag: &DagGraph<S, P, M>,
        genesis: Cid,
        timestamp: u64,
        policy: &dyn MergePolicy<P>,
    ) -> CrdtResult<Node<P, M>>
    where
        S: NodeStorage<P, M>,
        P: serde::Serialize + for<'de> serde::Deserialize<'de>,
        M: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        if heads.is_empty() {
            return Err(CrdtError::Internal(
                "ConflictResolver requires at least one head to merge".to_string(),
            ));
        }

        let inputs = self.collect_inputs(heads, dag)?;
        let merged_payload = policy.resolve(&inputs);
        let metadata = self.merge_metadata(heads, dag)?;
        Ok(Node::new_child(
            merged_payload,
            heads.to_vec(),
            genesis,
            timestamp,
            metadata,
        ))
    }

    fn collect_inputs<S>(
        &self,
        heads: &[Cid],
        dag: &DagGraph<S, P, M>,
    ) -> CrdtResult<Vec<ResolveInput<P>>>
    where
        S: NodeStorage<P, M>,
        P: serde::Serialize + for<'de> serde::Deserialize<'de>,
        M: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        let mut inputs = Vec::with_capacity(heads.len());
        for &cid in heads {
            let node = dag
                .get_node(&cid)
                .map_err(CrdtError::Graph)?
                .ok_or_else(|| CrdtError::Internal(format!("Head node not found: {cid}")))?;
            inputs.push(ResolveInput::new(
                cid,
                node.payload().clone(),
                node.timestamp(),
            ));
        }
        Ok(inputs)
    }

    fn merge_metadata<S>(&self, heads: &[Cid], dag: &DagGraph<S, P, M>) -> CrdtResult<M>
    where
        S: NodeStorage<P, M>,
        P: serde::Serialize + for<'de> serde::Deserialize<'de>,
        M: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        let first_head = heads
            .first()
            .ok_or_else(|| CrdtError::Internal("No heads provided".to_string()))?;
        let node = dag
            .get_node(first_head)
            .map_err(CrdtError::Graph)?
            .ok_or_else(|| CrdtError::Internal(format!("Head node not found: {first_head}")))?;
        Ok(node.metadata().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convergence::metadata::ContentMetadata;
    use crate::convergence::policies::lww::LwwMergePolicy;
    use crate::convergence::policy::{MergePolicy, ResolveInput};
    use crate::crdt::error::CrdtError;
    use crate::dasl::node::Node;
    use crate::graph::error::{GraphError, Result as GraphResult};
    use crate::graph::storage::NodeStorage;
    use multihash::Multihash;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct MemoryNodeStorage<P, M> {
        nodes: Arc<Mutex<HashMap<Cid, Node<P, M>>>>,
    }

    impl<P, M> MemoryNodeStorage<P, M>
    where
        P: Clone + Serialize + for<'de> Deserialize<'de>,
        M: Clone + Serialize + for<'de> Deserialize<'de>,
    {
        fn insert(&self, node: &Node<P, M>) -> GraphResult<()> {
            let cid = node
                .content_id()
                .map_err(|e| GraphError::NodeOperation(e.to_string()))?;
            self.nodes.lock().unwrap().insert(cid, node.clone());
            Ok(())
        }
    }

    impl<P, M> NodeStorage<P, M> for MemoryNodeStorage<P, M>
    where
        P: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync,
        M: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync,
    {
        fn get(&self, content_id: &Cid) -> GraphResult<Option<Node<P, M>>> {
            Ok(self.nodes.lock().unwrap().get(content_id).cloned())
        }

        fn put(&self, node: &Node<P, M>) -> GraphResult<()> {
            self.insert(node)
        }

        fn delete(&self, content_id: &Cid) -> GraphResult<()> {
            self.nodes.lock().unwrap().remove(content_id);
            Ok(())
        }

        fn get_node_map(&self) -> GraphResult<HashMap<Cid, Vec<Cid>>> {
            let mut map = HashMap::new();
            for (cid, node) in self.nodes.lock().unwrap().iter() {
                map.insert(*cid, node.parents().to_vec());
            }
            Ok(map)
        }
    }

    struct AssertingPolicy {
        expected: Vec<(Cid, String, u64)>,
        result: String,
    }

    impl MergePolicy<String> for AssertingPolicy {
        fn resolve(&self, nodes: &[ResolveInput<String>]) -> String {
            assert_eq!(nodes.len(), self.expected.len());
            for (input, expected) in nodes.iter().zip(&self.expected) {
                assert_eq!(input.cid, expected.0);
                assert_eq!(input.payload, expected.1);
                assert_eq!(input.timestamp, expected.2);
            }
            self.result.clone()
        }

        fn name(&self) -> &str {
            "assert"
        }
    }

    fn create_test_cid(label: &str) -> Cid {
        let digest = Multihash::<64>::wrap(0x12, label.as_bytes()).unwrap();
        Cid::new_v1(0x55, digest)
    }

    #[test]
    fn create_merge_node_merges_heads() {
        let storage = MemoryNodeStorage::<String, ContentMetadata>::default();
        let dag = DagGraph::new(storage.clone());

        let metadata = ContentMetadata::with_policy("custom");
        let genesis_node = Node::new_genesis("genesis".to_string(), 1, metadata.clone());
        let genesis_cid = genesis_node.content_id().unwrap();
        dag.storage.put(&genesis_node).unwrap();

        let head_a = Node::new_child(
            "payload-a".to_string(),
            vec![genesis_cid],
            genesis_cid,
            10,
            metadata.clone(),
        );
        let head_a_cid = head_a.content_id().unwrap();
        dag.storage.put(&head_a).unwrap();

        let head_b = Node::new_child(
            "payload-b".to_string(),
            vec![genesis_cid],
            genesis_cid,
            20,
            metadata.clone(),
        );
        let head_b_cid = head_b.content_id().unwrap();
        dag.storage.put(&head_b).unwrap();

        let policy = AssertingPolicy {
            expected: vec![
                (head_a_cid, "payload-a".to_string(), 10),
                (head_b_cid, "payload-b".to_string(), 20),
            ],
            result: "merged".to_string(),
        };

        let resolver = ConflictResolver::<String, ContentMetadata>::new();
        let merge_timestamp = 100;
        let merge_node = resolver
            .create_merge_node(
                &[head_a_cid, head_b_cid],
                &dag,
                genesis_cid,
                merge_timestamp,
                &policy,
            )
            .unwrap();

        assert_eq!(merge_node.payload(), "merged");
        assert_eq!(merge_node.parents(), &vec![head_a_cid, head_b_cid]);
        assert_eq!(merge_node.metadata(), &metadata);
        assert_eq!(merge_node.genesis, Some(genesis_cid));
        assert_eq!(merge_node.timestamp(), merge_timestamp);
    }

    #[test]
    fn create_merge_node_requires_non_empty_heads() {
        let dag = DagGraph::new(MemoryNodeStorage::<String, ContentMetadata>::default());
        let resolver = ConflictResolver::<String, ContentMetadata>::new();
        let policy = LwwMergePolicy;
        let genesis = create_test_cid("genesis");

        let result = resolver.create_merge_node(&[], &dag, genesis, 100, &policy);

        assert!(matches!(
            result,
            Err(CrdtError::Internal(message)) if message.contains("requires at least one head")
        ));
    }

    #[test]
    fn create_merge_node_fails_when_head_missing() {
        let dag = DagGraph::new(MemoryNodeStorage::<String, ContentMetadata>::default());
        let resolver = ConflictResolver::<String, ContentMetadata>::new();
        let policy = LwwMergePolicy;
        let missing_head = create_test_cid("missing-head");
        let genesis = create_test_cid("genesis");

        let result = resolver.create_merge_node(&[missing_head], &dag, genesis, 100, &policy);

        assert!(matches!(
            result,
            Err(CrdtError::Internal(message)) if message.contains("Head node not found")
        ));
    }
}
