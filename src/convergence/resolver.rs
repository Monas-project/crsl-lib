use crate::convergence::policy::{MergePolicy, ResolveInput};
use crate::graph::dag::DagGraph;
use crate::graph::storage::NodeStorage;
use crate::crdt::error::{CrdtError, Result as CrdtResult};
use crate::dasl::node::Node;
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

    pub fn create_merge_node<S>(
        &self,
        heads: &[Cid],
        dag: &DagGraph<S, P, M>,
        genesis: Cid,
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
        let timestamp = Self::current_timestamp()?;
        Ok(Node::new_child(
            merged_payload,
            heads.to_vec(),
            genesis,
            timestamp,
            metadata,
        ))
    }

    fn collect_inputs<S>(&self, heads: &[Cid], dag: &DagGraph<S, P, M>) -> CrdtResult<Vec<ResolveInput<P>>>
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
            inputs.push(ResolveInput::new(cid, node.payload().clone(), node.timestamp()));
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

    fn current_timestamp() -> CrdtResult<u64> {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| CrdtError::Internal(format!("timestamp error: {e}")))
            .map(|duration| duration.as_secs())
    }
}


