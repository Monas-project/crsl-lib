use crate::graph::storage::NodeStorage;
use cid::Cid;
use std::collections::HashSet;
use std::marker::PhantomData;

/// エラーの種類を表す列挙型
#[derive(Debug)]
pub enum GraphError {
    CycleDetected,
    NodeNotFound,
    StorageError,
}

#[derive(Debug)]
pub struct DagGraph<S, P, M>
where
    S: NodeStorage<P, M>,
{
    _storage: S,
    _p_marker: PhantomData<P>,
    _m_marker: PhantomData<M>,
}

impl<S, P, M> DagGraph<S, P, M>
where
    S: NodeStorage<P, M>,
    P: serde::Serialize + serde::de::DeserializeOwned,
    M: serde::Serialize + serde::de::DeserializeOwned,
{
    pub fn new(storage: S) -> Self {
        Self {
            _storage: storage,
            _p_marker: PhantomData,
            _m_marker: PhantomData,
        }
    }

    pub fn try_add_edge(&mut self, parent_cid: &Cid, child_cid: &Cid) -> Result<(), GraphError> {
        if self.would_create_cycle(parent_cid, child_cid)? {
            return Err(GraphError::CycleDetected);
        }

        Ok(())
    }

    /// Check if adding an edge would create a cycle
    fn would_create_cycle(&self, from: &Cid, to: &Cid) -> Result<bool, GraphError> {
        let mut visited = HashSet::new();
        self.detect_cycle(to, from, &mut visited)
    }

    fn detect_cycle(
        &self,
        current: &Cid,
        target: &Cid,
        visited: &mut HashSet<Cid>,
    ) -> Result<bool, GraphError> {
        if current == target {
            return Ok(true);
        }

        if !visited.insert(*current) {
            return Ok(false);
        }

        Ok(false)
    }
}
