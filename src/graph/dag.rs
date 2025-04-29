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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dasl::node::Node;
    use std::collections::{BTreeMap, HashMap};

    #[derive(Debug)]
    struct MockStorage {
        edges: HashMap<Cid, Vec<Cid>>,
    }
    impl MockStorage {
        fn new() -> Self {
            Self {
                edges: HashMap::new(),
            }
        }

        fn setup_graph(&mut self, structure: &[(Cid, Cid)]) {
            for (parent, child) in structure {
                self.edges
                    .entry(*parent)
                    .or_default()
                    .push(*child);
            }
        }
    }

    impl<P, M> NodeStorage<P, M> for MockStorage {
        fn get(&self, _content_id: &Cid) -> Option<Node<P, M>> {
            None
        }

        fn put(&mut self, _node: &Node<P, M>) {}

        fn delete(&mut self, _content_id: &Cid) {}
    }

    fn create_test_content_id(data: &[u8]) -> Cid {
        use multihash::Multihash;
        let code = 0x12;
        let digest = Multihash::<64>::wrap(code, data).unwrap();
        Cid::new_v1(0x55, digest)
    }

    #[test]
    fn test_no_cycle_in_simple_pash() {
        // 単純なパス 1 -> 2 -> 3
        let mut storage = MockStorage::new();
        let cid1 = create_test_content_id(b"node1");
        let cid2 = create_test_content_id(b"node2");
        let cid3 = create_test_content_id(b"node3");
        storage.setup_graph(&[(cid1, cid2), (cid2, cid3)]);
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        // 新しいノードを追加
        let cid4 = create_test_content_id(b"node4");
        let result = dag.would_create_cycle(&cid3, &cid4);
        assert!(result.is_ok());
        assert!(!result.unwrap(), "false");
    }

    #[test]
    fn test_empty_graph_has_no_cycles() {
        let storage = MockStorage::new();
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let cid1 = create_test_content_id(b"node1");
        let cid2 = create_test_content_id(b"node2");
        let result = dag.would_create_cycle(&cid1, &cid2);

        assert!(result.is_ok());
        assert!(!result.unwrap(), "false");
    }
}
