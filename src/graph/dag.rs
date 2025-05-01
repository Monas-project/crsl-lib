use crate::dasl::node::Node;
use crate::graph::storage::NodeStorage;
use cid::Cid;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::time::{SystemTime, UNIX_EPOCH};

/// エラーの種類を表す列挙型
#[derive(Debug)]
pub enum GraphError {
    CycleDetected,
    NodeNotFound,
    StorageError,
}

/// Directed Acyclic Graph(DAG) Structure
///
/// # Arguments
///
/// * `S` - NodeStorage
/// * `P` - Payload
/// * `M` - Metadata
#[derive(Debug)]
pub struct DagGraph<S, P, M>
where
    S: NodeStorage<P, M>,
{
    pub storage: S,
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
            storage,
            _p_marker: PhantomData,
            _m_marker: PhantomData,
        }
    }

    /// Add an edge to the graph
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload
    /// * `parents` - The parent content Ids
    /// * `metadata` - The metadata
    ///
    /// # Returns
    ///
    /// * `Cid` - The content Id of the new node
    ///
    pub fn add_node(
        &mut self,
        payload: P,
        parents: Vec<Cid>,
        metadata: M,
    ) -> Result<Cid, GraphError> {
        let timestamp = Self::current_timestamp()?;
        let node = Node::new(payload, parents.clone(), timestamp, metadata);
        for parent in &parents {
            if self.would_create_cycle(parent, &node.content_id())? {
                return Err(GraphError::CycleDetected);
            }
        }
        self.storage.put(&node);
        Ok(node.content_id())
    }

    fn current_timestamp() -> Result<u64, GraphError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GraphError::StorageError)
            .map(|d| d.as_secs())
    }

    /// Check if adding an edge would create a cycle
    ///
    /// # Arguments
    ///
    /// * `parent_cid` - The parent content Id
    /// * `child_cid` - The child content Id
    ///
    /// # Returns
    ///
    /// * `true` - If a cycle is detected
    /// * `false` - If no cycle is detected
    ///
    fn would_create_cycle(&self, parent_cid: &Cid, child_cid: &Cid) -> Result<bool, GraphError> {
        let mut visited = HashSet::new();
        self.detect_cycle(child_cid, parent_cid, &mut visited)
    }

    /// Detect a cycle in the graph
    ///
    /// # Arguments
    ///
    /// * `current` - The current content Id
    /// * `target` - The target content Id
    /// * `visited` - The visited content Ids
    ///
    /// # Returns
    ///
    /// * `true` - If a cycle is detected
    /// * `false` - If no cycle is detected
    ///
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
        let node = self.storage.get(current).ok_or(GraphError::NodeNotFound)?;
        for parent_cid in node.parents() {
            if self.detect_cycle(parent_cid, target, visited)? {
                return Ok(true);
            }
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
                self.edges.entry(*child).or_default().push(*parent);
            }
        }
    }

    impl<P, M> NodeStorage<P, M> for MockStorage
    where
        P: Default + serde::Serialize + serde::de::DeserializeOwned,
        M: Default + serde::Serialize + serde::de::DeserializeOwned,
    {
        fn get(&self, content_id: &Cid) -> Option<Node<P, M>> {
            Some(Node::new(
                P::default(),
                self.edges.get(content_id).cloned().unwrap_or_default(),
                0,
                M::default(),
            ))
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
    fn test_no_cycle_in_simple_path() {
        // simple path A -> B -> C
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        let cid_d = create_test_content_id(b"node_d");
        storage.setup_graph(&[(cid_b, cid_a), (cid_c, cid_b)]);
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let result = dag.would_create_cycle(&cid_d, &cid_c);

        assert!(result.is_ok());
        assert!(!result.unwrap(), "false");
    }

    #[test]
    fn test_empty_graph_has_no_cycles() {
        let storage = MockStorage::new();
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let result = dag.would_create_cycle(&cid_a, &cid_b);

        assert!(result.is_ok());
        assert!(!result.unwrap(), "false");
    }
    #[test]
    fn test_direct_cycle_detection() {
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        storage.setup_graph(&[(cid_a, cid_b)]);
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);
        let result = dag.would_create_cycle(&cid_a, &cid_b);

        assert!(result.is_ok());
        assert!(result.unwrap(), "true");
    }

    #[test]
    fn test_long_path_cycle_detection() {
        // 1 -> 2 -> 3 -> 4 : 4 -> 1 this is a cycle
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        let cid_d = create_test_content_id(b"node_d");
        storage.setup_graph(&[(cid_b, cid_a), (cid_c, cid_b), (cid_d, cid_c)]);
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let result = dag.would_create_cycle(&cid_d, &cid_a);

        assert!(result.is_ok());
        assert!(result.unwrap(), "true");
    }

    #[test]
    fn test_multi_path_cycle_detection() {
        // multi path cycle detection
        //    A
        //   ↗ ↘
        //  B     C
        // ↗ ↘   ↗
        // D   E ← A
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        let cid_d = create_test_content_id(b"node_d");
        let cid_e = create_test_content_id(b"node_e");
        storage.setup_graph(&[
            (cid_b, cid_a),
            (cid_c, cid_a),
            (cid_d, cid_b),
            (cid_e, cid_b),
            (cid_e, cid_c),
        ]);
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let result = dag.would_create_cycle(&cid_e, &cid_a);
        assert!(result.is_ok());
        assert!(result.unwrap(), "true");
    }
}
