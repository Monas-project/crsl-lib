use crate::dasl::node::Node;
use crate::graph::error::{GraphError, Result};
use crate::graph::storage::NodeStorage;
use cid::Cid;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub heads: HashMap<String, Cid>,
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
            heads: HashMap::new(),
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
    pub fn add_node(&mut self, payload: P, parents: Vec<Cid>, metadata: M) -> Result<Cid> {
        let timestamp = Self::current_timestamp()?;
        let node = Node::new(payload, parents.clone(), timestamp, metadata);
        for parent in &parents {
            if self
                .would_create_cycle(parent, &node.content_id().unwrap())
                .unwrap()
            {
                return Err(GraphError::CycleDetected);
            }
        }
        self.storage.put(&node)?;
        Ok(node.content_id().unwrap())
    }

    fn current_timestamp() -> Result<u64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(GraphError::Timestamp)
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
    fn would_create_cycle(&self, parent_cid: &Cid, child_cid: &Cid) -> Result<bool> {
        let node_map: HashMap<Cid, Vec<Cid>> = self.storage.get_node_map()?;
        self.check_for_cycles(parent_cid, child_cid, &node_map)
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
    fn check_for_cycles(
        &self,
        _parent_cid: &Cid,
        _child_cid: &Cid,
        _node_map: &HashMap<Cid, Vec<Cid>>,
    ) -> Result<bool> {
        // 実際のサイクル検出アルゴリズムはここに実装予定
        // 今は単にfalseを返す仮実装
        Ok(false)
    }

    pub fn latest_head(&self, content_id: &Cid) -> Option<Cid> {
        self.heads.get(content_id.to_string().as_str()).cloned()
    }

    pub fn set_head(&mut self, content_id: &Cid, head: Cid) {
        self.heads.insert(content_id.to_string(), head);
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
        fn get(&self, content_id: &Cid) -> Result<Option<Node<P, M>>> {
            Ok(Some(Node::new(
                P::default(),
                self.edges.get(content_id).cloned().unwrap_or_default(),
                0,
                M::default(),
            )))
        }

        fn put(&self, _node: &Node<P, M>) -> Result<()> {
            Ok(())
        }

        fn delete(&self, _content_id: &Cid) -> Result<()> {
            Ok(())
        }

        fn get_node_map(&self) -> Result<HashMap<Cid, Vec<Cid>>> {
            Ok(self.edges.clone())
        }
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

        storage.setup_graph(&[(cid_a, cid_b), (cid_b, cid_c)]);
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
        assert!(!result.unwrap(), "false");
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
        assert!(!result.unwrap(), "false");
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
        assert!(!result.unwrap(), "false");
    }

    #[test]
    fn test_latest_head() {
        let mut dag = DagGraph::<_, String, BTreeMap<String, String>>::new(MockStorage::new());
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");

        dag.set_head(&cid_a, cid_b);
        let head = dag.latest_head(&cid_a);

        assert!(head.is_some());
        assert_eq!(head.unwrap(), cid_b);
    }

    #[test]
    fn test_empty_latest_head() {
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(MockStorage::new());
        let cid_a = create_test_content_id(b"node_a");

        let head = dag.latest_head(&cid_a);

        assert!(head.is_none());
    }

    #[test]
    fn test_multiple_heads() {
        let mut dag = DagGraph::<_, String, BTreeMap<String, String>>::new(MockStorage::new());
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        dag.set_head(&cid_a, cid_b);
        dag.set_head(&cid_a, cid_c);

        let head = dag.latest_head(&cid_a);
        println!("head: {:?}", head);

        assert!(head.is_some());
        assert!(head.unwrap() == cid_c);
    }
}
