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
    edges_forward: HashMap<Cid, Vec<Cid>>, // parent -> children
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
            edges_forward: HashMap::new(),
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
        let new_cid = node.content_id().unwrap();
        if self.would_create_cycle_with(&new_cid, &parents)? {
            return Err(GraphError::CycleDetected);
        }

        self.storage.put(&node)?;

        self.ensure_cache()?;
        for &parent in &parents {
            self.edges_forward.entry(parent).or_default().push(new_cid);
        }

        self.edges_forward.entry(new_cid).or_default();

        Ok(new_cid)
    }

    fn current_timestamp() -> Result<u64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(GraphError::Timestamp)
            .map(|d| d.as_secs())
    }

    fn would_create_cycle_with(&mut self, new_cid: &Cid, parents: &[Cid]) -> Result<bool> {
        self.ensure_cache()?;

        for &parent in parents {
            if self.path_exists(parent, *new_cid) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn ensure_cache(&mut self) -> Result<()> {
        if self.edges_forward.is_empty() {
            let node_map = self.storage.get_node_map()?;
            self.edges_forward = Self::build_adjacency_list(&node_map);
        }
        Ok(())
    }

    fn path_exists(&self, start: Cid, target: Cid) -> bool {
        if start == target {
            return true;
        }
        let mut stack = vec![start];
        let mut visited = std::collections::HashSet::new();
        while let Some(node) = stack.pop() {
            if node == target {
                return true;
            }
            if visited.insert(node) {
                if let Some(children) = self.edges_forward.get(&node) {
                    for &child in children {
                        stack.push(child);
                    }
                }
            }
        }
        false
    }

    pub fn detect_cycle_cid(node_map: &HashMap<Cid, Vec<Cid>>) -> Result<bool> {
        let graph = Self::build_adjacency_list(node_map);
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();

        for node in graph.keys() {
            if !visited.contains(node)
                && Self::has_cycle(*node, &graph, &mut visited, &mut rec_stack)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn build_adjacency_list(node_map: &HashMap<Cid, Vec<Cid>>) -> HashMap<Cid, Vec<Cid>> {
        let mut graph: HashMap<Cid, Vec<Cid>> = HashMap::new();

        for (child, parents) in node_map {
            graph.entry(*child).or_default();
            for parent in parents {
                graph.entry(*parent).or_default();
            }
        }

        for (child, parents) in node_map {
            for parent in parents {
                graph.get_mut(parent).unwrap().push(*child);
            }
        }

        graph
    }

    fn has_cycle(
        node: Cid,
        graph: &HashMap<Cid, Vec<Cid>>,
        visited: &mut std::collections::HashSet<Cid>,
        rec_stack: &mut std::collections::HashSet<Cid>,
    ) -> bool {
        visited.insert(node);
        rec_stack.insert(node);

        if let Some(neighbors) = graph.get(&node) {
            for &neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    if Self::has_cycle(neighbor, graph, visited, rec_stack) {
                        return true;
                    }
                } else if rec_stack.contains(&neighbor) {
                    return true;
                }
            }
        }

        rec_stack.remove(&node);
        false
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
    use std::collections::BTreeMap;

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
    fn test_acyclic_in_simple_path() {
        // simple path A -> B -> C
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        let cid_d = create_test_content_id(b"node_d");
        storage.setup_graph(&[(cid_a, cid_b), (cid_b, cid_c), (cid_c, cid_d)]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let node_map =
            <MockStorage as NodeStorage<String, BTreeMap<String, String>>>::get_node_map(
                &dag.storage,
            )
            .unwrap();
        let result =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::detect_cycle_cid(&node_map);

        assert!(result.is_ok());
        assert!(!result.unwrap(), "false");
    }

    #[test]
    fn test_empty_graph_has_acyclic() {
        let storage = MockStorage::new();
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let node_map =
            <MockStorage as NodeStorage<String, BTreeMap<String, String>>>::get_node_map(
                &dag.storage,
            )
            .unwrap();
        let result =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::detect_cycle_cid(&node_map);

        assert!(result.is_ok());
        assert!(!result.unwrap(), "false");
    }

    #[test]
    fn test_large_acyclic_graph() {
        let mut storage = MockStorage::new();
        // 1000 nodes
        let num_nodes = 1000;
        let mut nodes = Vec::with_capacity(num_nodes + 1);
        for i in 0..=num_nodes {
            let node_label = format!("node_{i}");
            let cid = create_test_content_id(node_label.as_bytes());
            nodes.push(cid);
        }
        let edges: Vec<_> = nodes.windows(2).map(|pair| (pair[0], pair[1])).collect();
        storage.setup_graph(&edges);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let node_map =
            <MockStorage as NodeStorage<String, BTreeMap<String, String>>>::get_node_map(
                &dag.storage,
            )
            .unwrap();
        let result =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::detect_cycle_cid(&node_map);

        assert!(result.is_ok());
        assert!(!result.unwrap(), "false");
    }

    #[test]
    fn test_multi_path_acyclic_detection() {
        // multi path acyclic detection
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        let cid_d = create_test_content_id(b"node_d");
        let cid_e = create_test_content_id(b"node_e");
        let cid_f = create_test_content_id(b"node_f");
        let cid_g = create_test_content_id(b"node_g");
        let cid_h = create_test_content_id(b"node_h");
        storage.setup_graph(&[
            (cid_a, cid_f),
            (cid_b, cid_d),
            (cid_b, cid_g),
            (cid_c, cid_f),
            (cid_c, cid_h),
            (cid_d, cid_a),
            (cid_d, cid_h),
            (cid_e, cid_b),
            (cid_e, cid_c),
            (cid_e, cid_g),
            (cid_g, cid_h),
            (cid_h, cid_a),
        ]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let node_map =
            <MockStorage as NodeStorage<String, BTreeMap<String, String>>>::get_node_map(
                &dag.storage,
            )
            .unwrap();
        let result =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::detect_cycle_cid(&node_map);

        assert!(result.is_ok());
        assert!(!result.unwrap(), "false");
    }

    #[test]
    fn test_direct_cycle_detection() {
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        storage.setup_graph(&[(cid_a, cid_b), (cid_b, cid_c), (cid_c, cid_a)]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let node_map =
            <MockStorage as NodeStorage<String, BTreeMap<String, String>>>::get_node_map(
                &dag.storage,
            )
            .unwrap();
        let result =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::detect_cycle_cid(&node_map);

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
        let cid_e = create_test_content_id(b"node_e");
        let cid_f = create_test_content_id(b"node_f");
        let cid_g = create_test_content_id(b"node_g");
        let cid_h = create_test_content_id(b"node_h");
        let cid_i = create_test_content_id(b"node_i");
        let cid_j = create_test_content_id(b"node_j");
        storage.setup_graph(&[
            (cid_a, cid_b),
            (cid_b, cid_c),
            (cid_c, cid_d),
            (cid_d, cid_e),
            (cid_e, cid_f),
            (cid_f, cid_g),
            (cid_g, cid_h),
            (cid_h, cid_i),
            (cid_i, cid_j),
            (cid_j, cid_a),
        ]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let node_map =
            <MockStorage as NodeStorage<String, BTreeMap<String, String>>>::get_node_map(
                &dag.storage,
            )
            .unwrap();
        let result =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::detect_cycle_cid(&node_map);

        assert!(result.is_ok());
        assert!(result.unwrap(), "true");
    }

    #[test]
    fn test_multi_path_cycle_detection() {
        // multi path cycle detection
        //    　 A
        //   　↙ ↘
        //  　B     C
        //　↙ ↘ ↙
        // D    E → A
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        let cid_d = create_test_content_id(b"node_d");
        let cid_e = create_test_content_id(b"node_e");
        storage.setup_graph(&[
            (cid_a, cid_b),
            (cid_a, cid_c),
            (cid_b, cid_d),
            (cid_b, cid_e),
            (cid_c, cid_e),
            (cid_e, cid_a),
        ]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let node_map =
            <MockStorage as NodeStorage<String, BTreeMap<String, String>>>::get_node_map(
                &dag.storage,
            )
            .unwrap();
        let result =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::detect_cycle_cid(&node_map);

        assert!(result.is_ok());
        assert!(result.unwrap(), "true");
    }

    #[test]
    fn test_latest_head() {
        let mut dag =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(MockStorage::new());
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");

        dag.set_head(&cid_a, cid_b);
        let head = dag.latest_head(&cid_a);

        assert!(head.is_some());
        assert_eq!(head.unwrap(), cid_b);
    }

    #[test]
    fn test_empty_latest_head() {
        let dag =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(MockStorage::new());
        let cid_a = create_test_content_id(b"node_a");

        let head = dag.latest_head(&cid_a);

        assert!(head.is_none());
    }

    #[test]
    fn test_multiple_heads() {
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        storage.setup_graph(&[(cid_a, cid_b)]);
        let mut dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);
        dag.set_head(&cid_a, cid_b);
        dag.set_head(&cid_b, cid_c);
        let head_a = dag.latest_head(&cid_a).unwrap();
        let head_b = dag.latest_head(&cid_b).unwrap();
        assert_eq!(head_a, cid_b);
        assert_eq!(head_b, cid_c);
    }

    // -------------------------------------------------------
    // Incremental cycle detection / cache validation tests
    // -------------------------------------------------------

    #[test]
    fn test_incremental_add_node_no_cycle() {
        // Existing graph: A -> B
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        storage.setup_graph(&[(cid_a, cid_b)]);

        let mut dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        // Add a new node whose parent is B (should NOT create a cycle)
        let new_cid = dag
            .add_node("payload".to_string(), vec![cid_b], BTreeMap::new())
            .expect("add_node should succeed");

        // Verify edges_forward is updated (B -> new_cid)
        assert!(dag
            .edges_forward
            .get(&cid_b)
            .expect("parent key must exist")
            .contains(&new_cid));
    }

    #[test]
    fn test_incremental_cache_reuse() {
        // Existing graph: single edge A -> B
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"a");
        let cid_b = create_test_content_id(b"b");
        storage.setup_graph(&[(cid_a, cid_b)]);

        let mut dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        // The first add_node call builds the cache
        let cid1 = dag
            .add_node("n1".to_string(), vec![cid_b], BTreeMap::new())
            .expect("first add");
        let cache_size_before = dag.edges_forward.len();

        // The second add_node call reuses the cache
        let _cid2 = dag
            .add_node("n2".to_string(), vec![cid1], BTreeMap::new())
            .expect("second add");

        // One extra node -> cache size should increase by exactly 1
        assert_eq!(cache_size_before + 1, dag.edges_forward.len());
    }
}
