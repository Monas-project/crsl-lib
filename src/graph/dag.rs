use crate::dasl::node::Node;
use crate::graph::storage::NodeStorage;
use cid::Cid;
use std::collections::HashMap;
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
        if self.would_create_cycle()? {
            return Err(GraphError::CycleDetected);
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
    fn would_create_cycle(&self) -> Result<bool, GraphError> {
        let node_map: HashMap<Cid, Vec<Cid>> = self.storage.get_node_map();

        // まず全ての必要な文字列を生成して保存
        let mut cid_to_string: HashMap<Cid, String> = HashMap::new();

        // 全てのCidを文字列に変換して保存
        for (child, parents) in &node_map {
            if !cid_to_string.contains_key(child) {
                cid_to_string.insert(*child, child.to_string());
            }

            for parent in parents {
                if !cid_to_string.contains_key(parent) {
                    cid_to_string.insert(*parent, parent.to_string());
                }
            }
        }

        // 文字列参照を使ってエッジを作成
        let mut edges: Vec<(&str, &str)> = Vec::new();
        for (child, parents) in &node_map {
            let child_str = cid_to_string.get(child).unwrap().as_str();

            for parent in parents {
                let parent_str = cid_to_string.get(parent).unwrap().as_str();
                edges.push((parent_str, child_str));
            }
        }

        println!("{:?}", edges);
        self.detect_cycle(&edges)
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
    fn detect_cycle(&self, edges: &[(&str, &str)]) -> Result<bool, GraphError> {
        #[derive(PartialEq)]
        enum State {
            NotVisited,
            Visiting,
            Visited,
        }

        // edges: &[(Cid<64>, Cid<64>)]

        fn build_graph(lines: &[(&str, &str)]) -> HashMap<String, Vec<String>> {
            let mut graph = HashMap::new();
            for (u, v) in lines {
                graph
                    .entry(u.to_string())
                    .or_insert_with(Vec::new)
                    .push(v.to_string());
                graph.entry(v.to_string()).or_insert_with(Vec::new);
            }
            graph
        }

        fn dfs(
            vertex: &String,
            graph: &HashMap<String, Vec<String>>,
            state: &mut HashMap<String, State>,
        ) -> bool {
            state.insert(vertex.clone(), State::Visiting);
            if let Some(neighbors) = graph.get(vertex) {
                for neighbor in neighbors {
                    match state.get(neighbor) {
                        Some(State::NotVisited) => {
                            if dfs(neighbor, graph, state) {
                                return true;
                            }
                        }
                        Some(State::Visiting) => return true,
                        _ => {}
                    }
                }
            }
            state.insert(vertex.clone(), State::Visited);
            false
        }

        fn is_cyclic_graph(lines: &[(&str, &str)]) -> bool {
            let graph = build_graph(lines);
            let mut state = HashMap::new();
            for vertex in graph.keys() {
                state.insert(vertex.clone(), State::NotVisited);
            }
            for vertex in graph.keys() {
                if state.get(vertex) == Some(&State::NotVisited) && dfs(vertex, &graph, &mut state)
                {
                    return true;
                }
            }
            false
        }

        let lines = edges;
        if is_cyclic_graph(lines) {
            // 巡回グラフ
            Ok(true)
        } else {
            // 非巡回グラフ
            Ok(false)
        }
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

        fn get_node_map(&self) -> HashMap<Cid, Vec<Cid>> {
            self.edges.clone()
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
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let result = dag.would_create_cycle();

        assert!(result.is_ok());
        assert!(!result.unwrap(), "false");
    }

    #[test]
    fn test_empty_graph_has_acyclic() {
        let storage = MockStorage::new();
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let result = dag.would_create_cycle();
        println!("{:?}", result);

        assert!(result.is_ok());
        assert!(!result.unwrap(), "false");
    }

    #[test]
    fn test_large_acyclic_graph() {
        let mut storage = MockStorage::new();
        // 1000個
        let num_nodes = 1000;
        let mut nodes = Vec::with_capacity(num_nodes + 1);
        for i in 0..=num_nodes {
            let node_label = format!("node_{}", i);
            let cid = create_test_content_id(node_label.as_bytes());
            nodes.push(cid);
        }
        let edges: Vec<_> = nodes.windows(2).map(|pair| (pair[0], pair[1])).collect();
        storage.setup_graph(&edges);
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let result = dag.would_create_cycle();
        println!("{:?}", result);

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
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let result = dag.would_create_cycle();
        println!("{:?}", result);

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
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let result = dag.would_create_cycle();

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
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let result = dag.would_create_cycle();

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
        let dag = DagGraph::<_, String, BTreeMap<String, String>>::new(storage);

        let result = dag.would_create_cycle();
        println!("{:?}", result);

        assert!(result.is_ok());
        assert!(result.unwrap(), "true");
    }
}
