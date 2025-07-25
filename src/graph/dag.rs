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
    pub fn add_node(&mut self, payload: P, parents: Vec<Cid>, metadata: M) -> Result<Cid> {
        let timestamp = Self::current_timestamp()?;
        let node = Node::new_genesis(payload, timestamp, metadata);
        let new_cid = node.content_id()?;
        if self.would_create_cycle_with(&new_cid, &parents)? {
            return Err(GraphError::CycleDetected);
        }
        self.storage.put(&node)?;
        Ok(new_cid)
    }

    /// Add a genesis node (first version of content)
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload
    /// * `metadata` - The metadata
    ///
    /// # Returns
    ///
    /// * `Cid` - The content Id of the new genesis node
    ///
    pub fn add_genesis_node(&mut self, payload: P, metadata: M) -> Result<Cid> {
        let timestamp = Self::current_timestamp()?;
        let node = Node::new_genesis(payload, timestamp, metadata);
        let cid = node.content_id()?;

        self.storage.put(&node)?;
        Ok(cid)
    }

    /// Add a version node (subsequent version of content)
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload
    /// * `parents` - The parent content Ids
    /// * `genesis` - The genesis CID that this node belongs to
    /// * `metadata` - The metadata
    ///
    /// # Returns
    ///
    /// * `Cid` - The content Id of the new version node
    ///
    pub fn add_version_node(
        &mut self,
        payload: P,
        parents: Vec<Cid>,
        genesis: Cid,
        metadata: M,
    ) -> Result<Cid> {
        let timestamp = Self::current_timestamp()?;
        let node = Node::new_child(payload, parents.clone(), genesis, timestamp, metadata);
        let cid = node.content_id()?;

        // Use optimized genesis-based cycle detection
        if self.would_create_cycle_with(&cid, &parents)? {
            return Err(GraphError::CycleDetected);
        }

        self.storage.put(&node)?;
        Ok(cid)
    }

    fn current_timestamp() -> Result<u64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(GraphError::Timestamp)
            .map(|d| d.as_secs())
    }

    /// Check if adding an edge (new node with parents) would create a cycle
    fn would_create_cycle_with(&self, new_cid: &Cid, parents: &[Cid]) -> Result<bool> {
        // Optimized: only get the minimal necessary nodes for cycle detection
        let node_map = self.get_subgraph(new_cid, parents)?;
        Self::detect_cycle_cid(&node_map)
    }

    /// Get minimal subgraph needed for cycle detection
    /// Only collects nodes that could be part of a cycle with the new node
    fn get_subgraph(&self, new_cid: &Cid, parents: &[Cid]) -> Result<HashMap<Cid, Vec<Cid>>> {
        let mut node_map = HashMap::new();
        node_map.insert(*new_cid, parents.to_vec());

        let mut to_process = parents.to_vec();
        let mut processed = std::collections::HashSet::new();

        while let Some(current_cid) = to_process.pop() {
            if processed.contains(&current_cid) {
                continue;
            }
            processed.insert(current_cid);

            if let Some(node) = self.storage.get(&current_cid)? {
                let node_parents = node.parents().to_vec();
                node_map.insert(current_cid, node_parents.clone());

                to_process.extend(node_parents);
            }
        }

        Ok(node_map)
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

    /// Get genesis CID from any version CID
    ///
    /// # Arguments
    ///
    /// * `version_cid` - The version CID to get genesis for
    ///
    /// # Returns
    ///
    /// * `Cid` - The genesis CID
    ///
    pub fn get_genesis(&self, version_cid: &Cid) -> Result<Cid> {
        match self.storage.get(version_cid)? {
            Some(node) => match node.genesis {
                Some(genesis_cid) => Ok(genesis_cid),
                None => Ok(*version_cid),
            },
            None => Err(GraphError::NodeNotFound(*version_cid)),
        }
    }

    /// Get history from a specific version
    ///
    /// # Arguments
    ///
    /// * `version_cid` - The version CID to get history from
    ///
    /// # Returns
    ///
    /// * `Vec<Cid>` - History from oldest to newest
    ///
    pub fn get_history_from_version(&self, version_cid: &Cid) -> Result<Vec<Cid>> {
        let mut history = vec![];
        let mut current = *version_cid;

        loop {
            let node = match self.storage.get(&current)? {
                Some(node) => node,
                None => return Err(GraphError::NodeNotFound(current)),
            };
            history.push(current);

            if node.parents().is_empty() {
                break;
            }
            current = node.parents()[0];
        }

        history.reverse();
        Ok(history)
    }

    /// Calculates the latest version CID for a given genesis ID by finding the leaf node(s) with the most recent timestamp.
    ///
    /// # Arguments
    ///
    /// * `genesis_id` - The CID of the genesis node to calculate the latest version for
    ///
    /// # Returns
    ///
    /// * `Option<Cid>` - The CID of the latest version node, or None if no such node exists
    ///
    /// # Errors
    ///
    /// Returns an error if node retrieval fails or an internal error occurs.
    pub fn calculate_latest(&self, genesis_id: &Cid) -> Result<Option<Cid>> {
        let versions = self.get_all_versions_for_genesis(genesis_id)?;
        if versions.is_empty() {
            return Ok(None);
        }
        if versions.len() == 1 {
            return Ok(Some(versions[0]));
        }
        let has_children = self.collect_nodes_with_children(&versions)?;
        let mut leaf_nodes = self.collect_leaf_nodes(&versions, &has_children)?;
        leaf_nodes.sort_by_key(|(_, timestamp)| std::cmp::Reverse(*timestamp));
        Ok(leaf_nodes.first().map(|(cid, _)| *cid))
    }

    // Returns the set of nodes (CIDs) that are referenced as parents (i.e., nodes that have children) among the given versions.
    fn collect_nodes_with_children(
        &self,
        versions: &[Cid],
    ) -> Result<std::collections::HashSet<Cid>> {
        let mut has_children = std::collections::HashSet::new();
        for &node_cid in versions {
            if let Some(node) = self.storage.get(&node_cid)? {
                for parent_cid in node.parents() {
                    if versions.contains(parent_cid) {
                        has_children.insert(*parent_cid);
                    }
                }
            }
        }
        Ok(has_children)
    }

    // Returns a list of leaf nodes (nodes without children) and their timestamps among the given versions.
    fn collect_leaf_nodes(
        &self,
        versions: &[Cid],
        has_children: &std::collections::HashSet<Cid>,
    ) -> Result<Vec<(Cid, u64)>> {
        let mut leaf_nodes = Vec::new();
        for &node_cid in versions {
            if !has_children.contains(&node_cid) {
                if let Some(node) = self.storage.get(&node_cid)? {
                    leaf_nodes.push((node_cid, node.timestamp()));
                }
            }
        }
        Ok(leaf_nodes)
    }

    // Collects all CIDs of nodes related to the given genesis ID.
    fn get_all_versions_for_genesis(&self, genesis_id: &Cid) -> Result<Vec<Cid>> {
        let mut versions = Vec::new();
        let node_map = self.storage.get_node_map()?;
        for (cid, _) in node_map {
            if let Some(node) = self.storage.get(&cid)? {
                if cid == *genesis_id || node.genesis == Some(*genesis_id) {
                    versions.push(cid);
                }
            }
        }
        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[derive(Debug)]
    struct MockStorage {
        edges: RefCell<HashMap<Cid, Vec<Cid>>>,
        timestamps: RefCell<HashMap<Cid, u64>>,
    }
    impl MockStorage {
        fn new() -> Self {
            Self {
                edges: RefCell::new(HashMap::new()),
                timestamps: RefCell::new(HashMap::new()),
            }
        }

        fn setup_graph(&mut self, structure: &[(Cid, Cid)]) {
            let mut ts = 1;
            for (parent, child) in structure {
                self.edges
                    .borrow_mut()
                    .entry(*child)
                    .or_default()
                    .push(*parent);
                self.edges.borrow_mut().entry(*parent).or_default();
                self.timestamps.borrow_mut().entry(*parent).or_insert(ts);
                ts += 1;
                self.timestamps.borrow_mut().entry(*child).or_insert(ts);
                ts += 1;
            }
        }
    }

    impl<P, M> NodeStorage<P, M> for MockStorage
    where
        P: Default + serde::Serialize + serde::de::DeserializeOwned,
        M: Default + serde::Serialize + serde::de::DeserializeOwned,
    {
        fn get(&self, content_id: &Cid) -> Result<Option<Node<P, M>>> {
            let parents = self.edges.borrow().get(content_id).cloned();

            let parents = match parents {
                Some(p) => p,
                None => return Ok(None),
            };

            let ts = *self.timestamps.borrow().get(content_id).unwrap_or(&0);

            fn find_genesis(edges: &HashMap<Cid, Vec<Cid>>, cid: &Cid) -> Cid {
                let mut current = *cid;
                while let Some(parents) = edges.get(&current) {
                    if parents.is_empty() {
                        return current;
                    }
                    current = parents[0];
                }
                current
            }
            let genesis_cid = find_genesis(&self.edges.borrow(), content_id);

            let node = if parents.is_empty() {
                Node::new_genesis(P::default(), ts, M::default())
            } else {
                Node::new_child(P::default(), parents, genesis_cid, ts, M::default())
            };

            Ok(Some(node))
        }

        fn put(&self, node: &Node<P, M>) -> Result<()> {
            let cid = node.content_id()?;
            let parents = node.parents().to_vec();
            let ts = node.timestamp();
            self.edges.borrow_mut().insert(cid, parents);
            self.timestamps.borrow_mut().insert(cid, ts);
            Ok(())
        }

        fn delete(&self, _content_id: &Cid) -> Result<()> {
            Ok(())
        }

        fn get_node_map(&self) -> Result<HashMap<Cid, Vec<Cid>>> {
            Ok(self.edges.borrow().clone())
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
    fn test_add_genesis_node() {
        let mut dag =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(MockStorage::new());
        let cid = dag
            .add_genesis_node("test".to_string(), BTreeMap::new())
            .unwrap();
        let node: Option<Node<String, BTreeMap<String, String>>> = dag.storage.get(&cid).unwrap();
        assert!(node.is_some());
    }

    #[test]
    fn test_add_version_node() {
        let mut dag =
            DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(MockStorage::new());
        let genesis_cid = dag
            .add_genesis_node("genesis".to_string(), BTreeMap::new())
            .unwrap();
        let version_cid = dag
            .add_version_node(
                "version".to_string(),
                vec![genesis_cid],
                genesis_cid,
                BTreeMap::new(),
            )
            .unwrap();
        let node: Option<Node<String, BTreeMap<String, String>>> =
            dag.storage.get(&version_cid).unwrap();
        assert!(node.is_some());
        let node = node.unwrap();
        assert_eq!(node.parents(), &[genesis_cid]);
    }

    #[test]
    fn test_get_genesis_from_genesis_node() {
        let storage = MockStorage::new();
        let genesis_cid = create_test_content_id(b"genesis");
        storage.edges.borrow_mut().entry(genesis_cid).or_default();
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let result = dag.get_genesis(&genesis_cid);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), genesis_cid);
    }

    #[test]
    fn test_get_genesis_from_child_node() {
        let mut storage = MockStorage::new();
        let genesis_cid = create_test_content_id(b"genesis");
        let child_cid = create_test_content_id(b"child");
        storage.setup_graph(&[(genesis_cid, child_cid)]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let result = dag.get_genesis(&child_cid);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), genesis_cid);
    }

    #[test]
    fn test_get_genesis_node_not_found() {
        let storage = MockStorage::new();
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);
        let non_existent_cid = create_test_content_id(b"non_existent");

        let result = dag.get_genesis(&non_existent_cid);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_history_from_version_simple_path() {
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        storage.setup_graph(&[(cid_a, cid_b), (cid_b, cid_c)]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let history = dag.get_history_from_version(&cid_c).unwrap();
        assert_eq!(history, vec![cid_a, cid_b, cid_c]);
    }

    #[test]
    fn test_get_history_from_version_genesis_only() {
        let storage = MockStorage::new();
        let genesis_cid = create_test_content_id(b"genesis");
        storage.edges.borrow_mut().entry(genesis_cid).or_default();
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let history = dag.get_history_from_version(&genesis_cid).unwrap();
        assert_eq!(history, vec![genesis_cid]);
    }

    #[test]
    fn test_get_history_from_version_long_path() {
        let mut storage = MockStorage::new();
        let cid_a = create_test_content_id(b"node_a");
        let cid_b = create_test_content_id(b"node_b");
        let cid_c = create_test_content_id(b"node_c");
        let cid_d = create_test_content_id(b"node_d");
        let cid_e = create_test_content_id(b"node_e");
        storage.setup_graph(&[
            (cid_a, cid_b),
            (cid_b, cid_c),
            (cid_c, cid_d),
            (cid_d, cid_e),
        ]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);

        let history = dag.get_history_from_version(&cid_e).unwrap();
        assert_eq!(history, vec![cid_a, cid_b, cid_c, cid_d, cid_e]);
    }

    #[test]
    fn test_get_history_from_version_node_not_found() {
        let storage = MockStorage::new();
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);
        let non_existent_cid = create_test_content_id(b"non_existent");

        let result = dag.get_history_from_version(&non_existent_cid);
        assert!(result.is_err());
    }

    #[test]
    fn test_calculate_latest_genesis_only() {
        let storage = MockStorage::new();
        let genesis_cid = create_test_content_id(b"genesis");
        storage.edges.borrow_mut().entry(genesis_cid).or_default();
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);
        let result = dag.calculate_latest(&genesis_cid).unwrap();
        assert_eq!(result, Some(genesis_cid));
    }

    #[test]
    fn test_calculate_latest_linear_history() {
        let mut storage = MockStorage::new();
        let genesis_cid = create_test_content_id(b"genesis");
        let v1_cid = create_test_content_id(b"v1");
        let v2_cid = create_test_content_id(b"v2");
        let v3_cid = create_test_content_id(b"v3");
        storage.setup_graph(&[(genesis_cid, v1_cid), (v1_cid, v2_cid), (v2_cid, v3_cid)]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);
        let result = dag.calculate_latest(&genesis_cid).unwrap();
        assert_eq!(result, Some(v3_cid));
    }

    #[test]
    fn test_calculate_latest_branched_history() {
        let mut storage = MockStorage::new();
        let genesis_cid = create_test_content_id(b"genesis");
        let v1_cid = create_test_content_id(b"v1");
        let v2a_cid = create_test_content_id(b"v2a");
        let v2b_cid = create_test_content_id(b"v2b");
        storage.setup_graph(&[(genesis_cid, v1_cid), (v1_cid, v2a_cid), (v1_cid, v2b_cid)]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);
        let result = dag.calculate_latest(&genesis_cid).unwrap();
        assert!(result == Some(v2a_cid) || result == Some(v2b_cid));
    }

    #[test]
    fn test_calculate_latest_nonexistent_genesis() {
        let storage = MockStorage::new();
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);
        let non_existent_cid = create_test_content_id(b"non_existent");
        let result = dag.calculate_latest(&non_existent_cid).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_all_versions_genesis_only() {
        let storage = MockStorage::new();
        let genesis_cid = create_test_content_id(b"genesis");
        storage.edges.borrow_mut().entry(genesis_cid).or_default();
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);
        let result = dag.get_all_versions_for_genesis(&genesis_cid).unwrap();
        assert_eq!(result, vec![genesis_cid]);
    }

    #[test]
    fn test_get_all_versions_with_children() {
        let mut storage = MockStorage::new();
        let genesis_cid = create_test_content_id(b"genesis");
        let v1_cid = create_test_content_id(b"v1");
        let v2_cid = create_test_content_id(b"v2");
        storage.setup_graph(&[(genesis_cid, v1_cid), (v1_cid, v2_cid)]);
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);
        let mut result = dag.get_all_versions_for_genesis(&genesis_cid).unwrap();
        result.sort();
        let mut expected = vec![genesis_cid, v1_cid, v2_cid];
        expected.sort();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_get_all_versions_excludes_unrelated() {
        let mut storage = MockStorage::new();
        let genesis1_cid = create_test_content_id(b"genesis1");
        let v1_cid = create_test_content_id(b"v1");
        let unrelated_cid = create_test_content_id(b"unrelated");
        storage.setup_graph(&[(genesis1_cid, v1_cid)]);
        storage.edges.borrow_mut().entry(unrelated_cid).or_default();
        let dag = DagGraph::<MockStorage, String, BTreeMap<String, String>>::new(storage);
        let mut result = dag.get_all_versions_for_genesis(&genesis1_cid).unwrap();
        result.sort();
        let mut expected = vec![genesis1_cid, v1_cid];
        expected.sort();
        assert_eq!(result, expected);
        // unrelated_cid should not be included
        assert!(!result.contains(&unrelated_cid));
    }
}
