use crate::dasl::node::Node;
use crate::graph::error::{GraphError, Result};
use crate::storage::{SharedLeveldb, SharedLeveldbAccess};
use cid::Cid;
use rusty_leveldb::LdbIterator;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Minimal interface required for persisting DAG nodes.
pub trait NodeStorage<P, M>: Send + Sync {
    fn get(&self, content_id: &Cid) -> Result<Option<Node<P, M>>>;
    fn put(&self, node: &Node<P, M>) -> Result<()>;
    fn delete(&self, content_id: &Cid) -> Result<()>;
    fn get_node_map(&self) -> Result<HashMap<Cid, Vec<Cid>>>;
}

/// [`NodeStorage`] implementation backed by a shared LevelDB instance.
pub struct LeveldbNodeStorage<P, M> {
    shared: Arc<SharedLeveldb>,
    _marker: std::marker::PhantomData<(P, M)>,
}

impl<P, M> Clone for LeveldbNodeStorage<P, M> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<P, M> LeveldbNodeStorage<P, M> {
    /// Opens LevelDB and wraps it in a shared handle.
    pub fn open<Pth: AsRef<Path>>(path: Pth) -> Self {
        let shared = SharedLeveldb::open(path).expect("Failed to open LevelDB");
        Self::new(shared)
    }

    /// Creates the storage from an existing [`SharedLeveldb`] handle.
    pub fn new(shared: Arc<SharedLeveldb>) -> Self {
        Self {
            shared,
            _marker: std::marker::PhantomData,
        }
    }

    /// Builds the LevelDB key for nodes, prefixed with the `0x10` namespace.
    fn make_key(cid: &Cid) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + cid.to_bytes().len());
        v.push(0x10);
        v.extend_from_slice(&cid.to_bytes());
        v
    }

    /// Writes either into the active batch, or directly into the DB if no batch is active.
    fn write_bytes(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self
            .shared
            .with_active_batch(|batch| batch.put(key, value))
            .is_none()
        {
            self.shared
                .db()
                .put(key, value)
                .map_err(GraphError::Storage)?;
        }
        Ok(())
    }

    /// Deletes the given key, falling back to the DB when no batch is active.
    fn delete_key(&self, key: &[u8]) -> Result<()> {
        if self
            .shared
            .with_active_batch(|batch| batch.delete(key))
            .is_none()
        {
            self.shared
                .db()
                .delete(key)
                .map_err(GraphError::Storage)?;
        }
        Ok(())
    }
}

impl<P, M> SharedLeveldbAccess for LeveldbNodeStorage<P, M> {
    fn shared_leveldb(&self) -> Option<Arc<SharedLeveldb>> {
        Some(self.shared.clone())
    }
}

impl<P, M> NodeStorage<P, M> for LeveldbNodeStorage<P, M>
where
    P: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Send + Sync,
    M: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Send + Sync,
{
    fn get(&self, cid: &Cid) -> Result<Option<Node<P, M>>> {
        let key = Self::make_key(cid);
        match self.shared.db().get(&key) {
            Some(raw) => {
                let node =
                    Node::from_bytes(&raw).map_err(|e| GraphError::NodeOperation(e.to_string()))?;
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    fn put(&self, node: &Node<P, M>) -> Result<()> {
        let bytes = node
            .to_bytes()
            .map_err(|e| GraphError::NodeOperation(e.to_string()))?;
        let cid = node
            .content_id()
            .map_err(|e| GraphError::NodeOperation(e.to_string()))?;
        let key = Self::make_key(&cid);
        self.write_bytes(&key, &bytes)
    }

    fn delete(&self, cid: &Cid) -> Result<()> {
        let key = Self::make_key(cid);
        self.delete_key(&key)
    }

    /// Walks all nodes and constructs an adjacency map (parent → children).
    fn get_node_map(&self) -> Result<HashMap<Cid, Vec<Cid>>> {
        let mut node_map = HashMap::new();
        let mut iter = self
            .shared
            .db()
            .new_iter()
            .map_err(GraphError::Storage)?;
        iter.seek_to_first();
        let mut key = Vec::new();
        let mut value = Vec::new();

        while iter.valid() {
            iter.current(&mut key, &mut value);
            if !key.is_empty() && key[0] == 0x10 {
                let node = Node::<P, M>::from_bytes(&value)
                    .map_err(|e| GraphError::NodeOperation(e.to_string()))?;
                let node_cid = node
                    .content_id()
                    .map_err(|e| GraphError::NodeOperation(e.to_string()))?;
                node_map.insert(node_cid, node.parents().to_vec());
            }
            iter.advance();
        }
        Ok(node_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dasl::node::Node;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;

    /// Creates a simple test node helper.
    fn create_test_node(payload: &str) -> Node<String, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Node::new_genesis(payload.to_string(), timestamp, "metadata".to_string())
    }

    #[test]
    fn test_put_and_get() {
        let temp_dir = tempdir().unwrap();
        let storage = LeveldbNodeStorage::<String, String>::open(temp_dir.path());

        let node = create_test_node("test-payload");
        let cid = node.content_id().unwrap();

        storage.put(&node).unwrap();
        let retrieved_node = storage.get(&cid).unwrap();
        assert!(retrieved_node.is_some());

        let retrieved_node = retrieved_node.unwrap();
        assert_eq!(
            retrieved_node.content_id().unwrap(),
            node.content_id().unwrap()
        );
        assert_eq!(retrieved_node.payload(), node.payload());
        assert_eq!(retrieved_node.metadata(), node.metadata());
    }

    #[test]
    fn test_delete() {
        let temp_dir = tempdir().unwrap();
        let storage = LeveldbNodeStorage::<String, String>::open(temp_dir.path());

        let node = create_test_node("delete-test");
        let cid = node.content_id().unwrap();
        storage.put(&node).unwrap();
        assert!(storage.get(&cid).unwrap().is_some());

        storage.delete(&cid).unwrap();

        assert!(storage.get(&cid).unwrap().is_none());
    }

    #[test]
    fn test_multiple_nodes() {
        let temp_dir = tempdir().unwrap();
        let storage = LeveldbNodeStorage::<String, String>::open(temp_dir.path());
        let node1 = create_test_node("payload-1");
        let node2 = create_test_node("payload-2");
        let node3 = create_test_node("payload-3");

        storage.put(&node1).unwrap();
        storage.put(&node2).unwrap();
        storage.put(&node3).unwrap();

        assert!(storage.get(&node1.content_id().unwrap()).unwrap().is_some());
        assert!(storage.get(&node2.content_id().unwrap()).unwrap().is_some());
        assert!(storage.get(&node3.content_id().unwrap()).unwrap().is_some());

        assert_eq!(
            storage
                .get(&node1.content_id().unwrap())
                .unwrap()
                .unwrap()
                .payload(),
            "payload-1"
        );
        assert_eq!(
            storage
                .get(&node2.content_id().unwrap())
                .unwrap()
                .unwrap()
                .payload(),
            "payload-2"
        );
        assert_eq!(
            storage
                .get(&node3.content_id().unwrap())
                .unwrap()
                .unwrap()
                .payload(),
            "payload-3"
        );
    }

    #[test]
    fn test_nonexistent_node() {
        let temp_dir = tempdir().unwrap();
        let storage = LeveldbNodeStorage::<String, String>::open(temp_dir.path());

        let node = create_test_node("nonexistent");
        let cid = node.content_id().unwrap();

        assert!(storage.get(&cid).unwrap().is_none());
    }
}
