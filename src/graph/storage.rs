use crate::dasl::node::Node;
use cid::Cid;
use rusty_leveldb::{LdbIterator, Options, DB as Database};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

// todo: error handling
pub trait NodeStorage<P, M> {
    fn get(&self, content_id: &Cid) -> Option<Node<P, M>>;
    fn put(&self, node: &Node<P, M>);
    fn delete(&self, content_id: &Cid);
    fn get_node_map(&self) -> HashMap<Cid, Vec<Cid>>;
}

pub struct LeveldbNodeStorage<P, M> {
    db: RefCell<Database>,
    path: PathBuf,
    _marker: std::marker::PhantomData<(P, M)>,
}

impl<P, M> Clone for LeveldbNodeStorage<P, M> {
    fn clone(&self) -> Self {
        let opts = Options {
            create_if_missing: true,
            ..Default::default()
        };
        let db = Database::open(&self.path, opts).unwrap();
        Self {
            db: RefCell::new(db),
            path: self.path.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<P, M> LeveldbNodeStorage<P, M> {
    pub fn open<Pth: AsRef<Path>>(path: Pth) -> Self {
        let opts = Options {
            create_if_missing: true,
            ..Default::default()
        };
        let db = Database::open(path.as_ref(), opts).unwrap();
        Self {
            db: RefCell::new(db),
            path: path.as_ref().to_path_buf(),
            _marker: std::marker::PhantomData,
        }
    }
    fn make_key(cid: &Cid) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + cid.to_bytes().len());
        v.push(0x10);
        v.extend_from_slice(&cid.to_bytes());
        v
    }
}

impl<P, M> NodeStorage<P, M> for LeveldbNodeStorage<P, M>
where
    P: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone,
    M: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone,
{
    fn get(&self, cid: &Cid) -> Option<Node<P, M>> {
        let key = Self::make_key(cid);
        self.db
            .borrow_mut()
            .get(&key)
            .map(|raw| Node::from_bytes(&raw).unwrap())
    }

    fn put(&self, node: &Node<P, M>) {
        let bytes = node.to_bytes().unwrap();
        let key = Self::make_key(&node.content_id().unwrap());
        self.db.borrow_mut().put(&key, &bytes).unwrap();
    }

    fn delete(&self, cid: &Cid) {
        let key = Self::make_key(cid);
        self.db.borrow_mut().delete(&key).unwrap();
    }

    // todo: implement get_node_map
    fn get_node_map(&self) -> HashMap<Cid, Vec<Cid>> {
        let mut node_map = HashMap::new();
        let mut iter = self.db.borrow_mut().new_iter().unwrap();
        iter.seek_to_first();
        let mut key = Vec::new();
        let mut value = Vec::new();

        while iter.valid() {
            iter.current(&mut key, &mut value);
            if key[0] == 0x10 {
                // Check if it's a node key
                if let Ok((node, _)) = bincode::serde::decode_from_slice::<Node<P, M>, _>(
                    &value,
                    bincode::config::standard(),
                ) {
                    node_map.insert(node.content_id().unwrap(), node.parents().to_vec());
                }
            }
            iter.advance();
        }

        node_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dasl::node::Node;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;

    fn create_test_node(payload: &str) -> Node<String, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Node::new(
            payload.to_string(),
            vec![],
            timestamp,
            "metadata".to_string(),
        )
    }

    #[test]
    fn test_put_and_get() {
        let temp_dir = tempdir().unwrap();
        let storage = LeveldbNodeStorage::<String, String>::open(temp_dir.path());

        let node = create_test_node("test-payload");
        let cid = node.content_id().unwrap();

        storage.put(&node);
        let retrieved_node = storage.get(&cid);
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
        storage.put(&node);
        assert!(storage.get(&cid).is_some());

        storage.delete(&cid);

        assert!(storage.get(&cid).is_none());
    }

    #[test]
    fn test_multiple_nodes() {
        let temp_dir = tempdir().unwrap();
        let storage = LeveldbNodeStorage::<String, String>::open(temp_dir.path());
        let node1 = create_test_node("payload-1");
        let node2 = create_test_node("payload-2");
        let node3 = create_test_node("payload-3");

        storage.put(&node1);
        storage.put(&node2);
        storage.put(&node3);

        assert!(storage.get(&node1.content_id().unwrap()).is_some());
        assert!(storage.get(&node2.content_id().unwrap()).is_some());
        assert!(storage.get(&node3.content_id().unwrap()).is_some());

        assert_eq!(
            storage.get(&node1.content_id().unwrap()).unwrap().payload(),
            "payload-1"
        );
        assert_eq!(
            storage.get(&node2.content_id().unwrap()).unwrap().payload(),
            "payload-2"
        );
        assert_eq!(
            storage.get(&node3.content_id().unwrap()).unwrap().payload(),
            "payload-3"
        );
    }

    #[test]
    fn test_nonexistent_node() {
        let temp_dir = tempdir().unwrap();
        let storage = LeveldbNodeStorage::<String, String>::open(temp_dir.path());

        let node = create_test_node("nonexistent");
        let cid = node.content_id().unwrap();

        assert!(storage.get(&cid).is_none());
    }
}
