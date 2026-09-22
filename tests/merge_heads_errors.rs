use cid::Cid;
use crsl_lib::{
    convergence::metadata::ContentMetadata,
    crdt::{
        crdt_state::CrdtState,
        operation::{Operation, OperationType},
        storage::LeveldbStorage,
    },
    dasl::node::Node,
    graph::{
        dag::DagGraph,
        error::{GraphError, Result},
        storage::{LeveldbNodeStorage, NodeStorage},
    },
    repo::Repo,
    storage::{SharedLeveldb, SharedLeveldbAccess},
};
use rusty_leveldb::{Status, StatusCode};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

struct ReadFault {
    inner: LeveldbNodeStorage<String, ContentMetadata>,
    calls: AtomicUsize,
    fail_at: AtomicUsize,
}
impl ReadFault {
    fn arm(&self, fail_at: usize) {
        self.calls.store(0, Ordering::SeqCst);
        self.fail_at.store(fail_at, Ordering::SeqCst);
    }
}
impl SharedLeveldbAccess for ReadFault {
    fn shared_leveldb(&self) -> Option<Arc<SharedLeveldb>> {
        self.inner.shared_leveldb()
    }
}
impl NodeStorage<String, ContentMetadata> for ReadFault {
    fn get(&self, cid: &Cid) -> Result<Option<Node<String, ContentMetadata>>> {
        self.inner.get(cid)
    }
    fn put(&self, node: &Node<String, ContentMetadata>) -> Result<()> {
        self.inner.put(node)
    }
    fn delete(&self, cid: &Cid) -> Result<()> {
        self.inner.delete(cid)
    }
    fn get_node_map(&self) -> Result<HashMap<Cid, Vec<Cid>>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.fail_at.load(Ordering::SeqCst) {
            return Err(GraphError::Storage(Status::new(
                StatusCode::IOError,
                "review read failure",
            )));
        }
        let map = self.inner.get_node_map()?;
        Ok(map)
    }
}

#[test]
fn merge_heads_propagates_read_errors() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedLeveldb::open(dir.path().join("store")).unwrap();
    let state = CrdtState::new(LeveldbStorage::new(shared.clone()));
    let storage = ReadFault {
        inner: LeveldbNodeStorage::new(shared),
        calls: AtomicUsize::new(0),
        fail_at: AtomicUsize::new(0),
    };
    let mut repo = Repo::new(state, DagGraph::new(storage));
    let seed = Cid::new_v1(
        0x55,
        multihash::Multihash::<64>::wrap(0x12, b"review").unwrap(),
    );
    let genesis = repo
        .commit_operation(Operation::new(
            seed,
            OperationType::Create("persisted".to_owned()),
            "review".into(),
        ))
        .unwrap();
    assert_eq!(repo.heads(&genesis).unwrap(), vec![genesis]);
    assert_eq!(repo.merge_heads(&genesis).unwrap(), Some(genesis));

    repo.dag.storage.arm(1);
    let first = repo.merge_heads(&genesis);
    assert!(matches!(
        first,
        Err(crsl_lib::crdt::error::CrdtError::Graph(
            GraphError::Storage(_)
        ))
    ));

    repo.dag.storage.arm(2);
    let result = repo.merge_heads(&genesis);
    assert_eq!(repo.dag.storage.calls.load(Ordering::SeqCst), 2);
    assert!(
        matches!(
            result,
            Err(crsl_lib::crdt::error::CrdtError::Graph(
                GraphError::Storage(_)
            ))
        ),
        "read failure must propagate: {result:?}"
    );

    repo.dag.storage.arm(0);
    assert_eq!(repo.heads(&genesis).unwrap(), vec![genesis]);
    assert_eq!(repo.merge_heads(&genesis).unwrap(), Some(genesis));
}
