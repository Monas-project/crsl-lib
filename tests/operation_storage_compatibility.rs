use crsl_lib::{
    convergence::metadata::ContentMetadata,
    crdt::{
        operation::{Operation, OperationType},
        storage::{LeveldbStorage, OperationStorage},
    },
    storage::SharedLeveldb,
};
use serde::Serialize;
use ulid::Ulid;

// Exact pre-metadata persisted schema. Do not derive this from today's Operation.
#[derive(Serialize)]
struct LegacyOperation {
    id: Ulid,
    genesis: u64,
    kind: OperationType<String>,
    timestamp: u64,
    author: String,
    parents: Vec<u64>,
    node_timestamp: Option<u64>,
}
fn key(id: Ulid) -> Vec<u8> {
    let mut key = vec![1];
    key.extend_from_slice(&id.to_bytes());
    key
}
#[test]
fn reads_real_legacy_bincode_records_alongside_new_records() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharedLeveldb::open(dir.path()).unwrap();
    let storage = LeveldbStorage::<u64, String>::new(db.clone());
    let id = Ulid::new();
    let old = LegacyOperation {
        id,
        genesis: 42,
        kind: OperationType::Create("old".into()),
        timestamp: 9,
        author: "old-client".into(),
        parents: vec![],
        node_timestamp: Some(9),
    };
    let bytes = bincode::serde::encode_to_vec(&old, bincode::config::standard()).unwrap();
    db.db().put(&key(id), &bytes).unwrap();
    let old_read = storage.get_operation(&id).unwrap().unwrap();
    assert_eq!(old_read.node_metadata, None);
    assert_eq!(old_read.node_timestamp, Some(9));
    assert_eq!(old_read.kind, old.kind);
    let mut new = Operation::new(42, OperationType::Update("new".into()), "new-client".into());
    new.node_metadata = Some(ContentMetadata::with_policy("custom-v1"));
    storage.save_operation(&new).unwrap();
    assert_eq!(storage.get_operation(&new.id).unwrap(), Some(new.clone()));
    let all = storage.load_operations(&42).unwrap();
    assert_eq!(all.len(), 2);
    assert!(all.contains(&old_read));
    assert!(all.contains(&new));
}
#[test]
fn corrupt_or_trailing_operation_bytes_are_errors_not_dropped_or_downgraded() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharedLeveldb::open(dir.path()).unwrap();
    let storage = LeveldbStorage::<u64, String>::new(db.clone());
    let mut op = Operation::new(42, OperationType::Create("new".into()), "local".into());
    op.node_metadata = Some(ContentMetadata::with_policy("custom-v1"));
    storage.save_operation(&op).unwrap();
    let bytes = db.db().get(&key(op.id)).unwrap();
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    for corrupted in [bytes[..bytes.len() - 1].to_vec(), trailing, vec![255]] {
        db.db().put(&key(op.id), &corrupted).unwrap();
        assert!(storage.get_operation(&op.id).is_err());
        assert!(storage.load_operations(&42).is_err());
    }
}
