use leveldb::database::Database;
use leveldb::kv::KV;
use leveldb::options::{Options, WriteOptions, ReadOptions};
use serde::{Deserialize, Serialize};
use crate::crdt::operation::Operation;
use ulid::Ulid;

pub trait OperationStorage<ContentId, T>{
    fn save_operation(&self, op: &Operation<ContentId, T>);
    fn load_operations(&self, content_id: &ContentId) -> Vec<Operation<ContentId, T>>;
    fn get_operation(&self, op_id: &Ulid) -> Option<Operation<ContentId, T>>;
}

pub struct LeveldbStorage<ContentId, T> {
    db: Database<Vec<u8>>,
    _marker: PhantomData<ContentId>,
}

impl<ContentId, T> LeveldbStorage<ContentId, T> {
    pub fn open<P: AsRef<Path>>(path: P) -> Self{
        let mut opts = Options::new();
        opts.create_if_missing(true);
        let db = Database::open(path, opts).unwrap();
        LeveldbStorage {
            db,
            _marker: PhantomData,
        }
    }
}

impl<ContentId, T> OperationStorage<ContentId, T> for LeveldbStorage<ContentId, T> {
    fn save_operation(&self, op: &Operation<ContentId, T>) {}

    fn load_operations(&self, content_id: &ContentId) -> Vec<Operation<ContentId, T>> {}

    fn get_operation(&self, op_id: &Ulid) -> Option<Operation<ContentId, T>> {}
}