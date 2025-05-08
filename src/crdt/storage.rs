use rusty_leveldb::{LdbIterator, Options, DB as Database};
use serde::{Deserialize, Serialize};
use crate::crdt::operation::Operation;
use ulid::Ulid;
use std::path::Path;
use std::marker::PhantomData;
use std::cell::RefCell;
use bincode;



pub trait OperationStorage<ContentId, T>{
    fn save_operation(&self, op: &Operation<ContentId, T>);
    fn load_operations(&self, content_id: &ContentId) -> Vec<Operation<ContentId, T>>;
    fn get_operation(&self, op_id: &Ulid) -> Option<Operation<ContentId, T>>;
}

pub struct LeveldbStorage<ContentId, T> {
    db: RefCell<Database>,
    _marker: PhantomData<(ContentId, T)>,
}

impl<ContentId, T> LeveldbStorage<ContentId, T> {
    pub fn open<P: AsRef<Path>>(path: P) -> Self{
        let mut opts = Options::default();
        opts.create_if_missing = true;
        let db = Database::open(path, opts).unwrap();
        LeveldbStorage {
            db: RefCell::new(db),
            _marker: PhantomData,
        }
    }

    fn make_key(id: &Ulid) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 +16);
        key.push(0x01);
        key.extend_from_slice(id.to_bytes().as_ref());
        key
    }
}

impl<ContentId, T> OperationStorage<ContentId, T> for LeveldbStorage<ContentId, T> 
where
    ContentId: serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq,
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    fn save_operation(&self, op: &Operation<ContentId, T>) {
        let key = Self::make_key(&op.id);
        let value = bincode::serde::encode_to_vec(op, bincode::config::standard()).unwrap();
        self.db.borrow_mut().put(&key, &value).unwrap();
    }

    fn load_operations(&self, content_id: &ContentId) -> Vec<Operation<ContentId, T>> {
        let mut result = Vec::new();
        let mut iter = self.db.borrow_mut().new_iter().unwrap();
        let mut key = Vec::new();
        let mut value = Vec::new();

        while iter.valid() {
            iter.current(&mut key, &mut value);
            if let Ok((op, _)) = bincode::serde::decode_from_slice::<Operation<ContentId, T>, _>(&value, bincode::config::standard()) {
                if op.target == *content_id {
                    result.push(op);
                }
            }
            iter.advance();
        }

        result
    }

    fn get_operation(&self, op_id: &Ulid) -> Option<Operation<ContentId, T>> {
        let key = Self::make_key(op_id);
        self.db.borrow_mut().get(&key)
        .and_then(|raw| bincode::serde::decode_from_slice::<Operation<ContentId, T>, _>(&raw, bincode::config::standard()).ok().map(|(op, _)| op))
    }
}