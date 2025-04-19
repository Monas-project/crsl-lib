use cid::Cid;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "P: Serialize + for<'a> Deserialize<'a>, M: Serialize + for<'a> Deserialize<'a>")]
pub struct Entry<P, M = BTreeMap<String, String>> {
    pub payload: P,
    pub parents: Vec<Cid>,
    pub timestamp: u64,
    pub metadata: M,
}
impl<P, M> Entry<P, M> {
    pub fn new(payload: P, parents: Vec<Cid>, timestamp: u64, metadata: M) -> Self {
        Entry { payload, parents, timestamp, metadata }
    }
    pub fn payload(&self) -> &P {
        &self.payload
    }
    pub fn parents(&self) -> &Vec<Cid> {
        &self.parents
    }
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
    pub fn metadata(&self) -> &M {
        &self.metadata
    }
    pub fn verify(&self) -> bool {
        return true;
    }
    pub fn verify_parent(&self) -> bool {
        return true;
    }
}
