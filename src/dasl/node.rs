use cid::Cid;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A generic entry structure that represents a node in a directed acyclic graph (DAG).
/// This structure can store any type of payload data and metadata, making it versatile for various use cases.
///
/// # Type Parameters
/// * `P` - Payload type that implements `Serialize` for CID generation.
///         The serialization method for storage is up to the user.
/// * `M` - The type of the metadata. Defaults to `BTreeMap<String, String>` if not specified.
///
/// # Fields
/// * `payload` - The main content/data of the entry.
/// * `parents` - A vector of CIDs (Content Identifiers) pointing to parent entries, forming a DAG structure.
/// * `timestamp` - Unix timestamp representing when the entry was created.
/// * `metadata` - Additional information about the entry (e.g., author, tags, or other attributes).

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "P: Serialize + for<'a> Deserialize<'a>, M: Serialize + for<'a> Deserialize<'a>")]
pub struct DagNode<P, M = BTreeMap<String, String>> {
    pub payload: P,
    pub parents: Vec<Cid>,
    pub timestamp: u64,
    pub metadata: M,
}
impl<P, M> DagNode<P, M> {
    pub fn new(payload: P, parents: Vec<Cid>, timestamp: u64, metadata: M) -> Self {
        DagNode { payload, parents, timestamp, metadata }
    }

    pub fn content_id(&self) -> Cid {
        // todo: implement content id generation
        // memo: Serialize this node itself as dCBOR → SHA2-256 → convert to Cid(v1)
        unimplemented!()
    }
    pub fn to_bytes(&self) -> Vec<u8> { 
        // todo: implement serialization
        unimplemented!()
    }
    pub fn from_bytes(_buf: &[u8]) -> Self { 
        // todo: implement deserialization
        unimplemented!()
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
        // todo: verify payload
        return true;
    }
    pub fn verify_parents(&self) -> bool {
        // todo: verify parents
        return true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn create_test_cid(data: &[u8]) -> Cid {
        use multihash::Multihash;
        let code = 0x12;
        let digest = Multihash::<64>::wrap(code, data).unwrap();
        Cid::new_v1(0x55, digest)
    }

    #[test]
    fn test_entry_creation_with_default_metadata() {
        let payload = "test payload";
        let parents_cid = create_test_cid(b"test");
        let parents = vec![parents_cid];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();
        let entry = DagNode::new(payload, parents, timestamp, metadata);
        assert_eq!(entry.payload(), &payload);
        assert_eq!(entry.parents(), &vec![parents_cid]);
        assert_eq!(entry.timestamp(), timestamp);
    }

    #[test]
    fn test_entry_with_custom_metadata() {
        let payload = "test payload";
        let parents_cid = create_test_cid(b"test");
        let parents = vec![parents_cid];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();
        let entry = DagNode::new(payload, parents, timestamp, metadata);
        assert!(entry.verify());
    }

    #[test]
    fn test_entry_multiple_parents() {
        let payload = "test payload";
        let parents_cid1 = create_test_cid(b"test1");
        let parents_cid2 = create_test_cid(b"test2");
        let parents = vec![parents_cid1, parents_cid2];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();
        let entry = DagNode::new(payload, parents, timestamp, metadata);
        assert_eq!(entry.parents().len(), 2);
        assert_eq!(entry.parents()[0], parents_cid1);
        assert_eq!(entry.parents()[1], parents_cid2);
    }

    #[test]
    fn test_entry_verify() {
        let payload = "test payload";
        let parents_cid = create_test_cid(b"test");
        let parents = vec![parents_cid];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();
        let entry = DagNode::new(payload, parents, timestamp, metadata);
        assert!(entry.verify());
    }

    #[test]
    fn test_entry_verify_parents() {
        let payload = "test payload";
        let parents_cid = create_test_cid(b"test");
        let parents = vec![parents_cid];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();
        let entry = DagNode::new(payload, parents, timestamp, metadata);
        assert!(entry.verify_parents());
    }
    
}
