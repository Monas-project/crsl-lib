use cid::Cid;
use multihash::Multihash;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// For more details on these multicodec codes, see:
/// https://github.com/multiformats/multicodec/blob/master/table.csv
const SHA2_256_CODE: u64 = 0x12;
const RAW_CODE: u64 = 0x55;

/// A generic entry structure that represents a node in a directed acyclic graph (DAG).
/// This structure can store any type of payload data and metadata, making it versatile for various use cases.
///
/// # Type Parameters
/// * `P` - Payload type that implements `Serialize` for CID generation.
///   The serialization method for storage is up to the user.
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
impl<P, M> DagNode<P, M>
where
    P: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
{
    pub fn new(payload: P, parents: Vec<Cid>, timestamp: u64, metadata: M) -> Self {
        DagNode {
            payload,
            parents,
            timestamp,
            metadata,
        }
    }

    pub fn content_id(&self) -> Cid {
        let buf = self.to_bytes();
        let mh = Multihash::<64>::wrap(SHA2_256_CODE, &buf).unwrap();
        Cid::new_v1(RAW_CODE, mh)
    }

    /// Serialize this node itself as CBOR → SHA2-256 → convert to Cid(v1)
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(&self).unwrap()
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        serde_cbor::from_slice(buf).unwrap()
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
        let payload = "test payload".to_string();
        let parents_cid = create_test_cid(b"test");
        let parents = vec![parents_cid];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();
        let node = DagNode::new(payload.clone(), parents, timestamp, metadata);
        assert_eq!(node.payload(), &payload);
        assert_eq!(node.parents(), &vec![parents_cid]);
        assert_eq!(node.timestamp(), timestamp);
    }

    #[test]
    fn test_entry_multiple_parents() {
        let payload = "test payload".to_string();
        let parents_cid1 = create_test_cid(b"test1");
        let parents_cid2 = create_test_cid(b"test2");
        let parents = vec![parents_cid1, parents_cid2];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();

        let node = DagNode::new(payload.clone(), parents, timestamp, metadata);

        assert_eq!(node.parents().len(), 2);
        assert_eq!(node.parents()[0], parents_cid1);
        assert_eq!(node.parents()[1], parents_cid2);
    }

    #[test]
    fn test_to_bytes_roundtrip() {
        let payload = "test".to_string();
        let parents_cid = create_test_cid(b"test");
        let parents = vec![parents_cid];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();

        let node = DagNode::new(
            payload.clone(),
            parents.clone(),
            timestamp,
            metadata.clone(),
        );
        let bytes = node.to_bytes();
        let node2: DagNode<String, BTreeMap<String, String>> = DagNode::from_bytes(&bytes);

        assert_eq!(node2.payload(), &payload);
        assert_eq!(node2.parents(), &parents);
        assert_eq!(node2.timestamp(), timestamp);
        assert_eq!(node2.metadata(), &metadata);
    }

    #[test]
    fn test_content_id() {
        let payload = "test".to_string();
        let parents_cid = create_test_cid(b"test");
        let parents = vec![parents_cid];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();
        let node1 = DagNode::new(
            payload.clone(),
            parents.clone(),
            timestamp,
            metadata.clone(),
        );
        let node2 = DagNode::new(
            payload.clone(),
            parents.clone(),
            timestamp,
            metadata.clone(),
        );
        let content_id1 = node1.content_id();
        let content_id2 = node2.content_id();
        assert_eq!(content_id1.to_string(), content_id2.to_string());
    }
}
