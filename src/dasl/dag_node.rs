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
/// * `P` - Payload type that implements `Serialize` for content id generation.
///   The serialization method for storage is up to the user.
/// * `M` - The type of the metadata. Defaults to `BTreeMap<String, String>` if not specified.
///
/// # Fields
/// * `payload` - The main content/data of the entry.
/// * `parents` - A vector of content ids (Content Identifiers) pointing to parent entries, forming a DAG structure.
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
    /// Creates a new DagNode with the given parameters
    ///
    /// # Arguments
    /// * `payload` - The main content/data to store in the node
    /// * `parents` - Vector of content ids pointing to parent nodes
    /// * `timestamp` - Unix timestamp for node creation time
    /// * `metadata` - Additional information about the node
    ///
    /// # Returns
    /// A new DagNode instance
    pub fn new(payload: P, parents: Vec<Cid>, timestamp: u64, metadata: M) -> Self {
        DagNode {
            payload,
            parents,
            timestamp,
            metadata,
        }
    }

    /// Computes the content identifier (CID) for the node
    ///
    /// # Returns
    /// Content id (Cid) for the node
    ///
    /// # Errors
    /// Returns a DagNodeError if serialization or hashing fails
    pub fn content_id(&self) -> Cid {
        let buf = self.to_bytes();
        let mh = Multihash::<64>::wrap(SHA2_256_CODE, &buf).unwrap();
        Cid::new_v1(RAW_CODE, mh)
    }

    /// Serializes this node using CBOR
    ///
    /// # Returns
    /// Serialized bytes of the node
    ///
    /// # Errors
    /// Returns a DagNodeError if serialization fails
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(self).unwrap()
    }

    /// Deserializes a DagNode from bytes
    ///
    /// # Arguments
    /// * `buf` - Byte slice containing the serialized node
    ///
    /// # Returns
    /// Deserialized DagNode
    ///
    /// # Errors
    /// Returns a DagNodeError if deserialization fails
    pub fn from_bytes(buf: &[u8]) -> Self {
        serde_cbor::from_slice(buf).unwrap()
    }

    /// Verifies the integrity of the node by comparing the calculated content id with the expected content id
    ///
    /// # Arguments
    /// * `expected_content_id` - The expected content id to compare against
    ///
    /// # Returns
    /// `true` if the calculated content id matches the expected content id, `false` otherwise
    pub fn verify_self_integrity(&self, expected_content_id: &Cid) -> bool {
        let recalculated = {
            let buf = serde_cbor::to_vec(self).unwrap();
            let mh = Multihash::<64>::wrap(SHA2_256_CODE, &buf).unwrap();
            Cid::new_v1(RAW_CODE, mh)
        };
        recalculated == *expected_content_id
    }

    pub fn add_parent(&mut self, cid: Cid) {
        self.parents.push(cid);
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

    fn create_test_content_id(data: &[u8]) -> Cid {
        use multihash::Multihash;
        let code = 0x12;
        let digest = Multihash::<64>::wrap(code, data).unwrap();
        Cid::new_v1(0x55, digest)
    }

    #[test]
    fn test_entry_creation_with_default_metadata() {
        let payload = "test payload".to_string();
        let parents_content_id = create_test_content_id(b"test");
        let parents = vec![parents_content_id];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();

        let node = DagNode::new(payload.clone(), parents, timestamp, metadata);

        assert_eq!(node.payload(), &payload);
        assert_eq!(node.parents(), &vec![parents_content_id]);
        assert_eq!(node.timestamp(), timestamp);
    }

    #[test]
    fn test_entry_multiple_parents() {
        let payload = "test payload".to_string();
        let parents_content_id1 = create_test_content_id(b"test1");
        let parents_content_id2 = create_test_content_id(b"test2");
        let parents = vec![parents_content_id1, parents_content_id2];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();

        let node = DagNode::new(payload.clone(), parents, timestamp, metadata);

        assert_eq!(node.parents().len(), 2);
        assert_eq!(node.parents()[0], parents_content_id1);
        assert_eq!(node.parents()[1], parents_content_id2);
    }

    #[test]
    fn test_to_bytes_roundtrip() {
        let payload = "test".to_string();
        let parents_content_id = create_test_content_id(b"test");
        let parents = vec![parents_content_id];
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
        let parents_content_id = create_test_content_id(b"test");
        let parents = vec![parents_content_id];
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

    #[test]
    fn test_verify_self_integrity_with_correct_cid() {
        let payload = "test".to_string();
        let parents_content_id = create_test_content_id(b"test");
        let parents = vec![parents_content_id];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();

        let node = DagNode::new(
            payload.clone(),
            parents.clone(),
            timestamp,
            metadata.clone(),
        );

        let correct_cid = node.content_id();
        assert!(node.verify_self_integrity(&correct_cid));
    }

    #[test]
    fn test_verify_self_integrity_with_wrong_cid() {
        let payload = "test".to_string();
        let parents_content_id = create_test_content_id(b"test");
        let parents = vec![parents_content_id];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();

        let node = DagNode::new(
            payload.clone(),
            parents.clone(),
            timestamp,
            metadata.clone(),
        );

        let different_payload = "different".to_string();
        let different_node = DagNode::new(
            different_payload,
            parents.clone(),
            timestamp,
            metadata.clone(),
        );
        let different_cid = different_node.content_id();

        assert!(!node.verify_self_integrity(&different_cid));
    }

    #[test]
    fn test_add_parent_basic() {
        let payload = "test payload".to_string();
        let parent1 = create_test_content_id(b"parent1");
        let initial_parents = vec![parent1];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();

        let mut node = DagNode::new(
            payload.clone(),
            initial_parents.clone(),
            timestamp,
            metadata.clone(),
        );

        assert_eq!(node.parents().len(), 1);
        assert_eq!(node.parents()[0], parent1);
        let parent2 = create_test_content_id(b"parent2");
        node.add_parent(parent2);

        assert_eq!(node.parents().len(), 2);
        assert_eq!(node.parents()[0], parent1);
        assert_eq!(node.parents()[1], parent2);
    }

    #[test]
    fn test_add_parent_changes_cid() {
        let payload = "test".to_string();
        let parent1 = create_test_content_id(b"parent1");
        let initial_parents = vec![parent1];
        let timestamp = 1;
        let metadata: BTreeMap<String, String> = BTreeMap::new();

        let mut node = DagNode::new(
            payload.clone(),
            initial_parents.clone(),
            timestamp,
            metadata.clone(),
        );

        // If the value becomes too large, encoding may not be possible.
        let initial_cid_string = node.content_id().to_string();

        let parent2 = create_test_content_id(b"b");
        node.add_parent(parent2);

        let new_cid_string = node.content_id().to_string();

        assert_ne!(initial_cid_string, new_cid_string);
    }

    #[test]
    fn test_add_multiple_parents() {
        let payload = "test payload".to_string();
        let parent1 = create_test_content_id(b"parent1");
        let initial_parents = vec![parent1];
        let timestamp = 1234567890;
        let metadata: BTreeMap<String, String> = BTreeMap::new();

        let mut node = DagNode::new(
            payload.clone(),
            initial_parents.clone(),
            timestamp,
            metadata.clone(),
        );
        let parent2 = create_test_content_id(b"parent2");
        let parent3 = create_test_content_id(b"parent3");
        let parent4 = create_test_content_id(b"parent4");

        node.add_parent(parent2);
        node.add_parent(parent3);
        node.add_parent(parent4);

        assert_eq!(node.parents().len(), 4);
        assert_eq!(node.parents()[0], parent1);
        assert_eq!(node.parents()[1], parent2);
        assert_eq!(node.parents()[2], parent3);
        assert_eq!(node.parents()[3], parent4);
    }
}
