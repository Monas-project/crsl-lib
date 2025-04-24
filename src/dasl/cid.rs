use cid::Cid;
use multibase::Base;
use multihash::Multihash;
use std::fmt;

/// For more details on these multicodec codes, see:
/// https://github.com/multiformats/multicodec/blob/master/table.csv
const SHA2_256_CODE: u64 = 0x12;
const RAW_CODE: u64 = 0x55;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContentId(pub Cid);

impl ContentId {
    /// Creates a new `ContentId` by generating a CID.
    ///
    /// This function takes a byte slice as input, hashes it using the SHA2-256 algorithm
    /// via the `Multihash` library, and then creates a CID using the resulting hash.
    ///
    /// # Arguments
    ///
    /// * `data` - A byte slice representing the data to be hashed and included in the CID.
    ///
    /// # Returns
    ///
    /// A new `ContentId` instance containing the generated CID.
    pub fn new(data: &[u8]) -> Self {
        let code = SHA2_256_CODE;
        let digest = Multihash::<64>::wrap(code, data).unwrap();
        let cid = Cid::new_v1(RAW_CODE, digest);
        ContentId(cid)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    pub fn to_base(&self, base: Base) -> String {
        self.0.to_string_of_base(base).unwrap()
    }

    pub fn verify(&self, data: &[u8]) -> bool {
        let expected = ContentId::new(data);
        self == &expected
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let cid = Cid::try_from(bytes).unwrap();
        ContentId(cid)
    }

    /// Creates a `ContentId` from a string.
    /// The default base is base32
    pub fn from_string(s: &str) -> Self {
        let cid = Cid::try_from(s).unwrap();
        ContentId(cid)
    }

    /// Creates a `ContentId` from a custom base-encoded string.
    pub fn from_base(encoded: &str, base: Base) -> Self {
        let (decoded_base, decoded_bytes) = multibase::decode(encoded).unwrap();
        assert_eq!(
            decoded_base, base,
            "Base encoding does not match the expected base"
        );
        let cid = Cid::try_from(decoded_bytes.as_slice()).unwrap();
        ContentId(cid)
    }
}

impl fmt::Display for ContentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use multibase::Base;

    #[test]
    fn test_default_cid_creation() {
        let data = b"test data";
        let content_id = ContentId::new(data);
        assert_eq!(content_id.to_string(), content_id.0.to_string());
    }

    #[test]
    fn test_base64_cid_creation() {
        let data = b"test data";
        let content_id = ContentId::new(data);
        let base64_cid = content_id.to_base(Base::Base64);
        assert!(!base64_cid.is_empty());
    }

    #[test]
    fn test_content_id_to_string() {
        let data = b"test data";
        let content_id = ContentId::new(data);
        let cid_string = content_id.to_string();
        assert!(!cid_string.is_empty());
    }

    #[test]
    fn test_content_id_from_string() {
        let data = b"test data";
        let content_id = ContentId::new(data);
        let cid_string = content_id.to_string();
        let content_id_from_string = ContentId::from_string(&cid_string);
        assert_eq!(content_id, content_id_from_string);
    }

    #[test]
    fn test_content_id_from_bytes() {
        let data = b"test data";
        let content_id = ContentId::new(data);
        let cid_bytes = content_id.to_bytes();
        let content_id_from_bytes = ContentId::from_bytes(&cid_bytes);
        assert_eq!(content_id, content_id_from_bytes);
    }

    #[test]
    fn test_content_id_from_base() {
        let data = b"test data";
        let content_id = ContentId::new(data);
        let base64_cid = content_id.to_base(Base::Base64);
        let content_id_from_base = ContentId::from_base(&base64_cid, Base::Base64);
        assert_eq!(content_id, content_id_from_base);
    }

    #[test]
    fn test_content_id_to_bytes() {
        let data = b"test data";
        let content_id = ContentId::new(data);
        let cid_bytes = content_id.to_bytes();
        assert!(!cid_bytes.is_empty());
    }

    #[test]
    fn test_content_id_equality() {
        let data1 = b"test data";
        let data2 = b"test data";
        let content_id1 = ContentId::new(data1);
        let content_id2 = ContentId::new(data2);
        assert_eq!(content_id1, content_id2);
    }

    #[test]
    fn test_content_id_inequality() {
        let data1 = b"data 1";
        let data2 = b"data 2";
        let content_id1 = ContentId::new(data1);
        let content_id2 = ContentId::new(data2);
        assert_ne!(content_id1, content_id2);
    }

    #[test]
    fn test_content_id_verify() {
        let data = b"test data";
        let content_id = ContentId::new(data);
        assert!(content_id.verify(data));
    }

    #[test]
    fn test_content_id_verify_with_different_data() {
        let data1 = b"test data1";
        let data2 = b"test data2";
        let content_id = ContentId::new(data1);
        assert!(!content_id.verify(data2));
    }
}
