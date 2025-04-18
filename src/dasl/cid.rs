use cid::Cid;
use multihash::Multihash;

/// For more details on these multicodec codes, see: 
/// https://github.com/multiformats/multicodec/blob/master/table.csv
const SHA2_256_CODE: u64 = 0x12;
const RAW_CODE: u64 = 0x55;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContentId(pub Cid);

impl ContentId {
    pub fn new(data: &[u8]) -> Self {
        let code = SHA2_256_CODE;
        let digest = Multihash::<64>::wrap(code, data).unwrap();
        let cid = Cid::new_v1(RAW_CODE, digest);
        ContentId(cid)
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_creation() {
        let data = b"test data";
        let content_id = ContentId::new(data);
        assert_eq!(content_id.to_string(), content_id.0.to_string());
    }
    #[test]
    fn test_content_id_to_string() {
        let data = b"test data";
        let content_id = ContentId::new(data);
        let cid_string = content_id.to_string();
        assert!(!cid_string.is_empty());
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
}
