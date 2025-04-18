use sha2::{Digest, Sha256};
use data_encoding::BASE32;

pub struct Cid{
    bytes: Vec<u8>,
}

impl Cid{
    /// Generates a new Cid from the given data and codec
    pub fn generate(data: &[u8], codec: u8) -> Self{
        let digest = sha256_digest(data);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[codec]);
        bytes.extend_from_slice(&digest);
        Self { bytes }
    }

    /// Returns the bytes of the Cid
    pub fn to_bytes(&self) -> &[u8]{
        &self.bytes
    }

    /// Returns the BASE32 encoded string representation of the Cid
    pub fn to_string(&self) -> String{
        BASE32.encode(self.bytes.as_slice())
    }
}

fn sha256_digest(data: &[u8]) -> Vec<u8>{
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}


#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn test_cid_generation(){
        let data = b"test";
        let codec = 0x01;
        let cid = Cid::generate(data, codec);
        assert_eq!(cid.to_bytes()[0], codec);
        let encoded_string = cid.to_string();
        assert!(!encoded_string.is_empty());
    }
    #[test]
    fn test_cid_consistency(){
        let data = b"test";
        let codec = 0x01;
        let cid1 = Cid::generate(data, codec);
        let cid2 = Cid::generate(data, codec);
        assert_eq!(cid1.to_bytes(), cid2.to_bytes());
        assert_eq!(cid1.to_string(), cid2.to_string());
    }
}