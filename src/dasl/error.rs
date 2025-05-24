use thiserror::Error;
use cid::Error as CidError;
use multibase::Error as MultibaseError;
use multihash::Error as MultihashError;

#[derive(Error, Debug)]
pub enum DaslError {
    // Serialization/Deserialization errors
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_cbor::Error),

    #[error("deserialization error: {message}")]
    Deserialization { message: String },

    // CID related errors
    #[error("CID error: {0}")]
    Cid(#[from] CidError),

    #[error("invalid CID format: {0}")]
    InvalidCid(String),

    #[error("CID verification failed")]
    CidVerificationFailed,

    // Hash computation errors
    #[error("multihash error: {0}")]
    Multihash(#[from] MultihashError),

    #[error("hash computation failed: {0}")]
    HashComputation(String),

    // Base encoding errors
    #[error("multibase error: {0}")]
    Multibase(#[from] MultibaseError),

    #[error("base encoding mismatch: expected {expected}, got {actual}")]
    BaseEncodingMismatch { expected: String, actual: String },

    // Node specific errors
    #[error("node validation error: {0}")]
    NodeValidation(#[from] NodeValidationError),

    #[error("content integrity verification failed")]
    IntegrityVerificationFailed,
}

#[derive(Error, Debug)]
pub enum NodeValidationError {
    #[error("empty payload")]
    EmptyPayload,

    #[error("invalid parent CID: {0}")]
    InvalidParent(String),

    #[error("circular reference detected")]
    CircularReference,

    #[error("metadata validation failed: {0}")]
    MetadataValidation(String),
}

pub type Result<T> = std::result::Result<T, DaslError>;