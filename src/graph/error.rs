use crate::dasl::error::DaslError;
use bincode::error::{DecodeError, EncodeError};
use rusty_leveldb::Status as LeveldbError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GraphError {
    #[error("storage error: {0}")]
    Storage(#[from] LeveldbError),

    #[error("serialization error: {0}")]
    Serialize(#[from] EncodeError),

    #[error("deserialization error: {0}")]
    Deserialize(#[from] DecodeError),

    #[error("cycle detected in graph")]
    CycleDetected,

    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("invalid parent reference: {0}")]
    InvalidParent(String),

    #[error("empty graph operation not allowed")]
    EmptyGraph,

    #[error("dasl error: {0}")]
    Dasl(#[from] DaslError),

    #[error("timestamp error: {0}")]
    Timestamp(#[from] std::time::SystemTimeError),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, GraphError>;
