use bincode::error::{DecodeError, EncodeError};
use rusty_leveldb::Status as LeveldbError;
use thiserror::Error;
use ulid::DecodeError as UlidDecodeError;

#[derive(Error, Debug)]
pub enum CrdtError {
    #[error("storage error: {0}")]
    Storage(#[from] LeveldbError),

    #[error("serialization error: {0}")]
    Serialize(#[from] EncodeError),

    #[error("deserialization error: {0}")]
    Deserialize(#[from] DecodeError),

    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),

    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("missing CREATE operation for target: {0}")]
    MissingCreate(String),
    #[error("duplicate operation ID: {0}")]
    DuplicateOp(#[from] UlidDecodeError),
}

pub type Result<T> = std::result::Result<T, CrdtError>;
