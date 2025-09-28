use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use ulid::Ulid;

use crate::crdt::timestamp::next_monotonic_timestamp;

/// Unique identifier for operations (based on Ulid)
pub type OperationId = Ulid;
pub type Author = String;
pub type Timestamp = u64;

/// Enum representing the abstract kind of operation without payload
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    Create,
    Update,
    Delete,
}

/// Enum representing the type of operation
///
/// Create: Create a new content
/// Update: Update an existing content
/// Delete: Delete an existing content
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OperationType<T> {
    Create(T),
    Update(T),
    Delete,
}

/// Helper methods to check the operation type
impl<T> OperationType<T> {
    pub fn as_kind(&self) -> OperationKind {
        match self {
            OperationType::Create(_) => OperationKind::Create,
            OperationType::Update(_) => OperationKind::Update,
            OperationType::Delete => OperationKind::Delete,
        }
    }
}

/// Structure representing a CRDT operation
///
/// Each operation represents a create, update, or delete action on a target content.
/// Operations include a unique ID, execution timestamp, and information about the executor.
///
/// # Type Parameters
///
/// * `ContentId` - Type identifying the target content
/// * `T` - Type of the payload data for the operation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Operation<ContentId, T> {
    pub id: OperationId,
    pub target: ContentId,
    pub genesis: ContentId,
    pub kind: OperationType<T>,
    pub timestamp: Timestamp,
    pub author: Author,
}

impl<ContentId, T> Operation<ContentId, T>
where
    ContentId: Clone + Debug + Serialize,
    T: Clone + Debug + Serialize,
{
    /// Creates a new operation
    ///
    /// # Arguments
    ///
    /// * `target` - ID of the content being operated on
    /// * `kind` - Type of operation and its payload
    /// * `author` - User/system performing the operation
    ///
    /// # Returns
    ///
    /// A newly created operation object
    pub fn new(target: ContentId, kind: OperationType<T>, author: Author) -> Self {
        let timestamp = next_monotonic_timestamp();
        let id = Ulid::new();
        let genesis = target.clone();
        Self {
            id,
            target,
            genesis,
            kind,
            timestamp,
            author,
        }
    }

    /// Creates a new operation with a genesis content
    ///
    /// # Arguments
    ///
    /// * `target` - ID of the content being operated on
    /// * `root_id` - ID of the genesis content
    /// * `kind` - Type of operation and its payload
    pub fn new_with_genesis(
        target: ContentId,
        root_id: ContentId,
        kind: OperationType<T>,
        author: Author,
    ) -> Self {
        let timestamp = next_monotonic_timestamp();
        let id = Ulid::new();
        Self {
            id,
            target,
            genesis: root_id,
            kind,
            timestamp,
            author,
        }
    }

    /// Checks if this operation is of the given kind
    pub fn is_type(&self, kind: OperationKind) -> bool {
        self.kind.as_kind() == kind
    }

    /// Gets the payload of the operation
    ///
    /// Delete operations have no payload, so this returns `None` for them.
    ///
    /// # Returns
    ///
    /// `Some` containing a reference to the payload for create/update operations,
    /// or `None` for delete operations
    pub fn payload(&self) -> Option<&T> {
        match &self.kind {
            OperationType::Create(v) | OperationType::Update(v) => Some(v),
            OperationType::Delete => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Serialize)]
    struct DummyContentId(String);

    #[derive(Clone, Debug, Serialize, PartialEq)]
    struct DummyPayload(String);

    #[test]
    fn test_operation_new_create() {
        let target = DummyContentId("test".into());
        let payload = DummyPayload("test".into());
        let author = "Alice".to_string();

        let op = Operation::new(
            target.clone(),
            OperationType::Create(payload.clone()),
            author.clone(),
        );

        assert!(op.id != Ulid::nil());
        assert_eq!(op.target, target);
        assert_eq!(op.kind, OperationType::Create(payload.clone()));
        assert!(op.timestamp > 0);
        assert_eq!(op.author, author);
        assert_eq!(op.payload(), Some(&payload));
        assert!(op.is_type(OperationKind::Create));
        assert!(!op.is_type(OperationKind::Update));
        assert!(!op.is_type(OperationKind::Delete));
    }
    #[test]
    fn test_operation_create_with_genesis() {
        let target = DummyContentId("test".into());
        let genesis = DummyContentId("genesis".into());
        let payload = DummyPayload("test".into());
        let author = "Alice".to_string();

        let op = Operation::new_with_genesis(
            target.clone(),
            genesis.clone(),
            OperationType::Create(payload.clone()),
            author.clone(),
        );

        assert!(op.id != Ulid::nil());
        assert_eq!(op.target, target);
        assert_eq!(op.kind, OperationType::Create(payload.clone()));
        assert!(op.timestamp > 0);
        assert_eq!(op.author, author);
        assert_eq!(op.payload(), Some(&payload));
        assert!(op.is_type(OperationKind::Create));
        assert!(!op.is_type(OperationKind::Update));
        assert!(!op.is_type(OperationKind::Delete));
    }

    #[test]
    fn test_operation_update() {
        let target = DummyContentId("test".into());
        let genesis = DummyContentId("genesis".into());
        let payload = DummyPayload("updated".into());
        let author = "Alice".to_string();

        let op = Operation::new_with_genesis(
            target.clone(),
            genesis.clone(),
            OperationType::Update(payload.clone()),
            author.clone(),
        );

        assert!(op.id != Ulid::nil());
        assert_eq!(op.target, target);
        assert_eq!(op.kind, OperationType::Update(payload.clone()));
        assert!(op.timestamp > 0);
        assert_eq!(op.author, author);
        assert_eq!(op.payload(), Some(&payload));
        assert!(op.is_type(OperationKind::Update));
        assert!(!op.is_type(OperationKind::Create));
        assert!(!op.is_type(OperationKind::Delete));
    }

    #[test]
    fn test_operation_delete() {
        let target = DummyContentId("test".into());
        let genesis = DummyContentId("genesis".into());
        let author = "Alice".to_string();

        let op = Operation::<DummyContentId, DummyPayload>::new_with_genesis(
            target.clone(),
            genesis.clone(),
            OperationType::Delete,
            author.clone(),
        );

        assert!(op.id != Ulid::nil());
        assert_eq!(op.target, target);
        assert_eq!(op.kind, OperationType::Delete);
        assert!(op.timestamp > 0);
        assert_eq!(op.author, author);
        assert_eq!(op.payload(), None);
        assert!(!op.is_type(OperationKind::Create));
        assert!(op.is_type(OperationKind::Delete));
    }
}
