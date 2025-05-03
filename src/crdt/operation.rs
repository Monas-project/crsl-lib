use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use ulid::Ulid;

/// Unique identifier for operations (based on Ulid)
pub type OperationId = Ulid;
pub type Author = String;
pub type Timestamp = u64;

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
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as Timestamp;
        let id = Ulid::new();
        Self {
            id,
            target,
            kind,
            timestamp,
            author,
        }
    }

    /// Helper methods to check the operation type
    ///
    /// The following methods provide a convenient way to check what type of operation this is.
    /// Instead of directly pattern matching on the `kind` field, you can use these helper methods.
    ///
    /// Checks if this operation is a delete operation
    pub fn is_delete(&self) -> bool {
        matches!(self.kind, OperationType::Delete)
    }

    /// Checks if this operation is a create operation
    pub fn is_create(&self) -> bool {
        matches!(self.kind, OperationType::Create(_))
    }

    /// Checks if this operation is an update operation
    pub fn is_update(&self) -> bool {
        matches!(self.kind, OperationType::Update(_))
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
    fn test_operation_create() {
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
        assert!(op.is_create());
        assert!(!op.is_delete());
    }

    #[test]
    fn test_operation_update() {
        let target = DummyContentId("test".into());
        let payload = DummyPayload("updated".into());
        let author = "Alice".to_string();

        let op = Operation::new(
            target.clone(),
            OperationType::Update(payload.clone()),
            author.clone(),
        );

        assert!(op.id != Ulid::nil());
        assert_eq!(op.target, target);
        assert_eq!(op.kind, OperationType::Update(payload.clone()));
        assert!(op.timestamp > 0);
        assert_eq!(op.author, author);
        assert_eq!(op.payload(), Some(&payload));
        assert!(!op.is_create());
        assert!(op.is_update());
        assert!(!op.is_delete());
    }

    #[test]
    fn test_operation_delete() {
        let target = DummyContentId("test".into());
        let author = "Alice".to_string();

        let op = Operation::<DummyContentId, DummyPayload>::new(
            target.clone(),
            OperationType::Delete,
            author.clone(),
        );

        assert!(op.id != Ulid::nil());
        assert_eq!(op.target, target);
        assert_eq!(op.kind, OperationType::Delete);
        assert!(op.timestamp > 0);
        assert_eq!(op.author, author);
        assert_eq!(op.payload(), None);
        assert!(!op.is_create());
        assert!(op.is_delete());
    }
}
