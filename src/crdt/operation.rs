use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use ulid::Ulid;

pub type OperationId = Ulid;
pub type Author = String;
pub type Timestamp = u64;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OperationType<T> {
    Create(T),
    Update(T),
    Delete,
}

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
    pub fn new(target: ContentId, kind: OperationType<T>, author: Author) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as Timestamp;
        // Use UlId to make sorting possible.
        let id = Ulid::new();
        Self {
            id,
            target,
            kind,
            timestamp,
            author,
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(self.kind, OperationType::Delete)
    }

    pub fn is_create(&self) -> bool {
        matches!(self.kind, OperationType::Create(_))
    }

    pub fn is_update(&self) -> bool {
        matches!(self.kind, OperationType::Update(_))
    }

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
