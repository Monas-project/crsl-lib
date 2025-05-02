pub type OperationId = u128;
pub type Author = String;
pub type Signature = String;
pub type Timestamp = u64;

#[derive(Clone, Debug)]
pub enum OperationType<T> {
    Create(T),
    Update(T),
    Delete,
}

pub struct Operation<ContentId, T> {
    pub id: OperationId,
    pub target: ContentId,
    pub kind: OperationType<T>,
    pub timestamp: Timestamp,
    pub author: Author,
    pub signature: Option<Signature>,
}

impl<ContentId, T> Operation<ContentId, T> {
    pub fn new(target: ContentId, kind: OperationType<T>, author: Author) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as Timestamp;
        // todo: generate id
        let id = 0;
        Self { id, target, kind, timestamp, author, signature: None }
    }
    pub fn verify_signature(&self) -> bool {
        true
    }
}


