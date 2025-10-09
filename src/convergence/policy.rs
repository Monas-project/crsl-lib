use cid::Cid;

/// Unites metadata about a DAG node that should be considered during merge resolution.
#[derive(Clone, Debug)]
pub struct ResolveInput<P> {
    pub cid: Cid,
    pub payload: P,
    pub timestamp: u64,
}

impl<P> ResolveInput<P> {
    pub fn new(cid: Cid, payload: P, timestamp: u64) -> Self {
        Self {
            cid,
            payload,
            timestamp,
        }
    }
}

/// A merge strategy that produces a converged payload from candidate nodes.
pub trait MergePolicy<P>: Send + Sync {
    /// Resolve competing nodes into a single payload.
    fn resolve(&self, nodes: &[ResolveInput<P>]) -> P;

    /// Return a descriptive name of the policy (e.g. "lww").
    fn name(&self) -> &str;
}

