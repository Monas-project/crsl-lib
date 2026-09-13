use cid::Cid;

/// Unites metadata about a DAG node that should be considered during merge resolution.
#[derive(Clone, Debug)]
pub struct ResolveInput<P> {
    pub cid: Cid,
    pub payload: P,
    pub timestamp: u64,
    /// Payloads of this head's parents, in the node's parent order.
    ///
    /// A policy that treats the payload as more than an opaque value needs to
    /// know what a head *changed*, not just what it holds: a node that copied
    /// its parent's value for one field while updating another should not win
    /// a last-writer race on the field it left alone. The library cannot make
    /// that distinction — it does not know the payload's shape — so it hands
    /// the parents over and lets the policy compare.
    ///
    /// Empty for a genesis head (no parents) and for inputs constructed by
    /// callers that have no DAG at hand; `LwwMergePolicy` ignores it.
    pub parent_payloads: Vec<P>,
}

impl<P> ResolveInput<P> {
    pub fn new(cid: Cid, payload: P, timestamp: u64) -> Self {
        Self {
            cid,
            payload,
            timestamp,
            parent_payloads: Vec::new(),
        }
    }

    pub fn with_parents(cid: Cid, payload: P, timestamp: u64, parent_payloads: Vec<P>) -> Self {
        Self {
            cid,
            payload,
            timestamp,
            parent_payloads,
        }
    }
}

/// A merge strategy that produces a converged payload from candidate nodes.
///
/// The library ships [`LwwMergePolicy`](crate::convergence::policies::lww::LwwMergePolicy)
/// and selects it by the `policy_type` recorded in the genesis metadata. An
/// application whose payload is a composite — fields with different
/// convergence rules — supplies its own implementation through
/// [`Repo::with_merge_policy`](crate::repo::Repo::with_merge_policy); the
/// library then calls it for every auto-merge instead of the named policy.
pub trait MergePolicy<P>: Send + Sync {
    /// Resolve competing nodes into a single payload.
    fn resolve(&self, nodes: &[ResolveInput<P>]) -> P;

    /// Return a descriptive name of the policy (e.g. "lww").
    fn name(&self) -> &str;
}
