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
    /// The resolver supplies all immediate parents when the policy requires
    /// them, or returns an error if any are missing. Empty for a genesis head,
    /// policies that opt out of parent payloads (such as `LwwMergePolicy`),
    /// and inputs constructed by callers that have no DAG at hand.
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
/// library calls it only when its name matches the genesis metadata. The
/// built-in `lww` name is reserved and always selects the library's LWW rule.
pub trait MergePolicy<P>: Send + Sync {
    /// Whether resolution needs the complete payloads of each head's immediate parents.
    ///
    /// Defaults to true: the resolver returns a missing-node error before
    /// invoking `resolve` if any parent has not synced yet. The caller can
    /// retry after importing the missing parents; no merge is persisted.
    /// Return false only when resolution is independent of parent payloads.
    /// In that case the resolver does not load them and supplies empty vectors.
    fn requires_parent_payloads(&self) -> bool {
        true
    }

    /// Resolve competing nodes into a single payload.
    fn resolve(&self, nodes: &[ResolveInput<P>]) -> P;

    /// Return the stable policy identifier recorded in genesis metadata.
    ///
    /// All implementations sharing a name must have identical deterministic
    /// merge semantics across replicas. Use a new name when those semantics
    /// change; installing it does not migrate existing content. `lww` is
    /// reserved for the built-in rule, not an application override.
    fn name(&self) -> &str;
}
