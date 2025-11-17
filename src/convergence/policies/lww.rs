use crate::convergence::policy::{MergePolicy, ResolveInput};

/// A simple last-write-wins merge policy that selects the node with the
/// greatest timestamp. Ties fall back to the last node in the slice, mimicking
/// stable behaviour on equal timestamps.
#[derive(Debug, Default)]
pub struct LwwMergePolicy;

impl<P: Clone> MergePolicy<P> for LwwMergePolicy {
    fn resolve(&self, nodes: &[ResolveInput<P>]) -> P {
        let winner = nodes
            .iter()
            .max_by_key(|input| input.timestamp)
            .expect("LwwMergePolicy requires at least one candidate node");
        winner.payload.clone()
    }

    fn name(&self) -> &str {
        "lww"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convergence::policy::ResolveInput;
    use cid::Cid;
    use multihash::Multihash;

    fn create_test_cid(label: &str) -> Cid {
        let digest = Multihash::<64>::wrap(0x12, label.as_bytes()).unwrap();
        Cid::new_v1(0x55, digest)
    }

    #[test]
    fn selects_highest_timestamp() {
        let policy = LwwMergePolicy;
        let inputs = vec![
            ResolveInput::new(create_test_cid("a"), "older".to_string(), 10),
            ResolveInput::new(create_test_cid("b"), "newer".to_string(), 20),
        ];

        let result = policy.resolve(&inputs);
        assert_eq!(result, "newer");
    }

    #[test]
    fn ties_choose_last_entry() {
        let policy = LwwMergePolicy;
        let inputs = vec![
            ResolveInput::new(create_test_cid("a"), "first".to_string(), 42),
            ResolveInput::new(create_test_cid("b"), "second".to_string(), 42),
        ];

        let result = policy.resolve(&inputs);
        assert_eq!(result, "second");
    }

    #[test]
    fn selects_highest_timestamp_among_three() {
        let policy = LwwMergePolicy;
        let inputs = vec![
            ResolveInput::new(create_test_cid("a"), "payload-a".to_string(), 5),
            ResolveInput::new(create_test_cid("b"), "payload-b".to_string(), 10),
            ResolveInput::new(create_test_cid("c"), "payload-c".to_string(), 8),
        ];

        let result = policy.resolve(&inputs);
        assert_eq!(result, "payload-b");
    }
}
