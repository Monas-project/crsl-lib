use serde::{Deserialize, Serialize};

/// Metadata that stores information required for convergence policies.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContentMetadata {
    /// Policy type name (e.g. "lww", "text", "custom-policy").
    /// When this is `None`, it falls back to the default policy (currently "lww").
    policy_type: Option<String>,
}

impl ContentMetadata {
    /// Create metadata with the default LWW policy.
    pub fn new() -> Self {
        Self { policy_type: None }
    }

    /// Create metadata that uses the specified policy.
    pub fn with_policy(policy_type: impl Into<String>) -> Self {
        Self {
            policy_type: Some(policy_type.into()),
        }
    }

    /// Return the configured policy type; falls back to "lww" when unspecified.
    pub fn policy_type(&self) -> &str {
        self.policy_type.as_deref().unwrap_or("lww")
    }
}

impl Default for ContentMetadata {
    fn default() -> Self {
        Self::new()
    }
}
