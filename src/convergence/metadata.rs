use serde::{Deserialize, Serialize};

/// Built-in and custom convergence policy types.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PolicyType {
    /// Last-Write-Wins policy.
    Lww,
    /// Any non-builtin policy, identified by its name.
    Custom(String),
}

impl From<&str> for PolicyType {
    fn from(value: &str) -> Self {
        match value {
            "lww" => PolicyType::Lww,
            other => PolicyType::Custom(other.to_string()),
        }
    }
}

impl From<String> for PolicyType {
    fn from(value: String) -> Self {
        PolicyType::from(value.as_str())
    }
}

/// Metadata that stores information required for convergence policies.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContentMetadata {
    /// Policy type (e.g. Lww, custom named policy).
    ///
    /// When this is `None`, it falls back to the default policy (currently Lww).
    policy_type: Option<PolicyType>,
}

impl ContentMetadata {
    /// Create metadata with the default LWW policy.
    pub fn new() -> Self {
        Self { policy_type: None }
    }

    /// Create metadata that uses the specified policy.
    ///
    /// This accepts either a concrete `PolicyType` or a string like `"lww"` or `"custom-policy"`.
    pub fn with_policy(policy_type: impl Into<PolicyType>) -> Self {
        Self {
            policy_type: Some(policy_type.into()),
        }
    }

    /// Return the configured policy type name; falls back to `"lww"` when unspecified.
    pub fn policy_type(&self) -> &str {
        match &self.policy_type {
            Some(PolicyType::Lww) | None => "lww",
            Some(PolicyType::Custom(name)) => name.as_str(),
        }
    }
}

impl Default for ContentMetadata {
    fn default() -> Self {
        Self::new()
    }
}
