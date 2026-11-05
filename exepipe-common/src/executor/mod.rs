pub mod adapter;
pub mod error;
pub mod exector_database;
pub mod execute_tx;
pub mod executor;
pub mod executor_proxy;
pub mod get_change_set;

pub use adapter::*;
pub use executor::*;
pub use executor_proxy::*;

/// Whether predicted access sets constrain execution. OCC records actual accesses
/// in its database adapter instead; FAFO keeps enforcing its scheduling inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessPolicy {
    EnforcePredicted,
    Unrestricted,
}
