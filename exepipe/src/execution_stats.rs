//! Final outcomes only: speculative OCC attempts must not increment these.
use exepipe_common::executor::error::ExecutionError;
use revm::context::result::{ExecutionResult, ResultAndState};

pub(crate) fn record(result: &Result<ResultAndState, ExecutionError>) {
    metrics::counter!("execution.finalized").increment(1);
    match result {
        Ok(value) => match value.result {
            ExecutionResult::Success { .. } => {
                metrics::counter!("execution.successful").increment(1)
            }
            ExecutionResult::Revert { .. } => metrics::counter!("execution.reverted").increment(1),
            ExecutionResult::Halt { .. } => metrics::counter!("execution.halted").increment(1),
        },
        Err(_) => metrics::counter!("execution.invalid").increment(1),
    }
}
