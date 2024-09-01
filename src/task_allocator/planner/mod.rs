pub mod load_aware_distributor;
use std::collections::{HashMap, HashSet};

use indexify_internal_api::{ExecutorId, Task, TaskId};

#[derive(Debug, Clone)]
pub struct TaskAllocationPlan(pub HashMap<TaskId, ExecutorId>);

impl From<TaskAllocationPlan> for HashMap<TaskId, ExecutorId> {
    fn from(plan: TaskAllocationPlan) -> Self {
        plan.0
    }
}

pub type AllocationPlannerResult = Result<TaskAllocationPlan, anyhow::Error>;
#[async_trait::async_trait]
pub trait AllocationPlanner {
    async fn plan_allocations(&self, tasks: Vec<Task>) -> AllocationPlannerResult;
}
