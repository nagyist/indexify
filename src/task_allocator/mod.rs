use anyhow::Result;

use self::planner::TaskAllocationPlan;
use crate::state::SharedState;
use indexify_internal_api::Task;

pub mod planner;

#[allow(dead_code)] // until scheduler integration
pub struct TaskAllocator {
    shared_state: SharedState,
    planner: Box<dyn planner::AllocationPlanner + Send + Sync>,
}

#[allow(dead_code)] // until scheduler integration
impl TaskAllocator {
    pub fn new(shared_state: SharedState) -> Self {
        Self {
            shared_state: shared_state.clone(),
            planner: Box::new(planner::load_aware_distributor::LoadAwareDistributor::new(
                shared_state.clone(),
            )),
        }
    }

    pub async fn allocate_tasks(&self, tasks: Vec<Task>) -> Result<TaskAllocationPlan> {
        self.planner.plan_allocations(tasks).await
    }
}
