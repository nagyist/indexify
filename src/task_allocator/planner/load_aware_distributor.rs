use std::collections::{HashMap, HashSet};

use indexify_internal_api::Task;

use super::{AllocationPlanner, AllocationPlannerResult, TaskAllocationPlan};
use crate::state::SharedState;

pub struct LoadAwareDistributor {
    shared_state: SharedState,
}

impl LoadAwareDistributor {
    pub fn new(shared_state: SharedState) -> Self {
        Self { shared_state }
    }
}

// TODO 
// Mantain an inmemory index of Executor Labels -> Executor ID
// For every task, find the list of executors that mateches all the labels
// From the list of potential executors, select the one with the lowest load

#[async_trait::async_trait]
impl AllocationPlanner for LoadAwareDistributor {
    async fn plan_allocations(&self, tasks: Vec<Task>) -> AllocationPlannerResult {
        let mut plan = TaskAllocationPlan(HashMap::new());
        let executors = self.shared_state.get_executors().await?;
        for task in tasks {
            if executors.len() > 0 {
                let executor = executors.first().unwrap();
                plan.0.insert(task.id, executor.id.clone());
            }
        }
        Ok(plan)
    }
}
