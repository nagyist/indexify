use std::{collections::HashMap, time::SystemTime};

use indexify_internal_api::{self as internal_api, GarbageCollectionTask};
use internal_api::{
    ComputeGraph,
    DataObject,
    ExecutorMetadata,
    ExtractionGraphLink,
    GraphInvocationCtx,
    StateChange,
    StateChangeId,
};
use serde::{Deserialize, Serialize};

use super::{ExecutorId, TaskId};
use crate::state::NodeId;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
    pub new_state_changes: Vec<StateChange>,
    pub state_changes_processed: Vec<StateChangeProcessed>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateChangeProcessed {
    pub state_change_id: StateChangeId,
    pub processed_at: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateOrUpdateContentEntry {
    pub content: internal_api::ContentMetadata,
    pub previous_parent: Option<internal_api::ContentMetadataId>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InvokeComputeGraphRequest {
    pub namespace: String,
    pub graph_name: String,
    pub data_object: DataObject,
    pub invocation_ctx: GraphInvocationCtx,
    pub created_time: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateComputeGraphRequest {
    pub compute_graph: ComputeGraph,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TombstoneIngestedDataObjectRequest {
    pub namespace: String,
    pub id: String,
    pub compute_graph_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeleteComputeGraphRequest {
    pub namespace: String,
    pub graph_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MarkTaskFinishedRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub compute_fn_name: String,
    pub task_id: String,
    pub outcome: internal_api::TaskOutcome,
    pub data_objects: Vec<DataObject>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RequestPayload {
    //  NOTE: This isn't strictly a state machine update. It's used to change cluster membership.
    JoinCluster {
        node_id: NodeId,
        address: String,
        coordinator_addr: String,
    },
    InvokeComputeGraph(InvokeComputeGraphRequest),
    RegisterExecutor(ExecutorMetadata),
    RemoveExecutor {
        executor_id: String,
    },
    CreateNamespace {
        name: String,
    },
    CreateTasks {
        tasks: Vec<internal_api::Task>,
    },
    AssignTask {
        assignments: HashMap<TaskId, ExecutorId>,
    },
    CreateComputeGraph(CreateComputeGraphRequest),
    DeleteComputeGraph(DeleteComputeGraphRequest),
    CreateExtractionGraphLink {
        extraction_graph_link: ExtractionGraphLink,
    },
    TombstoneIngestedDataObject(TombstoneIngestedDataObjectRequest),
    UpdateTask(MarkTaskFinishedRequest),
    MarkStateChangesProcessed {
        state_changes: Vec<StateChangeProcessed>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StateMachineUpdateResponse {
    pub handled_by: NodeId,
}
