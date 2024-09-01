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
    CreateOrAssignGarbageCollectionTask {
        gc_tasks: Vec<GarbageCollectionTask>,
    },
    UpdateGarbageCollectionTask {
        gc_task: GarbageCollectionTask,
        mark_finished: bool,
    },
    CreateComputeGraph(CreateComputeGraphRequest),
    DeleteExtractionGraph {
        graph_id: String,
        gc_task: GarbageCollectionTask,
    },
    DeleteExtractionGraphByName {
        extraction_graph: String,
        namespace: String,
    },
    CreateExtractionGraphLink {
        extraction_graph_link: ExtractionGraphLink,
    },
    CreateOrUpdateContent {
        entries: Vec<CreateOrUpdateContentEntry>,
    },
    TombstoneContentTree {
        content_metadata: Vec<internal_api::ContentMetadata>,
    },
    // Tombstone content or delete one of the graph associations.
    // This is used when a graph is deleted.
    TombstoneContent {
        content_metadata: Vec<internal_api::ContentMetadata>,
    },
    UpdateTask {
        task: internal_api::Task,
        executor_id: Option<String>,
        update_time: SystemTime,
    },
    MarkStateChangesProcessed {
        state_changes: Vec<StateChangeProcessed>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StateMachineUpdateResponse {
    pub handled_by: NodeId,
}
