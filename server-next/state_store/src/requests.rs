use data_model::{
    ComputeGraph,
    ExecutorId,
    InvocationPayload,
    NodeOutput,
    StateChangeId,
    Task,
    TaskId,
};

pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
    pub state_changes_processed: Vec<StateChangeId>,
}

pub enum RequestPayload {
    InvokeComputeGraph(InvokeComputeGraphRequest),
    FinalizeTask(FinalizeTaskRequest),
    CreateNameSpace(NamespaceRequest),
    CreateComputeGraph(CreateComputeGraphRequest),
    DeleteComputeGraph(DeleteComputeGraphRequest),
    DeleteInvocation(DeleteInvocationRequest),
    SchedulerUpdate(SchedulerUpdateRequest),
}

pub struct StateChangeProcessedRequest {
    pub state_change_ids: Vec<StateChangeId>,
}

pub struct FinalizeTaskRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub task_id: TaskId,
    pub node_outputs: Vec<NodeOutput>,
    pub task_outcome: data_model::TaskOutcome,
    pub executor_id: ExecutorId,
}

pub struct InvokeComputeGraphRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub invocation_payload: InvocationPayload,
}

pub struct NamespaceRequest {
    pub name: String,
}

pub struct CreateComputeGraphRequest {
    pub namespace: String,
    pub compute_graph: ComputeGraph,
}

pub struct DeleteComputeGraphRequest {
    pub namespace: String,
    pub name: String,
}

pub struct CreateTasksRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
    pub tasks: Vec<Task>,
    // Invocation ID -> Finished
    pub invocation_finished: bool,
}

pub struct SchedulerUpdateRequest {
    pub task_requests: Vec<CreateTasksRequest>,
}

pub struct DeleteInvocationRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
}

pub struct MarkInvocationFinishedRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
}
