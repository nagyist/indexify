#![allow(clippy::uninlined_format_args)]
#![deny(unused_qualifications)]

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use grpc_server::RaftGrpcServer;
use indexify_internal_api::{
    self as internal_api,
    ChangeType,
    ExecutorMetadata,
    ExtractionGraphNode,
    GarbageCollectionTask,
};
use indexify_proto::{
    indexify_coordinator,
    indexify_coordinator::CreateContentStatus,
    indexify_raft::raft_api_server::RaftApiServer,
};
use internal_api::{
    ComputeGraph,
    ContentMetadataId,
    ContentSource,
    ExtractionGraph,
    ExtractionGraphLink,
    ExtractionPolicy,
    StateChange,
    StateChangeId,
    TaskOutcome,
    DataObject,
};
use itertools::Itertools;
use network::Network;
use openraft::{
    self,
    error::{InitializeError, RaftError},
    BasicNode,
    TokioRuntime,
};
use rocksdb::IteratorMode;
use serde::Serialize;
use store::{
    requests::{
        CreateComputeGraphRequest,
        RequestPayload,
        StateChangeProcessed,
        StateMachineUpdateRequest,
        MarkTaskFinishedRequest,
    },
    serializer::{JsonEncode, JsonEncoder},
    ExecutorId,
    ExecutorIdRef,
    Response,
    TaskId,
};
use tokio::{
    sync::{
        broadcast,
        watch::{self, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::{error, info, warn};

use self::{
    forwardable_raft::ForwardableRaft,
    store::{requests::CreateOrUpdateContentEntry, StateMachineColumns, StateMachineStore},
};
use crate::{
    garbage_collector::GarbageCollector,
    metrics::{
        coordinator::Metrics,
        raft_metrics::{self, network::MetricsSnapshot},
    },
    server_config::ServerConfig,
    state::{
        grpc_config::GrpcConfig,
        raft_client::RaftClient,
        store::{new_storage, FilterResponse},
    },
    utils::timestamp_secs,
};

pub mod forwardable_raft;
pub mod grpc_config;
pub mod grpc_server;
pub mod network;
pub mod raft_client;
pub mod store;

pub type NodeId = u64;

#[derive(Debug, Clone)]
pub struct SnapshotData {
    pub snapshot_dir: PathBuf,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = StateMachineUpdateRequest,
        R = Response,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = SnapshotData,
        AsyncRuntime = TokioRuntime
);

pub type Raft = openraft::Raft<TypeConfig>;

pub type SharedState = Arc<App>;

pub mod typ {
    use openraft::BasicNode;

    use super::{NodeId, TypeConfig};
    pub type Entry = openraft::Entry<TypeConfig>;

    pub type RPCError<E> = openraft::error::RPCError<NodeId, BasicNode, E>;
    pub type RemoteError<E> = openraft::error::RemoteError<NodeId, BasicNode, E>;
    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type NetworkError = openraft::error::NetworkError;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;
    pub type InstallSnapshotError = openraft::error::InstallSnapshotError;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

const MEMBERSHIP_CHECK_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_secs(3);

#[derive(Serialize)]
pub struct RaftMetrics {
    pub openraft_metrics: openraft::RaftMetrics<NodeId, BasicNode>,
    pub raft_metrics: MetricsSnapshot,
}

pub struct App {
    pub id: NodeId,
    pub addr: String,
    coordinator_addr: String,
    seed_node: String,
    pub forwardable_raft: ForwardableRaft,
    nodes: BTreeMap<NodeId, BasicNode>,
    shutdown_rx: Receiver<()>,
    shutdown_tx: Sender<()>,
    pub leader_change_rx: Receiver<bool>,
    join_handles: Mutex<Vec<JoinHandle<Result<()>>>>,
    pub config: Arc<openraft::Config>,
    state_change_rx: Receiver<StateChangeId>,
    pub network: Network,
    pub node_addr: String,
    pub state_machine: Arc<StateMachineStore>,
    pub garbage_collector: Arc<GarbageCollector>,
    pub registry: Arc<prometheus::Registry>,
    pub metrics: Metrics,
}

#[derive(Clone)]
pub struct RaftConfigOverrides {
    snapshot_policy: Option<openraft::SnapshotPolicy>,
    max_in_snapshot_log_to_keep: Option<u64>,
}

fn add_update_entry(
    update_entries: &mut Vec<CreateOrUpdateContentEntry>,
    state_changes: &mut Vec<StateChange>,
    statuses: &mut Vec<CreateContentStatus>,
    content: internal_api::ContentMetadata,
) {
    // Hold a reference to the content until the tasks are created if any.
    state_changes.push(StateChange::new_with_refcnt(
        content.id.id.clone(),
        ChangeType::NewContent,
        timestamp_secs(),
        content.get_root_id().to_string(),
    ));
    update_entries.push(CreateOrUpdateContentEntry {
        content,
        previous_parent: None,
    });
    statuses.push(CreateContentStatus::Created);
}

impl App {
    pub async fn new(
        server_config: Arc<ServerConfig>,
        overrides: Option<RaftConfigOverrides>,
        garbage_collector: Arc<GarbageCollector>,
        coordinator_addr: &str,
        registry: Arc<prometheus::Registry>,
    ) -> Result<Arc<Self>> {
        let mut raft_config = openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            enable_heartbeat: true,
            install_snapshot_timeout: 2000,
            snapshot_max_chunk_size: 4194304, //  4MB
            ..Default::default()
        };

        // Apply any overrides provided
        if let Some(overrides) = overrides {
            if let Some(snapshot_policy) = overrides.snapshot_policy {
                raft_config.snapshot_policy = snapshot_policy;
            }
            if let Some(max_in_snapshot_log_to_keep) = overrides.max_in_snapshot_log_to_keep {
                raft_config.max_in_snapshot_log_to_keep = max_in_snapshot_log_to_keep;
            }
        }

        let config = Arc::new(
            raft_config
                .validate()
                .map_err(|e| anyhow!("invalid raft config: {}", e.to_string()))?,
        );
        let storage_path = server_config.state_store.path.clone().unwrap_or_default();
        let db_path = Path::new(storage_path.as_str());

        let (log_store, state_machine) = new_storage(db_path).await?;
        let state_change_rx = state_machine.state_change_rx.clone();

        let raft_client = Arc::new(RaftClient::new());
        let network = Network::new(Arc::clone(&raft_client));

        let raft = openraft::Raft::new(
            server_config.node_id,
            config.clone(),
            network.clone(),
            log_store,
            Arc::clone(&state_machine),
        )
        .await
        .map_err(|e| anyhow!("unable to create raft: {}", e.to_string()))?;

        let forwardable_raft =
            ForwardableRaft::new(server_config.node_id, raft.clone(), network.clone());

        let mut nodes = BTreeMap::new();
        nodes.insert(
            server_config.node_id,
            BasicNode {
                addr: format!("{}:{}", server_config.listen_if, server_config.raft_port),
            },
        );
        let (tx, rx) = watch::channel::<()>(());

        let addr = server_config
            .raft_addr_sock()
            .map_err(|e| anyhow!("unable to create raft address : {}", e.to_string()))?;

        info!("starting raft server at {}", addr.to_string());
        let raft_srvr = RaftApiServer::new(RaftGrpcServer::new(
            server_config.node_id,
            Arc::new(raft.clone()),
            Arc::clone(&raft_client),
            addr.to_string(),
            server_config.coordinator_addr.clone(),
        ))
        .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE)
        .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE);
        let (leader_change_tx, leader_change_rx) = watch::channel::<bool>(false);

        let metrics = Metrics::new(state_machine.clone());

        let app = Arc::new(App {
            id: server_config.node_id,
            addr: server_config
                .coordinator_lis_addr_sock()
                .map_err(|e| anyhow!("unable to get coordinator address : {}", e.to_string()))?
                .to_string(),
            coordinator_addr: coordinator_addr.to_string(),
            seed_node: server_config.seed_node.clone(),
            forwardable_raft,
            shutdown_rx: rx,
            shutdown_tx: tx,
            leader_change_rx,
            join_handles: Mutex::new(vec![]),
            nodes,
            config,
            state_change_rx,
            network,
            node_addr: format!("{}:{}", server_config.listen_if, server_config.raft_port),
            state_machine,
            garbage_collector,
            registry,
            metrics,
        });

        let raft_clone = app.forwardable_raft.clone();

        let mut rx = app.shutdown_rx.clone();
        let shutdown_rx = app.shutdown_rx.clone();

        // Start task for watching leadership changes
        tokio::spawn(async move {
            let _ = watch_for_leader_change(raft_clone, leader_change_tx, shutdown_rx).await;
        });

        //  Start task for GRPC server
        let grpc_svc = tonic::transport::Server::builder().add_service(raft_srvr);
        let h = tokio::spawn(async move {
            grpc_svc
                .serve_with_shutdown(addr, async move {
                    let _ = rx.changed().await;
                    info!("shutting down grpc server");
                })
                .await
                .map_err(|e| anyhow!("grpc server error: {}", e))
        });
        app.join_handles.lock().await.push(h);

        //  Start task for cluster membership check
        let membership_shutdown_rx = app.shutdown_rx.clone();
        app.start_periodic_membership_check(membership_shutdown_rx);

        Ok(app)
    }

    /// This function checks whether this node is the seed node
    fn is_seed_node(&self) -> bool {
        let seed_node_port = self
            .seed_node
            .split(':')
            .nth(1)
            .and_then(|s| s.trim().parse::<u64>().ok());
        let node_addr_port = self
            .node_addr
            .split(':')
            .nth(1)
            .and_then(|s| s.trim().parse::<u64>().ok());
        match (seed_node_port, node_addr_port) {
            (Some(seed_port), Some(node_port)) => seed_port == node_port,
            _ => false,
        }
    }

    pub async fn initialize_raft(&self) -> Result<()> {
        if !self.is_seed_node() {
            return Ok(());
        }
        match self.forwardable_raft.initialize(self.nodes.clone()).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // match the type of the initialize error. if it's NotAllowed, ignore it.
                // this means that the node is already initialized.
                match e {
                    RaftError::APIError(InitializeError::NotAllowed(_)) => {
                        warn!("cluster is already initialized: {}", e);
                        Ok(())
                    }
                    _ => Err(anyhow!("unable to initialize raft: {}", e)),
                }
            }
        }
    }

    pub fn get_state_change_watcher(&self) -> Receiver<StateChangeId> {
        self.state_change_rx.clone()
    }

    pub async fn stop(&self) -> Result<()> {
        info!("stopping raft server");
        let _ = self.forwardable_raft.shutdown().await;
        self.shutdown_tx.send(()).unwrap();
        for j in self.join_handles.lock().await.iter_mut() {
            let res = j.await;
            info!("task quit res: {:?}", res);

            // The returned error does not mean this function call failed.
            // Do not need to return this error. Keep shutting down other tasks.
            if let Err(ref e) = res {
                error!("task quit with error: {:?}", e);
            }
        }
        Ok(())
    }

    pub async fn unprocessed_state_change_events(&self) -> Result<Vec<StateChange>> {
        let mut state_changes = vec![];
        let ids = self.state_machine.get_unprocessed_state_changes().await;
        let mut sorted_ids = ids.iter().collect_vec();
        sorted_ids.sort();

        for id in sorted_ids {
            let event = self
                .state_machine
                .get_from_cf::<StateChange, _>(StateMachineColumns::StateChanges, &id.to_key())?
                .ok_or_else(|| anyhow::anyhow!("Event with id {} not found", id))?;
            state_changes.push(event.clone());
        }
        Ok(state_changes)
    }

    pub async fn mark_change_events_as_processed(
        &self,
        events: Vec<StateChange>,
        new_state_changes: Vec<StateChange>,
    ) -> Result<()> {
        let mut state_changes = vec![];
        for event in events {
            state_changes.push(StateChangeProcessed {
                state_change_id: event.id,
                processed_at: timestamp_secs(),
            });
        }
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::MarkStateChangesProcessed { state_changes },
            new_state_changes,
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn get_extraction_policy(&self, id: &str) -> Result<ExtractionPolicy> {
        let extraction_policy = self
            .state_machine
            .get_from_cf::<ExtractionPolicy, _>(StateMachineColumns::ExtractionPolicies, id)?
            .ok_or_else(|| anyhow::anyhow!("Extraction policy with id {} not found", id))?;
        Ok(extraction_policy)
    }

    pub async fn unassigned_tasks(&self) -> Result<Vec<internal_api::Task>> {
        let mut tasks = vec![];
        for (task_id, _) in self.state_machine.get_unassigned_tasks().await.iter() {
            let task = self
                .state_machine
                .get_from_cf::<internal_api::Task, _>(StateMachineColumns::Tasks, task_id)?
                .ok_or_else(|| {
                    anyhow!(
                        "Unable to get task with id {} from state machine store",
                        task_id
                    )
                })?;
            tasks.push(task.clone());
        }
        Ok(tasks)
    }

    pub async fn task_assignments(&self) -> Result<HashMap<ExecutorId, TaskId>> {
        self.state_machine.get_all_task_assignments().await
    }

    pub async fn subscribe_to_new_tasks(&self, executor_id: &str) -> broadcast::Receiver<()> {
        self.state_machine
            .data
            .indexify_state
            .unfinished_tasks_by_executor
            .write()
            .unwrap()
            .entry(executor_id.to_string())
            .or_default()
            .subscribe()
    }

    /// Get all content from a namespace
    pub async fn list_content(
        &self,
        filter: impl Fn(&internal_api::ContentMetadata) -> bool,
        key_prefix: &[u8],
        key_reference: impl Fn(&[u8]) -> Result<Vec<u8>>,
        restart_key: Option<&[u8]>,
        limit: Option<u64>,
    ) -> Result<FilterResponse<internal_api::ContentMetadata>> {
        self.state_machine
            .list_content(filter, key_prefix, key_reference, restart_key, limit)
    }

    pub async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::RemoveExecutor {
                executor_id: executor_id.to_string(),
            },
            new_state_changes: vec![StateChange::new(
                executor_id.to_string(),
                ChangeType::ExecutorRemoved,
                timestamp_secs(),
            )],
            state_changes_processed: vec![],
        };
        self.forwardable_raft
            .client_write(req)
            .await
            .map_err(|e| anyhow!("unable to remove executor {}", e))?;
        Ok(())
    }

    pub async fn get_extraction_graph_links(
        &self,
        namespace: &str,
        graph_name: &str,
    ) -> Result<Vec<indexify_coordinator::ExtractionGraphLink>> {
        self.state_machine
            .get_extraction_graph_links(namespace, graph_name)
    }

    pub async fn link_graphs(&self, link: ExtractionGraphLink) -> Result<()> {
        let graphs = vec![link.node.graph_name.clone(), link.graph_name.clone()];
        let graphs = self.get_extraction_graphs_by_name(&link.node.namespace, &graphs)?;
        if graphs.len() != 2 || graphs[0].is_none() || graphs[1].is_none() {
            return Err(anyhow!(
                "unable to link extraction graphs: one or more graphs not found"
            ));
        }
        match link.node.source {
            ContentSource::Ingestion => (),
            ContentSource::ExtractionPolicyName(ref policy_name) => {
                if !graphs[0]
                    .as_ref()
                    .unwrap()
                    .extraction_policies
                    .iter()
                    .any(|policy| policy.name == *policy_name)
                {
                    return Err(anyhow!(
                        "unable to link extraction graphs: source extraction policy not found"
                    ));
                }
            }
        }
        if self.state_machine.creates_cycle(&link)? {
            return Err(anyhow!("unable to link extraction graphs: cycle detected"));
        }
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateExtractionGraphLink {
                extraction_graph_link: link,
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        self.forwardable_raft
            .client_write(req)
            .await
            .map_err(|e| anyhow!("unable to link extraction graphs: {}", e.to_string()))?;
        Ok(())
    }

    pub async fn create_compute_graph(&self, compute_graph: ComputeGraph) -> Result<()> {
        let existing_graph = self.state_machine.get_from_cf::<ComputeGraph, _>(
            StateMachineColumns::ComputeGraphs,
            &compute_graph.key(),
        )?;
        if existing_graph.is_some() {
            return Err(anyhow!(
                "compute graph {} already exists in namespace {}",
                compute_graph.name,
                compute_graph.namespace
            ));
        }
        let create_graph_request = CreateComputeGraphRequest { compute_graph };
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateComputeGraph(create_graph_request),
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        self.forwardable_raft
            .client_write(req)
            .await
            .map_err(|e| anyhow!("unable to create extraction graph: {}", e.to_string()))?;
        Ok(())
    }

    pub fn get_extraction_graphs_by_name(
        &self,
        namespace: &str,
        graph_names: &[impl AsRef<str>],
    ) -> Result<Vec<Option<ExtractionGraph>>> {
        self.state_machine
            .get_extraction_graphs_by_name(namespace, graph_names)
    }

    pub async fn update_task(
        &self,
        namespace: &str,
        compute_graph_name: &str,
        compute_fn_name: &str,
        task_id: &str,
        outcome: TaskOutcome,
        data_objects: Vec<DataObject>,
    ) -> Result<()> {
        let request = MarkTaskFinishedRequest {
            namespace: namespace.to_string(),
            compute_graph_name: compute_graph_name.to_string(),
            compute_fn_name: compute_fn_name.to_string(),
            task_id: task_id.to_string(),
            outcome,
            data_objects,
        };
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::UpdateTask(request),
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn get_graph_analytics(
        &self,
        namespace: &str,
        graph_name: &str,
    ) -> Result<Option<indexify_internal_api::ExtractionGraphAnalytics>> {
        self.state_machine
            .get_graph_analytics(namespace, graph_name)
            .await
    }

    pub fn extractor_with_name(
        &self,
        extractor: &str,
    ) -> Result<internal_api::ExtractorDescription> {
        let extractor = self
            .state_machine
            .get_from_cf::<internal_api::ExtractorDescription, _>(
                StateMachineColumns::Extractors,
                extractor,
            )?
            .ok_or_else(|| anyhow!("Extractor with name {} not found", extractor))?;
        Ok(extractor)
    }

    pub async fn create_namespace(&self, namespace: &str) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateNamespace {
                name: namespace.to_string(),
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        //  Fetch the namespaces from the db
        let namespaces: Vec<String> = self
            .state_machine
            .get_all_rows_from_cf::<String>(StateMachineColumns::Namespaces)
            .await?
            .into_iter()
            .map(|(key, _)| key)
            .collect();

        Ok(namespaces)
    }

    pub async fn namespace_exists(&self, namespace: &str) -> Result<bool> {
        self.state_machine.namespace_exists(namespace).await
    }

    pub async fn register_executor(&self, executor: ExecutorMetadata) -> Result<()> {
        let state_change = StateChange::new(
            executor.id.clone(),
            ChangeType::ExecutorAdded,
            timestamp_secs(),
        );
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::RegisterExecutor(executor),
            new_state_changes: vec![state_change.clone()],
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn get_executors(&self) -> Result<Vec<ExecutorMetadata>> {
        let executors: Vec<_> = self
            .state_machine
            .get_all_rows_from_cf::<ExecutorMetadata>(StateMachineColumns::Executors, self.state_machine.get_db())
            .await?
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        Ok(executors)
    }

    pub async fn get_executor_by_id(
        &self,
        executor_id: ExecutorIdRef<'_>,
    ) -> Result<ExecutorMetadata> {
        let executor = self
            .state_machine
            .get_from_cf::<ExecutorMetadata, _>(StateMachineColumns::Executors, executor_id)?
            .ok_or_else(|| anyhow!("Executor with id {} not found", executor_id))?;
        Ok(executor)
    }

    pub async fn commit_task_assignments(
        &self,
        assignments: HashMap<TaskId, ExecutorId>,
        state_change_id: StateChangeId,
    ) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::AssignTask { assignments },
            new_state_changes: vec![],
            state_changes_processed: vec![StateChangeProcessed {
                state_change_id,
                processed_at: timestamp_secs(),
            }],
        };
        self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    /// Get content based on id's without version. Will fetch the latest version
    /// for each one
    pub async fn get_content_metadata_batch(
        &self,
        content_ids: Vec<String>,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        self.state_machine.get_content_from_ids(content_ids).await
    }

    pub async fn create_tasks(
        &self,
        tasks: Vec<internal_api::Task>,
        state_change_id: StateChangeId,
    ) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateTasks { tasks },
            new_state_changes: vec![],
            state_changes_processed: vec![StateChangeProcessed {
                state_change_id,
                processed_at: timestamp_secs(),
            }],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn list_tasks<F>(
        &self,
        filter: F,
        start_id: Option<String>,
        limit: Option<u64>,
    ) -> Result<FilterResponse<internal_api::Task>>
    where
        F: Fn(&internal_api::Task) -> bool,
    {
        self.state_machine.list_tasks(filter, start_id, limit).await
    }

    pub async fn tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
    ) -> Result<Vec<internal_api::Task>> {
        let tasks = self
            .state_machine
            .get_tasks_for_executor(executor_id, limit)
            .await?;
        Ok(tasks)
    }

    pub async fn task_with_id(&self, task_id: &str) -> Result<internal_api::Task> {
        let task = self
            .state_machine
            .get_from_cf::<internal_api::Task, _>(StateMachineColumns::Tasks, task_id)?
            .ok_or_else(|| anyhow!("Task with id {} not found", task_id))?;
        Ok(task)
    }

    pub async fn list_state_changes(&self) -> Result<Vec<StateChange>> {
        let state_changes = self
            .state_machine
            .get_all_rows_from_cf::<StateChange>(StateMachineColumns::StateChanges)
            .await?
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        Ok(state_changes)
    }

    pub fn start_periodic_membership_check(self: &Arc<Self>, mut shutdown_rx: Receiver<()>) {
        let app_clone = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(MEMBERSHIP_CHECK_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("shutting down periodic membership check");
                        break;
                    }
                    _ = interval.tick() => {
                        if app_clone.is_seed_node() {
                            continue;
                        }
                        if let Err(e) = app_clone.check_cluster_membership().await {
                            error!("failed to check cluster membership: {}", e);
                        }
                    }
                }
            }
        });
    }

    pub async fn check_cluster_membership(
        &self,
    ) -> Result<store::requests::StateMachineUpdateResponse, anyhow::Error> {
        self.network
            .join_cluster(
                self.id,
                &self.node_addr,
                &self.coordinator_addr,
                &self.seed_node,
            )
            .await
    }

    pub fn get_raft_metrics(&self) -> RaftMetrics {
        let raft_metrics = raft_metrics::network::get_metrics_snapshot();
        let rx = self.forwardable_raft.raft.metrics();
        let openraft_metrics = rx.borrow().clone();

        RaftMetrics {
            openraft_metrics,
            raft_metrics,
        }
    }

    pub async fn subscribe_to_gc_task_events(&self) -> broadcast::Receiver<GarbageCollectionTask> {
        self.state_machine.subscribe_to_gc_task_events().await
    }

    pub async fn ensure_leader(&self) -> Result<Option<typ::ForwardToLeader>> {
        self.forwardable_raft.ensure_leader().await
    }

    pub async fn get_coordinator_addr(&self, node_id: NodeId) -> Result<Option<String>> {
        self.state_machine.get_coordinator_addr(node_id).await
    }
}

async fn watch_for_leader_change(
    forwardable_raft: ForwardableRaft,
    leader_change_tx: Sender<bool>,
    mut shutdown_rx: Receiver<()>,
) -> Result<()> {
    let mut rx = forwardable_raft.raft.metrics();
    let prev_server_state = RefCell::new(openraft::ServerState::Learner);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("shutting down leader change watcher");
                return Ok(());
            }
            _ = rx.changed() => {
                let server_state = rx.borrow_and_update().state;
                let mut prev_srvr_state = prev_server_state.borrow_mut();
                if !(prev_srvr_state).eq(&server_state) {
                    info!("raft change metrics prev {:?} current {:?}", prev_srvr_state, server_state);
                    let result = leader_change_tx.send(server_state.is_leader()).map_err(|e| anyhow!("unable to send leader change: {}", e));
                    match result {
                        Ok(_) => {}
                        Err(e) => {
                            error!("unable to send leader change: {}", e);
                        }
                    }
                    // replace the previous state with the new state
                    *prev_srvr_state = server_state;
                }
            }
        }
    }
}

