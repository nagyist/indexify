use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    time::SystemTime,
    vec,
};

use anyhow::{anyhow, Result};
use futures::Stream;
use indexify_internal_api::{
    self as internal_api,
    ContentMetadata,
    ExecutorMetadata,
    ExtractionGraphLink,
    ExtractionPolicy,
};
use indexify_proto::indexify_coordinator::{self, CreateContentStatus, HeartbeatResponse};
use internal_api::{
    ChangeType,
    ContentMetadataId,
    ExtractionGraph,
    GarbageCollectionTask,
    OutputSchema,
    ServerTaskType,
    StateChange,
    StateChangeId,
    StructuredDataSchema,
};
use tokio::sync::{broadcast, watch::Receiver};
use tracing::{debug, info, warn};

use crate::{
    api::NewContentStreamStart,
    coordinator_client::CoordinatorClient,
    coordinator_service::EXECUTOR_HEARTBEAT_PERIOD,
    forwardable_coordinator::ForwardableCoordinator,
    garbage_collector::GarbageCollector,
    metrics::Timer,
    scheduler::Scheduler,
    state::{
        store::{new_content_stream, requests::StateChangeProcessed, ExecutorId, FilterResponse},
        RaftMetrics,
        SharedState,
    },
    task_allocator::TaskAllocator,
    utils,
};

pub type ContentStream = Pin<Box<dyn Stream<Item = Result<ContentMetadata>> + Send + Sync>>;

pub struct Coordinator {
    pub shared_state: SharedState,
    scheduler: Scheduler,
    garbage_collector: Arc<GarbageCollector>,
    forwardable_coordinator: ForwardableCoordinator,
    /// Executors registered on this node.
    pub my_executors: std::sync::Mutex<HashSet<ExecutorId>>,

    /// All executors registered on the cluster.
    pub all_executors: std::sync::Mutex<HashMap<ExecutorId, SystemTime>>,
}

impl Coordinator {
    pub fn new(
        shared_state: SharedState,
        coordinator_client: CoordinatorClient,
        garbage_collector: Arc<GarbageCollector>,
    ) -> Arc<Self> {
        let task_allocator = TaskAllocator::new(shared_state.clone());
        let scheduler = Scheduler::new(shared_state.clone(), task_allocator);
        let forwardable_coordinator = ForwardableCoordinator::new(coordinator_client);
        Arc::new(Self {
            shared_state,
            scheduler,
            garbage_collector,
            forwardable_coordinator,
            my_executors: std::sync::Mutex::new(HashSet::new()),
            all_executors: std::sync::Mutex::new(HashMap::new()),
        })
    }

    pub async fn subscribe_to_new_tasks(&self, executor_id: &str) -> broadcast::Receiver<()> {
        self.shared_state.subscribe_to_new_tasks(executor_id).await
    }

    pub fn new_content_stream(&self, start: NewContentStreamStart) -> ContentStream {
        new_content_stream(self.shared_state.state_machine.clone(), start)
    }

    pub async fn get_extraction_graph_links(
        &self,
        namespace: &str,
        graph_name: &str,
    ) -> Result<Vec<indexify_coordinator::ExtractionGraphLink>> {
        self.shared_state
            .get_extraction_graph_links(namespace, graph_name)
            .await
    }

    pub async fn link_graphs(&self, link: ExtractionGraphLink) -> Result<()> {
        self.shared_state.link_graphs(link).await
    }

    pub fn get_locked_my_executors(&self) -> std::sync::MutexGuard<HashSet<String>> {
        self.my_executors.lock().unwrap()
    }

    pub fn get_locked_all_executors(&self) -> std::sync::MutexGuard<HashMap<String, SystemTime>> {
        self.all_executors.lock().unwrap()
    }

    pub async fn run_executor_heartbeat(&self, mut shutdown: Receiver<()>) {
        let mut watcher = self.get_leader_change_watcher();
        let mut interval = tokio::time::interval(EXECUTOR_HEARTBEAT_PERIOD);
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    break;
                }
                _ = interval.tick() => {
                    let _ = self.executor_heartbeat().await;
                }
                _ = watcher.changed() => {
                    // If this node becomes leader, reset last heartbeat values for
                    // all executors with current time.
                    let is_leader = *watcher.borrow_and_update();
                    if !is_leader {
                        self.get_locked_all_executors().clear();
                    }
                }
            }
        }
    }

    // If this node is follower, update the leader with the state of the executors
    // registered on this node. If this node is leader, process list of
    // all executors in the cluster and remove the stale executors.
    async fn executor_heartbeat(&self) -> Result<()> {
        if let Some(forward_to_leader) = self.shared_state.ensure_leader().await? {
            let leader_node_id = forward_to_leader
                .leader_id
                .ok_or_else(|| anyhow::anyhow!("could not get leader node id"))?;
            let leader_coord_addr = self
                .shared_state
                .get_coordinator_addr(leader_node_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("could not get leader node coordinator address"))?;
            let leader_coord_addr = format!("http://{}", leader_coord_addr);
            let my_executors = self.get_locked_my_executors().clone();
            self.forwardable_coordinator
                .executors_heartbeat(&leader_coord_addr, my_executors)
                .await?;
            return Ok(());
        }
        let remove_executors: Vec<_> = {
            let state_executors: HashSet<ExecutorId> = self
                .shared_state
                .get_executors()
                .await?
                .into_iter()
                .map(|e| e.id)
                .collect();
            let my_executors = self.get_locked_my_executors();
            let mut executors = self.get_locked_all_executors();
            let now = SystemTime::now();
            for executor_id in state_executors.iter() {
                if !executors.contains_key(executor_id) {
                    executors.insert(executor_id.clone(), now);
                }
            }
            for executor_id in my_executors.iter() {
                executors.insert(executor_id.clone(), now);
            }
            let mut deleted_executors = Vec::new();
            for executor_id in executors.keys() {
                if !state_executors.contains(executor_id) {
                    deleted_executors.push(executor_id.clone());
                }
            }
            for executor_id in deleted_executors {
                executors.remove(&executor_id);
            }
            executors
                .iter()
                .filter_map(|(executor_id, last_heartbeat)| {
                    match now.duration_since(*last_heartbeat) {
                        Ok(d) if d > 3 * EXECUTOR_HEARTBEAT_PERIOD => Some(executor_id.clone()),
                        _ => None,
                    }
                })
                .collect()
        };
        for executor_id in remove_executors {
            warn!("removing stale executor: {}", executor_id);
            self.shared_state.remove_executor(&executor_id).await?;
        }
        Ok(())
    }

    //  START CONVERSION METHODS
    pub fn internal_content_metadata_to_external(
        &self,
        content_list: Vec<internal_api::ContentMetadata>,
    ) -> Result<Vec<indexify_coordinator::ContentMetadata>> {
        let mut content_meta_list = Vec::new();
        for content in content_list {
            let content: indexify_coordinator::ContentMetadata = content.try_into()?;
            content_meta_list.push(content.clone());
        }
        Ok(content_meta_list)
    }

    pub fn external_content_metadata_to_internal(
        &self,
        content_list: Vec<indexify_coordinator::ContentMetadata>,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let mut contents = vec![];
        for content in content_list {
            let content: internal_api::ContentMetadata = content.try_into()?;
            contents.push(content.clone());
        }
        Ok(contents)
    }

    pub async fn list_content(
        &self,
        filter: impl Fn(&internal_api::ContentMetadata) -> bool,
        key_prefix: &[u8],
        key_reference: impl Fn(&[u8]) -> Result<Vec<u8>>,
        restart_key: Option<&[u8]>,
        limit: Option<u64>,
    ) -> Result<FilterResponse<internal_api::ContentMetadata>> {
        self.shared_state
            .list_content(filter, key_prefix, key_reference, restart_key, limit)
            .await
    }

    pub async fn get_extraction_policy(
        &self,
        namespace: &str,
        extraction_graph: &str,
        extraction_policy: &str,
    ) -> Result<internal_api::ExtractionPolicy> {
        let id = ExtractionPolicy::create_id(extraction_graph, extraction_policy, namespace);
        self.shared_state.get_extraction_policy(&id).await
    }

    pub async fn update_task(
        &self,
        task_id: &str,
        executor_id: &str,
        outcome: internal_api::TaskOutcome,
    ) -> Result<()> {
        info!(
            "updating task: {}, executor_id: {}, outcome: {:?}",
            task_id, executor_id, outcome
        );
        let mut task = self.shared_state.task_with_id(task_id).await?;
        task.outcome = outcome;
        self.shared_state
            .update_task(task, Some(executor_id.to_string()))
            .await?;
        Ok(())
    }

    pub async fn create_namespace(&self, namespace: &str) -> Result<()> {
        if self.shared_state.namespace_exists(namespace).await? {
            return Ok(());
        }
        self.shared_state.create_namespace(namespace).await
    }

    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        self.shared_state.list_namespaces().await
    }

    pub async fn list_extraction_graphs(&self, namespace: &str) -> Result<Vec<ExtractionGraph>> {
        self.shared_state.list_extraction_graphs(namespace).await
    }

    pub async fn list_extractors(&self) -> Result<Vec<internal_api::ExtractorDescription>> {
        self.shared_state.list_extractors().await
    }

    pub async fn heartbeat(
        &self,
        executor_id: &str,
        max_pending_tasks: u64,
    ) -> Result<HeartbeatResponse> {
        self.get_locked_my_executors()
            .insert(executor_id.to_string());

        let tasks = self
            .shared_state
            .tasks_for_executor(executor_id, Some(max_pending_tasks))
            .await?;
        let tasks = tasks
            .into_iter()
            .map(|task| -> Result<indexify_coordinator::Task> { task.try_into() })
            .collect::<Result<Vec<_>>>()?;
        Ok(HeartbeatResponse {
            executor_id: executor_id.to_string(),
            tasks,
        })
    }

    pub async fn all_task_assignments(&self) -> Result<HashMap<String, String>> {
        self.shared_state.task_assignments().await
    }

    pub async fn list_state_changes(&self) -> Result<Vec<internal_api::StateChange>> {
        self.shared_state.list_state_changes().await
    }

    pub async fn list_tasks<F>(
        &self,
        filter: F,
        start_id: Option<String>,
        limit: Option<u64>,
    ) -> Result<indexify_coordinator::ListTasksResponse>
    where
        F: Fn(&internal_api::Task) -> bool,
    {
        let response = self
            .shared_state
            .list_tasks(filter, start_id, limit)
            .await?;
        response.try_into()
    }

    pub async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        info!("removing executor: {}", executor_id);
        self.get_locked_my_executors().remove(executor_id);
        self.shared_state.remove_executor(executor_id).await?;
        Ok(())
    }

    pub async fn register_executor(&self, executor: ExecutorMetadata) -> Result<()> {
        self.shared_state.register_executor(executor).await
    }

    pub async fn register_ingestion_server(&self, ingestion_server_id: &str) -> Result<()> {
        if let Some(forward_to_leader) = self.shared_state.ensure_leader().await? {
            let leader_node_id = forward_to_leader
                .leader_id
                .ok_or_else(|| anyhow::anyhow!("could not get leader node id"))?;
            let leader_coord_addr = self
                .shared_state
                .get_coordinator_addr(leader_node_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("could not get leader node coordinator address"))?;
            let leader_coord_addr = format!("http://{}", leader_coord_addr);
            self.forwardable_coordinator
                .register_ingestion_server(&leader_coord_addr, ingestion_server_id)
                .await?;
            return Ok(());
        }
        self.garbage_collector.add_server(ingestion_server_id).await;
        Ok(())
    }

    pub async fn remove_ingestion_server(&self, ingestion_server_id: &str) -> Result<()> {
        if let Some(forward_to_leader) = self.shared_state.ensure_leader().await? {
            let leader_node_id = forward_to_leader
                .leader_id
                .ok_or_else(|| anyhow::anyhow!("could not get leader node id"))?;
            let leader_coord_addr = self
                .shared_state
                .get_coordinator_addr(leader_node_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("could not get leader node coordinator address"))?;
            self.forwardable_coordinator
                .remove_ingestion_server(&leader_coord_addr, ingestion_server_id)
                .await?;
            return Ok(());
        }
        self.garbage_collector
            .remove_server(ingestion_server_id)
            .await;
        Ok(())
    }

    pub async fn get_content_metadata(
        &self,
        content_ids: Vec<String>,
    ) -> Result<Vec<indexify_coordinator::ContentMetadata>> {
        let content = self
            .shared_state
            .get_content_metadata_batch(content_ids)
            .await?;
        let content = self.internal_content_metadata_to_external(content)?;
        Ok(content)
    }

    pub async fn get_task(&self, task_id: &str) -> Result<indexify_coordinator::Task> {
        let task = self.shared_state.task_with_id(task_id).await?;
        task.try_into()
    }

    pub async fn get_task_and_root_content(
        &self,
        task_id: &str,
    ) -> Result<(internal_api::Task, Option<internal_api::ContentMetadata>)> {
        let task = self.shared_state.task_with_id(task_id).await?;
        let mut root_content = None;
        if let Some(root_content_id) = &task.content_metadata.root_content_id {
            let root_cm = self
                .shared_state
                .get_content_metadata_batch(vec![root_content_id.clone()])
                .await?;
            if let Some(root_cm) = root_cm.first() {
                root_content.replace(root_cm.clone());
            }
        }
        Ok((task, root_content))
    }

    pub async fn get_content_tree_metadata(
        &self,
        content_id: &str,
    ) -> Result<Vec<indexify_coordinator::ContentMetadata>> {
        let content_tree = self.shared_state.get_content_tree_metadata(content_id)?;
        let content_tree = self.internal_content_metadata_to_external(content_tree)?;
        Ok(content_tree)
    }

    pub fn get_extractor(
        &self,
        extractor_name: &str,
    ) -> Result<internal_api::ExtractorDescription> {
        self.shared_state.extractor_with_name(extractor_name)
    }

    pub async fn create_extraction_graph(&self, extraction_graph: ExtractionGraph) -> Result<()> {
        Ok(())
    }

    pub async fn get_graph_analytics(
        &self,
        namespace: &str,
        graph_name: &str,
    ) -> Result<Option<indexify_internal_api::ExtractionGraphAnalytics>> {
        self.shared_state
            .get_graph_analytics(namespace, graph_name)
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn run_scheduler(&self) -> Result<()> {
        let _timer = Timer::start(&self.shared_state.metrics.scheduler_invocations);

        let state_changes = self.shared_state.unprocessed_state_change_events().await?;
        for change in state_changes {
            debug!(
                "processing change event: {}, type: {}, id: {}",
                change.id, change.change_type, change.object_id
            );

            match change.change_type {
                ChangeType::InvokeComputeGraph(payload) => {
                    self.scheduler
                        .invoke_compute_graph(&payload, change.id)
                        .await?
                }
                ChangeType::TaskCompleted {
                    ref root_content_id,
                } => {}
                ChangeType::ExecutorAdded => self.scheduler.redistribute_tasks(&change).await?,
                ChangeType::NewContent => {}
                ChangeType::ExecutorRemoved => {
                    self.scheduler.handle_executor_removed(change).await?
                }
            }
        }
        Ok(())
    }

    pub async fn subscribe_to_gc_events(&self) -> broadcast::Receiver<GarbageCollectionTask> {
        self.shared_state.subscribe_to_gc_task_events().await
    }

    pub fn get_state_watcher(&self) -> Receiver<StateChangeId> {
        self.shared_state.get_state_change_watcher()
    }

    pub fn get_leader_change_watcher(&self) -> Receiver<bool> {
        self.shared_state.leader_change_rx.clone()
    }

    pub fn get_raft_metrics(&self) -> RaftMetrics {
        self.shared_state.get_raft_metrics()
    }

    pub async fn wait_content_extraction(&self, content_id: &str) {
        self.shared_state
            .state_machine
            .data
            .indexify_state
            .wait_root_task_count_zero(content_id)
            .await
    }
}
