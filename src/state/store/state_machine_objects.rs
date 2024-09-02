use core::fmt;
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Arc, RwLock},
    time::SystemTime,
};
use tracing::error;

use anyhow::{anyhow, Result};
use indexify_internal_api::{
    self as internal_api,
    ContentOffset,
    ExtractionGraphNode,
    ServerTaskType,
    TaskAnalytics,
};
use internal_api::{
    ContentMetadata,
    ContentMetadataId,
    ContentSource,
    ExecutorMetadata,
    ExtractionGraph,
    ExtractionGraphLink,
    ExtractionPolicy,
    ExtractionPolicyName,
    ExtractorDescription,
    StateChange,
    StructuredDataSchema,
    Task,
    TaskOutcome,
};
use itertools::Itertools;
use opentelemetry::metrics::AsyncInstrument;
use rocksdb::{IteratorMode, OptimisticTransactionDB, ReadOptions, Transaction, Direction};
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;
use tracing::warn;

use super::{
    requests::{
        CreateComputeGraphRequest,
        InvokeComputeGraphRequest,
        RequestPayload,
        StateChangeProcessed,
        StateMachineUpdateRequest,
        DeleteComputeGraphRequest,
        TombstoneIngestedDataObjectRequest,
    },
    serializer::JsonEncode,
    ExecutorId,
    ExtractionPolicyId,
    ExtractorName,
    JsonEncoder,
    NamespaceName,
    SchemaId,
    StateChangeId,
    StateMachineColumns,
    StateMachineError,
    TaskId,
};
use crate::state::{store::get_from_cf, NodeId};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct UnassignedTasks {
    unassigned_tasks: Arc<RwLock<HashMap<TaskId, SystemTime>>>,
}

impl UnassignedTasks {
    pub fn insert(&self, task_id: &TaskId, creation_time: SystemTime) {
        let mut guard = self.unassigned_tasks.write().unwrap();
        guard.insert(task_id.into(), creation_time);
    }

    pub fn remove(&self, task_id: &TaskId) -> Option<UnfinishedTask> {
        let mut guard = self.unassigned_tasks.write().unwrap();
        guard.remove(task_id).map(|creation_time| UnfinishedTask {
            id: task_id.clone(),
            creation_time,
        })
    }

    pub fn inner(&self) -> HashMap<TaskId, SystemTime> {
        let guard = self.unassigned_tasks.read().unwrap();
        guard.clone()
    }

    pub fn set(&self, tasks: HashMap<TaskId, SystemTime>) {
        let mut guard = self.unassigned_tasks.write().unwrap();
        *guard = tasks;
    }

    pub fn count(&self) -> usize {
        let guard = self.unassigned_tasks.read().unwrap();
        guard.len()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct UnprocessedStateChanges {
    unprocessed_state_changes: Arc<RwLock<HashSet<StateChangeId>>>,
}

impl UnprocessedStateChanges {
    pub fn insert(&self, state_change_id: StateChangeId) {
        let mut guard = self.unprocessed_state_changes.write().unwrap();
        guard.insert(state_change_id);
    }

    pub fn remove(&self, state_change_id: &StateChangeId) {
        let mut guard = self.unprocessed_state_changes.write().unwrap();
        guard.remove(state_change_id);
    }

    pub fn inner(&self) -> HashSet<StateChangeId> {
        let guard = self.unprocessed_state_changes.read().unwrap();
        guard.clone()
    }
}

impl From<HashSet<StateChangeId>> for UnprocessedStateChanges {
    fn from(state_changes: HashSet<StateChangeId>) -> Self {
        let unprocessed_state_changes = Arc::new(RwLock::new(state_changes));
        Self {
            unprocessed_state_changes,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ContentNamespaceTable {
    content_namespace_table: Arc<RwLock<HashMap<NamespaceName, HashSet<ContentMetadataId>>>>,
}

impl ContentNamespaceTable {
    pub fn insert(&self, namespace: &NamespaceName, content_id: &ContentMetadataId) {
        let mut guard = self.content_namespace_table.write().unwrap();
        guard
            .entry(namespace.clone())
            .or_default()
            .insert(content_id.clone());
    }

    pub fn remove(&self, namespace: &NamespaceName, content_id: &ContentMetadataId) {
        let mut guard = self.content_namespace_table.write().unwrap();
        guard
            .entry(namespace.clone())
            .or_default()
            .remove(content_id);
    }

    pub fn inner(&self) -> HashMap<NamespaceName, HashSet<ContentMetadataId>> {
        let guard = self.content_namespace_table.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<NamespaceName, HashSet<ContentMetadataId>>> for ContentNamespaceTable {
    fn from(content_namespace_table: HashMap<NamespaceName, HashSet<ContentMetadataId>>) -> Self {
        let content_namespace_table = Arc::new(RwLock::new(content_namespace_table));
        Self {
            content_namespace_table,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ExecutorRunningTaskCount {
    executor_running_task_count: Arc<RwLock<HashMap<ExecutorId, u64>>>,
}

impl ExecutorRunningTaskCount {
    pub fn new() -> Self {
        Self {
            executor_running_task_count: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get(&self, executor_id: &ExecutorId) -> Option<u64> {
        let guard = self.executor_running_task_count.read().unwrap();
        guard.get(executor_id).copied()
    }

    pub fn insert(&self, executor_id: &ExecutorId, count: u64) {
        let mut guard = self.executor_running_task_count.write().unwrap();
        guard.insert(executor_id.clone(), count);
    }

    pub fn remove(&self, executor_id: &ExecutorId) {
        let mut guard = self.executor_running_task_count.write().unwrap();
        guard.remove(executor_id);
    }

    pub fn inner(&self) -> HashMap<ExecutorId, u64> {
        let guard = self.executor_running_task_count.read().unwrap();
        guard.clone()
    }

    pub fn increment_running_task_count(&self, executor_id: &ExecutorId) {
        let mut executor_load = self.executor_running_task_count.write().unwrap();
        let load = executor_load.entry(executor_id.clone()).or_insert(0);
        *load += 1;
    }

    pub fn decrement_running_task_count(&self, executor_id: &ExecutorId) {
        let mut executor_load = self.executor_running_task_count.write().unwrap();
        if let Some(load) = executor_load.get_mut(executor_id) {
            if *load > 0 {
                *load -= 1;
            } else {
                warn!("Tried to decrement load below 0. This is a bug because the state machine shouldn't allow it.");
            }
        } else {
            // Add the executor to the load map if it's not there, with an initial load of
            // 0.
            executor_load.insert(executor_id.clone(), 0);
        }
    }

    pub fn executor_count(&self) -> usize {
        let guard = self.executor_running_task_count.read().unwrap();
        guard.len()
    }
}

impl From<HashMap<ExecutorId, u64>> for ExecutorRunningTaskCount {
    fn from(executor_running_task_count: HashMap<ExecutorId, u64>) -> Self {
        let executor_running_task_count = Arc::new(RwLock::new(executor_running_task_count));
        Self {
            executor_running_task_count,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct Metrics {
    /// Number of tasks total
    pub tasks_completed: u64,

    /// Number of tasks completed with errors
    pub tasks_completed_with_errors: u64,

    /// Number of contents uploaded
    pub content_uploads: u64,

    /// Total number of bytes in uploaded contents
    pub content_bytes: u64,

    /// Number of contents extracted
    pub content_extracted: u64,

    /// Total number of bytes in extracted contents
    pub content_extracted_bytes: u64,
}

impl Metrics {
    pub fn update_task_completion(&mut self, outcome: TaskOutcome) {
        match outcome {
            TaskOutcome::Success => self.tasks_completed += 1,
            TaskOutcome::Failed => self.tasks_completed_with_errors += 1,
            _ => (),
        }
    }
}

#[derive(Debug, Default)]
struct TaskCount {
    count: u64,
    notify: Option<broadcast::Sender<()>>,
}

#[derive(Debug, Clone)]
pub struct UnfinishedTask {
    pub id: TaskId,
    pub creation_time: SystemTime,
}

impl PartialEq for UnfinishedTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for UnfinishedTask {}

impl PartialOrd for UnfinishedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Sort unfinished tasks by creation time so we can process them in order
impl Ord for UnfinishedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.creation_time
            .cmp(&other.creation_time)
            .then_with(|| self.id.cmp(&other.id))
    }
}

fn max_content_offset(db: &OptimisticTransactionDB) -> Result<u64, StateMachineError> {
    let mut iter = db.iterator_cf(
        StateMachineColumns::ChangeIdContentIndex.cf(db),
        IteratorMode::End,
    );
    match iter.next() {
        Some(Ok((key, _))) => {
            let key_bytes: Result<[u8; 8], _> = key.as_ref().try_into();
            match key_bytes {
                Ok(bytes) => Ok(u64::from_be_bytes(bytes) + 1),
                Err(e) => Err(StateMachineError::DatabaseError(format!(
                    "Failed to decode key: {}",
                    e
                ))),
            }
        }
        Some(Err(e)) => Err(StateMachineError::DatabaseError(format!(
            "Failed to read content index: {}",
            e
        ))),
        None => Ok(1),
    }
}

#[derive(Debug)]
pub struct ExecutorUnfinishedTasks {
    pub tasks: BTreeSet<UnfinishedTask>,
    pub new_task_channel: broadcast::Sender<()>,
}

impl ExecutorUnfinishedTasks {
    pub fn new() -> Self {
        let (new_task_channel, _) = broadcast::channel(1);
        Self {
            tasks: BTreeSet::new(),
            new_task_channel,
        }
    }

    pub fn insert(&mut self, task: UnfinishedTask) {
        self.tasks.insert(task);
        let _ = self.new_task_channel.send(());
    }

    // Send notification for remove because new task can be available if
    // were at maximum number of tasks before task completion.
    pub fn remove(&mut self, task: &UnfinishedTask) {
        self.tasks.remove(task);
        let _ = self.new_task_channel.send(());
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.new_task_channel.subscribe()
    }
}

impl Default for ExecutorUnfinishedTasks {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(thiserror::Error, Debug)]
pub struct IndexifyState {
    // Reverse Indexes
    /// The tasks that are currently unassigned
    pub unassigned_tasks: UnassignedTasks,

    /// State changes that have not been processed yet
    pub unprocessed_state_changes: UnprocessedStateChanges,

    /// Pending tasks per executor, sorted by creation time
    pub unfinished_tasks_by_executor: RwLock<HashMap<ExecutorId, ExecutorUnfinishedTasks>>,

    /// Metrics
    pub metrics: std::sync::Mutex<Metrics>,

    /// Next change id
    pub change_id: std::sync::Mutex<u64>,

    /// Graph+ContentSource->Policy Id
    pub graph_links: Arc<RwLock<HashMap<ExtractionGraphNode, HashSet<ExtractionPolicyId>>>>,

    pub new_content_channel: broadcast::Sender<()>,

    /// Next content offset
    pub content_offset: std::sync::Mutex<ContentOffset>,
}

impl fmt::Display for IndexifyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IndexifyState {{ unassigned_tasks: {:?}, unprocessed_state_changes: {:?}, extractor_executors_table: {:?}, namespace_index_table: {:?}, unfinished_tasks_by_extractor: {:?}, unfinished_tasks_by_executor: {:?}, schemas_by_namespace: {:?} }}, content_children_table: {:?}",
            self.unassigned_tasks,
            self.unprocessed_state_changes,
            self.unfinished_tasks_by_executor,
        )
    }
}

impl Default for IndexifyState {
    fn default() -> Self {
        let new_content_channel = broadcast::channel(1).0;
        Self {
            unassigned_tasks: Default::default(),
            unprocessed_state_changes: Default::default(),
            unfinished_tasks_by_executor: Default::default(),
            metrics: Default::default(),
            change_id: Default::default(),
            graph_links: Default::default(),
            new_content_channel,
            content_offset: std::sync::Mutex::new(ContentOffset(1)),
        }
    }
}

impl IndexifyState {
    pub fn next_content_offset(&self) -> ContentOffset {
        let mut offset_guard = self.content_offset.lock().unwrap();
        let offset = *offset_guard;
        *offset_guard = offset.next();
        offset
    }

    // Complete value of link is stored in db key since multiple graphs
    // can be linked to the same node.
    fn set_extraction_graph_link(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        link: &ExtractionGraphLink,
    ) -> Result<(), StateMachineError> {
        let key = JsonEncoder::encode(link)?;
        txn.put_cf(&StateMachineColumns::ExtractionGraphLinks.cf(db), key, [])
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))
    }

    fn create_compute_graph(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        compute_graph: &CreateComputeGraphRequest,
    ) -> Result<(), StateMachineError> {
        let key = compute_graph.compute_graph.key();
        let serialized_cg = JsonEncoder::encode(compute_graph)?;
        txn.put_cf(
            &StateMachineColumns::ComputeGraphs.cf(db),
            key,
            serialized_cg,
        )
        .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    fn set_new_state_changes(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        state_changes: &mut Vec<StateChange>,
    ) -> Result<(), StateMachineError> {
        let mut change_id = self.get_next_change_ids(state_changes.len());
        for change in state_changes {
            change.id = StateChangeId::new(change_id);
            change_id += 1;
            let serialized_change = JsonEncoder::encode(&change)?;
            txn.put_cf(
                StateMachineColumns::StateChanges.cf(db),
                change.id.to_key(),
                &serialized_change,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    fn set_processed_state_changes(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        state_changes: &Vec<StateChangeProcessed>,
    ) -> Result<Vec<StateChange>, StateMachineError> {
        let state_changes_cf = StateMachineColumns::StateChanges.cf(db);
        let mut changes = Vec::new();

        for change in state_changes {
            let key = change.state_change_id.to_key();
            let result = txn
                .get_cf(state_changes_cf, key)
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            let result = result.ok_or_else(|| {
                StateMachineError::DatabaseError(format!(
                    "State change {:?} not found",
                    change.state_change_id
                ))
            })?;

            let mut state_change = JsonEncoder::decode::<StateChange>(&result)?;
            state_change.processed_at = Some(change.processed_at);
            let serialized_change = JsonEncoder::encode(&state_change)?;
            txn.put_cf(state_changes_cf, key, &serialized_change)
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            changes.push(state_change);
        }
        Ok(changes)
    }

    fn create_tasks(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        tasks: Vec<Task>,
    ) -> Result<(), StateMachineError> {
        for task in tasks {
            // Write tasks
            let serialized_task = JsonEncoder::encode(&task)?;
            txn.put_cf(
                StateMachineColumns::Tasks.cf(db),
                task.key(),
                &serialized_task,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            // Update task analytics 
            let key = format!("{}_{}_{}", task.namespace, task.compute_graph_name, task.ingestion_data_object_id);
            let graph_ctx = txn.get_cf(StateMachineColumns::GraphInvocationCtx.cf(db), key).map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            if graph_ctx.is_none() {
                error!("Graph context not found for task: {}", task.key());
                continue;
            }
            let graph_ctx: GraphInvocationCtx = JsonEncoder::decode(&graph_ctx.unwrap())?;

        }
        Ok(())
    }

    fn get_task_assignments_for_executor(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        executor_id: &str,
    ) -> Result<HashSet<TaskId>, StateMachineError> {
        let value = txn
            .get_cf(StateMachineColumns::TaskAssignments.cf(db), executor_id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error reading task assignments: {}", e))
            })?;
        match value {
            Some(existing_value) => {
                let existing_value: HashSet<TaskId> = JsonEncoder::decode(&existing_value)
                    .map_err(|e| {
                        StateMachineError::DatabaseError(format!(
                            "Error deserializing task assignments: {}",
                            e
                        ))
                    })?;
                Ok(existing_value)
            }
            None => Ok(HashSet::new()),
        }
    }

    /// Set the list of tasks that have been assigned to some executor
    fn set_task_assignments(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        task_assignments: &HashMap<String, HashSet<TaskId>>,
    ) -> Result<(), StateMachineError> {
        let task_assignment_cf = StateMachineColumns::TaskAssignments.cf(db);
        for (executor_id, task_ids) in task_assignments {
            txn.put_cf(
                task_assignment_cf,
                executor_id,
                JsonEncoder::encode(&task_ids)?,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error writing task assignments: {}", e))
            })?;
        }
        Ok(())
    }

    // FIXME USE MULTI-GET HERE
    fn delete_task_assignments_for_executor(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        executor_id: &str,
    ) -> Result<Vec<TaskId>, StateMachineError> {
        let task_assignment_cf = StateMachineColumns::TaskAssignments.cf(db);
        let task_ids: Vec<TaskId> = txn
            .get_cf(task_assignment_cf, executor_id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error reading task assignments for executor: {}",
                    e
                ))
            })?
            .map(|db_vec| {
                JsonEncoder::decode(&db_vec).map_err(|e| {
                    StateMachineError::DatabaseError(format!(
                        "Error deserializing task assignments for executor: {}",
                        e
                    ))
                })
            })
            .unwrap_or_else(|| Ok(Vec::new()))?;

        txn.delete_cf(task_assignment_cf, executor_id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error deleting task assignments for executor: {}",
                    e
                ))
            })?;

        Ok(task_ids)
    }

    fn update_graphs<'a>(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        contents_iter: impl IntoIterator<Item = &'a ContentMetadata> + Clone,
    ) -> Result<(), StateMachineError> {
        let cf = StateMachineColumns::ContentTable.cf(db);
        let keys = contents_iter
            .clone()
            .into_iter()
            .map(|content| (cf, content.id.id.clone().into_bytes()))
            .collect::<Vec<_>>();
        let mut existing_contents = HashMap::new();
        for item in txn.multi_get_cf(keys) {
            if let Some(item) = item.map_err(|e| {
                StateMachineError::DatabaseError(format!("failed to read existing content: {}", e))
            })? {
                let content: ContentMetadata = JsonEncoder::decode(&item)?;
                existing_contents.insert(content.id.id.clone(), content);
            }
        }
        for content in contents_iter {
            if let Some(existing_content) = existing_contents.get(&content.id.id) {
                for graph in existing_content.extraction_graph_names.iter() {
                    if !content.extraction_graph_names.contains(graph) {
                        txn.delete_cf(
                            StateMachineColumns::GraphContentIndex.cf(db),
                            existing_content.graph_key(graph),
                        )
                        .map_err(|e| {
                            StateMachineError::DatabaseError(format!(
                                "error writing content: {}",
                                e
                            ))
                        })?;
                    }
                }
            }
            for graph in content.extraction_graph_names.iter() {
                txn.put_cf(
                    StateMachineColumns::GraphContentIndex.cf(db),
                    content.graph_key(graph),
                    [],
                )
                .map_err(|e| {
                    StateMachineError::DatabaseError(format!("error writing content: {}", e))
                })?;
            }
        }
        Ok(())
    }

    fn set_content<'a>(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        contents_vec: impl IntoIterator<Item = &'a ContentMetadata>,
    ) -> Result<(), StateMachineError> {
        for content in contents_vec {
            let mut content = content.clone();
            content.change_offset = self.next_content_offset();
            let serialized_content = JsonEncoder::encode(&content)?;
            txn.put_cf(
                StateMachineColumns::ContentTable.cf(db),
                content.id_key(),
                &serialized_content,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("error writing content: {}", e))
            })?;
            if content.latest {
                txn.put_cf(
                    StateMachineColumns::ChangeIdContentIndex.cf(db),
                    content.change_offset.0.to_be_bytes(),
                    &content.id.id,
                )
                .map_err(|e| {
                    StateMachineError::DatabaseError(format!("error writing content: {}", e))
                })?;
                for graph in content.extraction_graph_names.iter() {
                    txn.put_cf(
                        StateMachineColumns::GraphContentIndex.cf(db),
                        content.graph_key(graph),
                        [],
                    )
                    .map_err(|e| {
                        StateMachineError::DatabaseError(format!("error writing content: {}", e))
                    })?;
                }
            }
        }
        Ok(())
    }

    fn tombstone_content_tree<'a>(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        content_metadata: impl IntoIterator<Item = &'a ContentMetadata>,
    ) -> Result<(), StateMachineError> {
        for content in content_metadata {
            let cf = StateMachineColumns::ContentTable.cf(db);
            let mut content = content.clone();
            // If updating latest version of root node, the key will change so delete from
            // previous location.
            if content.latest {
                if content.parent_id.is_none() {
                    txn.delete_cf(cf, &content.id.id)
                        .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;
                    content.latest = false;
                }
                for graph in content.extraction_graph_names.iter() {
                    txn.delete_cf(
                        StateMachineColumns::GraphContentIndex.cf(db),
                        content.graph_key(graph),
                    )
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;
                }
            }
            let serialized_content = JsonEncoder::encode(&content)?;
            txn.put_cf(cf, content.id_key(), &serialized_content)
                .map_err(|e| {
                    StateMachineError::DatabaseError(format!("error writing content: {}", e))
                })?;
        }

        Ok(())
    }

    /// Function to delete content based on content ids
    fn delete_content(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        content_id: ContentMetadataId,
        latest: bool,
        change_offset: ContentOffset,
    ) -> Result<(), StateMachineError> {
        let key = if latest {
            content_id.id.clone()
        } else {
            format!("{}::v{}", content_id.id.clone(), content_id.version)
        };
        let res = txn
            .get_cf(StateMachineColumns::ContentTable.cf(db), &key)
            .map_err(|e| {
                StateMachineError::TransactionError(format!(
                    "error in txn while trying to delete content: {}",
                    e
                ))
            })?;
        if res.is_none() {
            warn!("Content with id {} not found", content_id);
        }
        txn.delete_cf(StateMachineColumns::ContentTable.cf(db), &key)
            .map_err(|e| {
                StateMachineError::TransactionError(format!(
                    "error in txn while trying to delete content: {}",
                    e
                ))
            })?;
        txn.delete_cf(
            StateMachineColumns::ChangeIdContentIndex.cf(db),
            change_offset.0.to_be_bytes(),
        )
        .map_err(|e| {
            StateMachineError::TransactionError(format!(
                "error in txn while trying to delete content: {}",
                e
            ))
        })?;
        Ok(())
    }

    fn set_executor(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        executor: &ExecutorMetadata,
    ) -> Result<(), StateMachineError> {
        let serialized_executor = JsonEncoder::encode(executor)?;
        txn.put_cf(
            StateMachineColumns::Executors.cf(db),
            &executor.id,
            serialized_executor,
        )
        .map_err(|e| StateMachineError::DatabaseError(format!("Error writing executor: {}", e)))?;
        Ok(())
    }

    fn delete_executor(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        executor_id: &str,
    ) -> Result<Option<ExecutorMetadata>, StateMachineError> {
        //  Get a handle on the executor before deleting it from the DB
        let executors_cf = StateMachineColumns::Executors.cf(db);
        match txn.get_cf(executors_cf, executor_id).map_err(|e| {
            StateMachineError::DatabaseError(format!("Error reading executor: {}", e))
        })? {
            Some(executor) => {
                let executor_meta = JsonEncoder::decode::<ExecutorMetadata>(&executor)?;
                txn.delete_cf(executors_cf, executor_id).map_err(|e| {
                    StateMachineError::DatabaseError(format!("Error deleting executor: {}", e))
                })?;
                Ok(Some(executor_meta))
            }
            None => Ok(None),
        }
    }

    fn set_extractors(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        extractors: &Vec<ExtractorDescription>,
    ) -> Result<(), StateMachineError> {
        for extractor in extractors {
            let serialized_extractor = JsonEncoder::encode(extractor)?;
            txn.put_cf(
                StateMachineColumns::Extractors.cf(db),
                &extractor.name,
                serialized_extractor,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error writing extractor: {}", e))
            })?;
        }
        Ok(())
    }

    fn set_extraction_policy(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        extraction_policy: &ExtractionPolicy,
    ) -> Result<(), StateMachineError> {
        let serialized_extraction_policy = JsonEncoder::encode(extraction_policy)?;
        txn.put_cf(
            &StateMachineColumns::ExtractionPolicies.cf(db),
            extraction_policy.id.clone(),
            serialized_extraction_policy,
        )
        .map_err(|e| {
            StateMachineError::DatabaseError(format!("Error writing extraction policy: {}", e))
        })?;
        Ok(())
    }
    
    fn tombstone_ingested_data_object(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        request: &TombstoneIngestedDataObjectRequest,
    ) -> Result<(), StateMachineError> {
        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);
        let ingested_object_key_prefix= format!("{}_{}_", request.namespace, request.compute_graph_name);
        let fn_output_key_prefix = format!("{}_{}_{}", request.namespace, request.compute_graph_name, request.id);
        let mode = IteratorMode::From(ingested_object_key_prefix.as_bytes(), Direction::Forward);
        let itr = db.iterator_cf_opt(StateMachineColumns::IngestedDataObjects.cf(db), read_options, mode);
        for kv in itr {
            let (key, _) = kv.map_err(|e| StateMachineError::DatabaseError(format!("Error reading ingested data object: {}", e)))?;
            let key = String::from_utf8(key.to_vec()).map_err(|e| StateMachineError::DatabaseError(format!("Error reading ingested data object: {}", e)))?;
            txn.delete_cf(StateMachineColumns::IngestedDataObjects.cf(db), key)
                .map_err(|e| StateMachineError::DatabaseError(format!("Error deleting ingested data object: {}", e)))?;
        }
        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);
        let mode = IteratorMode::From(fn_output_key_prefix.as_bytes(), Direction::Forward);
        let itr = db.iterator_cf_opt(StateMachineColumns::DataObjects.cf(db), read_options, mode);
        for kv in itr {
            let (key, _) = kv.map_err(|e| StateMachineError::DatabaseError(format!("Error reading ingested data object: {}", e)))?;
            let key = String::from_utf8(key.to_vec()).map_err(|e| StateMachineError::DatabaseError(format!("Error reading ingested data object: {}", e)))?;
            txn.delete_cf(StateMachineColumns::IngestedDataObjects.cf(db), key)
                .map_err(|e| StateMachineError::DatabaseError(format!("Error deleting ingested data object: {}", e)))?;
        }
        Ok(())
    }

    fn set_namespace(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        namespace: &NamespaceName,
    ) -> Result<(), StateMachineError> {
        let serialized_name = JsonEncoder::encode(namespace)?;
        txn.put_cf(
            &StateMachineColumns::Namespaces.cf(db),
            namespace,
            serialized_name,
        )
        .map_err(|e| StateMachineError::DatabaseError(format!("Error writing namespace: {}", e)))?;
        Ok(())
    }

    pub fn update_content_extraction_policy_state(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        content_id: &ContentMetadataId,
        extraction_policy_id: &str,
        policy_completion_time: SystemTime,
    ) -> Result<(), StateMachineError> {
        let value = txn
            .get_cf(StateMachineColumns::ContentTable.cf(db), &content_id.id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error getting the content policies applied on content id {}: {}",
                    content_id, e
                ))
            })?
            .ok_or_else(|| {
                StateMachineError::DatabaseError(format!(
                    "Content not found while updating applied extraction policies {}",
                    content_id
                ))
            })?;
        let mut content_meta = JsonEncoder::decode::<ContentMetadata>(&value)?;
        let epoch_time = policy_completion_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error converting policy completion time to u64: {}",
                    e
                ))
            })?
            .as_secs();
        content_meta
            .extraction_policy_ids
            .insert(extraction_policy_id.to_string(), epoch_time);
        let data = JsonEncoder::encode(&content_meta)?;
        txn.put_cf(
            StateMachineColumns::ContentTable.cf(db),
            &content_id.id,
            data,
        )
        .map_err(|e| {
            StateMachineError::DatabaseError(format!(
                "Error writing content policies applied on content for id {}: {}",
                content_id, e
            ))
        })?;

        Ok(())
    }

    pub fn set_coordinator_addr(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        node_id: NodeId,
        coordinator_addr: &str,
    ) -> Result<(), StateMachineError> {
        let serialized_coordinator_addr = JsonEncoder::encode(&coordinator_addr)?;
        txn.put_cf(
            StateMachineColumns::CoordinatorAddress.cf(db),
            node_id.to_string(),
            serialized_coordinator_addr,
        )
        .map_err(|e| {
            StateMachineError::DatabaseError(format!(
                "Error writing coordinator address for node {}: {}",
                node_id, e
            ))
        })?;
        Ok(())
    }

    pub fn mark_state_changes_processed(
        &self,
        state_change: &StateChangeProcessed,
        _processed_at: u64,
    ) {
        self.unprocessed_state_changes
            .remove(&state_change.state_change_id);
    }

    fn update_extraction_graph_reverse_idx(&self, extraction_graph: &ExtractionGraph) {
        for ep in &extraction_graph.extraction_policies {
            let key = ExtractionGraphNode {
                namespace: ep.namespace.clone(),
                graph_name: extraction_graph.name.clone(),
                source: ep.content_source.clone(),
            };
            self.graph_links
                .write()
                .unwrap()
                .entry(key)
                .or_default()
                .insert(ep.id.clone());
        }
    }

    /// This method will make all state machine forward index writes to RocksDB
    pub fn apply_state_machine_updates(
        &self,
        mut request: StateMachineUpdateRequest,
        db: &OptimisticTransactionDB,
        txn: Transaction<OptimisticTransactionDB>,
    ) -> Result<Option<StateChangeId>, StateMachineError> {
        self.set_new_state_changes(db, &txn, &mut request.new_state_changes)?;
        let mut state_changes_processed =
            self.set_processed_state_changes(db, &txn, &request.state_changes_processed)?;

        match &request.payload {
            RequestPayload::InvokeComputeGraph(invoke_compute_graph_request) => {
                self.invoke_compute_graph(db, &txn, invoke_compute_graph_request)?;
            }
            RequestPayload::DeleteComputeGraph(request) => {
                self.delete_compute_graph(db, &txn, request)?;
            }
            RequestPayload::CreateTasks { tasks } => {
                self.update_tasks(db, &txn, tasks.clone(), SystemTime::UNIX_EPOCH)?;
            }
            RequestPayload::AssignTask { assignments } => {
                let assignments: HashMap<&String, HashSet<TaskId>> =
                    assignments
                        .iter()
                        .fold(HashMap::new(), |mut acc, (task_id, executor_id)| {
                            acc.entry(executor_id).or_default().insert(task_id.clone());
                            acc
                        });

                // FIXME - Write a test which assigns tasks mutliple times to the same executor
                // and make sure it's additive.

                for (executor_id, tasks) in assignments.iter() {
                    let mut existing_tasks =
                        self.get_task_assignments_for_executor(db, &txn, executor_id)?;
                    existing_tasks.extend(tasks.clone());
                    let task_assignment =
                        HashMap::from([(executor_id.to_string(), existing_tasks)]);
                    self.set_task_assignments(db, &txn, &task_assignment)?;
                }
            }
            RequestPayload::UpdateTask(request) => {
                self.update_tasks(db, &txn, request)?;
            }             
            RequestPayload::RegisterExecutor(executor) => {
                //  Insert the executor
                self.set_executor(db, &txn, executor)?;
            }
            RequestPayload::RemoveExecutor { executor_id } => {
                self.delete_executor(db, &txn, executor_id)?;
                self.delete_task_assignments_for_executor(db, &txn, executor_id)?;
            }
            RequestPayload::TombstoneIngestedDataObject(request) => {
                self.tombstone_ingested_data_object(db, &txn, request)?;
            }
            RequestPayload::CreateNamespace { name } => {
                self.set_namespace(db, &txn, name)?;
            }
            RequestPayload::MarkStateChangesProcessed { state_changes } => {
                let payload_changes_processed =
                    self.set_processed_state_changes(db, &txn, state_changes)?;
                state_changes_processed.extend(payload_changes_processed);
            }
            RequestPayload::JoinCluster {
                node_id,
                address: _,
                coordinator_addr,
            } => {
                self.set_coordinator_addr(db, &txn, *node_id, coordinator_addr)?;
            }
            RequestPayload::CreateComputeGraph(request) => {
                self.create_compute_graph(db, &txn, request)?;
            }
            RequestPayload::CreateExtractionGraphLink {
                extraction_graph_link,
            } => {
            }
        };

        let last_change_id = request.new_state_changes.last().map(|sc| sc.id);

        self.update_reverse_indexes(request).map_err(|e| {
            StateMachineError::ExternalError(anyhow!(
                "Error while applying reverse index updates: {}",
                e
            ))
        })?;

        txn.commit()
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

        Ok(last_change_id)
    }

    /// This method handles all reverse index writes. All reverse indexes are
    /// written in memory
    pub fn update_reverse_indexes(&self, request: StateMachineUpdateRequest) -> Result<()> {
        for change in request.new_state_changes {
            self.unprocessed_state_changes.insert(change.id);
        }
        for change in request.state_changes_processed {
            self.mark_state_changes_processed(&change, change.processed_at);
        }
        match request.payload {
            RequestPayload::RegisterExecutor(executor) => {
                // initialize executor tasks
                self.unfinished_tasks_by_executor
                    .write()
                    .unwrap()
                    .entry(executor.id)
                    .or_default();

                Ok(())
            }
            RequestPayload::CreateTasks { tasks } => {
                for task in tasks {
                    self.unassigned_tasks.insert(&task.id, task.creation_time);
                    self.unfinished_tasks_by_extractor
                        .insert(&task.extractor, &task.id);
                }
                Ok(())
            }
            RequestPayload::AssignTask { assignments } => {
                for (task_id, executor_id) in assignments {
                    let task = self.unassigned_tasks.remove(&task_id);
                    if let Some(task) = task {
                        self.unfinished_tasks_by_executor
                            .write()
                            .unwrap()
                            .entry(executor_id.clone())
                            .or_default()
                            .insert(task);
                    }
                }
                Ok(())
            }
            RequestPayload::MarkStateChangesProcessed { state_changes } => {
                for state_change in state_changes {
                    self.mark_state_changes_processed(&state_change, state_change.processed_at);
                }
                Ok(())
            }
            RequestPayload::DeleteComputeGraph { .. } |
            RequestPayload::CreateExtractionGraphLink { .. } |
            RequestPayload::CreateNamespace { .. } |
            RequestPayload::JoinCluster { .. } |
            RequestPayload::RemoveExecutor { .. } |
            RequestPayload::TombstoneIngestedDataObject { .. } => Ok(()),
            _ => Ok(()),
        }
    }

    //  START READER METHODS FOR ROCKSDB FORWARD INDEXES

    fn invoke_compute_graph(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        request: &InvokeComputeGraphRequest,
    ) -> Result<(), StateMachineError> {
        let ingestion_object_key = request.data_object.ingestion_object_key();
        let serialized_content = JsonEncoder::encode(&request.data_object)?;
        txn.put_cf(
            StateMachineColumns::IngestedDataObjects.cf(db),
            ingestion_object_key,
            &serialized_content,
        )
        .map_err(|e| StateMachineError::DatabaseError(format!("error writing object: {}", e)))?;
        let ctx_key = request.invocation_ctx.key();
        let serialized_ctx = JsonEncoder::encode(&request.invocation_ctx)?;
        txn.put_cf(
            StateMachineColumns::GraphInvocationCtx.cf(db),
            ctx_key,
            &serialized_ctx,
        )
        .map_err(|e| StateMachineError::DatabaseError(format!("error writing ctx: {}", e)))?;
        Ok(())
    }

    fn delete_compute_graph(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        request: &DeleteComputeGraphRequest,
    ) -> Result<(), StateMachineError> {
        let key_prefix = format!("{}_{}", request.namespace, request.graph_name);
        // Delete ingested data objects 
        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);
        let mode = IteratorMode::From(key_prefix.as_bytes(), Direction::Forward);
        let itr = db.iterator_cf_opt(StateMachineColumns::IngestedDataObjects.cf(db), read_options, mode);
        for kv in itr {
            let (key, _) = kv.map_err(|e| StateMachineError::DatabaseError(format!("Error reading ingested data object: {}", e)))?;
            let key = String::from_utf8(key.to_vec()).map_err(|e| StateMachineError::DatabaseError(format!("Error reading ingested data object: {}", e)))?;
            txn.delete_cf(StateMachineColumns::IngestedDataObjects.cf(db), key)
                .map_err(|e| StateMachineError::DatabaseError(format!("Error deleting ingested data object: {}", e)))?;
        }
        // Delete fn outputs
        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);
        let mode = IteratorMode::From(key_prefix.as_bytes(), Direction::Forward);
        let itr = db.iterator_cf_opt(StateMachineColumns::DataObjects.cf(db), read_options, mode);
        for kv in itr {
            let (key, _) = kv.map_err(|e| StateMachineError::DatabaseError(format!("Error reading fn output: {}", e)))?;
            let key = String::from_utf8(key.to_vec()).map_err(|e| StateMachineError::DatabaseError(format!("Error reading fn output: {}", e)))?;
            txn.delete_cf(StateMachineColumns::DataObjects.cf(db), key)
                .map_err(|e| StateMachineError::DatabaseError(format!("Error deleting fn output: {}", e)))?;
        }
        // Delete compute graph
        txn.delete_cf(StateMachineColumns::ComputeGraphs.cf(db), key_prefix.as_bytes())
            .map_err(|e| StateMachineError::DatabaseError(format!("Error deleting compute graph: {}", e)))?;
        Ok(())
    }

    /// This method is used to get the tasks assigned to an executor
    /// It does this by looking up the TaskAssignments CF to get the task id's
    /// and then using those id's to look up tasks via Tasks CF
    pub fn get_tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<Task>, StateMachineError> {
        let limit = limit.unwrap_or(u64::MAX);
        let tasks: Vec<_> = match self
            .unfinished_tasks_by_executor
            .read()
            .unwrap()
            .get(executor_id)
        {
            Some(tasks) => tasks
                .tasks
                .iter()
                .take(limit as usize)
                .map(|task| task.id.clone())
                .collect(),
            None => return Ok(Vec::new()),
        };

        let txn = db.transaction();

        let tasks: Result<Vec<Task>, StateMachineError> = tasks
            .iter()
            .map(|task_id| {
                let task_bytes = txn
                    .get_cf(StateMachineColumns::Tasks.cf(db), task_id.as_bytes())
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(format!("Task {} not found", task_id))
                    })?;
                JsonEncoder::decode(&task_bytes).map_err(StateMachineError::from)
            })
            .collect();
        tasks
    }

    /// This method will fetch the executors from RocksDB CF based on the
    /// executor id's provided
    pub fn get_executors_from_ids(
        &self,
        executor_ids: HashSet<String>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<ExecutorMetadata>, StateMachineError> {
        let txn = db.transaction();
        let executors: Result<Vec<ExecutorMetadata>, StateMachineError> = executor_ids
            .into_iter()
            .map(|executor_id| {
                let executor_bytes = txn
                    .get_cf(
                        StateMachineColumns::Executors.cf(db),
                        executor_id.as_bytes(),
                    )
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(format!(
                            "Executor {} not found",
                            executor_id
                        ))
                    })?;
                JsonEncoder::decode(&executor_bytes).map_err(StateMachineError::from)
            })
            .collect();
        executors
    }

    /// This method will fetch content based on the id's provided. It will look
    /// for the latest version for each piece of content It will skip any
    /// that cannot be found and expect the consumer to decide what to do in
    /// that case.
    pub fn get_content_from_ids(
        &self,
        content_ids: impl IntoIterator<Item = String>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<ContentMetadata>, StateMachineError> {
        let txn = db.transaction();
        let mut contents = Vec::new();
        let cf_handle = StateMachineColumns::ContentTable.cf(db);
        let cf_keys = content_ids
            .into_iter()
            .map(|id| (cf_handle, id))
            .collect_vec();
        let results = txn.multi_get_cf(cf_keys);
        for res in results {
            match res {
                Ok(Some(value)) => {
                    contents.push(JsonEncoder::decode::<ContentMetadata>(&value)?);
                }
                Ok(None) => {}
                Err(e) => {
                    return Err(StateMachineError::DatabaseError(format!(
                        "error reading content: {}",
                        e
                    )))
                }
            }
        }
        Ok(contents)
    }

    /// This method gets all task assignments stored in the relevant CF
    pub fn get_all_task_assignments(
        &self,
        db: &OptimisticTransactionDB,
    ) -> Result<HashMap<TaskId, ExecutorId>, StateMachineError> {
        let mut assignments = HashMap::new();
        let iter = db.iterator_cf(
            StateMachineColumns::TaskAssignments.cf(db),
            IteratorMode::Start,
        );
        for item in iter {
            let (key, value) = item.map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "unable to get values from task assignment {}",
                    e
                ))
            })?;
            let executor_id = String::from_utf8(key.to_vec()).map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "unable to get executor id from task assignment {}",
                    e
                ))
            })?;
            let task_ids: HashSet<TaskId> = JsonEncoder::decode(&value).map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "unable to decoded task hashset from task assignment {}",
                    e
                ))
            })?;
            for task_id in task_ids {
                assignments.insert(task_id, executor_id.clone());
            }
        }
        Ok(assignments)
    }

    /// Allocate a block of state change ids
    pub fn get_next_change_ids(&self, num: usize) -> u64 {
        let mut guard = self.change_id.lock().unwrap();
        let next_id = *guard;
        *guard = next_id + num as u64;
        next_id
    }

    /// This method will get the namespace based on the key provided
    pub fn namespace_exists(&self, namespace: &str, db: &OptimisticTransactionDB) -> Result<bool> {
        Ok(get_from_cf::<String, &str>(db, StateMachineColumns::Namespaces, namespace)?.is_some())
    }

    pub fn get_extraction_graphs(
        &self,
        extraction_graph_ids: &[impl AsRef<str>],
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
    ) -> Result<Vec<Option<ExtractionGraph>>, StateMachineError> {
        let cf = StateMachineColumns::ExtractionGraphs.cf(db);
        let keys: Vec<(&rocksdb::ColumnFamily, &[u8])> = extraction_graph_ids
            .iter()
            .map(|egid| (cf, egid.as_ref().as_bytes()))
            .collect();
        let serialized_graphs = txn.multi_get_cf(keys);
        let mut graphs: Vec<Option<ExtractionGraph>> = Vec::new();
        for serialized_graph in serialized_graphs {
            match serialized_graph {
                Ok(Some(graph)) => {
                    graphs.push(Some(JsonEncoder::decode::<ExtractionGraph>(&graph)?));
                }
                Ok(None) => {
                    graphs.push(None);
                }
                Err(e) => {
                    return Err(StateMachineError::TransactionError(e.to_string()));
                }
            }
        }
        Ok(graphs)
    }

    pub fn get_extraction_graphs_by_name(
        &self,
        namespace: &str,
        graph_names: &[impl AsRef<str>],
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<Option<ExtractionGraph>>, StateMachineError> {
        let cf = StateMachineColumns::ExtractionGraphs.cf(db);
        let keys: Vec<(_, _)> = graph_names
            .iter()
            .map(|s| (cf, ExtractionGraph::make_key(namespace, s.as_ref())))
            .collect();
        let serialized_graphs = db.multi_get_cf(keys);
        let mut graphs: Vec<Option<ExtractionGraph>> = Vec::new();
        for serialized_graph in serialized_graphs {
            match serialized_graph {
                Ok(graph) => {
                    if graph.is_some() {
                        let deserialized_graph =
                            JsonEncoder::decode::<ExtractionGraph>(&graph.unwrap())?;
                        graphs.push(Some(deserialized_graph));
                    } else {
                        graphs.push(None);
                    }
                }
                Err(e) => {
                    return Err(StateMachineError::TransactionError(e.to_string()));
                }
            }
        }
        Ok(graphs)
    }

    pub fn get_coordinator_addr(
        &self,
        node_id: NodeId,
        db: &OptimisticTransactionDB,
    ) -> Result<Option<String>> {
        get_from_cf(
            db,
            StateMachineColumns::CoordinatorAddress,
            node_id.to_string(),
        )
    }

    pub fn iter_cf<'a, V>(
        &self,
        db: &'a OptimisticTransactionDB,
        column: StateMachineColumns,
    ) -> impl Iterator<Item = Result<(Box<[u8]>, V), StateMachineError>> + 'a
    where
        V: DeserializeOwned,
    {
        let cf_handle = db
            .cf_handle(column.as_ref())
            .ok_or(StateMachineError::DatabaseError(format!(
                "Failed to get column family {}",
                column
            )))
            .unwrap();
        db.iterator_cf(cf_handle, IteratorMode::Start).map(|item| {
            item.map_err(|e| StateMachineError::DatabaseError(e.to_string()))
                .and_then(|(key, value)| match JsonEncoder::decode::<V>(&value) {
                    Ok(value) => Ok((key, value)),
                    Err(e) => Err(StateMachineError::SerializationError(e.to_string())),
                })
        })
    }

    //  START READER METHODS FOR REVERSE INDEXES
    pub fn get_unassigned_tasks(&self) -> HashMap<TaskId, SystemTime> {
        self.unassigned_tasks.inner()
    }

    pub fn get_unprocessed_state_changes(&self) -> HashSet<StateChangeId> {
        self.unprocessed_state_changes.inner()
    }

    pub fn executor_count(&self) -> usize {
        0
    }

    //  END READER METHODS FOR REVERSE INDEXES

    // For each linked graph link it's top level policies to the graph node
    fn rebuild_graph_links(
        &self,
        db: &OptimisticTransactionDB,
        graph_links: &mut HashMap<ExtractionGraphNode, HashSet<String>>,
    ) -> Result<(), StateMachineError> {
        let cf_handle = StateMachineColumns::ExtractionGraphLinks.cf(db);
        for v in db.iterator_cf(cf_handle, IteratorMode::Start) {
            let (key, _) = v.map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            let link: ExtractionGraphLink = JsonEncoder::decode(&key)?;

            let linked_graphs =
                self.get_extraction_graphs_by_name(&link.node.namespace, &[link.graph_name], db)?;
            if let Some(Some(linked_graph)) = linked_graphs.first() {
                for policy in &linked_graph.extraction_policies {
                    if policy.content_source == ContentSource::Ingestion {
                        graph_links
                            .entry(link.node.clone())
                            .or_default()
                            .insert(policy.id.clone());
                    }
                }
            }
        }
        Ok(())
    }

    //  Build the in-memory reverse indexes on startup or when restoring from
    // snapshot.
    pub fn rebuild_reverse_indexes(
        &self,
        db: &OptimisticTransactionDB,
    ) -> Result<(), StateMachineError> {
        let mut unassigned_tasks = self.unassigned_tasks.unassigned_tasks.write().unwrap();
        let mut unprocessed_state_changes_guard = self
            .unprocessed_state_changes
            .unprocessed_state_changes
            .write()
            .unwrap();
        let mut extractor_executors_table = self
            .extractor_executors_table
            .extractor_executors_table
            .write()
            .unwrap();
        let namespace_index_table = self
            .namespace_index_table
            .namespace_index_table
            .write()
            .unwrap();
        let mut unfinished_tasks_by_extractor = self
            .unfinished_tasks_by_extractor
            .unfinished_tasks_by_extractor
            .write()
            .unwrap();
        let mut unfinished_tasks_by_executor = self.unfinished_tasks_by_executor.write().unwrap();
        let schemas_by_namespace = self
            .schemas_by_namespace
            .schemas_by_namespace
            .write()
            .unwrap();
        let mut content_children_table = self
            .content_children_table
            .content_children_table
            .write()
            .unwrap();
        let mut graph_links = self.graph_links.write().unwrap();

        self.rebuild_graph_links(db, &mut graph_links)?;

        for task_assignment in
            self.iter_cf::<HashSet<TaskId>>(db, StateMachineColumns::TaskAssignments)
        {
            let (executor_key, task_ids) = task_assignment?;
            let executor_id = String::from_utf8(executor_key.to_vec()).map_err(|e| {
                StateMachineError::DatabaseError(format!("Failed to decode executor key: {}", e))
            })?;
            for task_id in task_ids {
                let creation_time = unassigned_tasks.remove(&task_id);
                if let Some(creation_time) = creation_time {
                    unfinished_tasks_by_executor
                        .entry(executor_id.clone())
                        .or_default()
                        .insert(UnfinishedTask {
                            id: task_id.clone(),
                            creation_time,
                        });
                }
            }
        }

        let mut max_change_id = None;
        for state_change in self.iter_cf::<StateChange>(db, StateMachineColumns::StateChanges) {
            let (_, state_change) = state_change?;
            if state_change.processed_at.is_none() {
                unprocessed_state_changes_guard.insert(state_change.id);
            }
            max_change_id = std::cmp::max(max_change_id, Some(state_change.id));
        }
        *self.change_id.lock().unwrap() = match max_change_id {
            Some(val) => Into::<u64>::into(val) + 1,
            None => 0,
        };

        *self.content_offset.lock().unwrap() = ContentOffset(max_content_offset(db)?);

        for content in self.iter_cf::<ContentMetadata>(db, StateMachineColumns::ContentTable) {
            let (_, content) = content?;
            if let Some(parent_id) = &content.parent_id {
                content_children_table
                    .entry(parent_id.clone())
                    .or_default()
                    .insert(content.id.clone());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment_running_task_count() {
        let executor_running_task_count = ExecutorRunningTaskCount::new();
        let executor_id = "executor_id".to_string();
        executor_running_task_count.insert(&executor_id, 0);
        executor_running_task_count.increment_running_task_count(&executor_id);
        assert_eq!(executor_running_task_count.get(&executor_id).unwrap(), 1);
        executor_running_task_count.increment_running_task_count(&executor_id);
        assert_eq!(executor_running_task_count.get(&executor_id).unwrap(), 2);
    }

    #[test]
    fn test_decrement_running_task_count() {
        let executor_running_task_count = ExecutorRunningTaskCount::new();
        let executor_id = "executor_id".to_string();
        executor_running_task_count.insert(&executor_id, 2);
        executor_running_task_count.decrement_running_task_count(&executor_id);
        assert_eq!(executor_running_task_count.get(&executor_id).unwrap(), 1);
        executor_running_task_count.decrement_running_task_count(&executor_id);
        assert_eq!(executor_running_task_count.get(&executor_id).unwrap(), 0);
    }
}
