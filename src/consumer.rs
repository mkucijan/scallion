use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use redis::aio::ConnectionManager;
use redis::streams::StreamReadReply;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;
use tracing::{debug, error, info};

use crate::error::ConsumerError;
use crate::shared_state::{ConsumerState, Extensions};

use super::container::TaskContainer;
use super::stream::TaskStream;
use super::{consumer_task::ConsumerTask, registry::TaskRegistry};

pub struct StreamConsumer {
    num_parallel_tasks: usize,
    max_size: usize,
    timeout: usize,
    stream_accessor: StreamAccessor,
    shutdown_hook: Arc<AtomicBool>,
    registered_tasks: HashMap<&'static str, Arc<dyn TaskRegistry>>,
    command_channel: CommandChannel,
    extensions: Arc<Extensions>,
}

#[derive(Clone)]
pub struct StreamAccessor {
    pub consumer_group: TaskStream,
    pub manager: ConnectionManager,
}

impl std::fmt::Debug for StreamAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamAccessor")
            .field("consumer_group", &self.consumer_group)
            .finish()
    }
}

struct CommandChannel {
    pub receiver: Receiver<ConsumerCommands>,
    pub sender: Sender<ConsumerCommands>,
}

#[derive(Debug)]
pub enum ConsumerCommands {
    Shutdown,
}

impl StreamConsumer {
    #[allow(clippy::too_many_arguments)]
    fn new(
        num_parallel_tasks: usize,
        max_size: usize,
        timeout: usize,
        consumer_group: TaskStream,
        manager: ConnectionManager,
        registered_tasks: HashMap<&'static str, Arc<dyn TaskRegistry>>,
        shutdown_hook: Arc<AtomicBool>,
        extensions: Extensions,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Self {
            num_parallel_tasks,
            max_size,
            stream_accessor: StreamAccessor {
                consumer_group,
                manager,
            },
            timeout,
            shutdown_hook,
            registered_tasks,
            command_channel: CommandChannel { receiver, sender },
            extensions: Arc::new(extensions),
        }
    }

    pub fn consumer_state(&self, registry: Arc<dyn TaskRegistry>) -> ConsumerState {
        ConsumerState::new(
            self.stream_accessor(),
            Arc::clone(&self.shutdown_hook),
            Arc::clone(&self.extensions),
            self.consumer_channel(),
            registry,
        )
    }

    pub fn stream_accessor(&self) -> StreamAccessor {
        self.stream_accessor.clone()
    }

    pub fn consumer_channel(&self) -> Sender<ConsumerCommands> {
        self.command_channel.sender.clone()
    }

    async fn read_pending(&mut self) -> Result<StreamReadReply, ConsumerError> {
        Ok(self
            .stream_accessor
            .consumer_group
            .clone()
            .xreadgroup(self.timeout, self.max_size, &["0".to_string()])
            .query_async::<_, StreamReadReply>(&mut self.stream_accessor.manager)
            .await?)
    }

    pub async fn run(mut self) -> Result<(), ConsumerError> {
        info!(
            stream = ?self.stream_accessor.consumer_group,
            "Starting consumer."
        );
        let mut tasks = FuturesUnordered::new();
        let mut prefetched: VecDeque<TaskContainer> = VecDeque::with_capacity(self.max_size);
        let mut reply = None;
        let mut reply_broadcast = None;
        let mut result_ack = None;
        let mut results = Vec::new();

        // Check pending messages
        {
            let mut pending = self.read_pending().await?;
            prefetched.extend(deserialize_task_from_reply(&mut pending));
            if !prefetched.is_empty() {
                info!("Found {} pending messages.", prefetched.len());
                while (tasks.len() < self.num_parallel_tasks) && !prefetched.is_empty() {
                    let task = prefetched.pop_front().unwrap();
                    tasks.push(self.create_task(task).future());
                }
            }
        }
        // Start main loop
        while !self.shutdown_hook.load(Ordering::SeqCst) {
            debug!(
                num_tasks = tasks.len(),
                prefetched = prefetched.len(),
                results = results.len(),
                "Tick."
            );
            // Schedule future for fetching new tasks
            if reply.is_none() {
                let max_fetch_size = self.max_size - prefetched.len();
                if max_fetch_size > 0 {
                    let mut sa = self.stream_accessor.clone();
                    let timeout = self.timeout;
                    reply = Some(tokio::spawn(async move {
                        sa.consumer_group
                            .clone()
                            .xreadgroup(timeout, max_fetch_size, &[">".to_string()])
                            .query_async::<_, StreamReadReply>(&mut sa.manager)
                            .await
                    }));
                }
            }
            // Schedule future for fetching tasks from broadcast channel
            if reply_broadcast.is_none() {
                let mut sa = self.stream_accessor.clone();
                let timeout = self.timeout * 3;
                reply_broadcast = Some(tokio::spawn(async move {
                    sa.consumer_group
                        .clone()
                        .xread_broadcast(timeout)
                        .query_async::<_, StreamReadReply>(&mut sa.manager)
                        .await
                }));
            }

            // Schedule future acknowledging results
            if result_ack.is_none() && !results.is_empty() {
                result_ack =
                    Some(self.send_acks(Self::handle_task_result(std::mem::take(&mut results))));
            }

            // Add timeouts so we wouldn't hot loop all the time when idle
            let reply_future = async {
                if let Some(reply_ref) = reply.as_mut() {
                    Some(reply_ref.await)
                } else {
                    tokio::time::sleep(Duration::from_millis(self.timeout as u64)).await;
                    None
                }
            };
            let reply_broadcast_future = async {
                if let Some(reply_broadcast_ref) = reply_broadcast.as_mut() {
                    Some(reply_broadcast_ref.await)
                } else {
                    tokio::time::sleep(Duration::from_millis(self.timeout as u64 * 3)).await;
                    None
                }
            };
            let result_ack_future = async {
                if let Some(result_ack_ref) = result_ack.as_mut() {
                    Some(result_ack_ref.await)
                } else {
                    tokio::time::sleep(Duration::from_millis(self.timeout as u64)).await;
                    None
                }
            };
            let next_result_fut = async {
                if let Some(rez) = tasks.next().await {
                    Some(rez)
                } else {
                    tokio::time::sleep(Duration::from_millis(self.timeout as u64)).await;
                    None
                }
            };
            tokio::select! {
                rez = next_result_fut => {
                    if let Some(Some(rez)) = rez {
                        results.push(rez);
                    }
                    if !prefetched.is_empty() {
                        let task = prefetched.pop_front().unwrap();
                        tasks.push(self.create_task(task).future());
                    }
                }
                r = reply_future => {
                    if let Some(r) = r {
                        let mut r = r??;
                        prefetched.extend(deserialize_task_from_reply(&mut r));
                        while (tasks.len() < self.num_parallel_tasks) && !prefetched.is_empty() {
                            let task = prefetched.pop_front().unwrap();
                            tasks.push(self.create_task(task).future());
                        }
                        reply = None;
                    }
                },
                r = reply_broadcast_future => {
                    if let Some(r) = r {
                        let mut r = r??;
                        for task in deserialize_task_from_reply(&mut r) {
                            tasks.push(self.create_task(task).future());
                        }
                        reply_broadcast = None;
                    }
                },
                r = result_ack_future => {
                    if r.is_some() {
                        result_ack = None;
                    }
                }
                command = self.command_channel.receiver.recv() => {
                    let command = command.ok_or_else(|| ConsumerError::RuntimeError(format!("Command channel closed")))?;
                    match command {
                        ConsumerCommands::Shutdown => {
                            info!("Received shutdown command, exiting...");
                            self.shutdown_hook.store(true, Ordering::SeqCst);
                        }
                    }
                }
            }
        }
        info!(
            "SHUTDOWN: Waiting for remaining tasks to finish. Number of tasks: {}",
            tasks.len()
        );
        while let Some(Some(rez)) = tasks.next().await {
            self.send_acks(Self::handle_task_result(vec![rez])).await?;
        }
        Ok(())
    }

    fn handle_task_result(fs: Vec<String>) -> Vec<String> {
        fs
    }

    fn send_acks(&mut self, acks: Vec<String>) -> JoinHandle<()> {
        let mut sa = self.stream_accessor.clone();
        tokio::spawn(async move {
            debug!("Starting ack send");
            let now = Instant::now();
            let ack_num = sa
                .consumer_group
                .xack(acks.as_slice())
                .query_async::<_, i64>(&mut sa.manager)
                .await;
            debug!(duration = ?(Instant::now() - now), "Finished ack send");
            match ack_num {
                Ok(res) => {
                    debug!(
                        acks_sent = acks.len(),
                        num_ack = res,
                        "Messages acknowledged"
                    )
                }
                Err(e) => error!(err=?e, "Error acknowledging"),
            }
        })
    }
    fn create_task(&mut self, task: TaskContainer) -> ConsumerTask {
        let registry = Arc::clone(
            self.registered_tasks
                .get(task.task_name().as_str())
                .expect("Received unregistered task"),
        );
        self.consumer_state(registry).prepare_task(task)
    }
}

fn deserialize_task_from_reply(
    reply: &mut StreamReadReply,
) -> impl Iterator<Item = TaskContainer> + '_ {
    reply.keys.drain(..).flat_map(|mut k| {
        k.ids
            .drain(..)
            .filter_map(|s| match TaskContainer::from_stream_message(s) {
                Ok(o) => Some(o),
                Err(e) => {
                    error!(error=?e, "Failed retrieving task from stream.");
                    None
                }
            })
            .collect::<Vec<_>>()
    })
}

#[derive(Debug)]
pub struct ConsumerHandle {
    handle: JoinHandle<Result<(), ConsumerError>>,
    shutdown_hook: Arc<AtomicBool>,
}

impl ConsumerHandle {
    pub fn new(
        handle: JoinHandle<Result<(), ConsumerError>>,
        shutdown_hook: Arc<AtomicBool>,
    ) -> Self {
        Self {
            handle,
            shutdown_hook,
        }
    }

    pub fn shutdown(&self) {
        self.shutdown_hook.store(true, Ordering::SeqCst);
    }

    pub async fn wait(self) -> Result<Result<(), ConsumerError>, JoinError> {
        self.handle.await
    }
}

pub struct StreamConsumerBuilder {
    num_parallel_tasks: usize,
    max_size: usize,
    consumer_group: TaskStream,
    manager: ConnectionManager,
    timeout: usize,
    registered_tasks: HashMap<&'static str, Arc<dyn TaskRegistry>>,
    extensions: Extensions,
}

impl StreamConsumerBuilder {
    pub fn new(consumer_group: TaskStream, manager: ConnectionManager) -> Self {
        let mut extensions = Extensions::default();
        extensions.insert(());
        Self {
            num_parallel_tasks: 100,
            max_size: 1000,
            timeout: 1000,
            consumer_group,
            manager,
            registered_tasks: HashMap::new(),
            extensions,
        }
    }

    pub fn num_parallel_tasks(mut self, num_parallel_tasks: usize) -> Self {
        self.num_parallel_tasks = num_parallel_tasks;
        self
    }

    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self
    }

    pub fn timeout(mut self, timeout: usize) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn register_task(mut self, registry: Arc<dyn TaskRegistry>) -> Self {
        if self
            .registered_tasks
            .insert(registry.task_name(), registry)
            .is_some()
        {
            panic!("Multiple tasks with same name registered");
        }
        self
    }

    pub fn register_extension<T: 'static + Send + Sync>(mut self, item: T) -> Self {
        self.extensions.insert(item);
        self
    }

    pub fn spawn(self) -> ConsumerHandle {
        let shutdown_hook = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn(
            StreamConsumer::new(
                self.num_parallel_tasks,
                self.max_size,
                self.timeout,
                self.consumer_group,
                self.manager,
                self.registered_tasks,
                Arc::clone(&shutdown_hook),
                self.extensions,
            )
            .run(),
        );
        ConsumerHandle::new(handle, shutdown_hook)
    }
}
