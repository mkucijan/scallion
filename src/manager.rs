use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use redis::aio::{ConnectionManager, PubSub};
use redis::{Client, Cmd, ErrorKind, RedisError};
use tracing::debug;

use crate::registry::TaskRegistry;
use crate::{ConsumerError, Task};

use super::consumer::StreamConsumerBuilder;
use super::stream::TaskStream;
use super::TaskResult;

pub struct TaskManager {
    consumer_group: TaskStream,
    manager: ConnectionManager,
    pubsub: PubSub,
    registered_tasks: HashMap<&'static str, Arc<dyn TaskRegistry>>,
}

impl std::fmt::Debug for TaskManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskManager")
            .field("consumer_group", &self.consumer_group)
            .finish()
    }
}

impl TaskManager {
    pub async fn new(
        client: Client,
        prefix: impl ToString,
        group: impl ToString,
    ) -> Result<Self, ConsumerError> {
        let prefix = prefix.to_string();
        let consumer = hostname::get()
            .ok()
            .and_then(|sys_hostname| sys_hostname.into_string().ok())
            .map(|sys_hostname| format!("{}_consumer@{}", prefix.as_str(), sys_hostname))
            .expect("Failed fetching hostname");
        Self::with_consumer_name(client, prefix, group, consumer).await
    }

    pub async fn with_consumer_name(
        client: Client,
        prefix: impl ToString,
        group: impl ToString,
        consumer: impl ToString,
    ) -> Result<Self, ConsumerError> {
        let pubsub = client.get_async_connection().await?.into_pubsub();
        let manager = client.get_tokio_connection_manager().await?;
        let mut manager = TaskManager {
            consumer_group: TaskStream::new(
                prefix.to_string(),
                group.to_string(),
                consumer.to_string(),
            ),
            manager,
            pubsub,
            registered_tasks: HashMap::default(),
        };
        manager.initialize().await?;
        Ok(manager)
    }

    fn check_init_error(res: Result<String, RedisError>) -> Result<(), RedisError> {
        match res {
            Err(e)
                if e.kind() == ErrorKind::ExtensionError
                    && e.code().is_some()
                    && e.code().unwrap().eq("BUSYGROUP") =>
            {
                debug!(error=?e, "Consumer group already exists");
            }
            _ => {
                res?;
            }
        }
        Ok(())
    }

    async fn initialize(&mut self) -> Result<(), ConsumerError> {
        // Main channel
        let res = self
            .consumer_group
            .xgroup_create_mkstream()
            .query_async::<_, String>(&mut self.manager)
            .await;
        // Ignore group and stream already exists
        Self::check_init_error(res)?;
        // Broadcast channel
        let res = self
            .consumer_group
            .xgroup_create_mkstream_broadcast()
            .query_async::<_, String>(&mut self.manager)
            .await;
        // Ignore group and stream already exists
        Self::check_init_error(res)?;
        // For pubsub key notifications
        Cmd::new()
            .arg("config")
            .arg("set")
            .arg("notify-keyspace-events")
            .arg("g$K")
            .query_async(&mut self.manager)
            .await?;
        Ok(())
    }

    fn get_task_registry(&self, task_name: &'static str) -> Arc<dyn TaskRegistry> {
        Arc::clone(
            self.registered_tasks
                .get(task_name)
                .expect("Requesting task which is not registered"),
        )
    }

    pub fn register_task(&mut self, registry: Arc<dyn TaskRegistry>) -> &mut Self {
        if let Some(_) = self.registered_tasks.insert(registry.task_name(), registry) {
            panic!("Multiple tasks with same name registered");
        }
        self
    }

    pub fn consumer_builder(&self) -> StreamConsumerBuilder {
        let mut builder =
            StreamConsumerBuilder::new(self.consumer_group.clone(), self.manager.clone());
        for r in self.registered_tasks.values() {
            builder = builder.register_task(Arc::clone(r));
        }
        builder
    }

    pub async fn spawn_task<T: Task + 'static>(
        &self,
        task: T,
    ) -> Result<TaskResult<T>, ConsumerError> {
        Ok(TaskResult::spawn_task(
            self.consumer_group.clone(),
            self.manager.clone(),
            task,
            self.get_task_registry(T::task_name()),
        )
        .await?)
    }
    pub async fn spawn_tasks<T: Task + 'static>(
        &self,
        tasks: Vec<T>,
    ) -> Result<Vec<TaskResult<T>>, ConsumerError> {
        Ok(TaskResult::spawn_tasks(
            self.consumer_group.clone(),
            self.manager.clone(),
            tasks,
            self.get_task_registry(T::task_name()),
        )
        .await?)
    }

    pub async fn wait_on_result<T: Task + 'static>(
        &mut self,
        task: T,
    ) -> Result<Option<T::Output>, ConsumerError> {
        let mut task_result = TaskResult::spawn_task(
            self.consumer_group.clone(),
            self.manager.clone(),
            task,
            self.get_task_registry(T::task_name()),
        )
        .await?;
        if !task_result.result_saved() {
            return Ok(None);
        }
        self.pubsub
            .subscribe(format!(
                "__keyspace@0__:{}",
                self.consumer_group
                    .task_result_key(T::task_name(), task_result.id())
            ))
            .await?;
        loop {
            if let Some(msg) = self.pubsub.on_message().next().await {
                let response: String = msg.get_payload()?;
                debug!("Received message update for storage key: {}", response);
                if response.as_str() == "set" {
                    break;
                }
            } else {
                break;
            }
        }
        self.pubsub
            .unsubscribe(format!(
                "__keyspace@0__:{}",
                self.consumer_group
                    .task_result_key(T::task_name(), task_result.id())
            ))
            .await?;
        Ok(task_result.result().await?)
    }
}
