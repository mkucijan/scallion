use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use tokio::sync::mpsc::Sender;

use crate::{
    consumer::StreamAccessor, error::TaskError, registry::TaskRegistry, ConsumerCommands,
    ConsumerTask, Task, TaskContainer, TaskResult,
};

#[derive(Clone)]
pub struct ConsumerState {
    pub stream_accessor: StreamAccessor,
    shutdown_hook: Arc<AtomicBool>,
    extensions: Arc<Extensions>,
    consumer_channel: Sender<ConsumerCommands>,
    registry: Arc<dyn TaskRegistry>,
}

impl std::fmt::Debug for ConsumerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerState")
            .field("stream_accessor", &self.stream_accessor)
            .field("shutdown_hook", &self.shutdown_hook)
            .field("extensions", &self.extensions)
            .field("consumer_channel", &self.consumer_channel)
            .finish()
    }
}

impl ConsumerState {
    pub fn new(
        stream_accessor: StreamAccessor,
        shutdown_hook: Arc<AtomicBool>,
        extensions: Arc<Extensions>,
        consumer_channel: Sender<ConsumerCommands>,
        registry: Arc<dyn TaskRegistry>,
    ) -> Self {
        ConsumerState {
            stream_accessor,
            shutdown_hook,
            extensions,
            consumer_channel,
            registry,
        }
    }

    pub fn registry(&self) -> &Arc<dyn TaskRegistry> {
        &self.registry
    }

    pub fn task_state<T: 'static + Send + Sync>(&self) -> Arc<T> {
        self.extensions
            .get::<T>()
            .expect("Extension not registered.")
    }

    pub async fn spawn_task<T: Task>(&self, task: T) -> Result<TaskResult<T>, TaskError> {
        TaskResult::spawn_task(
            self.stream_accessor.consumer_group.clone(),
            self.stream_accessor.manager.clone(),
            task,
            Arc::clone(&self.registry),
        )
        .await
    }

    pub async fn send_command(&self, command: ConsumerCommands) -> Result<(), TaskError> {
        Ok(self.consumer_channel.send(command).await?)
    }

    pub fn prepare_task(self, container: TaskContainer) -> ConsumerTask {
        ConsumerTask::new(container, self)
    }
}

#[derive(Default)]
pub struct Extensions {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync + 'static>>,
}

impl Extensions {
    pub fn new() -> Extensions {
        Extensions {
            map: HashMap::new(),
        }
    }

    pub fn insert<T: 'static + Send + Sync>(&mut self, val: T) -> Option<Arc<T>> {
        self.map
            .insert(TypeId::of::<T>(), Arc::new(val))
            .and_then(|b| b.downcast().ok())
    }

    pub fn get<T: 'static + Send + Sync + Any>(&self) -> Option<Arc<T>> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|a| Arc::clone(a).downcast::<T>().ok())
    }
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("map", &self.map)
            .finish()
    }
}
