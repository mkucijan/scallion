use std::{any::Any, marker::PhantomData, sync::Arc};

use uuid::Uuid;

use crate::{
    container::TaskContainer,
    container::TaskHeader,
    error::TaskMessageError,
    message::{JsonMessageProvider, MessageProvider},
    task::RunnableTask,
    Task,
};

pub trait TaskRegistry: Send + Sync {
    fn task_name(&self) -> &'static str;
    fn to_task_future(&self, data: &[u8]) -> Result<Box<dyn RunnableTask>, TaskMessageError>;
    fn container_from_task(&self, task: &dyn Any) -> Result<TaskContainer, TaskMessageError>;
}

pub struct RegisteredTask<T: 'static> {
    task: PhantomData<T>,
    message_provider: Box<dyn MessageProvider<Message = T>>,
}
impl<T> RegisteredTask<T>
where
    T: Task,
{
    pub fn with_providers(
        message_provider: Box<dyn MessageProvider<Message = T>>,
    ) -> Arc<dyn TaskRegistry> {
        Arc::new(Self {
            task: PhantomData,
            message_provider,
        })
    }
}

// Set default message provider
impl<T> RegisteredTask<T>
where
    T: Task + serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
{
    pub fn registry() -> Arc<dyn TaskRegistry> {
        Self::with_providers(JsonMessageProvider::new_message_provider())
    }
}

impl<T> TaskRegistry for RegisteredTask<T>
where
    T: Task,
{
    fn task_name(&self) -> &'static str {
        T::TASK_NAME
    }

    fn to_task_future(&self, data: &[u8]) -> Result<Box<dyn RunnableTask>, TaskMessageError> {
        self.message_provider
            .deserialize(data)
            .map(|o| Box::new(o) as Box<dyn RunnableTask>)
    }

    fn container_from_task(&self, task: &dyn Any) -> Result<TaskContainer, TaskMessageError> {
        let task_name = T::TASK_NAME.to_string();
        let id = Uuid::new_v4();
        let stream_id = "^".to_string();
        let header = TaskHeader {
            id,
            task_name,
            retry_count: 0,
        };
        let data = self.message_provider.serialize(
            task.downcast_ref::<T>()
                .expect("Wrong task sent to Task registry."),
        )?;
        Ok(TaskContainer {
            stream_id,
            header,
            data,
        })
    }
}
