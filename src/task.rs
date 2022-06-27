use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::{
    error::TaskError, message::JsonMessageProvider, result_store::TaskOutput,
    shared_state::ConsumerState, MessageProvider, TaskOptions,
};

#[async_trait]
pub trait Task: RunnableTask + Send + Sync + 'static {
    type Output: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;
    const TASK_NAME: &'static str; // = std::any::type_name::<Self>();

    fn task_name(&self) -> &'static str {
        Self::TASK_NAME
    }

    fn task_options(&self) -> TaskOptions {
        TaskOptions::default()
    }

    fn result_message_provider() -> Box<dyn MessageProvider<Message = Self::Output>>
    where
        Self: Sized,
    {
        JsonMessageProvider::new_message_provider()
    }

    async fn task(self: Box<Self>, state: ConsumerState) -> Result<Self::Output, anyhow::Error>;
}

pub trait RunnableTask: Send + Sync {
    fn run(
        self: Box<Self>,
        state: ConsumerState,
    ) -> BoxFuture<'static, Result<TaskOutput, TaskError>>;

    fn active_task_options(&self) -> TaskOptions;
}

impl<T> RunnableTask for T
where
    T: Task,
{
    fn run(
        self: Box<Self>,
        state: ConsumerState,
    ) -> BoxFuture<'static, Result<TaskOutput, TaskError>> {
        Box::pin(async move {
            let result = self.task(state).await?;
            let result = TaskOutput {
                data: Self::result_message_provider().serialize(&result)?,
            };
            Ok(result)
        })
    }

    fn active_task_options(&self) -> TaskOptions {
        self.task_options()
    }
}
