use std::{marker::PhantomData, sync::Arc};

use redis::aio::ConnectionManager;
use tokio::time::{Duration, Instant};

use crate::{
    error::{TaskError, TaskMessageError},
    registry::TaskRegistry,
    Task,
};

use super::{stream::TaskStream, TaskOutput};

pub struct TaskResult<T> {
    consumer_group: TaskStream,
    manager: ConnectionManager,
    id: String,
    #[allow(dead_code)]
    stream_id: String,
    result_saved: bool,
    result_wait_millis: u64,
    task: PhantomData<T>,
}

impl<T> TaskResult<T>
where
    T: Task + 'static,
{
    pub async fn spawn_task(
        consumer_group: TaskStream,
        manager: ConnectionManager,
        task: T,
        registry: Arc<dyn TaskRegistry>,
    ) -> Result<Self, TaskError> {
        Self::spawn_tasks(consumer_group, manager, vec![task], registry)
            .await
            .map(|mut r| r.remove(0))
    }

    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn result_saved(&self) -> bool {
        self.result_saved
    }

    pub async fn spawn_tasks(
        consumer_group: TaskStream,
        mut manager: ConnectionManager,
        mut tasks: Vec<T>,
        registry: Arc<dyn TaskRegistry>,
    ) -> Result<Vec<Self>, TaskError> {
        let max_size = tasks.len();
        let mut pipeline = redis::pipe();
        let metadata: Result<Vec<_>, TaskMessageError> = tasks
            .drain(..)
            .map(|task| {
                let broadcast = task.task_options().broadcast;
                let save_result = task.task_options().save_result;
                let result_wait_millis = task.task_options().result_wait_millis;
                let task = registry.container_from_task(&task)?;
                let id = task.id();
                if broadcast {
                    pipeline.add_command(
                        consumer_group.xadd_broadcast(Some(max_size), &task.message()),
                    );
                } else {
                    pipeline.add_command(consumer_group.xadd(Some(max_size), &task.message()));
                }
                Ok((id, save_result, result_wait_millis))
            })
            .collect();
        let mut stream_ids = pipeline.query_async::<_, Vec<String>>(&mut manager).await?;
        Ok(metadata?
            .drain(..)
            .zip(stream_ids.drain(..))
            .map(
                |((id, save_result, result_wait_millis), stream_id)| TaskResult {
                    consumer_group: consumer_group.clone(),
                    manager: manager.clone(),
                    id,
                    stream_id,
                    result_saved: save_result,
                    result_wait_millis,
                    task: PhantomData,
                },
            )
            .collect())
    }

    pub async fn task_result(&mut self) -> Result<Option<TaskOutput>, TaskError> {
        if self.result_saved {
            let until = Instant::now() + Duration::from_millis(self.result_wait_millis);
            while Instant::now() < until {
                let data = self
                    .consumer_group
                    .get_task_result(T::TASK_NAME, self.id.as_str())
                    .query_async::<_, Option<Vec<u8>>>(&mut self.manager)
                    .await?;
                if let Some(data) = data {
                    return Ok(Some(TaskOutput { data }));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        Ok(None)
    }

    pub async fn result(&mut self) -> Result<Option<T::Output>, TaskError> {
        if let Some(r) = self.task_result().await? {
            Ok(T::result_message_provider()
                .deserialize(r.data.as_slice())
                .map(Some)?)
        } else {
            Ok(None)
        }
    }
}
