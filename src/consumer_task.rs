use futures::future::BoxFuture;
use tracing::{debug, error};

use crate::{error::TaskError, shared_state::ConsumerState};

use super::{container::TaskContainer, TaskOutput};

pub struct ConsumerTask {
    container: TaskContainer,
    state: ConsumerState,
}

impl ConsumerTask {
    pub fn new(container: TaskContainer, state: ConsumerState) -> Self {
        Self { container, state }
    }
}

#[derive(Debug)]
pub struct TaskOptions {
    pub save_result: bool,
    pub result_ttl: usize,
    pub result_wait_millis: u64,
    pub retry_count: usize,
    pub spawn: bool,
    pub broadcast: bool,
}

impl Default for TaskOptions {
    fn default() -> Self {
        Self {
            save_result: false,
            result_ttl: 10 * 60,
            result_wait_millis: 10_000,
            retry_count: 0,
            spawn: true,
            broadcast: false,
        }
    }
}

impl ConsumerTask {
    pub async fn future(self) -> Option<String> {
        let ConsumerTask {
            mut container,
            mut state,
        } = self;
        let stream_id = container.stream_id().clone();
        let task_header = container.header();
        let task = match state.registry().to_task_future(container.data()) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed creating task. {e}");
                return None;
            }
        };
        let task_options = task.active_task_options();
        let id = task_header.id.to_string();
        let task_name = task_header.task_name.as_str();

        if let Some(result) =
            Self::handle_task_result(task_name, task.run(state.clone()), task_options.spawn).await
        {
            debug!(stream_id=stream_id.as_str(), result=?result, "Task result");
            // Save result
            if task_options.save_result {
                match state
                    .stream_accessor
                    .consumer_group
                    .set_task_result(
                        task_name,
                        task_header.id.to_string().as_str(),
                        result.data,
                        task_options.result_ttl,
                    )
                    .query_async::<_, String>(&mut state.stream_accessor.manager)
                    .await
                {
                    Ok(response) => {
                        debug!(
                            id = id.as_str(),
                            stream_id = stream_id.as_str(),
                            task_name,
                            response = response.as_str(),
                            "Task successfully saved"
                        );
                    }
                    Err(error) => {
                        error!(
                            id = id.as_str(),
                            stream_id = stream_id.as_str(),
                            task_name,
                            "Failed saving task result. {error:?}"
                        );
                        return None;
                    }
                }
            }
            if task_options.broadcast {
                None
            } else {
                Some(stream_id)
            }
        } else {
            if task_options.retry_count > task_header.retry_count {
                container.increment_retry();
                match state
                    .stream_accessor
                    .consumer_group
                    .xadd(None, &container.message())
                    .query_async::<_, String>(&mut state.stream_accessor.manager)
                    .await
                {
                    Ok(response) => {
                        debug!(
                            id = id.as_str(),
                            stream_id = stream_id.as_str(),
                            task_name,
                            response = response.as_str(),
                            "Retried task sent"
                        );
                    }
                    Err(error) => {
                        error!(
                            id = id.as_str(),
                            stream_id = stream_id.as_str(),
                            task_name,
                            "Failed sending retry task. {error}"
                        );
                    }
                }
            }
            Some(stream_id)
        }
    }

    async fn handle_task_result(
        task_name: &str,
        task: BoxFuture<'static, Result<TaskOutput, TaskError>>,
        spawn: bool,
    ) -> Option<TaskOutput> {
        let task_result = if spawn {
            match tokio::spawn(task).await {
                Ok(result) => result,
                Err(e) => {
                    error!("Join handle fail: {e}");
                    return None;
                }
            }
        } else {
            task.await
        };
        match task_result {
            Ok(r) => {
                debug!("Task `{task_name}` finished successfully");
                Some(r)
            }
            Err(e) => {
                error!("Task `{task_name}` failed. {e}");
                None
            }
        }
    }
}
