use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing::info;

use scallion::{
    tasks::utility::{CheckConsumerStatus, Ping, Shutdown},
    ConsumerState, RegisteredTask, Task, TaskManager, TaskOptions,
};

#[derive(Parser, Debug)]
pub struct Config {
    #[clap(subcommand)]
    command: Commands,
    #[clap(short, long, default_value = "redis://:@127.0.0.1:6379/0")]
    redis_url: String,
    #[clap(short, long, default_value = "dev")]
    prefix: String,
    #[clap(short, long)]
    consumer: Option<String>,
}
#[derive(clap::Parser, Debug)]
pub enum Commands {
    Consume {
        #[clap(short, long, default_value_t = 100)]
        num_parallel_tasks: usize,
        #[clap(short, long, default_value_t = 1000)]
        max_prefetch_size: usize,
        #[clap(short, long, default_value_t = 1000)]
        timeout_ms: usize,
    },
    Produce {
        #[clap(subcommand)]
        task: TaskOpts,
    },
}

#[derive(clap::Parser, Debug)]
#[allow(non_camel_case_types)]
pub enum TaskOpts {
    PING {
        #[clap(short, long, default_value_t = 1)]
        number_of_tasks: usize,
        #[clap(short, long)]
        delay: Option<u64>,
        #[clap(short, long)]
        spawn: bool,
        #[clap(short, long)]
        fake_failure: bool,
    },
    SHUTDOWN,
    CONSUMER_STATUS,
    ADD {
        x: f64,
        y: f64,
    },
    EXAMPLE_WITH_STATE,
    EXAMPLE_RANDOM_DATA,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let opts = Config::parse();
    let client = redis::Client::open(opts.redis_url)?;
    let mut task_manager = if let Some(consumer) = opts.consumer {
        TaskManager::with_consumer_name(client, opts.prefix, "example_app", consumer).await?
    } else {
        TaskManager::new(client, opts.prefix, "example_app").await?
    };

    task_manager
        .register_task(RegisteredTask::<Ping>::registry())
        .register_task(RegisteredTask::<Shutdown>::registry())
        .register_task(RegisteredTask::<CheckConsumerStatus>::registry())
        .register_task(RegisteredTask::<Add>::registry())
        .register_task(RegisteredTask::<ExampleStoreRandomData>::registry())
        .register_task(RegisteredTask::<ExampleWithState>::registry());

    match opts.command {
        Commands::Consume {
            num_parallel_tasks,
            max_prefetch_size,
            timeout_ms,
        } => {
            let consumer = task_manager
                .consumer_builder()
                .num_parallel_tasks(num_parallel_tasks)
                .max_size(max_prefetch_size)
                .timeout(timeout_ms)
                .register_extension(ExampleState {
                    conf: format!("Hello World!"),
                })
                .spawn();
            consumer.wait().await??;
        }
        Commands::Produce { task } => match task {
            TaskOpts::PING {
                number_of_tasks,
                delay,
                spawn,
                fake_failure,
            } => {
                let _results = task_manager
                    .spawn_tasks(
                        (0..number_of_tasks)
                            .map(|_| Ping {
                                delay_ms: delay,
                                spawn_task: spawn,
                                fake_failure,
                                ..Default::default()
                            })
                            .collect(),
                    )
                    .await?;
            }
            TaskOpts::SHUTDOWN => {
                let _result = task_manager.spawn_task(Shutdown::now()).await?;
            }
            TaskOpts::CONSUMER_STATUS => {
                let result = task_manager
                    .wait_on_result(CheckConsumerStatus::default())
                    .await?;
                if let Some(result) = result.flatten() {
                    info!("Consumer status: {:#?}", result);
                } else {
                    info!("Consumer status timed out.");
                }
            }
            TaskOpts::ADD { x, y } => {
                let result = task_manager
                    .wait_on_result(Add { x, y })
                    .await?
                    .expect("Result missing");
                info!("Result adding (x: {}, y: {}) = {:?}", x, y, result)
            }
            TaskOpts::EXAMPLE_WITH_STATE => {
                let _result = task_manager.wait_on_result(ExampleWithState {}).await?;
            }
            TaskOpts::EXAMPLE_RANDOM_DATA => {
                let _result = task_manager
                    .wait_on_result(ExampleStoreRandomData {})
                    .await?;
            }
        },
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct ExampleStoreRandomData {}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize),
    archive_attr(derive(bytecheck::CheckBytes))
)]
struct RandomData {
    data: Vec<u8>,
}

#[async_trait]
impl Task for ExampleStoreRandomData {
    type Output = RandomData;
    const TASK_NAME: &'static str = "ExampleStoreRandomData";

    #[cfg(feature = "rkyv")]
    fn result_message_provider() -> Box<dyn scallion::MessageProvider<Message = Self::Output>> {
        scallion::RkyvMessageProvider::with_options(flate2::Compression::best())
    }

    fn task_options(&self) -> TaskOptions {
        TaskOptions {
            save_result: true,
            ..Default::default()
        }
    }

    async fn task(self: Box<Self>, _: ConsumerState) -> Result<Self::Output, anyhow::Error> {
        let size = rand::random::<u16>();
        let random_bytes: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
        Ok(RandomData { data: random_bytes })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ExampleWithState {}

#[derive(Debug)]
#[allow(dead_code)]
struct ExampleState {
    conf: String,
}

#[async_trait]
impl Task for ExampleWithState {
    type Output = ();
    const TASK_NAME: &'static str = "ExampleWithState";

    async fn task(self: Box<Self>, state: ConsumerState) -> Result<Self::Output, anyhow::Error> {
        let state: Arc<ExampleState> = state.task_state();
        info!(example_state=?state);
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Add {
    x: f64,
    y: f64,
}

#[async_trait]
impl Task for Add {
    type Output = f64;
    const TASK_NAME: &'static str = "Add";

    fn task_options(&self) -> TaskOptions {
        TaskOptions {
            save_result: true,
            ..Default::default()
        }
    }
    async fn task(self: Box<Self>, _: ConsumerState) -> Result<Self::Output, anyhow::Error> {
        Ok(self.x + self.y)
    }
}
