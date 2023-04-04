use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use systemstat::{saturating_sub_bytes, Platform, System};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    consumer::ConsumerCommands, consumer_task::TaskOptions, shared_state::ConsumerState, Task,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Ping {
    pub uuid: Uuid,
    pub created_at: DateTime<Utc>,
    pub delay_ms: Option<u64>,
    pub spawn_task: bool,
    pub fake_failure: bool,
}

impl Default for Ping {
    fn default() -> Self {
        Self {
            uuid: Uuid::new_v4(),
            created_at: Utc::now(),
            delay_ms: None,
            spawn_task: false,
            fake_failure: false,
        }
    }
}

#[async_trait]
impl Task for Ping {
    type Output = ();
    const TASK_NAME: &'static str = "utilites.ping";

    fn task_options(&self) -> TaskOptions {
        TaskOptions {
            spawn: self.spawn_task,
            ..Default::default()
        }
    }

    async fn task(
        self: Box<Self>,
        _: ConsumerState,
    ) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync + 'static>> {
        info!(
            id = self.uuid.to_string().as_str(),
            created_at = self.created_at.to_string().as_str(),
            "Received PING."
        );
        if let Some(delay) = self.delay_ms {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        if self.fake_failure {
            return Err("Fake task failure".into());
        }
        info!(
            id = self.uuid.to_string().as_str(),
            created_at = self.created_at.to_string().as_str(),
            "Ping finished."
        );
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Shutdown {
    shutdown_time: DateTime<Utc>,
}

impl Shutdown {
    pub fn now() -> Self {
        Shutdown {
            shutdown_time: Utc::now(),
        }
    }

    pub fn with_delay(delay: std::time::Duration) -> Self {
        Shutdown {
            shutdown_time: Utc::now()
                + chrono::Duration::from_std(delay)
                    .expect("Fail converting std duration to chrono"),
        }
    }
}

#[async_trait]
impl Task for Shutdown {
    type Output = ();
    const TASK_NAME: &'static str = "utilites.shutdown";

    fn task_options(&self) -> TaskOptions {
        TaskOptions {
            broadcast: true,
            ..Default::default()
        }
    }

    async fn task(
        self: Box<Self>,
        state: ConsumerState,
    ) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync + 'static>> {
        if self.shutdown_time > Utc::now() {
            tokio::time::sleep(
                (Utc::now() - self.shutdown_time)
                    .to_std()
                    .map_err(|e| format!("Failed converting chrono duration to std:\n{e:?}"))?,
            )
            .await
        }

        state.send_command(ConsumerCommands::Shutdown).await?;

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CheckConsumerStatus {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerStatus {
    created_at: DateTime<Utc>,
    cpu_perc: f64,
    memory_perc: f64,
}

#[async_trait]
impl Task for CheckConsumerStatus {
    type Output = Option<ConsumerStatus>;
    const TASK_NAME: &'static str = "utilites.check_consumer_status";

    fn task_options(&self) -> TaskOptions {
        TaskOptions {
            save_result: true,
            ..Default::default()
        }
    }

    async fn task(
        self: Box<Self>,
        _: ConsumerState,
    ) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut cpu_perc = None;
        let mut memory_perc = None;

        let sys = System::new();
        match sys.memory() {
            Ok(mem) => {
                let memory_used = saturating_sub_bytes(mem.total, mem.free);
                memory_perc = Some(memory_used.as_u64() as f64 / mem.total.as_u64() as f64);
                info!(
                    "Memory: {} used / {} ({} bytes) total",
                    memory_used,
                    mem.total,
                    mem.total.as_u64(),
                )
            }
            Err(x) => error!("\nMemory: error: {}", x),
        }
        match sys.cpu_load_aggregate() {
            Ok(cpu) => {
                info!("Measuring CPU load...");
                tokio::time::sleep(Duration::from_secs(1)).await;
                let cpu = cpu.done().unwrap();
                cpu_perc = Some(1. - cpu.idle as f64);
                info!(
                    "CPU load: {}% user, {}% nice, {}% system, {}% intr, {}% idle ",
                    cpu.user * 100.0,
                    cpu.nice * 100.0,
                    cpu.system * 100.0,
                    cpu.interrupt * 100.0,
                    cpu.idle * 100.0
                );
            }
            Err(x) => error!("\nCPU load: error: {x}"),
        }
        Ok(
            if let (Some(cpu_perc), Some(memory_perc)) = (cpu_perc, memory_perc) {
                Some(ConsumerStatus {
                    created_at: Utc::now(),
                    cpu_perc,
                    memory_perc,
                })
            } else {
                None
            },
        )
    }
}
