mod consumer;
mod consumer_task;
mod container;
mod error;
mod manager;
mod message;
mod registry;
mod result_store;
mod shared_state;
mod stream;
mod task;
mod task_result;
pub mod tasks;

pub use self::{
    consumer::{ConsumerCommands, StreamConsumer, StreamConsumerBuilder},
    consumer_task::{ConsumerTask, TaskOptions},
    container::TaskContainer,
    error::ConsumerError,
    manager::TaskManager,
    message::{JsonMessageProvider, MessageProvider},
    registry::RegisteredTask,
    result_store::TaskOutput,
    shared_state::ConsumerState,
    task::Task,
    task_result::TaskResult,
};

#[cfg(feature = "rkyv")]
pub use self::message::RkyvMessageProvider;

#[cfg(feature = "protobuf")]
pub use self::message::ProtobufMessageProvider;
