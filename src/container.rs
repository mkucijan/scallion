use redis::streams::StreamId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::TaskMessageError;

pub struct TaskContainer {
    pub(crate) stream_id: String,
    pub(crate) header: TaskHeader,
    pub(crate) data: Vec<u8>,
}

impl TaskContainer {
    pub fn stream_id(&self) -> &String {
        &self.stream_id
    }

    pub fn header(&self) -> TaskHeader {
        self.header.clone()
    }

    pub fn id(&self) -> String {
        self.header.id.to_string()
    }

    pub fn task_name(&self) -> &String {
        &self.header.task_name
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn increment_retry(&mut self) {
        self.header.retry_count += 1;
    }

    pub fn message(self) -> [(String, Vec<u8>); 2] {
        [
            (
                "task_header".to_string(),
                serde_json::to_vec(&self.header).expect("Failed serializing task header message"),
            ),
            ("task_data".to_string(), self.data),
        ]
    }

    pub fn from_stream_message(message: StreamId) -> Result<Self, TaskMessageError> {
        let stream_id = message.id.as_str();
        let header: TaskHeader = serde_json::from_str(
            message
                .get::<String>("task_header")
                .ok_or_else(|| {
                    TaskMessageError::MissingKey(format!(
                        "Missing task_header. Stream id: {}",
                        stream_id
                    ))
                })?
                .as_str(),
        )
        .map_err(|e| TaskMessageError::SerdeJsonError {
            message: format!(
                "Failed task header deserialization. Stream id: {}",
                stream_id
            ),
            source: e,
        })?;
        let data: Vec<u8> = message.get::<Vec<u8>>("task_data").ok_or_else(|| {
            TaskMessageError::MissingKey(format!("Missing task_data. Stream id: {}", stream_id))
        })?;
        Ok(Self {
            stream_id: message.id,
            header,
            data,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHeader {
    pub id: Uuid,
    pub task_name: String,
    pub retry_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct TaskMessageWrapper {
    pub header: TaskHeader,
    pub data: String,
}
