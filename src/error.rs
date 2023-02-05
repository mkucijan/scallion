use std::error::Error;

use thiserror::Error;

use crate::ConsumerCommands;

#[derive(Debug, Error)]
pub enum ConsumerError {
    #[error("Redis error: '{0}'")]
    RedisError(#[from] redis::RedisError),
    #[error("Join error: '{0}'")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Task Message error: '{0}'")]
    TaskMessageError(#[from] TaskMessageError),
    #[error("Runtime error: '{0}'")]
    RuntimeError(String),
    #[error("Task error: '{0};")]
    TaskError(#[from] TaskError),
}

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("Task Error: '{0}'")]
    TaskFailed(#[from] Box<dyn Error + Send + Sync + 'static>),
    #[error("Task Message error: '{0}'")]
    TaskMessageError(#[from] TaskMessageError),
    #[error("Redis error: '{0}'")]
    RedisError(#[from] redis::RedisError),
    #[error("Consumer Channel error: '{0}")]
    ChannelError(#[from] tokio::sync::mpsc::error::SendError<ConsumerCommands>),
}

#[derive(Debug, Error)]
pub enum TaskMessageError {
    #[error("Serde error: {message}\n{source}")]
    SerdeJsonError {
        message: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("Missing key in message: {0}")]
    MissingKey(String),
    #[cfg(feature = "rkyv")]
    #[error("Rykv error: {message}\n{source}")]
    RkyvError {
        message: String,
        #[source]
        source: RkyvError,
    },
    #[cfg(feature = "protobuf")]
    #[error("Prost error: {message}\n{source}")]
    ProstError {
        message: String,
        #[source]
        source: ProstError,
    },
    #[error("Compression Error: {message}\n{source}")]
    CompressionError {
        message: String,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(feature = "rkyv")]
#[derive(Debug, Error)]
pub enum RkyvError {
    #[error("Serializer Error: '{0}'")]
    SerializerError(
        #[from]
        rkyv::ser::serializers::CompositeSerializerError<
            std::convert::Infallible,
            rkyv::ser::serializers::AllocScratchError,
            rkyv::ser::serializers::SharedSerializeMapError,
        >,
    ),
    #[error("Deserializer Error: '{0}'")]
    DeserializerError(String),
}

#[cfg(feature = "protobuf")]
#[derive(Debug, Error)]
pub enum ProstError {
    #[error("Deserializer Error: '{0}'")]
    DeserializerError(#[from] prost::DecodeError),
}
