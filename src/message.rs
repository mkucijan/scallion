use std::marker::PhantomData;

use crate::error::TaskMessageError;

pub trait MessageProvider: Send + Sync {
    type Message;
    fn serialize(&self, task: &Self::Message) -> Result<Vec<u8>, TaskMessageError>;
    fn deserialize(&self, data: &[u8]) -> Result<Self::Message, TaskMessageError>;
}

pub struct JsonMessageProvider<T: ?Sized>(PhantomData<T>);

mod json_impl {
    use super::*;

    impl<T> JsonMessageProvider<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        pub fn new_message_provider() -> Box<dyn MessageProvider<Message = T>> {
            Box::new(JsonMessageProvider(PhantomData))
        }
    }

    impl<T> MessageProvider for JsonMessageProvider<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        type Message = T;

        fn serialize(&self, task: &Self::Message) -> Result<Vec<u8>, TaskMessageError> {
            serde_json::to_vec(task).map_err(|e| TaskMessageError::SerdeJsonError {
                message: format!("Failed serializing message"),
                source: e,
            })
        }

        fn deserialize(&self, data: &[u8]) -> Result<Self::Message, TaskMessageError> {
            serde_json::from_slice(data).map_err(|e| TaskMessageError::SerdeJsonError {
                message: format!("Failed deserializing message"),
                source: e,
            })
        }
    }
}

#[cfg(feature = "rkyv")]
pub use rkyv_impl::RkyvMessageProvider;
#[cfg(feature = "rkyv")]
mod rkyv_impl {
    use bytecheck::CheckBytes;
    use rkyv::{
        de::deserializers::SharedDeserializeMap, ser::serializers::AllocSerializer,
        validation::validators::DefaultValidator, Archive, Deserialize, Serialize,
    };

    use super::*;

    pub struct RkyvMessageProvider<T: ?Sized> {
        message: PhantomData<T>,
        compression: flate2::Compression,
    }

    impl<T> RkyvMessageProvider<T>
    where
        T: Serialize<AllocSerializer<4096>> + Archive + Send + Sync + 'static,
        for<'l> <T as Archive>::Archived:
            CheckBytes<DefaultValidator<'l>> + Deserialize<T, SharedDeserializeMap>,
    {
        pub fn new() -> Box<dyn MessageProvider<Message = T>> {
            Box::new(RkyvMessageProvider {
                message: PhantomData,
                compression: flate2::Compression::none(),
            })
        }

        pub fn with_options(
            compression: flate2::Compression,
        ) -> Box<dyn MessageProvider<Message = T>> {
            Box::new(RkyvMessageProvider {
                message: PhantomData,
                compression,
            })
        }
    }

    impl<T> MessageProvider for RkyvMessageProvider<T>
    where
        T: Serialize<AllocSerializer<4096>> + Archive + Send + Sync,
        for<'l> <T as Archive>::Archived:
            CheckBytes<DefaultValidator<'l>> + Deserialize<T, SharedDeserializeMap>,
    {
        type Message = T;

        fn serialize(&self, task: &Self::Message) -> Result<Vec<u8>, TaskMessageError> {
            let encoded = rkyv::to_bytes(task).map(|v| v.into_vec()).map_err(|e| {
                TaskMessageError::RkyvError {
                    message: format!("Failed serializing message"),
                    source: crate::error::RkyvError::SerializerError(e),
                }
            })?;
            let bytes = if self.compression == flate2::Compression::none() {
                encoded
            } else {
                deflate::compress_bytes(encoded.as_slice(), self.compression)?
            };
            Ok(bytes)
        }

        fn deserialize<'a>(&'a self, data: &'a [u8]) -> Result<Self::Message, TaskMessageError> {
            let data_holder;
            let data = if self.compression == flate2::Compression::none() {
                data
            } else {
                data_holder = deflate::decompress_bytes(data)?;
                data_holder.as_slice()
            };
            rkyv::from_bytes(data).map_err(|e| TaskMessageError::RkyvError {
                message: format!("Failed deserialize validation on message"),
                source: crate::error::RkyvError::DeserializerError(format!("{:?}", e)),
            })
        }
    }
}

#[cfg(feature = "protobuf")]
pub use protobuf_impl::ProtobufMessageProvider;
#[cfg(feature = "protobuf")]
mod protobuf_impl {
    use prost::Message;

    use super::*;

    pub struct ProtobufMessageProvider<T: ?Sized> {
        message: PhantomData<T>,
        compression: flate2::Compression,
    }

    impl<T> ProtobufMessageProvider<T>
    where
        T: Message + Default + 'static,
    {
        pub fn new() -> Box<dyn MessageProvider<Message = T>> {
            Box::new(ProtobufMessageProvider {
                message: PhantomData,
                compression: flate2::Compression::none(),
            })
        }

        pub fn with_options(
            compression: flate2::Compression,
        ) -> Box<dyn MessageProvider<Message = T>> {
            Box::new(ProtobufMessageProvider {
                message: PhantomData,
                compression,
            })
        }
    }

    impl<T> MessageProvider for ProtobufMessageProvider<T>
    where
        T: Message + Default + 'static,
    {
        type Message = T;

        fn serialize(&self, task: &Self::Message) -> Result<Vec<u8>, TaskMessageError> {
            let encoded = task.encode_to_vec();
            let bytes = if self.compression == flate2::Compression::none() {
                encoded
            } else {
                deflate::compress_bytes(encoded.as_slice(), self.compression)?
            };
            Ok(bytes)
        }

        fn deserialize<'a>(&'a self, data: &'a [u8]) -> Result<Self::Message, TaskMessageError> {
            let data_holder;
            let data = if self.compression == flate2::Compression::none() {
                data
            } else {
                data_holder = deflate::decompress_bytes(data)?;
                data_holder.as_slice()
            };
            T::decode(data).map_err(|e| TaskMessageError::ProstError {
                message: format!("Failed deserialize validation on message"),
                source: crate::error::ProstError::DeserializerError(e),
            })
        }
    }
}

#[cfg(any(feature = "rkyv", feature = "protobuf"))]
mod deflate {
    use std::io::Write;

    use flate2::{
        write::{ZlibDecoder, ZlibEncoder},
        Compression,
    };

    use crate::error::TaskMessageError;

    pub fn compress_bytes(
        bytes: &[u8],
        compression_level: Compression,
    ) -> Result<Vec<u8>, TaskMessageError> {
        let mut e = ZlibEncoder::new(Vec::with_capacity(bytes.len()), compression_level);
        e.write_all(bytes)
            .map_err(|e| TaskMessageError::CompressionError {
                message: format!("Error while compressing"),
                source: e,
            })?;
        e.finish().map_err(|e| TaskMessageError::CompressionError {
            message: format!("Error while compressing"),
            source: e,
        })
    }

    pub fn decompress_bytes(bytes: &[u8]) -> Result<Vec<u8>, TaskMessageError> {
        let mut d = ZlibDecoder::new(Vec::with_capacity(bytes.len() * 2));
        d.write_all(bytes)
            .map_err(|e| TaskMessageError::CompressionError {
                message: format!("Error while decompressing"),
                source: e,
            })?;
        d.finish().map_err(|e| TaskMessageError::CompressionError {
            message: format!("Error while decompressing"),
            source: e,
        })
    }
}
