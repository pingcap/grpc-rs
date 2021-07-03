// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, fmt, result};

use crate::call::RpcStatus;
use crate::grpc_sys::grpc_call_error;

#[cfg(feature = "protobuf-codec")]
use protobuf::ProtobufError;

/// Errors generated from this library.
#[derive(Debug)]
pub enum Error {
    /// Codec error.
    Codec(Box<dyn error::Error + Send + Sync>),
    /// Failed to start an internal async call.
    CallFailure(grpc_call_error),
    /// Rpc request fail.
    RpcFailure(RpcStatus),
    /// Try to write to a finished rpc call.
    RpcFinished(Option<RpcStatus>),
    /// Remote is stopped.
    RemoteStopped,
    /// Failed to shutdown.
    ShutdownFailed,
    /// Failed to bind.
    BindFail(String, u16),
    /// gRPC completion queue is shutdown.
    QueueShutdown,
    /// Failed to create Google default credentials.
    GoogleAuthenticationFailed,
    /// Invalid format of metadata.
    InvalidMetadata(String),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::RpcFailure(s) => {
                if s.message().is_empty() {
                    write!(fmt, "RpcFailure: {}", s.code())
                } else {
                    write!(fmt, "RpcFailure: {} {}", s.code(), s.message())
                }
            }
            other_error => write!(fmt, "{:?}", other_error),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Error::Codec(ref e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

#[cfg(feature = "protobuf-codec")]
impl From<ProtobufError> for Error {
    fn from(e: ProtobufError) -> Error {
        Error::Codec(Box::new(e))
    }
}

#[cfg(feature = "prost-codec")]
impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Error {
        Error::Codec(Box::new(e))
    }
}

#[cfg(feature = "prost-codec")]
impl From<prost::EncodeError> for Error {
    fn from(e: prost::EncodeError) -> Error {
        Error::Codec(Box::new(e))
    }
}

/// Type alias to use this library's [`Error`] type in a `Result`.
pub type Result<T> = result::Result<T, Error>;

#[cfg(all(test, feature = "protobuf-codec"))]
mod tests {
    use std::error::Error as StdError;

    use protobuf::error::WireError;
    use protobuf::ProtobufError;

    use super::Error;

    #[test]
    fn test_convert() {
        let error = ProtobufError::WireError(WireError::UnexpectedEof);
        let e: Error = error.into();
        assert_eq!(e.to_string(), "Codec(WireError(UnexpectedEof))");
        assert!(e.source().is_some());
    }
}
