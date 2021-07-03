// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::buf::GrpcSlice;
use crate::call::MessageReader;
use crate::error::Result;

pub type DeserializeFn<T> = fn(MessageReader) -> Result<T>;
pub type SerializeFn<T> = fn(&T, &mut GrpcSlice) -> Result<()>;

/// According to https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md, grpc uses
/// a four bytes to describe the length of a message, so it should not exceed u32::MAX.
pub const MAX_MESSAGE_SIZE: usize = u32::MAX as usize;

/// Defines how to serialize and deserialize between the specialized type and byte slice.
pub struct Marshaller<T> {
    // Use function pointer here to simplify the signature.
    // Compiler will probably inline the function so performance
    // impact can be omitted.
    //
    // Using trait will require a trait object or generic, which will
    // either have performance impact or make signature complicated.
    //
    // const function is not stable yet (rust-lang/rust#24111), hence
    // make all fields public.
    /// The serialize function.
    pub ser: SerializeFn<T>,

    /// The deserialize function.
    pub de: DeserializeFn<T>,
}

#[cfg(feature = "protobuf-codec")]
pub mod pb_codec {
    use protobuf::{CodedInputStream, CodedOutputStream, Message};

    use super::{MessageReader, MAX_MESSAGE_SIZE};
    use crate::buf::GrpcSlice;
    use crate::error::{Error, Result};

    #[inline]
    pub fn ser<T: Message>(t: &T, buf: &mut GrpcSlice) -> Result<()> {
        let cap = t.compute_size() as usize;
        // FIXME: This is not a practical fix until stepancheg/rust-protobuf#530 is fixed.
        if cap <= MAX_MESSAGE_SIZE {
            unsafe {
                let bytes = buf.realloc(cap);
                let raw_bytes = &mut *(bytes as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]);
                let mut s = CodedOutputStream::bytes(raw_bytes);
                t.write_to_with_cached_sizes(&mut s).map_err(Into::into)
            }
        } else {
            Err(Error::Codec(
                format!("message is too large: {} > {}", cap, MAX_MESSAGE_SIZE).into(),
            ))
        }
    }

    #[inline]
    pub fn de<T: Message>(mut reader: MessageReader) -> Result<T> {
        let mut s = CodedInputStream::from_buffered_reader(&mut reader);
        let mut m = T::new();
        m.merge_from(&mut s)?;
        Ok(m)
    }
}

#[cfg(feature = "prost-codec")]
pub mod pr_codec {
    use prost::Message;
    use std::u32;

    use super::MessageReader;
    use crate::buf::GrpcSlice;
    use crate::error::{Error, Result};

    #[inline]
    pub fn ser<M: Message>(msg: &M, buf: &mut GrpcSlice) -> Result<()> {
        let size = msg.encoded_len();
        if size <= MAX_MESSAGE_SIZE {
            unsafe {
                let bytes = buf.realloc(size);
                let mut b = &mut *(bytes as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]);
                msg.encode(&mut b)?;
                debug_assert!(b.is_empty());
            }
            Ok(())
        } else {
            Err(Error::Codec(
                format!("message is too large: {} > {}", size, MAX_MESSAGE_SIZE).into(),
            ))
        }
    }

    #[inline]
    pub fn de<M: Message + Default>(mut reader: MessageReader) -> Result<M> {
        use bytes::buf::Buf;
        reader.advance(0);
        M::decode(reader).map_err(Into::into)
    }
}
