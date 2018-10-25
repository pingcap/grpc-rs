// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod client;
pub mod server;

use std::io::{self, BufRead, ErrorKind, Read, Write};
use std::sync::Arc;
use std::{cmp, mem, ptr, slice, usize};

use cq::CompletionQueue;
use futures::{Async, Future, Poll};
use grpc_sys::{
    self, GrpcBatchContext, GrpcByteBuffer, GrpcByteBufferReader, GrpcCall, GrpcCallStatus,
    GrpcSlice,
};
use libc::{c_void, size_t};

use async::{self, BatchFuture, BatchType, CallTag, SpinLock};
use codec::{DeserializeFn, Marshaller, SerializeFn};
use error::{Error, Result};

use grpc_sys::GrpcByteBuffer;
pub use grpc_sys::GrpcStatusCode as RpcStatusCode;

/// Method types supported by gRPC.
#[derive(Clone, Copy)]
pub enum MethodType {
    /// Single request sent from client, single response received from server.
    Unary,

    /// Stream of requests sent from client, single response received from server.
    ClientStreaming,

    /// Single request sent from client, stream of responses received from server.
    ServerStreaming,

    /// Both server and client can stream arbitrary number of requests and responses simultaneously.
    Duplex,
}

/// A description of a remote method.
// TODO: add serializer and deserializer.
pub struct Method<Req, Resp> {
    /// Type of method.
    pub ty: MethodType,

    /// Full qualified name of the method.
    pub name: &'static str,

    /// The marshaller used for request messages.
    pub req_mar: Marshaller<Req>,

    /// The marshaller used for response messages.
    pub resp_mar: Marshaller<Resp>,
}

impl<Req, Resp> Method<Req, Resp> {
    /// Get the request serializer.
    #[inline]
    pub fn req_ser(&self) -> SerializeFn<Req> {
        self.req_mar.ser
    }

    /// Get the request deserializer.
    #[inline]
    pub fn req_de(&self) -> DeserializeFn<Req> {
        self.req_mar.de
    }

    /// Get the response serializer.
    #[inline]
    pub fn resp_ser(&self) -> SerializeFn<Resp> {
        self.resp_mar.ser
    }

    /// Get the response deserializer.
    #[inline]
    pub fn resp_de(&self) -> DeserializeFn<Resp> {
        self.resp_mar.de
    }
}

/// RPC result returned from the server.
#[derive(Debug, Clone)]
pub struct RpcStatus {
    /// gRPC status code. `Ok` indicates success, all other values indicate an error.
    pub status: RpcStatusCode,

    /// Optional detail string.
    pub details: Option<String>,
}

impl RpcStatus {
    /// Create a new [`RpcStatus`].
    pub fn new(status: RpcStatusCode, details: Option<String>) -> RpcStatus {
        RpcStatus { status, details }
    }

    /// Create a new [`RpcStatus`] that status code is Ok.
    pub fn ok() -> RpcStatus {
        RpcStatus::new(RpcStatusCode::Ok, None)
    }
}

struct BufferSlice {
    slice: GrpcSlice,
    offset: usize,
    length: usize,
}

impl BufferSlice {
    pub unsafe fn reset(&mut self) {
        self.length = grpc_sys::grpcwrap_slice_length(&self.slice);
        self.offset = 0;
    }

    pub fn is_finished(&self) -> bool {
        self.offset == self.length
    }

    pub unsafe fn as_slice(&mut self) -> &[u8] {
        let mut len = 0;
        let ptr = grpc_sys::grpcwrap_slice_raw_offset(&self.slice, self.offset, &mut len);
        slice::from_raw_parts(ptr as _, len)
    }
}

/// `MessageReader` is a zero-copy reader for the message payload.
///
/// To achieve zero-copy, use the BufRead API `fill_buf` and `consume`
/// to operate the reader.
pub struct MessageReader {
    buf: *mut GrpcByteBuffer,
    reader: GrpcByteBufferReader,
    buffer_slice: Option<BufferSlice>,
    length: usize,
}

impl MessageReader {
    /// Get the available bytes count of the reader.
    #[inline]
    pub fn pending_bytes_count(&self) -> usize {
        self.length
    }
}

unsafe impl Sync for MessageReader {}
unsafe impl Send for MessageReader {}

impl Read for MessageReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let amt = {
            let bytes = self.fill_buf()?;
            if bytes.is_empty() {
                return Ok(0);
            }
            let amt = cmp::min(buf.len(), bytes.len());
            if amt == 1 {
                buf[0] = bytes[0];
            } else {
                buf[..amt].copy_from_slice(&bytes[..amt]);
            }
            amt
        };
        self.consume(amt);
        Ok(amt)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        if self.length == 0 {
            return Ok(0);
        }
        buf.reserve(self.length);
        let start = buf.len();
        let mut len = start;
        unsafe {
            buf.set_len(start + self.length);
        }
        let ret = loop {
            match self.read(&mut buf[len..]) {
                Ok(0) => break Ok(len - start),
                Ok(n) => len += n,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => break Err(e),
            }
        };
        unsafe {
            buf.set_len(len);
        }
        ret
    }
}

impl BufRead for MessageReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.length == 0 {
            return Ok(&[]);
        }
        unsafe {
            let read_next = match self.buffer_slice {
                Some(ref mut buffer_slice) => {
                    if buffer_slice.is_finished() {
                        grpc_sys::grpc_slice_unref(buffer_slice.slice);
                        true
                    } else {
                        false
                    }
                }
                None => {
                    self.buffer_slice = Some(mem::zeroed());
                    true
                }
            };

            let buffer_slice = self.buffer_slice.as_mut().unwrap();
            if read_next {
                let code = grpc_sys::grpc_byte_buffer_reader_next(
                    &mut self.reader,
                    &mut buffer_slice.slice,
                );
                debug_assert_ne!(code, 0);
                buffer_slice.reset();
            }

            debug_assert!(buffer_slice.offset <= buffer_slice.length);
            Ok(buffer_slice.as_slice())
        }
    }

    fn consume(&mut self, amt: usize) {
        self.length -= amt;

        if let Some(buffer_slice) = self.buffer_slice.as_mut() {
            buffer_slice.offset += amt;
        }
    }
}

impl Drop for MessageReader {
    fn drop(&mut self) {
        unsafe {
            grpc_sys::grpc_byte_buffer_reader_destroy(&mut self.reader);
            if let Some(buffer_slice) = self.buffer_slice.take() {
                grpc_sys::grpc_slice_unref(buffer_slice.slice);
            }
            grpc_sys::grpc_byte_buffer_destroy(self.buf);
        }
    }
}

/// Context for batch request.
pub struct BatchContext {
    ctx: *mut GrpcBatchContext,
}

pub struct MessageWriter {
    data: Vec<GrpcSlice>,
    size: usize,
}

impl MessageWriter {
    pub fn new() -> MessageWriter {
        MessageWriter {
            data: Vec::new(),
            size: 0,
        }
    }

    pub fn clear(&mut self) {
        unsafe {
            for slice in &self.data {
                grpc_sys::grpc_slice_unref(*slice);
            }
        }
        self.data.clear();
        self.size = 0;
    }

    pub unsafe fn as_ptr(&self) -> *mut GrpcByteBuffer {
        grpc_sys::grpc_raw_byte_buffer_create(self.data.as_ptr(), self.data.len())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

impl Drop for MessageWriter {
    fn drop(&mut self) {
        self.clear();
    }
}

impl Write for MessageWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let in_len: size_t = buf.len();
        self.size += in_len;
        unsafe {
            self.data.push(grpc_sys::grpc_slice_from_copied_buffer(
                buf.as_ptr() as _,
                in_len,
            ));
        }
        Ok(in_len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl BatchContext {
    pub fn new() -> BatchContext {
        BatchContext {
            ctx: unsafe { grpc_sys::grpcwrap_batch_context_create() },
        }
    }

    pub fn as_ptr(&self) -> *mut GrpcBatchContext {
        self.ctx
    }

    /// Get the status of the rpc call.
    pub fn rpc_status(&self) -> RpcStatus {
        let status =
            unsafe { grpc_sys::grpcwrap_batch_context_recv_status_on_client_status(self.ctx) };
        let details = if status == RpcStatusCode::Ok {
            None
        } else {
            unsafe {
                let mut details_len = 0;
                let details_ptr = grpc_sys::grpcwrap_batch_context_recv_status_on_client_details(
                    self.ctx,
                    &mut details_len,
                );
                let details_slice = slice::from_raw_parts(details_ptr as *const _, details_len);
                Some(String::from_utf8_lossy(details_slice).into_owned())
            }
        };

        RpcStatus { status, details }
    }

    /// Fetch the response bytes of the rpc call.
    pub fn recv_message(&mut self) -> Option<MessageReader> {
        let buf;
        let mut reader;
        let length;
        unsafe {
            buf = grpc_sys::grpcwrap_batch_context_take_recv_message(self.ctx);
            if buf.is_null() {
                return None;
            }

            reader = mem::zeroed();
            assert_eq!(grpc_sys::grpc_byte_buffer_reader_init(&mut reader, buf), 1);
            length = grpc_sys::grpc_byte_buffer_length(reader.buffer_out);
        }

        Some(MessageReader {
            buf,
            reader,
            buffer_slice: None,
            length,
        })
    }
}

impl Drop for BatchContext {
    fn drop(&mut self) {
        unsafe { grpc_sys::grpcwrap_batch_context_destroy(self.ctx) }
    }
}

#[inline]
fn box_batch_tag(tag: CallTag) -> (*mut GrpcBatchContext, *mut c_void) {
    let tag_box = Box::new(tag);
    (
        tag_box.batch_ctx().unwrap().as_ptr(),
        Box::into_raw(tag_box) as _,
    )
}

/// A helper function that runs the batch call and checks the result.
fn check_run<F>(bt: BatchType, f: F) -> BatchFuture
where
    F: FnOnce(*mut GrpcBatchContext, *mut c_void) -> GrpcCallStatus,
{
    let (cq_f, tag) = CallTag::batch_pair(bt);
    let (batch_ptr, tag_ptr) = box_batch_tag(tag);
    let code = f(batch_ptr, tag_ptr);
    if code != GrpcCallStatus::Ok {
        unsafe {
            Box::from_raw(tag_ptr);
        }
        panic!("create call fail: {:?}", code);
    }
    cq_f
}

/// A Call represents an RPC.
///
/// When created, it is in a configuration state allowing properties to be
/// set until it is invoked. After invoke, the Call can have messages
/// written to it and read from it.
pub struct Call {
    pub call: *mut GrpcCall,
    pub cq: CompletionQueue,
}

unsafe impl Send for Call {}

impl Call {
    pub unsafe fn from_raw(call: *mut grpc_sys::GrpcCall, cq: CompletionQueue) -> Call {
        assert!(!call.is_null());
        Call { call, cq }
    }

    /// Send a message asynchronously.
    pub fn start_send_message(
        &mut self,
        msg: &MessageWriter,
        write_flags: u32,
        initial_meta: bool,
    ) -> Result<BatchFuture> {
        let _cq_ref = self.cq.borrow()?;
        let i = if initial_meta { 1 } else { 0 };
        let f = check_run(BatchType::Finish, |ctx, tag| unsafe {
            let buffer = msg.as_ptr();
            grpc_sys::grpcwrap_call_send_message(self.call, ctx, buffer, write_flags, i, tag)
        });
        Ok(f)
    }

    /// Finish the rpc call from client.
    pub fn start_send_close_client(&mut self) -> Result<BatchFuture> {
        let _cq_ref = self.cq.borrow()?;
        let f = check_run(BatchType::Finish, |_, tag| unsafe {
            grpc_sys::grpcwrap_call_send_close_from_client(self.call, tag)
        });
        Ok(f)
    }

    /// Receive a message asynchronously.
    pub fn start_recv_message(&mut self) -> Result<BatchFuture> {
        let _cq_ref = self.cq.borrow()?;
        let f = check_run(BatchType::Read, |ctx, tag| unsafe {
            grpc_sys::grpcwrap_call_recv_message(self.call, ctx, tag)
        });
        Ok(f)
    }

    /// Start handling from server side.
    ///
    /// Future will finish once close is received by the server.
    pub fn start_server_side(&mut self) -> Result<BatchFuture> {
        let _cq_ref = self.cq.borrow()?;
        let f = check_run(BatchType::Finish, |ctx, tag| unsafe {
            grpc_sys::grpcwrap_call_start_serverside(self.call, ctx, tag)
        });
        Ok(f)
    }

    /// Send a status from server.
    pub fn start_send_status_from_server(
        &mut self,
        status: &RpcStatus,
        send_empty_metadata: bool,
        payload: &Option<MessageWriter>,
        write_flags: u32,
    ) -> Result<BatchFuture> {
        let _cq_ref = self.cq.borrow()?;
        let send_empty_metadata = if send_empty_metadata { 1 } else { 0 };
        let buffer = payload
            .as_ref()
            .map_or_else(ptr::null_mut, |p| unsafe { p.as_ptr() });
        let f = check_run(BatchType::Finish, |ctx, tag| unsafe {
            let (details_ptr, details_len) = status
                .details
                .as_ref()
                .map_or_else(|| (ptr::null(), 0), |s| (s.as_ptr() as _, s.len()));
            grpc_sys::grpcwrap_call_send_status_from_server(
                self.call,
                ctx,
                status.status,
                details_ptr,
                details_len,
                ptr::null_mut(),
                send_empty_metadata,
                buffer,
                write_flags,
                tag,
            )
        });
        Ok(f)
    }

    /// Abort an rpc call before handler is called.
    pub fn abort(self, status: &RpcStatus) {
        match self.cq.borrow() {
            // Queue is shutdown, ignore.
            Err(Error::QueueShutdown) => return,
            Err(e) => panic!("unexpected error when aborting call: {:?}", e),
            _ => {}
        }
        let call_ptr = self.call;
        let tag = CallTag::abort(self);
        let (batch_ptr, tag_ptr) = box_batch_tag(tag);

        let code = unsafe {
            let (details_ptr, details_len) = status
                .details
                .as_ref()
                .map_or_else(|| (ptr::null(), 0), |s| (s.as_ptr() as _, s.len()));
            grpc_sys::grpcwrap_call_send_status_from_server(
                call_ptr,
                batch_ptr,
                status.status,
                details_ptr,
                details_len,
                ptr::null_mut(),
                1,
                ptr::null_mut(),
                0,
                tag_ptr as *mut c_void,
            )
        };
        if code != GrpcCallStatus::Ok {
            unsafe {
                Box::from_raw(tag_ptr);
            }
            panic!("create call fail: {:?}", code);
        }
    }

    /// Cancel the rpc call by client.
    fn cancel(&self) {
        match self.cq.borrow() {
            // Queue is shutdown, ignore.
            Err(Error::QueueShutdown) => return,
            Err(e) => panic!("unexpected error when canceling call: {:?}", e),
            _ => {}
        }
        unsafe {
            grpc_sys::grpc_call_cancel(self.call, ptr::null_mut());
        }
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        unsafe { grpc_sys::grpc_call_unref(self.call) }
    }
}

/// A share object for client streaming and duplex streaming call.
///
/// In both cases, receiver and sender can be polled in the same time,
/// hence we need to share the call in the both sides and abort the sink
/// once the call is canceled or finished early.
struct ShareCall {
    call: Call,
    close_f: BatchFuture,
    finished: bool,
    status: Option<RpcStatus>,
}

impl ShareCall {
    fn new(call: Call, close_f: BatchFuture) -> ShareCall {
        ShareCall {
            call,
            close_f,
            finished: false,
            status: None,
        }
    }

    /// Poll if the call is still alive.
    ///
    /// If the call is still running, will register a notification for its completion.
    fn poll_finish(&mut self) -> Poll<Option<MessageReader>, Error> {
        let res = match self.close_f.poll() {
            Err(Error::RpcFailure(status)) => {
                self.status = Some(status.clone());
                Err(Error::RpcFailure(status))
            }
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Ok(Async::Ready(msg)) => {
                self.status = Some(RpcStatus::ok());
                Ok(Async::Ready(msg))
            }
            res => res,
        };

        self.finished = true;
        res
    }

    /// Check if the call is finished.
    fn check_alive(&mut self) -> Result<()> {
        if self.finished {
            // maybe can just take here.
            return Err(Error::RpcFinished(self.status.clone()));
        }

        async::check_alive(&self.close_f)
    }
}

/// A helper trait that allows executing function on the inernal `ShareCall` struct.
trait ShareCallHolder {
    fn call<R, F: FnOnce(&mut ShareCall) -> R>(&mut self, f: F) -> R;
}

impl ShareCallHolder for ShareCall {
    fn call<R, F: FnOnce(&mut ShareCall) -> R>(&mut self, f: F) -> R {
        f(self)
    }
}

impl ShareCallHolder for Arc<SpinLock<ShareCall>> {
    fn call<R, F: FnOnce(&mut ShareCall) -> R>(&mut self, f: F) -> R {
        let mut call = self.lock();
        f(&mut call)
    }
}

/// A helper struct for constructing Stream object for batch requests.
struct StreamingBase {
    close_f: Option<BatchFuture>,
    msg_f: Option<BatchFuture>,
    read_done: bool,
}

impl StreamingBase {
    fn new(close_f: Option<BatchFuture>) -> StreamingBase {
        StreamingBase {
            close_f,
            msg_f: None,
            read_done: false,
        }
    }

    fn poll<C: ShareCallHolder>(
        &mut self,
        call: &mut C,
        skip_finish_check: bool,
    ) -> Poll<Option<MessageReader>, Error> {
        if !skip_finish_check {
            let mut finished = false;
            if let Some(ref mut close_f) = self.close_f {
                match close_f.poll() {
                    Ok(Async::Ready(_)) => {
                        // don't return immediately, there maybe pending data.
                        finished = true;
                    }
                    Err(e) => return Err(e),
                    Ok(Async::NotReady) => {}
                }
            }
            if finished {
                self.close_f.take();
            }
        }

        let mut bytes = None;
        if !self.read_done {
            if let Some(ref mut msg_f) = self.msg_f {
                bytes = try_ready!(msg_f.poll());
                if bytes.is_none() {
                    self.read_done = true;
                }
            }
        }

        if self.read_done {
            if self.close_f.is_none() {
                return Ok(Async::Ready(bytes));
            }
            return Ok(Async::NotReady);
        }

        // so msg_f must be either stale or not initialised yet.
        self.msg_f.take();
        let msg_f = call.call(|c| c.call.start_recv_message())?;
        self.msg_f = Some(msg_f);
        if bytes.is_none() {
            self.poll(call, true)
        } else {
            Ok(Async::Ready(bytes))
        }
    }

    // Cancel the call if we still have some messages or did not
    // receive status code.
    fn on_drop<C: ShareCallHolder>(&self, call: &mut C) {
        if !self.read_done || self.close_f.is_some() {
            call.call(|c| c.call.cancel());
        }
    }
}

/// Flags for write operations.
#[derive(Default, Clone, Copy)]
pub struct WriteFlags {
    flags: u32,
}

impl WriteFlags {
    /// Hint that the write may be buffered and need not go out on the wire immediately.
    ///
    /// gRPC is free to buffer the message until the next non-buffered write, or until write stream
    /// completion, but it need not buffer completely or at all.
    pub fn buffer_hint(mut self, need_buffered: bool) -> WriteFlags {
        client::change_flag(
            &mut self.flags,
            grpc_sys::GRPC_WRITE_BUFFER_HINT,
            need_buffered,
        );
        self
    }

    /// Force compression to be disabled.
    pub fn force_no_compress(mut self, no_compress: bool) -> WriteFlags {
        client::change_flag(
            &mut self.flags,
            grpc_sys::GRPC_WRITE_NO_COMPRESS,
            no_compress,
        );
        self
    }

    /// Get whether buffer hint is enabled.
    pub fn get_buffer_hint(self) -> bool {
        (self.flags & grpc_sys::GRPC_WRITE_BUFFER_HINT) != 0
    }

    /// Get whether compression is disabled.
    pub fn get_force_no_compress(self) -> bool {
        (self.flags & grpc_sys::GRPC_WRITE_NO_COMPRESS) != 0
    }
}

/// A helper struct for constructing Sink object for batch requests.
struct SinkBase {
    batch_f: Option<BatchFuture>,
    buf: MessageWriter,
    send_metadata: bool,
}

impl SinkBase {
    fn new(send_metadata: bool) -> SinkBase {
        SinkBase {
            batch_f: None,
            buf: MessageWriter::new(),
            send_metadata,
        }
    }

    fn start_send<T, C: ShareCallHolder>(
        &mut self,
        call: &mut C,
        t: &T,
        mut flags: WriteFlags,
        ser: SerializeFn<T>,
    ) -> Result<bool> {
        if self.batch_f.is_some() {
            // try its best not to return false.
            self.poll_complete()?;
            if self.batch_f.is_some() {
                return Ok(false);
            }
        }

        self.buf.clear();
        ser(t, &mut self.buf);
        if flags.get_buffer_hint() && self.send_metadata {
            // temporary fix: buffer hint with send meta will not send out any metadata.
            flags = flags.buffer_hint(false);
        }
        self.batch_f = Some(call.call(|c| {
            c.call
                .start_send_message(&self.buf, flags.flags, self.send_metadata)
        })?);
        self.send_metadata = false;
        Ok(true)
    }

    fn poll_complete(&mut self) -> Poll<(), Error> {
        if let Some(ref mut batch_f) = self.batch_f {
            try_ready!(batch_f.poll());
        }

        self.batch_f.take();
        Ok(Async::Ready(()))
    }
}

#[cfg(test)]
mod tests {
    use grpc_sys;
    use std::mem;

    use super::*;

    unsafe fn make_message_reader(source: &[u8], nslice: usize) -> MessageReader {
        let mut slices = Vec::with_capacity(nslice);
        for _ in 0..nslice {
            let len = source.len();
            let mut slice = grpc_sys::grpc_slice_from_copied_buffer(source.as_ptr() as _, len);
            slices.push(slice);
        }
        let buf = grpc_sys::grpc_raw_byte_buffer_create(slices.as_mut_ptr(), nslice);
        let mut reader = mem::zeroed();
        assert_eq!(grpc_sys::grpc_byte_buffer_reader_init(&mut reader, buf), 1);
        let length = grpc_sys::grpc_byte_buffer_length(reader.buffer_out);

        MessageReader {
            buf,
            reader,
            buffer_slice: None,
            length,
        }
    }

    #[test]
    fn test_message_reader() {
        for len in vec![0, 1, 2, 16, 32, 64, 128, 256, 512, 1024] {
            for nslice in 1..4 {
                let source = vec![len as u8; len];
                let expect = vec![len as u8; len * nslice];
                // Test read.
                let mut reader = unsafe { make_message_reader(&source, nslice) };
                let mut dest = [0; 7];
                let amt = reader.read(&mut dest).unwrap();

                assert_eq!(
                    dest[..amt],
                    expect[..amt],
                    "len: {}, nslice: {}",
                    len,
                    nslice
                );

                // Read after move.
                let mut box_reader = Box::new(reader);
                let amt = box_reader.read(&mut dest).unwrap();
                assert_eq!(
                    dest[..amt],
                    expect[..amt],
                    "len: {}, nslice: {}",
                    len,
                    nslice
                );

                // Test read_to_end.
                let mut reader = unsafe { make_message_reader(&source, nslice) };
                let mut dest = vec![];
                reader.read_to_end(&mut dest).unwrap();
                assert_eq!(dest, expect, "len: {}, nslice: {}", len, nslice);
            }
        }
    }
}
