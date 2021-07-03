// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub mod client;
pub mod server;

use std::fmt::{self, Debug, Display};
use std::pin::Pin;
use std::sync::Arc;
use std::{ptr, slice};

use crate::grpc_sys::{self, grpc_call, grpc_call_error, grpcwrap_batch_context};
use crate::{cq::CompletionQueue, Metadata, MetadataBuilder};
use futures::future::Future;
use futures::ready;
use futures::task::{Context, Poll};
use libc::c_void;
use parking_lot::Mutex;

use crate::buf::{GrpcByteBuffer, GrpcByteBufferReader, GrpcSlice};
use crate::codec::{DeserializeFn, Marshaller, SerializeFn};
use crate::error::{Error, Result};
use crate::grpc_sys::grpc_status_code::*;
use crate::task::{self, BatchFuture, BatchType, CallTag};

/// An gRPC status code structure.
/// This type contains constants for all gRPC status codes.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct RpcStatusCode(i32);

impl From<i32> for RpcStatusCode {
    fn from(code: i32) -> RpcStatusCode {
        RpcStatusCode(code)
    }
}

impl From<RpcStatusCode> for i32 {
    fn from(code: RpcStatusCode) -> i32 {
        code.0
    }
}

impl Display for RpcStatusCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

macro_rules! status_codes {
    (
        $(
            ($num:path, $konst:ident);
        )+
    ) => {
        impl RpcStatusCode {
        $(
            pub const $konst: RpcStatusCode = RpcStatusCode($num);
        )+
        }

        impl Debug for RpcStatusCode {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    f,
                    "{}-{}",
                    self.0,
                    match self {
                        $(RpcStatusCode($num) => stringify!($konst),)+
                        RpcStatusCode(_) => "INVALID_STATUS_CODE",
                    }
                )
            }
        }
    }
}

status_codes! {
    (GRPC_STATUS_OK, OK);
    (GRPC_STATUS_CANCELLED, CANCELLED);
    (GRPC_STATUS_UNKNOWN, UNKNOWN);
    (GRPC_STATUS_INVALID_ARGUMENT, INVALID_ARGUMENT);
    (GRPC_STATUS_DEADLINE_EXCEEDED, DEADLINE_EXCEEDED);
    (GRPC_STATUS_NOT_FOUND, NOT_FOUND);
    (GRPC_STATUS_ALREADY_EXISTS, ALREADY_EXISTS);
    (GRPC_STATUS_PERMISSION_DENIED, PERMISSION_DENIED);
    (GRPC_STATUS_RESOURCE_EXHAUSTED, RESOURCE_EXHAUSTED);
    (GRPC_STATUS_FAILED_PRECONDITION, FAILED_PRECONDITION);
    (GRPC_STATUS_ABORTED, ABORTED);
    (GRPC_STATUS_OUT_OF_RANGE, OUT_OF_RANGE);
    (GRPC_STATUS_UNIMPLEMENTED, UNIMPLEMENTED);
    (GRPC_STATUS_INTERNAL, INTERNAL);
    (GRPC_STATUS_UNAVAILABLE, UNAVAILABLE);
    (GRPC_STATUS_DATA_LOSS, DATA_LOSS);
    (GRPC_STATUS_UNAUTHENTICATED, UNAUTHENTICATED);
    (GRPC_STATUS__DO_NOT_USE, DO_NOT_USE);
}

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
    code: RpcStatusCode,

    /// error message.
    message: String,

    /// Additional details for rich error model.
    ///
    /// See also https://grpc.io/docs/guides/error/#richer-error-model.
    details: Vec<u8>,
}

impl Display for RpcStatus {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, fmt)
    }
}

impl RpcStatus {
    /// Create a new [`RpcStatus`].
    pub fn new<T: Into<RpcStatusCode>>(code: T) -> RpcStatus {
        RpcStatus::with_message(code, String::new())
    }

    /// Create a new [`RpcStatus`] with given message.
    pub fn with_message<T: Into<RpcStatusCode>>(code: T, message: String) -> RpcStatus {
        RpcStatus::with_details(code, message, vec![])
    }

    /// Create a new [`RpcStats`] with code, message and details.
    ///
    /// If using rich error model, `details` should be binary message that sets `code` and
    /// `message` to the same value. Or you can use `into` method to do automatical
    /// transformation if using `grpcio_proto::google::rpc::Status`.
    pub fn with_details<T: Into<RpcStatusCode>>(
        code: T,
        message: String,
        details: Vec<u8>,
    ) -> RpcStatus {
        RpcStatus {
            code: code.into(),
            message,
            details,
        }
    }

    /// Create a new [`RpcStatus`] that status code is Ok.
    pub fn ok() -> RpcStatus {
        RpcStatus::new(RpcStatusCode::OK)
    }

    /// Return the instance's error code.
    #[inline]
    pub fn code(&self) -> RpcStatusCode {
        self.code
    }

    /// Return the instance's error message.
    #[inline]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Return the (binary) error details.
    ///
    /// Usually it contains a serialized `google.rpc.Status` proto.
    pub fn details(&self) -> &[u8] {
        &self.details
    }
}

pub type MessageReader = GrpcByteBufferReader;

/// Context for batch request.
pub struct BatchContext {
    ctx: *mut grpcwrap_batch_context,
}

impl BatchContext {
    pub fn new() -> BatchContext {
        BatchContext {
            ctx: unsafe { grpc_sys::grpcwrap_batch_context_create() },
        }
    }

    pub fn as_ptr(&self) -> *mut grpcwrap_batch_context {
        self.ctx
    }

    pub fn take_recv_message(&self) -> Option<GrpcByteBuffer> {
        let ptr = unsafe { grpc_sys::grpcwrap_batch_context_take_recv_message(self.ctx) };
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { GrpcByteBuffer::from_raw(ptr) })
        }
    }

    /// Get the status of the rpc call.
    pub fn rpc_status(&self) -> RpcStatus {
        let status = RpcStatusCode(unsafe {
            grpc_sys::grpcwrap_batch_context_recv_status_on_client_status(self.ctx)
        });

        if status == RpcStatusCode::OK {
            RpcStatus::ok()
        } else {
            unsafe {
                let mut msg_len = 0;
                let details_ptr = grpc_sys::grpcwrap_batch_context_recv_status_on_client_details(
                    self.ctx,
                    &mut msg_len,
                );
                let msg_slice = slice::from_raw_parts(details_ptr as *const _, msg_len);
                let message = String::from_utf8_lossy(msg_slice).into_owned();
                let m_ptr =
                    grpc_sys::grpcwrap_batch_context_recv_status_on_client_trailing_metadata(
                        self.ctx,
                    );
                let metadata = &*(m_ptr as *const Metadata);
                let details = metadata.search_binary_error_details().to_vec();
                RpcStatus::with_details(status, message, details)
            }
        }
    }

    /// Fetch the response bytes of the rpc call.
    pub fn recv_message(&mut self) -> Option<MessageReader> {
        let buf = self.take_recv_message()?;
        Some(GrpcByteBufferReader::new(buf))
    }
}

impl Drop for BatchContext {
    fn drop(&mut self) {
        unsafe { grpc_sys::grpcwrap_batch_context_destroy(self.ctx) }
    }
}

#[inline]
fn box_batch_tag(tag: CallTag) -> (*mut grpcwrap_batch_context, *mut c_void) {
    let tag_box = Box::new(tag);
    (
        tag_box.batch_ctx().unwrap().as_ptr(),
        Box::into_raw(tag_box) as _,
    )
}

/// A helper function that runs the batch call and checks the result.
fn check_run<F>(bt: BatchType, f: F) -> BatchFuture
where
    F: FnOnce(*mut grpcwrap_batch_context, *mut c_void) -> grpc_call_error,
{
    let (cq_f, tag) = CallTag::batch_pair(bt);
    let (batch_ptr, tag_ptr) = box_batch_tag(tag);
    let code = f(batch_ptr, tag_ptr);
    if code != grpc_call_error::GRPC_CALL_OK {
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
    pub call: *mut grpc_call,
    pub cq: CompletionQueue,
}

unsafe impl Send for Call {}

impl Call {
    pub unsafe fn from_raw(call: *mut grpc_sys::grpc_call, cq: CompletionQueue) -> Call {
        assert!(!call.is_null());
        Call { call, cq }
    }

    /// Send a message asynchronously.
    pub fn start_send_message(
        &mut self,
        msg: &mut GrpcSlice,
        write_flags: u32,
        initial_meta: bool,
    ) -> Result<BatchFuture> {
        let _cq_ref = self.cq.borrow()?;
        let i = if initial_meta { 1 } else { 0 };
        let f = check_run(BatchType::Finish, |ctx, tag| unsafe {
            grpc_sys::grpcwrap_call_send_message(
                self.call,
                ctx,
                msg.as_mut_ptr(),
                write_flags,
                i,
                tag,
            )
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
        payload: &mut Option<GrpcSlice>,
        write_flags: u32,
    ) -> Result<BatchFuture> {
        let _cq_ref = self.cq.borrow()?;
        let send_empty_metadata = if send_empty_metadata { 1 } else { 0 };
        let f = check_run(BatchType::Finish, |ctx, tag| unsafe {
            let (msg_ptr, msg_len) = if status.code() == RpcStatusCode::OK {
                (ptr::null(), 0)
            } else {
                (status.message.as_ptr(), status.message.len())
            };
            let payload_p = match payload {
                Some(p) => p.as_mut_ptr(),
                None => ptr::null_mut(),
            };
            let mut trailing_metadata = if status.details.is_empty() {
                None
            } else {
                let mut builder = MetadataBuilder::new();
                builder.set_binary_error_details(&status.details);
                Some(builder.build())
            };
            grpc_sys::grpcwrap_call_send_status_from_server(
                self.call,
                ctx,
                status.code().into(),
                msg_ptr as _,
                msg_len,
                trailing_metadata
                    .as_mut()
                    .map_or_else(ptr::null_mut, |m| m as *mut _ as _),
                send_empty_metadata,
                payload_p,
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
            let (msg_ptr, msg_len) = if status.code() == RpcStatusCode::OK {
                (ptr::null(), 0)
            } else {
                (status.message.as_ptr(), status.message.len())
            };
            grpc_sys::grpcwrap_call_send_status_from_server(
                call_ptr,
                batch_ptr,
                status.code().into(),
                msg_ptr as _,
                msg_len,
                ptr::null_mut(),
                1,
                ptr::null_mut(),
                0,
                tag_ptr as *mut c_void,
            )
        };
        if code != grpc_call_error::GRPC_CALL_OK {
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
    fn poll_finish(&mut self, cx: &mut Context) -> Poll<Result<Option<MessageReader>>> {
        let res = match Pin::new(&mut self.close_f).poll(cx) {
            Poll::Ready(Ok(reader)) => {
                self.status = Some(RpcStatus::ok());
                Poll::Ready(Ok(reader))
            }
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(Error::RpcFailure(status))) => {
                self.status = Some(status.clone());
                Poll::Ready(Err(Error::RpcFailure(status)))
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

        task::check_alive(&self.close_f)
    }
}

/// A helper trait that allows executing function on the internal `ShareCall` struct.
trait ShareCallHolder {
    fn call<R, F: FnOnce(&mut ShareCall) -> R>(&mut self, f: F) -> R;
}

impl ShareCallHolder for ShareCall {
    fn call<R, F: FnOnce(&mut ShareCall) -> R>(&mut self, f: F) -> R {
        f(self)
    }
}

impl ShareCallHolder for Arc<Mutex<ShareCall>> {
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
        cx: &mut Context,
        call: &mut C,
        skip_finish_check: bool,
    ) -> Poll<Option<Result<MessageReader>>> {
        if !skip_finish_check {
            let mut finished = false;
            if let Some(close_f) = &mut self.close_f {
                if Pin::new(close_f).poll(cx)?.is_ready() {
                    // Don't return immediately, there may be pending data.
                    finished = true;
                }
            }
            if finished {
                self.close_f.take();
            }
        }

        let mut bytes = None;
        if !self.read_done {
            if let Some(msg_f) = &mut self.msg_f {
                bytes = ready!(Pin::new(msg_f).poll(cx)?);
                if bytes.is_none() {
                    self.read_done = true;
                }
            }
        }

        if self.read_done {
            if self.close_f.is_none() {
                return Poll::Ready(None);
            }
            return Poll::Pending;
        }

        // so msg_f must be either stale or not initialized yet.
        self.msg_f.take();
        let msg_f = call.call(|c| c.call.start_recv_message())?;
        self.msg_f = Some(msg_f);
        if bytes.is_none() {
            self.poll(cx, call, true)
        } else {
            Poll::Ready(bytes.map(Ok))
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
    // Batch job to be executed in `poll_ready`.
    batch_f: Option<BatchFuture>,
    send_metadata: bool,
    // Flag to indicate if enhance batch strategy. This behavior will modify the `buffer_hint` to batch
    // messages as much as possible.
    enhance_buffer_strategy: bool,
    // Buffer used to store the data to be sent, send out the last data in this round of `start_send`.
    buffer: GrpcSlice,
    // Write flags used to control the data to be sent in `buffer`.
    buf_flags: Option<WriteFlags>,
    // Used to records whether a message in which `buffer_hint` is false exists.
    // Note: only used in enhanced buffer strategy.
    last_buf_hint: bool,
}

impl SinkBase {
    fn new(send_metadata: bool) -> SinkBase {
        SinkBase {
            batch_f: None,
            buffer: GrpcSlice::default(),
            buf_flags: None,
            last_buf_hint: true,
            send_metadata,
            enhance_buffer_strategy: false,
        }
    }

    fn start_send<T, C: ShareCallHolder>(
        &mut self,
        call: &mut C,
        t: &T,
        flags: WriteFlags,
        ser: SerializeFn<T>,
    ) -> Result<()> {
        // temporary fix: buffer hint with send meta will not send out any metadata.
        // note: only the first message can enter this code block.
        if self.send_metadata {
            ser(t, &mut self.buffer)?;
            self.buf_flags = Some(flags);
            self.start_send_buffer_message(false, call)?;
            self.send_metadata = false;
            return Ok(());
        }

        // If there is already a buffered message waiting to be sent, set `buffer_hint` to true to indicate
        // that this is not the last message.
        if self.buf_flags.is_some() {
            self.start_send_buffer_message(true, call)?;
        }

        ser(t, &mut self.buffer)?;
        let hint = flags.get_buffer_hint();
        self.last_buf_hint &= hint;
        self.buf_flags = Some(flags);

        // If sink disable batch, start sending the message in buffer immediately.
        if !self.enhance_buffer_strategy {
            self.start_send_buffer_message(hint, call)?;
        }

        Ok(())
    }

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        match &mut self.batch_f {
            None => return Poll::Ready(Ok(())),
            Some(f) => {
                ready!(Pin::new(f).poll(cx)?);
            }
        }
        self.batch_f.take();
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn poll_flush<C: ShareCallHolder>(
        &mut self,
        cx: &mut Context,
        call: &mut C,
    ) -> Poll<Result<()>> {
        if self.batch_f.is_some() {
            ready!(self.poll_ready(cx)?);
        }
        if self.buf_flags.is_some() {
            self.start_send_buffer_message(self.last_buf_hint, call)?;
            ready!(self.poll_ready(cx)?);
        }
        self.last_buf_hint = true;
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn start_send_buffer_message<C: ShareCallHolder>(
        &mut self,
        buffer_hint: bool,
        call: &mut C,
    ) -> Result<()> {
        // `start_send` is supposed to be called after `poll_ready` returns ready.
        assert!(self.batch_f.is_none());

        let mut flags = self.buf_flags.unwrap();
        flags = flags.buffer_hint(buffer_hint);
        let write_f = call.call(|c| {
            c.call
                .start_send_message(&mut self.buffer, flags.flags, self.send_metadata)
        })?;
        self.batch_f = Some(write_f);
        if !self.buffer.is_inline() {
            self.buffer = GrpcSlice::default();
        }
        self.buf_flags.take();
        Ok(())
    }
}
