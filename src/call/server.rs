// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CStr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::{result, slice};

use crate::grpc_sys::{
    self, gpr_clock_type, gpr_timespec, grpc_call_error, grpcwrap_request_call_context,
};
use futures::future::Future;
use futures::ready;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::task::{Context, Poll};
use parking_lot::Mutex;

use super::{RpcStatus, ShareCall, ShareCallHolder, WriteFlags};
use crate::auth_context::AuthContext;
use crate::buf::GrpcSlice;
use crate::call::{
    BatchContext, Call, MessageReader, MethodType, RpcStatusCode, SinkBase, StreamingBase,
};
use crate::codec::{DeserializeFn, SerializeFn};
use crate::cq::CompletionQueue;
use crate::error::{Error, Result};
use crate::metadata::Metadata;
use crate::server::ServerChecker;
use crate::server::{BoxHandler, RequestCallContext};
use crate::task::{BatchFuture, CallTag, Executor, Kicker};
use crate::CheckResult;

/// A time point that an rpc or operation should finished before it.
#[derive(Clone, Copy)]
pub struct Deadline {
    pub(crate) spec: gpr_timespec,
}

impl Deadline {
    fn new(spec: gpr_timespec) -> Deadline {
        let realtime_spec =
            unsafe { grpc_sys::gpr_convert_clock_type(spec, gpr_clock_type::GPR_CLOCK_REALTIME) };

        Deadline {
            spec: realtime_spec,
        }
    }

    /// Checks if the deadline is exceeded.
    pub fn exceeded(self) -> bool {
        unsafe {
            let now = grpc_sys::gpr_now(gpr_clock_type::GPR_CLOCK_REALTIME);
            grpc_sys::gpr_time_cmp(now, self.spec) >= 0
        }
    }

    pub(crate) fn spec(self) -> gpr_timespec {
        self.spec
    }
}

impl From<Duration> for Deadline {
    /// Build a deadline from given duration.
    ///
    /// The deadline will be `now + duration`.
    #[inline]
    fn from(dur: Duration) -> Deadline {
        Deadline::new(dur.into())
    }
}

/// Context for accepting a request.
pub struct RequestContext {
    ctx: *mut grpcwrap_request_call_context,
    request_call: Option<RequestCallContext>,
}

impl RequestContext {
    pub fn new(rc: RequestCallContext) -> RequestContext {
        let ctx = unsafe { grpc_sys::grpcwrap_request_call_context_create() };

        RequestContext {
            ctx,
            request_call: Some(rc),
        }
    }

    /// Try to accept a client side streaming request.
    ///
    /// Return error if the request is a client side unary request.
    pub fn handle_stream_req(
        self,
        cq: &CompletionQueue,
        rc: &mut RequestCallContext,
    ) -> result::Result<(), Self> {
        let checker = rc.get_checker();
        let handler = unsafe { rc.get_handler(self.method()) };
        match handler {
            Some(handler) => match handler.method_type() {
                MethodType::Unary | MethodType::ServerStreaming => Err(self),
                _ => {
                    execute(self, cq, None, handler, checker);
                    Ok(())
                }
            },
            None => {
                execute_unimplemented(self, cq.clone());
                Ok(())
            }
        }
    }

    /// Accept a client side unary request.
    ///
    /// This method should be called after `handle_stream_req`. When handling
    /// client side unary request, handler will only be called after the unary
    /// request is received.
    pub fn handle_unary_req(self, rc: RequestCallContext, _: &CompletionQueue) {
        // fetch message before calling callback.
        let tag = Box::new(CallTag::unary_request(self, rc));
        let batch_ctx = tag.batch_ctx().unwrap().as_ptr();
        let request_ctx = tag.request_ctx().unwrap().as_ptr();
        let tag_ptr = Box::into_raw(tag);
        unsafe {
            let call = grpc_sys::grpcwrap_request_call_context_get_call(request_ctx);
            let code = grpc_sys::grpcwrap_call_recv_message(call, batch_ctx, tag_ptr as _);
            if code != grpc_call_error::GRPC_CALL_OK {
                Box::from_raw(tag_ptr);
                // it should not failed.
                panic!("try to receive message fail: {:?}", code);
            }
        }
    }

    pub fn take_request_call_context(&mut self) -> Option<RequestCallContext> {
        self.request_call.take()
    }

    pub fn as_ptr(&self) -> *mut grpcwrap_request_call_context {
        self.ctx
    }

    fn call(&self, cq: CompletionQueue) -> Call {
        unsafe {
            // It is okay to use a mutable pointer on a immutable reference, `self`,
            // because grpcwrap_request_call_context_ref_call is thread-safe.
            let call = grpc_sys::grpcwrap_request_call_context_ref_call(self.ctx);
            assert!(!call.is_null());
            Call::from_raw(call, cq)
        }
    }

    pub fn method(&self) -> &[u8] {
        let mut len = 0;
        let method = unsafe { grpc_sys::grpcwrap_request_call_context_method(self.ctx, &mut len) };

        unsafe { slice::from_raw_parts(method as _, len) }
    }

    fn host(&self) -> &[u8] {
        let mut len = 0;
        let host = unsafe { grpc_sys::grpcwrap_request_call_context_host(self.ctx, &mut len) };

        unsafe { slice::from_raw_parts(host as _, len) }
    }

    fn deadline(&self) -> Deadline {
        let t = unsafe { grpc_sys::grpcwrap_request_call_context_deadline(self.ctx) };

        Deadline::new(t)
    }

    fn metadata(&self) -> &Metadata {
        unsafe {
            let ptr = grpc_sys::grpcwrap_request_call_context_metadata_array(self.ctx);
            let arr_ptr: *const Metadata = ptr as _;
            &*arr_ptr
        }
    }

    fn peer(&self) -> String {
        unsafe {
            // RequestContext always holds a reference of the call.
            let call = grpc_sys::grpcwrap_request_call_context_get_call(self.ctx);
            let p = grpc_sys::grpc_call_get_peer(call);
            let peer = CStr::from_ptr(p)
                .to_str()
                .expect("valid UTF-8 data")
                .to_owned();
            grpc_sys::gpr_free(p as _);
            peer
        }
    }

    /// If the server binds in non-secure mode, this will return None
    fn auth_context(&self) -> Option<AuthContext> {
        unsafe {
            let call = grpc_sys::grpcwrap_request_call_context_get_call(self.ctx);
            AuthContext::from_call_ptr(call)
        }
    }
}

impl Drop for RequestContext {
    fn drop(&mut self) {
        unsafe { grpc_sys::grpcwrap_request_call_context_destroy(self.ctx) }
    }
}

/// A context for handling client side unary request.
pub struct UnaryRequestContext {
    request: RequestContext,
    request_call: Option<RequestCallContext>,
    batch: BatchContext,
}

impl UnaryRequestContext {
    pub fn new(ctx: RequestContext, rc: RequestCallContext) -> UnaryRequestContext {
        UnaryRequestContext {
            request: ctx,
            request_call: Some(rc),
            batch: BatchContext::new(),
        }
    }

    pub fn batch_ctx(&self) -> &BatchContext {
        &self.batch
    }

    pub fn batch_ctx_mut(&mut self) -> &mut BatchContext {
        &mut self.batch
    }

    pub fn request_ctx(&self) -> &RequestContext {
        &self.request
    }

    pub fn take_request_call_context(&mut self) -> Option<RequestCallContext> {
        self.request_call.take()
    }

    pub fn handle(
        self,
        rc: &mut RequestCallContext,
        cq: &CompletionQueue,
        reader: Option<MessageReader>,
    ) {
        let checker = rc.get_checker();
        let handler = unsafe { rc.get_handler(self.request.method()).unwrap() };
        if reader.is_some() {
            return execute(self.request, cq, reader, handler, checker);
        }

        let status = RpcStatus::with_message(RpcStatusCode::INTERNAL, "No payload".to_owned());
        self.request.call(cq.clone()).abort(&status)
    }
}

/// A stream for client a streaming call and a duplex streaming call.
///
/// The corresponding RPC will be canceled if the stream did not
/// finish before dropping.
#[must_use = "if unused the RequestStream may immediately cancel the RPC"]
pub struct RequestStream<T> {
    call: Arc<Mutex<ShareCall>>,
    base: StreamingBase,
    de: DeserializeFn<T>,
}

impl<T> RequestStream<T> {
    fn new(call: Arc<Mutex<ShareCall>>, de: DeserializeFn<T>) -> RequestStream<T> {
        RequestStream {
            call,
            base: StreamingBase::new(None),
            de,
        }
    }
}

impl<T> Stream for RequestStream<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<T>>> {
        {
            let mut call = self.call.lock();
            call.check_alive()?;
        }

        let t = &mut *self;
        match ready!(t.base.poll(cx, &mut t.call, false)?) {
            None => Poll::Ready(None),
            Some(data) => Poll::Ready(Some((t.de)(data))),
        }
    }
}

impl<T> Drop for RequestStream<T> {
    /// The corresponding RPC will be canceled if the stream did not
    /// finish before dropping.
    fn drop(&mut self) {
        self.base.on_drop(&mut self.call);
    }
}

/// A helper macro used to implement server side unary sink.
/// Not using generic here because we don't need to expose
/// `CallHolder` or `Call` to caller.
// TODO: Use type alias to be friendly for documentation.
macro_rules! impl_unary_sink {
    ($(#[$attr:meta])* $t:ident, $rt:ident, $holder:ty) => {
        pub struct $rt {
            call: $holder,
            cq_f: Option<BatchFuture>,
            err: Option<Error>,
        }

        impl Future for $rt {
            type Output = Result<()>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
                if let Some(e) = self.err.take() {
                    return Poll::Ready(Err(e));
                }

                if self.cq_f.is_some() {
                    ready!(Pin::new(self.cq_f.as_mut().unwrap()).poll(cx)?);
                    self.cq_f.take();
                }

                ready!(self.call.call(|c| c.poll_finish(cx))?);
                Poll::Ready(Ok(()))
            }
        }

        $(#[$attr])*
        pub struct $t<T> {
            call: Option<$holder>,
            write_flags: u32,
            ser: SerializeFn<T>,
        }

        impl<T> $t<T> {
            fn new(call: $holder, ser: SerializeFn<T>) -> $t<T> {
                $t {
                    call: Some(call),
                    write_flags: 0,
                    ser,
                }
            }

            pub fn success(self, t: T) -> $rt {
                self.complete(RpcStatus::ok(), Some(t))
            }

            pub fn fail(self, status: RpcStatus) -> $rt {
                self.complete(status, None)
            }

            fn complete(mut self, status: RpcStatus, t: Option<T>) -> $rt {
                let mut data = match t {
                    Some(t) => {
                        let mut buf = GrpcSlice::default();
                        if let Err(e) = (self.ser)(&t, &mut buf) {
                            return $rt {
                                call: self.call.take().unwrap(),
                                cq_f: None,
                                err: Some(e),
                            };
                        }
                        Some(buf)
                    }
                    None => None,
                };

                let write_flags = self.write_flags;
                let res = self.call.as_mut().unwrap().call(|c| {
                    c.call
                        .start_send_status_from_server(&status, true, &mut data, write_flags)
                });

                let (cq_f, err) = match res {
                    Ok(f) => (Some(f), None),
                    Err(e) => (None, Some(e)),
                };

                $rt {
                    call: self.call.take().unwrap(),
                    cq_f,
                    err,
                }
            }
        }

        impl<T> Drop for $t<T> {
            /// The corresponding RPC will be canceled if the sink did not
            /// send a response before dropping.
            fn drop(&mut self) {
                self.call
                    .as_mut()
                    .map(|call| call.call(|c| c.call.cancel()));
            }
        }
    };
}

impl_unary_sink!(
    /// A sink for unary call.
    ///
    /// To close the sink properly, you should call [`success`] or [`fail`] before dropping.
    ///
    /// [`success`]: #method.success
    /// [`fail`]: #method.fail
    #[must_use = "if unused the sink may immediately cancel the RPC"]
    UnarySink,
    UnarySinkResult,
    ShareCall
);
impl_unary_sink!(
    /// A sink for client streaming call.
    ///
    /// To close the sink properly, you should call [`success`] or [`fail`] before dropping.
    ///
    /// [`success`]: #method.success
    /// [`fail`]: #method.fail
    #[must_use = "if unused the sink may immediately cancel the RPC"]
    ClientStreamingSink,
    ClientStreamingSinkResult,
    Arc<Mutex<ShareCall>>
);

// A macro helper to implement server side streaming sink.
macro_rules! impl_stream_sink {
    ($(#[$attr:meta])* $t:ident, $ft:ident, $holder:ty) => {
        $(#[$attr])*
        pub struct $t<T> {
            call: Option<$holder>,
            base: SinkBase,
            flush_f: Option<BatchFuture>,
            status: RpcStatus,
            flushed: bool,
            closed: bool,
            ser: SerializeFn<T>,
        }

        impl<T> $t<T> {
            fn new(call: $holder, ser: SerializeFn<T>) -> $t<T> {
                $t {
                    call: Some(call),
                    base: SinkBase::new(true),
                    flush_f: None,
                    status: RpcStatus::ok(),
                    flushed: false,
                    closed: false,
                    ser,
                }
            }

            /// By default it always sends messages with their configured buffer hint. But when the
            /// `enhance_batch` is enabled, messages will be batched together as many as possible.
            /// The rules are listed as below:
            /// - All messages except the last one will be sent with `buffer_hint` set to true.
            /// - The last message will also be sent with `buffer_hint` set to true unless any message is
            ///    offered with buffer hint set to false.
            ///
            /// No matter `enhance_batch` is true or false, it's recommended to follow the contract of
            /// Sink and call `poll_flush` to ensure messages are handled by gRPC C Core.
            pub fn enhance_batch(&mut self, flag: bool) {
                self.base.enhance_buffer_strategy = flag;
            }

            pub fn set_status(&mut self, status: RpcStatus) {
                assert!(self.flush_f.is_none());
                self.status = status;
            }

            pub fn fail(mut self, status: RpcStatus) -> $ft {
                assert!(self.flush_f.is_none());
                let send_metadata = self.base.send_metadata;
                let res = self.call.as_mut().unwrap().call(|c| {
                    c.call
                        .start_send_status_from_server(&status, send_metadata, &mut None, 0)
                });

                let (fail_f, err) = match res {
                    Ok(f) => (Some(f), None),
                    Err(e) => (None, Some(e)),
                };

                $ft {
                    call: self.call.take().unwrap(),
                    fail_f,
                    err,
                }
            }
        }

        impl<T> Drop for $t<T> {
            /// The corresponding RPC will be canceled if the sink did not call
            /// [`close`] or [`fail`] before dropping.
            ///
            /// [`close`]: #method.close
            /// [`fail`]: #method.fail
            fn drop(&mut self) {
                // We did not close it explicitly and it was not dropped in the `fail`.
                if !self.closed && self.call.is_some() {
                    let mut call = self.call.take().unwrap();
                    call.call(|c| c.call.cancel());
                }
            }
        }

        impl<T> Sink<(T, WriteFlags)> for $t<T> {
            type Error = Error;

            #[inline]
            fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
                if let Poll::Ready(_) = self.call.as_mut().unwrap().call(|c| c.poll_finish(cx))? {
                    return Poll::Ready(Err(Error::RemoteStopped));
                }
                Pin::new(&mut self.base).poll_ready(cx)
            }

            #[inline]
            fn start_send(mut self: Pin<&mut Self>, (msg, flags): (T, WriteFlags)) -> Result<()> {
                let t = &mut *self;
                t.base.start_send(t.call.as_mut().unwrap(), &msg, flags, t.ser)
            }

            #[inline]
            fn poll_flush(mut self: Pin<&mut Self>,  cx: &mut Context) -> Poll<Result<()>> {
                if let Poll::Ready(_) = self.call.as_mut().unwrap().call(|c| c.poll_finish(cx))? {
                    return Poll::Ready(Err(Error::RemoteStopped));
                }
                let t = &mut *self;
                Pin::new(&mut t.base).poll_flush(cx, t.call.as_mut().unwrap())
            }

            fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
                if self.flush_f.is_none() {
                    ready!(Pin::new(&mut self.base).poll_ready(cx)?);

                    let send_metadata = self.base.send_metadata;
                    let t = &mut *self;
                    let status = &t.status;
                    let flush_f = t.call.as_mut().unwrap().call(|c| {
                        c.call
                            .start_send_status_from_server(status, send_metadata, &mut None, 0)
                    })?;
                    t.flush_f = Some(flush_f);
                }

                if !self.flushed {
                    ready!(Pin::new(self.flush_f.as_mut().unwrap()).poll(cx)?);
                    self.flushed = true;
                }

                ready!(self.call.as_mut().unwrap().call(|c| c.poll_finish(cx))?);
                self.closed = true;
                Poll::Ready(Ok(()))
            }
        }

        #[must_use = "if unused the sink failure may immediately cancel the RPC"]
        pub struct $ft {
            call: $holder,
            fail_f: Option<BatchFuture>,
            err: Option<Error>,
        }

        impl Future for $ft {
            type Output = Result<()>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
                if let Some(e) = self.err.take() {
                    return Poll::Ready(Err(e));
                }

                let readiness = self.call.call(|c| {
                    if c.finished {
                        return Poll::Ready(Ok(()));
                    }

                    c.poll_finish(cx).map(|r| r.map(|_| ()))
                })?;

                if let Some(ref mut f) = self.fail_f {
                    ready!(Pin::new(f).poll(cx)?);
                }

                self.fail_f.take();
                readiness.map(Ok)
            }
        }
    };
}

impl_stream_sink!(
    /// A sink for server streaming call.
    ///
    /// To close the sink properly, you should call [`close`] or [`fail`] before dropping.
    ///
    /// [`close`]: #method.close
    /// [`fail`]: #method.fail
    #[must_use = "if unused the sink may immediately cancel the RPC"]
    ServerStreamingSink,
    ServerStreamingSinkFailure,
    ShareCall
);
impl_stream_sink!(
    /// A sink for duplex streaming call.
    ///
    /// To close the sink properly, you should call [`close`] or [`fail`] before dropping.
    ///
    /// [`close`]: #method.close
    /// [`fail`]: #method.fail
    #[must_use = "if unused the sink may immediately cancel the RPC"]
    DuplexSink,
    DuplexSinkFailure,
    Arc<Mutex<ShareCall>>
);

/// A context for rpc handling.
pub struct RpcContext<'a> {
    ctx: RequestContext,
    executor: Executor<'a>,
    deadline: Deadline,
}

impl<'a> RpcContext<'a> {
    fn new(ctx: RequestContext, cq: &CompletionQueue) -> RpcContext<'_> {
        RpcContext {
            deadline: ctx.deadline(),
            ctx,
            executor: Executor::new(cq),
        }
    }

    fn kicker(&self) -> Kicker {
        let call = self.call();
        Kicker::from_call(call)
    }

    pub(crate) fn call(&self) -> Call {
        self.ctx.call(self.executor.cq().clone())
    }

    pub fn method(&self) -> &[u8] {
        self.ctx.method()
    }

    pub fn host(&self) -> &[u8] {
        self.ctx.host()
    }

    pub fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Get the initial metadata sent by client.
    pub fn request_headers(&self) -> &Metadata {
        self.ctx.metadata()
    }

    pub fn peer(&self) -> String {
        self.ctx.peer()
    }

    /// Wrapper around the gRPC Core AuthContext
    ///
    /// If the server binds in non-secure mode, this will return None
    pub fn auth_context(&self) -> Option<AuthContext> {
        self.ctx.auth_context()
    }

    /// Spawn the future into current gRPC poll thread.
    ///
    /// This can reduce a lot of context switching, but please make
    /// sure there is no heavy work in the future.
    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.executor.spawn(f, self.kicker())
    }
}

// Following four helper functions are used to create a callback closure.

macro_rules! accept_call {
    ($call:expr) => {
        match $call.start_server_side() {
            Err(Error::QueueShutdown) => return,
            Err(e) => panic!("unexpected error when trying to accept request: {:?}", e),
            Ok(f) => f,
        }
    };
}

// Helper function to call a unary handler.
pub fn execute_unary<P, Q, F>(
    ctx: RpcContext<'_>,
    ser: SerializeFn<Q>,
    de: DeserializeFn<P>,
    payload: MessageReader,
    f: &mut F,
) where
    F: FnMut(RpcContext<'_>, P, UnarySink<Q>),
{
    let mut call = ctx.call();
    let close_f = accept_call!(call);
    let request = match de(payload) {
        Ok(f) => f,
        Err(e) => {
            let status = RpcStatus::with_message(
                RpcStatusCode::INTERNAL,
                format!("Failed to deserialize response message: {:?}", e),
            );
            call.abort(&status);
            return;
        }
    };
    let sink = UnarySink::new(ShareCall::new(call, close_f), ser);
    f(ctx, request, sink)
}

// Helper function to call client streaming handler.
pub fn execute_client_streaming<P, Q, F>(
    ctx: RpcContext<'_>,
    ser: SerializeFn<Q>,
    de: DeserializeFn<P>,
    f: &mut F,
) where
    F: FnMut(RpcContext<'_>, RequestStream<P>, ClientStreamingSink<Q>),
{
    let mut call = ctx.call();
    let close_f = accept_call!(call);
    let call = Arc::new(Mutex::new(ShareCall::new(call, close_f)));

    let req_s = RequestStream::new(call.clone(), de);
    let sink = ClientStreamingSink::new(call, ser);
    f(ctx, req_s, sink)
}

// Helper function to call server streaming handler.
pub fn execute_server_streaming<P, Q, F>(
    ctx: RpcContext<'_>,
    ser: SerializeFn<Q>,
    de: DeserializeFn<P>,
    payload: MessageReader,
    f: &mut F,
) where
    F: FnMut(RpcContext<'_>, P, ServerStreamingSink<Q>),
{
    let mut call = ctx.call();
    let close_f = accept_call!(call);

    let request = match de(payload) {
        Ok(t) => t,
        Err(e) => {
            let status = RpcStatus::with_message(
                RpcStatusCode::INTERNAL,
                format!("Failed to deserialize response message: {:?}", e),
            );
            call.abort(&status);
            return;
        }
    };

    let sink = ServerStreamingSink::new(ShareCall::new(call, close_f), ser);
    f(ctx, request, sink)
}

// Helper function to call duplex streaming handler.
pub fn execute_duplex_streaming<P, Q, F>(
    ctx: RpcContext<'_>,
    ser: SerializeFn<Q>,
    de: DeserializeFn<P>,
    f: &mut F,
) where
    F: FnMut(RpcContext<'_>, RequestStream<P>, DuplexSink<Q>),
{
    let mut call = ctx.call();
    let close_f = accept_call!(call);
    let call = Arc::new(Mutex::new(ShareCall::new(call, close_f)));

    let req_s = RequestStream::new(call.clone(), de);
    let sink = DuplexSink::new(call, ser);
    f(ctx, req_s, sink)
}

// A helper function used to handle all undefined rpc calls.
pub fn execute_unimplemented(ctx: RequestContext, cq: CompletionQueue) {
    // Suppress needless-pass-by-value.
    let ctx = ctx;
    let mut call = ctx.call(cq);
    accept_call!(call);
    call.abort(&RpcStatus::new(RpcStatusCode::UNIMPLEMENTED))
}

// Helper function to call handler.
//
// Invoked after a request is ready to be handled.
fn execute(
    ctx: RequestContext,
    cq: &CompletionQueue,
    payload: Option<MessageReader>,
    f: &mut BoxHandler,
    mut checkers: Vec<Box<dyn ServerChecker>>,
) {
    let rpc_ctx = RpcContext::new(ctx, cq);

    for handler in checkers.iter_mut() {
        match handler.check(&rpc_ctx) {
            CheckResult::Continue => {}
            CheckResult::Abort(status) => {
                rpc_ctx.call().abort(&status);
                return;
            }
        }
    }

    f.handle(rpc_ctx, payload)
}
