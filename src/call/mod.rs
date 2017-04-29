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

use async::{BatchFuture, BatchType, Promise};
use error::{Error, Result};
use futures::{Async, Future, Poll};

use grpc_sys::{self, GrpcBatchContext, GrpcCall, GrpcCallStatus, GrpcStatusCode};
use libc::c_void;
use std::{ptr, result, slice, usize};

#[derive(Clone, Copy)]
pub enum MethodType {
    Unary,
    ClientStreaming,
    ServerStreaming,
    Dulex,
}

pub struct Method {
    pub ty: MethodType,
    pub name: &'static str,
}

impl Method {
    pub fn new(ty: MethodType, name: &'static str) -> Method {
        Method {
            ty: ty,
            name: name,
        }
    }
}

/// Status return from server.
#[derive(Debug)]
pub struct RpcStatus {
    pub status: GrpcStatusCode,
    pub details: Option<String>,
}

impl RpcStatus {
    pub fn new(status: GrpcStatusCode, details: Option<String>) -> RpcStatus {
        RpcStatus {
            status: status,
            details: details,
        }
    }

    /// Generate an Ok status.
    pub fn ok() -> RpcStatus {
        RpcStatus::new(GrpcStatusCode::Ok, None)
    }
}

pub struct BatchContext {
    ctx: *mut GrpcBatchContext,
}

impl BatchContext {
    pub fn new() -> BatchContext {
        BatchContext { ctx: unsafe { grpc_sys::grpcwrap_batch_context_create() } }
    }

    pub fn as_ptr(&self) -> *mut GrpcBatchContext {
        self.ctx
    }

    pub fn rpc_status(&self) -> RpcStatus {
        let status =
            unsafe { grpc_sys::grpcwrap_batch_context_recv_status_on_client_status(self.ctx) };
        let details = if status == GrpcStatusCode::Ok {
            None
        } else {
            unsafe {
                let mut details_len = 0;
                let details_ptr = grpc_sys::grpcwrap_batch_context_recv_status_on_client_details(
                    self.ctx, &mut details_len);
                let details_slice = slice::from_raw_parts(details_ptr as *const _, details_len);
                Some(String::from_utf8_lossy(details_slice).into_owned())
            }
        };

        RpcStatus {
            status: status,
            details: details,
        }
    }

    // TODO: return &[u8] instead.
    pub fn recv_message(&self) -> Vec<u8> {
        // TODO: avoid copy
        let len = unsafe { grpc_sys::grpcwrap_batch_context_recv_message_length(self.ctx) };
        if len == usize::MAX {
            return Vec::new();
        }
        let mut buffer = Vec::with_capacity(len);
        unsafe {
            grpc_sys::grpcwrap_batch_context_recv_message_to_buffer(self.ctx,
                                                                    buffer.as_mut_ptr() as *mut _,
                                                                    len);
            buffer.set_len(len);
        }
        buffer
    }
}

impl Drop for BatchContext {
    fn drop(&mut self) {
        unsafe { grpc_sys::grpcwrap_batch_context_destroy(self.ctx) }
    }
}

fn check_run<F>(bt: BatchType, f: F) -> Result<BatchFuture>
    where F: FnOnce(*mut GrpcBatchContext, *mut c_void) -> GrpcCallStatus
{
    let (cq_f, prom) = Promise::batch_pair(bt);
    let prom_box = Box::new(prom);
    let batch_ptr = prom_box.batch_ctx().unwrap().as_ptr();
    let prom_ptr = Box::into_raw(prom_box);
    let code = f(batch_ptr, prom_ptr as *mut c_void);
    if code != GrpcCallStatus::Ok {
        unsafe {
            Box::from_raw(prom_ptr);
        }
        return Err(Error::CallFailure(code));
    }
    Ok(cq_f)
}

pub struct Call {
    call: *mut GrpcCall,
}

unsafe impl Send for Call {}

impl Call {
    pub unsafe fn from_raw(call: *mut grpc_sys::GrpcCall) -> Call {
        assert!(!call.is_null());
        Call { call: call }
    }

    pub fn start_send_message(&mut self,
                              msg: &[u8],
                              write_flags: u32,
                              initial_meta: bool)
                              -> Result<BatchFuture> {
        let i = if initial_meta { 1 } else { 0 };
        check_run(BatchType::Finish, |ctx, tag| unsafe {
            grpc_sys::grpcwrap_call_send_message(self.call,
                                                 ctx,
                                                 msg.as_ptr() as _,
                                                 msg.len(),
                                                 write_flags,
                                                 i,
                                                 tag)
        })
    }

    pub fn start_send_close_client(&mut self) -> Result<BatchFuture> {
        check_run(BatchType::Finish, |ctx, tag| unsafe {
            grpc_sys::grpcwrap_call_send_close_from_client(self.call, ctx, tag)
        })
    }

    pub fn start_recv_message(&mut self) -> Result<BatchFuture> {
        check_run(BatchType::ReadOne,
                  |ctx, tag| unsafe { grpc_sys::grpcwrap_call_recv_message(self.call, ctx, tag) })
    }

    pub fn start_server_side(&mut self) -> Result<BatchFuture> {
        check_run(BatchType::Finish, |ctx, tag| unsafe {
            grpc_sys::grpcwrap_call_start_serverside(self.call, ctx, tag)
        })
    }

    pub fn start_send_status_from_server(&mut self,
                                         status: &RpcStatus,
                                         send_empty_metadata: bool,
                                         payload: Option<Vec<u8>>,
                                         write_flags: u32)
                                         -> Result<BatchFuture> {
        let send_empty_metadata = if send_empty_metadata { 1 } else { 0 };
        let (payload_ptr, payload_len) = payload.as_ref()
            .map_or((ptr::null(), 0), |b| (b.as_ptr(), b.len()));
        check_run(BatchType::Finish, |ctx, tag| unsafe {
            let details_ptr = status.details.as_ref().map_or_else(ptr::null, |s| s.as_ptr() as _);
            let details_len = status.details.as_ref().map_or(0, String::len);
            grpc_sys::grpcwrap_call_send_status_from_server(self.call,
                                                            ctx,
                                                            status.status,
                                                            details_ptr,
                                                            details_len,
                                                            ptr::null_mut(),
                                                            send_empty_metadata,
                                                            payload_ptr as _,
                                                            payload_len,
                                                            write_flags,
                                                            tag)
        })
    }

    fn cancel(&self) {
        unsafe { grpc_sys::grpc_call_cancel(self.call, ptr::null_mut()) }
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        unsafe { grpc_sys::grpc_call_destroy(self.call) }
    }
}

struct StreamingBase {
    close_f: Option<BatchFuture>,
    msg_f: Option<BatchFuture>,
    stale: bool,
}

impl StreamingBase {
    fn new(close_f: Option<BatchFuture>) -> StreamingBase {
        StreamingBase {
            close_f: close_f,
            msg_f: None,
            stale: false,
        }
    }

    fn poll(&mut self, call: &mut Call, skip_finish_check: bool) -> Poll<Option<Vec<u8>>, Error> {
        if self.stale {
            return Err(Error::FutureStale);
        }
        let mut repoll_resp = self.msg_f.is_none();
        if let Some(ref mut msg_f) = self.msg_f {
            match msg_f.poll() {
                // maybe we can schedule next poll immediately?
                Ok(Async::Ready(bytes)) => {
                    if bytes.is_empty() {
                        self.stale = true;
                        return Ok(Async::Ready(None));
                    }

                    return Ok(Async::Ready(Some(bytes)));
                }
                Err(Error::FutureStale) => repoll_resp = true,
                Err(e) => return Err(e),
                Ok(Async::NotReady) => {
                    if skip_finish_check {
                        return Ok(Async::NotReady);
                    }
                }
            }
        }

        if let Some(ref mut close_f) = self.close_f {
            match close_f.poll() {
                Ok(Async::Ready(_)) => {
                    self.stale = true;
                    return Ok(Async::Ready(None));
                }
                Err(e) => {
                    self.stale = true;
                    return Err(e);
                }
                Ok(Async::NotReady) => {}
            }
        }

        if !repoll_resp {
            return Ok(Async::NotReady);
        }

        // so msg_f must be either stale or not initialised yet.
        self.msg_f.take();
        match call.start_recv_message() {
            Err(e) => Err(e),
            Ok(msg_f) => {
                self.msg_f = Some(msg_f);
                self.poll(call, true)
            }
        }
    }
}

struct SinkBase {
    write_f: Option<BatchFuture>,
    close_f: Option<BatchFuture>,
    buf: Vec<u8>,
    flags: u32,
    send_metadata: bool,
}

impl SinkBase {
    fn new(flags: u32, send_metadata: bool) -> SinkBase {
        SinkBase {
            write_f: None,
            close_f: None,
            buf: Vec::new(),
            send_metadata: send_metadata,
            flags: flags,
        }
    }

    fn start_send<F, E>(&mut self, call: &mut Call, fill_buf: F) -> Result<bool>
        where F: FnOnce(&mut Vec<u8>) -> result::Result<(), E>,
              E: Into<Error>
    {
        if self.write_f.is_some() {
            // try its best not to return false.
            try!(self.poll_complete());
            if self.write_f.is_some() {
                return Ok(false);
            }
        }

        self.buf.clear();
        if let Err(e) = fill_buf(&mut self.buf) {
            return Err(e.into());
        }
        let write_f = try!(call.start_send_message(&self.buf, self.flags, self.send_metadata));
        self.write_f = Some(write_f);
        self.send_metadata = false;
        Ok(true)
    }

    fn poll_complete(&mut self) -> Poll<(), Error> {
        if let Some(ref mut write_f) = self.write_f {
            try_ready!(write_f.poll());
        }

        self.write_f.take();
        Ok(Async::Ready(()))
    }

    fn close(&mut self, call: &mut Call) -> Poll<(), Error> {
        if self.close_f.is_none() {
            if let Async::NotReady = try!(self.poll_complete()) {
                return Ok(Async::NotReady);
            }

            let close_f = try!(call.start_send_close_client());
            self.close_f = Some(close_f);
        }

        self.close_f.as_mut().unwrap().poll().map(|res| res.map(|_| {}))
    }
}
