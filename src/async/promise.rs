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


use std::sync::Arc;

use grpc_sys::GrpcStatusCode;

use call::BatchContext;
use error::Error;
use super::{BatchMessage, Inner};

/// Batch job type.
#[derive(PartialEq, Debug)]
pub enum BatchType {
    /// Finish without reading any message.
    Finish,
    /// Extract one message when finish.
    Read,
    /// Check the rpc code and then extract one message.
    CheckRead,
}

/// A promise used to resolve batch jobs.
pub struct Batch {
    ty: BatchType,
    ctx: BatchContext,
    inner: Arc<Inner<BatchMessage>>,
}

impl Batch {
    pub fn new(ty: BatchType, inner: Arc<Inner<BatchMessage>>) -> Batch {
        Batch {
            ty: ty,
            ctx: BatchContext::new(),
            inner: inner,
        }
    }

    pub fn context(&self) -> &BatchContext {
        &self.ctx
    }

    fn read_one_msg(&mut self) {
        let mut guard = self.inner.lock();
        guard.set_result(Ok(self.ctx.recv_message()));
    }

    fn finish_response(&mut self, succeed: bool) {
        let mut guard = self.inner.lock();
        if !succeed {
            guard.set_result(Err(Error::RemoteStopped));
            return;
        }
        let status = self.ctx.rpc_status();
        if status.status != GrpcStatusCode::Ok {
            guard.set_result(Err(Error::RpcFailure(status)));
            return;
        }

        guard.set_result(Ok(None))
    }

    fn handle_unary_response(&mut self) {
        let mut guard = self.inner.lock();
        let status = self.ctx.rpc_status();
        if status.status != GrpcStatusCode::Ok {
            guard.set_result(Err(Error::RpcFailure(status)));
            return;
        }

        guard.set_result(Ok(self.ctx.recv_message()))
    }

    pub fn resolve(mut self, success: bool) {
        match self.ty {
            BatchType::CheckRead => {
                assert!(success);
                self.handle_unary_response();
            }
            BatchType::Finish => {
                self.finish_response(success);
            }
            BatchType::Read => {
                assert!(success);
                self.read_one_msg();
            }
        }
    }
}

/// A promise used to resolve async shutdown result.
pub struct Shutdown {
    inner: Arc<Inner<()>>,
}

impl Shutdown {
    pub fn new(inner: Arc<Inner<()>>) -> Shutdown {
        Shutdown { inner: inner }
    }

    pub fn resolve(self, success: bool) {
        let mut guard = self.inner.lock();
        if success {
            guard.set_result(Ok(()))
        } else {
            guard.set_result(Err(Error::ShutdownFailed))
        }
    }
}
