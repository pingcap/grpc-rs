// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use super::Inner;
use crate::call::{BatchContext, MessageReader, RpcStatusCode};
use crate::error::Error;
use crate::metadata::Metadata;

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
    inner: Arc<Inner<(Option<Metadata>, Option<MessageReader>, Option<Metadata>)>>,
}

impl Batch {
    pub fn new(
        ty: BatchType,
        inner: Arc<Inner<(Option<Metadata>, Option<MessageReader>, Option<Metadata>)>>,
    ) -> Batch {
        Batch {
            ty,
            ctx: BatchContext::new(),
            inner,
        }
    }

    pub fn context(&self) -> &BatchContext {
        &self.ctx
    }

    fn read_one_msg(&mut self, success: bool) {
        let task = {
            let mut guard = self.inner.lock();
            if success {
                guard.set_result(Ok((
                    self.ctx.recv_initial_metadata(),
                    self.ctx.recv_message(),
                    self.ctx.recv_trailing_metadata(),
                )))
            } else {
                // rely on C core to handle the failed read (e.g. deliver approriate
                // statusCode on the clientside).
                guard.set_result(Ok((None, None, None)))
            }
        };
        task.map(|t| t.wake());
    }

    fn finish_response(&mut self, succeed: bool) {
        let task = {
            let mut guard = self.inner.lock();
            if succeed {
                let status = self.ctx.rpc_status();
                if status.code() == RpcStatusCode::OK {
                    guard.set_result(Ok((
                        self.ctx.recv_initial_metadata(),
                        None,
                        self.ctx.recv_trailing_metadata(),
                    )))
                } else {
                    guard.set_result(Err(Error::RpcFailure(status)))
                }
            } else {
                guard.set_result(Err(Error::RemoteStopped))
            }
        };
        task.map(|t| t.wake());
    }

    fn handle_unary_response(&mut self) {
        let task = {
            let mut guard = self.inner.lock();
            let status = self.ctx.rpc_status();
            if status.code() == RpcStatusCode::OK {
                guard.set_result(Ok((
                    self.ctx.recv_initial_metadata(),
                    self.ctx.recv_message(),
                    self.ctx.recv_trailing_metadata(),
                )))
            } else {
                guard.set_result(Err(Error::RpcFailure(status)))
            }
        };
        task.map(|t| t.wake());
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
                self.read_one_msg(success);
            }
        }
    }
}

impl Debug for Batch {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Batch [{:?}]", self.ty)
    }
}

/// A promise used to resolve async action status.
///
/// The action can only succeed or fail without extra error hint.
pub struct Action {
    inner: Arc<Inner<bool>>,
}

impl Action {
    pub fn new(inner: Arc<Inner<bool>>) -> Action {
        Action { inner }
    }

    pub fn resolve(self, success: bool) {
        let task = {
            let mut guard = self.inner.lock();
            guard.set_result(Ok(success))
        };
        task.map(|t| t.wake());
    }
}
