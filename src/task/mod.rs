// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod callback;
mod executor;
mod promise;

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;

use futures::future::Future;
use futures::task::{Context, Poll, Waker};
use parking_lot::Mutex;

use self::callback::{Abort, Request as RequestCallback, UnaryRequest as UnaryRequestCallback};
use self::executor::SpawnTask;
use self::promise::{Action as ActionPromise, Batch as BatchPromise};
use crate::call::server::RequestContext;
use crate::call::{BatchContext, Call, MessageReader};
use crate::cq::CompletionQueue;
use crate::error::{Error, Result};
use crate::server::RequestCallContext;
use crate::Metadata;

pub(crate) use self::executor::{Executor, Kicker, UnfinishedWork};
pub use self::promise::BatchType;

/// A handle that is used to notify future that the task finishes.
pub struct NotifyHandle<T> {
    result: Option<Result<T>>,
    waker: Option<Waker>,
    stale: bool,
}

impl<T> NotifyHandle<T> {
    fn new() -> NotifyHandle<T> {
        NotifyHandle {
            result: None,
            waker: None,
            stale: false,
        }
    }

    /// Set the result and notify future if necessary.
    fn set_result(&mut self, res: Result<T>) -> Option<Waker> {
        self.result = Some(res);

        self.waker.take()
    }
}

type Inner<T> = Mutex<NotifyHandle<T>>;

fn new_inner<T>() -> Arc<Inner<T>> {
    Arc::new(Mutex::new(NotifyHandle::new()))
}

/// Get the future status without the need to poll.
///
/// If the future is polled successfully, this function will return None.
/// Not implemented as method as it's only for internal usage.
pub fn check_alive<T>(f: &CqFuture<T>) -> Result<()> {
    let guard = f.inner.lock();
    match guard.result {
        None => Ok(()),
        Some(Err(Error::RpcFailure(ref status))) => {
            Err(Error::RpcFinished(Some(status.to_owned())))
        }
        Some(Ok(_)) | Some(Err(_)) => Err(Error::RpcFinished(None)),
    }
}

/// A future object for task that is scheduled to `CompletionQueue`.
pub struct CqFuture<T> {
    inner: Arc<Inner<T>>,
}

impl<T> CqFuture<T> {
    fn new(inner: Arc<Inner<T>>) -> CqFuture<T> {
        CqFuture { inner }
    }
}

impl<T> Future for CqFuture<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut guard = self.inner.lock();
        if guard.stale {
            panic!("Resolved future is not supposed to be polled again.");
        }

        if let Some(res) = guard.result.take() {
            guard.stale = true;
            return Poll::Ready(res);
        }

        // So the task has not been finished yet, add notification hook.
        if guard.waker.is_none() || !guard.waker.as_ref().unwrap().will_wake(cx.waker()) {
            guard.waker = Some(cx.waker().clone());
        }

        Poll::Pending
    }
}

/// Future object for batch jobs.
pub type BatchFuture = CqFuture<(Option<Metadata>, Option<MessageReader>, Option<Metadata>)>;

/// A result holder for asynchronous execution.
// This enum is going to be passed to FFI, so don't use trait or generic here.
pub enum CallTag {
    Batch(BatchPromise),
    Request(RequestCallback),
    UnaryRequest(UnaryRequestCallback),
    Abort(Abort),
    Action(ActionPromise),
    Spawn(Arc<SpawnTask>),
}

impl CallTag {
    /// Generate a Future/CallTag pair for batch jobs.
    pub fn batch_pair(ty: BatchType) -> (BatchFuture, CallTag) {
        let inner = new_inner();
        let batch = BatchPromise::new(ty, inner.clone());
        (CqFuture::new(inner), CallTag::Batch(batch))
    }

    /// Generate a CallTag for request job. We don't have an eventloop
    /// to pull the future, so just the tag is enough.
    pub fn request(ctx: RequestCallContext) -> CallTag {
        CallTag::Request(RequestCallback::new(ctx))
    }

    /// Generate a Future/CallTag pair for action call that only cares if the result is
    /// successful.
    pub fn action_pair() -> (CqFuture<bool>, CallTag) {
        let inner = new_inner();
        let action = ActionPromise::new(inner.clone());
        (CqFuture::new(inner), CallTag::Action(action))
    }

    /// Generate a CallTag for abort call before handler is called.
    pub fn abort(call: Call) -> CallTag {
        CallTag::Abort(Abort::new(call))
    }

    /// Generate a CallTag for unary request job.
    pub fn unary_request(ctx: RequestContext, rc: RequestCallContext) -> CallTag {
        let cb = UnaryRequestCallback::new(ctx, rc);
        CallTag::UnaryRequest(cb)
    }

    /// Get the batch context from result holder.
    pub fn batch_ctx(&self) -> Option<&BatchContext> {
        match *self {
            CallTag::Batch(ref prom) => Some(prom.context()),
            CallTag::UnaryRequest(ref cb) => Some(cb.batch_ctx()),
            CallTag::Abort(ref cb) => Some(cb.batch_ctx()),
            _ => None,
        }
    }

    /// Get the request context from the result holder.
    pub fn request_ctx(&self) -> Option<&RequestContext> {
        match *self {
            CallTag::Request(ref prom) => Some(prom.context()),
            CallTag::UnaryRequest(ref cb) => Some(cb.request_ctx()),
            _ => None,
        }
    }

    /// Resolve the CallTag with given status.
    pub fn resolve(self, cq: &CompletionQueue, success: bool) {
        match self {
            CallTag::Batch(prom) => prom.resolve(success),
            CallTag::Request(cb) => cb.resolve(cq, success),
            CallTag::UnaryRequest(cb) => cb.resolve(cq, success),
            CallTag::Abort(_) => {}
            CallTag::Action(prom) => prom.resolve(success),
            CallTag::Spawn(notify) => self::executor::resolve(notify, success),
        }
    }
}

impl Debug for CallTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            CallTag::Batch(ref ctx) => write!(f, "CallTag::Batch({:?})", ctx),
            CallTag::Request(_) => write!(f, "CallTag::Request(..)"),
            CallTag::UnaryRequest(_) => write!(f, "CallTag::UnaryRequest(..)"),
            CallTag::Abort(_) => write!(f, "CallTag::Abort(..)"),
            CallTag::Action(_) => write!(f, "CallTag::Action"),
            CallTag::Spawn(_) => write!(f, "CallTag::Spawn"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::*;
    use std::sync::*;
    use std::thread;

    use super::*;
    use crate::env::Environment;
    use futures::executor::block_on;

    #[test]
    fn test_resolve() {
        let env = Environment::new(1);

        let (cq_f1, tag1) = CallTag::action_pair();
        let (cq_f2, tag2) = CallTag::action_pair();
        let (tx, rx) = mpsc::channel();

        let handler = thread::spawn(move || {
            tx.send(block_on(cq_f1)).unwrap();
            tx.send(block_on(cq_f2)).unwrap();
        });

        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        tag1.resolve(&env.pick_cq(), true);
        assert!(rx.recv().unwrap().is_ok());

        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        tag2.resolve(&env.pick_cq(), false);
        match rx.recv() {
            Ok(Ok(false)) => {}
            res => panic!("expect Ok(false), but got {:?}", res),
        }

        handler.join().unwrap();
    }
}
