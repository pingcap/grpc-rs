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

//! gRPC C Core binds a call to a completion queue, all the related readiness
//! will be forwarded to the completion queue. This module utilizes the mechanism
//! and using `Kicker` to wake up completion queue.
//!
//! Apparently, to minimize context switch, it's better to bind the future to the
//! same completion queue as its inner call. Hence method `Executor::spawn` is provided.

use std::cell::UnsafeCell;
use std::mem;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use futures::executor::{self, Notify, Spawn};
use futures::{Async, Future};

use super::CallTag;
use crate::call::Call;
use crate::cq::{CompletionQueue, WorkQueue};
use crate::error::{Error, Result};
use crate::grpc_sys::{self, grpc_call_error};

type BoxFuture<T, E> = Box<dyn Future<Item = T, Error = E> + Send>;

/// A handle to a `Spawn`.
/// Inner future is expected to be polled in the same thread as cq.
type SpawnHandle = Option<Spawn<BoxFuture<(), ()>>>;

/// `Kicker` wakes up the completion queue that the inner call binds to.
pub(crate) struct Kicker {
    call: Call,
}

impl Kicker {
    pub fn from_call(call: Call) -> Kicker {
        Kicker { call }
    }

    /// Wakes up its completion queue.
    ///
    /// `tag` will be popped by `grpc_completion_queue_next` in the future.
    pub fn kick(&self, tag: Box<CallTag>) -> Result<()> {
        let _ref = self.call.cq.borrow()?;
        unsafe {
            let ptr = Box::into_raw(tag);
            let status = grpc_sys::grpcwrap_call_kick_completion_queue(self.call.call, ptr as _);
            if status == grpc_call_error::GRPC_CALL_OK {
                Ok(())
            } else {
                Err(Error::CallFailure(status))
            }
        }
    }
}

unsafe impl Sync for Kicker {}

impl Clone for Kicker {
    fn clone(&self) -> Kicker {
        // Bump call's reference count.
        let call = unsafe {
            grpc_sys::grpc_call_ref(self.call.call);
            self.call.call
        };
        let cq = self.call.cq.clone();
        Kicker {
            call: Call { call, cq },
        }
    }
}

/// When a future is scheduled, it becomes IDLE. When it's ready to be polled,
/// it will be notified via task.notify(), and marked as NOTIFIED. When executor
/// begins to poll the future, it's marked as POLLING. When the executor finishes
/// polling, the future can either be ready or not ready. In the former case, it's
/// marked as COMPLETED. If it's latter, it's marked as IDLE again.
///
/// Note it's possible the future is notified during polling, in which case, executor
/// should polling it when last polling is finished unless it returns ready.
const NOTIFIED: u8 = 1;
const IDLE: u8 = 2;
const POLLING: u8 = 3;
const COMPLETED: u8 = 4;

/// Maintains the spawned future with state, so that it can be notified and polled efficiently.
pub struct SpawnTask {
    handle: UnsafeCell<SpawnHandle>,
    state: AtomicU8,
    kicker: Kicker,
}

impl SpawnTask {
    fn new(s: Spawn<BoxFuture<(), ()>>, kicker: Kicker) -> SpawnTask {
        SpawnTask {
            handle: UnsafeCell::new(Some(s)),
            state: AtomicU8::new(IDLE),
            kicker,
        }
    }

    /// Notify the completion queue.
    ///
    /// It only makes sense to call this function from the thread
    /// that cq is not run on.
    fn mark_notified(&self) -> bool {
        loop {
            match self.state.compare_exchange_weak(
                IDLE,
                NOTIFIED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(POLLING) => match self.state.compare_exchange_weak(
                    POLLING,
                    NOTIFIED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Err(IDLE) | Err(POLLING) => continue,
                    // If it succeeds, then executor will poll the future again;
                    // if it fails, then the future should be resolved. In both
                    // cases, no need to notify the future, hence return false.
                    _ => return false,
                },
                Err(IDLE) => continue,
                _ => return false,
            }
        }
    }
}

pub fn resolve(cq: &CompletionQueue, task: Arc<SpawnTask>, success: bool) {
    // it should always be canceled for now.
    assert!(success);
    poll(cq, task, true);
}

/// A custom notify.
///
/// It will push the inner future to work_queue if it's notified on the
/// same thread as inner cq.
impl Notify for WorkQueue {
    fn notify(&self, id: usize) {
        let task = unsafe { Arc::from_raw(id as *mut SpawnTask) };
        if !task.mark_notified() {
            mem::forget(task);
            return;
        }

        // It can lead to deadlock if poll the future immediately. So we need to
        // defer the work instead.
        if let Some(UnfinishedWork(w)) = self.push_work(UnfinishedWork(task.clone())) {
            match task.kicker.kick(Box::new(CallTag::Spawn(w))) {
                // If the queue is shutdown, then the tag will be notified
                // eventually. So just skip here.
                Err(Error::QueueShutdown) => (),
                Err(e) => panic!("unexpected error when canceling call: {:?}", e),
                _ => (),
            }
        }
        mem::forget(task);
    }

    fn clone_id(&self, id: usize) -> usize {
        let task = unsafe { Arc::from_raw(id as *mut SpawnTask) };
        let t = task.clone();
        mem::forget(task);
        Arc::into_raw(t) as usize
    }

    fn drop_id(&self, id: usize) {
        unsafe { Arc::from_raw(id as *mut SpawnTask) };
    }
}

/// Work that should be differred to be handled.
///
/// Sometimes a work can't be done imediately as it might lead
/// to resourse conflict, deadlock for example. So they will be
/// pushed into a queue and handled when current work is done.
pub struct UnfinishedWork(Arc<SpawnTask>);

impl UnfinishedWork {
    pub fn finish(self, cq: &CompletionQueue) {
        resolve(cq, self.0, true);
    }
}

/// Poll the future.
///
/// `woken` indicates that if the cq is waken up by itself.
fn poll(cq: &CompletionQueue, task: Arc<SpawnTask>, woken: bool) {
    let mut init_state = if woken { NOTIFIED } else { IDLE };
    // TODO: maybe we need to break the loop to avoid hunger.
    loop {
        match task
            .state
            .compare_exchange(init_state, POLLING, Ordering::SeqCst, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(COMPLETED) => return,
            Err(s) => panic!("unexpected state {}", s),
        }

        let id = &*task as *const SpawnTask as usize;

        match unsafe { &mut *task.handle.get() }
            .as_mut()
            .unwrap()
            .poll_future_notify(&cq.worker, id)
        {
            Err(_) | Ok(Async::Ready(_)) => {
                task.state.store(COMPLETED, Ordering::SeqCst);
                unsafe { &mut *task.handle.get() }.take();
            }
            _ => {
                match task.state.compare_exchange(
                    POLLING,
                    IDLE,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(NOTIFIED) => {
                        init_state = NOTIFIED;
                    }
                    Err(s) => panic!("unexpected state {}", s),
                }
            }
        }
    }
}

/// An executor that drives a future in the gRPC poll thread, which
/// can reduce thread context switching.
pub(crate) struct Executor<'a> {
    cq: &'a CompletionQueue,
}

impl<'a> Executor<'a> {
    pub fn new(cq: &CompletionQueue) -> Executor<'_> {
        Executor { cq }
    }

    pub fn cq(&self) -> &CompletionQueue {
        self.cq
    }

    /// Spawn the future into inner poll loop.
    ///
    /// If you want to trace the future, you may need to create a sender/receiver
    /// pair by yourself.
    pub fn spawn<F>(&self, f: F, kicker: Kicker)
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        let s = executor::spawn(Box::new(f) as BoxFuture<_, _>);
        let notify = Arc::new(SpawnTask::new(s, kicker));
        poll(self.cq, notify, false)
    }
}
