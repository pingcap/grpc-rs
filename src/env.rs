// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::{Builder as ThreadBuilder, JoinHandle};

use crate::grpc_sys;

use crate::cq::{CompletionQueue, CompletionQueueHandle, EventType, WorkQueue};
use crate::task::CallTag;

// event loop
fn poll_queue(tx: mpsc::Sender<CompletionQueue>) {
    let cq = Arc::new(CompletionQueueHandle::new());
    let worker_info = Arc::new(WorkQueue::new());
    let cq = CompletionQueue::new(cq, worker_info);
    tx.send(cq.clone()).expect("send back completion queue");
    loop {
        let e = cq.next();
        match e.type_ {
            EventType::GRPC_QUEUE_SHUTDOWN => break,
            // timeout should not happen in theory.
            EventType::GRPC_QUEUE_TIMEOUT => continue,
            EventType::GRPC_OP_COMPLETE => {}
        }

        let tag: Box<CallTag> = unsafe { Box::from_raw(e.tag as _) };

        tag.resolve(&cq, e.success != 0);
        while let Some(work) = unsafe { cq.worker.pop_work() } {
            work.finish();
        }
    }
}

/// [`Environment`] factory in order to configure the properties.
pub struct EnvBuilder {
    cq_count: usize,
    name_prefix: Option<String>,
    after_start: Option<Arc<dyn Fn() + Send + Sync>>,
    before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl EnvBuilder {
    /// Initialize a new [`EnvBuilder`].
    pub fn new() -> EnvBuilder {
        EnvBuilder {
            cq_count: unsafe { grpc_sys::gpr_cpu_num_cores() as usize },
            name_prefix: None,
            after_start: None,
            before_stop: None,
        }
    }

    /// Set the number of completion queues and polling threads. Each thread polls
    /// one completion queue.
    ///
    /// # Panics
    ///
    /// This method will panic if `count` is 0.
    pub fn cq_count(mut self, count: usize) -> EnvBuilder {
        assert!(count > 0);
        self.cq_count = count;
        self
    }

    /// Set the thread name prefix of each polling thread.
    pub fn name_prefix<S: Into<String>>(mut self, prefix: S) -> EnvBuilder {
        self.name_prefix = Some(prefix.into());
        self
    }

    /// Execute function `f` after each thread is started but before it starts doing work.
    pub fn after_start<F: Fn() + Send + Sync + 'static>(mut self, f: F) -> EnvBuilder {
        self.after_start = Some(Arc::new(f));
        self
    }

    /// Execute function `f` before each thread stops.
    pub fn before_stop<F: Fn() + Send + Sync + 'static>(mut self, f: F) -> EnvBuilder {
        self.before_stop = Some(Arc::new(f));
        self
    }

    /// Finalize the [`EnvBuilder`], build the [`Environment`] and initialize the gRPC library.
    pub fn build(self) -> Environment {
        unsafe {
            grpc_sys::grpc_init();
        }
        let mut cqs = Vec::with_capacity(self.cq_count);
        let mut handles = Vec::with_capacity(self.cq_count);
        let (tx, rx) = mpsc::channel();
        for i in 0..self.cq_count {
            let tx_i = tx.clone();
            let mut builder = ThreadBuilder::new();
            if let Some(ref prefix) = self.name_prefix {
                builder = builder.name(format!("{}-{}", prefix, i));
            }
            let after_start = self.after_start.clone();
            let before_stop = self.before_stop.clone();
            let handle = builder
                .spawn(move || {
                    if let Some(f) = after_start {
                        f();
                    }
                    poll_queue(tx_i);
                    if let Some(f) = before_stop {
                        f();
                    }
                })
                .unwrap();
            handles.push(handle);
        }
        for _ in 0..self.cq_count {
            cqs.push(rx.recv().unwrap());
        }

        Environment {
            cqs,
            idx: AtomicUsize::new(0),
            _handles: handles,
        }
    }
}

/// An object that used to control concurrency and start gRPC event loop.
pub struct Environment {
    cqs: Vec<CompletionQueue>,
    idx: AtomicUsize,
    _handles: Vec<JoinHandle<()>>,
}

impl Environment {
    /// Initialize gRPC and create a thread pool to poll completion queue. The thread pool size
    /// and the number of completion queue is specified by `cq_count`. Each thread polls one
    /// completion queue.
    ///
    /// # Panics
    ///
    /// This method will panic if `cq_count` is 0.
    pub fn new(cq_count: usize) -> Environment {
        assert!(cq_count > 0);
        EnvBuilder::new()
            .name_prefix("grpc-poll")
            .cq_count(cq_count)
            .build()
    }

    /// Get all the created completion queues.
    pub fn completion_queues(&self) -> &[CompletionQueue] {
        self.cqs.as_slice()
    }

    /// Pick an arbitrary completion queue.
    pub fn pick_cq(&self) -> CompletionQueue {
        let idx = self.idx.fetch_add(1, Ordering::Relaxed);
        self.cqs[idx % self.cqs.len()].clone()
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        for cq in self.completion_queues() {
            // it's safe to shutdown more than once.
            cq.shutdown()
        }

        // Join our threads when we leave scope
        // Try not to join the current thread
        let current_thread_id = std::thread::current().id();
        for handle in self._handles.drain(..) {
            if handle.thread().id() != current_thread_id {
                handle.join().unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_loop() {
        let env = Environment::new(2);

        let q1 = env.pick_cq();
        let q2 = env.pick_cq();
        let q3 = env.pick_cq();
        let cases = vec![(&q1, &q3, true), (&q1, &q2, false)];
        for (lq, rq, is_eq) in cases {
            let lq_ref = lq.borrow().unwrap();
            let rq_ref = rq.borrow().unwrap();
            if is_eq {
                assert_eq!(lq_ref.as_ptr(), rq_ref.as_ptr());
            } else {
                assert_ne!(lq_ref.as_ptr(), rq_ref.as_ptr());
            }
        }

        assert_eq!(env.completion_queues().len(), 2);
    }
}
