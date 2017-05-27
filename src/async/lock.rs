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


use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, ThreadId};

struct Ownership {
    owner: ThreadId,
    holders: usize,
}

/// A simple spin lock for synchronization between Promise
/// and future.
pub struct SpinLock<T> {
    handle: UnsafeCell<(T, Option<Ownership>)>,
    lock: AtomicBool,
}

// It's a lock, as long as the content can be sent between
// threads, it's Sync and Send.
unsafe impl<T: Send> Sync for SpinLock<T> {}
unsafe impl<T: Send> Send for SpinLock<T> {}

impl<T> SpinLock<T> {
    /// Create a lock with the given value.
    pub fn new(t: T) -> SpinLock<T> {
        SpinLock {
            handle: UnsafeCell::new((t, None)),
            lock: AtomicBool::new(false),
        }
    }

    pub fn lock(&self) -> LockGuard<T> {
        loop {
            // TODO: what if poison?
            // It's safe to use swap here. If previous is false, then the lock
            // is taken, loop will break, set it to true is expected;
            // If previous is true, then the loop will go on until others swap
            // back a false, set it to true changes nothing.
            while self.lock.swap(true, Ordering::SeqCst) {}

            let handle = unsafe { &mut *self.handle.get() };
            match handle.1 {
                None => {
                    handle.1 = Some({
                                        Ownership {
                                            owner: thread::current().id(),
                                            holders: 1,
                                        }
                                    });
                    self.lock.swap(false, Ordering::SeqCst);
                    return LockGuard { inner: self };
                }
                Some(ref mut ownership) => {
                    if ownership.owner == thread::current().id() {
                        ownership.holders += 1;
                        self.lock.swap(false, Ordering::SeqCst);
                        return LockGuard { inner: self };
                    }
                }
            }
            self.lock.swap(false, Ordering::SeqCst);
            // maybe sleep a little time?
        }
    }
}

/// A guard for `SpinLock`.
pub struct LockGuard<'a, T: 'a> {
    inner: &'a SpinLock<T>,
}

impl<'a, T> Deref for LockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        let h = unsafe { &*self.inner.handle.get() };
        &h.0
    }
}

impl<'a, T> DerefMut for LockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        let h = unsafe { &mut *self.inner.handle.get() };
        &mut h.0
    }
}

impl<'a, T> Drop for LockGuard<'a, T> {
    fn drop(&mut self) {
        while self.inner.lock.swap(true, Ordering::SeqCst) {}
        let h = unsafe { &mut *self.inner.handle.get() };
        let cleanup = {
            let ownership = h.1.as_mut().unwrap();
            ownership.holders -= 1;
            ownership.holders == 0
        };
        if cleanup {
            h.1.take();
        }
        self.inner.lock.swap(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use std::sync::*;
    use std::sync::mpsc::*;
    use super::*;

    #[test]
    fn test_lock() {
        let lock1 = Arc::new(SpinLock::new(2));
        let lock2 = lock1.clone();
        let lock3 = lock2.clone();
        let guard1 = lock1.lock();
        let guard2 = lock2.lock();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let _guard = lock3.lock();
            tx.send(()).unwrap();
        });
        thread::sleep(Duration::from_millis(10));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        drop(guard1);
        thread::sleep(Duration::from_millis(10));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        drop(guard2);
        assert_eq!(rx.recv(), Ok(()));
    }
}
