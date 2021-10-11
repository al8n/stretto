cfg_async! {
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::future::Future;
    use std::pin::Pin;

    #[cfg(feature = "default")]
    pub struct WaitGroup {
        inner: Arc<Inner>,
    }

    impl Default for WaitGroup {
        fn default() -> Self {
            Self {
                inner: Arc::new(Inner {
                    count: AtomicUsize::new(0),
                    waker: Mutex::new(None),
                })
            }
        }
    }

    impl WaitGroup {
        /// Creates a new `WaitGroup`
        pub fn new() -> Self {
            Self::default()
        }

        /// Register a new worker.
        pub fn add(&self, num: usize) -> WaitGroup {
            self.inner.count.fetch_add(num, Ordering::Relaxed);
            WaitGroup {
                inner: self.inner.clone(),
            }
        }

        /// Notify the `WaitGroup` that this worker has finished execution.
        pub fn done(self) {
            drop(self)
        }

        /// Wait until all registered workers finish executing.
        pub async fn wait(&self) {
            WaitGroupFuture::new(&self.inner).await
        }
    }

    impl Drop for WaitGroup {
        fn drop(&mut self) {
            let count = self.inner.count.fetch_sub(1, Ordering::Relaxed);
            // We are the last worker
            if count == 1 {
                if let Some(waker) = self.inner.waker.lock().take() {
                    waker.wake();
                }
            }
        }
    }

    struct WaitGroupFuture<'a> {
        inner: &'a Arc<Inner>
    }

    impl<'a> WaitGroupFuture<'a> {
        fn new(inner: &'a Arc<Inner>) -> Self {
            Self{ inner }
        }
    }

    impl Future for WaitGroupFuture<'_> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let waker = cx.waker().clone();
            *self.inner.waker.lock() = Some(waker);

            match self.inner.count.load(Ordering::Relaxed) {
                0 => Poll::Ready(()),
                _ => Poll::Pending,
            }
        }
    }

    struct Inner {
        waker: Mutex<Option<Waker>>,
        count: AtomicUsize,
    }

    impl std::fmt::Debug for WaitGroup {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) ->   std::fmt::Result {
            let count = self.inner.count.load(Ordering::Relaxed);
            f.debug_struct("WaitGroup").field("count", &count).finish()
        }
    }

    #[cfg(test)]
    mod tests {
        use std::time::Duration;
        use super::*;

        #[tokio::test]
        async fn test_wait_group() {
            let wg = WaitGroup::new();
            let ctr = Arc::new(AtomicUsize::new(0));

            for _ in 0..5 {
                let ctrx = ctr.clone();
                let wg = wg.add(1);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    ctrx.fetch_add(1, Ordering::Relaxed);
                    wg.done();
                });
            }
            wg.wait().await;
            assert_eq!(ctr.load(Ordering::Relaxed), 5);
        }

        #[tokio::test]
        async fn test_wait_group_reuse() {
            let wg = WaitGroup::new();
            let ctr = Arc::new(AtomicUsize::new(0));
            for _ in 0..6 {
                let wg = wg.add(1);
                let ctrx = ctr.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    ctrx.fetch_add(1, Ordering::Relaxed);
                    wg.done();
                });
            }

            wg.wait().await;
            assert_eq!(ctr.load(Ordering::Relaxed), 6);

            let worker = wg.add(1);

            let ctrx = ctr.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                ctrx.fetch_add(1, Ordering::Relaxed);
                worker.done();
            });

            wg.wait().await;
            assert_eq!(ctr.load(Ordering::Relaxed), 7);
        }

        #[tokio::test]
        async fn test_worker_clone() {
            let wg = WaitGroup::new();
            let ctr = Arc::new(AtomicUsize::new(0));
            for _ in 0..5 {
                let worker = wg.add(1);
                let ctrx = ctr.clone();
                tokio::spawn(async move {
                    let nested_worker = worker.add(1);
                    let ctrxx = ctrx.clone();
                    tokio::spawn(async move {
                        ctrxx.fetch_add(1, Ordering::Relaxed);
                        nested_worker.done();
                    });
                    ctrx.fetch_add(1, Ordering::Relaxed);
                    worker.done();
                });
            }

            wg.wait().await;
            assert_eq!(ctr.load(Ordering::Relaxed), 10);
        }
    }
}

cfg_not_async!(
    pub type WaitGroup = crossbeam_utils::sync::WaitGroup;
);
