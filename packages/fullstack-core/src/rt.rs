//! Executor for `!Send` server futures: SSR renders, server function bodies and payload producers.
//!
//! Native: tokio-util `LocalPoolHandle` worker threads.
//! wasm32: the JS host's event loop via `wasm_bindgen_futures::spawn_local`. A wasm32 JS host
//! cannot spawn OS threads, so `LocalPoolHandle::new` panics there, and `tokio::task::spawn_local`
//! panics outside a tokio `LocalSet`.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// A handle to an executor that runs `!Send` futures.
#[derive(Clone, Debug)]
pub struct LocalRt {
    #[cfg(not(target_arch = "wasm32"))]
    pool: tokio_util::task::LocalPoolHandle,
}

impl LocalRt {
    /// Native: a pool with one worker thread per available core. wasm32: the host's event loop.
    pub fn new() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            pool: tokio_util::task::LocalPoolHandle::new(
                std::thread::available_parallelism()
                    .map(usize::from)
                    .unwrap_or(1),
            ),
        }
    }

    /// Spawn a `!Send` future, built on the executor by a `Send` closure.
    ///
    /// Dropping the returned [`LocalTask`] detaches the task, like a tokio `JoinHandle`.
    pub fn spawn_pinned<F, Fut>(&self, create: F) -> LocalTask<Fut::Output>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future + 'static,
        Fut::Output: Send + 'static,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            LocalTask {
                inner: self.pool.spawn_pinned(create),
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            use futures_util::future::{AbortHandle, Abortable};

            let (tx, rx) = futures_channel::oneshot::channel();
            let (abort, registration) = AbortHandle::new_pair();
            wasm_bindgen_futures::spawn_local(async move {
                if let Ok(output) = Abortable::new(create(), registration).await {
                    let _ = tx.send(output);
                }
            });
            LocalTask { rx, abort }
        }
    }
}

impl Default for LocalRt {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn a `!Send` future on a process-wide [`LocalRt`], created on first use.
pub fn spawn_pinned<F, Fut>(create: F) -> LocalTask<Fut::Output>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future + 'static,
    Fut::Output: Send + 'static,
{
    static RT: std::sync::OnceLock<LocalRt> = std::sync::OnceLock::new();
    RT.get_or_init(LocalRt::new).spawn_pinned(create)
}

/// A task spawned with [`LocalRt::spawn_pinned`] or [`spawn_pinned`].
pub struct LocalTask<T> {
    #[cfg(not(target_arch = "wasm32"))]
    inner: tokio::task::JoinHandle<T>,
    #[cfg(target_arch = "wasm32")]
    rx: futures_channel::oneshot::Receiver<T>,
    #[cfg(target_arch = "wasm32")]
    abort: futures_util::future::AbortHandle,
}

impl<T> LocalTask<T> {
    /// Cancel the task. Awaiting it afterwards yields an error unless it already finished.
    pub fn abort(&self) {
        #[cfg(not(target_arch = "wasm32"))]
        self.inner.abort();

        #[cfg(target_arch = "wasm32")]
        self.abort.abort();
    }
}

impl<T> Future for LocalTask<T> {
    type Output = Result<T, LocalTaskError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(not(target_arch = "wasm32"))]
        let poll = Pin::new(&mut self.inner).poll(cx);

        #[cfg(target_arch = "wasm32")]
        let poll = Pin::new(&mut self.rx).poll(cx);

        poll.map(|result| result.map_err(|err| LocalTaskError(err.into())))
    }
}

/// Why a [`LocalTask`] produced no value.
///
/// On native this displays exactly like tokio's `JoinError`. On wasm32 the only cause is
/// cancellation: `wasm32-unknown-unknown` aborts on panic, so a panicking task never returns.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct LocalTaskError(Repr);

#[derive(Debug, thiserror::Error)]
enum Repr {
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),

    #[cfg(target_arch = "wasm32")]
    #[error("task was cancelled")]
    Cancelled,
}

#[cfg(target_arch = "wasm32")]
impl From<futures_channel::oneshot::Canceled> for Repr {
    fn from(_: futures_channel::oneshot::Canceled) -> Self {
        Repr::Cancelled
    }
}
