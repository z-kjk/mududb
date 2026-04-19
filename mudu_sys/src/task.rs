use crate::env::default_env;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
#[cfg(not(target_arch = "wasm32"))]
use std::future::Future;
use std::thread;
use std::time::Duration;

pub async fn sleep(dur: Duration) -> RS<()> {
    default_env().task().sleep(dur).await
}

pub fn sleep_blocking(dur: Duration) {
    default_env().task().sleep_blocking(dur)
}

pub fn spawn_thread<F, T>(f: F) -> RS<thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Ok(thread::spawn(f))
}

pub fn spawn_thread_named<F, T>(name: impl Into<String>, f: F) -> RS<thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    thread::Builder::new()
        .name(name.into())
        .spawn(f)
        .map_err(|e| m_error!(EC::ThreadErr, "spawn thread error", e))
}

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_tokio<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(fut)
}
