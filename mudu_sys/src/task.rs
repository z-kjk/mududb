use crate::env::default_env;
#[cfg(not(target_arch = "wasm32"))]
use crate::sync::Notifier;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
#[cfg(not(target_arch = "wasm32"))]
use std::future::Future;
use std::thread;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use tracing::{error, info};

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

#[cfg(not(target_arch = "wasm32"))]
pub fn block_on_tokio_current_thread<F>(fut: F) -> RS<F::Output>
where
    F: Future,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| m_error!(EC::TokioErr, "create current thread runtime error", e))?;
    Ok(runtime.block_on(fut))
}

#[cfg(not(target_arch = "wasm32"))]
pub fn wait_for_shutdown_signal(stop: Notifier) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            error!("create runtime for signal listener error: {}", e);
            return;
        }
    };

    runtime.block_on(async move {
        let stop_wait = stop.clone().into();

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    error!("register SIGTERM handler error: {}", e);
                    return;
                }
            };

            tokio::select! {
                _ = stop_wait.notified() => {
                    return;
                }
                r = tokio::signal::ctrl_c() => {
                    if let Err(e) = r {
                        error!("register Ctrl-C handler error: {}", e);
                        return;
                    }
                    info!("received Ctrl-C/SIGINT, starting graceful shutdown");
                }
                _ = sigterm.recv() => {
                    info!("received SIGTERM, starting graceful shutdown");
                }
            }

            stop.notify_all();
            return;
        }

        #[cfg(not(unix))]
        {
            tokio::select! {
                _ = stop_wait.notified() => {
                    return;
                }
                r = tokio::signal::ctrl_c() => {
                    if let Err(e) = r {
                        error!("register Ctrl-C handler error: {}", e);
                        return;
                    }
                    info!("received Ctrl-C, starting graceful shutdown");
                    stop.notify_all();
                }
            }
        }
    });
}
