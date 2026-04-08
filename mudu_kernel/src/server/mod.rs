//! TCP server backend with a Linux-first `io_uring` implementation.
//!
//! The public `client` module name is kept for compatibility. On Linux the
//! backend uses the native `io_uring` worker loop; on other platforms the same
//! public API falls back to a portable thread-per-worker implementation.
//! Modules that depend on `rliburing` are therefore compiled only on Linux.

pub mod async_func_runtime;
mod async_func_task;
mod async_func_task_waker;
#[cfg(target_os = "linux")]
mod callback_registry;
#[cfg(target_os = "linux")]
mod connection_worker_task;
mod frame_dispatch;
pub mod fsm;
mod handlers;
#[cfg(target_os = "linux")]
mod inflight_op;
#[cfg(target_os = "linux")]
mod loop_mailbox;
#[cfg(target_os = "linux")]
mod loop_user_io;
mod message_dispatcher;
#[cfg(all(test, target_os = "linux"))]
mod perf_test;
#[cfg(target_os = "linux")]
mod protocol_codec;
mod request_ctx;
mod request_response_worker;
pub mod routing;
pub mod server;
#[cfg(target_os = "linux")]
mod server_iouring;
mod session_bound_worker_runtime;
mod task;
#[cfg(target_os = "linux")]
pub(crate) mod task_registry;
#[cfg(target_os = "linux")]
mod transferred_connection;
pub mod worker;
pub mod worker_local;
mod worker_loop_stats;
#[cfg(target_os = "linux")]
mod worker_mailbox;
pub mod worker_registry;
#[cfg(target_os = "linux")]
mod worker_ring_loop;
mod worker_session_manager;
pub mod worker_snapshot;
mod worker_storage;
#[cfg(target_os = "linux")]
mod worker_task;
mod worker_tx_manager;
pub mod x_contract;
mod x_lock_mgr;
