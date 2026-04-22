#[cfg(not(target_arch = "wasm32"))]
pub use mudu_sys::sync::{Notifier, NotifyWait, Waiter, notify_wait};
