use crate::env::default_env;
use crate::fd::RawFd;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use std::sync::mpsc;
use std::time::Duration;

pub fn eventfd() -> RS<RawFd> {
    default_env().sync().eventfd()
}

pub fn notify_eventfd(fd: RawFd) -> RS<()> {
    default_env().sync().notify_eventfd(fd)
}

pub fn read_eventfd(fd: RawFd) -> RS<u64> {
    default_env().sync().read_eventfd(fd)
}

pub fn close_fd(fd: RawFd) -> RS<()> {
    default_env().sync().close_fd(fd)
}

#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::Notify;
#[cfg(not(target_arch = "wasm32"))]
use tracing::trace;

// Notifies tasks to wake up.
// If Notifier::notify_waiters is called, all the task call Notifier::notified would complete, and
// the following invocation of Notifier::notified, which after Notifier::notify_waiters called,
// would return immediately
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct NotifyWait {
    inner: Arc<NotifyWaitInner>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct Notifier {
    inner: Arc<NotifyWaitInner>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct Waiter {
    inner: Arc<NotifyWaitInner>,
}

#[cfg(not(target_arch = "wasm32"))]
pub struct NotifyWaitInner {
    name: String,
    notify: Notify,
    is_notified: AtomicBool,
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for NotifyWait {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn notify_wait() -> (Notifier, Waiter) {
    NotifyWait::new_notify_wait()
}

#[cfg(not(target_arch = "wasm32"))]
impl NotifyWait {
    pub fn new_notify_wait() -> (Notifier, Waiter) {
        let inner = Arc::new(NotifyWaitInner::new());
        (
            Notifier {
                inner: inner.clone(),
            },
            Waiter { inner },
        )
    }

    pub fn notify_wait(&self) -> (Notifier, Waiter) {
        (
            Notifier {
                inner: self.inner.clone(),
            },
            Waiter {
                inner: self.inner.clone(),
            },
        )
    }

    pub fn new() -> Self {
        Self {
            inner: Arc::new(NotifyWaitInner::new()),
        }
    }

    pub fn new_with_name(name: String) -> Self {
        Self {
            inner: Arc::new(NotifyWaitInner::new_with_name(name)),
        }
    }

    pub fn is_notified(&self) -> bool {
        self.inner.is_notified()
    }

    pub async fn notified(&self) {
        trace!("notified {}", self.inner.name);
        self.inner.notified().await;
    }

    pub fn notify_all(&self) -> bool {
        trace!("notify waiter {}", self.inner.name);
        self.inner.notify_all()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NotifyWaitInner {
    fn new() -> Self {
        Self::new_with_name(Default::default())
    }

    fn new_with_name(name: String) -> Self {
        Self {
            name,
            is_notified: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    async fn notified(&self) {
        if !self.is_notified.load(Ordering::SeqCst) {
            self.notify.notified().await;
        }
    }

    fn is_notified(&self) -> bool {
        self.is_notified.load(Ordering::SeqCst)
    }

    fn notify_all(&self) -> bool {
        let r = self
            .is_notified
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);

        match r {
            Ok(_) => {
                self.notify.notify_waiters();
                true
            }
            Err(_) => {
                self.notify.notify_waiters();
                false
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Waiter {
    pub async fn wait(&self) {
        self.inner.notified().await;
    }

    pub fn into(self) -> NotifyWait {
        NotifyWait { inner: self.inner }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Notifier {
    pub fn is_notified(&self) -> bool {
        self.inner.is_notified()
    }
    pub fn notify_all(&self) -> bool {
        self.inner.notify_all()
    }

    pub fn into(self) -> NotifyWait {
        NotifyWait { inner: self.inner }
    }
}

pub struct ChannelSender<T> {
    inner: mpsc::Sender<T>,
}

pub struct ChannelSyncSender<T> {
    inner: mpsc::SyncSender<T>,
}

pub struct ChannelReceiver<T> {
    inner: mpsc::Receiver<T>,
}

impl<T> Clone for ChannelSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Clone for ChannelSyncSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub fn channel<T>() -> (ChannelSender<T>, ChannelReceiver<T>) {
    let (tx, rx) = mpsc::channel();
    (ChannelSender { inner: tx }, ChannelReceiver { inner: rx })
}

pub fn sync_channel<T>(bound: usize) -> (ChannelSyncSender<T>, ChannelReceiver<T>) {
    let (tx, rx) = mpsc::sync_channel(bound);
    (
        ChannelSyncSender { inner: tx },
        ChannelReceiver { inner: rx },
    )
}

impl<T> ChannelSender<T> {
    pub fn send(&self, value: T) -> RS<()> {
        match self.inner.send(value) {
            Ok(()) => Ok(()),
            Err(_) => Err(m_error!(EC::SyncErr, "channel send failed")),
        }
    }

    pub fn into_inner(self) -> mpsc::Sender<T> {
        self.inner
    }
}

impl<T> ChannelSyncSender<T> {
    pub fn send(&self, value: T) -> RS<()> {
        match self.inner.send(value) {
            Ok(()) => Ok(()),
            Err(_) => Err(m_error!(EC::SyncErr, "sync_channel send failed")),
        }
    }

    pub fn try_send(&self, value: T) -> RS<()> {
        match self.inner.try_send(value) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(_)) => Err(m_error!(EC::SyncErr, "sync_channel is full")),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                Err(m_error!(EC::SyncErr, "sync_channel is disconnected"))
            }
        }
    }

    pub fn into_inner(self) -> mpsc::SyncSender<T> {
        self.inner
    }
}

impl<T> ChannelReceiver<T> {
    pub fn recv(&self) -> RS<T> {
        self.inner
            .recv()
            .map_err(|e| m_error!(EC::SyncErr, "channel recv failed", e))
    }

    pub fn try_recv(&self) -> RS<Option<T>> {
        match self.inner.try_recv() {
            Ok(v) => Ok(Some(v)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(e) => Err(m_error!(EC::SyncErr, "channel try_recv failed", e)),
        }
    }

    pub fn recv_timeout(&self, dur: Duration) -> RS<Option<T>> {
        match self.inner.recv_timeout(dur) {
            Ok(v) => Ok(Some(v)),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(e) => Err(m_error!(EC::SyncErr, "channel recv_timeout failed", e)),
        }
    }

    pub fn into_inner(self) -> mpsc::Receiver<T> {
        self.inner
    }
}
