use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mudu::common::result::RS;

pub type WorkerTaskFuture = Pin<Box<dyn Future<Output = RS<()>> + 'static>>;

pub(in crate::server) struct WorkerTask {
    conn_id: Option<u64>,
    future: WorkerTaskFuture,
    queued: Arc<AtomicBool>,
    completed: Arc<AtomicBool>,
    waiting_on: Option<u64>,
}

impl WorkerTask {
    pub(in crate::server) fn new(conn_id: Option<u64>, future: WorkerTaskFuture) -> Self {
        Self {
            conn_id,
            future,
            queued: Arc::new(AtomicBool::new(false)),
            completed: Arc::new(AtomicBool::new(false)),
            waiting_on: None,
        }
    }

    pub(in crate::server) fn conn_id(&self) -> Option<u64> {
        self.conn_id
    }

    pub(in crate::server) fn future_mut(&mut self) -> WorkerTaskFutureRef<'_> {
        self.future.as_mut()
    }

    pub(in crate::server) fn queued(&self) -> &Arc<AtomicBool> {
        &self.queued
    }

    pub(in crate::server) fn completed(&self) -> &Arc<AtomicBool> {
        &self.completed
    }

    pub(in crate::server) fn clear_queued(&self) {
        self.queued.store(false, Ordering::Release);
    }

    pub(in crate::server) fn take_waiting_on(&mut self) -> Option<u64> {
        self.waiting_on.take()
    }

    pub(in crate::server) fn set_waiting_on(&mut self, op_id: u64) {
        self.waiting_on = Some(op_id);
    }
}

type WorkerTaskFutureRef<'a> = Pin<&'a mut (dyn Future<Output = RS<()>> + 'static)>;

#[allow(dead_code)]
pub(in crate::server) fn spawn_system_worker_task<F>(future: F) -> WorkerTaskFuture
where
    F: Future<Output = RS<()>> + 'static,
{
    Box::pin(async move { future.await })
}
