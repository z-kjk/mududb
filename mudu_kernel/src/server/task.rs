#[cfg(target_os = "linux")]
use crate::io::worker_ring::with_current_ring;
#[cfg(target_os = "linux")]
use crate::server::worker_task::WorkerTaskFuture;

#[cfg(target_os = "linux")]
#[allow(dead_code)]
pub fn spawn(conn_id: Option<u64>, future: WorkerTaskFuture) {
    let _ = with_current_ring(|ring| {
        ring.worker_task_registry().spawn(conn_id, future);
        Ok(())
    });
}

#[cfg(not(target_os = "linux"))]
pub fn spawn<T>(_conn_id: Option<u64>, _future: T)
where
    T: Send + 'static,
{
}
