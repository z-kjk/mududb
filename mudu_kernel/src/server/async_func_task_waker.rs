use crossbeam_queue::SegQueue;
use futures::task::ArcWake;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct AsyncFuncTaskWaker {
    op_id: u64,
    completion_queue: Arc<SegQueue<u64>>,
    completed: Arc<AtomicBool>,
    notified: Arc<AtomicBool>,
}

impl AsyncFuncTaskWaker {
    pub fn new(
        op_id: u64,
        completion_queue: Arc<SegQueue<u64>>,
        completed: Arc<AtomicBool>,
    ) -> Self {
        Self {
            op_id,
            completion_queue,
            completed,
            notified: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl ArcWake for AsyncFuncTaskWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        if arc_self.completed.load(Ordering::Acquire) {
            return;
        }

        if !arc_self.notified.swap(true, Ordering::AcqRel) {
            arc_self.completion_queue.push(arc_self.op_id);
        }
    }
}
