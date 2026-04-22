use std::collections::HashMap;

use crossbeam_queue::SegQueue;
use mudu::common::result::RS;

use crate::server::inflight_op::InflightOp;
use crate::server::worker_loop_stats::WorkerLoopStats;
use crate::server::worker_mailbox::WorkerMailboxMsg;

pub(in crate::server) struct LoopMailboxSubmitCtx<'a> {
    pub ring: &'a mut mudu_sys::uring::IoUring,
    pub mailbox_fd: i32,
    pub mailbox_read_submitted: &'a mut bool,
    pub inflight: &'a mut HashMap<u64, InflightOp>,
    pub next_token: &'a mut u64,
    pub stats: &'a mut WorkerLoopStats,
    pub shutting_down: bool,
}

pub(in crate::server) fn drain_messages(
    mailbox: &SegQueue<WorkerMailboxMsg>,
    stats: &mut WorkerLoopStats,
) -> Vec<WorkerMailboxMsg> {
    let mut drained = Vec::new();
    while let Some(msg) = mailbox.pop() {
        stats.mailbox_drained += 1;
        drained.push(msg);
    }
    drained
}

pub(in crate::server) fn submit_read_if_needed(ctx: &mut LoopMailboxSubmitCtx<'_>) -> RS<()> {
    if *ctx.mailbox_read_submitted || ctx.shutting_down {
        return Ok(());
    }
    let Some(mut sqe) = ctx.ring.next_sqe() else {
        return Ok(());
    };
    let mut value = Box::new(0u64);
    let token = alloc_token(ctx.next_token);
    sqe.set_user_data(token);
    sqe.prep_read_raw(
        ctx.mailbox_fd,
        (&mut *value as *mut u64).cast(),
        std::mem::size_of::<u64>(),
        0,
    );
    ctx.inflight
        .insert(token, InflightOp::MailboxRead { _value: value });
    *ctx.mailbox_read_submitted = true;
    ctx.stats.mailbox_submit += 1;
    Ok(())
}

pub(in crate::server) fn handle_read_completion(
    mailbox_read_submitted: &mut bool,
    stats: &mut WorkerLoopStats,
) {
    stats.cqe_mailbox += 1;
    *mailbox_read_submitted = false;
}

fn alloc_token(next_token: &mut u64) -> u64 {
    let token = *next_token;
    *next_token += 1;
    token
}
