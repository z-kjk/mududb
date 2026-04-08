use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::error::err::MError;
use mudu::m_error;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

pub(crate) fn completion_error(kind: &'static str, result: i32) -> MError {
    m_error!(
        EC::IOErr,
        format!("worker user {} completion error {}", kind, result)
    )
}

pub(crate) struct OpState<T> {
    result: Mutex<Option<RS<T>>>,
    waker: Mutex<Option<Waker>>,
}

pub(crate) fn op_state<T>() -> Arc<OpState<T>> {
    Arc::new(OpState {
        result: Mutex::new(None),
        waker: Mutex::new(None),
    })
}

pub(crate) fn complete_op<T>(state: Arc<OpState<T>>, result: RS<T>) {
    if let Ok(mut slot) = state.result.lock() {
        *slot = Some(result);
    }
    if let Ok(mut waker) = state.waker.lock() {
        if let Some(waker) = waker.take() {
            waker.wake();
        }
    }
}

pub(crate) fn poll_op<T>(state: &Arc<OpState<T>>, cx: &mut Context<'_>) -> Poll<RS<T>> {
    if let Ok(mut slot) = state.result.lock() {
        if let Some(result) = slot.take() {
            return Poll::Ready(result);
        }
    }
    if let Ok(mut waker) = state.waker.lock() {
        *waker = Some(cx.waker().clone());
    }
    Poll::Pending
}

pub(crate) fn try_take_op<T>(state: &Arc<OpState<T>>) -> Option<RS<T>> {
    state.result.lock().ok()?.take()
}
