use crossbeam_queue::SegQueue;
use mudu::common::result::RS;
use mudu_contract::protocol::encode_error_response;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;

use crate::io::socket::{close, IoSocket};
use crate::server::async_func_task::{HandleResult, SessionTransferDispatch};
use crate::server::frame_dispatch::dispatch_frame_async;
use crate::server::protocol_codec::{read_next_frame, write_response};
use crate::server::routing::ConnectionTransfer;
use crate::server::transferred_connection::TransferredConnection;
use crate::server::worker::IoUringWorker;
use crate::server::worker_mailbox::WorkerMailboxMsg;
use crate::server::worker_ring_loop::WorkerRingLoop;
use crate::server::worker_task::WorkerTaskFuture;

pub(in crate::server) fn spawn_connection_worker_task(
    worker: IoUringWorker,
    mailbox_fds: Vec<RawFd>,
    mailboxes: Vec<Arc<SegQueue<WorkerMailboxMsg>>>,
    connections: Arc<scc::HashMap<u64, RawFd>>,
    conn_id: u64,
    socket: IoSocket,
    remote_addr: SocketAddr,
    initial_response: Option<Vec<u8>>,
) -> WorkerTaskFuture {
    Box::pin(async move {
        run_connection_worker_task(
            worker,
            mailbox_fds,
            mailboxes,
            connections,
            conn_id,
            socket,
            remote_addr,
            initial_response,
        )
        .await
    })
}

async fn run_connection_worker_task(
    worker: IoUringWorker,
    mailbox_fds: Vec<RawFd>,
    mailboxes: Vec<Arc<SegQueue<WorkerMailboxMsg>>>,
    connections: Arc<scc::HashMap<u64, RawFd>>,
    conn_id: u64,
    socket: IoSocket,
    remote_addr: SocketAddr,
    initial_response: Option<Vec<u8>>,
) -> RS<()> {
    let r = _run_connection_worker_task(
        worker,
        mailbox_fds,
        mailboxes,
        conn_id,
        socket,
        remote_addr,
        initial_response,
    )
    .await;
    let _ = connections.remove_sync(&conn_id);
    r
}
async fn _run_connection_worker_task(
    worker: IoUringWorker,
    mailbox_fds: Vec<RawFd>,
    mailboxes: Vec<Arc<SegQueue<WorkerMailboxMsg>>>,
    conn_id: u64,
    socket: IoSocket,
    remote_addr: SocketAddr,
    initial_response: Option<Vec<u8>>,
) -> RS<()> {
    let mut socket = Some(socket);
    let mut read_buf = Vec::with_capacity(8192);

    if let Some(response) = initial_response {
        write_response(socket.as_ref().unwrap(), &response).await?;
    }

    loop {
        let frame = match read_next_frame(socket.as_ref().unwrap(), &mut read_buf).await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                close(socket.take().unwrap()).await?;
                worker.close_connection_sessions(conn_id)?;
                break;
            }
            Err(err) => {
                let _ = close(socket.take().unwrap()).await;
                return Err(err);
            }
        };

        let request_id = frame.header().request_id();
        match dispatch_frame_async(&worker, conn_id, &frame).await {
            Ok(HandleResult::Response(response)) => {
                write_response(socket.as_ref().unwrap(), &response).await?;
            }
            Ok(HandleResult::Transfer(transfer)) => {
                let connection = build_transfer(
                    conn_id,
                    remote_addr,
                    socket.take().unwrap(),
                    transfer.clone(),
                );
                WorkerRingLoop::dispatch_mailbox_message(
                    &mailbox_fds,
                    &mailboxes,
                    connection.transfer().target_worker(),
                    WorkerMailboxMsg::AdoptConnection(connection),
                )?;
                break;
            }
            Err(err) => {
                let response = encode_error_response(request_id, err.to_string())?;
                write_response(socket.as_ref().unwrap(), &response).await?;
            }
        }
        read_buf = frame.into_payload();
    }
    Ok(())
}

fn build_transfer(
    conn_id: u64,
    remote_addr: SocketAddr,
    socket: IoSocket,
    transfer: SessionTransferDispatch,
) -> TransferredConnection {
    TransferredConnection::new(
        ConnectionTransfer::new(
            conn_id,
            transfer.target_worker(),
            crate::server::connection_state::ConnectionState::Active,
            remote_addr,
        ),
        socket.into_raw_fd(),
        transfer.session_ids().to_vec(),
        Some(transfer.action()),
    )
}
