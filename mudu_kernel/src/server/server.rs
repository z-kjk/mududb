#![allow(dead_code)]

use crate::server::async_func_runtime::AsyncFuncInvokerPtr;
use crate::server::async_func_task::{AsyncFuncFuture, AsyncFuncTask, HandleResult};
use crate::server::async_func_task_waker::AsyncFuncTaskWaker;
use crate::server::frame_dispatch::{dispatch_frame_async, try_decode_next_frame};
use crate::server::routing::{ConnectionTransfer, RoutingMode, SessionOpenTransferAction};
use crate::server::worker::IoUringWorker;
use crate::server::worker_registry::{load_or_create_worker_registry, WorkerRegistry};
use crate::wal::worker_log::WorkerLogBatching;
use crossbeam_queue::SegQueue;
use futures::task::{waker, Context};
use futures::Future;
use mudu::common::id::OID;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu_contract::protocol::{
    encode_error_response, encode_session_create_response, Frame, SessionCreateResponse,
};
use mudu_utils::notifier::{notify_wait, Waiter};
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{atomic::AtomicBool, Arc};
use std::task::Poll;
use std::thread;
use std::time::Duration;

/// Configuration shared by both execution paths of the `client` backend.
///
/// The `IoUring*` naming is historical and preserved to avoid breaking callers.
/// On Linux this configuration is consumed by the native `io_uring` backend.
/// On non-Linux targets the same configuration is used by a compatible
/// fallback implementation that keeps the worker model and protocol surface
/// unchanged without depending on `io_uring`.
pub struct IoUringTcpServerConfig {
    worker_count: usize,
    listen_ip: String,
    listen_port: u16,
    prebound_listener: Option<TcpListener>,
    data_dir: String,
    log_dir: String,
    log_chunk_size: u64,
    log_batching: WorkerLogBatching,
    routing_mode: RoutingMode,
    procedure_runtime: Option<AsyncFuncInvokerPtr>,
    worker_procedure_runtimes: Option<Vec<AsyncFuncInvokerPtr>>,
    worker_registry: Arc<WorkerRegistry>,
}

impl IoUringTcpServerConfig {
    /// Creates a backend configuration.
    ///
    /// The resulting value can be used on all supported targets. Linux uses the
    /// native `io_uring` path, while other platforms use the fallback path with
    /// the same externally visible behavior.
    pub fn new(
        worker_count: usize,
        listen_ip: String,
        listen_port: u16,
        data_dir: String,
        log_dir: String,
        routing_mode: RoutingMode,
        procedure_runtime: Option<AsyncFuncInvokerPtr>,
    ) -> RS<Self> {
        let worker_registry = load_or_create_worker_registry(&log_dir, worker_count)?;
        Ok(Self {
            worker_count,
            listen_ip,
            listen_port,
            prebound_listener: None,
            data_dir,
            log_dir,
            log_chunk_size: 64 * 1024 * 1024,
            log_batching: WorkerLogBatching::default(),
            routing_mode,
            procedure_runtime,
            worker_procedure_runtimes: None,
            worker_registry,
        })
    }

    pub fn with_log_chunk_size(mut self, log_chunk_size: u64) -> Self {
        self.log_chunk_size = log_chunk_size;
        self
    }

    pub fn with_log_batching(mut self, log_batching: WorkerLogBatching) -> Self {
        self.log_batching = log_batching;
        self
    }

    pub fn with_prebound_listener(mut self, listener: TcpListener) -> Self {
        self.prebound_listener = Some(listener);
        self
    }

    pub fn with_worker_registry(mut self, worker_registry: Arc<WorkerRegistry>) -> RS<Self> {
        if worker_registry.workers().len() != self.worker_count {
            return Err(m_error!(
                EC::ParseErr,
                format!(
                    "worker registry count {} does not match expected {}",
                    worker_registry.workers().len(),
                    self.worker_count
                )
            ));
        }
        self.worker_registry = worker_registry;
        Ok(self)
    }

    /// Installs per-worker procedure runtimes.
    ///
    /// When this is not set, every worker uses `procedure_runtime()`. This hook
    /// exists so upper layers can give each worker an isolated invoker instance
    /// while keeping the transport API unchanged across Linux and non-Linux
    /// implementations.
    pub fn with_worker_procedure_runtimes(mut self, runtimes: Vec<AsyncFuncInvokerPtr>) -> Self {
        self.worker_procedure_runtimes = Some(runtimes);
        self
    }

    pub fn worker_count(&self) -> usize {
        self.worker_count
    }

    pub fn listen_ip(&self) -> &str {
        &self.listen_ip
    }

    pub fn listen_port(&self) -> u16 {
        self.listen_port
    }

    pub fn take_prebound_listener(&mut self) -> Option<TcpListener> {
        self.prebound_listener.take()
    }

    pub fn log_dir(&self) -> &str {
        &self.log_dir
    }

    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }

    pub fn log_chunk_size(&self) -> u64 {
        self.log_chunk_size
    }

    pub fn log_batching(&self) -> WorkerLogBatching {
        self.log_batching
    }

    pub fn routing_mode(&self) -> RoutingMode {
        self.routing_mode
    }

    pub fn worker_registry(&self) -> Arc<WorkerRegistry> {
        self.worker_registry.clone()
    }

    pub fn procedure_runtime(&self) -> Option<AsyncFuncInvokerPtr> {
        self.procedure_runtime.clone()
    }

    pub fn procedure_runtime_for_worker(&self, worker_id: usize) -> Option<AsyncFuncInvokerPtr> {
        self.worker_procedure_runtimes
            .as_ref()
            .and_then(|runtimes| runtimes.get(worker_id).cloned())
            .or_else(|| self.procedure_runtime())
    }
}

/// Historical backend entry point for the `client` transport.
///
/// The name is preserved for compatibility. Actual behavior is target-specific:
/// Linux runs the native `io_uring` backend, and other platforms run a
/// semantically compatible fallback implementation.
pub struct IoUringTcpBackend;

#[derive(Debug)]
struct TransferredConnection {
    transfer: ConnectionTransfer,
    stream: TcpStream,
    session_ids: Vec<OID>,
    session_open_action: Option<SessionOpenTransferAction>,
}

struct WorkerConnection {
    conn_id: u64,
    state: crate::server::fsm::ConnectionState,
    stream: TcpStream,
    remote_addr: SocketAddr,
    transferred: bool,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
}

fn apply_handle_result_to_connection(
    connection: &mut WorkerConnection,
    inboxes: &[Arc<SegQueue<TransferredConnection>>],
    result: HandleResult,
) -> RS<()> {
    match result {
        HandleResult::Response(payload) => {
            connection.write_buf.extend_from_slice(&payload);
        }
        HandleResult::Transfer(transfer) => {
            let stream = connection
                .stream
                .try_clone()
                .map_err(|e| m_error!(EC::NetErr, "clone transferred stream error", e))?;
            enqueue_transfer(
                inboxes,
                connection.conn_id,
                transfer.target_worker(),
                connection.remote_addr,
                stream,
                transfer.session_ids().to_vec(),
                Some(transfer.action()),
            )?;
            connection.transferred = true;
            connection.state = crate::server::fsm::ConnectionState::Closing;
            connection.write_buf.clear();
        }
    }
    Ok(())
}

fn apply_handle_result(
    connections: &mut HashMap<u64, WorkerConnection>,
    inboxes: &[Arc<SegQueue<TransferredConnection>>],
    conn_id: u64,
    result: HandleResult,
) -> RS<()> {
    let Some(connection) = connections.get_mut(&conn_id) else {
        return Ok(());
    };
    apply_handle_result_to_connection(connection, inboxes, result)
}

struct FallbackAsyncFuncState {
    next_task_id: u64,
    next_op_id: u64,
    tasks: HashMap<u64, AsyncFuncTask>,
    ready_queue: Arc<SegQueue<u64>>,
    completion_queue: Arc<SegQueue<u64>>,
    op_registry: HashMap<u64, u64>,
}

impl FallbackAsyncFuncState {
    fn new() -> Self {
        Self {
            next_task_id: 1,
            next_op_id: 1,
            tasks: HashMap::new(),
            ready_queue: Arc::new(SegQueue::new()),
            completion_queue: Arc::new(SegQueue::new()),
            op_registry: HashMap::new(),
        }
    }

    fn enqueue_future(&mut self, conn_id: u64, request_id: u64, future: AsyncFuncFuture) {
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        self.tasks.insert(
            task_id,
            AsyncFuncTask::new(
                conn_id,
                request_id,
                future,
                Arc::new(AtomicBool::new(false)),
            ),
        );
        self.ready_queue.push(task_id);
    }

    fn drain_completions(&mut self) -> bool {
        let mut progressed = false;
        while let Some(op_id) = self.completion_queue.pop() {
            let Some(task_id) = self.op_registry.remove(&op_id) else {
                continue;
            };
            let Some(task) = self.tasks.get(&task_id) else {
                continue;
            };
            if !task.queued().swap(true, Ordering::AcqRel) {
                self.ready_queue.push(task_id);
                progressed = true;
            }
        }
        progressed
    }

    fn poll_ready(
        &mut self,
        connections: &mut HashMap<u64, WorkerConnection>,
        inboxes: &[Arc<SegQueue<TransferredConnection>>],
    ) -> RS<bool> {
        let mut progressed = false;
        while let Some(task_id) = self.ready_queue.pop() {
            let Some(mut task) = self.tasks.remove(&task_id) else {
                continue;
            };
            progressed = true;
            task.clear_queued();
            if let Some(waiting_on) = task.take_waiting_on() {
                self.op_registry.remove(&waiting_on);
            }

            let op_id = self.next_op_id;
            self.next_op_id += 1;
            let waker = waker(Arc::new(AsyncFuncTaskWaker::new(
                op_id,
                self.completion_queue.clone(),
                task.completed().clone(),
            )));
            let mut cx = Context::from_waker(&waker);
            match task.future_mut().poll(&mut cx) {
                Poll::Ready(Ok(result)) => {
                    apply_handle_result(connections, inboxes, task.conn_id(), result)?;
                }
                Poll::Ready(Err(err)) => {
                    if let Some(connection) = connections.get_mut(&task.conn_id()) {
                        let response = encode_error_response(task.request_id(), err.to_string())?;
                        connection.write_buf.extend_from_slice(&response);
                    }
                }
                Poll::Pending => {
                    task.set_waiting_on(op_id);
                    self.op_registry.insert(op_id, task_id);
                    self.tasks.insert(task_id, task);
                }
            }
        }
        Ok(progressed)
    }
}

impl IoUringTcpBackend {
    /// Starts the backend until shutdown.
    ///
    /// This method keeps the old public entry point stable. It dispatches to
    /// the Linux `io_uring` implementation when available and otherwise uses
    /// the portable fallback path.
    pub fn sync_serve(cfg: IoUringTcpServerConfig) -> RS<()> {
        let (_stop_notifier, stop_waiter) = notify_wait();
        Self::sync_serve_with_stop(cfg, stop_waiter)
    }

    /// Internal serve entry that accepts an explicit stop waiter.
    ///
    /// Linux uses `server_iouring`; non-Linux bridges the async stop signal
    /// into an atomic flag and then runs the fallback worker loop.
    pub fn sync_serve_with_stop(cfg: IoUringTcpServerConfig, stop: Waiter) -> RS<()> {
        #[cfg(target_os = "linux")]
        {
            return crate::server::server_iouring::sync_serve_iouring(cfg, stop);
        }

        #[cfg(not(target_os = "linux"))]
        {
            let stop_flag = Arc::new(AtomicBool::new(false));
            let stop_for_fallback = stop_flag.clone();
            let notifier = thread::Builder::new()
                .name("iouring-stop-bridge".to_string())
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            m_error!(
                                EC::TokioErr,
                                "create runtime for io_uring fallback stop bridge error",
                                e
                            )
                        })?;
                    runtime.block_on(stop.wait());
                    stop_for_fallback.store(true, Ordering::Relaxed);
                    Ok(())
                })
                .map_err(|e| m_error!(EC::ThreadErr, "spawn io_uring stop bridge error", e))?;
            let result = sync_serve_fallback(cfg, stop_flag);
            let notify_result = notifier
                .join()
                .map_err(|_| m_error!(EC::ThreadErr, "join io_uring stop bridge error"))?;
            notify_result?;
            return result;
        }
    }
}

// Non-Linux compatibility path for the historical `IoUringTcpBackend` API.
fn sync_serve_fallback(mut cfg: IoUringTcpServerConfig, stop: Arc<AtomicBool>) -> RS<()> {
    if cfg.worker_count() == 0 {
        return Err(m_error!(EC::ParseErr, "invalid io_uring worker count"));
    }
    let listen_addr: SocketAddr = format!("{}:{}", cfg.listen_ip(), cfg.listen_port())
        .parse()
        .map_err(|e| m_error!(EC::ParseErr, "parse io_uring tcp listen address error", e))?;

    let conn_id_alloc = Arc::new(AtomicU64::new(1));
    let inboxes: Vec<_> = (0..cfg.worker_count())
        .map(|_| Arc::new(SegQueue::<TransferredConnection>::new()))
        .collect();
    let listener = match cfg.take_prebound_listener() {
        Some(listener) => listener,
        None => create_listener(listen_addr)?,
    };

    let mut handles = Vec::with_capacity(cfg.worker_count());
    for worker_id in 0..cfg.worker_count() {
        let worker_count = cfg.worker_count();
        let log_dir = cfg.log_dir().to_string();
        let log_chunk_size = cfg.log_chunk_size();
        let log_batching = cfg.log_batching();
        let routing_mode = cfg.routing_mode();
        let procedure_runtime = cfg.procedure_runtime_for_worker(worker_id);
        let worker_identity = cfg
            .worker_registry()
            .worker(worker_id)
            .cloned()
            .ok_or_else(|| {
                m_error!(
                    EC::NoSuchElement,
                    format!("missing worker identity {}", worker_id)
                )
            })?;
        let worker_registry = cfg.worker_registry();
        let data_dir = cfg.data_dir().to_string();
        let inbox = inboxes[worker_id].clone();
        let all_inboxes = inboxes.clone();
        let conn_id_alloc = conn_id_alloc.clone();
        let stop = stop.clone();
        let listener = listener
            .try_clone()
            .map_err(|e| m_error!(EC::NetErr, "clone tcp listener error", e))?;
        let handle = thread::Builder::new()
            .name(format!("iouring-tcp-worker-{worker_id}"))
            .spawn(move || {
                let worker = IoUringWorker::new_with_log_batching(
                    worker_identity,
                    worker_count,
                    routing_mode,
                    log_dir,
                    data_dir,
                    log_chunk_size,
                    log_batching,
                    procedure_runtime,
                    worker_registry,
                )?;
                run_worker_loop(worker, listener, inbox, all_inboxes, conn_id_alloc, stop)
            })
            .map_err(|e| m_error!(EC::ThreadErr, "spawn io_uring worker error", e))?;
        handles.push(handle);
    }

    for handle in handles {
        let result = handle
            .join()
            .map_err(|_| m_error!(EC::ThreadErr, "join io_uring worker error"))?;
        result?;
    }
    Ok(())
}

fn create_listener(listen_addr: SocketAddr) -> RS<TcpListener> {
    let domain = if listen_addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .map_err(|e| m_error!(EC::NetErr, "create tcp listener socket error", e))?;
    socket
        .set_reuse_address(true)
        .map_err(|e| m_error!(EC::NetErr, "enable SO_REUSEADDR error", e))?;
    socket
        .set_nonblocking(true)
        .map_err(|e| m_error!(EC::NetErr, "set listener nonblocking error", e))?;
    socket
        .bind(&listen_addr.into())
        .map_err(|e| m_error!(EC::NetErr, "bind io_uring tcp listener error", e))?;
    socket
        .listen(1024)
        .map_err(|e| m_error!(EC::NetErr, "listen io_uring tcp listener error", e))?;
    Ok(socket.into())
}

fn run_worker_loop(
    worker: IoUringWorker,
    listener: TcpListener,
    inbox: Arc<SegQueue<TransferredConnection>>,
    inboxes: Vec<Arc<SegQueue<TransferredConnection>>>,
    conn_id_alloc: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> RS<()> {
    let mut connections = HashMap::<u64, WorkerConnection>::new();
    let mut async_funcs = FallbackAsyncFuncState::new();
    let idle_sleep = Duration::from_millis(1);

    while !stop.load(Ordering::Relaxed) {
        let mut progressed = false;
        progressed |= drain_accepted_connections(
            &listener,
            &worker,
            &inboxes,
            &mut connections,
            &conn_id_alloc,
        )?;
        progressed |= drain_transferred_connections(&worker, inbox.as_ref(), &mut connections)?;
        progressed |= async_funcs.drain_completions();
        progressed |= async_funcs.poll_ready(&mut connections, &inboxes)?;
        progressed |= drive_connections(&worker, &mut async_funcs, &mut connections, &inboxes)?;

        if !progressed {
            thread::sleep(idle_sleep);
        }
    }
    Ok(())
}

fn drain_accepted_connections(
    listener: &TcpListener,
    worker: &IoUringWorker,
    inboxes: &[Arc<SegQueue<TransferredConnection>>],
    connections: &mut HashMap<u64, WorkerConnection>,
    conn_id_alloc: &AtomicU64,
) -> RS<bool> {
    let mut progressed = false;
    loop {
        match listener.accept() {
            Ok((stream, remote_addr)) => {
                progressed = true;
                let conn_id = conn_id_alloc.fetch_add(1, Ordering::Relaxed);
                let target_worker = worker.route_connection(conn_id, remote_addr);
                if target_worker == worker.worker_index() {
                    register_connection(connections, conn_id, remote_addr, stream)?;
                } else {
                    enqueue_transfer(
                        inboxes,
                        conn_id,
                        target_worker,
                        remote_addr,
                        stream,
                        Vec::new(),
                        None,
                    )?;
                }
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => break,
            Err(err) => {
                return Err(m_error!(
                    EC::NetErr,
                    "accept io_uring tcp connection error",
                    err
                ));
            }
        }
    }
    Ok(progressed)
}

fn enqueue_transfer(
    inboxes: &[Arc<SegQueue<TransferredConnection>>],
    conn_id: u64,
    target_worker: usize,
    remote_addr: SocketAddr,
    stream: TcpStream,
    session_ids: Vec<OID>,
    session_open_action: Option<SessionOpenTransferAction>,
) -> RS<()> {
    let target_inbox = inboxes.get(target_worker).ok_or_else(|| {
        m_error!(
            EC::InternalErr,
            format!("route target worker {} is out of range", target_worker)
        )
    })?;
    target_inbox.push(TransferredConnection {
        transfer: ConnectionTransfer::new(
            conn_id,
            target_worker,
            crate::server::fsm::ConnectionState::Accepted,
            remote_addr,
        ),
        stream,
        session_ids,
        session_open_action,
    });
    Ok(())
}

fn drain_transferred_connections(
    worker: &IoUringWorker,
    inbox: &SegQueue<TransferredConnection>,
    connections: &mut HashMap<u64, WorkerConnection>,
) -> RS<bool> {
    let mut progressed = false;
    while let Some(connection) = inbox.pop() {
        progressed = true;
        worker.adopt_connection_sessions(connection.transfer.conn_id(), &connection.session_ids)?;
        register_connection(
            connections,
            connection.transfer.conn_id(),
            connection.transfer.remote_addr(),
            connection.stream,
        )?;
        if let Some(action) = connection.session_open_action {
            let payload = match worker
                .open_session_with_config(connection.transfer.conn_id(), action.config())
            {
                Ok(session_id) => encode_session_create_response(
                    action.request_id(),
                    &SessionCreateResponse::new(session_id),
                )?,
                Err(err) => encode_error_response(action.request_id(), err.to_string())?,
            };
            if let Some(registered) = connections.get_mut(&connection.transfer.conn_id()) {
                registered.write_buf.extend_from_slice(&payload);
            }
        }
    }
    Ok(progressed)
}

fn register_connection(
    connections: &mut HashMap<u64, WorkerConnection>,
    conn_id: u64,
    remote_addr: SocketAddr,
    stream: TcpStream,
) -> RS<()> {
    stream
        .set_nonblocking(true)
        .map_err(|e| m_error!(EC::NetErr, "set connection nonblocking error", e))?;
    stream
        .set_nodelay(true)
        .map_err(|e| m_error!(EC::NetErr, "set connection nodelay error", e))?;
    connections.insert(
        conn_id,
        WorkerConnection {
            conn_id,
            state: crate::server::fsm::ConnectionState::Active,
            stream,
            remote_addr,
            transferred: false,
            read_buf: Vec::with_capacity(4096),
            write_buf: Vec::with_capacity(4096),
        },
    );
    Ok(())
}

fn drive_connections(
    worker: &IoUringWorker,
    async_funcs: &mut FallbackAsyncFuncState,
    connections: &mut HashMap<u64, WorkerConnection>,
    inboxes: &[Arc<SegQueue<TransferredConnection>>],
) -> RS<bool> {
    let mut progressed = false;
    let conn_ids: Vec<u64> = connections.keys().copied().collect();
    let mut closed = Vec::new();

    for conn_id in conn_ids {
        let Some(connection) = connections.get_mut(&conn_id) else {
            continue;
        };
        progressed |= flush_pending_writes(connection)?;
        let connection_progress = read_and_dispatch(worker, async_funcs, connection, inboxes)?;
        progressed |= connection_progress;
        if connection.state == crate::server::fsm::ConnectionState::Closing
            && connection.write_buf.is_empty()
        {
            closed.push((conn_id, connection.transferred));
        }
    }

    for (conn_id, transferred) in closed {
        if !transferred {
            worker.close_connection_sessions(conn_id)?;
        }
        connections.remove(&conn_id);
    }
    Ok(progressed)
}

fn flush_pending_writes(connection: &mut WorkerConnection) -> RS<bool> {
    let mut progressed = false;
    while !connection.write_buf.is_empty() {
        match connection.stream.write(&connection.write_buf) {
            Ok(0) => {
                connection.state = crate::server::fsm::ConnectionState::Closing;
                break;
            }
            Ok(written) => {
                progressed = true;
                connection.write_buf.drain(0..written);
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => break,
            Err(err) => return Err(m_error!(EC::NetErr, "write tcp response error", err)),
        }
    }
    Ok(progressed)
}

fn read_and_dispatch(
    worker: &IoUringWorker,
    async_funcs: &mut FallbackAsyncFuncState,
    connection: &mut WorkerConnection,
    inboxes: &[Arc<SegQueue<TransferredConnection>>],
) -> RS<bool> {
    let mut progressed = false;
    let mut buf = [0u8; 8192];
    loop {
        match connection.stream.read(&mut buf) {
            Ok(0) => {
                connection.state = crate::server::fsm::ConnectionState::Closing;
                break;
            }
            Ok(read) => {
                progressed = true;
                connection.read_buf.extend_from_slice(&buf[..read]);
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => break,
            Err(err) => return Err(m_error!(EC::NetErr, "read tcp request error", err)),
        }
    }

    while let Some((frame, consumed)) = try_decode_next_frame(&connection.read_buf)? {
        progressed = true;
        let response = dispatch_frame(worker, connection.conn_id, async_funcs, &frame);
        connection.read_buf.drain(0..consumed);
        match response {
            Ok(Some(result)) => {
                apply_handle_result_to_connection(connection, inboxes, result)?;
                if connection.transferred {
                    return Ok(true);
                }
            }
            Ok(None) => {}
            Err(err) => {
                let payload = encode_error_response(frame.header().request_id(), err.to_string())?;
                connection.write_buf.extend_from_slice(&payload);
            }
        }
    }
    Ok(progressed)
}

fn dispatch_frame(
    worker: &IoUringWorker,
    conn_id: u64,
    async_funcs: &mut FallbackAsyncFuncState,
    frame: &Frame,
) -> RS<Option<HandleResult>> {
    let request_id = frame.header().request_id();
    let worker = worker.clone();
    let frame = frame.clone();
    let mut future = Box::pin(async move { dispatch_frame_async(&worker, conn_id, &frame).await });
    let waker = waker(Arc::new(AsyncFuncTaskWaker::new(
        0,
        Arc::new(SegQueue::new()),
        Arc::new(AtomicBool::new(false)),
    )));
    let mut cx = Context::from_waker(&waker);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(Ok(result)) => Ok(Some(result)),
        Poll::Ready(Err(err)) => Err(err),
        Poll::Pending => {
            async_funcs.enqueue_future(conn_id, request_id, future);
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mudu_contract::protocol::encode_get_request;
    use mudu_contract::protocol::GetRequest;
    use mudu_contract::protocol::HEADER_LEN;

    #[test]
    fn try_decode_next_frame_waits_for_full_payload() {
        let encoded = encode_get_request(1, &GetRequest::new(1, b"k".to_vec())).unwrap();
        assert!(try_decode_next_frame(&encoded[..HEADER_LEN - 1])
            .unwrap()
            .is_none());
        assert!(try_decode_next_frame(&encoded[..HEADER_LEN])
            .unwrap()
            .is_none());
        let decoded = try_decode_next_frame(&encoded).unwrap().unwrap();
        assert_eq!(decoded.0.header().request_id(), 1);
        assert_eq!(decoded.1, encoded.len());
    }
}
