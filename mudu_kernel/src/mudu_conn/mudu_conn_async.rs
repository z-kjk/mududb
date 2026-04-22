use async_trait::async_trait;
use mudu::common::id::OID;
use mudu::common::result::RS;
use mudu::common::xid::XID;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu_contract::database::db_conn::DBConnAsync;
use mudu_contract::database::prepared_stmt::PreparedStmt;
use mudu_contract::database::result_set::ResultSetAsync;
use mudu_contract::database::sql_params::SQLParams;
use mudu_contract::database::sql_stmt::SQLStmt;
use mudu_contract::protocol::{
    decode_error_response, decode_server_response, decode_session_create_response,
    encode_batch_request, encode_client_request_with_message_type, encode_session_create_request,
    ClientRequest, Frame, FrameHeader, MessageType, SessionCreateRequest, HEADER_LEN,
};
use sql_parser::ast::parser::SQLParser;
use sql_parser::ast::stmt_type::StmtType;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;

use crate::mudu_conn::mudu_prepared_stmt::MuduPreparedStmt;
use crate::server::worker_local::{try_current_worker_local, WorkerExecute, WorkerLocalRef};
use crate::sql::describer::Describer;

static DEFAULT_REMOTE_ADDR: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static DEFAULT_REMOTE_WORKER_ID: OnceLock<Mutex<Option<OID>>> = OnceLock::new();

enum ConnBackend {
    WorkerLocal(WorkerLocalRef),
    Remote(Arc<RemoteWorkerConn>),
}

struct RemoteWorkerConn {
    addr: String,
    worker_id: Option<OID>,
    session_id: AsyncMutex<Option<OID>>,
    stream: AsyncMutex<Option<RemoteProtocolClient>>,
}

struct RemoteProtocolClient {
    stream: TcpStream,
    next_request_id: u64,
}

pub struct MuduConnAsync {
    backend: ConnBackend,
    parser: Arc<SQLParser>,
    session_id: Arc<AsyncMutex<Option<OID>>>,
}

pub fn set_default_remote_addr(addr: Option<String>) {
    let slot = DEFAULT_REMOTE_ADDR.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = slot.lock() {
        *guard = addr;
    }
}

pub fn set_default_remote_worker_id(worker_id: Option<OID>) {
    let slot = DEFAULT_REMOTE_WORKER_ID.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = slot.lock() {
        *guard = worker_id;
    }
}

fn default_remote_addr() -> Option<String> {
    DEFAULT_REMOTE_ADDR
        .get()
        .and_then(|slot| slot.lock().ok().and_then(|guard| guard.clone()))
}

fn default_remote_worker_id() -> Option<OID> {
    DEFAULT_REMOTE_WORKER_ID
        .get()
        .and_then(|slot| slot.lock().ok().and_then(|guard| *guard))
}

impl MuduConnAsync {
    pub fn new() -> RS<Self> {
        if let Some(worker_local) = try_current_worker_local() {
            return Ok(Self {
                backend: ConnBackend::WorkerLocal(worker_local),
                parser: Arc::new(SQLParser::new()),
                session_id: Arc::new(AsyncMutex::new(None)),
            });
        }
        let addr = default_remote_addr().ok_or_else(|| {
            m_error!(
                EC::NoSuchElement,
                "current worker local is not set and no default remote mududb addr is configured"
            )
        })?;
        let parser = Arc::new(SQLParser::new());
        let remote = Arc::new(RemoteWorkerConn {
            addr,
            worker_id: default_remote_worker_id(),
            session_id: AsyncMutex::new(None),
            stream: AsyncMutex::new(None),
        });
        Ok(Self {
            backend: ConnBackend::Remote(remote),
            parser,
            session_id: Arc::new(AsyncMutex::new(None)),
        })
    }

    fn parse_one(&self, sql: &dyn SQLStmt) -> RS<StmtType> {
        let stmt_list = self.parser.parse(&sql.to_sql_string())?;
        let mut stmts = stmt_list.into_stmts();
        if stmts.len() != 1 {
            return Err(m_error!(EC::ParseErr, "expected exactly one statement"));
        }
        Ok(stmts.remove(0))
    }

    async fn ensure_session_id(&self) -> RS<OID> {
        match &self.backend {
            ConnBackend::WorkerLocal(worker_local) => {
                let mut guard = self.session_id.lock().await;
                if let Some(session_id) = *guard {
                    return Ok(session_id);
                }
                let session_id = worker_local.open_async().await?;
                *guard = Some(session_id);
                Ok(session_id)
            }
            ConnBackend::Remote(remote) => remote.ensure_session_id().await,
        }
    }

    async fn active_session_id(&self) -> RS<OID> {
        match &self.backend {
            ConnBackend::WorkerLocal(_) => {
                let guard = self.session_id.lock().await;
                guard.ok_or_else(|| m_error!(EC::NoSuchElement, "no active session"))
            }
            ConnBackend::Remote(remote) => remote.active_session_id().await,
        }
    }
}

impl RemoteWorkerConn {
    async fn client(&self) -> RS<tokio::sync::MutexGuard<'_, Option<RemoteProtocolClient>>> {
        let mut guard = self.stream.lock().await;
        if guard.is_none() {
            *guard = Some(RemoteProtocolClient::connect(&self.addr).await?);
        }
        Ok(guard)
    }

    async fn ensure_session_id(&self) -> RS<OID> {
        let mut guard = self.session_id.lock().await;
        if let Some(session_id) = *guard {
            return Ok(session_id);
        }
        let mut client_guard = self.client().await?;
        let client = client_guard
            .as_mut()
            .ok_or_else(|| m_error!(EC::InternalErr, "remote worker client is missing"))?;
        let request_id = client.take_request_id();
        let config_json = self.worker_id.map(|worker_id| {
            serde_json::json!({
                "session_id": 0,
                "worker_id": worker_id.to_string()
            })
            .to_string()
        });
        let payload =
            encode_session_create_request(request_id, &SessionCreateRequest::new(config_json))?;
        let frame = client.send_and_receive(&payload).await?;
        let session_id = decode_session_create_response(&frame)?.session_id();
        *guard = Some(session_id);
        Ok(session_id)
    }

    async fn active_session_id(&self) -> RS<OID> {
        let guard = self.session_id.lock().await;
        guard.ok_or_else(|| m_error!(EC::NoSuchElement, "no active session"))
    }

    async fn batch_sql(&self, sql: String) -> RS<u64> {
        let _session_id = self.ensure_session_id().await?;
        let mut client_guard = self.client().await?;
        let client = client_guard
            .as_mut()
            .ok_or_else(|| m_error!(EC::InternalErr, "remote worker client is missing"))?;
        let payload = encode_batch_request(
            client.take_request_id(),
            &ClientRequest::new("default", sql),
        )?;
        let frame = client.send_and_receive(&payload).await?;
        Ok(decode_server_response(&frame)?.affected_rows())
    }

    async fn execute_sql(&self, sql: String) -> RS<u64> {
        let _session_id = self.ensure_session_id().await?;
        let mut client_guard = self.client().await?;
        let client = client_guard
            .as_mut()
            .ok_or_else(|| m_error!(EC::InternalErr, "remote worker client is missing"))?;
        let payload = encode_client_request_with_message_type(
            MessageType::Execute,
            client.take_request_id(),
            &ClientRequest::new("default", sql),
        )?;
        let frame = client.send_and_receive(&payload).await?;
        Ok(decode_server_response(&frame)?.affected_rows())
    }
}

impl RemoteProtocolClient {
    async fn connect(addr: &str) -> RS<Self> {
        let stream = TcpStream::connect(addr).await.map_err(|e| {
            m_error!(
                EC::NetErr,
                format!("connect io_uring tcp server error: addr={addr}"),
                e
            )
        })?;
        stream
            .set_nodelay(true)
            .map_err(|e| m_error!(EC::NetErr, format!("set tcp nodelay error: addr={addr}"), e))?;
        Ok(Self {
            stream,
            next_request_id: 1,
        })
    }

    fn take_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    async fn send_and_receive(&mut self, payload: &[u8]) -> RS<Frame> {
        self.stream
            .write_all(payload)
            .await
            .map_err(|e| m_error!(EC::NetErr, "write request frame error", e))?;
        self.stream
            .flush()
            .await
            .map_err(|e| m_error!(EC::NetErr, "flush request frame error", e))?;

        let mut header = [0u8; HEADER_LEN];
        self.stream
            .read_exact(&mut header)
            .await
            .map_err(|e| m_error!(EC::NetErr, "read response header error", e))?;
        let payload_len = FrameHeader::decode_header_bytes(&header)?.payload_len() as usize;
        let mut frame_bytes = Vec::with_capacity(HEADER_LEN + payload_len);
        frame_bytes.extend_from_slice(&header);
        if payload_len > 0 {
            let mut body = vec![0u8; payload_len];
            self.stream
                .read_exact(&mut body)
                .await
                .map_err(|e| m_error!(EC::NetErr, "read response payload error", e))?;
            frame_bytes.extend_from_slice(&body);
        }
        let frame = Frame::decode(&frame_bytes)?;
        if frame.header().message_type() == MessageType::Error {
            let error = decode_error_response(&frame)?;
            return Err(m_error!(EC::NetErr, error.message()));
        }
        Ok(frame)
    }
}

#[async_trait]
impl DBConnAsync for MuduConnAsync {
    async fn prepare(&self, stmt: Box<dyn SQLStmt>) -> RS<Arc<dyn PreparedStmt>> {
        match &self.backend {
            ConnBackend::WorkerLocal(worker_local) => {
                let parsed = self.parse_one(stmt.as_ref())?;
                let desc = Describer::describe(worker_local.meta_mgr().as_ref(), parsed).await?;
                Ok(Arc::new(MuduPreparedStmt::new(
                    worker_local.clone(),
                    self.session_id.clone(),
                    stmt,
                    Arc::new(desc),
                )))
            }
            ConnBackend::Remote(_) => Err(m_error!(
                EC::NotImplemented,
                "prepare is not supported without worker-local context"
            )),
        }
    }

    async fn exec_silent(&self, sql_text: String) -> RS<()> {
        match &self.backend {
            ConnBackend::WorkerLocal(worker_local) => {
                let session_id = self.ensure_session_id().await?;
                let _ = worker_local
                    .batch(session_id, Box::new(sql_text), Box::new(()))
                    .await?;
                Ok(())
            }
            ConnBackend::Remote(remote) => {
                let _ = remote.batch_sql(sql_text).await?;
                Ok(())
            }
        }
    }

    async fn begin_tx(&self) -> RS<XID> {
        let session_id = self.ensure_session_id().await?;
        match &self.backend {
            ConnBackend::WorkerLocal(worker_local) => {
                worker_local
                    .execute_async(session_id, WorkerExecute::BeginTx)
                    .await?;
                Ok(session_id)
            }
            ConnBackend::Remote(_) => Err(m_error!(
                EC::NotImplemented,
                "transaction control is not supported without worker-local context"
            )),
        }
    }

    async fn rollback_tx(&self) -> RS<()> {
        let session_id = self.active_session_id().await?;
        match &self.backend {
            ConnBackend::WorkerLocal(worker_local) => {
                worker_local
                    .execute_async(session_id, WorkerExecute::RollbackTx)
                    .await
            }
            ConnBackend::Remote(_) => Err(m_error!(
                EC::NotImplemented,
                "transaction control is not supported without worker-local context"
            )),
        }
    }

    async fn commit_tx(&self) -> RS<()> {
        let session_id = self.active_session_id().await?;
        match &self.backend {
            ConnBackend::WorkerLocal(worker_local) => {
                worker_local
                    .execute_async(session_id, WorkerExecute::CommitTx)
                    .await
            }
            ConnBackend::Remote(_) => Err(m_error!(
                EC::NotImplemented,
                "transaction control is not supported without worker-local context"
            )),
        }
    }

    async fn query(
        &self,
        sql: Box<dyn SQLStmt>,
        param: Box<dyn SQLParams>,
    ) -> RS<Arc<dyn ResultSetAsync>> {
        match &self.backend {
            ConnBackend::WorkerLocal(worker_local) => {
                let session_id = self.ensure_session_id().await?;
                worker_local.query(session_id, sql, param).await
            }
            ConnBackend::Remote(_) => Err(m_error!(
                EC::NotImplemented,
                "query is not supported without worker-local context"
            )),
        }
    }

    async fn execute(&self, sql: Box<dyn SQLStmt>, param: Box<dyn SQLParams>) -> RS<u64> {
        match &self.backend {
            ConnBackend::WorkerLocal(worker_local) => {
                let session_id = self.ensure_session_id().await?;
                worker_local.execute(session_id, sql, param).await
            }
            ConnBackend::Remote(remote) => {
                if param.size() != 0 {
                    return Err(m_error!(
                        EC::NotImplemented,
                        "execute with parameters is not supported without worker-local context"
                    ));
                }
                remote.execute_sql(sql.to_sql_string()).await
            }
        }
    }

    async fn batch(&self, sql: Box<dyn SQLStmt>, param: Box<dyn SQLParams>) -> RS<u64> {
        match &self.backend {
            ConnBackend::WorkerLocal(worker_local) => {
                let session_id = self.ensure_session_id().await?;
                worker_local.batch(session_id, sql, param).await
            }
            ConnBackend::Remote(remote) => {
                if param.size() != 0 {
                    return Err(m_error!(
                        EC::NotImplemented,
                        "batch with parameters is not supported without worker-local context"
                    ));
                }
                remote.batch_sql(sql.to_sql_string()).await
            }
        }
    }
}
