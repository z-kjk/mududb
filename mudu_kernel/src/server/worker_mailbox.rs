use crate::server::transferred_connection::TransferredConnection;

#[derive(Debug)]
pub(in crate::server) enum WorkerMailboxMsg {
    AdoptConnection(TransferredConnection),
    Shutdown,
}
