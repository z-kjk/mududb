use crate::server::message_bus_api::Envelope;
use crate::server::transferred_connection::TransferredConnection;

#[derive(Debug)]
pub(in crate::server) enum WorkerMailboxMsg {
    AdoptConnection(TransferredConnection),
    BusMessage(Envelope),
    Shutdown,
}
