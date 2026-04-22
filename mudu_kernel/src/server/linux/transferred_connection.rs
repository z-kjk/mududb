use crate::server::routing::{ConnectionTransfer, SessionOpenTransferAction};
use mudu::common::id::OID;
use std::os::fd::RawFd;

#[derive(Debug)]
pub(in crate::server) struct TransferredConnection {
    transfer: ConnectionTransfer,
    fd: RawFd,
    session_ids: Vec<OID>,
    session_open_action: Option<SessionOpenTransferAction>,
}

impl TransferredConnection {
    pub(in crate::server) fn new(
        transfer: ConnectionTransfer,
        fd: RawFd,
        session_ids: Vec<OID>,
        session_open_action: Option<SessionOpenTransferAction>,
    ) -> Self {
        Self {
            transfer,
            fd,
            session_ids,
            session_open_action,
        }
    }

    pub(in crate::server) fn transfer(&self) -> &ConnectionTransfer {
        &self.transfer
    }

    pub(in crate::server) fn fd(&self) -> RawFd {
        self.fd
    }

    pub(in crate::server) fn session_ids(&self) -> &[OID] {
        &self.session_ids
    }

    pub(in crate::server) fn session_open_action(&self) -> Option<SessionOpenTransferAction> {
        self.session_open_action
    }
}
