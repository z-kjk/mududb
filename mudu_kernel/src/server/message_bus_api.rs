use async_trait::async_trait;
use mudu::common::id::OID;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

pub type MessageId = u64;
pub type SubscriptionId = u64;
pub type MessageCallbackFuture = Pin<Box<dyn Future<Output = RS<()>> + 'static>>;
pub type OnRecvCallback = Arc<dyn Fn(Envelope) -> MessageCallbackFuture + 'static>;

thread_local! {
    static CURRENT_MESSAGE_BUS: UnsafeCell<Option<MessageBusRef>> =
        const { UnsafeCell::new(None) };
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum EndpointId {
    Worker(OID),
    External(u128),
    Session(OID),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeliveryMode {
    FireAndForget,
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SystemMessageKind {
    Ack,
    Nack,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MessageKind {
    User(u16),
    System(SystemMessageKind),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    msg_id: MessageId,
    correlation_id: Option<MessageId>,
    src: EndpointId,
    dst: EndpointId,
    kind: MessageKind,
    payload: Vec<u8>,
    delivery: DeliveryMode,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RecvFilter {
    pub src: Option<EndpointId>,
    pub dst: Option<EndpointId>,
    pub kind: Option<MessageKind>,
    pub correlation_id: Option<MessageId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutgoingMessage {
    kind: MessageKind,
    payload: Vec<u8>,
    correlation_id: Option<MessageId>,
    delivery: DeliveryMode,
}

#[async_trait(?Send)]
pub trait MessageBus {
    fn local_endpoint(&self) -> EndpointId;

    async fn send(&self, dst: EndpointId, message: OutgoingMessage) -> RS<MessageId>;

    async fn recv(&self, filter: RecvFilter) -> RS<Envelope>;

    fn on_recv_callback(&self, filter: RecvFilter, callback: OnRecvCallback) -> RS<SubscriptionId>;

    fn cancel_callback(&self, id: SubscriptionId) -> RS<bool>;
}

pub type MessageBusRef = Arc<dyn MessageBus>;

#[derive(Clone, Copy)]
struct MessageBusPtr(*const dyn MessageBus);

// Safety: the registry stores raw Arc pointers behind a mutex, holds one strong reference for
// each entry, and drops that reference on unregister.
unsafe impl Send for MessageBusPtr {}
unsafe impl Sync for MessageBusPtr {}

fn message_bus_registry() -> &'static Mutex<HashMap<OID, MessageBusPtr>> {
    static REGISTRY: OnceLock<Mutex<HashMap<OID, MessageBusPtr>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

impl Envelope {
    pub fn new(
        msg_id: MessageId,
        correlation_id: Option<MessageId>,
        src: EndpointId,
        dst: EndpointId,
        kind: MessageKind,
        payload: Vec<u8>,
        delivery: DeliveryMode,
    ) -> Self {
        Self {
            msg_id,
            correlation_id,
            src,
            dst,
            kind,
            payload,
            delivery,
        }
    }

    pub fn msg_id(&self) -> MessageId {
        self.msg_id
    }

    pub fn correlation_id(&self) -> Option<MessageId> {
        self.correlation_id
    }

    pub fn src(&self) -> &EndpointId {
        &self.src
    }

    pub fn dst(&self) -> &EndpointId {
        &self.dst
    }

    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn payload_owned(&self) -> Vec<u8> {
        self.payload.clone()
    }

    pub fn delivery(&self) -> DeliveryMode {
        self.delivery
    }

    pub fn matches(&self, filter: &RecvFilter) -> bool {
        filter.src.as_ref().is_none_or(|src| src == self.src())
            && filter.dst.as_ref().is_none_or(|dst| dst == self.dst())
            && filter.kind.is_none_or(|kind| kind == self.kind())
            && filter
                .correlation_id
                .is_none_or(|correlation_id| Some(correlation_id) == self.correlation_id())
    }
}

impl OutgoingMessage {
    pub fn new(kind: MessageKind, payload: Vec<u8>) -> Self {
        Self {
            kind,
            payload,
            correlation_id: None,
            delivery: DeliveryMode::FireAndForget,
        }
    }

    pub fn with_correlation_id(mut self, correlation_id: MessageId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_delivery(mut self, delivery: DeliveryMode) -> Self {
        self.delivery = delivery;
        self
    }

    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn payload_owned(&self) -> Vec<u8> {
        self.payload.clone()
    }

    pub fn correlation_id(&self) -> Option<MessageId> {
        self.correlation_id
    }

    pub fn delivery(&self) -> DeliveryMode {
        self.delivery
    }
}

pub(crate) fn set_current_message_bus(message_bus: MessageBusRef) {
    CURRENT_MESSAGE_BUS.with(|slot| {
        // Safety: the slot is thread-local and only mutated through these helpers.
        unsafe {
            *slot.get() = Some(message_bus);
        }
    });
}

pub(crate) fn unset_current_message_bus() {
    CURRENT_MESSAGE_BUS.with(|slot| {
        // Safety: the slot is thread-local and only mutated through these helpers.
        unsafe {
            *slot.get() = None;
        }
    });
}

#[allow(dead_code)]
pub(crate) fn current_message_bus() -> RS<MessageBusRef> {
    CURRENT_MESSAGE_BUS.with(|slot| {
        // Safety: shared reads are confined to the current thread-local slot.
        let message_bus = unsafe { &*slot.get() };
        message_bus
            .as_ref()
            .cloned()
            .ok_or_else(|| m_error!(EC::NoSuchElement, "current message bus is not set"))
    })
}

pub(crate) fn register_worker_message_bus(worker_id: OID, message_bus: &MessageBusRef) -> RS<()> {
    let raw = Arc::into_raw(message_bus.clone());
    let mut registry = message_bus_registry()
        .lock()
        .map_err(|_| m_error!(EC::InternalErr, "message bus registry lock poisoned"))?;
    if let Some(old) = registry.insert(worker_id, MessageBusPtr(raw)) {
        // Safety: the registry owns one strong ref per registered pointer.
        unsafe {
            drop(Arc::from_raw(old.0));
        }
    }
    Ok(())
}

pub(crate) fn unregister_worker_message_bus(worker_id: OID) -> RS<()> {
    let mut registry = message_bus_registry()
        .lock()
        .map_err(|_| m_error!(EC::InternalErr, "message bus registry lock poisoned"))?;
    let Some(raw) = registry.remove(&worker_id) else {
        return Ok(());
    };
    // Safety: the registry owns one strong ref per registered pointer.
    unsafe {
        drop(Arc::from_raw(raw.0));
    }
    Ok(())
}

pub(crate) fn message_bus_for_worker(worker_id: OID) -> RS<MessageBusRef> {
    let raw = {
        let registry = message_bus_registry()
            .lock()
            .map_err(|_| m_error!(EC::InternalErr, "message bus registry lock poisoned"))?;
        registry.get(&worker_id).copied().ok_or_else(|| {
            m_error!(
                EC::NoSuchElement,
                format!("message bus for worker {} is not registered", worker_id)
            )
        })?
    };
    // Safety: the registry entry came from `Arc::into_raw`; we temporarily bump the strong count
    // to materialize a cloned Arc for the caller.
    unsafe {
        Arc::increment_strong_count(raw.0);
        Ok(Arc::from_raw(raw.0))
    }
}
