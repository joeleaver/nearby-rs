//! The `EndpointChannel` transport seam + supporting enums.
//!
//! Ported from `connections/implementation/endpoint_channel.h` (the subset the
//! BWU state machine actually uses — see the porting spec) and the
//! `DisconnectionReason` enum from `proto/connections_enums.proto`.

use crate::frames::Exception;
use crate::mediums::Medium;

/// `location.nearby.proto.connections.DisconnectionReason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum DisconnectionReason {
    UnknownDisconnectionReason = 0,
    LocalDisconnection = 1,
    RemoteDisconnection = 2,
    IoError = 3,
    Upgraded = 4,
    Shutdown = 5,
    Unfinished = 6,
    PrevChannelDisconnectionInReconnect = 7,
    AuthenticationFailure = 8,
}

/// Result passed through unregister/close. Analytics-only: the BWU tests use it
/// purely as a call argument and never assert on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SafeDisconnectionResult {
    SafeDisconnection,
    UnsafeDisconnection,
}

/// The per-medium duplex channel the BWU state machine swaps over.
///
/// Methods take `&self` because channels are shared (`Arc`) and use interior
/// mutability — mirroring the C++ `shared_ptr<EndpointChannel>` whose
/// `Pause`/`Close`/`Write` mutate through a shared reference. Only the methods
/// the `BwuManager` actually calls are included; the analytics/keepalive/
/// encryption-context getters are intentionally omitted.
pub trait EndpointChannel: Send + Sync {
    /// `ExceptionOr<ByteArray> Read()`.
    fn read(&self) -> Result<Vec<u8>, Exception>;
    /// `Exception Write(string_view)`. Returns [`Exception::Success`] on success.
    fn write(&self, data: &[u8]) -> Exception;
    fn close(&self);
    fn close_with_reason(&self, reason: DisconnectionReason);
    fn medium(&self) -> Medium;
    fn service_id(&self) -> String;
    fn name(&self) -> String;
    fn channel_type(&self) -> String;
    fn local_endpoint_id(&self) -> String;
    fn set_local_endpoint_id(&self, local_endpoint_id: &str);
    fn pause(&self);
    fn resume(&self);
    fn is_paused(&self) -> bool;
    fn disable_encryption(&self);
}
