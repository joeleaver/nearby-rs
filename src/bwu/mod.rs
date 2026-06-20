//! Bandwidth-upgrade (BWU) subsystem — a test-first Rust port of Google
//! "Nearby" `connections/implementation/bwu_manager.cc` and its handler/channel
//! seams (Phase 2 of the port).
//!
//! Design (per the porting spec): `BwuManager` is a plain, synchronous, owned
//! state machine — no spawned tasks — so the 32-case `bwu_manager_test` oracle
//! stays deterministic; a Tokio actor wraps it only at the integration layer
//! (Phase 3). Channels are shared (`Arc<dyn EndpointChannel>`) with interior
//! mutability, mirroring the C++ `shared_ptr<EndpointChannel>`.
//!
//! Status: foundational seams (`EndpointChannel`, service-id wrapping) in place;
//! `ClientProxy` / `BwuHandler` / `BaseBwuHandler` / the fakes / `BwuManager`
//! are being ported incrementally.

#[cfg(feature = "tokio")]
pub mod actor;
pub mod channel;
pub mod channel_manager;
pub mod client;
pub mod handler;
pub mod manager;
pub mod service_id;
pub mod stream_channel;
pub mod testing;
pub mod wifi_lan;

#[cfg(feature = "tokio")]
pub use actor::{BwuActor, BwuCommand, BwuHandle};
pub use channel::{DisconnectionReason, EndpointChannel, SafeDisconnectionResult};
pub use channel_manager::EndpointChannelManager;
pub use client::ClientProxy;
pub use handler::{BaseBwuHandler, BwuHandler, IncomingSocketConnection, MediumBwuHandler};
pub use manager::{BwuConfig, BwuManager};
pub use stream_channel::{Cipher, DuplexStream, Pipe, StreamChannel};
pub use wifi_lan::{ConnectionSink, TcpDuplexStream, WifiLanBwuHandler};
