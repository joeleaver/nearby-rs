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

pub mod channel;
pub mod client;
pub mod handler;
pub mod service_id;

pub use channel::{DisconnectionReason, EndpointChannel, SafeDisconnectionResult};
pub use client::ClientProxy;
pub use handler::{BaseBwuHandler, BwuHandler, IncomingSocketConnection, MediumBwuHandler};
