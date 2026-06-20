//! # nearby-rs
//!
//! A faithful, test-first Rust port of Google's [Nearby Connections] protocol —
//! starting with the offline wire format (`offline_frames`) and its validator,
//! and building toward the bandwidth-upgrade (BWU) subsystem.
//!
//! The wire format types in [`proto`] are compiled from a verbatim copy of
//! Google's `offline_wire_formats.proto`, so this crate's encoding is pinned
//! against upstream by the golden tests under `tests/` (each test mirrors a
//! C++ `EqualsProto` assertion from `offline_frames_test.cc`).
//!
//! ## Layout
//! - [`proto`] — prost-generated wire types (`location.nearby.connections`).
//! - [`mediums`] — the canonical `Medium` / `WifiDirectAuthType` domain enums.
//! - [`frames`] — `offline_frames.cc`: builders, [`frames::from_bytes`],
//!   [`frames::get_frame_type`], and the medium conversions.
//! - [`validator`] — `offline_frames_validator.cc`:
//!   [`validator::ensure_valid_offline_frame`].
//!
//! [Nearby Connections]: https://github.com/google/nearby

pub mod bwu;
pub mod frames;
pub mod mediums;
pub mod proto;
pub mod validator;

pub use frames::{from_bytes, get_frame_type, ConnectionInfo, Exception, ServiceAddress};
pub use mediums::{Medium, WifiDirectAuthType};
pub use validator::ensure_valid_offline_frame;
