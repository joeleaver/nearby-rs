//! Generated prost types for the Nearby Connections wire format.
//!
//! These are compiled from the vendored `proto/offline_wire_formats.proto`
//! (Google "Nearby", Apache-2.0) by `build.rs`. The proto `package` is
//! `location.nearby.connections`, so prost emits a single module file of that
//! name into `OUT_DIR`, which we re-export here.
#![allow(clippy::all)]

include!(concat!(env!("OUT_DIR"), "/location.nearby.connections.rs"));
