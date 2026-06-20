//! Canonical Nearby domain enums: [`Medium`] and [`WifiDirectAuthType`].
//!
//! Upstream these live in `proto/connections_enums.proto`
//! (`location.nearby.proto.connections`). Only these two are needed by
//! `offline_frames`, so rather than vendoring that whole proto we model them by
//! hand with values copied verbatim. They are the "domain" view of a medium;
//! the per-frame proto enums (`ConnectionRequestFrame::Medium`,
//! `UpgradePathInfo::Medium`, …) are mapped to/from these in
//! [`crate::frames`], exactly as `offline_frames.cc` does.

/// `location.nearby.proto.connections.Medium`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
pub enum Medium {
    UnknownMedium = 0,
    Mdns = 1,
    Bluetooth = 2,
    WifiHotspot = 3,
    Ble = 4,
    WifiLan = 5,
    WifiAware = 6,
    Nfc = 7,
    WifiDirect = 8,
    WebRtc = 9,
    BleL2cap = 10,
    Usb = 11,
    WebRtcNonCellular = 12,
    Awdl = 13,
}

/// `location.nearby.proto.connections.WifiDirectAuthType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
pub enum WifiDirectAuthType {
    WifiDirectTypeUnknown = 0,
    WifiDirectWithPassword = 1,
    /// Deprecated upstream, kept for value-fidelity.
    WifiDirectWithPin = 2,
    WifiDirectWithDeviceName = 3,
}
