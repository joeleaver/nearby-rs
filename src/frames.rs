//! Serialize/deserialize Nearby Connections protocol messages.
//!
//! A faithful Rust port of Google "Nearby"
//! `connections/implementation/offline_frames.cc` (Apache-2.0). Each `for_*`
//! function builds and serializes one `OfflineFrame`; [`from_bytes`] parses and
//! validates one.
//!
//! ## Port notes
//! Google reads two process-global flags inside the builders. Because Rust
//! tests run in parallel within one process, a global would race, so the two
//! flag-gated behaviours are mapped to explicit parameters (the idiomatic
//! transcription of a flag):
//! - `kSafeToDisconnectVersion` → `safe_to_disconnect_version` arg of
//!   [`for_connection_response`].
//! - `kEnableDynamicRoleSwitch` → `enable_dynamic_role_switch` arg of
//!   [`for_connection_request_connections`].

use prost::Message;

use crate::mediums::{Medium, WifiDirectAuthType};
use crate::proto as pb;
use crate::validator::ensure_valid_offline_frame;

/// `connections/status.h`: `Status::kSuccess`.
const STATUS_SUCCESS: i32 = 0;

/// `internal_payload.h`: `InternalPayload::kIndeterminateSize`.
pub const INDETERMINATE_SIZE: i64 = -1;

/// Mirrors `nearby::Exception` (`internal/platform/exception.h`).
/// [`Exception::Success`] is the "no error" sentinel, matching the C++
/// `Exception{Exception::kSuccess}`. The parser/validator only ever produce
/// `Success`/`InvalidProtocolBuffer`/`IllegalCharacters`; the remaining
/// variants are used by the transport seam (e.g. an `EndpointChannel` write
/// returning `Io`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exception {
    Success,
    Failed,
    Io,
    Interrupted,
    InvalidProtocolBuffer,
    Execution,
    Timeout,
    IllegalCharacters,
}

impl Exception {
    /// Mirrors `ExceptionOr::ok()` / `Exception::Ok()`.
    pub fn ok(self) -> bool {
        matches!(self, Exception::Success)
    }
}

/// Platform-side network tuple (distinct from the proto `ServiceAddress`),
/// mirroring `internal/platform/service_address.h`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceAddress {
    /// 4 bytes (IPv4) or 16 bytes (IPv6), MSB-first.
    pub address: Vec<u8>,
    pub port: i32,
}

fn service_address_to_proto(addr: &ServiceAddress) -> pb::ServiceAddress {
    pb::ServiceAddress {
        ip_address: Some(addr.address.clone()),
        port: Some(addr.port),
    }
}

/// Mirrors `connections/connection_options.h`'s `ConnectionInfo` (the subset
/// `offline_frames.cc` reads).
#[derive(Clone, Debug, Default)]
pub struct ConnectionInfo {
    pub local_endpoint_id: String,
    pub local_endpoint_info: Vec<u8>,
    pub nonce: i32,
    pub supports_5ghz: bool,
    pub bssid: String,
    pub ap_frequency: i32,
    pub supported_mediums: Vec<Medium>,
    pub keep_alive_interval_millis: i32,
    pub keep_alive_timeout_millis: i32,
    pub medium_role: Option<pb::MediumRole>,
    pub supported_wifi_direct_auth_types: Vec<WifiDirectAuthType>,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parses an incoming message. Returns the `OfflineFrame` if it decodes and
/// validates, or an [`Exception`] otherwise (mirrors `FromBytes`).
pub fn from_bytes(bytes: &[u8]) -> Result<pb::OfflineFrame, Exception> {
    match pb::OfflineFrame::decode(bytes) {
        Ok(frame) => {
            let ex = ensure_valid_offline_frame(&frame);
            if ex.ok() {
                Ok(frame)
            } else {
                Err(ex)
            }
        }
        Err(_) => Err(Exception::InvalidProtocolBuffer),
    }
}

/// Returns the `FrameType` of a parsed message, or `UNKNOWN_FRAME_TYPE` if the
/// contents aren't recognized (mirrors `GetFrameType`).
pub fn get_frame_type(frame: &pb::OfflineFrame) -> pb::v1_frame::FrameType {
    if frame.version == Some(pb::offline_frame::Version::V1 as i32) {
        if let Some(v1) = &frame.v1 {
            return pb::v1_frame::FrameType::try_from(v1.r#type.unwrap_or(0))
                .unwrap_or(pb::v1_frame::FrameType::UnknownFrameType);
        }
    }
    pb::v1_frame::FrameType::UnknownFrameType
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

fn encode_v1<F: FnOnce(&mut pb::V1Frame)>(frame_type: pb::v1_frame::FrameType, fill: F) -> Vec<u8> {
    let mut v1 = pb::V1Frame {
        r#type: Some(frame_type as i32),
        ..Default::default()
    };
    fill(&mut v1);
    let frame = pb::OfflineFrame {
        version: Some(pb::offline_frame::Version::V1 as i32),
        v1: Some(v1),
    };
    let mut buf = Vec::with_capacity(frame.encoded_len());
    frame
        .encode(&mut buf)
        .expect("encoding an OfflineFrame into a Vec is infallible");
    buf
}

fn build_connection_request_common(connection_info: &ConnectionInfo) -> pb::ConnectionRequestFrame {
    let mut req = pb::ConnectionRequestFrame::default();
    if !connection_info.local_endpoint_info.is_empty() {
        // The C++ uses ByteArray::string_data() (raw bytes reinterpreted as a
        // string) for endpoint_name, and the same bytes for endpoint_info.
        req.endpoint_name =
            Some(String::from_utf8_lossy(&connection_info.local_endpoint_info).into_owned());
        req.endpoint_info = Some(connection_info.local_endpoint_info.clone());
    }
    req.nonce = Some(connection_info.nonce);

    let mut mm = pb::MediumMetadata {
        supports_5_ghz: Some(connection_info.supports_5ghz),
        ..Default::default()
    };
    if !connection_info.bssid.is_empty() {
        mm.bssid = Some(connection_info.bssid.clone());
    }
    mm.ap_frequency = Some(connection_info.ap_frequency);
    req.medium_metadata = Some(mm);

    if !connection_info.supported_mediums.is_empty() {
        for medium in &connection_info.supported_mediums {
            req.mediums
                .push(medium_to_connection_request_medium(*medium) as i32);
        }
    }
    if connection_info.keep_alive_interval_millis > 0 {
        req.keep_alive_interval_millis = Some(connection_info.keep_alive_interval_millis);
    }
    if connection_info.keep_alive_timeout_millis > 0 {
        req.keep_alive_timeout_millis = Some(connection_info.keep_alive_timeout_millis);
    }
    req
}

/// Builds a Connections `CONNECTION_REQUEST`.
pub fn for_connection_request_connections(
    proto_connections_device: Option<pb::ConnectionsDevice>,
    connection_info: &ConnectionInfo,
    enable_dynamic_role_switch: bool,
) -> Vec<u8> {
    let mut req = pb::ConnectionRequestFrame::default();
    if let Some(dev) = proto_connections_device {
        if dev.endpoint_id.is_some() {
            req.device = Some(pb::connection_request_frame::Device::ConnectionsDevice(dev));
        }
    }
    if !connection_info.local_endpoint_id.is_empty() {
        req.endpoint_id = Some(connection_info.local_endpoint_id.clone());
    }

    let common = build_connection_request_common(connection_info);
    req.endpoint_name = common.endpoint_name;
    req.endpoint_info = common.endpoint_info;
    req.nonce = common.nonce;
    let mut mm = common.medium_metadata.unwrap();
    if enable_dynamic_role_switch {
        if let Some(role) = &connection_info.medium_role {
            mm.medium_role = Some(*role);
        }
    }
    if !connection_info.supported_wifi_direct_auth_types.is_empty() {
        for auth_type in &connection_info.supported_wifi_direct_auth_types {
            mm.supported_wifi_direct_auth_types
                .push(wfd_auth_type_to_medium_metadata_wfd_auth_type(*auth_type) as i32);
        }
    }
    req.medium_metadata = Some(mm);
    req.mediums = common.mediums;
    req.keep_alive_interval_millis = common.keep_alive_interval_millis;
    req.keep_alive_timeout_millis = common.keep_alive_timeout_millis;

    encode_v1(pb::v1_frame::FrameType::ConnectionRequest, |v1| {
        v1.connection_request = Some(req);
    })
}

/// Builds a Presence `CONNECTION_REQUEST`.
pub fn for_connection_request_presence(
    proto_presence_device: pb::PresenceDevice,
    connection_info: &ConnectionInfo,
) -> Vec<u8> {
    let mut req = build_connection_request_common(connection_info);
    if !connection_info.local_endpoint_id.is_empty() {
        // C++ calls set_endpoint_id(proto_presence_device.endpoint_id()); the
        // proto2 accessor yields "" when unset and the setter marks the field
        // present, so an unset device endpoint_id still emits a present-but-empty
        // endpoint_id on the wire.
        req.endpoint_id = Some(
            proto_presence_device
                .endpoint_id
                .clone()
                .unwrap_or_default(),
        );
    }
    req.device = Some(pb::connection_request_frame::Device::PresenceDevice(
        proto_presence_device,
    ));
    encode_v1(pb::v1_frame::FrameType::ConnectionRequest, |v1| {
        v1.connection_request = Some(req);
    })
}

/// Builds a `CONNECTION_RESPONSE`.
///
/// For backward compatibility (and to match `offline_frames.cc`) the
/// deprecated `status` field is set alongside the `response` enum.
#[allow(deprecated)]
pub fn for_connection_response(
    status: i32,
    os_info: pb::OsInfo,
    safe_to_disconnect_version: i32,
) -> Vec<u8> {
    let response = if status == STATUS_SUCCESS {
        pb::connection_response_frame::ResponseStatus::Accept
    } else {
        pb::connection_response_frame::ResponseStatus::Reject
    };
    let sub = pb::ConnectionResponseFrame {
        status: Some(status),
        response: Some(response as i32),
        os_info: Some(os_info),
        multiplex_socket_bitmask: Some(0),
        safe_to_disconnect_version: Some(safe_to_disconnect_version),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::ConnectionResponse, |v1| {
        v1.connection_response = Some(sub);
    })
}

/// Builds a DATA `PAYLOAD_TRANSFER`.
pub fn for_data_payload_transfer(
    header: pb::payload_transfer_frame::PayloadHeader,
    chunk: pb::payload_transfer_frame::PayloadChunk,
) -> Vec<u8> {
    let sub = pb::PayloadTransferFrame {
        packet_type: Some(pb::payload_transfer_frame::PacketType::Data as i32),
        payload_header: Some(header),
        payload_chunk: Some(chunk),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::PayloadTransfer, |v1| {
        v1.payload_transfer = Some(sub);
    })
}

/// Builds a CONTROL `PAYLOAD_TRANSFER`.
pub fn for_control_payload_transfer(
    header: pb::payload_transfer_frame::PayloadHeader,
    control: pb::payload_transfer_frame::ControlMessage,
) -> Vec<u8> {
    let sub = pb::PayloadTransferFrame {
        packet_type: Some(pb::payload_transfer_frame::PacketType::Control as i32),
        payload_header: Some(header),
        control_message: Some(control),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::PayloadTransfer, |v1| {
        v1.payload_transfer = Some(sub);
    })
}

/// Builds a PAYLOAD_ACK `PAYLOAD_TRANSFER`.
pub fn for_payload_ack_payload_transfer(payload_id: i64) -> Vec<u8> {
    let header = pb::payload_transfer_frame::PayloadHeader {
        id: Some(payload_id),
        total_size: Some(INDETERMINATE_SIZE),
        ..Default::default()
    };
    let sub = pb::PayloadTransferFrame {
        packet_type: Some(pb::payload_transfer_frame::PacketType::PayloadAck as i32),
        payload_header: Some(header),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::PayloadTransfer, |v1| {
        v1.payload_transfer = Some(sub);
    })
}

fn bwu_upgrade_path_available(
    upgrade_path_info: pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo,
) -> Vec<u8> {
    let sub = pb::BandwidthUpgradeNegotiationFrame {
        event_type: Some(
            pb::bandwidth_upgrade_negotiation_frame::EventType::UpgradePathAvailable as i32,
        ),
        upgrade_path_info: Some(upgrade_path_info),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(sub);
    })
}

/// Builds a BWU `UPGRADE_PATH_AVAILABLE` for `WIFI_HOTSPOT`.
pub fn for_bwu_wifi_hotspot_path_available(
    credentials: pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiHotspotCredentials,
    supports_disabling_encryption: bool,
) -> Vec<u8> {
    let info = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
        medium: Some(
            pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium::WifiHotspot as i32,
        ),
        supports_client_introduction_ack: Some(true),
        supports_disabling_encryption: Some(supports_disabling_encryption),
        wifi_hotspot_credentials: Some(credentials),
        ..Default::default()
    };
    bwu_upgrade_path_available(info)
}

/// Builds a BWU `UPGRADE_PATH_AVAILABLE` for `WIFI_LAN`.
pub fn for_bwu_wifi_lan_path_available(addresses: &[ServiceAddress]) -> Vec<u8> {
    let mut socket =
        pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiLanSocket::default();
    if let Some(last_address) = addresses.last() {
        // For compatibility with Android versions, only use IPv4 address.
        // IPv4 addresses are always at the end of the list.
        if last_address.address.len() == 4 {
            socket.ip_address = Some(last_address.address.clone());
            socket.wifi_port = Some(last_address.port);
        }
        for address in addresses {
            socket
                .address_candidates
                .push(service_address_to_proto(address));
        }
    }
    let info = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
        medium: Some(
            pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium::WifiLan as i32,
        ),
        supports_client_introduction_ack: Some(true),
        wifi_lan_socket: Some(socket),
        ..Default::default()
    };
    bwu_upgrade_path_available(info)
}

/// Builds a BWU `UPGRADE_PATH_AVAILABLE` for `AWDL`.
pub fn for_bwu_awdl_path_available(
    service_name: &str,
    service_type: &str,
    password: &str,
    supports_disabling_encryption: bool,
) -> Vec<u8> {
    let creds = pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::AwdlCredentials {
        service_name: Some(service_name.to_owned()),
        service_type: Some(service_type.to_owned()),
        password: Some(password.to_owned()),
    };
    let info = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
        medium: Some(
            pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium::Awdl as i32,
        ),
        supports_client_introduction_ack: Some(true),
        supports_disabling_encryption: Some(supports_disabling_encryption),
        awdl_credentials: Some(creds),
        ..Default::default()
    };
    bwu_upgrade_path_available(info)
}

/// Builds a BWU `UPGRADE_PATH_AVAILABLE` for `WIFI_AWARE`.
pub fn for_bwu_wifi_aware_path_available(
    service_id: &str,
    service_info: &[u8],
    password: &str,
    supports_disabling_encryption: bool,
) -> Vec<u8> {
    let mut creds =
        pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiAwareCredentials {
            service_id: Some(service_id.to_owned()),
            service_info: Some(service_info.to_vec()),
            password: None,
        };
    if !password.is_empty() {
        creds.password = Some(password.to_owned());
    }
    let info = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
        medium: Some(
            pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium::WifiAware as i32,
        ),
        supports_client_introduction_ack: Some(true),
        supports_disabling_encryption: Some(supports_disabling_encryption),
        wifi_aware_credentials: Some(creds),
        ..Default::default()
    };
    bwu_upgrade_path_available(info)
}

/// Builds a BWU `UPGRADE_PATH_AVAILABLE` for `WIFI_DIRECT`.
#[allow(clippy::too_many_arguments)]
pub fn for_bwu_wifi_direct_path_available(
    ssid: &str,
    password: &str,
    port: i32,
    frequency: i32,
    supports_disabling_encryption: bool,
    gateway: &str,
    device_name: &str,
    pin: &str,
) -> Vec<u8> {
    let creds = pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiDirectCredentials {
        ssid: Some(ssid.to_owned()),
        password: Some(password.to_owned()),
        port: Some(port),
        frequency: Some(frequency),
        gateway: Some(gateway.to_owned()),
        device_name: Some(device_name.to_owned()),
        pin: Some(pin.to_owned()),
        ..Default::default()
    };
    let info = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
        medium: Some(
            pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium::WifiDirect as i32,
        ),
        supports_client_introduction_ack: Some(true),
        supports_disabling_encryption: Some(supports_disabling_encryption),
        wifi_direct_credentials: Some(creds),
        ..Default::default()
    };
    bwu_upgrade_path_available(info)
}

/// Builds a BWU `UPGRADE_PATH_AVAILABLE` for `BLUETOOTH`.
pub fn for_bwu_bluetooth_path_available(service_id: &str, mac_address: &str) -> Vec<u8> {
    let creds = pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::BluetoothCredentials {
        service_name: Some(service_id.to_owned()),
        mac_address: Some(mac_address.to_owned()),
    };
    let info = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
        medium: Some(
            pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium::Bluetooth as i32,
        ),
        supports_client_introduction_ack: Some(true),
        bluetooth_credentials: Some(creds),
        ..Default::default()
    };
    bwu_upgrade_path_available(info)
}

/// Builds a BWU `UPGRADE_PATH_AVAILABLE` for `WEB_RTC`.
pub fn for_bwu_webrtc_path_available(peer_id: &str, location_hint: pb::LocationHint) -> Vec<u8> {
    let creds = pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WebRtcCredentials {
        peer_id: Some(peer_id.to_owned()),
        location_hint: Some(location_hint),
    };
    let info = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
        medium: Some(
            pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium::WebRtc as i32,
        ),
        supports_client_introduction_ack: Some(true),
        web_rtc_credentials: Some(creds),
        ..Default::default()
    };
    bwu_upgrade_path_available(info)
}

/// Builds a BWU `UPGRADE_FAILURE` carrying the failed upgrade path.
pub fn for_bwu_failure(info: pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo) -> Vec<u8> {
    let sub = pb::BandwidthUpgradeNegotiationFrame {
        event_type: Some(pb::bandwidth_upgrade_negotiation_frame::EventType::UpgradeFailure as i32),
        upgrade_path_info: Some(info),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(sub);
    })
}

/// Builds a BWU `LAST_WRITE_TO_PRIOR_CHANNEL`.
pub fn for_bwu_last_write() -> Vec<u8> {
    let sub = pb::BandwidthUpgradeNegotiationFrame {
        event_type: Some(
            pb::bandwidth_upgrade_negotiation_frame::EventType::LastWriteToPriorChannel as i32,
        ),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(sub);
    })
}

/// Builds a BWU `SAFE_TO_CLOSE_PRIOR_CHANNEL`.
pub fn for_bwu_safe_to_close() -> Vec<u8> {
    let sub = pb::BandwidthUpgradeNegotiationFrame {
        event_type: Some(
            pb::bandwidth_upgrade_negotiation_frame::EventType::SafeToClosePriorChannel as i32,
        ),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(sub);
    })
}

/// Builds a BWU `CLIENT_INTRODUCTION`.
pub fn for_bwu_introduction(
    endpoint_id: &str,
    last_endpoint_id: &str,
    supports_disabling_encryption: bool,
) -> Vec<u8> {
    let mut intro = pb::bandwidth_upgrade_negotiation_frame::ClientIntroduction {
        endpoint_id: Some(endpoint_id.to_owned()),
        supports_disabling_encryption: Some(supports_disabling_encryption),
        last_endpoint_id: None,
    };
    if !last_endpoint_id.is_empty() {
        intro.last_endpoint_id = Some(last_endpoint_id.to_owned());
    }
    let sub = pb::BandwidthUpgradeNegotiationFrame {
        event_type: Some(
            pb::bandwidth_upgrade_negotiation_frame::EventType::ClientIntroduction as i32,
        ),
        client_introduction: Some(intro),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(sub);
    })
}

/// Builds a BWU `CLIENT_INTRODUCTION_ACK`.
pub fn for_bwu_introduction_ack() -> Vec<u8> {
    let sub = pb::BandwidthUpgradeNegotiationFrame {
        event_type: Some(
            pb::bandwidth_upgrade_negotiation_frame::EventType::ClientIntroductionAck as i32,
        ),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(sub);
    })
}

/// Builds a BWU `UPGRADE_PATH_REQUEST`.
pub fn for_bwu_path_request(
    medium: Medium,
    mediums: &[Medium],
    medium_role: pb::MediumRole,
) -> Vec<u8> {
    let mut request =
        pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::UpgradePathRequest {
            mediums: mediums
                .iter()
                .map(|m| medium_to_upgrade_path_info_medium(*m) as i32)
                .collect(),
            medium_meta_data: Some(pb::MediumMetadata::default()),
        };
    request.medium_meta_data.as_mut().unwrap().medium_role = Some(medium_role);
    let info = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
        medium: Some(medium_to_upgrade_path_info_medium(medium) as i32),
        upgrade_path_request: Some(request),
        ..Default::default()
    };
    let sub = pb::BandwidthUpgradeNegotiationFrame {
        event_type: Some(
            pb::bandwidth_upgrade_negotiation_frame::EventType::UpgradePathRequest as i32,
        ),
        upgrade_path_info: Some(info),
        ..Default::default()
    };
    encode_v1(pb::v1_frame::FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(sub);
    })
}

/// Builds an empty `KEEP_ALIVE`.
pub fn for_keep_alive() -> Vec<u8> {
    encode_v1(pb::v1_frame::FrameType::KeepAlive, |v1| {
        v1.keep_alive = Some(pb::KeepAliveFrame::default());
    })
}

/// Builds a `KEEP_ALIVE` carrying an ack flag and sequence number.
pub fn for_keep_alive_ack(ack: bool, seq_num: u32) -> Vec<u8> {
    encode_v1(pb::v1_frame::FrameType::KeepAlive, |v1| {
        v1.keep_alive = Some(pb::KeepAliveFrame {
            ack: Some(ack),
            seq_num: Some(seq_num),
        });
    })
}

/// Builds a `DISCONNECTION`.
pub fn for_disconnection(
    request_safe_to_disconnect: bool,
    ack_safe_to_disconnect: bool,
) -> Vec<u8> {
    encode_v1(pb::v1_frame::FrameType::Disconnection, |v1| {
        v1.disconnection = Some(pb::DisconnectionFrame {
            request_safe_to_disconnect: Some(request_safe_to_disconnect),
            ack_safe_to_disconnect: Some(ack_safe_to_disconnect),
        });
    })
}

// ---------------------------------------------------------------------------
// Medium <-> per-frame-enum conversions
// ---------------------------------------------------------------------------

/// `Medium` -> `ConnectionRequestFrame::Medium`.
pub fn medium_to_connection_request_medium(medium: Medium) -> pb::connection_request_frame::Medium {
    use pb::connection_request_frame::Medium as Out;
    match medium {
        Medium::Mdns => Out::Mdns,
        Medium::Bluetooth => Out::Bluetooth,
        Medium::WifiHotspot => Out::WifiHotspot,
        Medium::Ble => Out::Ble,
        Medium::BleL2cap => Out::BleL2cap,
        Medium::WifiLan => Out::WifiLan,
        Medium::WifiAware => Out::WifiAware,
        Medium::Nfc => Out::Nfc,
        Medium::WifiDirect => Out::WifiDirect,
        Medium::WebRtc => Out::WebRtc,
        Medium::WebRtcNonCellular => Out::WebRtcNonCellular,
        Medium::Usb => Out::Usb,
        Medium::Awdl => Out::Awdl,
        Medium::UnknownMedium => Out::UnknownMedium,
    }
}

/// `ConnectionRequestFrame::Medium` -> `Medium`.
pub fn connection_request_medium_to_medium(medium: pb::connection_request_frame::Medium) -> Medium {
    use pb::connection_request_frame::Medium as In;
    match medium {
        In::Mdns => Medium::Mdns,
        In::Bluetooth => Medium::Bluetooth,
        In::WifiHotspot => Medium::WifiHotspot,
        In::Ble => Medium::Ble,
        In::BleL2cap => Medium::BleL2cap,
        In::WifiLan => Medium::WifiLan,
        In::WifiAware => Medium::WifiAware,
        In::Nfc => Medium::Nfc,
        In::WifiDirect => Medium::WifiDirect,
        In::WebRtc => Medium::WebRtc,
        In::WebRtcNonCellular => Medium::WebRtcNonCellular,
        In::Usb => Medium::Usb,
        In::Awdl => Medium::Awdl,
        In::UnknownMedium => Medium::UnknownMedium,
    }
}

/// `Medium` -> `UpgradePathInfo::Medium`. Note `UpgradePathInfo::Medium` has no
/// `BLE_L2CAP` (10 reserved), so `BleL2cap` maps to `UNKNOWN_MEDIUM` (as the C++
/// `default:` branch does).
pub fn medium_to_upgrade_path_info_medium(
    medium: Medium,
) -> pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium {
    use pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium as Out;
    match medium {
        Medium::Mdns => Out::Mdns,
        Medium::Bluetooth => Out::Bluetooth,
        Medium::WifiHotspot => Out::WifiHotspot,
        Medium::Ble => Out::Ble,
        Medium::WifiLan => Out::WifiLan,
        Medium::WifiAware => Out::WifiAware,
        Medium::Nfc => Out::Nfc,
        Medium::WifiDirect => Out::WifiDirect,
        Medium::WebRtc => Out::WebRtc,
        Medium::WebRtcNonCellular => Out::WebRtcNonCellular,
        Medium::Usb => Out::Usb,
        Medium::Awdl => Out::Awdl,
        Medium::BleL2cap | Medium::UnknownMedium => Out::UnknownMedium,
    }
}

/// `UpgradePathInfo::Medium` -> `Medium`.
pub fn upgrade_path_info_medium_to_medium(
    medium: pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium,
) -> Medium {
    use pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium as In;
    match medium {
        In::Mdns => Medium::Mdns,
        In::Bluetooth => Medium::Bluetooth,
        In::WifiHotspot => Medium::WifiHotspot,
        In::Ble => Medium::Ble,
        In::WifiLan => Medium::WifiLan,
        In::WifiAware => Medium::WifiAware,
        In::Nfc => Medium::Nfc,
        In::WifiDirect => Medium::WifiDirect,
        In::WebRtc => Medium::WebRtc,
        In::WebRtcNonCellular => Medium::WebRtcNonCellular,
        In::Usb => Medium::Usb,
        In::Awdl => Medium::Awdl,
        In::UnknownMedium => Medium::UnknownMedium,
    }
}

/// Maps a `ConnectionRequestFrame`'s repeated `mediums` to a `Vec<Medium>`.
pub fn connection_request_mediums_to_mediums(frame: &pb::ConnectionRequestFrame) -> Vec<Medium> {
    frame
        .mediums
        .iter()
        .map(|&int_medium| {
            let m = pb::connection_request_frame::Medium::try_from(int_medium)
                .unwrap_or(pb::connection_request_frame::Medium::UnknownMedium);
            connection_request_medium_to_medium(m)
        })
        .collect()
}

/// `WifiDirectAuthType` -> `MediumMetadata::WifiDirectAuthType`.
pub fn wfd_auth_type_to_medium_metadata_wfd_auth_type(
    wifi_direct_auth_type: WifiDirectAuthType,
) -> pb::medium_metadata::WifiDirectAuthType {
    use pb::medium_metadata::WifiDirectAuthType as Out;
    match wifi_direct_auth_type {
        WifiDirectAuthType::WifiDirectWithPassword => Out::WifiDirectWithPassword,
        WifiDirectAuthType::WifiDirectWithDeviceName => Out::WifiDirectWithDeviceName,
        _ => Out::WifiDirectTypeUnknown,
    }
}

/// `MediumMetadata::WifiDirectAuthType` -> `WifiDirectAuthType`.
pub fn medium_metadata_wfd_auth_type_to_wfd_auth_type(
    wifi_direct_auth_type: pb::medium_metadata::WifiDirectAuthType,
) -> WifiDirectAuthType {
    use pb::medium_metadata::WifiDirectAuthType as In;
    match wifi_direct_auth_type {
        In::WifiDirectWithPassword => WifiDirectAuthType::WifiDirectWithPassword,
        In::WifiDirectWithDeviceName => WifiDirectAuthType::WifiDirectWithDeviceName,
        _ => WifiDirectAuthType::WifiDirectTypeUnknown,
    }
}

/// Maps a `MediumMetadata`'s repeated `supported_wifi_direct_auth_types`.
pub fn medium_metadata_wfd_auth_types_to_wfd_auth_types(
    medium_metadata: &pb::MediumMetadata,
) -> Vec<WifiDirectAuthType> {
    medium_metadata
        .supported_wifi_direct_auth_types
        .iter()
        .map(|&int_type| {
            let t = pb::medium_metadata::WifiDirectAuthType::try_from(int_type)
                .unwrap_or(pb::medium_metadata::WifiDirectAuthType::WifiDirectTypeUnknown);
            medium_metadata_wfd_auth_type_to_wfd_auth_type(t)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_MEDIUMS: [Medium; 14] = [
        Medium::UnknownMedium,
        Medium::Mdns,
        Medium::Bluetooth,
        Medium::WifiHotspot,
        Medium::Ble,
        Medium::WifiLan,
        Medium::WifiAware,
        Medium::Nfc,
        Medium::WifiDirect,
        Medium::WebRtc,
        Medium::BleL2cap,
        Medium::Usb,
        Medium::WebRtcNonCellular,
        Medium::Awdl,
    ];

    #[test]
    fn connection_request_medium_round_trips() {
        for m in ALL_MEDIUMS {
            assert_eq!(
                connection_request_medium_to_medium(medium_to_connection_request_medium(m)),
                m
            );
        }
    }

    #[test]
    fn presence_request_emits_present_empty_endpoint_id_when_device_has_none() {
        // C++ ForConnectionRequestPresence sets endpoint_id from the device's
        // accessor (which yields "") whenever local_endpoint_id is non-empty, so
        // the field must be present-but-empty even when the device omits it.
        let info = ConnectionInfo {
            local_endpoint_id: "ABC".into(),
            local_endpoint_info: b"X".to_vec(),
            ..Default::default()
        };
        let device = pb::PresenceDevice {
            endpoint_id: None,
            ..Default::default()
        };
        let frame =
            pb::OfflineFrame::decode(&for_connection_request_presence(device, &info)[..]).unwrap();
        let req = frame.v1.unwrap().connection_request.unwrap();
        assert_eq!(req.endpoint_id, Some(String::new()));
    }

    #[test]
    fn upgrade_path_info_medium_round_trips() {
        use pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium as Upi;
        for m in ALL_MEDIUMS {
            // UpgradePathInfo::Medium has no BLE_L2CAP (10 reserved), so it
            // collapses to UNKNOWN_MEDIUM (matching the C++ default branch).
            if m == Medium::BleL2cap {
                assert_eq!(medium_to_upgrade_path_info_medium(m), Upi::UnknownMedium);
                continue;
            }
            assert_eq!(
                upgrade_path_info_medium_to_medium(medium_to_upgrade_path_info_medium(m)),
                m
            );
        }
    }
}
