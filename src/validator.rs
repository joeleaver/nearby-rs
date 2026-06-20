//! Validates parsed `OfflineFrame`s.
//!
//! A faithful Rust port of Google "Nearby"
//! `connections/implementation/offline_frames_validator.cc` (Apache-2.0).
//! [`ensure_valid_offline_frame`] is called by [`crate::frames::from_bytes`]
//! after decoding, exactly as `FromBytes` calls `EnsureValidOfflineFrame`.
//!
//! Field presence/value access is done explicitly on the prost `Option` fields
//! (rather than via generated accessors) so proto2 default semantics are
//! reproduced verbatim — notably `WifiHotspotCredentials.gateway` defaulting to
//! `"0.0.0.0"` when unset.

use crate::frames::{get_frame_type, Exception, INDETERMINATE_SIZE};
use crate::proto as pb;

const ILLEGAL_FILE_NAME_PATTERNS: &[&str] = &[":", "/", "\\"];
const ILLEGAL_PARENT_FOLDER_PATTERNS: &[&str] = &[":", ".."];

const WIFI_DIRECT_SSID_MAX_LENGTH: usize = 32;
const WIFI_PASSWORD_SSID_MIN_LENGTH: usize = 8;
const WIFI_PASSWORD_SSID_MAX_LENGTH: usize = 64;
const WIFI_DIRECT_PIN_MIN_LENGTH: usize = 0;
const WIFI_DIRECT_PIN_MAX_LENGTH: usize = 16;

/// `min <= value < max` (mirrors `WithinRange`).
fn within_range(value: usize, min: usize, max: usize) -> bool {
    value >= min && value < max
}

/// Mirrors `kIpv4PatternString`: four dot-separated octets, each 0-255.
fn is_valid_ipv4(s: &str) -> bool {
    let mut parts = 0;
    for octet in s.split('.') {
        parts += 1;
        if parts > 4 {
            return false;
        }
        if octet.is_empty()
            || octet.len() > 3
            || !octet.bytes().all(|b| b.is_ascii_digit())
            || octet.parse::<u16>().map(|n| n > 255).unwrap_or(true)
        {
            return false;
        }
    }
    parts == 4
}

/// Mirrors `kWifiDirectSsidPatternString`: `^DIRECT-[a-zA-Z0-9]{2}.*$`.
fn matches_wifi_direct_ssid(s: &str) -> bool {
    let Some(rest) = s.strip_prefix("DIRECT-") else {
        return false;
    };
    let mut chars = rest.chars();
    match (chars.next(), chars.next()) {
        (Some(a), Some(b)) => a.is_ascii_alphanumeric() && b.is_ascii_alphanumeric(),
        _ => false,
    }
}

fn has_illegal_characters(to_validate: &str, illegal_patterns: &[&str]) -> bool {
    if to_validate.is_empty() {
        return false;
    }
    illegal_patterns
        .iter()
        .any(|pat| to_validate.contains(pat))
}

fn ensure_valid_connection_request_frame(frame: &pb::ConnectionRequestFrame) -> Exception {
    if frame.endpoint_id.as_deref().unwrap_or("").is_empty() {
        return Exception::InvalidProtocolBuffer;
    }
    if frame.endpoint_name.as_deref().unwrap_or("").is_empty() {
        return Exception::InvalidProtocolBuffer;
    }
    Exception::Success
}

fn ensure_valid_connection_response_frame(_frame: &pb::ConnectionResponseFrame) -> Exception {
    // For backwards compatibility no fields are null-checked.
    Exception::Success
}

fn ensure_valid_payload_transfer_data_frame(
    payload_chunk: &pb::payload_transfer_frame::PayloadChunk,
    total_size: i64,
) -> Exception {
    if payload_chunk.flags.is_none() {
        return Exception::InvalidProtocolBuffer;
    }
    let last_chunk_flag =
        pb::payload_transfer_frame::payload_chunk::Flags::LastChunk as i32;
    let is_last_chunk = (payload_chunk.flags.unwrap_or(0) & last_chunk_flag) != 0;
    if payload_chunk.body.is_none() && !is_last_chunk {
        return Exception::InvalidProtocolBuffer;
    }
    let offset = payload_chunk.offset.unwrap_or(0);
    if payload_chunk.offset.is_none() || offset < 0 {
        return Exception::InvalidProtocolBuffer;
    }
    if total_size != INDETERMINATE_SIZE && total_size < offset {
        return Exception::InvalidProtocolBuffer;
    }
    Exception::Success
}

fn ensure_valid_payload_transfer_control_frame(
    control_message: &pb::payload_transfer_frame::ControlMessage,
    total_size: i64,
) -> Exception {
    let offset = control_message.offset.unwrap_or(0);
    if control_message.offset.is_none() || offset < 0 {
        return Exception::InvalidProtocolBuffer;
    }
    if total_size != INDETERMINATE_SIZE && total_size < offset {
        return Exception::InvalidProtocolBuffer;
    }
    Exception::Success
}

fn ensure_valid_payload_transfer_frame(frame: &pb::PayloadTransferFrame) -> Exception {
    let header = match &frame.payload_header {
        Some(h) => h,
        None => return Exception::InvalidProtocolBuffer,
    };
    let packet_type = frame
        .packet_type
        .and_then(|v| pb::payload_transfer_frame::PacketType::try_from(v).ok())
        .unwrap_or(pb::payload_transfer_frame::PacketType::UnknownPacketType);
    if packet_type == pb::payload_transfer_frame::PacketType::PayloadAck {
        // Phone side doesn't set total_size for PAYLOAD_ACK, so skip checking it.
        return Exception::Success;
    }
    let total_size = header.total_size.unwrap_or(0);
    if header.total_size.is_none() || (total_size < 0 && total_size != INDETERMINATE_SIZE) {
        return Exception::InvalidProtocolBuffer;
    }
    if header.r#type == Some(pb::payload_transfer_frame::payload_header::PayloadType::File as i32) {
        if let Some(file_name) = &header.file_name {
            if has_illegal_characters(file_name, ILLEGAL_FILE_NAME_PATTERNS) {
                return Exception::IllegalCharacters;
            }
        }
        if let Some(parent_folder) = &header.parent_folder {
            if has_illegal_characters(parent_folder, ILLEGAL_PARENT_FOLDER_PATTERNS) {
                return Exception::IllegalCharacters;
            }
        }
    }
    if frame.packet_type.is_none() {
        return Exception::InvalidProtocolBuffer;
    }
    match packet_type {
        pb::payload_transfer_frame::PacketType::Data => {
            if let Some(chunk) = &frame.payload_chunk {
                return ensure_valid_payload_transfer_data_frame(chunk, total_size);
            }
            Exception::InvalidProtocolBuffer
        }
        pb::payload_transfer_frame::PacketType::Control => {
            if let Some(control) = &frame.control_message {
                return ensure_valid_payload_transfer_control_frame(control, total_size);
            }
            Exception::InvalidProtocolBuffer
        }
        _ => Exception::Success,
    }
}

fn ensure_valid_bwu_wifi_hotspot_path_available(
    creds: &pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiHotspotCredentials,
) -> Exception {
    if creds.ssid.is_none() {
        return Exception::InvalidProtocolBuffer;
    }
    match &creds.password {
        None => return Exception::InvalidProtocolBuffer,
        Some(p) => {
            if !within_range(p.len(), WIFI_PASSWORD_SSID_MIN_LENGTH, WIFI_PASSWORD_SSID_MAX_LENGTH)
            {
                return Exception::InvalidProtocolBuffer;
            }
        }
    }
    if creds.gateway.is_none() && creds.address_candidates.is_empty() {
        return Exception::InvalidProtocolBuffer;
    }
    // proto2 default for gateway is "0.0.0.0".
    let gateway = creds.gateway.as_deref().unwrap_or("0.0.0.0");
    if !gateway.is_empty() && !is_valid_ipv4(gateway) {
        return Exception::InvalidProtocolBuffer;
    }
    for candidate in &creds.address_candidates {
        if candidate.ip_address.is_none() || candidate.port.is_none() {
            return Exception::InvalidProtocolBuffer;
        }
        let len = candidate.ip_address.as_ref().map_or(0, |v| v.len());
        if len != 4 && len != 16 {
            return Exception::InvalidProtocolBuffer;
        }
    }
    Exception::Success
}

fn ensure_valid_bwu_wifi_lan_path_available(
    socket: &pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiLanSocket,
) -> Exception {
    let wifi_port = socket.wifi_port.unwrap_or(0);
    if (socket.ip_address.is_none() || wifi_port <= 0) && socket.address_candidates.is_empty() {
        return Exception::InvalidProtocolBuffer;
    }
    Exception::Success
}

fn ensure_valid_bwu_wifi_aware_path_available(
    creds: &pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiAwareCredentials,
) -> Exception {
    if creds.service_id.is_none() {
        return Exception::InvalidProtocolBuffer;
    }
    if creds.service_info.is_none() {
        return Exception::InvalidProtocolBuffer;
    }
    Exception::Success
}

fn ensure_valid_bwu_wifi_direct_path_available(
    creds: &pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiDirectCredentials,
) -> Exception {
    if creds.frequency.map_or(true, |f| f < -1) {
        return Exception::InvalidProtocolBuffer;
    }
    let ssid_valid = match &creds.ssid {
        Some(ssid) => ssid.len() < WIFI_DIRECT_SSID_MAX_LENGTH && matches_wifi_direct_ssid(ssid),
        None => false,
    };
    let password_valid = match &creds.password {
        Some(p) => within_range(p.len(), WIFI_PASSWORD_SSID_MIN_LENGTH, WIFI_PASSWORD_SSID_MAX_LENGTH),
        None => false,
    };
    let device_name_valid = match &creds.device_name {
        Some(d) => d.len() < WIFI_DIRECT_SSID_MAX_LENGTH,
        None => false,
    };
    let pin_valid = match &creds.pin {
        Some(p) => within_range(p.len(), WIFI_DIRECT_PIN_MIN_LENGTH, WIFI_DIRECT_PIN_MAX_LENGTH),
        None => false,
    };
    if (ssid_valid && password_valid) || (device_name_valid && pin_valid) {
        return Exception::Success;
    }
    Exception::InvalidProtocolBuffer
}

fn ensure_valid_bwu_bluetooth_path_available(
    creds: &pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::BluetoothCredentials,
) -> Exception {
    if creds.service_name.is_none() {
        return Exception::InvalidProtocolBuffer;
    }
    if creds.mac_address.is_none() {
        return Exception::InvalidProtocolBuffer;
    }
    Exception::Success
}

fn ensure_valid_bwu_web_rtc_path_available(
    creds: &pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WebRtcCredentials,
) -> Exception {
    if creds.peer_id.is_none() {
        return Exception::InvalidProtocolBuffer;
    }
    Exception::Success
}

fn ensure_valid_bwu_path_available_frame(
    upgrade_path_info: &pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo,
) -> Exception {
    use pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium;
    let medium = match upgrade_path_info
        .medium
        .and_then(|v| Medium::try_from(v).ok())
    {
        Some(m) => m,
        None => return Exception::InvalidProtocolBuffer,
    };
    match medium {
        Medium::WifiHotspot => match &upgrade_path_info.wifi_hotspot_credentials {
            Some(c) => ensure_valid_bwu_wifi_hotspot_path_available(c),
            None => Exception::InvalidProtocolBuffer,
        },
        Medium::WifiLan => match &upgrade_path_info.wifi_lan_socket {
            Some(s) => ensure_valid_bwu_wifi_lan_path_available(s),
            None => Exception::InvalidProtocolBuffer,
        },
        Medium::WifiAware => match &upgrade_path_info.wifi_aware_credentials {
            Some(c) => ensure_valid_bwu_wifi_aware_path_available(c),
            None => Exception::InvalidProtocolBuffer,
        },
        Medium::WifiDirect => match &upgrade_path_info.wifi_direct_credentials {
            Some(c) => ensure_valid_bwu_wifi_direct_path_available(c),
            None => Exception::InvalidProtocolBuffer,
        },
        Medium::Bluetooth => match &upgrade_path_info.bluetooth_credentials {
            Some(c) => ensure_valid_bwu_bluetooth_path_available(c),
            None => Exception::InvalidProtocolBuffer,
        },
        Medium::WebRtc => match &upgrade_path_info.web_rtc_credentials {
            Some(c) => ensure_valid_bwu_web_rtc_path_available(c),
            None => Exception::InvalidProtocolBuffer,
        },
        _ => Exception::Success,
    }
}

fn ensure_valid_bwu_client_introduction_frame(
    client_introduction: &pb::bandwidth_upgrade_negotiation_frame::ClientIntroduction,
) -> Exception {
    if client_introduction.endpoint_id.is_none() {
        return Exception::InvalidProtocolBuffer;
    }
    Exception::Success
}

fn ensure_valid_bwu_negotiation_frame(
    frame: &pb::BandwidthUpgradeNegotiationFrame,
) -> Exception {
    use pb::bandwidth_upgrade_negotiation_frame::EventType;
    let event_type = match frame.event_type.and_then(|v| EventType::try_from(v).ok()) {
        Some(e) => e,
        None => return Exception::InvalidProtocolBuffer,
    };
    match event_type {
        EventType::UpgradePathAvailable => match &frame.upgrade_path_info {
            Some(info) => ensure_valid_bwu_path_available_frame(info),
            None => Exception::InvalidProtocolBuffer,
        },
        EventType::ClientIntroduction => match &frame.client_introduction {
            Some(intro) => ensure_valid_bwu_client_introduction_frame(intro),
            None => Exception::InvalidProtocolBuffer,
        },
        _ => Exception::Success,
    }
}

/// Validates a fully-parsed `OfflineFrame` (mirrors `EnsureValidOfflineFrame`).
pub fn ensure_valid_offline_frame(offline_frame: &pb::OfflineFrame) -> Exception {
    let frame_type = get_frame_type(offline_frame);
    let v1 = offline_frame.v1.as_ref();
    match frame_type {
        pb::v1_frame::FrameType::ConnectionRequest => {
            match v1.and_then(|v| v.connection_request.as_ref()) {
                Some(req) => ensure_valid_connection_request_frame(req),
                None => Exception::InvalidProtocolBuffer,
            }
        }
        pb::v1_frame::FrameType::ConnectionResponse => {
            match v1.and_then(|v| v.connection_response.as_ref()) {
                Some(resp) => ensure_valid_connection_response_frame(resp),
                None => Exception::InvalidProtocolBuffer,
            }
        }
        pb::v1_frame::FrameType::PayloadTransfer => {
            match v1.and_then(|v| v.payload_transfer.as_ref()) {
                Some(pt) => ensure_valid_payload_transfer_frame(pt),
                None => Exception::InvalidProtocolBuffer,
            }
        }
        pb::v1_frame::FrameType::BandwidthUpgradeNegotiation => {
            match v1.and_then(|v| v.bandwidth_upgrade_negotiation.as_ref()) {
                Some(bwu) => ensure_valid_bwu_negotiation_frame(bwu),
                None => Exception::InvalidProtocolBuffer,
            }
        }
        // KEEP_ALIVE, UNKNOWN_FRAME_TYPE and everything else: nothing to check.
        _ => Exception::Success,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_matcher_mirrors_pattern() {
        assert!(is_valid_ipv4("0.0.0.0"));
        assert!(is_valid_ipv4("192.168.1.1"));
        assert!(is_valid_ipv4("255.255.255.255"));
        assert!(!is_valid_ipv4("256.0.0.1"));
        assert!(!is_valid_ipv4("1.2.3"));
        assert!(!is_valid_ipv4("1.2.3.4.5"));
        assert!(!is_valid_ipv4("1.2.3."));
        assert!(!is_valid_ipv4(""));
        assert!(!is_valid_ipv4("a.b.c.d"));
    }

    #[test]
    fn wifi_direct_ssid_matcher_mirrors_pattern() {
        assert!(matches_wifi_direct_ssid("DIRECT-A0-0123456789AB"));
        assert!(matches_wifi_direct_ssid("DIRECT-Zz"));
        assert!(!matches_wifi_direct_ssid("DIRECT-A*-0123456789AB"));
        assert!(!matches_wifi_direct_ssid("NOTDIRECT-AB"));
        assert!(!matches_wifi_direct_ssid("DIRECT-A"));
        assert!(!matches_wifi_direct_ssid("DIRECT-"));
    }

    #[test]
    fn within_range_is_half_open() {
        assert!(within_range(8, 8, 64));
        assert!(!within_range(64, 8, 64));
        assert!(!within_range(7, 8, 64));
    }
}
