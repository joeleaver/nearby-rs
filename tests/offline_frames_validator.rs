//! Port of Google "Nearby" `offline_frames_validator_test.cc` (Apache-2.0).
//!
//! Each test builds a frame with a `for_*` call, decodes it raw (the C++ uses
//! `ParseFromString`, NOT `FromBytes`, so the validator is exercised in
//! isolation), optionally mutates the decoded struct exactly as the C++ mutates
//! the proto, then asserts the [`ensure_valid_offline_frame`] verdict.

use nearby_rs::frames::*;
use nearby_rs::mediums::Medium;
use nearby_rs::proto as pb;
use nearby_rs::{ensure_valid_offline_frame, Exception};
use prost::Message;

use pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info as upi;
use pb::payload_transfer_frame as ptf;

const PORT: i32 = 1000;
const SSID: &str = "ssid";
const PASSWORD: &str = "password";
const WIFI_HOTSPOT_GATEWAY: &str = "0.0.0.0";
const HOTSPOT_FREQUENCY: i32 = 2412;
const WIFI_DIRECT_SSID: &str = "DIRECT-A0-0123456789AB";
const WIFI_DIRECT_PASSWORD: &str = "WIFIDIRECT123456";
const WIFI_DIRECT_DEVICE_NAME: &str = "NC-WifiDirectTest";
const WIFI_DIRECT_PIN: &str = "b592f7d3";
const GATEWAY: &str = "192.168.1.1";
const WIFI_DIRECT_FREQUENCY: i32 = 2412;
const SUPPORTS_DISABLING_ENCRYPTION: bool = true;

fn decode(bytes: &[u8]) -> pb::OfflineFrame {
    pb::OfflineFrame::decode(bytes).expect("frame should decode")
}

fn connection_info() -> ConnectionInfo {
    use Medium::*;
    ConnectionInfo {
        local_endpoint_id: "ABC".into(),
        local_endpoint_info: b"XYZ".to_vec(),
        nonce: 1234,
        supports_5ghz: true,
        bssid: "FF:FF:FF:FF:FF:FF".into(),
        ap_frequency: 2412,
        supported_mediums: vec![
            Mdns,
            Bluetooth,
            WifiHotspot,
            Ble,
            WifiLan,
            WifiAware,
            Nfc,
            WifiDirect,
            WebRtc,
        ],
        keep_alive_interval_millis: 1000,
        keep_alive_timeout_millis: 5000,
        medium_role: None,
        supported_wifi_direct_auth_types: vec![],
    }
}

fn bytes_header(
    total_size: i64,
    payload_type: ptf::payload_header::PayloadType,
) -> ptf::PayloadHeader {
    ptf::PayloadHeader {
        id: Some(12345),
        r#type: Some(payload_type as i32),
        total_size: Some(total_size),
        ..Default::default()
    }
}

fn data_chunk() -> ptf::PayloadChunk {
    ptf::PayloadChunk {
        flags: Some(1),
        offset: Some(150),
        body: Some(b"payload data".to_vec()),
        ..Default::default()
    }
}

// --- ConnectionRequest ------------------------------------------------------

#[test]
fn validates_as_ok_with_valid_connection_request_frame() {
    let frame = decode(&for_connection_request_connections(
        None,
        &connection_info(),
        false,
    ));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_null_connection_request_frame() {
    let mut frame = decode(&for_connection_request_connections(
        None,
        &connection_info(),
        false,
    ));
    frame.v1.as_mut().unwrap().connection_request = None;
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_null_endpoint_id_in_connection_request_frame() {
    let mut info = connection_info();
    info.local_endpoint_id = String::new();
    let frame = decode(&for_connection_request_connections(None, &info, false));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_empty_endpoint_id_in_connection_request_frame() {
    let mut info = connection_info();
    info.local_endpoint_id = String::new();
    let mut frame = decode(&for_connection_request_connections(None, &info, false));
    // Present-but-empty endpoint_id should still fail.
    frame
        .v1
        .as_mut()
        .unwrap()
        .connection_request
        .as_mut()
        .unwrap()
        .endpoint_id = Some(String::new());
    let frame = decode(&frame.encode_to_vec());
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_null_endpoint_info_in_connection_request_frame() {
    let mut info = connection_info();
    info.local_endpoint_info = Vec::new();
    let frame = decode(&for_connection_request_connections(None, &info, false));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_ok_with_null_bssid_in_connection_request_frame() {
    let mut info = connection_info();
    info.bssid = String::new();
    let frame = decode(&for_connection_request_connections(None, &info, false));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_ok_with_null_mediums_in_connection_request_frame() {
    let mut info = connection_info();
    info.supported_mediums = Vec::new();
    let frame = decode(&for_connection_request_connections(None, &info, false));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

// --- ConnectionResponse -----------------------------------------------------

#[test]
fn validates_as_ok_with_valid_connection_response_frame() {
    let frame = decode(&for_connection_response(0, pb::OsInfo::default(), 0));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_null_connection_response_frame() {
    let mut frame = decode(&for_connection_response(0, pb::OsInfo::default(), 0));
    frame.v1.as_mut().unwrap().connection_response = None;
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_unexpected_status_in_connection_response_frame() {
    // To maintain forward compatibility, unexpected status codes are allowed.
    let frame = decode(&for_connection_response(-1, pb::OsInfo::default(), 0));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

// --- PayloadTransfer --------------------------------------------------------

#[test]
fn validates_as_ok_with_valid_payload_transfer_frame() {
    // 3e10 exercises a file larger than int max.
    let frame = decode(&for_data_payload_transfer(
        bytes_header(30_000_000_000, ptf::payload_header::PayloadType::Bytes),
        data_chunk(),
    ));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_ok_type_file_with_empty_file_path_and_parent() {
    let mut header = bytes_header(30_000_000_000, ptf::payload_header::PayloadType::File);
    header.file_name = Some(String::new());
    header.parent_folder = Some(String::new());
    let frame = decode(&for_data_payload_transfer(header, data_chunk()));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_ok_type_file_with_legal_file_path() {
    let mut header = bytes_header(30_000_000_000, ptf::payload_header::PayloadType::File);
    header.file_name = Some("earth_85MB_test (1) (3) (4) (8) (1) (2) (2) (1).jpg".to_string());
    header.parent_folder = Some(String::new());
    let frame = decode(&for_data_payload_transfer(header, data_chunk()));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_failed_type_file_with_illegal_file_path() {
    let mut header = bytes_header(30_000_000_000, ptf::payload_header::PayloadType::File);
    header.file_name = Some("earth_85MB_test (1): (3) (4) (8) (1) (2) (2) (1).jpg".to_string());
    header.parent_folder = Some(String::new());
    let frame = decode(&for_data_payload_transfer(header, data_chunk()));
    assert_eq!(
        ensure_valid_offline_frame(&frame),
        Exception::IllegalCharacters
    );
}

#[test]
fn validates_as_ok_type_file_with_legal_parent_folder() {
    let mut header = bytes_header(30_000_000_000, ptf::payload_header::PayloadType::File);
    header.file_name = Some(String::new());
    header.parent_folder = Some("earth_85MB_test (1) (3) (4) (8) (1) (2) (2) (1).jpg".to_string());
    let frame = decode(&for_data_payload_transfer(header, data_chunk()));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_failed_type_file_with_illegal_parent_folder() {
    let mut header = bytes_header(30_000_000_000, ptf::payload_header::PayloadType::File);
    header.file_name = Some(String::new());
    header.parent_folder = Some("earth_85MB_test (1): (3) (4) (8) (1) (2) (2) (1).jpg".to_string());
    let frame = decode(&for_data_payload_transfer(header, data_chunk()));
    assert_eq!(
        ensure_valid_offline_frame(&frame),
        Exception::IllegalCharacters
    );
}

#[test]
fn validates_as_fail_with_null_payload_transfer_frame() {
    let mut frame = decode(&for_data_payload_transfer(
        bytes_header(1024, ptf::payload_header::PayloadType::UnknownPayloadType),
        data_chunk(),
    ));
    frame.v1.as_mut().unwrap().payload_transfer = None;
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_null_payload_header_in_payload_transfer_frame() {
    let mut frame = decode(&for_data_payload_transfer(
        bytes_header(1024, ptf::payload_header::PayloadType::Bytes),
        data_chunk(),
    ));
    frame
        .v1
        .as_mut()
        .unwrap()
        .payload_transfer
        .as_mut()
        .unwrap()
        .payload_header = None;
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_invalid_size_in_payload_header() {
    let frame = decode(&for_data_payload_transfer(
        bytes_header(-5, ptf::payload_header::PayloadType::Bytes),
        data_chunk(),
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_null_payload_chunk_in_payload_transfer_frame() {
    let mut frame = decode(&for_data_payload_transfer(
        bytes_header(1024, ptf::payload_header::PayloadType::Bytes),
        data_chunk(),
    ));
    frame
        .v1
        .as_mut()
        .unwrap()
        .payload_transfer
        .as_mut()
        .unwrap()
        .payload_chunk = None;
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_invalid_offset_in_payload_chunk() {
    let mut chunk = data_chunk();
    chunk.offset = Some(-1);
    let frame = decode(&for_data_payload_transfer(
        bytes_header(1024, ptf::payload_header::PayloadType::Bytes),
        chunk,
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_invalid_large_offset_in_payload_chunk() {
    let mut chunk = data_chunk();
    chunk.offset = Some(4999);
    let frame = decode(&for_data_payload_transfer(
        bytes_header(1024, ptf::payload_header::PayloadType::Bytes),
        chunk,
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_invalid_flags_in_payload_chunk() {
    let mut frame = decode(&for_data_payload_transfer(
        bytes_header(1024, ptf::payload_header::PayloadType::Bytes),
        data_chunk(),
    ));
    frame
        .v1
        .as_mut()
        .unwrap()
        .payload_transfer
        .as_mut()
        .unwrap()
        .payload_chunk
        .as_mut()
        .unwrap()
        .flags = None;
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_null_control_message_in_payload_transfer_frame() {
    let control = ptf::ControlMessage {
        event: Some(ptf::control_message::EventType::PayloadCanceled as i32),
        offset: Some(150),
    };
    let mut frame = decode(&for_control_payload_transfer(
        bytes_header(1024, ptf::payload_header::PayloadType::Bytes),
        control,
    ));
    frame
        .v1
        .as_mut()
        .unwrap()
        .payload_transfer
        .as_mut()
        .unwrap()
        .control_message = None;
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_invalid_negative_offset_in_control_message() {
    let control = ptf::ControlMessage {
        event: Some(ptf::control_message::EventType::PayloadCanceled as i32),
        offset: Some(-1),
    };
    let frame = decode(&for_control_payload_transfer(
        bytes_header(1024, ptf::payload_header::PayloadType::Bytes),
        control,
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_invalid_large_offset_in_control_message() {
    let control = ptf::ControlMessage {
        event: Some(ptf::control_message::EventType::PayloadCanceled as i32),
        offset: Some(4999),
    };
    let frame = decode(&for_control_payload_transfer(
        bytes_header(1024, ptf::payload_header::PayloadType::Bytes),
        control,
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

// --- Bandwidth upgrade ------------------------------------------------------

#[test]
fn validate_hotspot_upgrade_frame_with_gateway_succeeds() {
    let credentials = upi::WifiHotspotCredentials {
        ssid: Some(SSID.to_string()),
        password: Some(PASSWORD.to_string()),
        port: Some(PORT),
        frequency: Some(HOTSPOT_FREQUENCY),
        gateway: Some(WIFI_HOTSPOT_GATEWAY.to_string()),
        ..Default::default()
    };
    let frame = decode(&for_bwu_wifi_hotspot_path_available(
        credentials,
        SUPPORTS_DISABLING_ENCRYPTION,
    ));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validate_hotspot_upgrade_frame_with_address_candidates_succeeds() {
    let credentials = upi::WifiHotspotCredentials {
        ssid: Some(SSID.to_string()),
        password: Some(PASSWORD.to_string()),
        frequency: Some(HOTSPOT_FREQUENCY),
        address_candidates: vec![
            pb::ServiceAddress {
                ip_address: Some(vec![
                    0xfe, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4d, 0xb2, 0xb3, 0x5c, 0x22,
                    0x03, 0x98, 0xa1,
                ]),
                port: Some(PORT),
            },
            pb::ServiceAddress {
                ip_address: Some(vec![0xc0, 0xa8, 0x00, 0x01]),
                port: Some(PORT),
            },
        ],
        ..Default::default()
    };
    let frame = decode(&for_bwu_wifi_hotspot_path_available(
        credentials,
        SUPPORTS_DISABLING_ENCRYPTION,
    ));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validate_hotspot_upgrade_frame_with_invalid_address_candidates_length_fails() {
    let credentials = upi::WifiHotspotCredentials {
        ssid: Some(SSID.to_string()),
        password: Some(PASSWORD.to_string()),
        frequency: Some(HOTSPOT_FREQUENCY),
        address_candidates: vec![pb::ServiceAddress {
            // 12 bytes — neither IPv4 (4) nor IPv6 (16).
            ip_address: Some(vec![
                0xfe, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4d, 0xb2, 0xb3, 0x5c,
            ]),
            port: Some(PORT),
        }],
        ..Default::default()
    };
    let frame = decode(&for_bwu_wifi_hotspot_path_available(
        credentials,
        SUPPORTS_DISABLING_ENCRYPTION,
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validate_hotspot_upgrade_frame_with_address_candidates_no_port_fails() {
    let credentials = upi::WifiHotspotCredentials {
        ssid: Some(SSID.to_string()),
        password: Some(PASSWORD.to_string()),
        frequency: Some(HOTSPOT_FREQUENCY),
        address_candidates: vec![pb::ServiceAddress {
            ip_address: Some(vec![
                0xfe, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4d, 0xb2, 0xb3, 0x5c, 0x22, 0x03,
                0x98, 0xa1,
            ]),
            port: None,
        }],
        ..Default::default()
    };
    let frame = decode(&for_bwu_wifi_hotspot_path_available(
        credentials,
        SUPPORTS_DISABLING_ENCRYPTION,
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validate_wifi_lan_upgrade_frame_with_address_candidates_succeeds() {
    let address_candidates = vec![
        ServiceAddress {
            address: vec![
                0x2a, 0x00, 0x79, 0xe0, 0x2e, 0x87, 0x00, 0x06, 0xb7, 0x28, 0x67, 0x45, 0x7a, 0xdd,
                0x01, 0x53,
            ],
            port: PORT,
        },
        ServiceAddress {
            address: vec![0xc0, 0xa8, 0x00, 0x01],
            port: PORT,
        },
    ];
    let frame = decode(&for_bwu_wifi_lan_path_available(&address_candidates));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_null_bandwidth_upgrade_negotiation_frame() {
    let credentials = upi::WifiHotspotCredentials {
        ssid: Some(SSID.to_string()),
        password: Some(PASSWORD.to_string()),
        port: Some(PORT),
        frequency: Some(HOTSPOT_FREQUENCY),
        gateway: Some(WIFI_HOTSPOT_GATEWAY.to_string()),
        ..Default::default()
    };
    let mut frame = decode(&for_bwu_wifi_hotspot_path_available(
        credentials,
        SUPPORTS_DISABLING_ENCRYPTION,
    ));
    frame.v1.as_mut().unwrap().bandwidth_upgrade_negotiation = None;
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_ok_bandwidth_upgrade_wifi_direct() {
    let frame = decode(&for_bwu_wifi_direct_path_available(
        WIFI_DIRECT_SSID,
        WIFI_DIRECT_PASSWORD,
        PORT,
        WIFI_DIRECT_FREQUENCY,
        SUPPORTS_DISABLING_ENCRYPTION,
        GATEWAY,
        WIFI_DIRECT_DEVICE_NAME,
        WIFI_DIRECT_PIN,
    ));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_valid_frequency_in_bandwidth_upgrade_wifi_direct() {
    // Anything less than -1 is invalid.
    let frame = decode(&for_bwu_wifi_direct_path_available(
        WIFI_DIRECT_SSID,
        WIFI_DIRECT_PASSWORD,
        PORT,
        -2,
        SUPPORTS_DISABLING_ENCRYPTION,
        GATEWAY,
        WIFI_DIRECT_DEVICE_NAME,
        WIFI_DIRECT_PIN,
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());

    // But -1 itself is valid.
    let frame = decode(&for_bwu_wifi_direct_path_available(
        WIFI_DIRECT_SSID,
        WIFI_DIRECT_PASSWORD,
        PORT,
        -1,
        SUPPORTS_DISABLING_ENCRYPTION,
        GATEWAY,
        WIFI_DIRECT_DEVICE_NAME,
        WIFI_DIRECT_PIN,
    ));
    assert!(ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_invalid_ssid_in_bandwidth_upgrade_wifi_direct() {
    let wifi_direct_ssid = "DIRECT-A*-0123456789AB";
    let wifi_direct_pin_wrong_length = "01234567890123456";
    let frame = decode(&for_bwu_wifi_direct_path_available(
        wifi_direct_ssid,
        WIFI_DIRECT_PASSWORD,
        PORT,
        WIFI_DIRECT_FREQUENCY,
        SUPPORTS_DISABLING_ENCRYPTION,
        GATEWAY,
        WIFI_DIRECT_DEVICE_NAME,
        wifi_direct_pin_wrong_length,
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());

    let ssid_wrong_length = format!("{WIFI_DIRECT_SSID}ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789");
    let device_name_wrong_length =
        format!("{WIFI_DIRECT_DEVICE_NAME}ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789");
    let frame = decode(&for_bwu_wifi_direct_path_available(
        &ssid_wrong_length,
        WIFI_DIRECT_PASSWORD,
        PORT,
        WIFI_DIRECT_FREQUENCY,
        SUPPORTS_DISABLING_ENCRYPTION,
        GATEWAY,
        &device_name_wrong_length,
        WIFI_DIRECT_PIN,
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}

#[test]
fn validates_as_fail_with_invalid_password_in_bandwidth_upgrade_wifi_direct() {
    let long_password =
        format!("{WIFI_DIRECT_SSID}AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789");
    let long_pin =
        format!("{WIFI_DIRECT_PIN}AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789");
    let frame = decode(&for_bwu_wifi_direct_path_available(
        WIFI_DIRECT_SSID,
        &long_password,
        PORT,
        WIFI_DIRECT_FREQUENCY,
        SUPPORTS_DISABLING_ENCRYPTION,
        GATEWAY,
        WIFI_DIRECT_DEVICE_NAME,
        &long_pin,
    ));
    assert!(!ensure_valid_offline_frame(&frame).ok());
}
