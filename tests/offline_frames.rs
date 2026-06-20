//! Port of Google "Nearby" `offline_frames_test.cc` (Apache-2.0).
//!
//! Each C++ `EXPECT_THAT(msg, EqualsProto(R"pb(...)"))` becomes: build the frame
//! with the same `for_*` call, `from_bytes()` it (which also runs the validator,
//! exactly as `FromBytes` does), then assert structural equality against the
//! Rust struct that mirrors the golden text-proto. prost derives `PartialEq`
//! over `Option`/`Vec` fields, so this reproduces `EqualsProto`'s presence-aware
//! semantics. This pins nearby-rs's wire format against upstream.

use nearby_rs::frames::*;
use nearby_rs::mediums::{Medium, WifiDirectAuthType};
use nearby_rs::proto as pb;
use prost::Message;

use pb::bandwidth_upgrade_negotiation_frame as bwu;
use pb::offline_frame::Version;
use pb::v1_frame::FrameType;
use bwu::upgrade_path_info as upi;

// --- shared fixtures (mirror the C++ constexpr constants) -------------------

const ENDPOINT_ID: &str = "ABC";
const ENDPOINT_NAME: &str = "XYZ";
const NONCE: i32 = 1234;
const BSSID: &str = "FF:FF:FF:FF:FF:FF";
const AP_FREQUENCY: i32 = 2412;
const KEEP_ALIVE_INTERVAL_MILLIS: i32 = 1000;
const KEEP_ALIVE_TIMEOUT_MILLIS: i32 = 5000;

/// kMediums: the 11-element medium list used by `offline_frames_test.cc`.
fn k_mediums() -> Vec<Medium> {
    use Medium::*;
    vec![
        Mdns, Bluetooth, WifiHotspot, Ble, WifiLan, WifiAware, Nfc, WifiDirect, WebRtc, Usb, Awdl,
    ]
}

/// The same list as `ConnectionRequestFrame::Medium` i32s (the expected wire enum).
fn expected_mediums() -> Vec<i32> {
    use pb::connection_request_frame::Medium::*;
    vec![
        Mdns, Bluetooth, WifiHotspot, Ble, WifiLan, WifiAware, Nfc, WifiDirect, WebRtc, Usb, Awdl,
    ]
    .into_iter()
    .map(|m| m as i32)
    .collect()
}

fn base_connection_info() -> ConnectionInfo {
    ConnectionInfo {
        local_endpoint_id: ENDPOINT_ID.to_string(),
        local_endpoint_info: ENDPOINT_NAME.as_bytes().to_vec(),
        nonce: NONCE,
        supports_5ghz: true,
        bssid: BSSID.to_string(),
        ap_frequency: AP_FREQUENCY,
        supported_mediums: k_mediums(),
        keep_alive_interval_millis: KEEP_ALIVE_INTERVAL_MILLIS,
        keep_alive_timeout_millis: KEEP_ALIVE_TIMEOUT_MILLIS,
        medium_role: None,
        supported_wifi_direct_auth_types: vec![],
    }
}

fn expect_offline_frame(
    frame_type: FrameType,
    set: impl FnOnce(&mut pb::V1Frame),
) -> pb::OfflineFrame {
    let mut v1 = pb::V1Frame {
        r#type: Some(frame_type as i32),
        ..Default::default()
    };
    set(&mut v1);
    pb::OfflineFrame {
        version: Some(Version::V1 as i32),
        v1: Some(v1),
    }
}

fn expect_path_available(info: bwu::UpgradePathInfo) -> pb::OfflineFrame {
    expect_offline_frame(FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(pb::BandwidthUpgradeNegotiationFrame {
            event_type: Some(bwu::EventType::UpgradePathAvailable as i32),
            upgrade_path_info: Some(info),
            ..Default::default()
        });
    })
}

// --- tests ------------------------------------------------------------------

#[test]
fn can_parse_message_from_bytes() {
    let tx = expect_offline_frame(FrameType::ConnectionRequest, |v1| {
        v1.connection_request = Some(pb::ConnectionRequestFrame {
            endpoint_id: Some(ENDPOINT_ID.to_string()),
            endpoint_name: Some(ENDPOINT_NAME.to_string()),
            endpoint_info: Some(ENDPOINT_NAME.as_bytes().to_vec()),
            nonce: Some(NONCE),
            mediums: expected_mediums(),
            medium_metadata: Some(pb::MediumMetadata {
                supports_5_ghz: Some(true),
                bssid: Some(BSSID.to_string()),
                ..Default::default()
            }),
            keep_alive_interval_millis: Some(KEEP_ALIVE_INTERVAL_MILLIS),
            keep_alive_timeout_millis: Some(KEEP_ALIVE_TIMEOUT_MILLIS),
            ..Default::default()
        });
    });

    let rx = from_bytes(&tx.encode_to_vec()).expect("frame should parse");
    assert_eq!(rx, tx);
    assert_eq!(get_frame_type(&rx), FrameType::ConnectionRequest);

    let req = rx.v1.unwrap().connection_request.unwrap();
    assert_eq!(connection_request_mediums_to_mediums(&req), k_mediums());
}

#[test]
fn can_generate_legacy_connection_request() {
    let bytes = for_connection_request_connections(None, &base_connection_info(), false);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::ConnectionRequest, |v1| {
        v1.connection_request = Some(pb::ConnectionRequestFrame {
            endpoint_id: Some(ENDPOINT_ID.to_string()),
            endpoint_name: Some(ENDPOINT_NAME.to_string()),
            endpoint_info: Some(ENDPOINT_NAME.as_bytes().to_vec()),
            nonce: Some(NONCE),
            medium_metadata: Some(pb::MediumMetadata {
                supports_5_ghz: Some(true),
                bssid: Some(BSSID.to_string()),
                ap_frequency: Some(AP_FREQUENCY),
                ..Default::default()
            }),
            mediums: expected_mediums(),
            keep_alive_interval_millis: Some(KEEP_ALIVE_INTERVAL_MILLIS),
            keep_alive_timeout_millis: Some(KEEP_ALIVE_TIMEOUT_MILLIS),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_connections_connection_request() {
    let mut info = base_connection_info();
    info.medium_role = Some(pb::MediumRole {
        support_wifi_hotspot_client: Some(true),
        ..Default::default()
    });
    let device = pb::ConnectionsDevice {
        endpoint_id: Some(ENDPOINT_ID.to_string()),
        endpoint_type: Some(pb::EndpointType::ConnectionsEndpoint as i32),
        endpoint_info: Some(ENDPOINT_NAME.as_bytes().to_vec()),
        ..Default::default()
    };

    let bytes = for_connection_request_connections(Some(device.clone()), &info, true);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::ConnectionRequest, |v1| {
        v1.connection_request = Some(pb::ConnectionRequestFrame {
            endpoint_id: Some(ENDPOINT_ID.to_string()),
            endpoint_name: Some(ENDPOINT_NAME.to_string()),
            endpoint_info: Some(ENDPOINT_NAME.as_bytes().to_vec()),
            nonce: Some(NONCE),
            medium_metadata: Some(pb::MediumMetadata {
                supports_5_ghz: Some(true),
                bssid: Some(BSSID.to_string()),
                ap_frequency: Some(AP_FREQUENCY),
                medium_role: Some(pb::MediumRole {
                    support_wifi_hotspot_client: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            mediums: expected_mediums(),
            keep_alive_interval_millis: Some(KEEP_ALIVE_INTERVAL_MILLIS),
            keep_alive_timeout_millis: Some(KEEP_ALIVE_TIMEOUT_MILLIS),
            device: Some(pb::connection_request_frame::Device::ConnectionsDevice(device)),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_presence_connection_request() {
    let presence_device = pb::PresenceDevice {
        endpoint_id: Some(ENDPOINT_ID.to_string()),
        endpoint_type: Some(pb::EndpointType::PresenceEndpoint as i32),
        device_name: Some("TEST DEVICE".to_string()),
        ..Default::default()
    };

    let bytes = for_connection_request_presence(presence_device.clone(), &base_connection_info());
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::ConnectionRequest, |v1| {
        v1.connection_request = Some(pb::ConnectionRequestFrame {
            endpoint_id: Some(ENDPOINT_ID.to_string()),
            endpoint_name: Some(ENDPOINT_NAME.to_string()),
            endpoint_info: Some(ENDPOINT_NAME.as_bytes().to_vec()),
            nonce: Some(NONCE),
            medium_metadata: Some(pb::MediumMetadata {
                supports_5_ghz: Some(true),
                bssid: Some(BSSID.to_string()),
                ap_frequency: Some(AP_FREQUENCY),
                ..Default::default()
            }),
            mediums: expected_mediums(),
            keep_alive_interval_millis: Some(KEEP_ALIVE_INTERVAL_MILLIS),
            keep_alive_timeout_millis: Some(KEEP_ALIVE_TIMEOUT_MILLIS),
            device: Some(pb::connection_request_frame::Device::PresenceDevice(presence_device)),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn for_connection_request_connections_populates_wifi_direct_auth_types() {
    let mut info = base_connection_info();
    info.supported_wifi_direct_auth_types = vec![
        WifiDirectAuthType::WifiDirectWithDeviceName,
        WifiDirectAuthType::WifiDirectWithPassword,
    ];
    let device = pb::ConnectionsDevice {
        endpoint_id: Some(ENDPOINT_ID.to_string()),
        endpoint_type: Some(pb::EndpointType::ConnectionsEndpoint as i32),
        endpoint_info: Some(ENDPOINT_NAME.as_bytes().to_vec()),
        ..Default::default()
    };

    let bytes = for_connection_request_connections(Some(device.clone()), &info, false);
    let message = from_bytes(&bytes).expect("frame should parse");

    use pb::medium_metadata::WifiDirectAuthType as Wfd;
    let expected = expect_offline_frame(FrameType::ConnectionRequest, |v1| {
        v1.connection_request = Some(pb::ConnectionRequestFrame {
            endpoint_id: Some(ENDPOINT_ID.to_string()),
            endpoint_name: Some(ENDPOINT_NAME.to_string()),
            endpoint_info: Some(ENDPOINT_NAME.as_bytes().to_vec()),
            nonce: Some(NONCE),
            medium_metadata: Some(pb::MediumMetadata {
                supports_5_ghz: Some(true),
                bssid: Some(BSSID.to_string()),
                ap_frequency: Some(AP_FREQUENCY),
                supported_wifi_direct_auth_types: vec![
                    Wfd::WifiDirectWithDeviceName as i32,
                    Wfd::WifiDirectWithPassword as i32,
                ],
                ..Default::default()
            }),
            mediums: expected_mediums(),
            keep_alive_interval_millis: Some(KEEP_ALIVE_INTERVAL_MILLIS),
            keep_alive_timeout_millis: Some(KEEP_ALIVE_TIMEOUT_MILLIS),
            device: Some(pb::connection_request_frame::Device::ConnectionsDevice(device)),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_connection_response() {
    let os_info = pb::OsInfo {
        r#type: Some(pb::os_info::OsType::Linux as i32),
    };
    let bytes = for_connection_response(1, os_info, 5);
    let message = from_bytes(&bytes).expect("frame should parse");

    #[allow(deprecated)]
    let expected = expect_offline_frame(FrameType::ConnectionResponse, |v1| {
        v1.connection_response = Some(pb::ConnectionResponseFrame {
            status: Some(1),
            response: Some(pb::connection_response_frame::ResponseStatus::Reject as i32),
            os_info: Some(pb::OsInfo {
                r#type: Some(pb::os_info::OsType::Linux as i32),
            }),
            multiplex_socket_bitmask: Some(0),
            safe_to_disconnect_version: Some(5),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_control_payload_transfer() {
    let header = pb::payload_transfer_frame::PayloadHeader {
        id: Some(12345),
        r#type: Some(pb::payload_transfer_frame::payload_header::PayloadType::Bytes as i32),
        total_size: Some(1024),
        ..Default::default()
    };
    let control = pb::payload_transfer_frame::ControlMessage {
        event: Some(pb::payload_transfer_frame::control_message::EventType::PayloadCanceled as i32),
        offset: Some(150),
    };

    let bytes = for_control_payload_transfer(header.clone(), control);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::PayloadTransfer, |v1| {
        v1.payload_transfer = Some(pb::PayloadTransferFrame {
            packet_type: Some(pb::payload_transfer_frame::PacketType::Control as i32),
            payload_header: Some(header),
            control_message: Some(control),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_data_payload_transfer() {
    let header = pb::payload_transfer_frame::PayloadHeader {
        id: Some(12345),
        r#type: Some(pb::payload_transfer_frame::payload_header::PayloadType::Bytes as i32),
        total_size: Some(1024),
        ..Default::default()
    };
    let chunk = pb::payload_transfer_frame::PayloadChunk {
        flags: Some(1),
        offset: Some(150),
        body: Some(b"payload data".to_vec()),
        ..Default::default()
    };

    let bytes = for_data_payload_transfer(header.clone(), chunk.clone());
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::PayloadTransfer, |v1| {
        v1.payload_transfer = Some(pb::PayloadTransferFrame {
            packet_type: Some(pb::payload_transfer_frame::PacketType::Data as i32),
            payload_header: Some(header),
            payload_chunk: Some(chunk),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_payload_ack_payload_transfer() {
    let bytes = for_payload_ack_payload_transfer(12345);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::PayloadTransfer, |v1| {
        v1.payload_transfer = Some(pb::PayloadTransferFrame {
            packet_type: Some(pb::payload_transfer_frame::PacketType::PayloadAck as i32),
            payload_header: Some(pb::payload_transfer_frame::PayloadHeader {
                id: Some(12345),
                total_size: Some(-1),
                ..Default::default()
            }),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_bwu_wifi_hotspot_path_available() {
    let credentials = upi::WifiHotspotCredentials {
        ssid: Some("ssid".to_string()),
        password: Some("password".to_string()),
        port: Some(1234),
        frequency: Some(2412),
        gateway: Some("0.0.0.0".to_string()),
        address_candidates: vec![
            pb::ServiceAddress {
                ip_address: Some(vec![
                    0xfe, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4d, 0xb2, 0xb3, 0x5c, 0x22,
                    0x03, 0x98, 0xa1,
                ]),
                port: Some(1234),
            },
            pb::ServiceAddress {
                ip_address: Some(vec![0xc0, 0xa8, 0x00, 0x01]),
                port: Some(5678),
            },
        ],
    };

    let bytes = for_bwu_wifi_hotspot_path_available(credentials.clone(), false);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_path_available(bwu::UpgradePathInfo {
        medium: Some(upi::Medium::WifiHotspot as i32),
        wifi_hotspot_credentials: Some(credentials),
        supports_disabling_encryption: Some(false),
        supports_client_introduction_ack: Some(true),
        ..Default::default()
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_bwu_wifi_lan_path_available() {
    let addresses = vec![
        ServiceAddress {
            address: vec![
                0x2a, 0x00, 0x79, 0xe0, 0x2e, 0x87, 0x00, 0x06, 0xb7, 0x28, 0x67, 0x45, 0x7a, 0xdd,
                0x01, 0x53,
            ],
            port: 1234,
        },
        ServiceAddress {
            address: vec![0x01, 0x02, 0x03, 0x04],
            port: 1234,
        },
    ];

    let bytes = for_bwu_wifi_lan_path_available(&addresses);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_path_available(bwu::UpgradePathInfo {
        medium: Some(upi::Medium::WifiLan as i32),
        wifi_lan_socket: Some(upi::WifiLanSocket {
            ip_address: Some(vec![0x01, 0x02, 0x03, 0x04]),
            wifi_port: Some(1234),
            address_candidates: vec![
                pb::ServiceAddress {
                    ip_address: Some(vec![
                        0x2a, 0x00, 0x79, 0xe0, 0x2e, 0x87, 0x00, 0x06, 0xb7, 0x28, 0x67, 0x45,
                        0x7a, 0xdd, 0x01, 0x53,
                    ]),
                    port: Some(1234),
                },
                pb::ServiceAddress {
                    ip_address: Some(vec![0x01, 0x02, 0x03, 0x04]),
                    port: Some(1234),
                },
            ],
        }),
        supports_client_introduction_ack: Some(true),
        ..Default::default()
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_bwu_awdl_path_available() {
    let bytes = for_bwu_awdl_path_available("service_name", "nearby_upgrade", "password", true);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_path_available(bwu::UpgradePathInfo {
        medium: Some(upi::Medium::Awdl as i32),
        supports_client_introduction_ack: Some(true),
        awdl_credentials: Some(upi::AwdlCredentials {
            service_name: Some("service_name".to_string()),
            service_type: Some("nearby_upgrade".to_string()),
            password: Some("password".to_string()),
        }),
        supports_disabling_encryption: Some(true),
        ..Default::default()
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_bwu_wifi_aware_path_available() {
    let bytes = for_bwu_wifi_aware_path_available("service_id", b"service_info", "password", false);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_path_available(bwu::UpgradePathInfo {
        medium: Some(upi::Medium::WifiAware as i32),
        wifi_aware_credentials: Some(upi::WifiAwareCredentials {
            service_id: Some("service_id".to_string()),
            service_info: Some(b"service_info".to_vec()),
            password: Some("password".to_string()),
        }),
        supports_disabling_encryption: Some(false),
        supports_client_introduction_ack: Some(true),
        ..Default::default()
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_bwu_wifi_direct_path_available() {
    let bytes = for_bwu_wifi_direct_path_available(
        "", "", 1000, 2412, false, "192.168.1.1", "NC-WifiDirectTest", "b592f7d3",
    );
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_path_available(bwu::UpgradePathInfo {
        medium: Some(upi::Medium::WifiDirect as i32),
        wifi_direct_credentials: Some(upi::WifiDirectCredentials {
            ssid: Some("".to_string()),
            password: Some("".to_string()),
            port: Some(1000),
            frequency: Some(2412),
            gateway: Some("192.168.1.1".to_string()),
            device_name: Some("NC-WifiDirectTest".to_string()),
            pin: Some("b592f7d3".to_string()),
            ..Default::default()
        }),
        supports_disabling_encryption: Some(false),
        supports_client_introduction_ack: Some(true),
        ..Default::default()
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_bwu_bluetooth_path_available() {
    let bytes = for_bwu_bluetooth_path_available("service", "11:22:33:44:55:66");
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_path_available(bwu::UpgradePathInfo {
        medium: Some(upi::Medium::Bluetooth as i32),
        bluetooth_credentials: Some(upi::BluetoothCredentials {
            service_name: Some("service".to_string()),
            mac_address: Some("11:22:33:44:55:66".to_string()),
        }),
        supports_client_introduction_ack: Some(true),
        ..Default::default()
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_bwu_last_write() {
    let bytes = for_bwu_last_write();
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(pb::BandwidthUpgradeNegotiationFrame {
            event_type: Some(bwu::EventType::LastWriteToPriorChannel as i32),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_bwu_safe_to_close() {
    let bytes = for_bwu_safe_to_close();
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(pb::BandwidthUpgradeNegotiationFrame {
            event_type: Some(bwu::EventType::SafeToClosePriorChannel as i32),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_bwu_introduction() {
    let bytes = for_bwu_introduction(ENDPOINT_ID, "DEF", false);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(pb::BandwidthUpgradeNegotiationFrame {
            event_type: Some(bwu::EventType::ClientIntroduction as i32),
            client_introduction: Some(bwu::ClientIntroduction {
                endpoint_id: Some(ENDPOINT_ID.to_string()),
                supports_disabling_encryption: Some(false),
                last_endpoint_id: Some("DEF".to_string()),
            }),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_keep_alive() {
    let bytes = for_keep_alive();
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::KeepAlive, |v1| {
        v1.keep_alive = Some(pb::KeepAliveFrame::default());
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_disconnection() {
    let bytes = for_disconnection(true, true);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::Disconnection, |v1| {
        v1.disconnection = Some(pb::DisconnectionFrame {
            request_safe_to_disconnect: Some(true),
            ack_safe_to_disconnect: Some(true),
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn can_generate_bwu_path_request() {
    let medium_role = pb::MediumRole {
        support_wifi_hotspot_client: Some(true),
        ..Default::default()
    };
    let bytes = for_bwu_path_request(Medium::WifiHotspot, &[Medium::WifiHotspot], medium_role);
    let message = from_bytes(&bytes).expect("frame should parse");

    let expected = expect_offline_frame(FrameType::BandwidthUpgradeNegotiation, |v1| {
        v1.bandwidth_upgrade_negotiation = Some(pb::BandwidthUpgradeNegotiationFrame {
            event_type: Some(bwu::EventType::UpgradePathRequest as i32),
            upgrade_path_info: Some(bwu::UpgradePathInfo {
                medium: Some(upi::Medium::WifiHotspot as i32),
                upgrade_path_request: Some(upi::UpgradePathRequest {
                    mediums: vec![upi::Medium::WifiHotspot as i32],
                    medium_meta_data: Some(pb::MediumMetadata {
                        medium_role: Some(pb::MediumRole {
                            support_wifi_hotspot_client: Some(true),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
    });
    assert_eq!(message, expected);
}

#[test]
fn wfd_auth_type_to_medium_metadata_wfd_auth_type_maps() {
    use pb::medium_metadata::WifiDirectAuthType as Out;
    assert_eq!(
        wfd_auth_type_to_medium_metadata_wfd_auth_type(WifiDirectAuthType::WifiDirectWithPassword),
        Out::WifiDirectWithPassword
    );
    assert_eq!(
        wfd_auth_type_to_medium_metadata_wfd_auth_type(
            WifiDirectAuthType::WifiDirectWithDeviceName
        ),
        Out::WifiDirectWithDeviceName
    );
    assert_eq!(
        wfd_auth_type_to_medium_metadata_wfd_auth_type(WifiDirectAuthType::WifiDirectTypeUnknown),
        Out::WifiDirectTypeUnknown
    );
}

#[test]
fn medium_metadata_wfd_auth_type_to_wfd_auth_type_maps() {
    use pb::medium_metadata::WifiDirectAuthType as In;
    assert_eq!(
        medium_metadata_wfd_auth_type_to_wfd_auth_type(In::WifiDirectWithPassword),
        WifiDirectAuthType::WifiDirectWithPassword
    );
    assert_eq!(
        medium_metadata_wfd_auth_type_to_wfd_auth_type(In::WifiDirectWithDeviceName),
        WifiDirectAuthType::WifiDirectWithDeviceName
    );
    assert_eq!(
        medium_metadata_wfd_auth_type_to_wfd_auth_type(In::WifiDirectTypeUnknown),
        WifiDirectAuthType::WifiDirectTypeUnknown
    );
}

#[test]
fn medium_metadata_wfd_auth_types_to_wfd_auth_types_maps() {
    use pb::medium_metadata::WifiDirectAuthType as Wfd;
    let medium_metadata = pb::MediumMetadata {
        supported_wifi_direct_auth_types: vec![
            Wfd::WifiDirectWithPassword as i32,
            Wfd::WifiDirectWithDeviceName as i32,
        ],
        ..Default::default()
    };
    assert_eq!(
        medium_metadata_wfd_auth_types_to_wfd_auth_types(&medium_metadata),
        vec![
            WifiDirectAuthType::WifiDirectWithPassword,
            WifiDirectAuthType::WifiDirectWithDeviceName,
        ]
    );

    let empty = pb::MediumMetadata::default();
    assert!(medium_metadata_wfd_auth_types_to_wfd_auth_types(&empty).is_empty());
}
