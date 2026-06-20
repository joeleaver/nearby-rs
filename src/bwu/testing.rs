//! Test doubles for the BWU subsystem: `FakeEndpointChannel` and
//! `FakeBwuHandler`, ported from `fake_endpoint_channel.h` / `fake_bwu_handler.h`.
//!
//! These are shipped (not `#[cfg(test)]`) so the crate's integration tests under
//! `tests/` can use them; they could be moved behind a `testing` feature later.

use std::sync::{Arc, Mutex};

use crate::bwu::channel::{DisconnectionReason, EndpointChannel};
use crate::bwu::client::ClientProxy;
use crate::bwu::handler::MediumBwuHandler;
use crate::frames::{
    for_bwu_bluetooth_path_available, for_bwu_webrtc_path_available,
    for_bwu_wifi_direct_path_available, for_bwu_wifi_hotspot_path_available,
    for_bwu_wifi_lan_path_available, Exception, ServiceAddress,
};
use crate::mediums::Medium;
use crate::proto as pb;

type UpgradePathInfo = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo;

// ---------------------------------------------------------------------------
// FakeEndpointChannel
// ---------------------------------------------------------------------------

struct ChannelState {
    read_output: Result<Vec<u8>, Exception>,
    write_output: Exception,
    is_closed: bool,
    disconnection_reason: DisconnectionReason,
    is_paused: bool,
    local_endpoint_id: String,
    /// Mirrors `GetLastWriteTimestamp() != InfinitePast()` — i.e. "a frame was
    /// written".
    wrote_anything: bool,
}

/// An `EndpointChannel` whose read/write outputs are settable and whose
/// close/pause state is observable, for driving the BWU state machine.
pub struct FakeEndpointChannel {
    medium: Medium,
    service_id: String,
    state: Mutex<ChannelState>,
}

impl FakeEndpointChannel {
    pub fn new(medium: Medium, service_id: &str) -> Self {
        Self {
            medium,
            service_id: service_id.to_string(),
            state: Mutex::new(ChannelState {
                // Default ExceptionOr<ByteArray> is a failure, like the C++ fake.
                read_output: Err(Exception::Failed),
                write_output: Exception::Success,
                is_closed: false,
                disconnection_reason: DisconnectionReason::UnknownDisconnectionReason,
                is_paused: false,
                local_endpoint_id: String::new(),
                wrote_anything: false,
            }),
        }
    }

    pub fn set_read_output(&self, output: Result<Vec<u8>, Exception>) {
        self.state.lock().unwrap().read_output = output;
    }

    pub fn set_write_output(&self, output: Exception) {
        self.state.lock().unwrap().write_output = output;
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().unwrap().is_closed
    }

    pub fn disconnection_reason(&self) -> DisconnectionReason {
        self.state.lock().unwrap().disconnection_reason
    }

    pub fn is_paused(&self) -> bool {
        self.state.lock().unwrap().is_paused
    }

    /// Whether any frame has been written (mirrors `GetLastWriteTimestamp() !=
    /// InfinitePast()`).
    pub fn wrote_anything(&self) -> bool {
        self.state.lock().unwrap().wrote_anything
    }
}

impl EndpointChannel for FakeEndpointChannel {
    fn read(&self) -> Result<Vec<u8>, Exception> {
        self.state.lock().unwrap().read_output.clone()
    }
    fn write(&self, _data: &[u8]) -> Exception {
        let mut s = self.state.lock().unwrap();
        s.wrote_anything = true;
        s.write_output
    }
    fn close(&self) {
        self.state.lock().unwrap().is_closed = true;
    }
    fn close_with_reason(&self, reason: DisconnectionReason) {
        let mut s = self.state.lock().unwrap();
        s.is_closed = true;
        s.disconnection_reason = reason;
    }
    fn medium(&self) -> Medium {
        self.medium
    }
    fn service_id(&self) -> String {
        self.service_id.clone()
    }
    fn name(&self) -> String {
        format!("fake-channel-{}", self.service_id)
    }
    fn channel_type(&self) -> String {
        "fake-channel-type".to_string()
    }
    fn local_endpoint_id(&self) -> String {
        self.state.lock().unwrap().local_endpoint_id.clone()
    }
    fn set_local_endpoint_id(&self, local_endpoint_id: &str) {
        self.state.lock().unwrap().local_endpoint_id = local_endpoint_id.to_string();
    }
    fn pause(&self) {
        self.state.lock().unwrap().is_paused = true;
    }
    fn resume(&self) {
        self.state.lock().unwrap().is_paused = false;
    }
    fn is_paused(&self) -> bool {
        self.state.lock().unwrap().is_paused
    }
    fn disable_encryption(&self) {}
}

// ---------------------------------------------------------------------------
// FakeBwuHandler
// ---------------------------------------------------------------------------

/// Arguments captured by a `FakeBwuHandler` call (mirrors the C++ `InputData`).
/// `client_id` stands in for the recorded `ClientProxy*` pointer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InputData {
    pub client_id: i64,
    pub service_id: Option<String>,
    pub endpoint_id: Option<String>,
}

/// The call vectors recorded by a `FakeBwuHandler`, shared with the test via a
/// handle (the handler itself is moved into the `BwuManager` handler map).
#[derive(Default)]
pub struct FakeBwuHandlerRecords {
    pub create_calls: Vec<InputData>,
    pub disconnect_calls: Vec<InputData>,
    pub handle_initialize_calls: Vec<InputData>,
    pub handle_revert_calls: Vec<InputData>,
}

pub type FakeBwuHandlerHandle = Arc<Mutex<FakeBwuHandlerRecords>>;

/// A medium handler that records its calls and returns real, medium-appropriate
/// `UPGRADE_PATH_AVAILABLE` bytes. Wrap in `BaseBwuHandler` before use.
pub struct FakeBwuHandler {
    medium: Medium,
    records: FakeBwuHandlerHandle,
}

impl FakeBwuHandler {
    pub fn new(medium: Medium, records: FakeBwuHandlerHandle) -> Self {
        Self { medium, records }
    }

    /// A fresh shared records handle.
    pub fn records() -> FakeBwuHandlerHandle {
        Arc::new(Mutex::new(FakeBwuHandlerRecords::default()))
    }

    fn path_available_bytes(&self, upgrade_service_id: &str) -> Vec<u8> {
        match self.medium {
            Medium::Bluetooth => {
                for_bwu_bluetooth_path_available(upgrade_service_id, "01:02:03:04:05:06")
            }
            Medium::WifiLan => for_bwu_wifi_lan_path_available(&[ServiceAddress {
                address: vec![b'A', b'B', b'C', b'D'],
                port: 1234,
            }]),
            Medium::WebRtc | Medium::WebRtcNonCellular => {
                for_bwu_webrtc_path_available("peer-id", pb::LocationHint::default())
            }
            Medium::WifiHotspot => {
                let credentials = pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiHotspotCredentials {
                    ssid: Some("Direct-357a2d8c".to_string()),
                    password: Some("b592f7d3".to_string()),
                    port: Some(1234),
                    frequency: Some(2412),
                    gateway: Some("123.234.23.1".to_string()),
                    address_candidates: vec![
                        pb::ServiceAddress {
                            ip_address: Some(vec![
                                0xfe, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4d, 0xb2, 0xb3,
                                0x5c, 0x22, 0x03, 0x98, 0xa1,
                            ]),
                            port: Some(1234),
                        },
                        pb::ServiceAddress {
                            ip_address: Some(vec![0x7b, 0xea, 0x17, 0x01]),
                            port: Some(2412),
                        },
                    ],
                };
                for_bwu_wifi_hotspot_path_available(credentials, false)
            }
            Medium::WifiDirect => for_bwu_wifi_direct_path_available(
                "", "", 2143, 2412, false, "123.234.23.1", "NC-WifiDirectTest", "b592f7d3",
            ),
            _ => Vec::new(),
        }
    }
}

impl MediumBwuHandler for FakeBwuHandler {
    fn handle_initialize_upgraded_medium_for_endpoint(
        &mut self,
        client: &ClientProxy,
        upgrade_service_id: &str,
        endpoint_id: &str,
    ) -> Vec<u8> {
        self.records
            .lock()
            .unwrap()
            .handle_initialize_calls
            .push(InputData {
                client_id: client.client_id(),
                service_id: Some(upgrade_service_id.to_string()),
                endpoint_id: Some(endpoint_id.to_string()),
            });
        self.path_available_bytes(upgrade_service_id)
    }

    fn handle_revert_initiator_state_for_service(&mut self, upgrade_service_id: &str) {
        self.records
            .lock()
            .unwrap()
            .handle_revert_calls
            .push(InputData {
                service_id: Some(upgrade_service_id.to_string()),
                ..Default::default()
            });
    }

    fn create_upgraded_endpoint_channel(
        &mut self,
        client: &ClientProxy,
        service_id: &str,
        endpoint_id: &str,
        _upgrade_path_info: &UpgradePathInfo,
    ) -> Option<Arc<dyn EndpointChannel>> {
        self.records.lock().unwrap().create_calls.push(InputData {
            client_id: client.client_id(),
            service_id: Some(service_id.to_string()),
            endpoint_id: Some(endpoint_id.to_string()),
        });
        Some(Arc::new(FakeEndpointChannel::new(self.medium, service_id)))
    }

    fn get_upgrade_medium(&self) -> Medium {
        self.medium
    }

    fn on_endpoint_disconnect(&mut self, client: &ClientProxy, endpoint_id: &str) {
        self.records
            .lock()
            .unwrap()
            .disconnect_calls
            .push(InputData {
                client_id: client.client_id(),
                endpoint_id: Some(endpoint_id.to_string()),
                ..Default::default()
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::{from_bytes, get_frame_type};
    use crate::proto::v1_frame::FrameType;

    #[test]
    fn channel_records_close_pause_and_write() {
        let ch = FakeEndpointChannel::new(Medium::Bluetooth, "A");
        assert!(!ch.is_closed());
        assert!(!ch.wrote_anything());
        assert_eq!(ch.write(b"x"), Exception::Success);
        assert!(ch.wrote_anything());

        ch.pause();
        assert!(EndpointChannel::is_paused(&ch));
        ch.resume();
        assert!(!EndpointChannel::is_paused(&ch));

        ch.set_write_output(Exception::Io);
        assert_eq!(ch.write(b"y"), Exception::Io);

        ch.close_with_reason(DisconnectionReason::Upgraded);
        assert!(ch.is_closed());
        assert_eq!(ch.disconnection_reason(), DisconnectionReason::Upgraded);
    }

    #[test]
    fn handler_records_and_emits_valid_path_available_frames() {
        let client = ClientProxy::default();
        for medium in [
            Medium::Bluetooth,
            Medium::WifiLan,
            Medium::WebRtc,
            Medium::WifiHotspot,
            Medium::WifiDirect,
        ] {
            let mut handler = FakeBwuHandler::new(medium, FakeBwuHandler::records());
            let bytes = handler.handle_initialize_upgraded_medium_for_endpoint(
                &client,
                "ServiceA_UPGRADE",
                "Endpoint1",
            );
            assert!(!bytes.is_empty(), "medium {medium:?} should emit a frame");
            // The emitted bytes are a valid BANDWIDTH_UPGRADE_NEGOTIATION frame.
            let frame = from_bytes(&bytes).expect("path-available frame should parse");
            assert_eq!(
                get_frame_type(&frame),
                FrameType::BandwidthUpgradeNegotiation
            );
            assert_eq!(
                handler.records.lock().unwrap().handle_initialize_calls.len(),
                1
            );
        }
    }
}
