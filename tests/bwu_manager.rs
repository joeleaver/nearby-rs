//! Port of Google "Nearby" `bwu_manager_test.cc`.
//!
//! All 23 portable oracle cases are ported: the canonical upgrade flow
//! (`InitiateBwu_Success`, both `support_multiple_bwu_mediums` values), the
//! initiate-error guards, the unexpected/out-of-order frame cases, the two
//! frame-blocking cases, and the full revert/disconnect cluster (incl. the
//! flag-disabled off-by-one, multi-service/multi-medium independence, the
//! upgrade-failure revert, and responder hotspot/wifi-direct/wifi-lan revert).
//!
//! The 9 unported cases are out of scope: the dynamic-role-switch tests
//! (`InitiateBwu_NeedToSwitchRole_*`, `ProcessUpgradePathRequest_*`,
//! `OnIncomingConnection_EndpointAliasesToLastEndpointId`), `AllowToUpgradeMedium`
//! (relies on the constructor's `InitBwuHandlers` auto-creating handlers from
//! sim `Mediums`, which the explicit-handler port omits), and the two empty
//! placeholder tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nearby_rs::bwu::channel::{DisconnectionReason, SafeDisconnectionResult};
use nearby_rs::bwu::testing::{
    notify_bwu_manager_of_incoming_connection, FakeBwuHandler, FakeBwuHandlerHandle,
    FakeEndpointChannel,
};
use nearby_rs::bwu::{
    BaseBwuHandler, BwuConfig, BwuHandler, BwuManager, ClientProxy, EndpointChannelManager,
};
use nearby_rs::frames::{
    for_bwu_failure, for_bwu_last_write, for_bwu_safe_to_close, for_bwu_wifi_direct_path_available,
    for_bwu_wifi_hotspot_path_available, for_bwu_wifi_lan_path_available, from_bytes, Exception,
    ServiceAddress,
};
use nearby_rs::mediums::Medium;
use nearby_rs::proto as pb;

const SERVICE_A: &str = "ServiceA";
const SERVICE_A_UPGRADE: &str = "ServiceA_UPGRADE";
const SERVICE_B: &str = "ServiceB";
const SERVICE_B_UPGRADE: &str = "ServiceB_UPGRADE";
const ENDPOINT_1: &str = "Endpoint1";
const ENDPOINT_2: &str = "Endpoint2";
const ENDPOINT_3: &str = "Endpoint3";
const ENDPOINT_4: &str = "Endpoint4";
const ENDPOINT_5: &str = "Endpoint5";

struct Fixture {
    client: ClientProxy,
    ecm: Arc<Mutex<EndpointChannelManager>>,
    bwu: BwuManager,
    web_rtc: FakeBwuHandlerHandle,
    wifi_lan: FakeBwuHandlerHandle,
    wifi_direct: FakeBwuHandlerHandle,
    wifi_hotspot: FakeBwuHandlerHandle,
}

fn make_handler(medium: Medium, records: &FakeBwuHandlerHandle) -> Box<dyn BwuHandler> {
    Box::new(BaseBwuHandler::new(FakeBwuHandler::new(
        medium,
        records.clone(),
    )))
}

fn fixture(support_multiple_bwu_mediums: bool) -> Fixture {
    let ecm = Arc::new(Mutex::new(EndpointChannelManager::new()));
    let web_rtc = FakeBwuHandler::records();
    let wifi_lan = FakeBwuHandler::records();
    let wifi_direct = FakeBwuHandler::records();
    let wifi_hotspot = FakeBwuHandler::records();

    let mut handlers: HashMap<Medium, Box<dyn BwuHandler>> = HashMap::new();
    handlers.insert(Medium::WebRtc, make_handler(Medium::WebRtc, &web_rtc));
    handlers.insert(Medium::WifiLan, make_handler(Medium::WifiLan, &wifi_lan));
    handlers.insert(
        Medium::WifiDirect,
        make_handler(Medium::WifiDirect, &wifi_direct),
    );
    handlers.insert(
        Medium::WifiHotspot,
        make_handler(Medium::WifiHotspot, &wifi_hotspot),
    );

    let mut bwu = BwuManager::new(
        ecm.clone(),
        handlers,
        BwuConfig {
            support_multiple_bwu_mediums,
            ..Default::default()
        },
    );
    bwu.make_single_threaded_for_testing();

    Fixture {
        client: ClientProxy::new(0, "LocalEndpoint"),
        ecm,
        bwu,
        web_rtc,
        wifi_lan,
        wifi_direct,
        wifi_hotspot,
    }
}

impl Fixture {
    /// Sets up an accepted pre-upgrade endpoint channel (the discoverer side).
    fn create_initial_endpoint(
        &mut self,
        service_id: &str,
        endpoint_id: &str,
        medium: Medium,
    ) -> Arc<FakeEndpointChannel> {
        self.client
            .on_connection_initiated(endpoint_id, false, false);
        self.client.on_connection_accepted(endpoint_id);
        let channel = Arc::new(FakeEndpointChannel::new(medium, service_id));
        self.ecm.lock().unwrap().register_channel_for_endpoint(
            &self.client,
            endpoint_id,
            channel.clone(),
        );
        channel
    }

    fn current_medium(&self, endpoint_id: &str) -> Medium {
        self.ecm
            .lock()
            .unwrap()
            .get_channel_for_endpoint(endpoint_id)
            .unwrap()
            .medium()
    }

    fn feed(&mut self, frame_bytes: Vec<u8>, endpoint_id: &str, medium: Medium) {
        let frame = from_bytes(&frame_bytes).expect("frame should parse");
        self.bwu
            .on_incoming_frame(&frame, endpoint_id, &mut self.client, medium);
    }

    fn records_for(&self, medium: Medium) -> &FakeBwuHandlerHandle {
        match medium {
            Medium::WebRtc => &self.web_rtc,
            Medium::WifiLan => &self.wifi_lan,
            Medium::WifiDirect => &self.wifi_direct,
            Medium::WifiHotspot => &self.wifi_hotspot,
            _ => panic!("no fake handler for {medium:?}"),
        }
    }

    /// Drives a full successful upgrade (the `[5]` flow) end to end.
    fn fully_upgrade_endpoint(
        &mut self,
        endpoint_id: &str,
        initial_medium: Medium,
        upgrade_medium: Medium,
    ) -> Arc<FakeEndpointChannel> {
        self.bwu
            .initiate_bwu_for_endpoint(&mut self.client, endpoint_id, upgrade_medium);
        let records = self.records_for(upgrade_medium).clone();
        let index = records.lock().unwrap().handle_initialize_calls.len() - 1;
        let upgraded = notify_bwu_manager_of_incoming_connection(
            &records,
            upgrade_medium,
            index,
            &mut self.bwu,
            &mut self.client,
        );
        self.feed(for_bwu_last_write(), endpoint_id, initial_medium);
        self.feed(for_bwu_safe_to_close(), endpoint_id, initial_medium);
        upgraded
    }

    fn unregister(&mut self, endpoint_id: &str) {
        self.ecm.lock().unwrap().unregister_channel_for_endpoint(
            endpoint_id,
            DisconnectionReason::LocalDisconnection,
            SafeDisconnectionResult::SafeDisconnection,
        );
    }

    fn on_endpoint_disconnect(&mut self, service_id: &str, endpoint_id: &str) {
        self.bwu.on_endpoint_disconnect(
            &mut self.client,
            service_id,
            endpoint_id,
            DisconnectionReason::LocalDisconnection,
        );
    }
}

/// Builds a responder-side `UPGRADE_PATH_AVAILABLE` frame (the local device is
/// the responder), with `supports_client_introduction_ack = false` so the
/// responder path completes without an ack read.
fn responder_path_available_frame(medium: Medium) -> pb::OfflineFrame {
    let bytes = match medium {
        Medium::WifiDirect => for_bwu_wifi_direct_path_available(
            "",
            "",
            2143,
            2412,
            false,
            "123.234.23.1",
            "NC-WifiDirectTest",
            "b592f7d3",
        ),
        Medium::WifiHotspot => {
            let creds = pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiHotspotCredentials {
                ssid: Some("Direct-357a2d8c".to_string()),
                password: Some("b592f7d3".to_string()),
                port: Some(1234),
                frequency: Some(2412),
                gateway: Some("123.234.23.1".to_string()),
                ..Default::default()
            };
            for_bwu_wifi_hotspot_path_available(creds, false)
        }
        Medium::WifiLan => for_bwu_wifi_lan_path_available(&[ServiceAddress {
            address: vec![b'A', b'B', b'C', b'D'],
            port: 1234,
        }]),
        _ => panic!("unsupported responder medium {medium:?}"),
    };
    let mut frame = from_bytes(&bytes).expect("frame should parse");
    if let Some(info) = frame
        .v1
        .as_mut()
        .and_then(|v1| v1.bandwidth_upgrade_negotiation.as_mut())
        .and_then(|b| b.upgrade_path_info.as_mut())
    {
        info.supports_client_introduction_ack = Some(false);
    }
    frame
}

fn web_rtc_upgrade_path_info() -> pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
    pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
        medium: Some(
            pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium::WebRtc as i32,
        ),
        ..Default::default()
    }
}

fn initiate_bwu_success(support_multiple: bool) {
    let mut f = fixture(support_multiple);
    let initial = f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);

    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WebRtc);

    // Only the WEB_RTC handler was initialized, with the WRAPPED service id.
    {
        let r = f.web_rtc.lock().unwrap();
        assert_eq!(r.handle_initialize_calls.len(), 1);
        assert_eq!(
            r.handle_initialize_calls[0].service_id.as_deref(),
            Some("ServiceA_UPGRADE")
        );
        assert_eq!(
            r.handle_initialize_calls[0].endpoint_id.as_deref(),
            Some(ENDPOINT_1)
        );
    }
    assert!(f
        .wifi_lan
        .lock()
        .unwrap()
        .handle_initialize_calls
        .is_empty());
    assert!(f
        .wifi_hotspot
        .lock()
        .unwrap()
        .handle_initialize_calls
        .is_empty());
    assert!(f
        .wifi_direct
        .lock()
        .unwrap()
        .handle_initialize_calls
        .is_empty());
    assert!(f.bwu.is_upgrade_ongoing(ENDPOINT_1));

    // The channel has NOT been swapped yet (still Bluetooth).
    assert_eq!(f.current_medium(ENDPOINT_1), Medium::Bluetooth);

    // The remote dials in to the new medium and sends CLIENT_INTRODUCTION.
    let upgraded = notify_bwu_manager_of_incoming_connection(
        &f.web_rtc,
        Medium::WebRtc,
        0,
        &mut f.bwu,
        &mut f.client,
    );

    // The channel is now swapped to the upgraded one, and it is paused until the
    // old channel is drained.
    assert_eq!(f.current_medium(ENDPOINT_1), Medium::WebRtc);
    assert!(upgraded.is_paused());
    assert!(!initial.is_closed());

    // Drain + close the prior channel.
    f.feed(for_bwu_last_write(), ENDPOINT_1, Medium::Bluetooth);
    f.feed(for_bwu_safe_to_close(), ENDPOINT_1, Medium::Bluetooth);

    assert!(!upgraded.is_paused());
    assert!(initial.is_closed());
    assert_eq!(
        initial.disconnection_reason(),
        DisconnectionReason::Upgraded
    );
}

/// A bare remote retry (BANDWIDTH_UPGRADE_RETRY with no preceding UPGRADE_FAILURE):
/// the `in_progress_upgrades` guard would drop a re-offer, so the initiator calls
/// `reset_upgrade_for_endpoint` to clear it, then re-initiates. The re-offer must
/// re-run `handle_initialize` (re-sending UPGRADE_PATH_AVAILABLE, reusing the
/// standing medium) WITHOUT ever calling revert — so a SoftAP that took seconds to
/// bring up is reused, not torn down and rebuilt. Covers #23.
#[test]
fn reset_upgrade_reoffers_without_reverting_the_medium() {
    let mut f = fixture(false);
    let _initial = f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);

    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WebRtc);
    assert!(f.bwu.is_upgrade_ongoing(ENDPOINT_1));
    assert_eq!(f.web_rtc.lock().unwrap().handle_initialize_calls.len(), 1);

    // A re-initiate WITHOUT a reset is dropped by the in-progress guard (the #23 bug).
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WebRtc);
    assert_eq!(
        f.web_rtc.lock().unwrap().handle_initialize_calls.len(),
        1,
        "the in-progress guard drops a re-offer"
    );

    // Reset clears the in-progress flag WITHOUT reverting the handler...
    f.bwu.reset_upgrade_for_endpoint(ENDPOINT_1);
    assert!(!f.bwu.is_upgrade_ongoing(ENDPOINT_1));

    // ...so a re-initiate now re-sends the offer (handle_initialize runs again).
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WebRtc);
    assert!(f.bwu.is_upgrade_ongoing(ENDPOINT_1));
    let r = f.web_rtc.lock().unwrap();
    assert_eq!(
        r.handle_initialize_calls.len(),
        2,
        "the re-offer re-initializes the (reused) medium"
    );
    assert!(
        r.handle_revert_calls.is_empty(),
        "reset must NOT revert/tear down the medium (no SoftAP churn)"
    );
}

#[test]
fn initiate_bwu_success_flag_disabled() {
    initiate_bwu_success(false);
}

#[test]
fn initiate_bwu_success_flag_enabled() {
    initiate_bwu_success(true);
}

#[test]
fn receive_unexpected_safe_to_close_does_not_crash() {
    let mut f = fixture(true);
    f.feed(for_bwu_safe_to_close(), ENDPOINT_1, Medium::Bluetooth);
    assert!(!f.bwu.is_upgrade_ongoing(ENDPOINT_1));
}

#[test]
fn receive_unexpected_last_write_does_not_crash() {
    let mut f = fixture(true);
    f.feed(for_bwu_last_write(), ENDPOINT_1, Medium::Bluetooth);
    assert!(!f.bwu.is_upgrade_ongoing(ENDPOINT_1));
}

#[test]
fn receive_early_last_write_completes_upgrade() {
    let mut f = fixture(true);
    let initial = f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WebRtc);
    assert!(f.bwu.is_upgrade_ongoing(ENDPOINT_1));

    // LAST_WRITE arrives BEFORE the incoming upgraded connection (early).
    f.feed(for_bwu_last_write(), ENDPOINT_1, Medium::Bluetooth);
    let upgraded = notify_bwu_manager_of_incoming_connection(
        &f.web_rtc,
        Medium::WebRtc,
        0,
        &mut f.bwu,
        &mut f.client,
    );
    f.feed(for_bwu_safe_to_close(), ENDPOINT_1, Medium::Bluetooth);

    assert!(!upgraded.is_paused());
    assert!(initial.is_closed());
    assert_eq!(
        initial.disconnection_reason(),
        DisconnectionReason::Upgraded
    );
}

#[test]
fn receive_unexpected_last_write_before_upgrade_does_not_wedge() {
    let mut f = fixture(true);

    // A totally-unexpected LAST_WRITE before the endpoint even exists.
    f.feed(for_bwu_last_write(), ENDPOINT_1, Medium::Bluetooth);

    let initial = f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WebRtc);
    let upgraded = notify_bwu_manager_of_incoming_connection(
        &f.web_rtc,
        Medium::WebRtc,
        0,
        &mut f.bwu,
        &mut f.client,
    );

    // A second LAST_WRITE during the upgrade, then SAFE_TO_CLOSE.
    f.feed(for_bwu_last_write(), ENDPOINT_1, Medium::Bluetooth);
    f.feed(for_bwu_safe_to_close(), ENDPOINT_1, Medium::Bluetooth);

    assert!(!upgraded.is_paused());
    assert!(initial.is_closed());
}

fn upgrade_path_available_frame() -> Vec<u8> {
    for_bwu_wifi_lan_path_available(&[ServiceAddress {
        address: vec![1, 2, 3, 4],
        port: 1234,
    }])
}

#[test]
fn block_bwu_frame_before_accept() {
    let mut f = fixture(true);
    // Register a channel WITHOUT accepting the connection.
    f.ecm.lock().unwrap().register_channel_for_endpoint(
        &f.client,
        ENDPOINT_2,
        Arc::new(FakeEndpointChannel::new(Medium::Bluetooth, SERVICE_A)),
    );

    f.feed(
        upgrade_path_available_frame(),
        ENDPOINT_2,
        Medium::Bluetooth,
    );

    // The frame is dropped because the connection isn't accepted yet.
    assert!(!f.bwu.is_upgrade_ongoing(ENDPOINT_2));
}

#[test]
fn block_bwu_frame_from_advertiser() {
    let mut f = fixture(true);
    // A fully-accepted INCOMING (advertiser) connection.
    f.client.on_connection_initiated(ENDPOINT_2, true, false);
    f.client.local_endpoint_accepted_connection(ENDPOINT_2);
    f.client.remote_endpoint_accepted_connection(ENDPOINT_2);
    assert!(f.client.is_connection_accepted(ENDPOINT_2));
    f.client.on_connection_accepted(ENDPOINT_2);
    assert!(f.client.is_connected_to_endpoint(ENDPOINT_2));
    f.ecm.lock().unwrap().register_channel_for_endpoint(
        &f.client,
        ENDPOINT_2,
        Arc::new(FakeEndpointChannel::new(Medium::Bluetooth, SERVICE_A)),
    );

    f.feed(
        upgrade_path_available_frame(),
        ENDPOINT_2,
        Medium::Bluetooth,
    );

    // The advertiser must not act as the BWU responder.
    assert!(!f.bwu.is_upgrade_ongoing(ENDPOINT_2));
}

// --- initiate-error cluster ([6]-[10]) -------------------------------------

#[test]
fn dont_upgrade_if_already_connected_over_requested_medium() {
    let mut f = fixture(true);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.fully_upgrade_endpoint(ENDPOINT_1, Medium::Bluetooth, Medium::WebRtc);
    assert_eq!(f.web_rtc.lock().unwrap().handle_initialize_calls.len(), 1);

    // Already connected over WEB_RTC → no new initialize.
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WebRtc);
    assert_eq!(f.web_rtc.lock().unwrap().handle_initialize_calls.len(), 1);
}

#[test]
fn dont_upgrade_from_wifi_lan_to_wifi_hotspot() {
    let mut f = fixture(true);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::WifiLan);
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WifiHotspot);
    assert!(f
        .wifi_hotspot
        .lock()
        .unwrap()
        .handle_initialize_calls
        .is_empty());
}

#[test]
fn no_initial_medium() {
    let mut f = fixture(true);
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WifiHotspot);
    for r in [&f.web_rtc, &f.wifi_lan, &f.wifi_direct, &f.wifi_hotspot] {
        assert!(r.lock().unwrap().handle_initialize_calls.is_empty());
    }
}

#[test]
fn upgrade_already_in_progress() {
    let mut f = fixture(true);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WebRtc);
    assert_eq!(f.web_rtc.lock().unwrap().handle_initialize_calls.len(), 1);

    // A second initiate while the first is in progress is ignored.
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WifiLan);
    assert_eq!(f.web_rtc.lock().unwrap().handle_initialize_calls.len(), 1);
    assert!(f
        .wifi_lan
        .lock()
        .unwrap()
        .handle_initialize_calls
        .is_empty());
}

#[test]
fn failed_to_write_upgrade_path_available_frame() {
    let mut f = fixture(true);
    let initial = f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    // Writing the UPGRADE_PATH_AVAILABLE frame will fail.
    initial.set_write_output(Exception::Io);

    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WebRtc);
    // The handler WAS initialized (before the write attempt)...
    assert_eq!(f.web_rtc.lock().unwrap().handle_initialize_calls.len(), 1);
    // ...but the write failed, so no in-progress upgrade was recorded.
    assert!(!f.bwu.is_upgrade_ongoing(ENDPOINT_1));
    assert_eq!(f.current_medium(ENDPOINT_1), Medium::Bluetooth);

    // A later incoming upgraded connection is dropped (no recorded upgrade).
    let _upgraded = notify_bwu_manager_of_incoming_connection(
        &f.web_rtc,
        Medium::WebRtc,
        0,
        &mut f.bwu,
        &mut f.client,
    );
    assert_eq!(f.current_medium(ENDPOINT_1), Medium::Bluetooth);
}

// --- revert / disconnect cluster ([11]-[20]) -------------------------------

#[test]
fn revert_on_disconnect_multiple_endpoints_flag_enabled() {
    let mut f = fixture(true);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_2, Medium::Bluetooth);
    f.fully_upgrade_endpoint(ENDPOINT_1, Medium::Bluetooth, Medium::WebRtc);
    f.fully_upgrade_endpoint(ENDPOINT_2, Medium::Bluetooth, Medium::WebRtc);

    f.unregister(ENDPOINT_1);
    f.on_endpoint_disconnect(SERVICE_A_UPGRADE, ENDPOINT_1);
    {
        let r = f.web_rtc.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 1);
        assert_eq!(
            r.disconnect_calls[0].endpoint_id.as_deref(),
            Some(ENDPOINT_1)
        );
        // Not the last endpoint for the medium+service → no handler revert.
        assert!(r.handle_revert_calls.is_empty());
    }

    f.unregister(ENDPOINT_2);
    f.on_endpoint_disconnect(SERVICE_A_UPGRADE, ENDPOINT_2);
    {
        let r = f.web_rtc.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 2);
        assert_eq!(
            r.disconnect_calls[1].endpoint_id.as_deref(),
            Some(ENDPOINT_2)
        );
        assert_eq!(r.handle_revert_calls.len(), 1);
        assert_eq!(
            r.handle_revert_calls[0].service_id.as_deref(),
            Some(SERVICE_A_UPGRADE)
        );
    }
}

#[test]
fn revert_on_disconnect_multiple_endpoints_flag_disabled() {
    let mut f = fixture(false);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_2, Medium::Bluetooth);
    f.fully_upgrade_endpoint(ENDPOINT_1, Medium::Bluetooth, Medium::WebRtc);
    f.fully_upgrade_endpoint(ENDPOINT_2, Medium::Bluetooth, Medium::WebRtc);

    // Off-by-one: revert fires at <=1 connected endpoint, while ep2 still remains.
    f.unregister(ENDPOINT_1);
    f.on_endpoint_disconnect(SERVICE_A_UPGRADE, ENDPOINT_1);
    {
        let r = f.web_rtc.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 1);
        assert_eq!(r.handle_revert_calls.len(), 1);
        assert_eq!(
            r.handle_revert_calls[0].service_id.as_deref(),
            Some(SERVICE_A_UPGRADE)
        );
    }

    // Medium already reverted (global medium is now UNKNOWN) → second is a no-op.
    f.unregister(ENDPOINT_2);
    f.on_endpoint_disconnect(SERVICE_A_UPGRADE, ENDPOINT_2);
    {
        let r = f.web_rtc.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 1);
        assert_eq!(r.handle_revert_calls.len(), 1);
    }
}

#[test]
fn revert_on_disconnect_multiple_services_flag_enabled() {
    let mut f = fixture(true);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.create_initial_endpoint(SERVICE_B, ENDPOINT_2, Medium::Bluetooth);
    f.fully_upgrade_endpoint(ENDPOINT_1, Medium::Bluetooth, Medium::WifiLan);
    f.fully_upgrade_endpoint(ENDPOINT_2, Medium::Bluetooth, Medium::WifiLan);
    assert_eq!(f.ecm.lock().unwrap().get_connected_endpoints_count(), 2);

    f.unregister(ENDPOINT_1);
    f.on_endpoint_disconnect(SERVICE_A_UPGRADE, ENDPOINT_1);
    {
        let r = f.wifi_lan.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 1);
        assert_eq!(r.handle_revert_calls.len(), 1);
        assert_eq!(
            r.handle_revert_calls[0].service_id.as_deref(),
            Some(SERVICE_A_UPGRADE)
        );
    }

    f.unregister(ENDPOINT_2);
    f.on_endpoint_disconnect(SERVICE_B_UPGRADE, ENDPOINT_2);
    {
        let r = f.wifi_lan.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 2);
        assert_eq!(r.handle_revert_calls.len(), 2);
        assert_eq!(
            r.handle_revert_calls[1].service_id.as_deref(),
            Some(SERVICE_B_UPGRADE)
        );
    }
}

#[test]
fn revert_on_disconnect_multiple_services_flag_disabled() {
    let mut f = fixture(false);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.create_initial_endpoint(SERVICE_B, ENDPOINT_2, Medium::Bluetooth);
    f.fully_upgrade_endpoint(ENDPOINT_1, Medium::Bluetooth, Medium::WifiLan);
    f.fully_upgrade_endpoint(ENDPOINT_2, Medium::Bluetooth, Medium::WifiLan);

    // Flag-disabled reverts ALL tracked services at once (2 reverts) at <=1.
    f.unregister(ENDPOINT_1);
    f.on_endpoint_disconnect(SERVICE_A_UPGRADE, ENDPOINT_1);
    {
        let r = f.wifi_lan.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 1);
        assert_eq!(r.handle_revert_calls.len(), 2);
    }

    f.unregister(ENDPOINT_2);
    f.on_endpoint_disconnect(SERVICE_B_UPGRADE, ENDPOINT_2);
    {
        let r = f.wifi_lan.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 1);
        assert_eq!(r.handle_revert_calls.len(), 2);
    }
}

#[test]
fn revert_on_disconnect_multiple_services_and_endpoints_flag_enabled() {
    let mut f = fixture(true);
    // Service A: ep1, ep2.  Service B: ep3, ep4, ep5.
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_2, Medium::Bluetooth);
    f.create_initial_endpoint(SERVICE_B, ENDPOINT_3, Medium::Bluetooth);
    f.create_initial_endpoint(SERVICE_B, ENDPOINT_4, Medium::Bluetooth);
    f.create_initial_endpoint(SERVICE_B, ENDPOINT_5, Medium::Bluetooth);
    f.fully_upgrade_endpoint(ENDPOINT_1, Medium::Bluetooth, Medium::WebRtc);
    f.fully_upgrade_endpoint(ENDPOINT_4, Medium::Bluetooth, Medium::WifiHotspot);
    f.fully_upgrade_endpoint(ENDPOINT_5, Medium::Bluetooth, Medium::WifiDirect);
    f.fully_upgrade_endpoint(ENDPOINT_2, Medium::Bluetooth, Medium::WifiLan);
    f.fully_upgrade_endpoint(ENDPOINT_3, Medium::Bluetooth, Medium::WifiLan);

    // ep1 / service A / WEB_RTC (last WEB_RTC for A).
    f.unregister(ENDPOINT_1);
    f.on_endpoint_disconnect(SERVICE_A_UPGRADE, ENDPOINT_1);
    {
        let r = f.web_rtc.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 1);
        assert_eq!(r.handle_revert_calls.len(), 1);
        assert_eq!(
            r.handle_revert_calls[0].service_id.as_deref(),
            Some(SERVICE_A_UPGRADE)
        );
    }

    // ep2 / service A / WIFI_LAN (last LAN for A).
    f.unregister(ENDPOINT_2);
    f.on_endpoint_disconnect(SERVICE_A_UPGRADE, ENDPOINT_2);
    {
        let r = f.wifi_lan.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 1);
        assert_eq!(r.handle_revert_calls.len(), 1);
        assert_eq!(
            r.handle_revert_calls[0].service_id.as_deref(),
            Some(SERVICE_A_UPGRADE)
        );
    }

    // ep3 / service B / WIFI_LAN (last LAN for B).
    f.unregister(ENDPOINT_3);
    f.on_endpoint_disconnect(SERVICE_B_UPGRADE, ENDPOINT_3);
    {
        let r = f.wifi_lan.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 2);
        assert_eq!(r.handle_revert_calls.len(), 2);
        assert_eq!(
            r.handle_revert_calls[1].service_id.as_deref(),
            Some(SERVICE_B_UPGRADE)
        );
    }

    // ep4 / service B / WIFI_HOTSPOT.
    f.unregister(ENDPOINT_4);
    f.on_endpoint_disconnect(SERVICE_B_UPGRADE, ENDPOINT_4);
    {
        let r = f.wifi_hotspot.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 1);
        assert_eq!(r.handle_revert_calls.len(), 1);
        assert_eq!(
            r.handle_revert_calls[0].service_id.as_deref(),
            Some(SERVICE_B_UPGRADE)
        );
    }

    // ep5 / service B / WIFI_DIRECT.
    f.unregister(ENDPOINT_5);
    f.on_endpoint_disconnect(SERVICE_B_UPGRADE, ENDPOINT_5);
    {
        let r = f.wifi_direct.lock().unwrap();
        assert_eq!(r.disconnect_calls.len(), 1);
        assert_eq!(r.handle_revert_calls.len(), 1);
        assert_eq!(
            r.handle_revert_calls[0].service_id.as_deref(),
            Some(SERVICE_B_UPGRADE)
        );
    }

    // WEB_RTC untouched after the first disconnect.
    assert_eq!(f.web_rtc.lock().unwrap().disconnect_calls.len(), 1);
    assert_eq!(f.web_rtc.lock().unwrap().handle_revert_calls.len(), 1);
}

fn setup_upgrade_failure(support_multiple: bool) -> Fixture {
    let mut f = fixture(support_multiple);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_2, Medium::Bluetooth);
    f.fully_upgrade_endpoint(ENDPOINT_1, Medium::Bluetooth, Medium::WebRtc);
    f.fully_upgrade_endpoint(ENDPOINT_2, Medium::Bluetooth, Medium::WebRtc);
    f.create_initial_endpoint(SERVICE_B, ENDPOINT_3, Medium::Bluetooth);
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_3, Medium::WebRtc);
    // index 2 = ep3's initialize call on the web_rtc handler.
    notify_bwu_manager_of_incoming_connection(
        &f.web_rtc,
        Medium::WebRtc,
        2,
        &mut f.bwu,
        &mut f.client,
    );
    f
}

#[test]
fn revert_on_upgrade_failure_flag_enabled() {
    let mut f = setup_upgrade_failure(true);
    let failure = for_bwu_failure(web_rtc_upgrade_path_info());
    f.feed(failure, ENDPOINT_3, Medium::WebRtc);
    let r = f.web_rtc.lock().unwrap();
    assert_eq!(r.handle_revert_calls.len(), 1);
    assert_eq!(
        r.handle_revert_calls[0].service_id.as_deref(),
        Some(SERVICE_B_UPGRADE)
    );
}

#[test]
fn revert_on_upgrade_failure_flag_disabled() {
    let mut f = setup_upgrade_failure(false);
    let failure = for_bwu_failure(web_rtc_upgrade_path_info());
    f.feed(failure, ENDPOINT_3, Medium::WebRtc);
    // Flag disabled + other connected endpoints → no revert.
    assert!(f.web_rtc.lock().unwrap().handle_revert_calls.is_empty());
}

#[test]
fn revert_on_disconnect_wifi_direct_responder() {
    let mut f = fixture(true);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    // Receiving UPGRADE_PATH_AVAILABLE makes the local device the responder.
    let frame = responder_path_available_frame(Medium::WifiDirect);
    f.bwu
        .on_incoming_frame(&frame, ENDPOINT_1, &mut f.client, Medium::Bluetooth);

    // Responder disconnect uses the RAW service id.
    f.on_endpoint_disconnect(SERVICE_A, ENDPOINT_1);
    let r = f.wifi_direct.lock().unwrap();
    assert_eq!(r.disconnect_calls.len(), 1);
    assert_eq!(
        r.disconnect_calls[0].endpoint_id.as_deref(),
        Some(ENDPOINT_1)
    );
    // Responder reverts for WIFI_DIRECT.
    assert_eq!(r.handle_revert_calls.len(), 1);
}

#[test]
fn revert_on_disconnect_hotspot_responder() {
    let mut f = fixture(true);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    let frame = responder_path_available_frame(Medium::WifiHotspot);
    f.bwu
        .on_incoming_frame(&frame, ENDPOINT_1, &mut f.client, Medium::Bluetooth);

    f.on_endpoint_disconnect(SERVICE_A, ENDPOINT_1);
    // Responder reverts for WIFI_HOTSPOT.
    assert_eq!(f.wifi_hotspot.lock().unwrap().handle_revert_calls.len(), 1);
}

#[test]
fn revert_on_disconnect_wifi_lan_responder() {
    let mut f = fixture(true);
    f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    let frame = responder_path_available_frame(Medium::WifiLan);
    f.bwu
        .on_incoming_frame(&frame, ENDPOINT_1, &mut f.client, Medium::Bluetooth);

    f.on_endpoint_disconnect(SERVICE_A, ENDPOINT_1);
    // Responder does NOT revert for WIFI_LAN (only Hotspot/WifiDirect).
    assert!(f.wifi_lan.lock().unwrap().handle_revert_calls.is_empty());
}
