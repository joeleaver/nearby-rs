//! Port of the core cases of Google "Nearby" `bwu_manager_test.cc`.
//!
//! Covered here: the canonical upgrade flow (`InitiateBwu_Success`, run with both
//! `support_multiple_bwu_mediums` values), the no-crash unexpected-frame cases,
//! the out-of-order LAST_WRITE cases, and the two frame-blocking cases. The
//! revert/disconnect cluster and the initiate-error cluster land in follow-ups;
//! the dynamic-role-switch tests are intentionally omitted (out of scope).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use nearby_rs::bwu::channel::DisconnectionReason;
use nearby_rs::bwu::testing::{
    notify_bwu_manager_of_incoming_connection, FakeBwuHandler, FakeBwuHandlerHandle,
    FakeEndpointChannel,
};
use nearby_rs::bwu::{
    BaseBwuHandler, BwuConfig, BwuHandler, BwuManager, ClientProxy, EndpointChannelManager,
};
use nearby_rs::frames::{
    for_bwu_last_write, for_bwu_safe_to_close, for_bwu_wifi_lan_path_available, from_bytes,
    Exception, ServiceAddress,
};
use nearby_rs::mediums::Medium;

const SERVICE_A: &str = "ServiceA";
const ENDPOINT_1: &str = "Endpoint1";
const ENDPOINT_2: &str = "Endpoint2";

struct Fixture {
    client: ClientProxy,
    ecm: Rc<RefCell<EndpointChannelManager>>,
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
    let ecm = Rc::new(RefCell::new(EndpointChannelManager::new()));
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
        self.client.on_connection_initiated(endpoint_id, false, false);
        self.client.on_connection_accepted(endpoint_id);
        let channel = Arc::new(FakeEndpointChannel::new(medium, service_id));
        self.ecm
            .borrow_mut()
            .register_channel_for_endpoint(&self.client, endpoint_id, channel.clone());
        channel
    }

    fn current_medium(&self, endpoint_id: &str) -> Medium {
        self.ecm
            .borrow()
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
    assert!(f.wifi_lan.lock().unwrap().handle_initialize_calls.is_empty());
    assert!(f.wifi_hotspot.lock().unwrap().handle_initialize_calls.is_empty());
    assert!(f.wifi_direct.lock().unwrap().handle_initialize_calls.is_empty());
    assert!(f.bwu.is_upgrade_ongoing(ENDPOINT_1));

    // The channel has NOT been swapped yet (still Bluetooth).
    assert_eq!(f.current_medium(ENDPOINT_1), Medium::Bluetooth);

    // The remote dials in to the new medium and sends CLIENT_INTRODUCTION.
    let upgraded =
        notify_bwu_manager_of_incoming_connection(&f.web_rtc, Medium::WebRtc, 0, &mut f.bwu, &mut f.client);

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
    assert_eq!(initial.disconnection_reason(), DisconnectionReason::Upgraded);
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
    let upgraded =
        notify_bwu_manager_of_incoming_connection(&f.web_rtc, Medium::WebRtc, 0, &mut f.bwu, &mut f.client);
    f.feed(for_bwu_safe_to_close(), ENDPOINT_1, Medium::Bluetooth);

    assert!(!upgraded.is_paused());
    assert!(initial.is_closed());
    assert_eq!(initial.disconnection_reason(), DisconnectionReason::Upgraded);
}

#[test]
fn receive_unexpected_last_write_before_upgrade_does_not_wedge() {
    let mut f = fixture(true);

    // A totally-unexpected LAST_WRITE before the endpoint even exists.
    f.feed(for_bwu_last_write(), ENDPOINT_1, Medium::Bluetooth);

    let initial = f.create_initial_endpoint(SERVICE_A, ENDPOINT_1, Medium::Bluetooth);
    f.bwu
        .initiate_bwu_for_endpoint(&mut f.client, ENDPOINT_1, Medium::WebRtc);
    let upgraded =
        notify_bwu_manager_of_incoming_connection(&f.web_rtc, Medium::WebRtc, 0, &mut f.bwu, &mut f.client);

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
    f.ecm.borrow_mut().register_channel_for_endpoint(
        &f.client,
        ENDPOINT_2,
        Arc::new(FakeEndpointChannel::new(Medium::Bluetooth, SERVICE_A)),
    );

    f.feed(upgrade_path_available_frame(), ENDPOINT_2, Medium::Bluetooth);

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
    f.ecm.borrow_mut().register_channel_for_endpoint(
        &f.client,
        ENDPOINT_2,
        Arc::new(FakeEndpointChannel::new(Medium::Bluetooth, SERVICE_A)),
    );

    f.feed(upgrade_path_available_frame(), ENDPOINT_2, Medium::Bluetooth);

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
    assert!(f.wifi_lan.lock().unwrap().handle_initialize_calls.is_empty());
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
