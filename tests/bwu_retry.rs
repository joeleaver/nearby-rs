//! Tests for the BWU retry machinery (`TryNextBestUpgradeMediums`,
//! `ChooseBestUpgradeMedium`, `RetryUpgradesAfterDelay`,
//! `CalculateNextRetryDelay`, and the retry-alarm bookkeeping).
//!
//! Unlike `bwu_manager.rs`, these are NOT ports of upstream oracle cases: the
//! Google `bwu_manager_test.cc` has NO retry tests at all. They are
//! hand-authored to pin the ported behaviour, driving it black-box through the
//! real `UPGRADE_FAILURE` dispatch path plus the alarm seam
//! (`pending_retry_delay` / `fire_retry_alarm`) the Phase-3 Tokio actor will use.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use nearby_rs::bwu::testing::{FakeBwuHandler, FakeBwuHandlerHandle, FakeEndpointChannel};
use nearby_rs::bwu::{
    BaseBwuHandler, BwuConfig, BwuHandler, BwuManager, ClientProxy, EndpointChannelManager,
};
use nearby_rs::frames::for_bwu_failure;
use nearby_rs::mediums::Medium;
use nearby_rs::proto as pb;

const SERVICE_A: &str = "ServiceA";
const E1: &str = "Endpoint1";

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

struct Harness {
    client: ClientProxy,
    ecm: Rc<RefCell<EndpointChannelManager>>,
    bwu: BwuManager,
    records: HashMap<Medium, FakeBwuHandlerHandle>,
}

impl Harness {
    /// Builds a manager with handlers for `mediums` and the given retry config.
    fn new(mediums: &[Medium], config: BwuConfig) -> Self {
        let ecm = Rc::new(RefCell::new(EndpointChannelManager::new()));
        let mut handlers: HashMap<Medium, Box<dyn BwuHandler>> = HashMap::new();
        let mut records = HashMap::new();
        for &m in mediums {
            let handle = FakeBwuHandler::records();
            handlers.insert(
                m,
                Box::new(BaseBwuHandler::new(FakeBwuHandler::new(m, handle.clone()))),
            );
            records.insert(m, handle);
        }
        let mut bwu = BwuManager::new(ecm.clone(), handlers, config);
        bwu.make_single_threaded_for_testing();
        Self {
            client: ClientProxy::new(0, "LocalEndpoint"),
            ecm,
            bwu,
            records,
        }
    }

    /// Registers an accepted, connected (discoverer-side) endpoint channel.
    fn connect(&mut self, endpoint_id: &str, channel_medium: Medium, upgrade_mediums: &[Medium]) {
        self.client
            .on_connection_initiated(endpoint_id, false, false);
        self.client.on_connection_accepted(endpoint_id);
        self.client
            .set_upgrade_mediums(endpoint_id, upgrade_mediums.to_vec());
        let channel = Arc::new(FakeEndpointChannel::new(channel_medium, SERVICE_A));
        self.ecm
            .borrow_mut()
            .register_channel_for_endpoint(&self.client, endpoint_id, channel);
    }

    /// Feeds an `UPGRADE_FAILURE` for `endpoint_id` whose `UpgradePathInfo`
    /// names `tried_medium` (the medium the remote failed to reach).
    fn feed_failure(&mut self, endpoint_id: &str, tried_medium: Medium) {
        let info = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
            medium: Some(medium_to_upi(tried_medium)),
            ..Default::default()
        };
        let bytes = for_bwu_failure(info);
        let frame = nearby_rs::frames::from_bytes(&bytes).expect("failure frame should parse");
        self.bwu
            .on_incoming_frame(&frame, endpoint_id, &mut self.client, Medium::Bluetooth);
    }

    fn initialize_calls(&self, medium: Medium) -> usize {
        self.records[&medium]
            .lock()
            .unwrap()
            .handle_initialize_calls
            .len()
    }

    fn last_initialize_service_id(&self, medium: Medium) -> Option<String> {
        self.records[&medium]
            .lock()
            .unwrap()
            .handle_initialize_calls
            .last()
            .and_then(|c| c.service_id.clone())
    }
}

fn medium_to_upi(medium: Medium) -> i32 {
    use pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium as M;
    let m = match medium {
        Medium::WifiLan => M::WifiLan,
        Medium::WifiDirect => M::WifiDirect,
        Medium::WifiHotspot => M::WifiHotspot,
        Medium::WebRtc => M::WebRtc,
        Medium::Bluetooth => M::Bluetooth,
        _ => M::UnknownMedium,
    };
    m as i32
}

// --- the delay math --------------------------------------------------------

#[test]
fn first_retry_uses_initial_delay_exp_default() {
    let h = Harness::new(&[], BwuConfig::default());
    // No prior delay recorded → the configured initial (3s exp-backoff default).
    assert_eq!(h.bwu.calculate_next_retry_delay("fresh"), secs(3));
}

#[test]
fn exp_backoff_doubles_and_caps_at_max() {
    // current channel is non-WIFI_LAN and there is never a viable next medium,
    // so every fire reschedules with the next backoff step.
    let mut h = Harness::new(&[], BwuConfig::default());
    h.connect(E1, Medium::Bluetooth, &[]);

    h.feed_failure(E1, Medium::WebRtc);
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(3)));

    let mut seen = Vec::new();
    for _ in 0..8 {
        h.bwu.fire_retry_alarm(&mut h.client, E1);
        seen.push(h.bwu.pending_retry_delay(E1).unwrap());
    }
    // 3 → 6 → 12 → 24 → 48 → 96 → 192 → 300 (capped) → 300.
    assert_eq!(
        seen,
        [
            secs(6),
            secs(12),
            secs(24),
            secs(48),
            secs(96),
            secs(192),
            secs(300),
            secs(300),
        ]
    );
}

#[test]
fn linear_backoff_grows_by_initial_and_caps() {
    let config = BwuConfig {
        use_exp_backoff_in_bwu_retry: false,
        ..Default::default()
    };
    let mut h = Harness::new(&[], config);
    h.connect(E1, Medium::Bluetooth, &[]);

    h.feed_failure(E1, Medium::WebRtc);
    // Linear default: initial 5s, max 10s → 5, 10, 10, 10...
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(5)));
    h.bwu.fire_retry_alarm(&mut h.client, E1);
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(10)));
    h.bwu.fire_retry_alarm(&mut h.client, E1);
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(10)));
}

// --- next-best-medium vs retry ---------------------------------------------

#[test]
fn failure_tries_next_untried_medium() {
    // Two mutually-supported mediums; WEB_RTC was tried and failed, so the
    // upgrade should re-initiate on the untried WIFI_DIRECT (no retry pending).
    let mut h = Harness::new(&[Medium::WebRtc, Medium::WifiDirect], BwuConfig::default());
    h.connect(E1, Medium::Bluetooth, &[Medium::WebRtc, Medium::WifiDirect]);

    // First upgrade attempt over WEB_RTC (records the bwu medium + in-progress).
    h.bwu
        .initiate_bwu_for_endpoint(&mut h.client, E1, Medium::WebRtc);
    assert_eq!(h.initialize_calls(Medium::WebRtc), 1);

    // The remote couldn't reach WEB_RTC.
    h.feed_failure(E1, Medium::WebRtc);

    // We re-initiated on the next untried medium, WIFI_DIRECT (wrapped service).
    assert_eq!(h.initialize_calls(Medium::WifiDirect), 1);
    assert_eq!(
        h.last_initialize_service_id(Medium::WifiDirect).as_deref(),
        Some("ServiceA_UPGRADE")
    );
    assert!(h.bwu.is_upgrade_ongoing(E1));
    // A concrete medium was chosen, so no delayed retry was scheduled.
    assert_eq!(h.bwu.pending_retry_delay(E1), None);
}

#[test]
fn failure_with_no_untried_medium_schedules_retry() {
    let mut h = Harness::new(&[Medium::WebRtc], BwuConfig::default());
    h.connect(E1, Medium::Bluetooth, &[]);

    h.feed_failure(E1, Medium::WebRtc);

    // No untried medium and not on WIFI_LAN → a delayed retry is scheduled.
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(3)));
    assert!(!h.bwu.is_upgrade_ongoing(E1));
}

#[test]
fn no_retry_scheduled_when_already_on_wifi_lan() {
    // On WIFI_LAN with no better medium, the reference does NOT retry (the
    // `current_medium != WIFI_LAN` guard / Google TODO b/228610864).
    let mut h = Harness::new(&[Medium::WebRtc], BwuConfig::default());
    h.connect(E1, Medium::WifiLan, &[]);

    h.feed_failure(E1, Medium::WebRtc);

    assert_eq!(h.bwu.pending_retry_delay(E1), None);
}

// --- cancel / lifecycle semantics ------------------------------------------

#[test]
fn cancel_keeps_backoff_so_a_new_initiate_resumes_it() {
    let mut h = Harness::new(&[Medium::WebRtc], BwuConfig::default());
    h.connect(E1, Medium::Bluetooth, &[]);

    // Schedule a retry (records last delay = 3s).
    h.feed_failure(E1, Medium::WebRtc);
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(3)));

    // A fresh initiate cancels the pending alarm...
    h.bwu
        .initiate_bwu_for_endpoint(&mut h.client, E1, Medium::WebRtc);
    assert_eq!(h.bwu.pending_retry_delay(E1), None);

    // ...but the backoff state survived, so the next scheduled retry is 6s, not 3s.
    h.feed_failure(E1, Medium::WebRtc);
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(6)));
}

#[test]
fn disconnect_resets_backoff_and_cancels_alarm() {
    let mut h = Harness::new(&[Medium::WebRtc], BwuConfig::default());
    h.connect(E1, Medium::Bluetooth, &[]);

    h.feed_failure(E1, Medium::WebRtc);
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(3)));

    h.bwu
        .on_endpoint_disconnect(&mut h.client, SERVICE_A, E1, nearby_rs::bwu::channel::DisconnectionReason::LocalDisconnection);
    // The pending alarm is cancelled...
    assert_eq!(h.bwu.pending_retry_delay(E1), None);

    // ...and the backoff is reset, so a new schedule starts back at 3s.
    h.feed_failure(E1, Medium::WebRtc);
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(3)));
}

#[test]
fn fire_with_no_pending_alarm_is_a_noop() {
    let mut h = Harness::new(&[], BwuConfig::default());
    // Nothing scheduled — must not panic or schedule anything.
    h.bwu.fire_retry_alarm(&mut h.client, "Nobody");
    assert_eq!(h.bwu.pending_retry_delay("Nobody"), None);
}

#[test]
fn fire_after_disconnect_does_not_reschedule() {
    let mut h = Harness::new(&[Medium::WebRtc], BwuConfig::default());
    h.connect(E1, Medium::Bluetooth, &[]);
    h.feed_failure(E1, Medium::WebRtc);
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(3)));

    // The client learns the endpoint is gone before the timer fires.
    h.client.on_disconnected(E1);
    h.bwu.fire_retry_alarm(&mut h.client, E1);

    // The alarm was consumed and NOT rescheduled (endpoint no longer connected).
    assert_eq!(h.bwu.pending_retry_delay(E1), None);
}

#[test]
fn shutdown_cancels_all_retry_alarms() {
    let mut h = Harness::new(&[Medium::WebRtc], BwuConfig::default());
    h.connect(E1, Medium::Bluetooth, &[]);
    h.feed_failure(E1, Medium::WebRtc);
    assert_eq!(h.bwu.pending_retry_delay(E1), Some(secs(3)));

    h.bwu.shutdown();
    assert_eq!(h.bwu.pending_retry_delay(E1), None);
}
