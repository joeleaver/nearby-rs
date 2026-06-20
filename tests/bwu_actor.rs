//! Tests for the Tokio integration actor (`bwu::actor`).
//!
//! These run on a current-thread runtime with a paused clock
//! (`start_paused = true`), so the retry timer is fully deterministic: nothing
//! advances time except explicit `tokio::time::advance`. The actor is driven
//! purely through its `BwuHandle` (the same surface a real consumer uses).
//! Using `tokio::spawn` (whose future bound is `Send`) also pins that the
//! actor's `run` future is `Send`, i.e. spawnable on any runtime.
#![cfg(feature = "tokio")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nearby_rs::bwu::testing::{FakeBwuHandler, FakeBwuHandlerHandle, FakeEndpointChannel};
use nearby_rs::bwu::{
    BaseBwuHandler, BwuActor, BwuConfig, BwuHandler, DisconnectionReason, IncomingSocketConnection,
};
use nearby_rs::frames::{
    for_bwu_failure, for_bwu_introduction, for_bwu_last_write, for_bwu_safe_to_close, from_bytes,
};
use nearby_rs::mediums::Medium;
use nearby_rs::proto as pb;

const SERVICE_A: &str = "ServiceA";
const E1: &str = "Endpoint1";

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn make_handler(medium: Medium, records: &FakeBwuHandlerHandle) -> Box<dyn BwuHandler> {
    Box::new(BaseBwuHandler::new(FakeBwuHandler::new(
        medium,
        records.clone(),
    )))
}

fn failure_frame(tried_medium: Medium) -> pb::OfflineFrame {
    use pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium as M;
    let m = match tried_medium {
        Medium::WifiLan => M::WifiLan,
        Medium::WifiDirect => M::WifiDirect,
        Medium::WifiHotspot => M::WifiHotspot,
        Medium::WebRtc => M::WebRtc,
        Medium::Bluetooth => M::Bluetooth,
        _ => M::UnknownMedium,
    };
    let info = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo {
        medium: Some(m as i32),
        ..Default::default()
    };
    from_bytes(&for_bwu_failure(info)).expect("failure frame should parse")
}

/// Builds the handler map for the given mediums plus a parallel records map.
fn handlers_for(
    mediums: &[Medium],
) -> (
    HashMap<Medium, Box<dyn BwuHandler>>,
    HashMap<Medium, FakeBwuHandlerHandle>,
) {
    let mut handlers: HashMap<Medium, Box<dyn BwuHandler>> = HashMap::new();
    let mut records = HashMap::new();
    for &m in mediums {
        let handle = FakeBwuHandler::records();
        handlers.insert(m, make_handler(m, &handle));
        records.insert(m, handle);
    }
    (handlers, records)
}

fn initialize_calls(records: &FakeBwuHandlerHandle) -> usize {
    records.lock().unwrap().handle_initialize_calls.len()
}

fn bluetooth_channel() -> Arc<FakeEndpointChannel> {
    Arc::new(FakeEndpointChannel::new(Medium::Bluetooth, SERVICE_A))
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retry_timer_fires_and_backs_off_through_the_actor() {
    let (handlers, records) = handlers_for(&[Medium::WebRtc]);
    let (handle, actor) = BwuActor::new(handlers, BwuConfig::default(), "Local", 32);
    tokio::spawn(actor.run());

    handle.connection_initiated(E1, false, false).await;
    handle.connection_accepted(E1).await;
    handle.register_channel(E1, bluetooth_channel()).await;
    // No untried mediums → a failure can only schedule a delayed retry.
    handle.set_upgrade_mediums(E1, vec![]).await;
    handle
        .incoming_frame(failure_frame(Medium::WebRtc), E1, Medium::Bluetooth)
        .await;

    // The actor armed a 3s retry timer (exp-backoff default).
    assert_eq!(handle.pending_retry_delay(E1).await, Some(secs(3)));

    // Advancing the clock fires it; the actor reschedules at 2× (6s).
    tokio::time::advance(secs(3)).await;
    assert_eq!(handle.pending_retry_delay(E1).await, Some(secs(6)));

    tokio::time::advance(secs(6)).await;
    assert_eq!(handle.pending_retry_delay(E1).await, Some(secs(12)));

    // The handler was never (re)initialized — there was no medium to upgrade
    // to, only the bare retry loop.
    assert_eq!(initialize_calls(&records[&Medium::WebRtc]), 0);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn full_upgrade_handshake_through_the_actor() {
    let (handlers, records) = handlers_for(&[Medium::WebRtc]);
    let web_rtc = records[&Medium::WebRtc].clone();
    let (handle, actor) = BwuActor::new(handlers, BwuConfig::default(), "Local", 32);
    tokio::spawn(actor.run());

    handle.connection_initiated(E1, false, false).await;
    handle.connection_accepted(E1).await;
    let initial = bluetooth_channel();
    handle.register_channel(E1, initial.clone()).await;

    handle.initiate_bwu(E1, Medium::WebRtc).await;
    assert!(handle.is_upgrade_ongoing(E1).await);

    // The remote dials into the new medium and introduces itself.
    let (svc, ep) = {
        let r = web_rtc.lock().unwrap();
        let c = &r.handle_initialize_calls[0];
        (
            c.service_id.clone().unwrap(),
            c.endpoint_id.clone().unwrap(),
        )
    };
    let upgraded = Arc::new(FakeEndpointChannel::new(Medium::WebRtc, &svc));
    upgraded.set_read_output(Ok(for_bwu_introduction(&ep, "", false)));
    handle
        .incoming_connection(IncomingSocketConnection {
            channel: upgraded.clone(),
        })
        .await;

    // Drain and close the prior channel.
    handle
        .incoming_frame(
            from_bytes(&for_bwu_last_write()).unwrap(),
            E1,
            Medium::Bluetooth,
        )
        .await;
    handle
        .incoming_frame(
            from_bytes(&for_bwu_safe_to_close()).unwrap(),
            E1,
            Medium::Bluetooth,
        )
        .await;

    // The upgrade completed: a bandwidth-changed event fired and the old
    // channel was closed as UPGRADED.
    assert_eq!(
        handle.bandwidth_changed_events().await,
        vec![(E1.to_string(), Medium::WebRtc)]
    );
    assert!(initial.is_closed());
    assert_eq!(
        initial.disconnection_reason(),
        DisconnectionReason::Upgraded
    );
    assert!(!upgraded.is_paused());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn failure_re_initiates_on_next_medium_through_the_actor() {
    let (handlers, records) = handlers_for(&[Medium::WebRtc, Medium::WifiDirect]);
    let (handle, actor) = BwuActor::new(handlers, BwuConfig::default(), "Local", 32);
    tokio::spawn(actor.run());

    handle.connection_initiated(E1, false, false).await;
    handle.connection_accepted(E1).await;
    handle.register_channel(E1, bluetooth_channel()).await;
    handle
        .set_upgrade_mediums(E1, vec![Medium::WebRtc, Medium::WifiDirect])
        .await;

    handle.initiate_bwu(E1, Medium::WebRtc).await;
    // Barrier: a query forces the actor to finish the initiate before we
    // inspect the (directly-shared) handler records.
    assert!(handle.is_upgrade_ongoing(E1).await);
    assert_eq!(initialize_calls(&records[&Medium::WebRtc]), 1);

    // The remote couldn't reach WEB_RTC → re-initiate on WIFI_DIRECT, not a
    // delayed retry.
    handle
        .incoming_frame(failure_frame(Medium::WebRtc), E1, Medium::Bluetooth)
        .await;
    assert!(handle.is_upgrade_ongoing(E1).await);
    assert_eq!(initialize_calls(&records[&Medium::WifiDirect]), 1);
    assert_eq!(handle.pending_retry_delay(E1).await, None);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_new_initiate_cancels_the_pending_retry_timer() {
    let (handlers, _records) = handlers_for(&[Medium::WebRtc]);
    let (handle, actor) = BwuActor::new(handlers, BwuConfig::default(), "Local", 32);
    tokio::spawn(actor.run());

    handle.connection_initiated(E1, false, false).await;
    handle.connection_accepted(E1).await;
    handle.register_channel(E1, bluetooth_channel()).await;
    handle.set_upgrade_mediums(E1, vec![]).await;
    handle
        .incoming_frame(failure_frame(Medium::WebRtc), E1, Medium::Bluetooth)
        .await;
    assert_eq!(handle.pending_retry_delay(E1).await, Some(secs(3)));

    // A fresh initiate supersedes the pending retry.
    handle.initiate_bwu(E1, Medium::WebRtc).await;
    assert_eq!(handle.pending_retry_delay(E1).await, None);
    assert!(handle.is_upgrade_ongoing(E1).await);

    // Advancing past the old deadline must NOT resurrect a fire.
    tokio::time::advance(secs(30)).await;
    assert_eq!(handle.pending_retry_delay(E1).await, None);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn shutdown_stops_the_actor() {
    let (handlers, records) = handlers_for(&[Medium::WebRtc]);
    let web_rtc = records[&Medium::WebRtc].clone();
    let (handle, actor) = BwuActor::new(handlers, BwuConfig::default(), "Local", 32);
    tokio::spawn(actor.run());

    handle.connection_initiated(E1, false, false).await;
    handle.connection_accepted(E1).await;
    handle.register_channel(E1, bluetooth_channel()).await;
    handle.initiate_bwu(E1, Medium::WebRtc).await;
    // Barrier before inspecting the directly-shared records.
    assert!(handle.is_upgrade_ongoing(E1).await);
    assert_eq!(initialize_calls(&web_rtc), 1);

    handle.shutdown().await;

    // The actor has stopped, so further commands are inert: a second initiate
    // must not reach the handler.
    handle.initiate_bwu(E1, Medium::WebRtc).await;
    tokio::task::yield_now().await;
    assert_eq!(initialize_calls(&web_rtc), 1);
    // Queries against the stopped actor fall back to defaults.
    assert!(!handle.is_upgrade_ongoing(E1).await);
}
