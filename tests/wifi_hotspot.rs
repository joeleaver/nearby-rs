//! Integration test for the WIFI_HOTSPOT BWU handler — a transcription of Google
//! "Nearby" `wifi_hotspot_bwu_handler_test.cc` (which runs against the g3-sim
//! `MediumEnvironment`, not real radios). Here the platform SoftAP is the
//! [`FakeSoftAp`] seam (loopback creds), so the handler protocol runs end to end
//! over real TCP on localhost with no hardware: the initiator stands up the
//! "SoftAP" + listener and advertises credentials; the responder joins as a STA
//! and dials the gateway; the two `StreamChannel`s exchange frames.

use std::sync::{mpsc, Arc};

use nearby_rs::bwu::{
    ClientProxy, ConnectionSink, FakeSoftAp, IncomingSocketConnection, MediumBwuHandler, SoftAp,
    WifiHotspotBwuHandler,
};
use nearby_rs::frames::{from_bytes, Exception};
use nearby_rs::mediums::Medium;
use nearby_rs::proto as pb;

type UpgradePathInfo = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo;

fn upgrade_path_info(path_available_bytes: &[u8]) -> UpgradePathInfo {
    from_bytes(path_available_bytes)
        .expect("path-available frame should parse")
        .v1
        .and_then(|v1| v1.bandwidth_upgrade_negotiation)
        .and_then(|bwu| bwu.upgrade_path_info)
        .expect("a WIFI_HOTSPOT UPGRADE_PATH_AVAILABLE carries upgrade_path_info")
}

/// `CanCreateBwuHandler`: init + revert must not panic, and produce a frame.
#[test]
fn can_create_bwu_handler() {
    let client = ClientProxy::default();
    let mut handler = WifiHotspotBwuHandler::new(Arc::new(FakeSoftAp::new()), Arc::new(|_| {}));
    let frame = handler.handle_initialize_upgraded_medium_for_endpoint(
        &client,
        "com.google.location.nearby.apps.test_UPGRADE",
        "Hotspot_Server",
    );
    assert!(
        !frame.is_empty(),
        "SoftAP + listener should produce a frame"
    );
    handler
        .handle_revert_initiator_state_for_service("com.google.location.nearby.apps.test_UPGRADE");
}

/// `SoftAPBWUInit_STACreateEndpointChannel`: the initiator stands up the SoftAP +
/// listener and advertises WIFI_HOTSPOT credentials; the STA rejects an empty
/// offer, then joins on the real one, gets an upgraded channel, and frames flow.
#[test]
fn softap_init_then_sta_creates_endpoint_channel() {
    let client = ClientProxy::default();

    // Initiator (SoftAP host): the sink delivers each accepted connection.
    let (tx, rx) = mpsc::channel::<IncomingSocketConnection>();
    let sink: ConnectionSink = Arc::new(move |connection| {
        let _ = tx.send(connection);
    });
    let mut initiator = WifiHotspotBwuHandler::new(Arc::new(FakeSoftAp::new()), sink);
    let path_available = initiator.handle_initialize_upgraded_medium_for_endpoint(
        &client,
        "ServiceA_UPGRADE",
        "Hotspot_Server",
    );
    assert!(
        !path_available.is_empty(),
        "SoftAP should start + advertise"
    );

    // The advertised frame carries WIFI_HOTSPOT credentials (ssid/pw/gateway/freq).
    let info = upgrade_path_info(&path_available);
    let creds = info
        .wifi_hotspot_credentials
        .as_ref()
        .expect("WIFI_HOTSPOT offer carries hotspot credentials");
    assert_eq!(creds.gateway(), "127.0.0.1");
    assert_eq!(creds.frequency(), 2437);
    assert!(!creds.ssid().is_empty());
    assert!(creds.password().len() >= 8);

    // Responder (STA). An empty offer has no credentials → nothing to join.
    let sta_softap = Arc::new(FakeSoftAp::new());
    let mut responder = WifiHotspotBwuHandler::new(sta_softap.clone(), Arc::new(|_| {}));
    assert!(
        responder
            .create_upgraded_endpoint_channel(
                &client,
                "ServiceA",
                "Hotspot_Server",
                &UpgradePathInfo::default(),
            )
            .is_none(),
        "an offer without credentials must not produce a channel"
    );
    assert!(
        !sta_softap.is_connected_to_hotspot(),
        "a rejected offer must not join the hotspot"
    );

    // The real offer: join the SoftAP as a STA and dial the gateway.
    let responder_channel = responder
        .create_upgraded_endpoint_channel(&client, "ServiceA", "Hotspot_Server", &info)
        .expect("STA should join + connect to the advertised hotspot");
    assert_eq!(responder.get_upgrade_medium(), Medium::WifiHotspot);
    assert_eq!(responder_channel.medium(), Medium::WifiHotspot);
    assert!(
        sta_softap.is_connected_to_hotspot(),
        "creating the channel joins the hotspot as a STA"
    );

    // The initiator's accept loop produced the matching upgraded channel.
    let initiator_channel = rx
        .recv()
        .expect("the accept loop should deliver the connection")
        .channel;
    assert_eq!(initiator_channel.medium(), Medium::WifiHotspot);

    // Frames flow both ways over the real TCP link.
    assert_eq!(responder_channel.write(b"ping"), Exception::Success);
    assert_eq!(initiator_channel.read().unwrap(), b"ping");
    assert_eq!(initiator_channel.write(b"pong"), Exception::Success);
    assert_eq!(responder_channel.read().unwrap(), b"pong");

    // Responder teardown leaves the hotspot (ref: RevertResponderState).
    responder.on_endpoint_disconnect(&client, "Hotspot_Server");
    assert!(
        !sta_softap.is_connected_to_hotspot(),
        "disconnect should leave the hotspot"
    );

    // Initiator teardown stops the listener + SoftAP cleanly (must not hang).
    initiator.handle_revert_initiator_state_for_service("ServiceA_UPGRADE");
}

/// No credentials in the offer → no channel (ref: CONNECTIVITY_WIFI_HOTSPOT_
/// INVALID_CREDENTIAL).
#[test]
fn create_channel_without_credentials_returns_none() {
    let client = ClientProxy::default();
    let mut handler = WifiHotspotBwuHandler::new(Arc::new(FakeSoftAp::new()), Arc::new(|_| {}));
    let info = UpgradePathInfo::default();
    assert!(handler
        .create_upgraded_endpoint_channel(&client, "ServiceA", "E1", &info)
        .is_none());
}
