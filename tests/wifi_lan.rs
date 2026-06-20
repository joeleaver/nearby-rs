//! Loopback integration test for the WIFI_LAN BWU handler.
//!
//! There is no upstream test oracle (Google ships no Linux WIFI_LAN backend), so
//! this drives the handler end to end over real TCP on localhost: the initiator
//! binds a listener and advertises it, the responder dials in, and the two
//! resulting `StreamChannel`s exchange frames both ways. No hardware required.

use std::sync::{mpsc, Arc};

use nearby_rs::bwu::{
    ClientProxy, ConnectionSink, IncomingSocketConnection, MediumBwuHandler, WifiLanBwuHandler,
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
        .expect("a WIFI_LAN UPGRADE_PATH_AVAILABLE carries upgrade_path_info")
}

#[test]
fn wifi_lan_upgrade_channel_round_trips_over_loopback() {
    let client = ClientProxy::default();

    // Initiator: bind + advertise; the sink delivers each accepted connection.
    let (tx, rx) = mpsc::channel::<IncomingSocketConnection>();
    let sink: ConnectionSink = Arc::new(move |connection| {
        let _ = tx.send(connection);
    });
    let mut initiator = WifiLanBwuHandler::new(sink);
    let path_available =
        initiator.handle_initialize_upgraded_medium_for_endpoint(&client, "ServiceA_UPGRADE", "E1");
    assert!(
        !path_available.is_empty(),
        "listener should bind + advertise"
    );

    // Responder: read the advertised ip:port and dial back.
    let info = upgrade_path_info(&path_available);
    let mut responder = WifiLanBwuHandler::new(Arc::new(|_| {}));
    let responder_channel = responder
        .create_upgraded_endpoint_channel(&client, "ServiceA", "E1", &info)
        .expect("responder should connect to the advertised socket");

    // The initiator's accept loop produced the matching upgraded channel.
    let initiator_channel = rx
        .recv()
        .expect("the accept loop should deliver the connection")
        .channel;

    assert_eq!(initiator_channel.medium(), Medium::WifiLan);
    assert_eq!(responder_channel.medium(), Medium::WifiLan);

    // Frames flow both ways over the real TCP link.
    assert_eq!(responder_channel.write(b"ping"), Exception::Success);
    assert_eq!(initiator_channel.read().unwrap(), b"ping");
    assert_eq!(initiator_channel.write(b"pong"), Exception::Success);
    assert_eq!(responder_channel.read().unwrap(), b"pong");

    // Reverting tears the listener down cleanly (must not hang).
    initiator.handle_revert_initiator_state_for_service("ServiceA_UPGRADE");
}

#[test]
fn create_channel_without_credentials_returns_none() {
    let client = ClientProxy::default();
    let mut handler = WifiLanBwuHandler::new(Arc::new(|_| {}));
    // No wifi_lan_socket → nothing to dial.
    let info = UpgradePathInfo::default();
    assert!(handler
        .create_upgraded_endpoint_channel(&client, "ServiceA", "E1", &info)
        .is_none());
}
