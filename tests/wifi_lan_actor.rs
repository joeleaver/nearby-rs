//! End-to-end: a WIFI_LAN bandwidth upgrade driven through the Tokio actor over
//! real TCP, with no hardware.
//!
//! This wires the three Phase-3 pieces together — the `BwuActor`, the
//! `WifiLanBwuHandler` (whose accept loop posts `IncomingConnection` via
//! `BwuHandle::connection_sink`), and the `StreamChannel` transport. The actor
//! initiates an upgrade (binding a real listener and writing
//! `UPGRADE_PATH_AVAILABLE` on the pre-upgrade channel); a separate "responder"
//! thread dials the advertised socket and sends `CLIENT_INTRODUCTION`; and we
//! verify the actor's accept→sink→`on_incoming_connection` path runs the upgrade
//! protocol — observable as the `LAST_WRITE` it then writes on the old channel.
#![cfg(feature = "tokio")]

use std::collections::HashMap;
use std::sync::Arc;

use nearby_rs::bwu::{
    BaseBwuHandler, BwuActor, BwuConfig, BwuHandler, ClientProxy, EndpointChannel,
    MediumBwuHandler, Pipe, StreamChannel, WifiLanBwuHandler,
};
use nearby_rs::frames::{for_bwu_introduction, for_bwu_last_write, from_bytes};
use nearby_rs::mediums::Medium;
use nearby_rs::proto as pb;

type UpgradePathInfo = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo;

fn upgrade_path_info(bytes: &[u8]) -> UpgradePathInfo {
    from_bytes(bytes)
        .expect("frame parses")
        .v1
        .and_then(|v1| v1.bandwidth_upgrade_negotiation)
        .and_then(|bwu| bwu.upgrade_path_info)
        .expect("UPGRADE_PATH_AVAILABLE carries upgrade_path_info")
}

#[tokio::test(flavor = "current_thread")]
async fn wifi_lan_upgrade_drives_through_the_actor() {
    // Build the actor, wiring a WIFI_LAN handler whose accept loop posts
    // IncomingConnection back to this same actor.
    let (handle, rx) = BwuActor::channel(32);
    let sink = handle.connection_sink();
    let mut handlers: HashMap<Medium, Box<dyn BwuHandler>> = HashMap::new();
    handlers.insert(
        Medium::WifiLan,
        Box::new(BaseBwuHandler::new(WifiLanBwuHandler::new(sink))),
    );
    let actor = BwuActor::build(rx, handlers, BwuConfig::default(), "Local");
    tokio::spawn(actor.run());

    // The pre-upgrade channel is a Pipe-backed StreamChannel we can read: the
    // actor writes UPGRADE_PATH_AVAILABLE then LAST_WRITE on it.
    let old: Arc<dyn EndpointChannel> = Arc::new(StreamChannel::new(
        "ServiceA",
        "old",
        Medium::Bluetooth,
        Pipe::new(),
    ));

    handle.connection_initiated("E1", false, false).await;
    handle.connection_accepted("E1").await;
    handle.register_channel("E1", old.clone()).await;

    // Initiate: the WIFI_LAN handler binds a listener + the actor writes
    // UPGRADE_PATH_AVAILABLE on the old channel.
    handle.initiate_bwu("E1", Medium::WifiLan).await;
    assert!(handle.is_upgrade_ongoing("E1").await);

    // Read the advertised socket credentials off the old channel.
    let info = {
        let old = old.clone();
        let bytes = tokio::task::spawn_blocking(move || old.read())
            .await
            .unwrap()
            .expect("UPGRADE_PATH_AVAILABLE on the old channel");
        upgrade_path_info(&bytes)
    };

    // Responder: dial the advertised socket, introduce ourselves, drain the ack.
    let responder = std::thread::spawn(move || {
        let mut handler = WifiLanBwuHandler::new(Arc::new(|_| {}));
        let channel = handler
            .create_upgraded_endpoint_channel(&ClientProxy::default(), "ServiceA", "E1", &info)
            .expect("responder connects to the advertised socket");
        assert_eq!(
            channel.write(&for_bwu_introduction("E1", "", false)),
            nearby_rs::frames::Exception::Success
        );
        let _ack = channel.read();
        channel // keep the socket open until the test is done
    });

    // The actor accepted the socket, ran on_incoming_connection, and wrote
    // LAST_WRITE on the old channel — the observable proof the whole path ran.
    let last_write = {
        let old = old.clone();
        tokio::task::spawn_blocking(move || old.read())
            .await
            .unwrap()
            .expect("LAST_WRITE on the old channel")
    };
    assert_eq!(last_write, for_bwu_last_write());

    let _channel = responder.join().unwrap();
}
