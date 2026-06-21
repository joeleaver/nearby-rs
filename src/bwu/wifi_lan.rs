//! A concrete WIFI_LAN [`MediumBwuHandler`] over TCP.
//!
//! This is the first real medium handler: the **initiator** binds a
//! `TcpListener`, advertises its `ip:port` in a WIFI_LAN `UPGRADE_PATH_AVAILABLE`
//! frame, and runs an accept loop that wraps each accepted socket in a
//! [`StreamChannel`](crate::bwu::stream_channel::StreamChannel) and hands it to
//! the BWU layer via a *connection sink* callback (the C++ `OnIncomingConnection`
//! bind_front target — in the Tokio integration the sink posts a
//! `BwuCommand::IncomingConnection`). The **responder** reads those credentials
//! from the `UpgradePathInfo` and dials a `TcpStream` back, wrapping it in a
//! `StreamChannel`. The TCP plumbing lives in [`crate::bwu::tcp`].
//!
//! Unlike the rest of the crate this has no upstream test oracle (Google ships no
//! Linux WIFI_LAN backend); it is pinned by the loopback integration test in
//! `tests/wifi_lan.rs`.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::sync::Arc;

use crate::bwu::channel::EndpointChannel;
use crate::bwu::client::ClientProxy;
use crate::bwu::handler::MediumBwuHandler;
use crate::bwu::tcp::{start_listener, stop_listener, tcp_channel, ConnectionSink, Listener};
use crate::frames::{for_bwu_wifi_lan_path_available, ServiceAddress};
use crate::mediums::Medium;
use crate::proto as pb;

type UpgradePathInfo = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo;

const CHANNEL_NAME: &str = "wifi-lan";

/// The WIFI_LAN bandwidth-upgrade handler.
pub struct WifiLanBwuHandler {
    sink: ConnectionSink,
    /// What the accept listener binds to. `0.0.0.0` accepts on every interface;
    /// `127.0.0.1` (the default) is loopback-only, for tests / same-host use.
    bind_ip: Ipv4Addr,
    /// The routable IPv4 address advertised in `UPGRADE_PATH_AVAILABLE` — the
    /// address the remote peer actually dials. Distinct from `bind_ip` because a
    /// real device must advertise the LAN IP (you can't advertise `0.0.0.0`).
    advertise_ip: Ipv4Addr,
    listeners: HashMap<String, Listener>,
}

impl WifiLanBwuHandler {
    /// Loopback handler: binds and advertises `127.0.0.1`. Fine for tests and
    /// same-host upgrades; a remote phone cannot reach loopback — use
    /// [`WifiLanBwuHandler::with_endpoint`] for that.
    pub fn new(sink: ConnectionSink) -> Self {
        Self::with_endpoint(sink, Ipv4Addr::LOCALHOST, Ipv4Addr::LOCALHOST)
    }

    /// Bind the accept listener to `bind_ip` (e.g. `0.0.0.0` to accept on every
    /// interface) and advertise `advertise_ip` (the routable LAN address the
    /// remote peer dials) in the `UPGRADE_PATH_AVAILABLE` frame. Use this for a
    /// real device upgrade.
    pub fn with_endpoint(sink: ConnectionSink, bind_ip: Ipv4Addr, advertise_ip: Ipv4Addr) -> Self {
        Self {
            sink,
            bind_ip,
            advertise_ip,
            listeners: HashMap::new(),
        }
    }
}

impl Drop for WifiLanBwuHandler {
    fn drop(&mut self) {
        let service_ids: Vec<String> = self.listeners.keys().cloned().collect();
        for service_id in service_ids {
            self.handle_revert_initiator_state_for_service(&service_id);
        }
    }
}

impl MediumBwuHandler for WifiLanBwuHandler {
    fn handle_initialize_upgraded_medium_for_endpoint(
        &mut self,
        _client: &ClientProxy,
        upgrade_service_id: &str,
        _endpoint_id: &str,
    ) -> Vec<u8> {
        // Reuse the existing listener for this service, or bind a new one and
        // start accepting.
        let addr: SocketAddr = match self.listeners.get(upgrade_service_id) {
            Some(listener) => listener.addr,
            None => {
                let listener = match start_listener(
                    self.bind_ip,
                    upgrade_service_id,
                    CHANNEL_NAME,
                    Medium::WifiLan,
                    self.sink.clone(),
                ) {
                    Some(listener) => listener,
                    None => return Vec::new(), // EMPTY = MEDIUM_ERROR
                };
                let addr = listener.addr;
                self.listeners
                    .insert(upgrade_service_id.to_string(), listener);
                addr
            }
        };

        // Advertise the routable IP (not the bind IP, which may be 0.0.0.0) with
        // the port the OS actually assigned.
        for_bwu_wifi_lan_path_available(&[ServiceAddress {
            address: self.advertise_ip.octets().to_vec(),
            port: i32::from(addr.port()),
        }])
    }

    fn handle_revert_initiator_state_for_service(&mut self, upgrade_service_id: &str) {
        if let Some(listener) = self.listeners.remove(upgrade_service_id) {
            stop_listener(listener);
        }
    }

    fn create_upgraded_endpoint_channel(
        &mut self,
        _client: &ClientProxy,
        service_id: &str,
        _endpoint_id: &str,
        upgrade_path_info: &UpgradePathInfo,
    ) -> Option<Arc<dyn EndpointChannel>> {
        let socket = upgrade_path_info.wifi_lan_socket.as_ref()?;
        let ip = socket.ip_address.as_ref()?;
        if ip.len() != 4 {
            return None;
        }
        let port = u16::try_from(socket.wifi_port?).ok()?;
        let addr = SocketAddr::from((Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]), port));
        let stream = TcpStream::connect(addr).ok()?;
        tcp_channel(stream, service_id, CHANNEL_NAME, Medium::WifiLan)
    }

    fn get_upgrade_medium(&self) -> Medium {
        Medium::WifiLan
    }

    fn on_endpoint_disconnect(&mut self, _client: &ClientProxy, _endpoint_id: &str) {}
}
