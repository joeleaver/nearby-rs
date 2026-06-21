//! A concrete WIFI_HOTSPOT [`MediumBwuHandler`] over TCP — a port of Google
//! "Nearby" `connections/implementation/mediums/wifi_hotspot_bwu_handler.cc`.
//!
//! The **initiator** (the host, e.g. beamish) stands up a SoftAP via the
//! [`SoftAp`] seam, binds a `TcpListener` on the AP gateway, advertises the
//! hotspot credentials (SSID/password/frequency/gateway/port) in a WIFI_HOTSPOT
//! `UPGRADE_PATH_AVAILABLE`, and runs the same accept loop as WIFI_LAN. The
//! **responder** reads those credentials, joins the AP as a STA via the seam, then
//! dials the gateway and wraps the socket in a `StreamChannel`.
//!
//! The handler logic is faithful to the reference; the platform SoftAP bring-up
//! (the [`SoftAp`] impl) is the one part with no upstream Linux backend (Google
//! ships only apple/windows/g3-sim) — same situation as WIFI_LAN's raw
//! `TcpListener`. Pinned by the transcribed g3-sim oracle in `tests/wifi_hotspot.rs`
//! (from `wifi_hotspot_bwu_handler_test.cc`).

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::bwu::channel::EndpointChannel;
use crate::bwu::client::ClientProxy;
use crate::bwu::handler::MediumBwuHandler;
use crate::bwu::tcp::{start_listener, stop_listener, tcp_channel, ConnectionSink, Listener};
use crate::frames::for_bwu_wifi_hotspot_path_available;
use crate::mediums::Medium;
use crate::proto as pb;

type UpgradePathInfo = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo;
type WifiHotspotCredentials =
    pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::WifiHotspotCredentials;

const CHANNEL_NAME: &str = "wifi-hotspot";

/// SoftAP credentials + bind/gateway address. Returned by [`SoftAp::start`] and
/// carried in the `UPGRADE_PATH_AVAILABLE` frame.
#[derive(Clone, Debug)]
pub struct HotspotCreds {
    pub ssid: String,
    pub password: String,
    /// Band or frequency (MHz) the AP runs on; advertised so the STA joins faster.
    pub frequency: i32,
    /// The AP gateway IPv4 — what the responder dials.
    pub gateway: Ipv4Addr,
    /// What the initiator's accept listener binds to (the AP gateway for a real
    /// SoftAP; loopback in tests).
    pub bind_ip: Ipv4Addr,
}

/// The platform SoftAP seam: bring up / tear down a Wi-Fi hotspot (initiator) and
/// join / leave one as a STA (responder). The real impl drives NetworkManager
/// (D-Bus) + a privileged vif helper on Linux; tests use [`FakeSoftAp`]. This is
/// the one part the port has no upstream Linux backend for.
pub trait SoftAp: Send + Sync {
    /// Bring up the SoftAP for `service_id`; returns the credentials to advertise
    /// (and the gateway/bind IP). `None` = could not start (→ MEDIUM_ERROR).
    fn start(&self, service_id: &str) -> Option<HotspotCreds>;
    /// Join `creds`' hotspot as a STA (responder side). `None` = join failed.
    fn connect_as_sta(&self, creds: &HotspotCreds) -> Option<()>;
    /// Tear down the SoftAP for `service_id` (initiator revert).
    fn stop(&self, service_id: &str);
    /// Leave the joined hotspot (responder revert). Default: no-op.
    fn disconnect_sta(&self) {}
    /// Whether this side is currently joined to a hotspot as a STA. Mirrors the
    /// reference `WifiHotspot::IsConnectedToHotspot()`.
    fn is_connected_to_hotspot(&self) -> bool {
        false
    }
}

/// A listener plus the SoftAP credentials it advertises (the port comes from the
/// listener, so the two are stored together per service).
struct HotspotListener {
    listener: Listener,
    creds: HotspotCreds,
}

/// The WIFI_HOTSPOT bandwidth-upgrade handler.
pub struct WifiHotspotBwuHandler {
    sink: ConnectionSink,
    softap: Arc<dyn SoftAp>,
    listeners: HashMap<String, HotspotListener>,
}

impl WifiHotspotBwuHandler {
    pub fn new(softap: Arc<dyn SoftAp>, sink: ConnectionSink) -> Self {
        Self {
            sink,
            softap,
            listeners: HashMap::new(),
        }
    }
}

impl Drop for WifiHotspotBwuHandler {
    fn drop(&mut self) {
        let service_ids: Vec<String> = self.listeners.keys().cloned().collect();
        for service_id in service_ids {
            self.handle_revert_initiator_state_for_service(&service_id);
        }
    }
}

impl MediumBwuHandler for WifiHotspotBwuHandler {
    fn handle_initialize_upgraded_medium_for_endpoint(
        &mut self,
        _client: &ClientProxy,
        upgrade_service_id: &str,
        _endpoint_id: &str,
    ) -> Vec<u8> {
        // Reuse the existing SoftAP + listener for this service, or stand up a new
        // one. (Ref: StartWifiHotspot → StartAcceptingConnections → GetCredentials,
        // in that order — credentials aren't valid until the AP + server socket
        // exist.)
        let (addr, creds) = match self.listeners.get(upgrade_service_id) {
            Some(hl) => (hl.listener.addr, hl.creds.clone()),
            None => {
                let creds = match self.softap.start(upgrade_service_id) {
                    Some(creds) => creds,
                    None => return Vec::new(), // EMPTY = MEDIUM_ERROR
                };
                let listener = match start_listener(
                    creds.bind_ip,
                    upgrade_service_id,
                    CHANNEL_NAME,
                    Medium::WifiHotspot,
                    self.sink.clone(),
                ) {
                    Some(listener) => listener,
                    None => {
                        self.softap.stop(upgrade_service_id);
                        return Vec::new();
                    }
                };
                let addr = listener.addr;
                self.listeners.insert(
                    upgrade_service_id.to_string(),
                    HotspotListener {
                        listener,
                        creds: creds.clone(),
                    },
                );
                (addr, creds)
            }
        };

        let credentials = WifiHotspotCredentials {
            ssid: Some(creds.ssid),
            password: Some(creds.password),
            port: Some(i32::from(addr.port())),
            gateway: Some(creds.gateway.to_string()),
            frequency: Some(creds.frequency),
            ..Default::default()
        };
        // The consumer drives UKEY2 over the channel, so we do NOT signal
        // disabling encryption (matches the Pixel-validated spike frame).
        for_bwu_wifi_hotspot_path_available(credentials, false)
    }

    fn handle_revert_initiator_state_for_service(&mut self, upgrade_service_id: &str) {
        if let Some(hl) = self.listeners.remove(upgrade_service_id) {
            stop_listener(hl.listener);
            self.softap.stop(upgrade_service_id);
        }
    }

    fn create_upgraded_endpoint_channel(
        &mut self,
        _client: &ClientProxy,
        service_id: &str,
        _endpoint_id: &str,
        upgrade_path_info: &UpgradePathInfo,
    ) -> Option<Arc<dyn EndpointChannel>> {
        // Ref: no credentials → CONNECTIVITY_WIFI_HOTSPOT_INVALID_CREDENTIAL.
        let creds_pb = upgrade_path_info.wifi_hotspot_credentials.as_ref()?;
        let gateway: Ipv4Addr = creds_pb.gateway.as_deref()?.parse().ok()?;
        let port = u16::try_from(creds_pb.port?).ok()?;
        let creds = HotspotCreds {
            ssid: creds_pb.ssid.clone().unwrap_or_default(),
            password: creds_pb.password.clone().unwrap_or_default(),
            frequency: creds_pb.frequency.unwrap_or(-1),
            gateway,
            bind_ip: gateway,
        };
        // Join the AP as a STA (ref: ConnectWifiHotspot), then dial the gateway.
        self.softap.connect_as_sta(&creds)?;
        let addr = SocketAddr::from((gateway, port));
        let stream = TcpStream::connect(addr).ok()?;
        tcp_channel(stream, service_id, CHANNEL_NAME, Medium::WifiHotspot)
    }

    fn get_upgrade_medium(&self) -> Medium {
        Medium::WifiHotspot
    }

    fn on_endpoint_disconnect(&mut self, _client: &ClientProxy, _endpoint_id: &str) {
        // Responder teardown: leave the hotspot we joined as a STA (ref:
        // RevertResponderState → DisconnectWifiHotspot). No-op on the initiator,
        // which never joined as a STA and tears its AP down via the revert path.
        self.softap.disconnect_sta();
    }
}

/// A test [`SoftAp`] that does no real radio work: `start` returns loopback creds
/// (so the listener binds `127.0.0.1` and the STA dials `127.0.0.1` over real TCP)
/// and `connect_as_sta` / `disconnect_sta` flip an "is connected" flag.
pub struct FakeSoftAp {
    ssid: String,
    password: String,
    frequency: i32,
    connected: AtomicBool,
}

impl FakeSoftAp {
    pub fn new() -> Self {
        Self {
            ssid: "DIRECT-beamish-test".to_string(),
            // >= 8 chars to satisfy the WIFI_PASSWORD-length validator bound.
            password: "testpassword".to_string(),
            frequency: 2437,
            connected: AtomicBool::new(false),
        }
    }
}

impl Default for FakeSoftAp {
    fn default() -> Self {
        Self::new()
    }
}

impl SoftAp for FakeSoftAp {
    fn start(&self, _service_id: &str) -> Option<HotspotCreds> {
        Some(HotspotCreds {
            ssid: self.ssid.clone(),
            password: self.password.clone(),
            frequency: self.frequency,
            gateway: Ipv4Addr::LOCALHOST,
            bind_ip: Ipv4Addr::LOCALHOST,
        })
    }

    fn connect_as_sta(&self, _creds: &HotspotCreds) -> Option<()> {
        self.connected.store(true, Ordering::SeqCst);
        Some(())
    }

    fn stop(&self, _service_id: &str) {}

    fn disconnect_sta(&self) {
        self.connected.store(false, Ordering::SeqCst);
    }

    fn is_connected_to_hotspot(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}
