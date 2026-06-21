//! A concrete WIFI_LAN [`MediumBwuHandler`] over TCP.
//!
//! This is the first real medium handler: the **initiator** binds a
//! `TcpListener`, advertises its `ip:port` in a WIFI_LAN `UPGRADE_PATH_AVAILABLE`
//! frame, and runs an accept loop that wraps each accepted socket in a
//! [`StreamChannel`] and hands it to the BWU layer via a *connection sink*
//! callback (the C++ `OnIncomingConnection` bind_front target — in the Tokio
//! integration the sink posts a `BwuCommand::IncomingConnection`). The
//! **responder** reads those credentials from the `UpgradePathInfo` and dials a
//! `TcpStream` back, wrapping it in a [`StreamChannel`].
//!
//! Encryption is the consumer's concern: the channels here are plaintext until
//! the consumer calls [`StreamChannel::enable_encryption`] with its UKEY2 cipher.
//!
//! Unlike the rest of the crate this has no upstream test oracle (Google ships no
//! Linux WIFI_LAN backend); it is pinned by the loopback integration test in
//! `tests/wifi_lan.rs`.

use std::collections::HashMap;
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::bwu::channel::EndpointChannel;
use crate::bwu::client::ClientProxy;
use crate::bwu::handler::{IncomingSocketConnection, MediumBwuHandler};
use crate::bwu::stream_channel::{DuplexStream, StreamChannel};
use crate::frames::{for_bwu_wifi_lan_path_available, Exception, ServiceAddress};
use crate::mediums::Medium;
use crate::proto as pb;

type UpgradePathInfo = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo;

/// Receives upgraded sockets the initiator's accept loop produces. In the Tokio
/// integration this posts a `BwuCommand::IncomingConnection`; in tests it pushes
/// onto a channel.
pub type ConnectionSink = Arc<dyn Fn(IncomingSocketConnection) + Send + Sync>;

const CHANNEL_NAME: &str = "wifi-lan";

/// A [`DuplexStream`] over a `std::net::TcpStream`. `close` shuts the socket down
/// (both directions), which unblocks a blocked `read_exact` with EOF.
pub struct TcpDuplexStream {
    reader: Mutex<TcpStream>,
    writer: Mutex<TcpStream>,
    shutdown: TcpStream,
}

impl TcpDuplexStream {
    pub fn new(stream: TcpStream) -> std::io::Result<Self> {
        Ok(Self {
            reader: Mutex::new(stream.try_clone()?),
            writer: Mutex::new(stream.try_clone()?),
            shutdown: stream,
        })
    }
}

impl DuplexStream for TcpDuplexStream {
    fn read_exact(&self, buf: &mut [u8]) -> Result<(), Exception> {
        use std::io::Read;
        match self.reader.lock().unwrap().read_exact(buf) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Err(Exception::NoData),
            Err(_) => Err(Exception::Io),
        }
    }

    fn write_all(&self, buf: &[u8]) -> Result<(), Exception> {
        use std::io::Write;
        self.writer
            .lock()
            .unwrap()
            .write_all(buf)
            .map_err(|_| Exception::Io)
    }

    fn flush(&self) -> Result<(), Exception> {
        use std::io::Write;
        self.writer
            .lock()
            .unwrap()
            .flush()
            .map_err(|_| Exception::Io)
    }

    fn close(&self) {
        let _ = self.shutdown.shutdown(Shutdown::Both);
    }
}

/// Wraps an accepted/connected `TcpStream` in a WIFI_LAN [`StreamChannel`].
fn tcp_channel(stream: TcpStream, service_id: &str) -> Option<Arc<dyn EndpointChannel>> {
    let duplex = TcpDuplexStream::new(stream).ok()?;
    Some(Arc::new(StreamChannel::new(
        service_id,
        CHANNEL_NAME,
        Medium::WifiLan,
        Arc::new(duplex),
    )))
}

struct Listener {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

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

fn accept_loop(
    listener: TcpListener,
    service_id: String,
    sink: ConnectionSink,
    stop: Arc<AtomicBool>,
) {
    for incoming in listener.incoming() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        match incoming {
            Ok(stream) => {
                if let Some(channel) = tcp_channel(stream, &service_id) {
                    sink(IncomingSocketConnection { channel });
                }
            }
            Err(_) => break,
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
        let addr = match self.listeners.get(upgrade_service_id) {
            Some(listener) => listener.addr,
            None => {
                let listener = match TcpListener::bind((self.bind_ip, 0)) {
                    Ok(listener) => listener,
                    Err(_) => return Vec::new(), // EMPTY = MEDIUM_ERROR
                };
                let addr = match listener.local_addr() {
                    Ok(addr) => addr,
                    Err(_) => return Vec::new(),
                };
                let stop = Arc::new(AtomicBool::new(false));
                let join = {
                    let (sink, service_id, stop) = (
                        self.sink.clone(),
                        upgrade_service_id.to_string(),
                        stop.clone(),
                    );
                    std::thread::spawn(move || accept_loop(listener, service_id, sink, stop))
                };
                self.listeners.insert(
                    upgrade_service_id.to_string(),
                    Listener {
                        addr,
                        stop,
                        join: Some(join),
                    },
                );
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
        if let Some(mut listener) = self.listeners.remove(upgrade_service_id) {
            listener.stop.store(true, Ordering::SeqCst);
            // Unblock the accept loop so it can observe `stop` and exit.
            let _ = TcpStream::connect(listener.addr);
            if let Some(join) = listener.join.take() {
                let _ = join.join();
            }
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
        tcp_channel(stream, service_id)
    }

    fn get_upgrade_medium(&self) -> Medium {
        Medium::WifiLan
    }

    fn on_endpoint_disconnect(&mut self, _client: &ClientProxy, _endpoint_id: &str) {}
}
