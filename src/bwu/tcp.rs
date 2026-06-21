//! Shared TCP plumbing for the concrete TCP-based medium handlers (WIFI_LAN and
//! WIFI_HOTSPOT). Hoisted out of `wifi_lan.rs` so both handlers share one copy of
//! the `DuplexStream`-over-`TcpStream` adapter, the accept loop, and the listener
//! lifecycle.
//!
//! Encryption is the consumer's concern: the channels here are plaintext until
//! the consumer calls [`StreamChannel::enable_encryption`] with its UKEY2 cipher.

use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::bwu::channel::EndpointChannel;
use crate::bwu::handler::IncomingSocketConnection;
use crate::bwu::stream_channel::{DuplexStream, StreamChannel};
use crate::frames::Exception;
use crate::mediums::Medium;

/// Receives upgraded sockets the initiator's accept loop produces. In the Tokio
/// integration this posts a `BwuCommand::IncomingConnection`; in tests it pushes
/// onto a channel.
pub type ConnectionSink = Arc<dyn Fn(IncomingSocketConnection) + Send + Sync>;

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

/// Wraps an accepted/connected `TcpStream` in a [`StreamChannel`] for `medium`.
pub fn tcp_channel(
    stream: TcpStream,
    service_id: &str,
    channel_name: &str,
    medium: Medium,
) -> Option<Arc<dyn EndpointChannel>> {
    let duplex = TcpDuplexStream::new(stream).ok()?;
    Some(Arc::new(StreamChannel::new(
        service_id,
        channel_name,
        medium,
        Arc::new(duplex),
    )))
}

/// A running accept loop bound to `addr`, with a stop flag + join handle.
pub struct Listener {
    pub addr: SocketAddr,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

fn accept_loop(
    listener: TcpListener,
    service_id: String,
    channel_name: &'static str,
    medium: Medium,
    sink: ConnectionSink,
    stop: Arc<AtomicBool>,
) {
    for incoming in listener.incoming() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        match incoming {
            Ok(stream) => {
                if let Some(channel) = tcp_channel(stream, &service_id, channel_name, medium) {
                    sink(IncomingSocketConnection { channel });
                }
            }
            Err(_) => break,
        }
    }
}

/// Bind a listener on `bind_ip:0` and spawn its accept loop, wrapping each
/// accepted socket as `medium` and firing `sink`. Returns the [`Listener`] (whose
/// `addr` carries the OS-assigned port) or `None` on bind failure (= MEDIUM_ERROR).
pub fn start_listener(
    bind_ip: Ipv4Addr,
    service_id: &str,
    channel_name: &'static str,
    medium: Medium,
    sink: ConnectionSink,
) -> Option<Listener> {
    let listener = TcpListener::bind((bind_ip, 0)).ok()?;
    let addr = listener.local_addr().ok()?;
    let stop = Arc::new(AtomicBool::new(false));
    let join = {
        let (sink, service_id, stop) = (sink, service_id.to_string(), stop.clone());
        std::thread::spawn(move || {
            accept_loop(listener, service_id, channel_name, medium, sink, stop)
        })
    };
    Some(Listener {
        addr,
        stop,
        join: Some(join),
    })
}

/// Stop a listener: signal the loop, unblock its blocking `accept()` with a
/// throwaway connection, and join the thread.
pub fn stop_listener(mut listener: Listener) {
    listener.stop.store(true, Ordering::SeqCst);
    let _ = TcpStream::connect(listener.addr);
    if let Some(join) = listener.join.take() {
        let _ = join.join();
    }
}
