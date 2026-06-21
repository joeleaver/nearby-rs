//! `StreamChannel` — a concrete [`EndpointChannel`] over a blocking byte stream.
//!
//! A port of `connections/implementation/base_endpoint_channel.cc`: each frame
//! is a 4-byte big-endian length prefix followed by the (optionally encrypted)
//! payload (`Base64Utils::WriteInt`/`ReadInt` — despite the name, a raw int, not
//! base64). The encryption layer is a **seam**: the consumer supplies a [`Cipher`]
//! (in the real stack, the UKEY2 session — `EncodeMessageToPeer`/
//! `DecodeMessageFromPeer`); nearby-rs itself ships none, so a channel is
//! plaintext until [`StreamChannel::enable_encryption`] is called. The byte
//! transport is the [`DuplexStream`] seam (a TCP socket in production; the
//! in-memory [`Pipe`] in tests).
//!
//! Like the C++ original the channel is **blocking** (`read`/`write` block the
//! calling thread, as Google's dedicated reader threads expect) and uses interior
//! mutability so it can be shared as `Arc<dyn EndpointChannel>`. `close` does not
//! take the reader/writer locks (a read/write may be in progress); it closes the
//! underlying stream, which unblocks an in-flight read with [`Exception::NoData`]
//! (EOF) — matching the C++ `kNoData` — or, on a genuine transport error,
//! [`Exception::Io`].

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

use crate::bwu::channel::{DisconnectionReason, EndpointChannel};
use crate::frames::{from_bytes, get_frame_type, Exception};
use crate::mediums::Medium;
use crate::proto as pb;

/// The encryption seam (C++ `BaseEndpointChannel::EncryptionContext`). The real
/// stack backs this with the UKEY2 session; `None` from either method signals
/// failure (e.g. a decrypt of a frame from before encryption was established).
pub trait Cipher: Send + Sync {
    /// `EncodeMessageToPeer` — encrypt an outgoing plaintext frame.
    fn encode(&self, plaintext: &[u8]) -> Option<Vec<u8>>;
    /// `DecodeMessageFromPeer` — decrypt an incoming frame.
    fn decode(&self, ciphertext: &[u8]) -> Option<Vec<u8>>;
}

/// The blocking byte-transport seam under a [`StreamChannel`] (the C++
/// `InputStream`/`OutputStream` pair). Methods take `&self`; an implementation
/// is internally synchronized and shareable. `close` must interrupt any blocked
/// `read_exact`/`write_all` (so an in-progress channel read returns).
pub trait DuplexStream: Send + Sync {
    /// Block until exactly `buf.len()` bytes are read. `Err(NoData)` if the
    /// stream reaches EOF / is closed first (the C++ `ReadExactly` `kNoData`);
    /// `Err(Io)` on a transport error.
    fn read_exact(&self, buf: &mut [u8]) -> Result<(), Exception>;
    /// Write every byte. `Err(Io)` on error/close.
    fn write_all(&self, buf: &[u8]) -> Result<(), Exception>;
    fn flush(&self) -> Result<(), Exception>;
    /// Interrupt any in-flight read/write and close. Idempotent.
    fn close(&self);
}

/// The C++ default `kMediumMaxAllowedReadBytes` ceiling is effectively `INT_MAX`.
const DEFAULT_MAX_ALLOWED_READ_BYTES: usize = i32::MAX as usize;

#[derive(Default)]
struct PauseState {
    paused: bool,
    closed: bool,
}

/// A blocking, length-framed [`EndpointChannel`] over a [`DuplexStream`].
pub struct StreamChannel {
    service_id: String,
    name: String,
    medium: Medium,
    max_allowed_read_bytes: usize,
    stream: Arc<dyn DuplexStream>,
    /// Serializes a whole framed read (length + payload); mirrors `reader_mutex_`.
    reader_lock: Mutex<()>,
    /// Serializes a whole framed write (encrypt + length + payload + flush).
    writer_lock: Mutex<()>,
    cipher: Mutex<Option<Arc<dyn Cipher>>>,
    pause: Mutex<PauseState>,
    pause_cond: Condvar,
    local_endpoint_id: Mutex<String>,
}

impl StreamChannel {
    pub fn new(
        service_id: impl Into<String>,
        name: impl Into<String>,
        medium: Medium,
        stream: Arc<dyn DuplexStream>,
    ) -> Self {
        Self {
            service_id: service_id.into(),
            name: name.into(),
            medium,
            max_allowed_read_bytes: DEFAULT_MAX_ALLOWED_READ_BYTES,
            stream,
            reader_lock: Mutex::new(()),
            writer_lock: Mutex::new(()),
            cipher: Mutex::new(None),
            pause: Mutex::new(PauseState::default()),
            pause_cond: Condvar::new(),
            local_endpoint_id: Mutex::new(String::new()),
        }
    }

    /// `EnableEncryption` — route subsequent reads/writes through `cipher`.
    pub fn enable_encryption(&self, cipher: Arc<dyn Cipher>) {
        *self.cipher.lock().unwrap() = Some(cipher);
    }

    /// `IsEncrypted`.
    pub fn is_encrypted(&self) -> bool {
        self.cipher.lock().unwrap().is_some()
    }

    /// `TryDecrypt` — `Err(Failed)` if encryption is off, `Err(Execution)` if the
    /// cipher rejects the data.
    pub fn try_decrypt(&self, data: &[u8]) -> Result<Vec<u8>, Exception> {
        match self.cipher.lock().unwrap().as_ref() {
            None => Err(Exception::Failed),
            Some(cipher) => cipher.decode(data).ok_or(Exception::Execution),
        }
    }

    fn do_close(&self) {
        {
            let mut state = self.pause.lock().unwrap();
            if state.closed {
                return;
            }
            state.closed = true;
            // Unblock a writer parked in the pause wait so it can fail on the
            // now-closed stream (C++ `UnblockPausedWriter`).
            state.paused = false;
            self.pause_cond.notify_all();
        }
        // Deliberately NOT under reader_lock/writer_lock: a read/write may be in
        // progress, and closing the stream is what unblocks it (C++ `CloseIo`).
        self.stream.close();
    }
}

impl EndpointChannel for StreamChannel {
    fn read(&self) -> Result<Vec<u8>, Exception> {
        let raw = {
            let _guard = self.reader_lock.lock().unwrap();
            let mut len_buf = [0u8; 4];
            self.stream.read_exact(&mut len_buf)?;
            let len = i32::from_be_bytes(len_buf);
            if len < 0 || len as usize > self.max_allowed_read_bytes {
                return Err(Exception::Io);
            }
            let mut data = vec![0u8; len as usize];
            self.stream.read_exact(&mut data)?;
            data
        };

        let cipher = self.cipher.lock().unwrap().clone();
        let Some(cipher) = cipher else {
            return Ok(raw);
        };
        if let Some(plaintext) = cipher.decode(&raw) {
            // A successful-but-empty decrypt is treated as an error, mirroring
            // the C++ `if (result.Empty())` guard on the encrypted path.
            if plaintext.is_empty() {
                return Err(Exception::InvalidProtocolBuffer);
            }
            return Ok(plaintext);
        }
        // Decrypt failed. It may be a protocol race where the peer sent a
        // plaintext KEEP_ALIVE before switching to encryption; let that one
        // frame through, otherwise the frame is unreadable.
        match from_bytes(&raw) {
            Ok(frame) if get_frame_type(&frame) == pb::v1_frame::FrameType::KeepAlive => Ok(raw),
            // Parsed but not a KEEP_ALIVE: C++ keeps the default
            // `kInvalidProtocolBuffer`.
            Ok(_) => Err(Exception::InvalidProtocolBuffer),
            // Parse/validation failed: forward the validator's specific code
            // (e.g. `IllegalCharacters`), as C++ does via `parsed.exception()`.
            Err(exception) => Err(exception),
        }
    }

    fn write(&self, data: &[u8]) -> Exception {
        // Block while paused (the upgrade protocol parks the new channel until
        // the old one drains). `close`/`resume` release us.
        {
            let mut state = self.pause.lock().unwrap();
            while state.paused {
                state = self.pause_cond.wait(state).unwrap();
            }
        }

        let _guard = self.writer_lock.lock().unwrap();
        let payload = {
            let cipher = self.cipher.lock().unwrap();
            match cipher.as_ref() {
                Some(cipher) => match cipher.encode(data) {
                    Some(encrypted) => encrypted,
                    None => return Exception::Io,
                },
                None => data.to_vec(),
            }
        };
        if payload.len() > self.max_allowed_read_bytes {
            return Exception::Io;
        }
        let len = payload.len() as i32;
        if self.stream.write_all(&len.to_be_bytes()).is_err() {
            return Exception::Io;
        }
        if self.stream.write_all(&payload).is_err() {
            return Exception::Io;
        }
        if self.stream.flush().is_err() {
            return Exception::Io;
        }
        Exception::Success
    }

    fn close(&self) {
        self.do_close();
    }

    fn close_with_reason(&self, _reason: DisconnectionReason) {
        // The reason is analytics-only (see `EndpointChannel`); closing is the
        // same regardless.
        self.do_close();
    }

    fn medium(&self) -> Medium {
        self.medium
    }

    fn service_id(&self) -> String {
        self.service_id.clone()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn channel_type(&self) -> String {
        // C++ `GetType`: "ENCRYPTED_<MEDIUM>" when encrypted, else "<MEDIUM>".
        let prefix = if self.is_encrypted() {
            "ENCRYPTED_"
        } else {
            ""
        };
        format!("{prefix}{:?}", self.medium)
    }

    fn local_endpoint_id(&self) -> String {
        self.local_endpoint_id.lock().unwrap().clone()
    }

    fn set_local_endpoint_id(&self, local_endpoint_id: &str) {
        *self.local_endpoint_id.lock().unwrap() = local_endpoint_id.to_string();
    }

    fn pause(&self) {
        self.pause.lock().unwrap().paused = true;
    }

    fn resume(&self) {
        let mut state = self.pause.lock().unwrap();
        state.paused = false;
        self.pause_cond.notify_all();
    }

    fn is_paused(&self) -> bool {
        self.pause.lock().unwrap().paused
    }

    // Same body as the inherent `enable_encryption` above; exposed on the trait so
    // a consumer holding an `Arc<dyn EndpointChannel>` can install a cipher.
    fn enable_encryption(&self, cipher: Arc<dyn Cipher>) {
        *self.cipher.lock().unwrap() = Some(cipher);
    }

    fn disable_encryption(&self) {
        *self.cipher.lock().unwrap() = None;
    }
}

// ---------------------------------------------------------------------------
// Pipe — an in-memory DuplexStream
// ---------------------------------------------------------------------------

#[derive(Default)]
struct PipeState {
    buf: VecDeque<u8>,
    closed: bool,
}

/// A simple in-memory [`DuplexStream`]: bytes written are readable back from the
/// same buffer (a self-loopback). Useful for tests and for wiring two channels
/// over one buffer; `close` unblocks any parked reader.
pub struct Pipe {
    state: Mutex<PipeState>,
    cond: Condvar,
}

impl Pipe {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PipeState::default()),
            cond: Condvar::new(),
        })
    }
}

impl DuplexStream for Pipe {
    fn read_exact(&self, buf: &mut [u8]) -> Result<(), Exception> {
        let mut state = self.state.lock().unwrap();
        let mut filled = 0;
        while filled < buf.len() {
            if state.buf.is_empty() {
                if state.closed {
                    // EOF before the requested bytes — `kNoData`, not `kIo`.
                    return Err(Exception::NoData);
                }
                state = self.cond.wait(state).unwrap();
                continue;
            }
            while filled < buf.len() {
                match state.buf.pop_front() {
                    Some(byte) => {
                        buf[filled] = byte;
                        filled += 1;
                    }
                    None => break,
                }
            }
        }
        Ok(())
    }

    fn write_all(&self, buf: &[u8]) -> Result<(), Exception> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(Exception::Io);
        }
        state.buf.extend(buf.iter().copied());
        self.cond.notify_all();
        Ok(())
    }

    fn flush(&self) -> Result<(), Exception> {
        Ok(())
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        self.cond.notify_all();
    }
}
