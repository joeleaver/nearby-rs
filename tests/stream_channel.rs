//! Tests for `StreamChannel` (the `base_endpoint_channel.cc` port).
//!
//! These mirror the portable subset of Google's `base_endpoint_channel_test.cc`
//! (read/write round-trips, encrypted vs plaintext, try-decrypt, close-unblocks-
//! read, pause/resume, and the unencrypted-KEEP_ALIVE-on-an-encrypted-channel
//! edge case). The C++ encryption tests drive a real UKEY2 handshake; here the
//! encryption is the `Cipher` seam, exercised with a marker cipher. The
//! BLE/L2CAP `DispatchPacket` flag tests are out of scope.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use nearby_rs::bwu::{Cipher, EndpointChannel, Pipe, StreamChannel};
use nearby_rs::frames::{for_bwu_last_write, for_keep_alive, Exception};
use nearby_rs::mediums::Medium;

const MARK: u8 = 0xE5;

/// A stand-in for UKEY2: `encode` prepends a marker byte, `decode` strips it (and
/// fails on anything lacking the marker, like a real cipher fed plaintext).
struct MarkerCipher;

impl Cipher for MarkerCipher {
    fn encode(&self, plaintext: &[u8]) -> Option<Vec<u8>> {
        let mut out = Vec::with_capacity(plaintext.len() + 1);
        out.push(MARK);
        out.extend_from_slice(plaintext);
        Some(out)
    }
    fn decode(&self, ciphertext: &[u8]) -> Option<Vec<u8>> {
        match ciphertext.split_first() {
            Some((&MARK, rest)) => Some(rest.to_vec()),
            _ => None,
        }
    }
}

fn channel(name: &str) -> Arc<StreamChannel> {
    Arc::new(StreamChannel::new(
        "svc",
        name,
        Medium::WifiLan,
        Pipe::new(),
    ))
}

#[test]
fn read_write_round_trips_unencrypted() {
    let ch = channel("rw");
    assert!(!ch.is_encrypted());

    assert_eq!(ch.write(b"hello world"), Exception::Success);
    assert_eq!(ch.read().unwrap(), b"hello world");

    // An empty frame round-trips (length 0, no payload).
    assert_eq!(ch.write(b""), Exception::Success);
    assert_eq!(ch.read().unwrap(), b"");
}

#[test]
fn encrypted_round_trips() {
    let ch = channel("enc");
    ch.enable_encryption(Arc::new(MarkerCipher));
    assert!(ch.is_encrypted());

    assert_eq!(ch.write(b"secret"), Exception::Success);
    assert_eq!(ch.read().unwrap(), b"secret");
}

#[test]
fn encrypted_payload_is_ciphertext_on_the_wire() {
    // A second, unencrypted channel sharing the same pipe sees the raw framed
    // bytes — they must be ciphertext, not the plaintext (cannot be intercepted).
    let pipe = Pipe::new();
    let enc = Arc::new(StreamChannel::new(
        "svc",
        "enc",
        Medium::WifiLan,
        pipe.clone(),
    ));
    enc.enable_encryption(Arc::new(MarkerCipher));
    let wire = Arc::new(StreamChannel::new(
        "svc",
        "wire",
        Medium::WifiLan,
        pipe.clone(),
    ));

    assert_eq!(enc.write(b"secret"), Exception::Success);

    let on_wire = wire.read().unwrap();
    let mut expected_ciphertext = vec![MARK];
    expected_ciphertext.extend_from_slice(b"secret");
    assert_eq!(on_wire, expected_ciphertext);
    assert_ne!(on_wire, b"secret");
}

#[test]
fn try_decrypt_behaviour() {
    let ch = channel("td");
    // No encryption configured → Failed.
    assert_eq!(ch.try_decrypt(b"anything"), Err(Exception::Failed));

    ch.enable_encryption(Arc::new(MarkerCipher));
    let mut ciphertext = vec![MARK];
    ciphertext.extend_from_slice(b"hi");
    assert_eq!(ch.try_decrypt(&ciphertext).unwrap(), b"hi");
    // Data the cipher rejects → Execution.
    assert_eq!(ch.try_decrypt(b"no-marker"), Err(Exception::Execution));
}

#[test]
fn read_after_close_returns_io() {
    let ch = channel("closed");
    ch.close();
    assert_eq!(ch.read(), Err(Exception::Io));
    // Closing again is idempotent.
    ch.close();
}

#[test]
fn paused_write_blocks_until_resume() {
    let ch = channel("paused");
    ch.pause();

    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let ch = ch.clone();
        let done = done.clone();
        thread::spawn(move || {
            assert_eq!(ch.write(b"x"), Exception::Success);
            done.store(true, Ordering::SeqCst);
        })
    };

    // While paused, the write must not complete.
    thread::sleep(Duration::from_millis(50));
    assert!(
        !done.load(Ordering::SeqCst),
        "write completed while the channel was paused"
    );

    ch.resume();
    writer.join().unwrap();
    assert!(done.load(Ordering::SeqCst));
    assert_eq!(ch.read().unwrap(), b"x");
}

#[test]
fn unencrypted_keepalive_passes_but_other_frames_rejected_on_encrypted_channel() {
    let pipe = Pipe::new();
    let enc = Arc::new(StreamChannel::new(
        "svc",
        "enc",
        Medium::WifiLan,
        pipe.clone(),
    ));
    enc.enable_encryption(Arc::new(MarkerCipher));
    // A peer that has not yet switched to encryption writes plaintext frames.
    let peer = Arc::new(StreamChannel::new(
        "svc",
        "peer",
        Medium::WifiLan,
        pipe.clone(),
    ));

    // A plaintext KEEP_ALIVE is let through despite decryption failing.
    let keep_alive = for_keep_alive();
    assert_eq!(peer.write(&keep_alive), Exception::Success);
    assert_eq!(enc.read().unwrap(), keep_alive);

    // Any other unencrypted frame is rejected.
    assert_eq!(peer.write(&for_bwu_last_write()), Exception::Success);
    assert_eq!(enc.read(), Err(Exception::InvalidProtocolBuffer));
}
