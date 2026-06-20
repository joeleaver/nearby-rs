# nearby-rs

A faithful, **test-first** Rust port of [Google's Nearby Connections][nearby]
protocol — the peer-to-peer transport that powers Quick Share.

The goal is a clean-room-ish, pure-logic core (wire format + the
bandwidth-upgrade state machine) that can be reused by any Rust implementation,
with Google's own test suite ported alongside the code so the port is pinned to
upstream behaviour rather than to one author's understanding of it.

## Status

**Phase 1 — offline wire format + validator.** Done.
**Phase 2 — bandwidth-upgrade (BWU) state machine.** Done.

| Module | Ported from (google/nearby) | Tests |
| --- | --- | --- |
| `proto/offline_wire_formats.proto` | `connections/implementation/proto/offline_wire_formats.proto` (verbatim) | — |
| `src/frames.rs` | `offline_frames.cc` | `tests/offline_frames.rs` (24 golden round-trips) |
| `src/validator.rs` | `offline_frames_validator.cc` | `tests/offline_frames_validator.rs` (36 cases) |
| `src/mediums.rs` | `proto/connections_enums.proto` (the `Medium` / `WifiDirectAuthType` enums) | — |
| `src/bwu/` | `bwu_manager.cc`, `base_bwu_handler.cc`, `service_id_constants.h`, the fakes | `tests/bwu_manager.rs` (23 cases) + unit tests |

Each golden test mirrors a C++ `EXPECT_THAT(msg, EqualsProto(...))` assertion:
build the frame with the same `for_*` call, `from_bytes()` it (which also runs
the validator, exactly as `FromBytes` does), and assert structural equality
against the Rust struct that mirrors the golden text-proto. Because prost derives
presence-aware `PartialEq`, this reproduces `EqualsProto` semantics and pins the
encoding byte-compatibly against upstream.

The BWU layer (`src/bwu/`) is a **plain synchronous owned state machine** — the
C++ serial executor maps to "run inline", so the 23-case `bwu_manager_test`
oracle stays deterministic. It drives the full upgrade handshake (pause → channel
swap → `LAST_WRITE` → drain → `SAFE_TO_CLOSE` → close-`UPGRADED`), the early-race
latch, the initiator/responder paths, and `OnEndpointDisconnect`/revert with both
`support_multiple_bwu_mediums` branches. A Tokio-actor wrapper (plus the retry
machinery and a real medium handler) is Phase 3.

```
cargo test   # 101 tests: 24 golden + 36 validator + 23 BWU + 18 unit
```

### Why a full proto, not a subset?

The vendored `offline_wire_formats.proto` is kept **byte-for-byte faithful** to
upstream — including `AWDL` mediums/credentials, `upgrade_path_request`,
`address_candidates`, `safe_to_disconnect_version`,
`ClientIntroduction.last_endpoint_id`, `KeepAliveFrame.seq_num`,
`connections_device`/`presence_device`, and `medium_role`. Several of these are
exercised by the golden tests and are absent from the trimmed proto carried by
some existing implementations; restoring them is what makes "pin our wire format
vs Google's" meaningful.

## Roadmap

- **Phase 3** — wrap the state machine in a Tokio actor and a concrete WIFI_LAN
  `BwuHandler` (TcpListener), port the retry machinery
  (`TryNextBestUpgradeMediums` / retry alarms), and route an existing
  UKEY2 + WIFI_LAN + L2CAP stack's upgrade through this crate as the tested
  protocol core — validated against a real device.
- **Phase 4+** — a Linux direct-medium handler (SoftAP / Wi-Fi Direct).

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at
your option. This crate is a derivative of Google's Apache-2.0–licensed Nearby
project; see [NOTICE](NOTICE) for attribution.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be dual licensed as above, without any
additional terms or conditions.

[nearby]: https://github.com/google/nearby
