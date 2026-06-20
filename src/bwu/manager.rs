//! `BwuManager` — the bandwidth-upgrade state machine.
//!
//! A faithful port of `connections/implementation/bwu_manager.cc`. Per the
//! porting spec it is a PLAIN SYNCHRONOUS owned state machine (the C++
//! `serial_executor_` becomes "run inline"; a Tokio actor would wrap this at the
//! integration layer). It owns all of its maps, so no locks are needed; the
//! shared `EndpointChannelManager` is an `Rc<RefCell<_>>` and channels are
//! `Arc<dyn EndpointChannel>` (a stashed clone survives the upgrade swap).
//!
//! OMITTED (per spec + project scope): all analytics, dynamic role switch
//! (`UPGRADE_PATH_REQUEST`/`NeedToSwitchRole`/aliasing), Apple BLE scanning,
//! `ChooseBestUpgradeMedium`/`StripOutUnavailableMediums` (tests pass an explicit
//! medium + explicit handlers), and the dead `safe_to_close_write_timestamps_`.
//!
//! Status: the full synchronous state machine the 23-case `bwu_manager_test`
//! oracle exercises is in place — initiate, dispatch, the responder path, the
//! converged upgrade protocol, out-of-order handling, and
//! `OnEndpointDisconnect`/revert (both feature-flag branches). The retry
//! machinery (`TryNextBestUpgradeMediums` / `ChooseBestUpgradeMedium` /
//! `RetryUpgradesAfterDelay` / `CalculateNextRetryDelay` / the retry-alarm maps)
//! is also ported; since the upstream oracle has NO retry tests, it is covered
//! by hand-authored tests in `tests/bwu_retry.rs`. The async timer is modelled
//! as a seam: the scheduled delay is recorded ([`BwuManager::pending_retry_delay`])
//! and the integration layer (the Phase-3 Tokio actor) arms a real timer and
//! calls [`BwuManager::fire_retry_alarm`] when it elapses. The `ChooseBest`
//! medium-availability check maps to "a handler is registered" since the
//! platform radio layer (`Mediums`) is omitted.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::bwu::channel::{DisconnectionReason, EndpointChannel};
use crate::bwu::channel_manager::EndpointChannelManager;
use crate::bwu::client::ClientProxy;
use crate::bwu::handler::{BwuHandler, IncomingSocketConnection};
use crate::bwu::service_id::{
    is_initiator_upgrade_service_id, wrap_initiator_upgrade_service_id, UNKNOWN_SERVICE_ID,
};
use crate::frames::{
    for_bwu_failure, for_bwu_introduction, for_bwu_introduction_ack, for_bwu_last_write,
    for_bwu_safe_to_close, for_disconnection, from_bytes, get_frame_type,
    medium_to_upgrade_path_info_medium, upgrade_path_info_medium_to_medium,
};
use crate::mediums::Medium;
use crate::proto as pb;

use pb::bandwidth_upgrade_negotiation_frame::EventType;
type UpgradePathInfo = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo;

/// `BwuManager::Config` (the subset the port uses).
///
/// `support_multiple_bwu_mediums` and `use_exp_backoff_in_bwu_retry` are
/// `FeatureFlags` process-globals in C++; per the port's established pattern
/// (a global races across parallel Rust tests) they are explicit config here.
/// Their defaults match `internal/platform/feature_flags.h` (both `true`).
#[derive(Clone, Debug)]
pub struct BwuConfig {
    /// Selects the per-(medium,service) vs total-count revert bookkeeping.
    pub support_multiple_bwu_mediums: bool,
    /// When set, the BWU retry interval doubles each attempt (capped at
    /// `bandwidth_upgrade_retry_max_delay`); otherwise it grows linearly by
    /// `bandwidth_upgrade_retry_delay` each attempt.
    pub use_exp_backoff_in_bwu_retry: bool,
    /// Initial retry delay. `ZERO` means "resolve from the backoff flag" in the
    /// constructor (3s with exp backoff, 5s without), mirroring the C++ ctor.
    pub bandwidth_upgrade_retry_delay: Duration,
    /// Maximum retry delay. `ZERO` resolves to 300s (exp) / 10s (linear).
    pub bandwidth_upgrade_retry_max_delay: Duration,
}

impl Default for BwuConfig {
    fn default() -> Self {
        Self {
            support_multiple_bwu_mediums: true,
            use_exp_backoff_in_bwu_retry: true,
            bandwidth_upgrade_retry_delay: Duration::ZERO,
            bandwidth_upgrade_retry_max_delay: Duration::ZERO,
        }
    }
}

pub struct BwuManager {
    config: BwuConfig,
    ecm: Rc<RefCell<EndpointChannelManager>>,
    handlers: HashMap<Medium, Box<dyn BwuHandler>>,

    /// Single global upgrade medium (used when `support_multiple_bwu_mediums` is
    /// disabled).
    medium: Medium,
    /// Per-endpoint upgrade medium (used when `support_multiple_bwu_mediums`).
    endpoint_id_to_bwu_medium: HashMap<String, Medium>,

    /// Endpoints with an initiated-but-not-completed upgrade.
    in_progress_upgrades: HashSet<String>,
    /// The displaced OLD channel, parked until the SAFE_TO_CLOSE handshake.
    previous_endpoint_channels: HashMap<String, Arc<dyn EndpointChannel>>,
    /// Endpoints whose remote LAST_WRITE arrived before we parked the old
    /// channel (the early-LAST_WRITE race latch).
    successfully_upgraded_endpoints: HashSet<String>,

    /// Endpoints with a scheduled-but-not-yet-fired retry, keyed to the delay
    /// the (async) timer was armed with. Mirrors the C++ `retry_upgrade_alarms_`
    /// `(CancelableAlarm, delay)` map: here the alarm object is the integration
    /// seam (the Tokio actor arms a real timer from `pending_retry_delay` and
    /// calls `fire_retry_alarm` when it elapses), so we keep only the delay.
    /// Cleared on cancel; an entry's *absence* means "cancelled / already fired".
    retry_upgrade_alarms: HashMap<String, Duration>,
    /// The last retry delay used per endpoint, kept so the backoff can be
    /// resumed across a cancel (C++ `retry_delays_`). Unlike `retry_upgrade_alarms`
    /// this survives `cancel_retry_upgrade_alarm`; only `OnEndpointDisconnect`
    /// and `CancelAllRetryUpgradeAlarms` erase it.
    retry_delays: HashMap<String, Duration>,
}

impl BwuManager {
    pub fn new(
        ecm: Rc<RefCell<EndpointChannelManager>>,
        handlers: HashMap<Medium, Box<dyn BwuHandler>>,
        mut config: BwuConfig,
    ) -> Self {
        // Resolve zero-valued retry delays from the backoff flag (C++ ctor,
        // bwu_manager.cc:79-94, using the feature_flags.h defaults).
        if config.bandwidth_upgrade_retry_delay.is_zero() {
            config.bandwidth_upgrade_retry_delay = if config.use_exp_backoff_in_bwu_retry {
                Duration::from_secs(3)
            } else {
                Duration::from_secs(5)
            };
        }
        if config.bandwidth_upgrade_retry_max_delay.is_zero() {
            config.bandwidth_upgrade_retry_max_delay = if config.use_exp_backoff_in_bwu_retry {
                Duration::from_secs(300)
            } else {
                Duration::from_secs(10)
            };
        }
        Self {
            config,
            ecm,
            handlers,
            medium: Medium::UnknownMedium,
            endpoint_id_to_bwu_medium: HashMap::new(),
            in_progress_upgrades: HashSet::new(),
            previous_endpoint_channels: HashMap::new(),
            successfully_upgraded_endpoints: HashSet::new(),
            retry_upgrade_alarms: HashMap::new(),
            retry_delays: HashMap::new(),
        }
    }

    /// No-op: the port is already synchronous (the C++ serial executor maps to
    /// "run inline").
    pub fn make_single_threaded_for_testing(&mut self) {}

    pub fn is_upgrade_ongoing(&self, endpoint_id: &str) -> bool {
        self.in_progress_upgrades.contains(endpoint_id)
    }

    // -- bwu-medium map (feature-flag gated) --------------------------------

    fn get_bwu_medium_for_endpoint(&self, endpoint_id: &str) -> Medium {
        if self.config.support_multiple_bwu_mediums {
            self.endpoint_id_to_bwu_medium
                .get(endpoint_id)
                .copied()
                .unwrap_or(Medium::UnknownMedium)
        } else {
            self.medium
        }
    }

    fn set_bwu_medium_for_endpoint(&mut self, endpoint_id: &str, medium: Medium) {
        if self.config.support_multiple_bwu_mediums {
            self.endpoint_id_to_bwu_medium
                .insert(endpoint_id.to_string(), medium);
        } else {
            self.medium = medium;
        }
    }

    fn has_handler_for_medium(&self, medium: Medium) -> bool {
        medium != Medium::UnknownMedium && self.handlers.contains_key(&medium)
    }

    // -- initiator entry ----------------------------------------------------

    /// Initiates a bandwidth upgrade and sends `UPGRADE_PATH_AVAILABLE` to the
    /// remote (responder). The medium must be explicit (the port omits
    /// `ChooseBestUpgradeMedium`).
    pub fn initiate_bwu_for_endpoint(
        &mut self,
        client: &mut ClientProxy,
        endpoint_id: &str,
        new_medium: Medium,
    ) {
        let proposed_medium = new_medium;

        if self.ecm.borrow().is_wifi_lan_connected() && proposed_medium == Medium::WifiHotspot {
            // STA connecting to a hotspot would tear down WIFI_LAN.
            return;
        }

        self.set_bwu_medium_for_endpoint(endpoint_id, proposed_medium);

        if !self.has_handler_for_medium(proposed_medium) {
            return;
        }
        if self.in_progress_upgrades.contains(endpoint_id) {
            return;
        }
        // A fresh initiate supersedes any pending retry (bwu_manager.cc:257).
        self.cancel_retry_upgrade_alarm(endpoint_id);

        let channel = self.ecm.borrow().get_channel_for_endpoint(endpoint_id);
        let channel_medium = channel
            .as_ref()
            .map(|c| c.medium())
            .unwrap_or(Medium::UnknownMedium);
        // Don't upgrade to the medium we're already connected over.
        if proposed_medium == channel_medium {
            return;
        }
        let channel = match channel {
            Some(c) => c,
            None => return,
        };

        let service_id = channel.service_id();
        let bytes = self
            .handlers
            .get_mut(&proposed_medium)
            .unwrap()
            .initialize_upgraded_medium_for_endpoint(client, &service_id, endpoint_id);

        if bytes.is_empty() {
            let info = upgrade_path_info_for_medium(proposed_medium);
            self.process_upgrade_failure_event(client, endpoint_id, &info);
            return;
        }
        if !channel.write(&bytes).ok() {
            let info = upgrade_path_info_for_medium(proposed_medium);
            self.process_upgrade_failure_event(client, endpoint_id, &info);
            return;
        }
        self.in_progress_upgrades.insert(endpoint_id.to_string());
    }

    // -- incoming-frame dispatch -------------------------------------------

    /// `EndpointManager::FrameProcessor` entry for `BANDWIDTH_UPGRADE_NEGOTIATION`.
    pub fn on_incoming_frame(
        &mut self,
        frame: &pb::OfflineFrame,
        endpoint_id: &str,
        client: &mut ClientProxy,
        _medium: Medium,
    ) {
        if get_frame_type(frame) != pb::v1_frame::FrameType::BandwidthUpgradeNegotiation {
            return;
        }
        let bwu = match frame
            .v1
            .as_ref()
            .and_then(|v1| v1.bandwidth_upgrade_negotiation.as_ref())
        {
            Some(b) => b.clone(),
            None => return,
        };
        self.on_bwu_negotiation_frame(client, &bwu, endpoint_id);
    }

    fn on_bwu_negotiation_frame(
        &mut self,
        client: &mut ClientProxy,
        frame: &pb::BandwidthUpgradeNegotiationFrame,
        endpoint_id: &str,
    ) {
        let event_type = frame
            .event_type
            .and_then(|v| EventType::try_from(v).ok())
            .unwrap_or(EventType::UnknownEventType);

        // Connection gate: drop BWU frames before the PCP connection is accepted.
        if !client.is_connected_to_endpoint(endpoint_id) {
            if event_type == EventType::UpgradePathAvailable {
                if let Some(info) = &frame.upgrade_path_info {
                    self.run_upgrade_failed_protocol(client, endpoint_id, info);
                }
            }
            return;
        }

        match event_type {
            EventType::UpgradePathAvailable => {
                if let Some(info) = frame.upgrade_path_info.clone() {
                    self.process_bwu_path_available_event(client, endpoint_id, &info);
                }
            }
            EventType::UpgradeFailure => {
                if let Some(info) = &frame.upgrade_path_info {
                    self.process_upgrade_failure_event(client, endpoint_id, info);
                }
            }
            EventType::LastWriteToPriorChannel => {
                if !self.in_progress_upgrades.contains(endpoint_id) {
                    return;
                }
                self.process_last_write_to_prior_channel_event(client, endpoint_id);
            }
            EventType::SafeToClosePriorChannel => {
                if !self.in_progress_upgrades.contains(endpoint_id) {
                    return;
                }
                self.process_safe_to_close_prior_channel_event(client, endpoint_id);
            }
            // UPGRADE_PATH_REQUEST (dynamic role switch — omitted),
            // CLIENT_INTRODUCTION(_ACK) (consumed inline, never dispatched), and
            // UNKNOWN: ignore.
            _ => {}
        }
    }

    // -- responder path -----------------------------------------------------

    fn process_bwu_path_available_event(
        &mut self,
        client: &mut ClientProxy,
        endpoint_id: &str,
        upgrade_path_info: &UpgradePathInfo,
    ) {
        let upgrade_medium = upi_medium(upgrade_path_info);

        // WIFI_LAN protection: don't tear down an active WIFI_LAN for a hotspot.
        if self.ecm.borrow().is_wifi_lan_connected() && upgrade_medium == Medium::WifiHotspot {
            self.run_upgrade_failed_protocol(client, endpoint_id, upgrade_path_info);
            return;
        }

        // The advertiser (incoming connection) does not act as BWU responder.
        // (Dynamic role switch omitted, so this is unconditional.)
        if client.is_incoming_connection(endpoint_id) {
            return;
        }

        // Duplicate / out-of-sync guard: close everything.
        if self.in_progress_upgrades.contains(endpoint_id) {
            if let Some(prev) = self.previous_endpoint_channels.remove(endpoint_id) {
                prev.close_with_reason(DisconnectionReason::Unfinished);
            }
            if let Some(cur) = self.ecm.borrow().get_channel_for_endpoint(endpoint_id) {
                cur.resume();
                cur.close_with_reason(DisconnectionReason::Unfinished);
            }
            return;
        }

        // Responder adopts the medium the initiator chose.
        if self.get_bwu_medium_for_endpoint(endpoint_id) == Medium::UnknownMedium {
            self.set_bwu_medium_for_endpoint(endpoint_id, upgrade_medium);
        }
        if upgrade_medium != self.get_bwu_medium_for_endpoint(endpoint_id) {
            self.run_upgrade_failed_protocol(client, endpoint_id, upgrade_path_info);
            return;
        }

        let channel =
            match self.process_bwu_path_available_event_internal(client, endpoint_id, upgrade_path_info) {
                Some(c) => c,
                None => {
                    self.run_upgrade_failed_protocol(client, endpoint_id, upgrade_path_info);
                    return;
                }
            };

        self.in_progress_upgrades.insert(endpoint_id.to_string());
        let enable_encryption = !upgrade_path_info.supports_disabling_encryption.unwrap_or(false);
        self.run_upgrade_protocol(client, endpoint_id, channel, enable_encryption);
    }

    fn process_bwu_path_available_event_internal(
        &mut self,
        client: &mut ClientProxy,
        endpoint_id: &str,
        upgrade_path_info: &UpgradePathInfo,
    ) -> Option<Arc<dyn EndpointChannel>> {
        let medium = upi_medium(upgrade_path_info);
        if medium != self.get_bwu_medium_for_endpoint(endpoint_id) {
            return None;
        }
        if !self.has_handler_for_medium(medium) {
            return None;
        }
        let (service_id, last_local) = {
            let ecm = self.ecm.borrow();
            let old_channel = ecm.get_channel_for_endpoint(endpoint_id)?;
            (old_channel.service_id(), old_channel.local_endpoint_id())
        };
        let last_local = if last_local.is_empty() {
            client.last_local_endpoint_id().to_string()
        } else {
            last_local
        };

        let new_channel = self.handlers.get_mut(&medium).unwrap().create_upgraded_endpoint_channel(
            client,
            &service_id,
            endpoint_id,
            upgrade_path_info,
        )?;

        let intro = for_bwu_introduction(
            client.local_endpoint_id(),
            &last_local,
            upgrade_path_info.supports_disabling_encryption.unwrap_or(false),
        );
        if !new_channel.write(&intro).ok() {
            new_channel.close();
            return None;
        }
        if upgrade_path_info.supports_client_introduction_ack.unwrap_or(false)
            && !read_client_introduction_ack_frame(&new_channel)
        {
            new_channel.close();
            return None;
        }
        Some(new_channel)
    }

    // -- initiator incoming-connection path ---------------------------------

    /// Test hook: inject a ready upgraded channel (the medium-layer
    /// incoming-connection callback the real handlers fire).
    pub fn invoke_on_incoming_connection_for_testing(
        &mut self,
        client: &mut ClientProxy,
        connection: IncomingSocketConnection,
    ) {
        self.on_incoming_connection(client, connection);
    }

    fn on_incoming_connection(
        &mut self,
        client: &mut ClientProxy,
        connection: IncomingSocketConnection,
    ) {
        let channel = connection.channel;
        let introduction = match read_client_introduction_frame(&channel) {
            Some(i) => i,
            None => {
                channel.close();
                return;
            }
        };
        if !write_client_introduction_ack_frame(&channel) {
            channel.close();
            return;
        }
        let endpoint_id = introduction.endpoint_id.clone().unwrap_or_default();
        // (Dynamic role-switch last_endpoint_id aliasing omitted.)
        if !self.in_progress_upgrades.contains(&endpoint_id) {
            return;
        }
        // The upgrade connected, so any pending retry for it is moot (bwu_manager.cc:686).
        self.cancel_retry_upgrade_alarm(&endpoint_id);
        let enable_encryption = !introduction.supports_disabling_encryption.unwrap_or(false);
        self.run_upgrade_protocol(client, &endpoint_id, channel, enable_encryption);
    }

    // -- the converged upgrade protocol -------------------------------------

    fn run_upgrade_protocol(
        &mut self,
        client: &mut ClientProxy,
        endpoint_id: &str,
        new_channel: Arc<dyn EndpointChannel>,
        enable_encryption: bool,
    ) {
        new_channel.set_local_endpoint_id(client.local_endpoint_id());
        // Pause the new channel FIRST: the shared UKEY2 context is sequence
        // numbered, so we must not write payloads on the new channel until the
        // old one is fully drained.
        new_channel.pause();

        let old_channel = match self.ecm.borrow().get_channel_for_endpoint(endpoint_id) {
            Some(c) => c,
            None => return,
        };

        self.ecm.borrow_mut().replace_channel_for_endpoint(
            client,
            endpoint_id,
            new_channel,
            enable_encryption,
        );

        if !old_channel.write(&for_bwu_last_write()).ok() {
            // LAST_WRITE write failed; old channel not parked.
            return;
        }
        self.previous_endpoint_channels
            .insert(endpoint_id.to_string(), old_channel);

        // Early-LAST_WRITE race: the remote's LAST_WRITE may have arrived before
        // we parked the old channel; if so, run the deferred event now.
        if self.successfully_upgraded_endpoints.remove(endpoint_id) {
            self.process_last_write_to_prior_channel_event(client, endpoint_id);
        }
    }

    fn process_last_write_to_prior_channel_event(
        &mut self,
        _client: &mut ClientProxy,
        endpoint_id: &str,
    ) {
        let prev = match self.previous_endpoint_channels.get(endpoint_id) {
            Some(c) => c.clone(),
            None => {
                // LAST_WRITE arrived before RunUpgradeProtocol parked the old
                // channel — latch it.
                self.successfully_upgraded_endpoints
                    .insert(endpoint_id.to_string());
                return;
            }
        };
        if !prev.write(&for_bwu_safe_to_close()).ok() {
            prev.close_with_reason(DisconnectionReason::IoError);
            self.previous_endpoint_channels.remove(endpoint_id);
        }
        // Success: keep the old channel parked until the remote's SAFE_TO_CLOSE.
    }

    fn process_safe_to_close_prior_channel_event(
        &mut self,
        client: &mut ClientProxy,
        endpoint_id: &str,
    ) {
        let prev = match self.previous_endpoint_channels.remove(endpoint_id) {
            Some(c) => c,
            None => return,
        };
        // Send a plaintext DISCONNECTION so the shared crypto counter isn't
        // incremented, then a best-effort drain Read (b/172380349) so the peer
        // can receive our SAFE_TO_CLOSE, then close.
        prev.disable_encryption();
        let _ = prev.write(&for_disconnection(false, false));
        let _ = prev.read();
        prev.close_with_reason(DisconnectionReason::Upgraded);

        let channel = match self.ecm.borrow().get_channel_for_endpoint(endpoint_id) {
            Some(c) => c,
            None => return, // NB: in_progress_upgrades is intentionally NOT erased here.
        };
        channel.resume();
        let medium = channel.medium();
        client.on_bandwidth_changed(endpoint_id, medium);
        self.in_progress_upgrades.remove(endpoint_id);
    }

    // -- failure (minimal; full machinery TODO) -----------------------------

    /// Responder couldn't join the medium the initiator set up: tell the remote
    /// (so it can pick another) and clean up our medium.
    fn run_upgrade_failed_protocol(
        &mut self,
        _client: &mut ClientProxy,
        endpoint_id: &str,
        upgrade_path_info: &UpgradePathInfo,
    ) {
        let channel = match self.ecm.borrow().get_channel_for_endpoint(endpoint_id) {
            Some(c) => c,
            None => return,
        };
        if !channel.write(&for_bwu_failure(upgrade_path_info.clone())).ok() {
            channel.close_with_reason(DisconnectionReason::IoError);
            return;
        }
        if self.get_bwu_medium_for_endpoint(endpoint_id) != Medium::UnknownMedium {
            self.revert_bwu_medium_for_endpoint(&channel.service_id(), endpoint_id);
        }
        self.in_progress_upgrades.remove(endpoint_id);
    }

    /// The remote failed to upgrade to the medium we set up. Revert our current
    /// upgrade medium and try the next-best untried medium (retrying after a
    /// delay if none remain). Mirrors `ProcessUpgradeFailureEvent`.
    fn process_upgrade_failure_event(
        &mut self,
        client: &mut ClientProxy,
        endpoint_id: &str,
        upgrade_path_info: &UpgradePathInfo,
    ) {
        self.in_progress_upgrades.remove(endpoint_id);

        // With the single global medium (flag disabled), we can only switch
        // mediums if there's at most one connected endpoint.
        if !self.config.support_multiple_bwu_mediums
            && self.ecm.borrow().get_connected_endpoints_count() > 1
        {
            return;
        }

        let service_id = self
            .ecm
            .borrow()
            .get_channel_for_endpoint(endpoint_id)
            .map(|c| c.service_id())
            .unwrap_or_else(|| UNKNOWN_SERVICE_ID.to_string());
        if self.get_bwu_medium_for_endpoint(endpoint_id) != Medium::UnknownMedium {
            // Initiator side: wrap so the revert reaches the platform layer.
            let upgrade_service_id = wrap_initiator_upgrade_service_id(&service_id);
            self.revert_bwu_medium_for_endpoint(&upgrade_service_id, endpoint_id);
        }

        // Drop everything up to and including the medium we last attempted,
        // leaving only the untried tail of the preference-ordered list, then
        // try the next-best of those (bwu_manager.cc:1451-1465).
        let last = upi_medium(upgrade_path_info);
        let all_possible = client.get_upgrade_mediums(endpoint_id);
        let mut untried = all_possible.clone();
        for medium in &all_possible {
            untried.remove(0);
            if *medium == last {
                break;
            }
        }
        self.try_next_best_upgrade_mediums(client, endpoint_id, untried);
    }

    // -- retry machinery ----------------------------------------------------

    /// Picks the best medium from `upgrade_mediums` and either re-initiates the
    /// upgrade on it or, if none is viable, schedules a delayed retry. Mirrors
    /// `TryNextBestUpgradeMediums`.
    fn try_next_best_upgrade_mediums(
        &mut self,
        client: &mut ClientProxy,
        endpoint_id: &str,
        upgrade_mediums: Vec<Medium>,
    ) {
        let next_medium = self.choose_best_upgrade_medium(endpoint_id, &upgrade_mediums);

        let current_medium = self
            .ecm
            .borrow()
            .get_channel_for_endpoint(endpoint_id)
            .map(|c| c.medium())
            .unwrap_or(Medium::UnknownMedium);

        // If we're not already on WIFI_LAN and there's no new medium to try
        // (the best pick is the current one, or unknown, or the list is empty),
        // retry the same upgrade after a delay instead of giving up.
        // (Google TODO b/228610864 questions treating WIFI_LAN differently.)
        if current_medium != Medium::WifiLan
            && (next_medium == current_medium
                || next_medium == Medium::UnknownMedium
                || upgrade_mediums.is_empty())
        {
            self.retry_upgrades_after_delay(endpoint_id);
            return;
        }

        // A medium with no handler was stripped out already, so this shouldn't
        // be hit; bail rather than initiate with no handler.
        if !self.has_handler_for_medium(next_medium) {
            return;
        }
        self.set_bwu_medium_for_endpoint(endpoint_id, next_medium);
        self.initiate_bwu_for_endpoint(client, endpoint_id, next_medium);
    }

    /// `ChooseBestUpgradeMedium` — from the remote's preference-ordered mediums,
    /// keep the ones we can actually use, then either keep the current upgrade
    /// medium (if still supported) or pick the most-preferred available one.
    fn choose_best_upgrade_medium(&self, endpoint_id: &str, mediums: &[Medium]) -> Medium {
        let available = self.strip_out_unavailable_mediums(mediums);
        let current = self.get_bwu_medium_for_endpoint(endpoint_id);
        if current == Medium::UnknownMedium {
            // First upgrade attempt: take the most-preferred available medium.
            if let Some(&first) = available.first() {
                return first;
            }
        } else if available.contains(&current) {
            // Already upgraded and the current medium is still supported.
            return current;
        }
        // No first-time medium available, or the current one is no longer
        // supported and we can't switch: give up on a concrete medium.
        Medium::UnknownMedium
    }

    /// `StripOutUnavailableMediums` — in the port, a medium is "available" iff a
    /// handler is registered for it (the platform radio-availability layer
    /// `Mediums`/`IsAvailable`/`IsAPAvailable` is omitted; the consumer decides
    /// which handlers exist). Preserves the input (preference) order.
    fn strip_out_unavailable_mediums(&self, mediums: &[Medium]) -> Vec<Medium> {
        mediums
            .iter()
            .copied()
            .filter(|m| self.has_handler_for_medium(*m))
            .collect()
    }

    /// Schedules a delayed retry of the upgrade for `endpoint_id`. In the
    /// synchronous port this records the pending alarm and its computed delay;
    /// the integration layer (Tokio actor) arms a real timer from
    /// [`Self::pending_retry_delay`] and calls [`Self::fire_retry_alarm`] when it
    /// elapses. Mirrors `RetryUpgradesAfterDelay`.
    fn retry_upgrades_after_delay(&mut self, endpoint_id: &str) {
        let delay = self.calculate_next_retry_delay(endpoint_id);
        self.cancel_retry_upgrade_alarm(endpoint_id);
        self.retry_upgrade_alarms
            .insert(endpoint_id.to_string(), delay);
        self.retry_delays.insert(endpoint_id.to_string(), delay);
    }

    /// `CalculateNextRetryDelay` — the first retry uses the initial delay; each
    /// subsequent retry doubles (exp backoff) or adds the initial delay
    /// (linear), capped at the configured maximum.
    pub fn calculate_next_retry_delay(&self, endpoint_id: &str) -> Duration {
        let initial_delay = self.config.bandwidth_upgrade_retry_delay;
        let Some(&last) = self.retry_delays.get(endpoint_id) else {
            return initial_delay;
        };
        let delay = if self.config.use_exp_backoff_in_bwu_retry {
            last * 2
        } else {
            last + initial_delay
        };
        delay.min(self.config.bandwidth_upgrade_retry_max_delay)
    }

    /// The delay of the retry currently scheduled for `endpoint_id`, if any.
    /// The integration layer uses this to arm a timer; `None` means no retry is
    /// pending (never scheduled, already fired, or cancelled).
    pub fn pending_retry_delay(&self, endpoint_id: &str) -> Option<Duration> {
        self.retry_upgrade_alarms.get(endpoint_id).copied()
    }

    /// Fires the scheduled retry for `endpoint_id` (the integration layer calls
    /// this when the armed timer elapses). No-op if the alarm was cancelled or
    /// already fired, or if the endpoint is no longer connected. Mirrors the
    /// `RetryUpgradesAfterDelay` alarm callback.
    pub fn fire_retry_alarm(&mut self, client: &mut ClientProxy, endpoint_id: &str) {
        // Consume the pending alarm; absence means it was cancelled (a real
        // `CancelableAlarm` would never invoke its callback after `Cancel()`).
        if self.retry_upgrade_alarms.remove(endpoint_id).is_none() {
            return;
        }
        if !client.is_connected_to_endpoint(endpoint_id) {
            return;
        }
        let mediums = client.get_upgrade_mediums(endpoint_id);
        self.try_next_best_upgrade_mediums(client, endpoint_id, mediums);
    }

    /// `CancelRetryUpgradeAlarm` — cancels a pending retry. Note this does NOT
    /// clear `retry_delays`, so the backoff resumes from the last delay on the
    /// next schedule.
    fn cancel_retry_upgrade_alarm(&mut self, endpoint_id: &str) {
        self.retry_upgrade_alarms.remove(endpoint_id);
    }

    /// `CancelAllRetryUpgradeAlarms` — cancels every pending retry and resets all
    /// backoff state.
    fn cancel_all_retry_upgrade_alarms(&mut self) {
        self.retry_upgrade_alarms.clear();
        self.retry_delays.clear();
    }

    // -- endpoint disconnect / revert ---------------------------------------

    /// Cleans up an upgrade after the endpoint disconnects (mirrors the C++
    /// `OnEndpointDisconnect` serial-thread lambda). The `CountDownLatch` barrier
    /// is unnecessary in the synchronous port.
    pub fn on_endpoint_disconnect(
        &mut self,
        client: &mut ClientProxy,
        service_id: &str,
        endpoint_id: &str,
        _reason: DisconnectionReason,
    ) {
        let medium = self.get_bwu_medium_for_endpoint(endpoint_id);
        if self.has_handler_for_medium(medium) {
            self.handlers
                .get_mut(&medium)
                .unwrap()
                .on_endpoint_disconnect(client, endpoint_id);
        }
        if let Some(old_channel) = self.previous_endpoint_channels.remove(endpoint_id) {
            old_channel.close_with_reason(DisconnectionReason::Shutdown);
        }
        self.in_progress_upgrades.remove(endpoint_id);
        self.retry_delays.remove(endpoint_id);
        self.cancel_retry_upgrade_alarm(endpoint_id);
        self.successfully_upgraded_endpoints.remove(endpoint_id);

        if self.config.support_multiple_bwu_mediums
            || self.ecm.borrow().get_connected_endpoints_count() <= 1
        {
            self.revert_bwu_medium_for_endpoint(service_id, endpoint_id);
        }
    }

    fn revert_bwu_medium_for_endpoint(&mut self, service_id: &str, endpoint_id: &str) {
        let medium = self.get_bwu_medium_for_endpoint(endpoint_id);

        if !self.config.support_multiple_bwu_mediums {
            // Coarse: reset the single global medium and revert ALL services.
            self.medium = Medium::UnknownMedium;
            if self.has_handler_for_medium(medium) {
                self.handlers.get_mut(&medium).unwrap().revert_initiator_state();
            }
            return;
        }

        // Fine-grained, per-endpoint.
        self.endpoint_id_to_bwu_medium.remove(endpoint_id);
        if !self.has_handler_for_medium(medium) {
            return;
        }
        if !is_initiator_upgrade_service_id(service_id) {
            // Responder: only Hotspot/WifiDirect need to disconnect from the AP
            // to restore the prior connection.
            if medium == Medium::WifiHotspot || medium == Medium::WifiDirect {
                self.handlers
                    .get_mut(&medium)
                    .unwrap()
                    .revert_responder_state(service_id);
            }
            return;
        }
        // Initiator.
        self.handlers
            .get_mut(&medium)
            .unwrap()
            .revert_initiator_state_for_endpoint(service_id, endpoint_id);
    }

    pub fn shutdown(&mut self) {
        self.cancel_all_retry_upgrade_alarms();
        for handler in self.handlers.values_mut() {
            handler.revert_initiator_state();
        }
        self.in_progress_upgrades.clear();
        self.previous_endpoint_channels.clear();
        self.successfully_upgraded_endpoints.clear();
    }
}

// -- free helpers -----------------------------------------------------------

fn upi_medium(upgrade_path_info: &UpgradePathInfo) -> Medium {
    let m = upgrade_path_info
        .medium
        .and_then(|v| {
            pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium::try_from(v).ok()
        })
        .unwrap_or(pb::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium::UnknownMedium);
    upgrade_path_info_medium_to_medium(m)
}

fn upgrade_path_info_for_medium(medium: Medium) -> UpgradePathInfo {
    UpgradePathInfo {
        medium: Some(medium_to_upgrade_path_info_medium(medium) as i32),
        ..Default::default()
    }
}

fn read_client_introduction_frame(
    channel: &Arc<dyn EndpointChannel>,
) -> Option<pb::bandwidth_upgrade_negotiation_frame::ClientIntroduction> {
    // TODO(port): 5s read timeout (the C++ CancelableAlarm closes the channel).
    let data = channel.read().ok()?;
    let frame = from_bytes(&data).ok()?;
    let bwu = frame
        .v1
        .and_then(|v1| v1.bandwidth_upgrade_negotiation)?;
    if bwu.event_type != Some(EventType::ClientIntroduction as i32) {
        return None;
    }
    bwu.client_introduction
}

fn read_client_introduction_ack_frame(channel: &Arc<dyn EndpointChannel>) -> bool {
    let Ok(data) = channel.read() else {
        return false;
    };
    let Ok(frame) = from_bytes(&data) else {
        return false;
    };
    matches!(
        frame
            .v1
            .and_then(|v1| v1.bandwidth_upgrade_negotiation)
            .and_then(|bwu| bwu.event_type),
        Some(v) if v == EventType::ClientIntroductionAck as i32
    )
}

fn write_client_introduction_ack_frame(channel: &Arc<dyn EndpointChannel>) -> bool {
    channel.write(&for_bwu_introduction_ack()).ok()
}
