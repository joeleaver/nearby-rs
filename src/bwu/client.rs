//! A minimal `ClientProxy` — the per-endpoint connection state the BWU state
//! machine queries.
//!
//! The real Google `ClientProxy` is a large class (PCP state, encryption,
//! advertising/discovery, analytics). Per the porting spec, the BWU test surface
//! only needs: the connection lifecycle (initiated → accepted/connected), the
//! advertiser gate (`is_incoming_connection`), the local/last-local endpoint
//! ids, and the `on_bandwidth_changed` success callback. The dynamic-role-switch
//! getters (OS type, medium role, remote OS info) are intentionally omitted.

use std::collections::HashMap;

use crate::mediums::Medium;

#[derive(Debug, Default, Clone)]
struct EndpointState {
    /// `OnConnectionInitiated` has been seen.
    initiated: bool,
    /// `OnConnectionAccepted` has been seen → `IsConnectedToEndpoint`.
    connected: bool,
    local_accepted: bool,
    remote_accepted: bool,
    /// True if this side accepted an INCOMING connection (the advertiser/
    /// responder role); gates out an inbound `UPGRADE_PATH_AVAILABLE`.
    is_incoming: bool,
    auto_upgrade_bandwidth: bool,
}

/// Per-client connection bookkeeping queried by `BwuManager`.
#[derive(Debug, Clone)]
pub struct ClientProxy {
    client_id: i64,
    local_endpoint_id: String,
    last_local_endpoint_id: String,
    endpoints: HashMap<String, EndpointState>,
    /// Records `(endpoint_id, medium)` from `on_bandwidth_changed` (the success
    /// callback BwuManager fires after a completed upgrade).
    bandwidth_changed: Vec<(String, Medium)>,
}

impl Default for ClientProxy {
    fn default() -> Self {
        Self::new(0, "LOCAL")
    }
}

impl ClientProxy {
    pub fn new(client_id: i64, local_endpoint_id: &str) -> Self {
        Self {
            client_id,
            local_endpoint_id: local_endpoint_id.to_string(),
            last_local_endpoint_id: local_endpoint_id.to_string(),
            endpoints: HashMap::new(),
            bandwidth_changed: Vec::new(),
        }
    }

    pub fn client_id(&self) -> i64 {
        self.client_id
    }

    pub fn local_endpoint_id(&self) -> &str {
        &self.local_endpoint_id
    }

    pub fn last_local_endpoint_id(&self) -> &str {
        &self.last_local_endpoint_id
    }

    fn entry(&mut self, endpoint_id: &str) -> &mut EndpointState {
        self.endpoints.entry(endpoint_id.to_string()).or_default()
    }

    /// `OnConnectionInitiated`: the connection is pending (not yet accepted).
    /// `is_incoming` marks the advertiser/responder role.
    pub fn on_connection_initiated(
        &mut self,
        endpoint_id: &str,
        is_incoming: bool,
        auto_upgrade_bandwidth: bool,
    ) {
        let e = self.entry(endpoint_id);
        e.initiated = true;
        e.is_incoming = is_incoming;
        e.auto_upgrade_bandwidth = auto_upgrade_bandwidth;
    }

    pub fn local_endpoint_accepted_connection(&mut self, endpoint_id: &str) {
        self.entry(endpoint_id).local_accepted = true;
    }

    pub fn remote_endpoint_accepted_connection(&mut self, endpoint_id: &str) {
        self.entry(endpoint_id).remote_accepted = true;
    }

    /// `OnConnectionAccepted`: the connection is established → connected.
    pub fn on_connection_accepted(&mut self, endpoint_id: &str) {
        self.entry(endpoint_id).connected = true;
    }

    /// True once both local and remote have accepted.
    pub fn is_connection_accepted(&self, endpoint_id: &str) -> bool {
        self.endpoints
            .get(endpoint_id)
            .map(|e| e.local_accepted && e.remote_accepted)
            .unwrap_or(false)
    }

    /// True once `OnConnectionAccepted` has been seen. BWU frames are dropped
    /// for endpoints that are not connected.
    pub fn is_connected_to_endpoint(&self, endpoint_id: &str) -> bool {
        self.endpoints
            .get(endpoint_id)
            .map(|e| e.connected)
            .unwrap_or(false)
    }

    pub fn is_incoming_connection(&self, endpoint_id: &str) -> bool {
        self.endpoints
            .get(endpoint_id)
            .map(|e| e.is_incoming)
            .unwrap_or(false)
    }

    pub fn auto_upgrade_bandwidth(&self, endpoint_id: &str) -> bool {
        self.endpoints
            .get(endpoint_id)
            .map(|e| e.auto_upgrade_bandwidth)
            .unwrap_or(false)
    }

    /// `OnBandwidthChanged` — the success callback fired after an upgrade.
    pub fn on_bandwidth_changed(&mut self, endpoint_id: &str, medium: Medium) {
        self.bandwidth_changed
            .push((endpoint_id.to_string(), medium));
    }

    /// Test/inspection accessor for the recorded `on_bandwidth_changed` events.
    pub fn bandwidth_changed_events(&self) -> &[(String, Medium)] {
        &self.bandwidth_changed
    }
}
