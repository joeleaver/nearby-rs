//! A minimal `EndpointChannelManager` — the endpoint→channel registry the BWU
//! state machine swaps over.
//!
//! Per the porting spec the BWU test surface needs only: `get_channel_for_endpoint`
//! (a shared `Arc` clone), `replace_channel_for_endpoint` (the upgrade swap),
//! `get_connected_endpoints_count` (asserted 2→1→0), and `is_wifi_lan_connected`
//! (blocks WIFI_LAN→HOTSPOT/DIRECT). `register`/`unregister` are driven by the
//! test harness. Channels are held as `Arc<dyn EndpointChannel>` so a clone held
//! externally (e.g. the old channel stashed by `BwuManager`) survives the swap
//! and still observes `close`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::bwu::channel::{DisconnectionReason, EndpointChannel, SafeDisconnectionResult};
use crate::bwu::client::ClientProxy;
use crate::mediums::Medium;

#[derive(Default)]
pub struct EndpointChannelManager {
    endpoints: HashMap<String, Arc<dyn EndpointChannel>>,
}

impl EndpointChannelManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_channel_for_endpoint(
        &mut self,
        _client: &ClientProxy,
        endpoint_id: &str,
        channel: Arc<dyn EndpointChannel>,
    ) {
        self.endpoints.insert(endpoint_id.to_string(), channel);
    }

    /// Swaps the active channel for an existing endpoint to the upgraded one.
    pub fn replace_channel_for_endpoint(
        &mut self,
        _client: &ClientProxy,
        endpoint_id: &str,
        channel: Arc<dyn EndpointChannel>,
        _enable_encryption: bool,
    ) {
        self.endpoints.insert(endpoint_id.to_string(), channel);
    }

    pub fn get_channel_for_endpoint(&self, endpoint_id: &str) -> Option<Arc<dyn EndpointChannel>> {
        self.endpoints.get(endpoint_id).cloned()
    }

    /// Removes the endpoint's channel registration. Returns whether it existed.
    /// (The BWU state machine, not unregister, closes the displaced channel.)
    pub fn unregister_channel_for_endpoint(
        &mut self,
        endpoint_id: &str,
        _reason: DisconnectionReason,
        _result: SafeDisconnectionResult,
    ) -> bool {
        self.endpoints.remove(endpoint_id).is_some()
    }

    pub fn get_connected_endpoints_count(&self) -> usize {
        self.endpoints.len()
    }

    pub fn is_wifi_lan_connected(&self) -> bool {
        self.endpoints
            .values()
            .any(|c| c.medium() == Medium::WifiLan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bwu::testing::FakeEndpointChannel;

    #[test]
    fn register_replace_count_and_wifi_lan() {
        let client = ClientProxy::default();
        let mut ecm = EndpointChannelManager::new();
        assert_eq!(ecm.get_connected_endpoints_count(), 0);
        assert!(!ecm.is_wifi_lan_connected());

        let bt: Arc<dyn EndpointChannel> = Arc::new(FakeEndpointChannel::new(Medium::Bluetooth, "A"));
        ecm.register_channel_for_endpoint(&client, "Endpoint1", bt.clone());
        assert_eq!(ecm.get_connected_endpoints_count(), 1);
        assert!(!ecm.is_wifi_lan_connected());

        // A stashed clone of the original channel survives the swap.
        let stashed = ecm.get_channel_for_endpoint("Endpoint1").unwrap();
        let lan: Arc<dyn EndpointChannel> = Arc::new(FakeEndpointChannel::new(Medium::WifiLan, "A"));
        ecm.replace_channel_for_endpoint(&client, "Endpoint1", lan, false);
        assert_eq!(ecm.get_connected_endpoints_count(), 1); // swap, not a new endpoint
        assert!(ecm.is_wifi_lan_connected());
        assert_eq!(stashed.medium(), Medium::Bluetooth); // the clone is unaffected

        assert!(ecm.unregister_channel_for_endpoint(
            "Endpoint1",
            DisconnectionReason::LocalDisconnection,
            SafeDisconnectionResult::SafeDisconnection
        ));
        assert_eq!(ecm.get_connected_endpoints_count(), 0);
    }
}
