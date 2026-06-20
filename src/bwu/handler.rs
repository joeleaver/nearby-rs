//! The bandwidth-upgrade handler seam.
//!
//! Port of `connections/implementation/bwu_handler.h` + `base_bwu_handler.{h,cc}`.
//! C++ models this as `BwuHandler` (the interface `BwuManager` calls) with a
//! `BaseBwuHandler` subclass that adds the per-(wrapped-service → endpoint-ids)
//! bookkeeping and forwards medium-specific work to pure-virtual `Handle*`
//! methods. Rust has no inheritance, so the split is:
//!
//! - [`MediumBwuHandler`] — the medium-specific operations a concrete handler
//!   implements (the C++ `Handle*` virtuals + the leaf `BwuHandler` methods that
//!   are not bookkeeping).
//! - [`BaseBwuHandler`] — wraps a `MediumBwuHandler`, owns the bookkeeping, and
//!   implements the full [`BwuHandler`] interface.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::bwu::channel::EndpointChannel;
use crate::bwu::client::ClientProxy;
use crate::bwu::service_id::{is_initiator_upgrade_service_id, wrap_initiator_upgrade_service_id};
use crate::mediums::Medium;
use crate::proto as pb;

type UpgradePathInfo = pb::bandwidth_upgrade_negotiation_frame::UpgradePathInfo;

/// `BwuHandler::IncomingSocketConnection`. The C++ struct also carries an
/// `IncomingSocket` (only `ToString`/`Close`, unused by the state machine for
/// the test surface), which is omitted here.
pub struct IncomingSocketConnection {
    pub channel: Arc<dyn EndpointChannel>,
}

/// The full handler interface `BwuManager` uses (the C++ `BwuHandler`).
///
/// `Send` so the manager (and the Tokio actor wrapping it) can be moved to a
/// dedicated runtime thread.
pub trait BwuHandler: Send {
    /// Initiator: set up the upgraded medium and return the serialized
    /// `UPGRADE_PATH_AVAILABLE` bytes. An EMPTY return signals failure
    /// (the `MEDIUM_ERROR` path in `BwuManager`).
    fn initialize_upgraded_medium_for_endpoint(
        &mut self,
        client: &ClientProxy,
        service_id: &str,
        endpoint_id: &str,
    ) -> Vec<u8>;

    /// Revert all initiator state for every service (flag-disabled path /
    /// shutdown).
    fn revert_initiator_state(&mut self);

    /// Revert initiator state for one endpoint; the medium handler is alerted
    /// only after the last endpoint of the (wrapped) service is reverted.
    fn revert_initiator_state_for_endpoint(&mut self, upgrade_service_id: &str, endpoint_id: &str);

    /// Responder: revert handler state for a (raw) service id.
    fn revert_responder_state(&mut self, service_id: &str);

    /// Responder: create the upgraded `EndpointChannel` from the initiator's
    /// `UpgradePathInfo`. `None` signals failure.
    fn create_upgraded_endpoint_channel(
        &mut self,
        client: &ClientProxy,
        service_id: &str,
        endpoint_id: &str,
        upgrade_path_info: &UpgradePathInfo,
    ) -> Option<Arc<dyn EndpointChannel>>;

    fn get_upgrade_medium(&self) -> Medium;

    fn on_endpoint_disconnect(&mut self, client: &ClientProxy, endpoint_id: &str);
}

/// The medium-specific operations a concrete handler implements (the C++
/// `Handle*` virtuals plus the non-bookkeeping leaf methods).
///
/// `Send` so a [`BaseBwuHandler`] wrapping it satisfies [`BwuHandler`]'s `Send`.
pub trait MediumBwuHandler: Send {
    fn handle_initialize_upgraded_medium_for_endpoint(
        &mut self,
        client: &ClientProxy,
        upgrade_service_id: &str,
        endpoint_id: &str,
    ) -> Vec<u8>;

    fn handle_revert_initiator_state_for_service(&mut self, upgrade_service_id: &str);

    fn create_upgraded_endpoint_channel(
        &mut self,
        client: &ClientProxy,
        service_id: &str,
        endpoint_id: &str,
        upgrade_path_info: &UpgradePathInfo,
    ) -> Option<Arc<dyn EndpointChannel>>;

    fn get_upgrade_medium(&self) -> Medium;

    fn on_endpoint_disconnect(&mut self, client: &ClientProxy, endpoint_id: &str);
}

/// Common bookkeeping for all medium handlers (port of `BaseBwuHandler`):
/// tracks, per WRAPPED upgrade-service-id, the set of endpoint ids that
/// initiated an upgrade.
pub struct BaseBwuHandler<H: MediumBwuHandler> {
    inner: H,
    upgrade_service_id_to_active_endpoint_ids: HashMap<String, HashSet<String>>,
}

impl<H: MediumBwuHandler> BaseBwuHandler<H> {
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            upgrade_service_id_to_active_endpoint_ids: HashMap::new(),
        }
    }

    pub fn inner(&self) -> &H {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut H {
        &mut self.inner
    }
}

impl<H: MediumBwuHandler> BwuHandler for BaseBwuHandler<H> {
    fn initialize_upgraded_medium_for_endpoint(
        &mut self,
        client: &ClientProxy,
        service_id: &str,
        endpoint_id: &str,
    ) -> Vec<u8> {
        let upgrade_service_id = wrap_initiator_upgrade_service_id(service_id);
        let frame = self.inner.handle_initialize_upgraded_medium_for_endpoint(
            client,
            &upgrade_service_id,
            endpoint_id,
        );
        if !frame.is_empty() {
            self.upgrade_service_id_to_active_endpoint_ids
                .entry(upgrade_service_id)
                .or_default()
                .insert(endpoint_id.to_string());
        }
        frame
    }

    fn revert_initiator_state(&mut self) {
        let service_ids: Vec<String> = self
            .upgrade_service_id_to_active_endpoint_ids
            .keys()
            .cloned()
            .collect();
        for service_id in service_ids {
            self.inner
                .handle_revert_initiator_state_for_service(&service_id);
        }
        self.upgrade_service_id_to_active_endpoint_ids.clear();
    }

    fn revert_initiator_state_for_endpoint(&mut self, upgrade_service_id: &str, endpoint_id: &str) {
        if !is_initiator_upgrade_service_id(upgrade_service_id) {
            // Not a BWU initiator id; ignore.
            return;
        }
        let now_empty = match self
            .upgrade_service_id_to_active_endpoint_ids
            .get_mut(upgrade_service_id)
        {
            Some(set) if !set.is_empty() => {
                set.remove(endpoint_id);
                set.is_empty()
            }
            _ => return,
        };
        if now_empty {
            self.upgrade_service_id_to_active_endpoint_ids
                .remove(upgrade_service_id);
            self.inner
                .handle_revert_initiator_state_for_service(upgrade_service_id);
        }
    }

    fn revert_responder_state(&mut self, service_id: &str) {
        self.inner
            .handle_revert_initiator_state_for_service(service_id);
    }

    fn create_upgraded_endpoint_channel(
        &mut self,
        client: &ClientProxy,
        service_id: &str,
        endpoint_id: &str,
        upgrade_path_info: &UpgradePathInfo,
    ) -> Option<Arc<dyn EndpointChannel>> {
        self.inner.create_upgraded_endpoint_channel(
            client,
            service_id,
            endpoint_id,
            upgrade_path_info,
        )
    }

    fn get_upgrade_medium(&self) -> Medium {
        self.inner.get_upgrade_medium()
    }

    fn on_endpoint_disconnect(&mut self, client: &ClientProxy, endpoint_id: &str) {
        self.inner.on_endpoint_disconnect(client, endpoint_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny in-module medium handler that records the bookkeeping callbacks.
    #[derive(Default)]
    struct RecordingHandler {
        initialize_calls: Vec<(String, String)>, // (upgrade_service_id, endpoint_id)
        revert_calls: Vec<String>,               // upgrade_service_id
        /// If set, `handle_initialize` returns empty (failure) for this endpoint.
        fail_for: Option<String>,
    }

    impl MediumBwuHandler for RecordingHandler {
        fn handle_initialize_upgraded_medium_for_endpoint(
            &mut self,
            _client: &ClientProxy,
            upgrade_service_id: &str,
            endpoint_id: &str,
        ) -> Vec<u8> {
            self.initialize_calls
                .push((upgrade_service_id.to_string(), endpoint_id.to_string()));
            if self.fail_for.as_deref() == Some(endpoint_id) {
                return Vec::new();
            }
            vec![1] // non-empty = success
        }
        fn handle_revert_initiator_state_for_service(&mut self, upgrade_service_id: &str) {
            self.revert_calls.push(upgrade_service_id.to_string());
        }
        fn create_upgraded_endpoint_channel(
            &mut self,
            _client: &ClientProxy,
            _service_id: &str,
            _endpoint_id: &str,
            _upgrade_path_info: &UpgradePathInfo,
        ) -> Option<Arc<dyn EndpointChannel>> {
            None
        }
        fn get_upgrade_medium(&self) -> Medium {
            Medium::WebRtc
        }
        fn on_endpoint_disconnect(&mut self, _client: &ClientProxy, _endpoint_id: &str) {}
    }

    #[test]
    fn initialize_wraps_service_id_and_tracks_endpoints() {
        let client = ClientProxy::default();
        let mut handler = BaseBwuHandler::new(RecordingHandler::default());

        let frame =
            handler.initialize_upgraded_medium_for_endpoint(&client, "ServiceA", "Endpoint1");
        assert_eq!(frame, vec![1]);
        // HandleInitialize is called with the WRAPPED id.
        assert_eq!(
            handler.inner().initialize_calls,
            vec![("ServiceA_UPGRADE".to_string(), "Endpoint1".to_string())]
        );
    }

    #[test]
    fn empty_initialize_frame_is_not_tracked() {
        let client = ClientProxy::default();
        let mut handler = BaseBwuHandler::new(RecordingHandler {
            fail_for: Some("Endpoint1".to_string()),
            ..Default::default()
        });
        let frame =
            handler.initialize_upgraded_medium_for_endpoint(&client, "ServiceA", "Endpoint1");
        assert!(frame.is_empty());
        // Not tracked → reverting the endpoint does nothing.
        handler.revert_initiator_state_for_endpoint("ServiceA_UPGRADE", "Endpoint1");
        assert!(handler.inner().revert_calls.is_empty());
    }

    #[test]
    fn revert_per_endpoint_fires_only_on_last() {
        let client = ClientProxy::default();
        let mut handler = BaseBwuHandler::new(RecordingHandler::default());
        handler.initialize_upgraded_medium_for_endpoint(&client, "ServiceA", "Endpoint1");
        handler.initialize_upgraded_medium_for_endpoint(&client, "ServiceA", "Endpoint2");

        // First endpoint reverted: not the last → no handler revert.
        handler.revert_initiator_state_for_endpoint("ServiceA_UPGRADE", "Endpoint1");
        assert!(handler.inner().revert_calls.is_empty());
        // Last endpoint reverted: handler revert fires once with the wrapped id.
        handler.revert_initiator_state_for_endpoint("ServiceA_UPGRADE", "Endpoint2");
        assert_eq!(
            handler.inner().revert_calls,
            vec!["ServiceA_UPGRADE".to_string()]
        );
    }

    #[test]
    fn revert_ignores_unwrapped_service_id() {
        let client = ClientProxy::default();
        let mut handler = BaseBwuHandler::new(RecordingHandler::default());
        handler.initialize_upgraded_medium_for_endpoint(&client, "ServiceA", "Endpoint1");
        // Raw (unwrapped) id is rejected.
        handler.revert_initiator_state_for_endpoint("ServiceA", "Endpoint1");
        assert!(handler.inner().revert_calls.is_empty());
    }

    #[test]
    fn revert_all_reverts_each_service_once() {
        let client = ClientProxy::default();
        let mut handler = BaseBwuHandler::new(RecordingHandler::default());
        handler.initialize_upgraded_medium_for_endpoint(&client, "ServiceA", "Endpoint1");
        handler.initialize_upgraded_medium_for_endpoint(&client, "ServiceB", "Endpoint2");
        handler.revert_initiator_state();
        let mut reverts = handler.inner().revert_calls.clone();
        reverts.sort();
        assert_eq!(reverts, vec!["ServiceA_UPGRADE", "ServiceB_UPGRADE"]);
    }

    #[test]
    fn responder_revert_calls_handler_with_raw_id() {
        let mut handler = BaseBwuHandler::new(RecordingHandler::default());
        handler.revert_responder_state("ServiceA");
        assert_eq!(handler.inner().revert_calls, vec!["ServiceA".to_string()]);
    }
}
