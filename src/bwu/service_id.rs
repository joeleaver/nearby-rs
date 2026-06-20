//! Service-id wrapping for the bandwidth-upgrade initiator path.
//!
//! Port of `connections/implementation/service_id_constants.h`. The INITIATOR
//! of an upgrade records/looks-up state under a WRAPPED service id (the raw id
//! plus an `_UPGRADE` postfix), to distinguish the upgrade mediums from those
//! used for advertising/discovery; the RESPONDER path uses the raw service id.

/// `kUnknownServiceId`.
pub const UNKNOWN_SERVICE_ID: &str = "UNKNOWN_SERVICE";

/// Suffix appended to service IDs when initiating a bandwidth upgrade.
const INITIATOR_UPGRADE_SERVICE_ID_POSTFIX: &str = "_UPGRADE";

/// True if `service_id` is non-empty and has the initiator's upgrade postfix.
pub fn is_initiator_upgrade_service_id(service_id: &str) -> bool {
    !service_id.is_empty() && service_id.ends_with(INITIATOR_UPGRADE_SERVICE_ID_POSTFIX)
}

/// Appends the upgrade postfix to `service_id` if necessary (no-op if empty or
/// already wrapped).
pub fn wrap_initiator_upgrade_service_id(service_id: &str) -> String {
    if service_id.is_empty() || is_initiator_upgrade_service_id(service_id) {
        return service_id.to_string();
    }
    format!("{service_id}{INITIATOR_UPGRADE_SERVICE_ID_POSTFIX}")
}

/// Strips the upgrade postfix from `service_id` if present (no-op otherwise).
pub fn unwrap_initiator_upgrade_service_id(service_id: &str) -> String {
    if service_id.is_empty() || !is_initiator_upgrade_service_id(service_id) {
        return service_id.to_string();
    }
    service_id
        .strip_suffix(INITIATOR_UPGRADE_SERVICE_ID_POSTFIX)
        .unwrap_or(service_id)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_only_when_needed() {
        assert_eq!(wrap_initiator_upgrade_service_id("ServiceA"), "ServiceA_UPGRADE");
        // Idempotent and empty-safe.
        assert_eq!(
            wrap_initiator_upgrade_service_id("ServiceA_UPGRADE"),
            "ServiceA_UPGRADE"
        );
        assert_eq!(wrap_initiator_upgrade_service_id(""), "");
    }

    #[test]
    fn recognizes_and_unwraps() {
        assert!(is_initiator_upgrade_service_id("ServiceA_UPGRADE"));
        assert!(!is_initiator_upgrade_service_id("ServiceA"));
        assert!(!is_initiator_upgrade_service_id(""));
        assert_eq!(unwrap_initiator_upgrade_service_id("ServiceA_UPGRADE"), "ServiceA");
        assert_eq!(unwrap_initiator_upgrade_service_id("ServiceA"), "ServiceA");
        assert_eq!(unwrap_initiator_upgrade_service_id(""), "");
    }
}
