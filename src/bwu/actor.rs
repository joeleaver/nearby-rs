//! A Tokio integration actor that wraps the synchronous [`BwuManager`].
//!
//! The state machine in [`crate::bwu::manager`] is a single-owner synchronous
//! object — Google's serial-executor model requires that every frame, timer, and
//! connection event be applied in a strict order, or the upgrade handshake
//! reorders and wedges. This actor provides that single owner: one task owns the
//! `BwuManager` + its [`ClientProxy`] + the shared [`EndpointChannelManager`],
//! and every operation arrives as a [`BwuCommand`] over a channel, so they are
//! applied one at a time. It also closes the retry seam the manager exposes
//! ([`BwuManager::pending_retry_delays`] / [`BwuManager::fire_retry_alarm`]) by
//! arming a real timer for the earliest pending retry and firing it inline.
//!
//! ## Threading model
//! [`BwuActor`] is `Send`, so it runs wherever you like: spawned on a
//! multi-thread runtime with [`tokio::spawn`], or — to keep it off a shared
//! runtime — moved onto its own dedicated thread:
//!
//! ```no_run
//! # fn spawn(actor: nearby_rs::bwu::BwuActor) {
//! std::thread::spawn(move || {
//!     let rt = tokio::runtime::Builder::new_current_thread()
//!         .enable_time()
//!         .build()
//!         .unwrap();
//!     rt.block_on(actor.run());
//! });
//! # }
//! ```
//!
//! The returned [`BwuHandle`] is `Send + Sync + Clone`, so any task on any thread
//! can drive the actor; [`BwuCommand`] only carries `Send` payloads. The actor
//! never holds the [`EndpointChannelManager`] lock across an `.await`, so its
//! `run` future stays `Send`.

use std::collections::{HashMap, HashSet};
use std::future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::bwu::channel::{DisconnectionReason, EndpointChannel, SafeDisconnectionResult};
use crate::bwu::channel_manager::EndpointChannelManager;
use crate::bwu::client::ClientProxy;
use crate::bwu::handler::{BwuHandler, IncomingSocketConnection};
use crate::bwu::manager::{BwuConfig, BwuManager};
use crate::mediums::Medium;
use crate::proto as pb;

/// A unit of work for the [`BwuActor`]. All variants carry only `Send` payloads,
/// so the [`BwuHandle`] is usable across threads.
pub enum BwuCommand {
    // --- ClientProxy lifecycle (the actor owns the client) ---
    /// `ClientProxy::OnConnectionInitiated`.
    ConnectionInitiated {
        endpoint_id: String,
        is_incoming: bool,
        auto_upgrade_bandwidth: bool,
    },
    /// `ClientProxy::OnConnectionAccepted` (→ connected).
    ConnectionAccepted { endpoint_id: String },
    /// `ClientProxy::LocalEndpointAcceptedConnection`.
    LocalEndpointAccepted { endpoint_id: String },
    /// `ClientProxy::RemoteEndpointAcceptedConnection`.
    RemoteEndpointAccepted { endpoint_id: String },
    /// `ClientProxy::OnDisconnected`.
    Disconnected { endpoint_id: String },
    /// Sets the endpoint's preference-ordered upgrade mediums.
    SetUpgradeMediums {
        endpoint_id: String,
        mediums: Vec<Medium>,
    },

    // --- channel registry (EndpointChannelManager) ---
    /// Registers the active (pre-upgrade) channel for an endpoint.
    RegisterChannel {
        endpoint_id: String,
        channel: Arc<dyn EndpointChannel>,
    },
    /// Removes an endpoint's channel registration.
    UnregisterChannel {
        endpoint_id: String,
        reason: DisconnectionReason,
        result: SafeDisconnectionResult,
    },

    // --- BWU operations ---
    /// Initiator: start an upgrade to `medium`.
    InitiateBwu { endpoint_id: String, medium: Medium },
    /// Feed an incoming `BANDWIDTH_UPGRADE_NEGOTIATION` frame.
    IncomingFrame {
        frame: Box<pb::OfflineFrame>,
        endpoint_id: String,
        medium: Medium,
    },
    /// The medium layer accepted an upgraded socket (the `OnIncomingConnection`
    /// callback).
    IncomingConnection {
        connection: IncomingSocketConnection,
    },
    /// An endpoint disconnected; clean up its upgrade state.
    EndpointDisconnect {
        service_id: String,
        endpoint_id: String,
        reason: DisconnectionReason,
    },
    /// Revert all handler state and stop the actor.
    Shutdown,

    // --- queries (reply over a oneshot) ---
    /// Whether an upgrade is in progress for `endpoint_id`.
    IsUpgradeOngoing {
        endpoint_id: String,
        reply: oneshot::Sender<bool>,
    },
    /// The delay of the retry currently scheduled for `endpoint_id`, if any.
    PendingRetryDelay {
        endpoint_id: String,
        reply: oneshot::Sender<Option<Duration>>,
    },
    /// A snapshot of the `on_bandwidth_changed` success events recorded so far.
    BandwidthChangedEvents {
        reply: oneshot::Sender<Vec<(String, Medium)>>,
    },
    /// The channel currently registered for `endpoint_id`. After a converged
    /// upgrade this is the new (upgraded) channel, which the consumer needs to
    /// continue the transfer on the upgraded medium — the actor otherwise keeps
    /// it private. `None` if the endpoint has no registered channel.
    GetUpgradedChannel {
        endpoint_id: String,
        reply: oneshot::Sender<Option<Arc<dyn EndpointChannel>>>,
    },
}

/// A cheap, `Send + Sync` handle for driving a [`BwuActor`] from any task/thread.
#[derive(Clone)]
pub struct BwuHandle {
    tx: mpsc::Sender<BwuCommand>,
}

impl BwuHandle {
    async fn fire(&self, cmd: BwuCommand) {
        // A send error means the actor stopped; callers treat that as a no-op.
        let _ = self.tx.send(cmd).await;
    }

    async fn query<T>(&self, make: impl FnOnce(oneshot::Sender<T>) -> BwuCommand) -> Option<T> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(make(reply)).await.ok()?;
        rx.await.ok()
    }

    pub async fn connection_initiated(
        &self,
        endpoint_id: impl Into<String>,
        is_incoming: bool,
        auto_upgrade_bandwidth: bool,
    ) {
        self.fire(BwuCommand::ConnectionInitiated {
            endpoint_id: endpoint_id.into(),
            is_incoming,
            auto_upgrade_bandwidth,
        })
        .await;
    }

    pub async fn connection_accepted(&self, endpoint_id: impl Into<String>) {
        self.fire(BwuCommand::ConnectionAccepted {
            endpoint_id: endpoint_id.into(),
        })
        .await;
    }

    pub async fn local_endpoint_accepted(&self, endpoint_id: impl Into<String>) {
        self.fire(BwuCommand::LocalEndpointAccepted {
            endpoint_id: endpoint_id.into(),
        })
        .await;
    }

    pub async fn remote_endpoint_accepted(&self, endpoint_id: impl Into<String>) {
        self.fire(BwuCommand::RemoteEndpointAccepted {
            endpoint_id: endpoint_id.into(),
        })
        .await;
    }

    pub async fn disconnected(&self, endpoint_id: impl Into<String>) {
        self.fire(BwuCommand::Disconnected {
            endpoint_id: endpoint_id.into(),
        })
        .await;
    }

    pub async fn set_upgrade_mediums(&self, endpoint_id: impl Into<String>, mediums: Vec<Medium>) {
        self.fire(BwuCommand::SetUpgradeMediums {
            endpoint_id: endpoint_id.into(),
            mediums,
        })
        .await;
    }

    pub async fn register_channel(
        &self,
        endpoint_id: impl Into<String>,
        channel: Arc<dyn EndpointChannel>,
    ) {
        self.fire(BwuCommand::RegisterChannel {
            endpoint_id: endpoint_id.into(),
            channel,
        })
        .await;
    }

    pub async fn unregister_channel(
        &self,
        endpoint_id: impl Into<String>,
        reason: DisconnectionReason,
        result: SafeDisconnectionResult,
    ) {
        self.fire(BwuCommand::UnregisterChannel {
            endpoint_id: endpoint_id.into(),
            reason,
            result,
        })
        .await;
    }

    pub async fn initiate_bwu(&self, endpoint_id: impl Into<String>, medium: Medium) {
        self.fire(BwuCommand::InitiateBwu {
            endpoint_id: endpoint_id.into(),
            medium,
        })
        .await;
    }

    pub async fn incoming_frame(
        &self,
        frame: pb::OfflineFrame,
        endpoint_id: impl Into<String>,
        medium: Medium,
    ) {
        self.fire(BwuCommand::IncomingFrame {
            frame: Box::new(frame),
            endpoint_id: endpoint_id.into(),
            medium,
        })
        .await;
    }

    pub async fn incoming_connection(&self, connection: IncomingSocketConnection) {
        self.fire(BwuCommand::IncomingConnection { connection })
            .await;
    }

    pub async fn endpoint_disconnect(
        &self,
        service_id: impl Into<String>,
        endpoint_id: impl Into<String>,
        reason: DisconnectionReason,
    ) {
        self.fire(BwuCommand::EndpointDisconnect {
            service_id: service_id.into(),
            endpoint_id: endpoint_id.into(),
            reason,
        })
        .await;
    }

    pub async fn shutdown(&self) {
        self.fire(BwuCommand::Shutdown).await;
    }

    /// A [`ConnectionSink`](crate::bwu::wifi_lan::ConnectionSink) that posts an
    /// upgraded socket to this actor as a [`BwuCommand::IncomingConnection`]. It
    /// uses `blocking_send`, so call it only from a **blocking** (non-async)
    /// context — e.g. a `TcpListener` accept loop thread, which is exactly where
    /// the WIFI_LAN handler invokes it.
    pub fn connection_sink(&self) -> Arc<dyn Fn(IncomingSocketConnection) + Send + Sync> {
        let tx = self.tx.clone();
        Arc::new(move |connection| {
            let _ = tx.blocking_send(BwuCommand::IncomingConnection { connection });
        })
    }

    pub async fn is_upgrade_ongoing(&self, endpoint_id: impl Into<String>) -> bool {
        let endpoint_id = endpoint_id.into();
        self.query(|reply| BwuCommand::IsUpgradeOngoing { endpoint_id, reply })
            .await
            .unwrap_or(false)
    }

    pub async fn pending_retry_delay(&self, endpoint_id: impl Into<String>) -> Option<Duration> {
        let endpoint_id = endpoint_id.into();
        self.query(|reply| BwuCommand::PendingRetryDelay { endpoint_id, reply })
            .await
            .flatten()
    }

    pub async fn bandwidth_changed_events(&self) -> Vec<(String, Medium)> {
        self.query(|reply| BwuCommand::BandwidthChangedEvents { reply })
            .await
            .unwrap_or_default()
    }

    /// The channel currently registered for `endpoint_id`. After a converged
    /// upgrade — observed via [`BwuHandle::bandwidth_changed_events`] — this is
    /// the new upgraded channel; the consumer retrieves it here to continue the
    /// transfer on the upgraded medium. `None` if the endpoint has no channel.
    pub async fn get_upgraded_channel(
        &self,
        endpoint_id: impl Into<String>,
    ) -> Option<Arc<dyn EndpointChannel>> {
        let endpoint_id = endpoint_id.into();
        self.query(|reply| BwuCommand::GetUpgradedChannel { endpoint_id, reply })
            .await
            .flatten()
    }
}

/// The owning side of the actor. Construct with [`BwuActor::new`] (or
/// [`BwuActor::channel`] + [`BwuActor::build`] when a handler needs a sink to the
/// actor), then `tokio::spawn` it or run it on a dedicated thread.
pub struct BwuActor {
    manager: BwuManager,
    client: ClientProxy,
    ecm: Arc<Mutex<EndpointChannelManager>>,
    rx: mpsc::Receiver<BwuCommand>,
    /// Endpoints with an armed retry timer and the absolute deadline it fires at.
    armed: HashMap<String, Instant>,
}

impl BwuActor {
    /// Builds an actor and its handle. The command channel is bounded by
    /// `command_buffer`.
    pub fn new(
        handlers: HashMap<Medium, Box<dyn BwuHandler>>,
        config: BwuConfig,
        local_endpoint_id: impl Into<String>,
        command_buffer: usize,
    ) -> (BwuHandle, BwuActor) {
        let (handle, rx) = Self::channel(command_buffer);
        let actor = Self::build(rx, handlers, config, local_endpoint_id);
        (handle, actor)
    }

    /// Creates just the command channel + handle, so handlers can be built with a
    /// [`BwuHandle::connection_sink`] that posts to this actor *before* the actor
    /// is constructed (e.g. a WIFI_LAN listener accept loop). Pair with
    /// [`BwuActor::build`], passing the returned receiver.
    pub fn channel(command_buffer: usize) -> (BwuHandle, mpsc::Receiver<BwuCommand>) {
        let (tx, rx) = mpsc::channel(command_buffer.max(1));
        (BwuHandle { tx }, rx)
    }

    /// Builds the actor around a receiver from [`BwuActor::channel`].
    pub fn build(
        rx: mpsc::Receiver<BwuCommand>,
        handlers: HashMap<Medium, Box<dyn BwuHandler>>,
        config: BwuConfig,
        local_endpoint_id: impl Into<String>,
    ) -> BwuActor {
        let ecm = Arc::new(Mutex::new(EndpointChannelManager::new()));
        let manager = BwuManager::new(ecm.clone(), handlers, config);
        let client = ClientProxy::new(0, &local_endpoint_id.into());
        BwuActor {
            manager,
            client,
            ecm,
            rx,
            armed: HashMap::new(),
        }
    }

    /// Runs the actor until [`BwuCommand::Shutdown`] or until every [`BwuHandle`]
    /// is dropped. `Send`, so spawn it on any runtime or a dedicated thread.
    pub async fn run(mut self) {
        self.reconcile_timers();
        loop {
            let next_deadline = self.armed.values().min().copied();
            let timer = async {
                match next_deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => future::pending::<()>().await,
                }
            };

            tokio::select! {
                // Bias the timer so that, when a retry deadline and a command are
                // both ready, the retry is applied first (deterministic ordering).
                biased;
                _ = timer => {
                    self.fire_due_retries();
                }
                cmd = self.rx.recv() => match cmd {
                    Some(BwuCommand::Shutdown) => {
                        self.manager.shutdown();
                        break;
                    }
                    Some(cmd) => {
                        self.handle(cmd);
                        self.reconcile_timers();
                    }
                    None => break,
                },
            }
        }
    }

    fn handle(&mut self, cmd: BwuCommand) {
        match cmd {
            BwuCommand::ConnectionInitiated {
                endpoint_id,
                is_incoming,
                auto_upgrade_bandwidth,
            } => self.client.on_connection_initiated(
                &endpoint_id,
                is_incoming,
                auto_upgrade_bandwidth,
            ),
            BwuCommand::ConnectionAccepted { endpoint_id } => {
                self.client.on_connection_accepted(&endpoint_id)
            }
            BwuCommand::LocalEndpointAccepted { endpoint_id } => {
                self.client.local_endpoint_accepted_connection(&endpoint_id)
            }
            BwuCommand::RemoteEndpointAccepted { endpoint_id } => self
                .client
                .remote_endpoint_accepted_connection(&endpoint_id),
            BwuCommand::Disconnected { endpoint_id } => self.client.on_disconnected(&endpoint_id),
            BwuCommand::SetUpgradeMediums {
                endpoint_id,
                mediums,
            } => self.client.set_upgrade_mediums(&endpoint_id, mediums),
            BwuCommand::RegisterChannel {
                endpoint_id,
                channel,
            } => self.ecm.lock().unwrap().register_channel_for_endpoint(
                &self.client,
                &endpoint_id,
                channel,
            ),
            BwuCommand::UnregisterChannel {
                endpoint_id,
                reason,
                result,
            } => {
                self.ecm.lock().unwrap().unregister_channel_for_endpoint(
                    &endpoint_id,
                    reason,
                    result,
                );
            }
            BwuCommand::InitiateBwu {
                endpoint_id,
                medium,
            } => self
                .manager
                .initiate_bwu_for_endpoint(&mut self.client, &endpoint_id, medium),
            BwuCommand::IncomingFrame {
                frame,
                endpoint_id,
                medium,
            } => self
                .manager
                .on_incoming_frame(&frame, &endpoint_id, &mut self.client, medium),
            BwuCommand::IncomingConnection { connection } => self
                .manager
                .on_incoming_connection(&mut self.client, connection),
            BwuCommand::EndpointDisconnect {
                service_id,
                endpoint_id,
                reason,
            } => self.manager.on_endpoint_disconnect(
                &mut self.client,
                &service_id,
                &endpoint_id,
                reason,
            ),
            BwuCommand::IsUpgradeOngoing { endpoint_id, reply } => {
                let _ = reply.send(self.manager.is_upgrade_ongoing(&endpoint_id));
            }
            BwuCommand::PendingRetryDelay { endpoint_id, reply } => {
                let _ = reply.send(self.manager.pending_retry_delay(&endpoint_id));
            }
            BwuCommand::BandwidthChangedEvents { reply } => {
                let _ = reply.send(self.client.bandwidth_changed_events().to_vec());
            }
            BwuCommand::GetUpgradedChannel { endpoint_id, reply } => {
                // Lock the ecm only for the clone — never across the reply/await,
                // so the actor's run future stays Send and can't deadlock.
                let channel = self
                    .ecm
                    .lock()
                    .unwrap()
                    .get_channel_for_endpoint(&endpoint_id);
                let _ = reply.send(channel);
            }
            // Handled in `run` before dispatch.
            BwuCommand::Shutdown => unreachable!("Shutdown is handled in run()"),
        }
    }

    /// Fires every retry whose deadline has elapsed, then re-arms from whatever
    /// the fire scheduled next.
    fn fire_due_retries(&mut self) {
        let now = Instant::now();
        let due: Vec<String> = self
            .armed
            .iter()
            .filter(|(_, &deadline)| deadline <= now)
            .map(|(ep, _)| ep.clone())
            .collect();
        for ep in due {
            self.armed.remove(&ep);
            self.manager.fire_retry_alarm(&mut self.client, &ep);
        }
        self.reconcile_timers();
    }

    /// Reconciles the armed-timer set against the manager's pending retries: arm
    /// a deadline for any newly-pending endpoint, drop timers whose retry was
    /// cancelled. Existing deadlines are kept (not reset) so a timer fires at the
    /// instant it was first scheduled for.
    fn reconcile_timers(&mut self) {
        let now = Instant::now();
        let pending = self.manager.pending_retry_delays();
        let pending_eps: HashSet<&String> = pending.iter().map(|(ep, _)| ep).collect();
        self.armed.retain(|ep, _| pending_eps.contains(ep));
        for (ep, delay) in &pending {
            self.armed.entry(ep.clone()).or_insert_with(|| now + *delay);
        }
    }
}
