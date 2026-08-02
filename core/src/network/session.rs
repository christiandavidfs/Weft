use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{accept_async, connect_async};
use uuid::Uuid;

use crate::media::{MediaEngine, MediaStats, MemberMedia};
use crate::network::control::{C2S, S2C};

pub const SERVICE_TYPE: &str = "_weft._tcp.local.";
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_millis(2500);
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Bootstrap,
    Coordinator,
    Member,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Bootstrap => "bootstrap",
            Role::Coordinator => "coordinator",
            Role::Member => "member",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberInfo {
    pub device_id: String,
    pub device_name: String,
    pub addr: String,
    pub port: u16,
    pub media_port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    pub device_id: String,
    pub device_name: String,
    pub addr: String,
    pub port: u16,
    pub media_port: u16,
    pub is_coordinator: bool,
}

#[derive(Debug, Clone)]
pub struct NetworkStatus {
    pub running: bool,
    pub device_id: String,
    pub device_name: String,
    pub role: String,
    pub session_id: String,
    pub coordinator_id: String,
    pub transmitter_id: String,
    pub members: Vec<MemberInfo>,
    pub peers: Vec<PeerInfo>,
    pub pending_transmit_requests: Vec<String>,
    pub media_port: u16,
}

#[derive(Debug, Clone)]
pub struct NetworkEvent {
    pub kind: String,
    pub device_id: String,
    pub device_name: String,
    pub message: String,
}

struct State {
    stopped: bool,
    device_id: String,
    device_name: String,
    role: Role,
    session_id: String,
    coordinator_id: String,
    transmitter_id: Option<String>,
    members: BTreeMap<String, MemberInfo>,
    peers: BTreeMap<String, PeerInfo>,
    peers_by_instance: HashMap<String, String>,
    pending_requests: Vec<String>,
    addr: IpAddr,
    port: u16,
    media_port: u16,
    coordinator_addr: Option<SocketAddr>,
    connecting_to: Option<SocketAddr>,
    conns: HashMap<String, tokio::sync::mpsc::UnboundedSender<WsMessage>>,
    coordinator_tx: Option<tokio::sync::mpsc::UnboundedSender<WsMessage>>,
}

struct Advert {
    daemon: ServiceDaemon,
    instance: String,
    host: String,
    addr: IpAddr,
    port: u16,
}

pub struct SessionInner {
    rt: Handle,
    state: Mutex<State>,
    events: Mutex<Option<Box<dyn Fn(NetworkEvent) + Send + Sync>>>,
    mdns: Mutex<Option<Advert>>,
    media: Mutex<Option<Arc<MediaEngine>>>,
}

impl SessionInner {
    pub fn new(rt: Handle, device_id: String, device_name: String) -> Arc<Self> {
        Arc::new(Self {
            rt,
            state: Mutex::new(State {
                stopped: false,
                device_id,
                device_name,
                role: Role::Bootstrap,
                session_id: String::new(),
                coordinator_id: String::new(),
                transmitter_id: None,
                members: BTreeMap::new(),
                peers: BTreeMap::new(),
                peers_by_instance: HashMap::new(),
                pending_requests: Vec::new(),
                addr: "127.0.0.1".parse().unwrap(),
                port: 0,
                media_port: 0,
                coordinator_addr: None,
                connecting_to: None,
                conns: HashMap::new(),
                coordinator_tx: None,
            }),
            events: Mutex::new(None),
            mdns: Mutex::new(None),
            media: Mutex::new(None),
        })
    }

    fn emit(&self, kind: &str, device_id: &str, device_name: &str, message: &str) {
        let event = NetworkEvent {
            kind: kind.to_string(),
            device_id: device_id.to_string(),
            device_name: device_name.to_string(),
            message: message.to_string(),
        };
        if let Some(cb) = self.events.lock().unwrap().as_ref() {
            cb(event);
        }
    }

    pub fn set_event_callback(&self, cb: Box<dyn Fn(NetworkEvent) + Send + Sync>) {
        *self.events.lock().unwrap() = Some(cb);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    pub fn status(&self) -> NetworkStatus {
        let s = self.lock();
        NetworkStatus {
            running: !s.stopped,
            device_id: s.device_id.clone(),
            device_name: s.device_name.clone(),
            role: s.role.as_str().to_string(),
            session_id: s.session_id.clone(),
            coordinator_id: s.coordinator_id.clone(),
            transmitter_id: s.transmitter_id.clone().unwrap_or_default(),
            members: s.members.values().cloned().collect(),
            peers: s.peers.values().cloned().collect(),
            pending_transmit_requests: s.pending_requests.clone(),
            media_port: s.media_port,
        }
    }

    pub fn media(&self) -> Option<Arc<MediaEngine>> {
        self.media.lock().unwrap().clone()
    }

    pub fn media_stats(&self) -> Option<MediaStats> {
        self.media().map(|m| m.stats())
    }

    pub fn set_media(self: &Arc<Self>, engine: Arc<MediaEngine>) {
        let media_port = engine.media_port();
        {
            let mut s = self.lock();
            s.media_port = media_port;
        }
        *self.media.lock().unwrap() = Some(engine.clone());
        let inner = self.clone();
        engine.set_event_callback(Box::new(move |kind, msg| {
            match kind {
                "transmit_finished" => {
                    let my_id = inner.my_id();
                    let is_tx = {
                        let s = inner.lock();
                        s.transmitter_id.as_deref() == Some(my_id.as_str())
                    };
                    if is_tx {
                        inner.release_transmit();
                    }
                }
                _ => {}
            }
            inner.emit(kind, "", "", &msg);
        }));
    }

    fn is_stopped(&self) -> bool {
        self.lock().stopped
    }

    fn my_id(&self) -> String {
        self.lock().device_id.clone()
    }

    fn my_name(&self) -> String {
        self.lock().device_name.clone()
    }

    fn is_coordinator(&self) -> bool {
        self.lock().role == Role::Coordinator
    }

    pub fn set_addr_port(&self, addr: IpAddr, port: u16) {
        let mut s = self.lock();
        s.addr = addr;
        s.port = port;
    }

    // ---- mDNS advertisement ----

    pub fn setup_mdns(&self, daemon: ServiceDaemon, host: String, addr: IpAddr, port: u16) {
        let instance = format!("weft-{}", &Uuid::new_v4().to_string()[..8]);
        *self.mdns.lock().unwrap() = Some(Advert { daemon, instance, host, addr, port });
    }

    fn mdns_daemon(&self) -> ServiceDaemon {
        self.mdns.lock().unwrap().as_ref().unwrap().daemon.clone()
    }

    fn re_advertise(&self) {
        let (daemon, instance, host, addr, port, role, device_id, device_name, media_port) = {
            let mdns = self.mdns.lock().unwrap();
            let a = mdns.as_ref().expect("mdns not set up");
            let s = self.lock();
            (
                a.daemon.clone(),
                a.instance.clone(),
                a.host.clone(),
                a.addr,
                a.port,
                s.role.as_str().to_string(),
                s.device_id.clone(),
                s.device_name.clone(),
                s.media_port,
            )
        };
        let mut txt = HashMap::new();
        txt.insert("device_id".to_string(), device_id);
        txt.insert("device_name".to_string(), device_name);
        txt.insert("coordinator".to_string(), (role == "coordinator").to_string());
        txt.insert("port".to_string(), port.to_string());
        txt.insert("media_port".to_string(), media_port.to_string());
        let service =
            ServiceInfo::new(SERVICE_TYPE, &instance, &host, addr, port, Some(txt)).expect("bad service");
        let _ = daemon.register(service);
    }

    // ---- peer discovery (runs on mdns thread) ----

    pub fn on_peer_resolved(self: Arc<Self>, info: &ServiceInfo) {
        let props = info.get_properties();
        let Some(device_id) = props.get_property_val_str("device_id").map(str::to_string) else {
            return;
        };
        if device_id == self.my_id() {
            return;
        }
        let addrs: Vec<std::net::Ipv4Addr> = info.get_addresses_v4().into_iter().copied().collect();
        let Some(ip) = addrs.first() else {
            return;
        };
        let addr = IpAddr::V4(*ip);
        let port: u16 = props
            .get_property_val_str("port")
            .and_then(|p| p.parse().ok())
            .unwrap_or(0);
        let media_port: u16 = props
            .get_property_val_str("media_port")
            .and_then(|p| p.parse().ok())
            .unwrap_or(0);
        let is_coordinator = props
            .get_property_val_str("coordinator")
            .map(|v| v == "true")
            .unwrap_or(false);
        let device_name = props.get_property_val_str("device_name").unwrap_or("").to_string();

        let mut join_addr = None;
        let mut demote = false;
        {
            let mut s = self.lock();
            s.peers_by_instance.insert(info.get_fullname().to_string(), device_id.clone());
            s.peers.insert(
                device_id.clone(),
                PeerInfo {
                    device_id: device_id.clone(),
                    device_name: device_name.clone(),
                    addr: addr.to_string(),
                    port,
                    media_port,
                    is_coordinator,
                },
            );
            if is_coordinator {
                let coord_addr = SocketAddr::new(addr, port);
                if s.role == Role::Coordinator && device_id < s.device_id {
                    demote = true;
                    join_addr = Some(coord_addr);
                } else if s.role != Role::Coordinator && s.coordinator_id != device_id {
                    join_addr = Some(coord_addr);
                }
            }
        }
        if demote {
            self.clone().demote_to_member(&join_addr.unwrap(), &device_id);
        } else if let Some(addr) = join_addr {
            self.clone().join_coordinator(&addr, &device_id);
        }
        self.emit("device_found", &device_id, &device_name, &format!("visible en {addr}"));
    }

    pub fn on_peer_removed(&self, fullname: &str) {
        let removed = {
            let mut s = self.lock();
            let id = s.peers_by_instance.remove(fullname);
            if let Some(id) = &id {
                s.peers.remove(id);
            }
            id
        };
        if let Some(id) = &removed {
            self.emit("device_lost", id, "", "dejó la red");
            let was_coord = {
                let s = self.lock();
                s.coordinator_id == *id && s.role == Role::Member
            };
            if was_coord {
                self.promote_to_coordinator();
            }
        }
    }

    pub fn bootstrap_elapsed(&self) {
        let promote = {
            let mut s = self.lock();
            if s.stopped || s.role != Role::Bootstrap {
                false
            } else if s.coordinator_id.is_empty() && s.coordinator_addr.is_none() {
                true
            } else {
                s.role = Role::Member;
                false
            }
        };
        if promote {
            self.become_coordinator();
        }
    }

    // ---- role transitions ----

    pub fn become_coordinator(&self) {
        let session_id;
        {
            let mut s = self.lock();
            s.role = Role::Coordinator;
            s.coordinator_id = s.device_id.clone();
            if s.session_id.is_empty() {
                s.session_id = Uuid::new_v4().to_string();
            }
            s.coordinator_addr = None;
            s.connecting_to = None;
            s.pending_requests.clear();
            let (self_id, self_name, self_addr, self_port, self_media_port) =
                (s.device_id.clone(), s.device_name.clone(), s.addr.to_string(), s.port, s.media_port);
            s.members.insert(
                self_id.clone(),
                MemberInfo {
                    device_id: self_id,
                    device_name: self_name,
                    addr: self_addr,
                    port: self_port,
                    media_port: self_media_port,
                },
            );
            session_id = s.session_id.clone();
        }
        self.re_advertise();
        self.configure_media_session();
        self.refresh_transmitter_media();
        self.emit(
            "session_created",
            &self.my_id(),
            &self.my_name(),
            &format!("sesión creada por mí ({session_id})"),
        );
        self.emit("role_changed", &self.my_id(), &self.my_name(), "coordinator");
    }

    fn join_coordinator(self: Arc<Self>, addr: &SocketAddr, coord_id: &str) {
        {
            let mut s = self.lock();
            s.coordinator_id = coord_id.to_string();
            s.coordinator_addr = Some(*addr);
            if s.role == Role::Bootstrap {
                s.role = Role::Member;
            }
        }
        self.spawn_connect_if_needed(*addr);
    }

    fn demote_to_member(self: Arc<Self>, addr: &SocketAddr, coord_id: &str) {
        {
            let mut s = self.lock();
            if s.role != Role::Coordinator {
                return;
            }
            s.role = Role::Member;
            s.coordinator_id = coord_id.to_string();
            s.coordinator_addr = Some(*addr);
            s.transmitter_id = None;
            s.pending_requests.clear();
            s.conns.clear();
        }
        self.re_advertise();
        self.refresh_transmitter_media();
        self.emit("role_changed", &self.my_id(), &self.my_name(), "member");
        self.spawn_connect_if_needed(*addr);
    }

    fn promote_to_coordinator(&self) {
        let should = {
            let s = self.lock();
            s.role == Role::Member && !s.stopped
        };
        if should {
            self.become_coordinator();
        }
    }

    fn spawn_connect_if_needed(self: Arc<Self>, addr: SocketAddr) {
        let spawned = {
            let mut s = self.lock();
            if s.stopped || s.role != Role::Member || s.coordinator_addr != Some(addr) {
                false
            } else if s.connecting_to == Some(addr) {
                false
            } else {
                s.connecting_to = Some(addr);
                true
            }
        };
        if spawned {
            let rt = self.rt.clone();
            rt.spawn(async move { run_member_loop(self, addr).await });
        }
    }
}

pub struct NetworkEngine {
    inner: Arc<SessionInner>,
    #[allow(dead_code)]
    rt: tokio::runtime::Runtime,
}

impl NetworkEngine {
    pub fn start(device_name: String) -> Result<Self, String> {
        Self::start_with(device_name, true)
    }

    pub fn start_with(device_name: String, enable_audio: bool) -> Result<Self, String> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let device_id = Uuid::new_v4().to_string();
        let addr: IpAddr = detect_local_ip().unwrap_or(IpAddr::V4("127.0.0.1".parse().unwrap()));
        let inner = SessionInner::new(rt.handle().clone(), device_id.clone(), device_name);
        inner.set_addr_port(addr, 0);

        let listener = rt
            .block_on(TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 0))))
            .map_err(|e| format!("no se pudo abrir puerto de control: {e}"))?;
        let port = listener.local_addr().map_err(|e| e.to_string())?.port();
        inner.set_addr_port(addr, port);

        let media = Arc::new(MediaEngine::new(enable_audio)?);
        inner.set_media(media);

        let daemon = ServiceDaemon::new().map_err(|e| format!("mDNS: {e}"))?;
        let short = &device_id[..8];
        inner.setup_mdns(daemon.clone(), format!("weft-{short}.local."), addr, port);

        let inner_server = inner.clone();
        rt.spawn(async move { run_server(inner_server, listener).await });

        let inner_mdns = inner.clone();
        std::thread::spawn(move || run_mdns(inner_mdns));

        Ok(Self { inner, rt })
    }

    pub fn stop(&self) {
        self.inner.stop();
    }

    pub fn status(&self) -> NetworkStatus {
        self.inner.status()
    }

    pub fn media_stats(&self) -> Option<MediaStats> {
        self.inner.media_stats()
    }

    pub fn set_event_callback(&self, cb: Box<dyn Fn(NetworkEvent) + Send + Sync>) {
        self.inner.set_event_callback(cb);
    }

    pub fn request_transmit(&self) {
        self.inner.request_transmit();
    }

    pub fn release_transmit(&self) {
        self.inner.release_transmit();
    }

    pub fn approve_transmit(&self, device_id: &str) {
        self.inner.approve_transmit(device_id);
    }

    pub fn deny_transmit(&self, device_id: &str) {
        self.inner.deny_transmit(device_id);
    }

    pub fn transmit_file(&self, path: &str) -> Result<(), String> {
        self.inner.transmit_file(path)
    }

    pub fn start_capture(&self, device_name: Option<&str>) -> Result<(), String> {
        self.inner.start_capture(device_name)
    }

    pub fn stop_capture(&self) {
        self.inner.stop_capture();
    }
}

impl SessionInner {
    pub fn stop(&self) {
        {
            let mut s = self.lock();
            s.stopped = true;
            s.role = Role::Bootstrap;
            s.coordinator_id.clear();
            s.transmitter_id = None;
            s.members.clear();
            s.pending_requests.clear();
            s.conns.clear();
        }
        if let Some(advert) = self.mdns.lock().unwrap().as_ref() {
            let _ = advert.daemon.shutdown();
        }
        if let Some(engine) = self.media() {
            engine.leave_session();
        }
    }

    // ---- coordinator-side helpers ----

    fn session_ids(&self) -> (String, String) {
        let s = self.lock();
        (s.session_id.clone(), s.coordinator_id.clone())
    }

    fn member_wires(&self) -> Vec<crate::network::control::MemberWire> {
        self.lock()
            .members
            .values()
            .map(|m| crate::network::control::MemberWire {
                device_id: m.device_id.clone(),
                device_name: m.device_name.clone(),
                addr: m.addr.clone(),
                port: m.port,
                media_port: m.media_port,
            })
            .collect()
    }

    fn session_tag(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.lock().session_id.hash(&mut hasher);
        hasher.finish()
    }

    /// Build the media member list (all session members except ourselves) and
    /// push it into the media engine.
    fn refresh_media_members(&self) {
        if let Some(engine) = self.media() {
            let members: Vec<MemberMedia> = {
                let s = self.lock();
                let my_id = s.device_id.clone();
                s.members
                    .values()
                    .filter(|m| m.device_id != my_id)
                    .filter_map(|m| {
                        let ip: IpAddr = m.addr.parse().ok()?;
                        Some(MemberMedia {
                            device_id: m.device_id.clone(),
                            media_addr: SocketAddr::new(ip, m.media_port),
                        })
                    })
                    .collect()
            };
            engine.update_members(members);
        }
    }

    fn configure_media_session(&self) {
        if let Some(engine) = self.media() {
            let is_coordinator = self.is_coordinator();
            let tag = self.session_tag();
            engine.set_session(tag, is_coordinator);
        }
    }

    fn refresh_transmitter_media(&self) {
        if let Some(engine) = self.media() {
            let (my_id, tx_id) = {
                let s = self.lock();
                (s.device_id.clone(), s.transmitter_id.clone())
            };
            engine.set_transmitter(tx_id.as_deref() == Some(my_id.as_str()));
        }
    }

    fn register_conn(&self, device_id: String, tx: tokio::sync::mpsc::UnboundedSender<WsMessage>) {
        self.lock().conns.insert(device_id, tx);
    }

    fn remove_conn(&self, device_id: &str) {
        let was_transmitter = {
            let mut s = self.lock();
            s.conns.remove(device_id);
            let was = s.transmitter_id.as_ref() == Some(&device_id.to_string());
            if was {
                s.transmitter_id = None;
            }
            if let Some(pos) = s.pending_requests.iter().position(|d| d == device_id) {
                s.pending_requests.remove(pos);
            }
            s.members.remove(device_id);
            was
        };
        self.broadcast(&S2C::MemberLeave { device_id: device_id.to_string() });
        self.broadcast_members();
        self.refresh_media_members();
        if was_transmitter {
            self.broadcast(&S2C::TransmitRevoked { device_id: device_id.to_string() });
            self.grant_next_pending();
            self.refresh_transmitter_media();
            self.emit("transmit_revoked", device_id, "", "transmisor desconectado");
        }
    }

    fn add_member(&self, device_id: &str, device_name: &str, addr: String, port: u16, media_port: u16) {
        {
            let mut s = self.lock();
            s.members.insert(
                device_id.to_string(),
                MemberInfo {
                    device_id: device_id.to_string(),
                    device_name: device_name.to_string(),
                    addr,
                    port,
                    media_port,
                },
            );
        }
        self.broadcast_members();
        self.refresh_media_members();
        self.emit("member_joined", device_id, device_name, "se unió a la sesión");
    }

    fn broadcast_members(&self) {
        self.broadcast(&S2C::Members { members: self.member_wires() });
    }

    fn broadcast(&self, msg: &S2C) {
        let payload = serde_json::to_string(msg).unwrap_or_default();
        let conns = {
            let s = self.lock();
            s.conns.values().cloned().collect::<Vec<_>>()
        };
        for tx in conns {
            let _ = tx.send(WsMessage::Text(payload.clone()));
        }
    }

    fn grant_next_pending(&self) {
        let next = {
            let mut s = self.lock();
            if s.transmitter_id.is_some() {
                None
            } else if let Some(id) = s.pending_requests.first().cloned() {
                s.transmitter_id = Some(id.clone());
                s.pending_requests.remove(0);
                Some(id)
            } else {
                None
            }
        };
        if let Some(id) = next {
            self.broadcast(&S2C::TransmitGranted { device_id: id.clone() });
            self.emit("transmit_granted", &id, "", "tiene el token de transmisión");
        }
        self.refresh_transmitter_media();
    }

    // ---- transmit token ----

    pub fn request_transmit(&self) {
        let id = self.my_id();
        if self.is_coordinator() {
            let already = {
                let mut s = self.lock();
                if s.transmitter_id.is_none() {
                    s.transmitter_id = Some(id.clone());
                    true
                } else if !s.pending_requests.contains(&id) {
                    s.pending_requests.push(id.clone());
                    false
                } else {
                    false
                }
            };
            if already {
                self.broadcast(&S2C::TransmitGranted { device_id: id });
                self.emit("transmit_granted", &self.my_id(), &self.my_name(), "transmito");
            } else {
                self.emit("transmit_requested", &self.my_id(), &self.my_name(), "en espera");
            }
            self.refresh_transmitter_media();
        } else {
            self.send_to_coordinator(&C2S::RequestTransmit { device_id: id });
        }
    }

    pub fn release_transmit(&self) {
        let id = self.my_id();
        if self.is_coordinator() {
            {
                let mut s = self.lock();
                if s.transmitter_id.as_deref() == Some(id.as_str()) {
                    s.transmitter_id = None;
                }
            }
            self.broadcast(&S2C::TransmitRevoked { device_id: id.clone() });
            self.emit("transmit_revoked", &id, &self.my_name(), "liberé la transmisión");
            self.grant_next_pending();
        } else {
            self.send_to_coordinator(&C2S::ReleaseTransmit { device_id: id });
        }
        self.refresh_transmitter_media();
    }

    pub fn approve_transmit(&self, device_id: &str) {
        if !self.is_coordinator() {
            return;
        }
        let found = {
            let mut s = self.lock();
            let found = s.pending_requests.iter().any(|d| d == device_id);
            if found && s.transmitter_id.is_none() {
                s.transmitter_id = Some(device_id.to_string());
                s.pending_requests.retain(|d| d != device_id);
            }
            found
        };
        if found {
            self.broadcast(&S2C::TransmitGranted { device_id: device_id.to_string() });
            self.emit("transmit_granted", device_id, "", "aprobado por el coordinador");
        }
        self.refresh_transmitter_media();
    }

    pub fn deny_transmit(&self, device_id: &str) {
        if !self.is_coordinator() {
            return;
        }
        {
            let mut s = self.lock();
            s.pending_requests.retain(|d| d != device_id);
        }
        self.broadcast(&S2C::TransmitDenied { device_id: device_id.to_string() });
        self.emit("transmit_denied", device_id, "", "solicitud rechazada");
    }

    pub fn transmit_file(&self, path: &str) -> Result<(), String> {
        let has_token = {
            let s = self.lock();
            s.transmitter_id.as_deref() == Some(s.device_id.as_str())
        };
        if !has_token {
            return Err("no tengo el token de transmisión".to_string());
        }
        let Some(engine) = self.media() else {
            return Err("media no disponible".to_string());
        };
        engine.transmit_file(path)?;
        let name = self.my_name();
        self.emit("transmit_started", &self.my_id(), &name, path);
        Ok(())
    }

    /// Start optional microphone capture and stream it to all members.
    /// Requires the transmit token and an active session.
    pub fn start_capture(&self, device_name: Option<&str>) -> Result<(), String> {
        let has_token = {
            let s = self.lock();
            s.transmitter_id.as_deref() == Some(s.device_id.as_str())
        };
        if !has_token {
            return Err("no tengo el token de transmisión".to_string());
        }
        let Some(engine) = self.media() else {
            return Err("media no disponible".to_string());
        };
        engine.start_capture(device_name)
    }

    /// Stop optional microphone capture. No-op if not capturing.
    pub fn stop_capture(&self) {
        if let Some(engine) = self.media() {
            engine.stop_capture();
        }
    }

    // ---- coordinator incoming messages ----

    fn on_c2s(&self, msg: C2S) {
        match msg {
            C2S::Hello { .. } => {
                let (addr, port) = {
                    let s = self.lock();
                    (s.coordinator_addr.map(|a| a.to_string()).unwrap_or_default(), s.port)
                };
                let _ = (addr, port);
                // caller (handle_incoming) registers conn and calls add_member
            }
            C2S::RequestTransmit { device_id } => {
                let (free, queued) = {
                    let mut s = self.lock();
                    if s.transmitter_id.is_none() {
                        s.transmitter_id = Some(device_id.clone());
                        (true, false)
                    } else if s.pending_requests.contains(&device_id) {
                        (false, true)
                    } else {
                        s.pending_requests.push(device_id.clone());
                        (false, false)
                    }
                };
                if free {
                    self.broadcast(&S2C::TransmitGranted { device_id: device_id.clone() });
                    let name = self
                        .lock()
                        .members
                        .get(&device_id)
                        .map(|m| m.device_name.clone())
                        .unwrap_or_default();
                    self.emit("transmit_granted", &device_id, &name, "tiene el token");
                } else if queued {
                    self.emit("transmit_requested", &device_id, "", "ya estaba en espera");
                } else {
                    let name = self
                        .lock()
                        .members
                        .get(&device_id)
                        .map(|m| m.device_name.clone())
                        .unwrap_or_default();
                    self.broadcast(&S2C::TransmitRequested { device_id: device_id.clone(), device_name: name.clone() });
                    self.emit("transmit_requested", &device_id, &name, "pide el token");
                }
                self.refresh_transmitter_media();
            }
            C2S::ReleaseTransmit { device_id } => {
                let was = {
                    let mut s = self.lock();
                    let was = s.transmitter_id.as_deref() == Some(device_id.as_str());
                    if was {
                        s.transmitter_id = None;
                    }
                    was
                };
                if was {
                    self.broadcast(&S2C::TransmitRevoked { device_id: device_id.clone() });
                    let name = self
                        .lock()
                        .members
                        .get(&device_id)
                        .map(|m| m.device_name.clone())
                        .unwrap_or_default();
                    self.emit("transmit_revoked", &device_id, &name, "liberó la transmisión");
                    self.grant_next_pending();
                }
                self.refresh_transmitter_media();
            }
            C2S::ClockQuery { .. } => {
                // Handled directly in handle_incoming (needs the conn tx).
            }
            C2S::Leave => {}
        }
    }

    // ---- member-side ----

    fn set_coordinator_tx(&self, tx: tokio::sync::mpsc::UnboundedSender<WsMessage>) {
        self.lock().coordinator_tx = Some(tx);
    }

    fn clear_coordinator_tx(&self) {
        self.lock().coordinator_tx = None;
    }

    fn send_to_coordinator(&self, msg: &C2S) {
        let tx = self.lock().coordinator_tx.clone();
        if let Some(tx) = tx {
            if let Ok(payload) = serde_json::to_string(msg) {
                let _ = tx.send(WsMessage::Text(payload));
            }
        }
    }

    fn apply_s2c(&self, msg: S2C) {
        match msg {
            S2C::Welcome { session_id, coordinator_id } => {
                {
                    let mut s = self.lock();
                    s.session_id = session_id.clone();
                    if !coordinator_id.is_empty() {
                        s.coordinator_id = coordinator_id.clone();
                    }
                }
                self.configure_media_session();
                self.refresh_media_members();
                self.emit("session_joined", &self.my_id(), &self.my_name(), "unido a la sesión");
            }
            S2C::Members { members } => {
                let mut map = BTreeMap::new();
                for m in members {
                    map.insert(
                        m.device_id.clone(),
                        MemberInfo {
                            device_id: m.device_id,
                            device_name: m.device_name,
                            addr: m.addr,
                            port: m.port,
                            media_port: m.media_port,
                        },
                    );
                }
                self.lock().members = map;
                self.refresh_media_members();
            }
            S2C::MemberJoin { member } => {
                self.lock().members.insert(
                    member.device_id.clone(),
                    MemberInfo {
                        device_id: member.device_id.clone(),
                        device_name: member.device_name.clone(),
                        addr: member.addr,
                        port: member.port,
                        media_port: member.media_port,
                    },
                );
                self.refresh_media_members();
                self.emit("member_joined", &member.device_id, &member.device_name, "se unió a la sesión");
            }
            S2C::MemberLeave { device_id } => {
                self.lock().members.remove(&device_id);
                self.refresh_media_members();
            }
            S2C::TransmitGranted { device_id } => {
                {
                    let mut s = self.lock();
                    s.transmitter_id = Some(device_id.clone());
                }
                self.refresh_transmitter_media();
                let name = self.lock().members.get(&device_id).map(|m| m.device_name.clone()).unwrap_or_default();
                self.emit("transmit_granted", &device_id, &name, "tiene el token");
            }
            S2C::TransmitRevoked { device_id } => {
                {
                    let mut s = self.lock();
                    if s.transmitter_id.as_deref() == Some(device_id.as_str()) {
                        s.transmitter_id = None;
                    }
                }
                self.refresh_transmitter_media();
                self.emit("transmit_revoked", &device_id, "", "se liberó la transmisión");
            }
            S2C::TransmitRequested { device_id, device_name } => {
                self.emit("transmit_requested", &device_id, &device_name, "pide el token");
            }
            S2C::TransmitDenied { device_id } => {
                self.refresh_transmitter_media();
                self.emit("transmit_denied", &device_id, "", "solicitud rechazada");
            }
            S2C::ClockReply { query_sent_us, query_received_us, reply_sent_us } => {
                if let Some(engine) = self.media() {
                    let t3 = engine.now_local_us();
                    engine.process_ntp(
                        query_sent_us as u128,
                        t3,
                        query_received_us as u128,
                        reply_sent_us as u128,
                    );
                }
            }
        }
    }
}

fn detect_local_ip() -> Option<IpAddr> {
    local_ip_address::local_ip().ok().map(IpAddr::from)
}

// ---- async tasks ----

async fn run_server(inner: Arc<SessionInner>, listener: TcpListener) {
    while !inner.is_stopped() {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_incoming(inner.clone(), stream));
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn handle_incoming(inner: Arc<SessionInner>, stream: TcpStream) {
    let peer_ip: Option<String> = stream.peer_addr().ok().map(|a| a.ip().to_string());
    let ws = match accept_async(stream).await {
        Ok(w) => w,
        Err(_) => return,
    };
    let (mut ws_write, mut ws_read) = ws.split();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsMessage>();
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_write.send(msg).await.is_err() {
                break;
            }
        }
    });
    let mut my_id: Option<String> = None;
    while let Some(msg) = ws_read.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(_) => break,
        };
        match msg {
            WsMessage::Text(text) => {
                let Ok(c2s) = serde_json::from_str::<C2S>(&text) else {
                    continue;
                };
                match c2s {
                    C2S::Hello { device_id, device_name, media_port } => {
                        if !inner.is_coordinator() {
                            break;
                        }
                        let (session_id, coordinator_id) = inner.session_ids();
                        let my_port = inner.lock().port;
                        let addr = peer_ip.clone().unwrap_or_default();
                        inner.register_conn(device_id.clone(), tx.clone());
                        inner.add_member(&device_id, &device_name, addr, my_port, media_port);
                        let _ = tx.send(WsMessage::Text(
                            serde_json::to_string(&S2C::Welcome { session_id, coordinator_id }).unwrap(),
                        ));
                        my_id = Some(device_id);
                    }
                    C2S::ClockQuery { query_sent_us } => {
                        let reply_sent = inner
                            .media()
                            .map(|m| m.now_session_us())
                            .unwrap_or(query_sent_us as u128) as u64;
                        let reply = S2C::ClockReply {
                            query_sent_us,
                            query_received_us: reply_sent,
                            reply_sent_us: reply_sent,
                        };
                        if let Ok(payload) = serde_json::to_string(&reply) {
                            let _ = tx.send(WsMessage::Text(payload));
                        }
                    }
                    other => inner.on_c2s(other),
                }
            }
            _ => {}
        }
    }
    writer.abort();
    if let Some(id) = my_id {
        inner.remove_conn(&id);
    }
}

async fn run_member_loop(inner: Arc<SessionInner>, coord: SocketAddr) {
    let start = Instant::now();
    loop {
        if inner.is_stopped() {
            break;
        }
        {
            let s = inner.lock();
            if s.role != Role::Member || s.coordinator_addr != Some(coord) {
                break;
            }
        }
        let connected = match connect_async(format!("ws://{coord}")).await {
            Ok((ws, _)) => {
                let (mut ws_write, mut ws_read) = ws.split();
                let media_port = inner.lock().media_port;
                let hello = C2S::Hello {
                    device_id: inner.my_id(),
                    device_name: inner.my_name(),
                    media_port,
                };
                if let Ok(payload) = serde_json::to_string(&hello) {
                    let _ = ws_write.send(WsMessage::Text(payload)).await;
                }
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsMessage>();
                inner.set_coordinator_tx(tx);
                let writer = tokio::spawn(async move {
                    while let Some(msg) = rx.recv().await {
                        if ws_write.send(msg).await.is_err() {
                            break;
                        }
                    }
                });
                let mut ntp = tokio::time::interval(Duration::from_secs(2));
                ntp.tick().await;
                loop {
                    tokio::select! {
                        _ = ntp.tick() => {
                            if let Some(engine) = inner.media() {
                                let t0 = engine.now_local_us();
                                inner.send_to_coordinator(&C2S::ClockQuery { query_sent_us: t0 as u64 });
                            }
                        }
                        msg = ws_read.next() => {
                            match msg {
                                Some(Ok(WsMessage::Text(t))) => {
                                    if let Ok(s2c) = serde_json::from_str::<S2C>(&t) {
                                        inner.apply_s2c(s2c);
                                    }
                                }
                                Some(Ok(WsMessage::Close(_))) | None | Some(Err(_)) => break,
                                _ => {}
                            }
                        }
                    }
                }
                writer.abort();
                inner.clear_coordinator_tx();
                true
            }
            Err(_) => false,
        };
        {
            let mut s = inner.lock();
            if s.connecting_to == Some(coord) {
                s.connecting_to = None;
            }
        }
        if !connected && start.elapsed() > Duration::from_secs(8) {
            inner.promote_to_coordinator();
            break;
        }
        if inner.is_stopped() {
            break;
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
    {
        let mut s = inner.lock();
        if s.connecting_to == Some(coord) {
            s.connecting_to = None;
        }
    }
}

fn run_mdns(inner: Arc<SessionInner>) {
    let daemon = inner.mdns_daemon();
    inner.re_advertise();
    let Ok(browse) = daemon.browse(SERVICE_TYPE) else {
        return;
    };
    let deadline = Instant::now() + BOOTSTRAP_TIMEOUT;
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            inner.bootstrap_elapsed();
            break;
        }
        match browse.recv_timeout(remain) {
            Ok(event) => handle_mdns_event(&inner, event),
            Err(flume::RecvTimeoutError::Timeout) => continue,
            Err(_) => return,
        }
    }
    while !inner.is_stopped() {
        match browse.recv_timeout(Duration::from_millis(500)) {
            Ok(event) => handle_mdns_event(&inner, event),
            Err(flume::RecvTimeoutError::Timeout) => continue,
            Err(_) => return,
        }
    }
}

fn handle_mdns_event(inner: &Arc<SessionInner>, event: ServiceEvent) {
    match event {
        ServiceEvent::ServiceResolved(info) => {
            inner.clone().on_peer_resolved(&info);
        }
        ServiceEvent::ServiceRemoved(_ty, fullname) => inner.on_peer_removed(&fullname),
        _ => {}
    }
}
