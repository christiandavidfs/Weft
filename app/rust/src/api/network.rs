use crate::frb_generated::StreamSink;
use std::sync::Mutex;
use std::sync::OnceLock;

use weft_core::network::{NetworkEngine, NetworkEvent, NetworkStatus};

static ENGINE: OnceLock<Mutex<Option<NetworkEngine>>> = OnceLock::new();
static EVENT_SINK: Mutex<Option<StreamSink<NetworkEventView>>> = Mutex::new(None);

fn engine_ref() -> &'static Mutex<Option<NetworkEngine>> {
    ENGINE.get_or_init(|| Mutex::new(None))
}

fn wire_event_callback() {
    if let Some(engine) = engine_ref().lock().unwrap().as_ref() {
        engine.set_event_callback(Box::new(|ev: NetworkEvent| {
            if let Some(sink) = EVENT_SINK.lock().unwrap().as_ref() {
                let _ = sink.add(NetworkEventView::from(ev));
            }
        }));
    }
}

#[derive(Debug, Clone)]
pub struct MemberView {
    pub device_id: String,
    pub device_name: String,
    pub addr: String,
    pub is_transmitter: bool,
    pub is_me: bool,
}

#[derive(Debug, Clone)]
pub struct PeerView {
    pub device_id: String,
    pub device_name: String,
    pub addr: String,
    pub is_coordinator: bool,
}

#[derive(Debug, Clone)]
pub struct NetworkStatusView {
    pub running: bool,
    pub device_id: String,
    pub device_name: String,
    pub role: String,
    pub session_id: String,
    pub coordinator_id: String,
    pub transmitter_id: String,
    pub members: Vec<MemberView>,
    pub peers: Vec<PeerView>,
    pub pending_transmit_requests: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NetworkEventView {
    pub kind: String,
    pub device_id: String,
    pub device_name: String,
    pub message: String,
}

impl From<NetworkStatus> for NetworkStatusView {
    fn from(s: NetworkStatus) -> Self {
        let me = s.device_id.clone();
        let transmitter_id = s.transmitter_id.clone();
        NetworkStatusView {
            running: s.running,
            device_id: s.device_id,
            device_name: s.device_name,
            role: s.role,
            session_id: s.session_id,
            coordinator_id: s.coordinator_id,
            transmitter_id: s.transmitter_id,
            members: s
                .members
                .into_iter()
                .map(|m| MemberView {
                    device_id: m.device_id.clone(),
                    device_name: m.device_name,
                    addr: m.addr,
                    is_transmitter: m.device_id == transmitter_id,
                    is_me: m.device_id == me,
                })
                .collect(),
            peers: s
                .peers
                .into_iter()
                .map(|p| PeerView {
                    device_id: p.device_id,
                    device_name: p.device_name,
                    addr: p.addr,
                    is_coordinator: p.is_coordinator,
                })
                .collect(),
            pending_transmit_requests: s.pending_transmit_requests,
        }
    }
}

impl From<NetworkEvent> for NetworkEventView {
    fn from(e: NetworkEvent) -> Self {
        NetworkEventView {
            kind: e.kind,
            device_id: e.device_id,
            device_name: e.device_name,
            message: e.message,
        }
    }
}

fn empty_status() -> NetworkStatusView {
    NetworkStatusView {
        running: false,
        device_id: String::new(),
        device_name: String::new(),
        role: "off".to_string(),
        session_id: String::new(),
        coordinator_id: String::new(),
        transmitter_id: String::new(),
        members: Vec::new(),
        peers: Vec::new(),
        pending_transmit_requests: Vec::new(),
    }
}

#[flutter_rust_bridge::frb(sync)]
pub fn network_start(device_name: String) -> Result<(), String> {
    let engine = NetworkEngine::start(device_name).map_err(|e| e.to_string())?;
    {
        let mut guard = engine_ref().lock().unwrap();
        *guard = Some(engine);
    }
    wire_event_callback();
    Ok(())
}

#[flutter_rust_bridge::frb(sync)]
pub fn network_stop() {
    if let Some(engine) = engine_ref().lock().unwrap().as_ref() {
        engine.stop();
    }
    *engine_ref().lock().unwrap() = None;
}

#[flutter_rust_bridge::frb(sync)]
pub fn network_status() -> NetworkStatusView {
    match engine_ref().lock().unwrap().as_ref() {
        Some(engine) => engine.status().into(),
        None => empty_status(),
    }
}

#[flutter_rust_bridge::frb(sync)]
pub fn network_events(event_sink: StreamSink<NetworkEventView>) {
    *EVENT_SINK.lock().unwrap() = Some(event_sink);
    wire_event_callback();
}

#[flutter_rust_bridge::frb(sync)]
pub fn network_request_transmit() {
    if let Some(engine) = engine_ref().lock().unwrap().as_ref() {
        engine.request_transmit();
    }
}

#[flutter_rust_bridge::frb(sync)]
pub fn network_release_transmit() {
    if let Some(engine) = engine_ref().lock().unwrap().as_ref() {
        engine.release_transmit();
    }
}

#[flutter_rust_bridge::frb(sync)]
pub fn network_approve_transmit(device_id: String) {
    if let Some(engine) = engine_ref().lock().unwrap().as_ref() {
        engine.approve_transmit(&device_id);
    }
}

#[flutter_rust_bridge::frb(sync)]
pub fn network_deny_transmit(device_id: String) {
    if let Some(engine) = engine_ref().lock().unwrap().as_ref() {
        engine.deny_transmit(&device_id);
    }
}
