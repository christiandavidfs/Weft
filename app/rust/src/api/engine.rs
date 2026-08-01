use weft_core::config::Config;
use weft_core::engine::SyncEngine;
use std::sync::LazyLock;
use std::sync::Mutex;

static ENGINE: LazyLock<Mutex<SyncEngine>> =
    LazyLock::new(|| Mutex::new(SyncEngine::new(Config::default())));

#[derive(Debug, Clone)]
pub struct SessionStatusView {
    pub active: bool,
    pub target_latency_ms: u64,
    pub session_id: Option<u64>,
    pub elapsed_us: Option<u64>,
}

#[flutter_rust_bridge::frb(sync)]
pub fn bridge_version() -> String {
    weft_core::version()
}

#[flutter_rust_bridge::frb(sync)]
pub fn engine_start_session() -> u64 {
    ENGINE.lock().unwrap().start_session()
}

#[flutter_rust_bridge::frb(sync)]
pub fn engine_stop_session() {
    ENGINE.lock().unwrap().stop_session();
}

#[flutter_rust_bridge::frb(sync)]
pub fn engine_status() -> SessionStatusView {
    let status = ENGINE.lock().unwrap().status();
    SessionStatusView {
        active: status.active,
        target_latency_ms: status.config_target_latency_ms,
        session_id: status.clock.as_ref().map(|c| c.session_id),
        elapsed_us: status.clock.as_ref().map(|c| c.elapsed_us as u64),
    }
}
