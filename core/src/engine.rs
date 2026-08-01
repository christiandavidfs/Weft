use crate::clock::{ClockState, SessionClock};
use crate::config::Config;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

pub struct SyncEngine {
    pub config: Config,
    clock: Option<SessionClock>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStatus {
    pub active: bool,
    pub config_target_latency_ms: u64,
    pub clock: Option<ClockState>,
}

impl SyncEngine {
    pub fn new(config: Config) -> Self {
        Self { config, clock: None }
    }

    pub fn start_session(&mut self) -> u64 {
        let id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
        self.clock = Some(SessionClock::new(id));
        id
    }

    pub fn stop_session(&mut self) {
        self.clock = None;
    }

    pub fn status(&self) -> SessionStatus {
        SessionStatus {
            active: self.clock.is_some(),
            config_target_latency_ms: self.config.target_latency_ms,
            clock: self.clock.as_ref().map(|c| c.state()),
        }
    }
}
