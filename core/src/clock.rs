use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockState {
    pub session_id: u64,
    pub elapsed_us: u128,
}

pub struct SessionClock {
    session_id: u64,
    start: Instant,
}

impl SessionClock {
    pub fn new(session_id: u64) -> Self {
        Self {
            session_id,
            start: Instant::now(),
        }
    }

    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn now_us(&self) -> u128 {
        self.start.elapsed().as_micros()
    }

    pub fn state(&self) -> ClockState {
        ClockState {
            session_id: self.session_id,
            elapsed_us: self.now_us(),
        }
    }
}
