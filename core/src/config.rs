pub struct Config {
    pub target_latency_ms: u64,
    pub jitter_buffer_ms: u64,
    pub session_announce_interval_ms: u64,
    pub max_devices: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            target_latency_ms: 100,
            jitter_buffer_ms: 60,
            session_announce_interval_ms: 1000,
            max_devices: 16,
        }
    }
}
