use std::time::Instant;

use crate::clock::SessionClock;

/// NTP-lite clock offset estimator.
///
/// Session time is defined by the coordinator's `SessionClock`. Every other
/// device estimates `session_time = local_time + offset` by exchanging
/// round-trip timestamps with the coordinator over the control plane.
///
/// NTP offset formula (with symmetric network delay assumption):
///   offset = (S1 + S2 - L0 - L3) / 2
/// where L0 = client send (local), S1 = server receive (session),
/// S2 = server send (session), L3 = client receive (local).
#[derive(Debug, Clone)]
pub struct ClockOffset {
    offset_us: f64,
    rtt_us: u128,
    last_sync_at: Option<Instant>,
    syncs: u32,
}

impl Default for ClockOffset {
    fn default() -> Self {
        Self::new()
    }
}

impl ClockOffset {
    pub fn new() -> Self {
        Self {
            offset_us: 0.0,
            rtt_us: 0,
            last_sync_at: None,
            syncs: 0,
        }
    }

    pub fn process_ntp(
        &mut self,
        query_sent_local_us: u128,
        reply_received_local_us: u128,
        server_received_us: u128,
        server_sent_us: u128,
    ) {
        let rtt = (reply_received_local_us.saturating_sub(query_sent_local_us))
            .saturating_sub(server_sent_us.saturating_sub(server_received_us));
        if rtt > 50_000 {
            return;
        }
        let offset = ((server_received_us as i128) + (server_sent_us as i128)
            - (query_sent_local_us as i128)
            - (reply_received_local_us as i128)) as f64
            / 2.0;
        if self.syncs == 0 {
            self.offset_us = offset;
        } else {
            self.offset_us += (offset - self.offset_us) * 0.25;
        }
        self.rtt_us = rtt;
        self.syncs += 1;
        self.last_sync_at = Some(Instant::now());
    }

    pub fn is_synced(&self) -> bool {
        self.syncs > 0
    }

    pub fn offset_us(&self) -> f64 {
        self.offset_us
    }

    pub fn rtt_us(&self) -> u128 {
        self.rtt_us
    }

    pub fn syncs(&self) -> u32 {
        self.syncs
    }

    pub fn to_session_us(&self, local_us: u128) -> u128 {
        (local_us as f64 + self.offset_us).max(0.0) as u128
    }
}

/// Combines a local monotonic `SessionClock` with a `ClockOffset` to map local
/// time onto the shared session timeline.
pub struct MediaClock {
    local: SessionClock,
    offset: ClockOffset,
}

impl MediaClock {
    pub fn new(session_id: u64) -> Self {
        Self {
            local: SessionClock::new(session_id),
            offset: ClockOffset::new(),
        }
    }

    pub fn session_id(&self) -> u64 {
        self.local.session_id()
    }

    pub fn set_session_id(&mut self, session_id: u64) {
        self.local.set_session_id(session_id);
    }

    /// Make this clock the session reference (offset reset to zero).
    pub fn make_reference(&mut self) {
        self.offset = ClockOffset::new();
    }

    pub fn now_local_us(&self) -> u128 {
        self.local.now_us()
    }

    pub fn now_session_us(&self) -> u128 {
        self.offset.to_session_us(self.local.now_us())
    }

    pub fn process_ntp(
        &mut self,
        query_sent_local_us: u128,
        reply_received_local_us: u128,
        server_received_us: u128,
        server_sent_us: u128,
    ) {
        self.offset
            .process_ntp(query_sent_local_us, reply_received_local_us, server_received_us, server_sent_us);
    }

    pub fn clock_offset(&self) -> &ClockOffset {
        &self.offset
    }

    pub fn offset_us(&self) -> f64 {
        self.offset.offset_us()
    }

    pub fn rtt_us(&self) -> u128 {
        self.offset.rtt_us()
    }

    pub fn is_synced(&self) -> bool {
        self.offset.is_synced()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ntp_offset_symmetric() {
        // Server clock is ahead of client clock by exactly 5_000_000us (5s).
        let mut offset = ClockOffset::new();
        let query_sent_local_us = 1_000_000;
        let one_way_us = 2_000;
        let server_received_us = query_sent_local_us + 5_000_000 + one_way_us;
        let server_sent_us = server_received_us + 500;
        let reply_received_local_us = server_sent_us - 5_000_000 + one_way_us;
        offset.process_ntp(
            query_sent_local_us,
            reply_received_local_us,
            server_received_us,
            server_sent_us,
        );
        assert!(offset.is_synced());
        assert!((offset.offset_us() - 5_000_000.0).abs() < 100.0, "offset {}", offset.offset_us());
        assert_eq!(offset.rtt_us(), 4000);
    }

    #[test]
    fn rejects_high_rtt() {
        let mut offset = ClockOffset::new();
        offset.process_ntp(0, 100_000_000, 1_000, 2_000);
        assert!(!offset.is_synced());
    }
}
