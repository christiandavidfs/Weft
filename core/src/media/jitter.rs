use std::collections::BTreeMap;

use crate::media::packet::AudioPacket;

#[derive(Debug, Clone, Default)]
pub struct JitterStats {
    pub buffered_packets: usize,
    pub buffered_us: u64,
    pub lost_packets: u64,
    pub late_packets: u64,
    pub played_packets: u64,
}

/// Orders incoming packets by sequence number and releases them when their
/// playback timestamp (`pts_us`, in session time) is due.
///
/// Out-of-order packets are re-ordered. Missing sequences whose deadline has
/// passed are counted as lost and skipped.
pub struct JitterBuffer {
    max_buffered_us: u128,
    packets: BTreeMap<u32, AudioPacket>,
    next_seq: Option<u32>,
    stats: JitterStats,
}

impl JitterBuffer {
    pub fn new(max_buffered_us: u128) -> Self {
        Self {
            max_buffered_us,
            packets: BTreeMap::new(),
            next_seq: None,
            stats: JitterStats::default(),
        }
    }

    fn buffered_us(&self) -> u128 {
        self.packets
            .values()
            .fold(0u128, |acc, p| acc + p.duration_us() as u128)
    }

    /// Insert a packet. Returns `true` if it was accepted, `false` if dropped.
    pub fn push(&mut self, pkt: AudioPacket, now_session_us: u128) -> bool {
        if pkt.is_empty() {
            return false;
        }
        let pts = pkt.pts_us as u128;
        let grace = pkt.duration_us() as u128;
        if pts.saturating_add(grace).saturating_add(5_000) < now_session_us {
            self.stats.late_packets += 1;
            return false;
        }
        self.packets.insert(pkt.seq, pkt);
        while self.buffered_us() > self.max_buffered_us {
            if let Some((&seq, _)) = self.packets.iter().next() {
                self.packets.remove(&seq);
                self.stats.late_packets += 1;
            } else {
                break;
            }
        }
        true
    }

    /// Pop all packets that are due (in order) as of `now_session_us`.
    pub fn pop_ready(&mut self, now_session_us: u128) -> Vec<AudioPacket> {
        let mut out = Vec::new();
        loop {
            let (seq, playable) = {
                let Some((&seq, pkt)) = self.packets.iter().next() else {
                    break;
                };
                if (pkt.pts_us as u128) > now_session_us {
                    break;
                }
                let playable = match self.next_seq {
                    None => true,
                    Some(next) => {
                        if seq == next {
                            true
                        } else if seq > next {
                            let missing = (seq - next) as u64;
                            let frame_us = pkt.duration_us() as u128;
                            let missing_span_us = missing as u128 * frame_us;
                        let missing_pts = (pkt.pts_us as u128).saturating_sub(missing_span_us);
                        let late_by = now_session_us.saturating_sub(missing_pts);
                        if late_by >= frame_us {
                            self.stats.lost_packets += missing;
                            true
                        } else {
                            false
                        }
                        } else {
                            false
                        }
                    }
                };
                (seq, playable)
            };
            if !playable {
                break;
            }
            let pkt = self.packets.remove(&seq).expect("seq present");
            self.next_seq = Some(seq + 1);
            self.stats.played_packets += 1;
            out.push(pkt);
        }
        self.stats.buffered_packets = self.packets.len();
        self.stats.buffered_us = self.buffered_us() as u64;
        out
    }

    pub fn peek_front(&self) -> Option<&AudioPacket> {
        self.packets.iter().next().map(|(_, p)| p)
    }

    pub fn stats(&self) -> JitterStats {
        JitterStats {
            buffered_us: self.buffered_us() as u64,
            ..self.stats
        }
    }

    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::{CHANNELS, FRAME_SAMPLES, SAMPLE_RATE};

    fn pkt(seq: u32, pts_us: u64) -> AudioPacket {
        AudioPacket::new(1, seq, pts_us, vec![0i16; FRAME_SAMPLES * CHANNELS as usize], SAMPLE_RATE, CHANNELS)
    }

    #[test]
    fn reorders_and_plays_in_order() {
        let mut jb = JitterBuffer::new(200_000);
        // Arrive out of order.
        jb.push(pkt(1, 20_000), 0);
        jb.push(pkt(0, 0), 0);
        jb.push(pkt(2, 40_000), 0);
        // seq0 due now.
        let ready = jb.pop_ready(0);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].seq, 0);
        // seq1 due at 20ms.
        let ready = jb.pop_ready(20_000);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].seq, 1);
        let ready = jb.pop_ready(40_000);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].seq, 2);
    }

    #[test]
    fn counts_lost_on_gap() {
        let mut jb = JitterBuffer::new(200_000);
        jb.push(pkt(0, 0), 0);
        assert_eq!(jb.pop_ready(0).len(), 1);
        // seq1 missing, seq2 arrives with a big pts gap -> seq1 declared lost.
        jb.push(pkt(2, 60_000), 0);
        let ready = jb.pop_ready(60_000);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].seq, 2);
        assert_eq!(jb.stats().lost_packets, 1);
    }

    #[test]
    fn drops_late_packets() {
        let mut jb = JitterBuffer::new(200_000);
        // A packet whose play time is far in the past is rejected.
        assert!(!jb.push(pkt(0, 0), 500_000));
        assert_eq!(jb.stats().late_packets, 1);
        // Bounded by max buffered: overflow drops the oldest.
        assert!(jb.push(pkt(1, 0), 0));
        for seq in 2..=20 {
            assert!(jb.push(pkt(seq, 0), 0));
        }
        assert!(jb.buffered_us() <= 200_000);
    }
}
