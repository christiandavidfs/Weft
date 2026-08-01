use serde::{Deserialize, Serialize};

use crate::media::{CHANNELS, SAMPLE_RATE};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioPacket {
    pub session_id: u64,
    pub seq: u32,
    pub pts_us: u64,
    pub sample_rate: u32,
    pub channels: u16,
    pub samples: Vec<i16>,
}

impl AudioPacket {
    pub fn new(
        session_id: u64,
        seq: u32,
        pts_us: u64,
        samples: Vec<i16>,
        sample_rate: u32,
        channels: u16,
    ) -> Self {
        Self {
            session_id,
            seq,
            pts_us,
            sample_rate,
            channels,
            samples,
        }
    }

    pub fn frames(&self) -> usize {
        if self.channels == 0 {
            return 0;
        }
        self.samples.len() / self.channels as usize
    }

    pub fn duration_us(&self) -> u64 {
        if self.sample_rate == 0 {
            return 0;
        }
        (self.frames() as u64) * 1_000_000 / (self.sample_rate as u64)
    }

    pub fn is_standard(&self) -> bool {
        self.sample_rate == SAMPLE_RATE && self.channels == CHANNELS
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

pub fn encode_packet(pkt: &AudioPacket) -> Result<Vec<u8>, String> {
    bincode::serialize(pkt).map_err(|e| e.to_string())
}

pub fn decode_packet(data: &[u8]) -> Result<AudioPacket, String> {
    bincode::deserialize(data).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::FRAME_SAMPLES;

    #[test]
    fn frame_helpers() {
        let pkt = AudioPacket::new(1, 0, 0, vec![0i16; FRAME_SAMPLES * CHANNELS as usize], SAMPLE_RATE, CHANNELS);
        assert_eq!(pkt.frames(), 960);
        assert_eq!(pkt.duration_us(), 20_000);
        assert!(pkt.is_standard());
    }

    #[test]
    fn roundtrip() {
        let samples: Vec<i16> = (0..1920).map(|i| (i as i16).wrapping_mul(3)).collect();
        let pkt = AudioPacket::new(42, 7, 123_456, samples.clone(), SAMPLE_RATE, CHANNELS);
        let bytes = encode_packet(&pkt).unwrap();
        let back = decode_packet(&bytes).unwrap();
        assert_eq!(back, pkt);
        assert_eq!(back.samples, samples);
    }
}
