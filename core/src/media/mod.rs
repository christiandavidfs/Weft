pub mod clock;
pub mod engine;
pub mod jitter;
pub mod packet;
pub mod sink;
pub mod source;
pub mod transport;

pub const SAMPLE_RATE: u32 = 48_000;
pub const CHANNELS: u16 = 2;
pub const FRAME_MS: u64 = 20;
pub const FRAME_SAMPLES: usize = ((SAMPLE_RATE as u64) * FRAME_MS / 1000) as usize;
pub const FRAME_US: u64 = FRAME_MS * 1000;

pub use clock::{ClockOffset, MediaClock};
pub use engine::{MediaEngine, MediaStats, MemberMedia};
pub use jitter::{JitterBuffer, JitterStats};
pub use packet::{decode_packet, encode_packet, AudioPacket};
pub use source::{decode_file_to_pcm, PacketizedSource};
