use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};

use crate::media::clock::MediaClock;
use crate::media::jitter::JitterBuffer;
use crate::media::packet::AudioPacket;
use crate::media::{CHANNELS, SAMPLE_RATE};

/// How far (in frames) the output may drift from the session timeline before a
/// correction frame is stuffed (duplicated) or dropped. ~1ms at 48kHz.
const DRIFT_THRESHOLD_FRAMES: u64 = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriftAction {
    None,
    Stuff,
    Drop,
}

/// Keeps the amount of source consumed aligned with the session timeline,
/// compensating for device-clock drift via pulse stuffing/dropping.
pub struct DriftController {
    anchor_session_us: Option<u128>,
    anchor_consumed: u64,
    threshold: u64,
}

impl DriftController {
    pub fn new(threshold_frames: u64) -> Self {
        Self {
            anchor_session_us: None,
            anchor_consumed: 0,
            threshold: threshold_frames,
        }
    }

    pub fn is_anchored(&self) -> bool {
        self.anchor_session_us.is_some()
    }

    pub fn reset(&mut self, now_session_us: u128, consumed_frames: u64) {
        self.anchor_session_us = Some(now_session_us);
        self.anchor_consumed = consumed_frames;
    }

    pub fn tick(&mut self, now_session_us: u128, consumed_frames: u64) -> DriftAction {
        let Some(anchor) = self.anchor_session_us else {
            return DriftAction::None;
        };
        if now_session_us <= anchor {
            return DriftAction::None;
        }
        let expected = (now_session_us - anchor) * SAMPLE_RATE as u128 / 1_000_000;
        let actual = consumed_frames.saturating_sub(self.anchor_consumed) as u128;
        let delta = actual as i128 - expected as i128;
        if delta > self.threshold as i128 {
            DriftAction::Stuff
        } else if delta < -(self.threshold as i128) {
            DriftAction::Drop
        } else {
            DriftAction::None
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PlaybackStats {
    pub playing: bool,
    pub played_frames: u64,
    pub underruns: u64,
    pub stuffed: u64,
    pub dropped: u64,
    pub buffered_packets: usize,
    pub buffered_us: u64,
    pub lost_packets: u64,
    pub clock_offset_us: i64,
    pub clock_rtt_us: u128,
    pub clock_synced: bool,
}

pub struct PlaybackState {
    clock: Arc<Mutex<MediaClock>>,
    jitter: JitterBuffer,
    pending: VecDeque<AudioPacket>,
    cur: Option<AudioPacket>,
    cur_offset: usize,
    last_frame: [i16; 2],
    consumed_frames: u64,
    played_frames: u64,
    underruns: u64,
    stuffed: u64,
    dropped: u64,
    drift: DriftController,
}

impl PlaybackState {
    pub fn new(clock: Arc<Mutex<MediaClock>>, jitter: JitterBuffer) -> Self {
        Self {
            clock,
            jitter,
            pending: VecDeque::new(),
            cur: None,
            cur_offset: 0,
            last_frame: [0, 0],
            consumed_frames: 0,
            played_frames: 0,
            underruns: 0,
            stuffed: 0,
            dropped: 0,
            drift: DriftController::new(DRIFT_THRESHOLD_FRAMES),
        }
    }

    pub fn now_session_us(&self) -> u128 {
        self.clock.lock().map(|c| c.now_session_us()).unwrap_or(0)
    }

    pub fn push(&mut self, pkt: AudioPacket) -> bool {
        let now = self.now_session_us();
        self.jitter.push(pkt, now)
    }

    /// Fill an interleaved stereo f32 output buffer. This is the realtime core
    /// used by the audio callback (guarded by the engine's mutex).
    pub fn fill_float(&mut self, out: &mut [f32]) {
        let n = out.len() / 2;
        for i in 0..n {
            let now = self.now_session_us();
            match self.drift.tick(now, self.consumed_frames) {
                DriftAction::Stuff => {
                    out[i * 2] = self.last_frame[0] as f32 / 32768.0;
                    out[i * 2 + 1] = self.last_frame[1] as f32 / 32768.0;
                    self.played_frames += 1;
                    self.stuffed += 1;
                    continue;
                }
                DriftAction::Drop => {
                    let _ = self.take_frame();
                    self.dropped += 1;
                }
                DriftAction::None => {}
            }
            match self.take_frame() {
                Some(f) => {
                    if !self.drift.is_anchored() {
                        self.drift.reset(now, self.consumed_frames);
                    }
                    self.consumed_frames += 1;
                    self.played_frames += 1;
                    self.last_frame = f;
                    out[i * 2] = f[0] as f32 / 32768.0;
                    out[i * 2 + 1] = f[1] as f32 / 32768.0;
                }
                None => {
                    out[i * 2..].fill(0.0);
                    self.underruns += 1;
                    break;
                }
            }
        }
    }

    fn take_frame(&mut self) -> Option<[i16; 2]> {
        loop {
            if let Some(cur) = &self.cur {
                let frames = cur.samples.len() / CHANNELS as usize;
                if self.cur_offset < frames {
                    let s = self.cur_offset * CHANNELS as usize;
                    let f = [cur.samples[s], cur.samples[s + 1]];
                    self.cur_offset += 1;
                    if self.cur_offset >= frames {
                        self.cur = None;
                    }
                    return Some(f);
                }
                self.cur = None;
            }
            if let Some(p) = self.pending.pop_front() {
                self.cur = Some(p);
                self.cur_offset = 0;
                continue;
            }
            let now = self.now_session_us();
            let ready = self.jitter.pop_ready(now);
            if ready.is_empty() {
                return None;
            }
            self.pending.extend(ready);
        }
    }

    pub fn stats(&self) -> PlaybackStats {
        let js = self.jitter.stats();
        let (offset_us, rtt_us, synced) = {
            let c = self.clock.lock().unwrap();
            (c.offset_us() as i64, c.rtt_us(), c.is_synced())
        };
        PlaybackStats {
            playing: self.played_frames > 0,
            played_frames: self.played_frames,
            underruns: self.underruns,
            stuffed: self.stuffed,
            dropped: self.dropped,
            buffered_packets: js.buffered_packets,
            buffered_us: js.buffered_us,
            lost_packets: js.lost_packets,
            clock_offset_us: offset_us,
            clock_rtt_us: rtt_us,
            clock_synced: synced,
        }
    }
}

/// Spawns a thread that owns a cpal output stream and feeds it from a shared
/// `PlaybackState`. Packets arrive via `rx` and are drained by the audio
/// callback. Returns the thread handle.
pub fn spawn_playback(
    state: Arc<Mutex<PlaybackState>>,
    rx: flume::Receiver<AudioPacket>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let host = cpal::default_host();
        let device = match host.default_output_device() {
            Some(d) => d,
            None => {
                eprintln!("weft: no hay dispositivo de salida de audio");
                return;
            }
        };
        let supported = match device.default_output_config() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("weft: config de salida: {e}");
                return;
            }
        };
        let sample_format = supported.sample_format();
        let config: cpal::StreamConfig = supported.into();
        if config.channels != CHANNELS {
            eprintln!("weft: el dispositivo no es estéreo ({} canales)", config.channels);
            return;
        }
        let stream = match sample_format {
            cpal::SampleFormat::F32 => build_stream::<f32>(&device, &config, state.clone(), rx),
            cpal::SampleFormat::I16 => build_stream::<i16>(&device, &config, state.clone(), rx),
            cpal::SampleFormat::U16 => build_stream::<u16>(&device, &config, state.clone(), rx),
            other => {
                eprintln!("weft: formato de salida no soportado: {other:?}");
                return;
            }
        };
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("weft: no se pudo abrir el stream de audio: {e}");
                return;
            }
        };
        if let Err(e) = stream.play() {
            eprintln!("weft: no se pudo reproducir: {e}");
            return;
        }
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            thread::park_timeout(Duration::from_millis(200));
        }
        drop(stream);
    })
}

fn build_stream<T>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    state: Arc<Mutex<PlaybackState>>,
    rx: flume::Receiver<AudioPacket>,
) -> Result<cpal::Stream, String>
where
    T: cpal::SizedSample + cpal::FromSample<f32>,
{
    let err_cb = |e| eprintln!("weft: error de audio: {e}");
    let data_cb = move |data: &mut [T], _: &cpal::OutputCallbackInfo| {
        let mut st = match state.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        while let Ok(pkt) = rx.try_recv() {
            st.push(pkt);
        }
        let mut buf = vec![0.0f32; data.len()];
        st.fill_float(&mut buf);
        for (dst, s) in data.iter_mut().zip(buf.iter()) {
            *dst = T::from_sample(*s);
        }
    };
    device
        .build_output_stream(config, data_cb, err_cb, None)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::FRAME_US;

    fn shared_clock(tag: u64) -> Arc<Mutex<MediaClock>> {
        Arc::new(Mutex::new(MediaClock::new(tag)))
    }

    #[test]
    fn drift_controller_stuffs_and_drops() {
        let mut d = DriftController::new(48);
        d.reset(100_000, 0);
        // Consumed a lot more than session time allows -> ahead -> stuff.
        assert_eq!(d.tick(110_000, 1_000), DriftAction::Stuff);
        // Consumed far less -> behind -> drop.
        assert_eq!(d.tick(200_000, 1_000), DriftAction::Drop);
        // Roughly aligned -> none.
        assert_eq!(d.tick(110_000, 481), DriftAction::None);
    }

    #[test]
    fn playback_outputs_source_then_underruns() {
        let clock = shared_clock(1);
        let mut st = PlaybackState::new(clock, JitterBuffer::new(200_000));
        // One packet with 10 frames (20 samples), pts due immediately.
        let samples: Vec<i16> = (0..20).map(|i| i as i16).collect();
        let pkt = AudioPacket::new(1, 0, 0, samples.clone(), SAMPLE_RATE, CHANNELS);
        assert!(st.push(pkt));

        let mut out = vec![0.0f32; 20];
        st.fill_float(&mut out);
        for (i, s) in out.iter().enumerate() {
            assert_eq!(*s, samples[i] as f32 / 32768.0, "sample {i}");
        }
        let stats = st.stats();
        assert_eq!(stats.played_frames, 10);

        // Nothing left -> underrun -> silence.
        let mut out2 = vec![0.0f32; 20];
        st.fill_float(&mut out2);
        assert!(out2.iter().all(|s| *s == 0.0));
        assert_eq!(st.stats().underruns, 1);
    }

    #[test]
    fn playback_orders_multiple_packets() {
        let clock = shared_clock(1);
        let mut st = PlaybackState::new(clock, JitterBuffer::new(200_000));
        // Two one-frame packets, arrival in reverse order, both due immediately.
        assert!(st.push(AudioPacket::new(1, 1, 0, vec![333, 444], SAMPLE_RATE, CHANNELS)));
        assert!(st.push(AudioPacket::new(1, 0, 0, vec![111, 222], SAMPLE_RATE, CHANNELS)));

        let mut out = vec![0.0f32; 4];
        st.fill_float(&mut out);
        assert_eq!(out[0], 111.0 / 32768.0);
        assert_eq!(out[1], 222.0 / 32768.0);
        assert_eq!(out[2], 333.0 / 32768.0);
        assert_eq!(out[3], 444.0 / 32768.0);
    }

    #[test]
    fn packet_pts_gate_playback() {
        let clock = shared_clock(1);
        let mut st = PlaybackState::new(clock, JitterBuffer::new(200_000));
        // Packet scheduled in the future (pts in the future relative to now).
        let samples = vec![0i16; 20];
        let future_pts = st.now_session_us() + 100 * FRAME_US as u128;
        assert!(st.push(AudioPacket::new(1, 0, future_pts as u64, samples, SAMPLE_RATE, CHANNELS)));

        let mut out = vec![0.0f32; 20];
        st.fill_float(&mut out);
        // Not due yet -> underrun silence.
        assert!(out.iter().all(|s| *s == 0.0));
        assert_eq!(st.stats().underruns, 1);
    }
}
