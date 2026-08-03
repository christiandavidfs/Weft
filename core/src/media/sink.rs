use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};

use crate::media::clock::MediaClock;
use crate::media::jitter::JitterBuffer;
use crate::media::packet::AudioPacket;
use crate::media::{CHANNELS, SAMPLE_RATE};

/// Length of the crossfade during a handoff, in output frames. ~100ms at 48kHz.
const CROSSFADE_FRAMES: usize = 4800;

/// A crossfade in progress: mixes the previous source's tail (fading out) with
/// the new source's head (fading in) over `total` frames.
struct Crossfade {
    old: VecDeque<[i16; 2]>,
    last_old: [i16; 2],
    remaining: usize,
    total: usize,
}

impl Crossfade {
    fn gain(&self) -> f32 {
        let elapsed = self.total.saturating_sub(self.remaining) as f32;
        (elapsed / self.total.max(1) as f32).clamp(0.0, 1.0)
    }
}

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
    source_id: Option<u64>,
    crossfade: Option<Crossfade>,
}

impl PlaybackState {
    pub fn new(
        clock: Arc<Mutex<MediaClock>>,
        jitter: JitterBuffer,
        drift_threshold_frames: u64,
    ) -> Self {
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
            drift: DriftController::new(drift_threshold_frames),
            source_id: None,
            crossfade: None,
        }
    }

    pub fn now_session_us(&self) -> u128 {
        self.clock.lock().map(|c| c.now_session_us()).unwrap_or(0)
    }

    pub fn push(&mut self, pkt: AudioPacket) -> bool {
        if let Some(cur) = self.source_id {
            if cur != pkt.transmitter_id {
                self.begin_crossfade();
            }
        }
        self.source_id = Some(pkt.transmitter_id);
        let now = self.now_session_us();
        self.jitter.push(pkt, now)
    }

    /// Harvest whatever the previous source still has buffered (current packet
    /// remainder, pending queue, jitter) into a fade-out tail, then reset the
    /// buffers so the new source's sequence space starts clean.
    fn begin_crossfade(&mut self) {
        let mut old: VecDeque<[i16; 2]> = VecDeque::new();
        let append = |frames: &mut VecDeque<[i16; 2]>, samples: &[i16]| {
            for f in samples.chunks_exact(CHANNELS as usize) {
                frames.push_back([f[0], f[1]]);
                if frames.len() >= CROSSFADE_FRAMES {
                    break;
                }
            }
        };
        if let Some(cur) = self.cur.take() {
            let start = self.cur_offset * CHANNELS as usize;
            if start < cur.samples.len() {
                append(&mut old, &cur.samples[start..]);
            }
        }
        for p in self.pending.drain(..) {
            append(&mut old, &p.samples);
            if old.len() >= CROSSFADE_FRAMES {
                break;
            }
        }
        for p in self.jitter.drain_all() {
            append(&mut old, &p.samples);
            if old.len() >= CROSSFADE_FRAMES {
                break;
            }
        }
        self.cur = None;
        self.cur_offset = 0;
        self.pending.clear();
        let total = old.len();
        self.crossfade = Some(Crossfade {
            old,
            last_old: self.last_frame,
            remaining: total,
            total,
        });
    }

    /// Fill an interleaved stereo f32 output buffer. This is the realtime core
    /// used by the audio callback (guarded by the engine's mutex).
    pub fn fill_float(&mut self, out: &mut [f32]) {
        let n = out.len() / 2;
        for i in 0..n {
            let now = self.now_session_us();

            // Crossfade in progress: mix the old source's tail with the new
            // source's head. Drift correction is suspended during the fade.
            let fade_frame = self.crossfade.as_mut().map(|cf| cf.old.pop_front().unwrap_or(cf.last_old));
            let fade_gain = self.crossfade.as_ref().map(|cf| cf.gain());
            if let Some(old) = fade_frame {
                let new = self.take_frame().unwrap_or([0, 0]);
                let g = fade_gain.unwrap_or(0.0);
                let l = old[0] as f32 * (1.0 - g) + new[0] as f32 * g;
                let r = old[1] as f32 * (1.0 - g) + new[1] as f32 * g;
                out[i * 2] = l / 32768.0;
                out[i * 2 + 1] = r / 32768.0;
                self.last_frame = [l as i16, r as i16];
                self.played_frames += 1;
                if let Some(cf) = &mut self.crossfade {
                    cf.remaining -= 1;
                    if cf.remaining == 0 {
                        self.crossfade = None;
                        self.drift.reset(now, self.consumed_frames);
                    }
                }
                continue;
            }

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

/// Any object that can absorb incoming audio packets and render interleaved
/// stereo f32 frames. One filled buffer drives a cpal callback. Both the plain
/// `PlaybackState` (single source) and the `DjMixer` (multiple simultaneous
/// sources) implement it, letting the engine pick at construction time.
pub trait Sink: Send {
    fn push(&mut self, pkt: AudioPacket) -> bool;
    fn fill_float(&mut self, out: &mut [f32]);
    fn stats(&self) -> PlaybackStats;
}

/// DJ mode: mixes several simultaneous sources (one per transmitter) into a
/// single output. Each source keeps its own `PlaybackState` (jitter + drift)
/// but they all share the session `MediaClock`, so everything stays on the same
/// timeline. Output is the sum of every active source's frame, auto-gained by
/// the number of sources that actually produced sound.
pub struct DjMixer {
    clock: Arc<Mutex<MediaClock>>,
    sources: HashMap<u64, PlaybackState>,
    jitter_capacity_us: u128,
    drift_threshold_frames: u64,
    played_frames: u64,
}

impl DjMixer {
    pub fn new(
        clock: Arc<Mutex<MediaClock>>,
        jitter_capacity_us: u128,
        drift_threshold_frames: u64,
    ) -> Self {
        Self {
            clock,
            sources: HashMap::new(),
            jitter_capacity_us,
            drift_threshold_frames,
            played_frames: 0,
        }
    }

    pub fn num_sources(&self) -> usize {
        self.sources.len()
    }
}

impl Sink for DjMixer {
    fn push(&mut self, pkt: AudioPacket) -> bool {
        let source = self.sources.entry(pkt.transmitter_id).or_insert_with(|| {
            PlaybackState::new(
                self.clock.clone(),
                JitterBuffer::new(self.jitter_capacity_us),
                self.drift_threshold_frames,
            )
        });
        source.push(pkt)
    }

    fn fill_float(&mut self, out: &mut [f32]) {
        let mut mix = vec![0.0f32; out.len()];
        let mut active = 0u32;
        for src in self.sources.values_mut() {
            let mut buf = vec![0.0f32; out.len()];
            src.fill_float(&mut buf);
            if buf.iter().any(|s| s.abs() > f32::EPSILON) {
                active += 1;
            }
            for (m, s) in mix.iter_mut().zip(buf.iter()) {
                *m += s;
            }
        }
        self.played_frames += (out.len() / 2) as u64;
        if active == 0 {
            out.fill(0.0);
            return;
        }
        let gain = 1.0 / active as f32;
        for (o, m) in out.iter_mut().zip(mix.iter()) {
            *o = (m * gain).clamp(-1.0, 1.0);
        }
    }

    fn stats(&self) -> PlaybackStats {
        let mut agg = PlaybackStats {
            playing: self.played_frames > 0,
            played_frames: self.played_frames,
            ..PlaybackStats::default()
        };
        for s in self.sources.values() {
            let st = s.stats();
            agg.underruns += st.underruns;
            agg.stuffed += st.stuffed;
            agg.dropped += st.dropped;
            agg.buffered_packets += st.buffered_packets;
            agg.buffered_us += st.buffered_us;
            agg.lost_packets += st.lost_packets;
        }
        agg
    }
}

impl Sink for PlaybackState {
    fn push(&mut self, pkt: AudioPacket) -> bool {
        PlaybackState::push(self, pkt)
    }

    fn fill_float(&mut self, out: &mut [f32]) {
        PlaybackState::fill_float(self, out)
    }

    fn stats(&self) -> PlaybackStats {
        PlaybackState::stats(self)
    }
}

/// Spawns a thread that owns a cpal output stream and feeds it from a shared
/// `Sink`. Packets arrive via `rx` and are drained by the audio callback.
/// Returns the thread handle.
pub fn spawn_playback(
    state: Arc<Mutex<dyn Sink>>,
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
    state: Arc<Mutex<dyn Sink>>,
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

    /// Default jitter capacity for unit tests, ~200ms.
    const TEST_JITTER_CAPACITY_US: u128 = 200_000;
    /// Default drift threshold for unit tests, ~1ms at 48kHz.
    const DRIFT_THRESHOLD_FRAMES: u64 = 48;

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
    fn drift_threshold_is_configurable() {
        // With a tight threshold (4 frames), a 5-frame surplus already stuffs,
        // while a default 48-frame threshold would stay quiet.
        let mut d = DriftController::new(4);
        d.reset(100_000, 0);
        assert_eq!(d.tick(110_000, 484), DriftAction::None);
        assert_eq!(d.tick(110_000, 485), DriftAction::Stuff);
    }

    #[test]
    fn playback_outputs_source_then_underruns() {
        let clock = shared_clock(1);
        let mut st = PlaybackState::new(clock, JitterBuffer::new(TEST_JITTER_CAPACITY_US), DRIFT_THRESHOLD_FRAMES);
        // One packet with 10 frames (20 samples), pts due immediately.
        let samples: Vec<i16> = (0..20).map(|i| i as i16).collect();
        let pkt = AudioPacket::new(1, 0, 0, 0, samples.clone(), SAMPLE_RATE, CHANNELS);
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
        let mut st = PlaybackState::new(clock, JitterBuffer::new(TEST_JITTER_CAPACITY_US), DRIFT_THRESHOLD_FRAMES);
        // Two one-frame packets, arrival in reverse order, both due immediately.
        assert!(st.push(AudioPacket::new(1, 0, 1, 0, vec![333, 444], SAMPLE_RATE, CHANNELS)));
        assert!(st.push(AudioPacket::new(1, 0, 0, 0, vec![111, 222], SAMPLE_RATE, CHANNELS)));

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
        let mut st = PlaybackState::new(clock, JitterBuffer::new(TEST_JITTER_CAPACITY_US), DRIFT_THRESHOLD_FRAMES);
        // Packet scheduled in the future (pts in the future relative to now).
        let samples = vec![0i16; 20];
        let future_pts = st.now_session_us() + 100 * FRAME_US as u128;
        assert!(st.push(AudioPacket::new(1, 0, 0, future_pts as u64, samples, SAMPLE_RATE, CHANNELS)));

        let mut out = vec![0.0f32; 20];
        st.fill_float(&mut out);
        // Not due yet -> underrun silence.
        assert!(out.iter().all(|s| *s == 0.0));
        assert_eq!(st.stats().underruns, 1);
    }

    #[test]
    fn source_switch_crossfades_old_tail_with_new_head() {
        let clock = shared_clock(1);
        let mut st = PlaybackState::new(clock, JitterBuffer::new(TEST_JITTER_CAPACITY_US), DRIFT_THRESHOLD_FRAMES);
        // Old source: two one-frame packets, due immediately.
        assert!(st.push(AudioPacket::new(1, 7, 0, 0, vec![1000, 1000], SAMPLE_RATE, CHANNELS)));
        assert!(st.push(AudioPacket::new(1, 7, 1, 0, vec![2000, 2000], SAMPLE_RATE, CHANNELS)));
        // New source (transmitter 42) restarts its sequence at 0.
        assert!(st.push(AudioPacket::new(1, 42, 0, 0, vec![100, 100], SAMPLE_RATE, CHANNELS)));
        assert!(st.push(AudioPacket::new(1, 42, 1, 0, vec![200, 200], SAMPLE_RATE, CHANNELS)));

        let mut out = vec![0.0f32; 4];
        st.fill_float(&mut out);
        // Frame 0: pure old tail (t=0). Frame 1: old tail faded to 0.5,
        // new head blended in at 0.5.
        let f0 = 1000.0 / 32768.0;
        assert!((out[0] - f0).abs() < 1e-4, "first frame {:?}", out[0]);
        let expected = (2000.0 * 0.5 + 200.0 * 0.5) / 32768.0;
        assert!((out[2] - expected).abs() < 1e-4, "second frame {:?}", out[2]);
        assert_eq!(st.stats().underruns, 0);
        assert!(st.crossfade.is_none(), "crossfade should finish");
    }

    #[test]
    fn dj_mixer_sums_and_auto_gains_two_sources() {
        let clock = shared_clock(1);
        let mut mix = DjMixer::new(clock, TEST_JITTER_CAPACITY_US, DRIFT_THRESHOLD_FRAMES);
        // Source A: one frame at full scale.
        assert!(mix.push(AudioPacket::new(1, 10, 0, 0, vec![16384, 16384], SAMPLE_RATE, CHANNELS)));
        // Source B: same frame; not active yet (no sound until it fills).
        // Two active sources -> gain 0.5 each.

        let mut two = vec![0.0f32; 4];
        mix.fill_float(&mut two);
        // Only source A has data so far -> single active -> full scale half.
        // 16384/32768 = 0.5, no other source -> gain 1 -> 0.5.
        assert!((two[0] - 0.5).abs() < 1e-6, "single source {:?}", two[0]);

        // Now source B streams too.
        assert!(mix.push(AudioPacket::new(1, 20, 0, 0, vec![16384, 16384], SAMPLE_RATE, CHANNELS)));
        let mut both = vec![0.0f32; 4];
        mix.fill_float(&mut both);
        // Both active, each contributes 16384/32768 = 0.5, sum=1.0, gain 0.5.
        let expected = (0.5 + 0.5) * 0.5; // 0.5
        assert!((both[0] - expected).abs() < 1e-6, "two sources {:?}", both[0]);
        assert_eq!(mix.num_sources(), 2);
    }
}
