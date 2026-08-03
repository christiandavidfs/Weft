use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::media::capture::{check_input_device, open_capture, silence_frame, StreamResampler};
use crate::media::clock::MediaClock;
use crate::media::jitter::JitterBuffer;
use crate::media::packet::AudioPacket;
use crate::media::sink::{spawn_playback, PlaybackState, PlaybackStats};
use crate::media::source::{decode_file_to_pcm, PacketizedSource};
use crate::media::transport::MediaSocket;
use crate::media::{CHANNELS, FRAME_SAMPLES, FRAME_US, SAMPLE_RATE};

const RX_TIMEOUT: Duration = Duration::from_millis(50);

/// Tunable media-plane parameters. Latency/robustness trade-offs are exposed
/// here so deployments can adapt to their network without recompiling:
/// - `jitter_capacity_us`: how much arrival jitter the receiver absorbs.
/// - `target_latency_us`: end-to-end pre-roll the transmitter schedules.
/// - `drift_threshold_frames`: how far the output may drift from the session
///   timeline before a correction frame is stuffed/dropped (~1ms at 48kHz).
/// - `dj`: enables DJ mode to mix multiple simultaneous sources.
/// All non-`dj` values retain original defaults.
#[derive(Debug, Clone, Copy)]
pub struct MediaConfig {
    pub jitter_capacity_us: u128,
    pub target_latency_us: u128,
    pub drift_threshold_frames: u64,
    pub dj: bool,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            jitter_capacity_us: 200_000,
            target_latency_us: 100_000,
            drift_threshold_frames: 48,
            dj: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemberMedia {
    pub device_id: String,
    pub media_addr: std::net::SocketAddr,
}

#[derive(Debug, Clone, Default)]
pub struct MediaStats {
    pub session_tag: u64,
    pub session_active: bool,
    pub is_transmitter: bool,
    pub media_port: u16,
    pub received_packets: u64,
    pub transmitted_packets: u64,
    pub capturing: bool,
    pub clock_synced: bool,
    pub clock_offset_us: i64,
    pub clock_rtt_us: u128,
    pub last_error: String,
    pub last_source_id: u64,
    pub playback: Option<PlaybackStats>,
}

#[derive(Default)]
struct MediaState {
    session_tag: u64,
    session_active: bool,
    is_transmitter: bool,
    members: Vec<MemberMedia>,
}

/// Owns the media plane: a UDP socket, the shared session clock, an optional
/// audio output thread, and the receiver/transmitter threads.
pub struct MediaEngine {
    socket: MediaSocket,
    clock: Arc<Mutex<MediaClock>>,
    state: Arc<Mutex<MediaState>>,
    playback: Option<Arc<Mutex<PlaybackState>>>,
    events: Arc<Mutex<Option<Box<dyn Fn(&str, String) + Send + Sync>>>>,
    received: Arc<AtomicU64>,
    transmitted: Arc<AtomicU64>,
    last_source_id: Arc<AtomicU64>,
    transmitting: Arc<AtomicBool>,
    capturing: Arc<AtomicBool>,
    capture: Mutex<Option<CaptureState>>,
    source_id: Arc<AtomicU64>,
    last_error: Arc<Mutex<String>>,
    stop: Arc<AtomicBool>,
    config: MediaConfig,
    #[allow(dead_code)]
    rx_thread: thread::JoinHandle<()>,
    #[allow(dead_code)]
    pb_thread: Option<thread::JoinHandle<()>>,
}

/// A running live capture: keeps the cpal stream alive and owns the capture
/// transmitter thread.
struct CaptureState {
    stop: Arc<AtomicBool>,
    thread: thread::JoinHandle<()>,
}

impl MediaEngine {
    pub fn new(enable_audio: bool) -> Result<Self, String> {
        Self::new_with_config(enable_audio, MediaConfig::default())
    }

    pub fn new_with_config(enable_audio: bool, config: MediaConfig) -> Result<Self, String> {
        let socket = MediaSocket::bind()?;
        let clock = Arc::new(Mutex::new(MediaClock::new(0)));
        let (packet_tx, packet_rx) = flume::bounded::<AudioPacket>(2048);
        let state = Arc::new(Mutex::new(MediaState::default()));
        let stop = Arc::new(AtomicBool::new(false));

        let playback = if enable_audio {
            let pb_state = Arc::new(Mutex::new(PlaybackState::new(
                clock.clone(),
                JitterBuffer::new(config.jitter_capacity_us),
                config.drift_threshold_frames,
            )));
            let pb_thread = spawn_playback(pb_state.clone(), packet_rx, stop.clone());
            Some((pb_state, pb_thread))
        } else {
            None
        };

        let rx_socket = socket.try_clone()?;
        let rx_clock = clock.clone();
        let rx_state = state.clone();
        let rx_tx = packet_tx;
        let rx_stop = stop.clone();
        let received = Arc::new(AtomicU64::new(0));
        let rx_received = received.clone();
        let last_source_id = Arc::new(AtomicU64::new(0));
        let rx_last_source = last_source_id.clone();
        let rx_thread = thread::spawn(move || {
            run_receiver(rx_socket, rx_clock, rx_state, rx_tx, rx_received, rx_last_source, rx_stop);
        });

        Ok(Self {
            socket,
            clock,
            state,
            playback: playback.as_ref().map(|(s, _)| s.clone()),
            pb_thread: playback.map(|(_, t)| t),
            events: Arc::new(Mutex::new(None)),
            received,
            transmitted: Arc::new(AtomicU64::new(0)),
            last_source_id,
            transmitting: Arc::new(AtomicBool::new(false)),
            capturing: Arc::new(AtomicBool::new(false)),
            capture: Mutex::new(None),
            source_id: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(Mutex::new(String::new())),
            stop,
            config,
            rx_thread,
        })
    }

    /// Set this device's identity, stamped on every packet we transmit. Used by
    /// receivers to detect a source switch during handoffs.
    pub fn set_source_id(&self, id: u64) {
        self.source_id.store(id, Ordering::Relaxed);
    }

    pub fn set_event_callback(&self, cb: Box<dyn Fn(&str, String) + Send + Sync>) {
        *self.events.lock().unwrap() = Some(cb);
    }

    #[allow(dead_code)]
    pub fn push_packet(&self, pkt: AudioPacket) -> bool {
        // Used by tests/consumers that feed packets directly (no network).
        if let Some(p) = &self.playback {
            p.lock().unwrap().push(pkt)
        } else {
            false
        }
    }

    pub fn media_port(&self) -> u16 {
        self.socket.local_addr().port()
    }

    pub fn set_session(&self, session_tag: u64, is_coordinator: bool) {
        {
            let mut c = self.clock.lock().unwrap();
            c.set_session_id(session_tag);
            if is_coordinator {
                c.make_reference();
            }
        }
        {
            let mut s = self.state.lock().unwrap();
            if s.session_tag != session_tag || !s.session_active {
                s.session_tag = session_tag;
                s.session_active = true;
                s.members.clear();
            }
        }
    }

    pub fn leave_session(&self) {
        {
            let mut s = self.state.lock().unwrap();
            s.session_active = false;
            s.is_transmitter = false;
            s.members.clear();
        }
        self.transmitting.store(false, Ordering::Relaxed);
        self.stop_capture();
    }

    pub fn update_members(&self, members: Vec<MemberMedia>) {
        self.state.lock().unwrap().members = members;
    }

    pub fn set_transmitter(&self, active: bool) {
        self.state.lock().unwrap().is_transmitter = active;
        if !active {
            self.transmitting.store(false, Ordering::Relaxed);
            self.stop_capture();
        }
    }

    /// Start capturing from an input device (by name, or the default) and
    /// streaming it to all members. Optional: can be stopped at any time with
    /// `stop_capture`. Requires an active session.
    pub fn start_capture(&self, device_name: Option<&str>) -> Result<(), String> {
        if self.capturing.swap(true, Ordering::Relaxed) {
            return Err("ya hay una captura en curso".to_string());
        }
        let (session_tag, base_pts, base_local) = {
            let c = self.clock.lock().unwrap();
            (
                c.session_id(),
                (c.now_session_us() + self.config.target_latency_us) as u64,
                c.now_local_us(),
            )
        };
        let active = self.state.lock().unwrap().session_active;
        if !active || session_tag == 0 {
            self.capturing.store(false, Ordering::Relaxed);
            return Err("no hay sesión activa".to_string());
        }

        // Fail fast if the device doesn't exist. The stream itself is opened on
        // the capture thread (cpal::Stream is !Send and lives there).
        check_input_device(device_name).inspect_err(|e| {
            self.capturing.store(false, Ordering::Relaxed);
            *self.last_error.lock().unwrap() = e.clone();
        })?;

        let device_name = device_name.map(|s| s.to_string());
        let t_clock = self.clock.clone();
        let t_state = self.state.clone();
        let t_socket = match self.socket.try_clone() {
            Ok(s) => s,
            Err(e) => {
                self.capturing.store(false, Ordering::Relaxed);
                return Err(format!("socket: {e}"));
            }
        };
        let t_transmitted = self.transmitted.clone();
        let t_errors = self.last_error.clone();
        let t_capturing = self.capturing.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let t_stop = stop.clone();
        let t_events = self.events.clone();
        let source_id = self.source_id.load(Ordering::Relaxed);

        let thread = thread::spawn(move || {
            let result = (|| {
                let cap = open_capture(device_name.as_deref())?;
                let rx = cap.rx.clone();
                let rate = cap.rate;
                let ctx = CaptureContext {
                    clock: t_clock,
                    state: t_state,
                    socket: t_socket,
                    session_tag,
                    source_id,
                    base_pts,
                    base_local,
                };
                let res = run_capture_transmitter(ctx, rx, rate, &t_transmitted, &t_stop);
                drop(cap); // stops the device stream (owned on this thread)
                res
            })();
            match result {
                Ok(()) => {
                    if let Some(cb) = t_events.lock().unwrap().as_ref() {
                        cb("capture_stopped", "captura detenida".to_string());
                    }
                }
                Err(e) => {
                    *t_errors.lock().unwrap() = e.clone();
                    if let Some(cb) = t_events.lock().unwrap().as_ref() {
                        cb("capture_error", e);
                    }
                }
            }
            t_capturing.store(false, Ordering::Relaxed);
        });

        *self.capture.lock().unwrap() = Some(CaptureState { stop, thread });
        if let Some(cb) = self.events.lock().unwrap().as_ref() {
            cb("capture_started", "captura iniciada".to_string());
        }
        Ok(())
    }

    /// Stop live capture (optional). No-op if not capturing.
    pub fn stop_capture(&self) {
        self.capturing.store(false, Ordering::Relaxed);
        if let Some(st) = self.capture.lock().unwrap().take() {
            st.stop.store(true, Ordering::Relaxed);
            let _ = st.thread.join();
            if let Some(cb) = self.events.lock().unwrap().as_ref() {
                cb("capture_stopped", "captura detenida".to_string());
            }
        }
    }

    pub fn is_capturing(&self) -> bool {
        self.capturing.load(Ordering::Relaxed)
    }

    pub fn process_ntp(
        &self,
        query_sent_local_us: u128,
        reply_received_local_us: u128,
        server_received_us: u128,
        server_sent_us: u128,
    ) {
        if let Ok(mut c) = self.clock.lock() {
            c.process_ntp(query_sent_local_us, reply_received_local_us, server_received_us, server_sent_us);
        }
    }

    pub fn now_session_us(&self) -> u128 {
        self.clock.lock().map(|c| c.now_session_us()).unwrap_or(0)
    }

    pub fn now_local_us(&self) -> u128 {
        self.clock.lock().map(|c| c.now_local_us()).unwrap_or(0)
    }

    pub fn clock(&self) -> Arc<Mutex<MediaClock>> {
        self.clock.clone()
    }

    pub fn is_transmitter(&self) -> bool {
        self.state.lock().unwrap().is_transmitter
    }

    /// Transmit an audio file to all members, paced at 20ms per packet. Runs on
    /// a background thread. Returns an error if a transmission is already in
    /// progress or decoding fails synchronously.
    pub fn transmit_file(&self, path: &str) -> Result<(), String> {
        if self.transmitting.swap(true, Ordering::Relaxed) {
            return Err("ya hay una transmisión en curso".to_string());
        }
        let path_buf = path.to_string();
        let clock = self.clock.clone();
        let state = self.state.clone();
        let socket = self.socket.try_clone()?;
        let transmitted = self.transmitted.clone();
        let errors = self.last_error.clone();
        let stop = self.stop.clone();
        let self_events = self.events.clone();
        let transmitting = self.transmitting.clone();
        let self_transmitting = self.transmitting.clone();
        let source_id = self.source_id.load(Ordering::Relaxed);
        let target_latency_us = self.config.target_latency_us;

        thread::spawn(move || {
            let result = run_transmitter(
                &path_buf,
                clock,
                state,
                socket,
                &transmitted,
                &errors,
                &stop,
                &self_transmitting,
                source_id,
                target_latency_us,
            );
            match result {
                Ok(()) => {
                    if let Some(cb) = self_events.lock().unwrap().as_ref() {
                        cb("transmit_finished", "transmisión terminada".to_string());
                    }
                }
                Err(e) => {
                    if let Some(cb) = self_events.lock().unwrap().as_ref() {
                        cb("transmit_error", e);
                    }
                }
            }
            transmitting.store(false, Ordering::Relaxed);
        });
        Ok(())
    }

    pub fn stats(&self) -> MediaStats {
        let (session_tag, session_active, is_transmitter) = {
            let s = self.state.lock().unwrap();
            (s.session_tag, s.session_active, s.is_transmitter)
        };
        let (clock_synced, clock_offset_us, clock_rtt_us) = {
            let c = self.clock.lock().unwrap();
            (c.is_synced(), c.offset_us() as i64, c.rtt_us())
        };
        let playback = self.playback.as_ref().map(|p| p.lock().unwrap().stats());
        MediaStats {
            session_tag,
            session_active,
            is_transmitter,
            media_port: self.media_port(),
            received_packets: self.received.load(Ordering::Relaxed),
            transmitted_packets: self.transmitted.load(Ordering::Relaxed),
            capturing: self.capturing.load(Ordering::Relaxed),
            clock_synced,
            clock_offset_us,
            clock_rtt_us,
            last_error: self.last_error.lock().unwrap().clone(),
            last_source_id: self.last_source_id.load(Ordering::Relaxed),
            playback,
        }
    }

    /// The transmitter_id of the most recent packet received. Used by the
    /// coordinator to detect whether a handed-off transmitter is actually
    /// streaming (rollback).
    pub fn last_source_id(&self) -> u64 {
        self.last_source_id.load(Ordering::Relaxed)
    }
}

fn run_receiver(
    socket: MediaSocket,
    _clock: Arc<Mutex<MediaClock>>,
    state: Arc<Mutex<MediaState>>,
    packet_tx: flume::Sender<AudioPacket>,
    received: Arc<AtomicU64>,
    last_source_id: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        let Ok(Some((pkt, _src))) = socket.recv_packet_timeout(RX_TIMEOUT) else {
            continue;
        };
        let (active, tag) = {
            let s = state.lock().unwrap();
            (s.session_active, s.session_tag)
        };
        if !active || pkt.session_id != tag {
            continue;
        }
        received.fetch_add(1, Ordering::Relaxed);
        last_source_id.store(pkt.transmitter_id, Ordering::Relaxed);
        let _ = packet_tx.try_send(pkt);
    }
}

#[allow(clippy::too_many_arguments)]
fn run_transmitter(
    path: &str,
    clock: Arc<Mutex<MediaClock>>,
    state: Arc<Mutex<MediaState>>,
    socket: MediaSocket,
    transmitted: &AtomicU64,
    errors: &Mutex<String>,
    stop: &AtomicBool,
    transmitting: &AtomicBool,
    source_id: u64,
    target_latency_us: u128,
) -> Result<(), String> {
    let pcm = decode_file_to_pcm(Path::new(path))?;
    let (session_tag, base_pts, base_local) = {
        let c = clock.lock().unwrap();
        (
            c.session_id(),
            (c.now_session_us() + target_latency_us) as u64,
            c.now_local_us(),
        )
    };
    if session_tag == 0 {
        return Err("no hay sesión activa".to_string());
    }
    let mut src = PacketizedSource::new(pcm, session_tag, source_id, base_pts, 0)?;
    let mut index = 0u64;
    let own_addr = socket.local_addr();
    while let Some(pkt) = src.next_packet() {
        if stop.load(Ordering::Relaxed) || !transmitting.load(Ordering::Relaxed) {
            break;
        }
        let targets: Vec<MemberMedia> = state.lock().unwrap().members.clone();
        for m in &targets {
            if m.media_addr == own_addr {
                continue;
            }
            let _ = socket.send_packet(&pkt, m.media_addr);
        }
        transmitted.fetch_add(1, Ordering::Relaxed);
        index += 1;
        let deadline = base_local + (index as u128) * (FRAME_US as u128);
        let now = clock.lock().map(|c| c.now_local_us()).unwrap_or(0);
        if deadline > now {
            thread::sleep(Duration::from_micros((deadline - now) as u64));
        }
    }
    if stop.load(Ordering::Relaxed) {
        return Err("transmisión detenida".to_string());
    }
    if let Ok(mut e) = errors.lock() {
        e.clear();
    }
    Ok(())
}

/// Live capture context: pacing clock, session state, socket, and stream timing.
struct CaptureContext {
    clock: Arc<Mutex<MediaClock>>,
    state: Arc<Mutex<MediaState>>,
    socket: MediaSocket,
    session_tag: u64,
    source_id: u64,
    base_pts: u64,
    base_local: u128,
}

/// Stream live microphone input to all members, one 20ms frame per packet,
/// paced against the session timeline. Sends silence on underrun to keep the
/// receivers' timeline continuous.
fn run_capture_transmitter(
    ctx: CaptureContext,
    rx: flume::Receiver<Vec<i16>>,
    rate: u32,
    transmitted: &AtomicU64,
    stop: &AtomicBool,
) -> Result<(), String> {
    let CaptureContext { clock, state, socket, session_tag, source_id, base_pts, base_local } = ctx;
    let mut resampler = StreamResampler::new(rate, SAMPLE_RATE);
    let mut index = 0u64;
    loop {
        if stop.load(Ordering::Relaxed) {
            return Err("captura detenida".to_string());
        }
        let (active, is_tx) = {
            let s = state.lock().unwrap();
            (s.session_active, s.is_transmitter)
        };
        if !active || !is_tx {
            return Err("captura detenida: se perdió el rol de transmisor".to_string());
        }

        let deadline = base_local + (index as u128) * FRAME_US as u128;
        let now = clock.lock().map(|c| c.now_local_us()).unwrap_or(0);
        if now < deadline {
            thread::sleep(Duration::from_micros((deadline - now) as u64));
        }

        // Gather enough input for one 20ms frame (or give up -> silence).
        while resampler.frames_needed_for(FRAME_SAMPLES) > 0 {
            match rx.recv_timeout(Duration::from_millis(250)) {
                Ok(chunk) => resampler.push(&chunk),
                Err(flume::RecvTimeoutError::Disconnected) => {
                    return Err("captura finalizada".to_string());
                }
                Err(flume::RecvTimeoutError::Timeout) => break,
            }
        }
        let mut samples = resampler.take(FRAME_SAMPLES);
        if samples.len() < FRAME_SAMPLES * CHANNELS as usize {
            let mut full = silence_frame();
            let to_copy = samples.len().min(full.len());
            full[..to_copy].copy_from_slice(&samples[..to_copy]);
            samples = full;
        }

        let pkt = AudioPacket::new(
            session_tag,
            source_id,
            index as u32,
            base_pts + index * FRAME_US,
            samples,
            SAMPLE_RATE,
            CHANNELS,
        );
        let own_addr = socket.local_addr();
        let targets: Vec<MemberMedia> = state.lock().unwrap().members.clone();
        for m in &targets {
            if m.media_addr == own_addr {
                continue;
            }
            let _ = socket.send_packet(&pkt, m.media_addr);
        }
        transmitted.fetch_add(1, Ordering::Relaxed);
        index += 1;
    }
}
