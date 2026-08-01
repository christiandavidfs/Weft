use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::media::clock::MediaClock;
use crate::media::jitter::JitterBuffer;
use crate::media::packet::AudioPacket;
use crate::media::sink::{spawn_playback, PlaybackState, PlaybackStats};
use crate::media::source::{decode_file_to_pcm, PacketizedSource};
use crate::media::transport::MediaSocket;
use crate::media::FRAME_US;

const RX_TIMEOUT: Duration = Duration::from_millis(50);
const JITTER_CAPACITY_US: u128 = 200_000;
const TARGET_LATENCY_US: u128 = 100_000;

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
    pub clock_synced: bool,
    pub clock_offset_us: i64,
    pub clock_rtt_us: u128,
    pub last_error: String,
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
    transmitting: Arc<AtomicBool>,
    last_error: Arc<Mutex<String>>,
    stop: Arc<AtomicBool>,
    #[allow(dead_code)]
    rx_thread: thread::JoinHandle<()>,
    #[allow(dead_code)]
    pb_thread: Option<thread::JoinHandle<()>>,
}

impl MediaEngine {
    pub fn new(enable_audio: bool) -> Result<Self, String> {
        let socket = MediaSocket::bind()?;
        let clock = Arc::new(Mutex::new(MediaClock::new(0)));
        let (packet_tx, packet_rx) = flume::bounded::<AudioPacket>(2048);
        let state = Arc::new(Mutex::new(MediaState::default()));
        let stop = Arc::new(AtomicBool::new(false));

        let playback = if enable_audio {
            let pb_state = Arc::new(Mutex::new(PlaybackState::new(
                clock.clone(),
                JitterBuffer::new(JITTER_CAPACITY_US),
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
        let rx_thread = thread::spawn(move || {
            run_receiver(rx_socket, rx_clock, rx_state, rx_tx, rx_received, rx_stop);
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
            transmitting: Arc::new(AtomicBool::new(false)),
            last_error: Arc::new(Mutex::new(String::new())),
            stop,
            rx_thread,
        })
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
        let mut s = self.state.lock().unwrap();
        s.session_active = false;
        s.is_transmitter = false;
        s.members.clear();
        self.transmitting.store(false, Ordering::Relaxed);
    }

    pub fn update_members(&self, members: Vec<MemberMedia>) {
        self.state.lock().unwrap().members = members;
    }

    pub fn set_transmitter(&self, active: bool) {
        self.state.lock().unwrap().is_transmitter = active;
        if !active {
            self.transmitting.store(false, Ordering::Relaxed);
        }
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
            clock_synced,
            clock_offset_us,
            clock_rtt_us,
            last_error: self.last_error.lock().unwrap().clone(),
            playback,
        }
    }
}

fn run_receiver(
    socket: MediaSocket,
    _clock: Arc<Mutex<MediaClock>>,
    state: Arc<Mutex<MediaState>>,
    packet_tx: flume::Sender<AudioPacket>,
    received: Arc<AtomicU64>,
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
        let _ = packet_tx.try_send(pkt);
    }
}

fn run_transmitter(
    path: &str,
    clock: Arc<Mutex<MediaClock>>,
    state: Arc<Mutex<MediaState>>,
    socket: MediaSocket,
    transmitted: &AtomicU64,
    errors: &Mutex<String>,
    stop: &AtomicBool,
    transmitting: &AtomicBool,
) -> Result<(), String> {
    let pcm = decode_file_to_pcm(Path::new(path))?;
    let (session_tag, base_pts, base_local) = {
        let c = clock.lock().unwrap();
        (
            c.session_id(),
            (c.now_session_us() + TARGET_LATENCY_US) as u64,
            c.now_local_us(),
        )
    };
    if session_tag == 0 {
        return Err("no hay sesión activa".to_string());
    }
    let mut src = PacketizedSource::new(pcm, session_tag, base_pts, 0)?;
    let mut index = 0u64;
    while let Some(pkt) = src.next_packet() {
        if stop.load(Ordering::Relaxed) || !transmitting.load(Ordering::Relaxed) {
            break;
        }
        let targets: Vec<MemberMedia> = state.lock().unwrap().members.clone();
        for m in &targets {
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
