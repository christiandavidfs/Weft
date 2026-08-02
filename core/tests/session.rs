use std::io::Write;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use weft_core::network::NetworkEngine;

// mDNS daemons interfere when two sessions run concurrently in the same
// process, so serialize the session tests.
static SERIAL: Mutex<()> = Mutex::new(());

fn wait_until<F>(timeout: Duration, mut pred: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    false
}

#[test]fn two_devices_form_session_and_hand_transmit_token() {
    let _guard = SERIAL.lock().unwrap();
    let a = NetworkEngine::start("Alpha".to_string()).unwrap();
    let b = NetworkEngine::start("Beta".to_string()).unwrap();

    let sa = || a.status();
    let sb = || b.status();

    // 1. Deben converger a un coordinador + un miembro, con ambos en la lista de miembros.
    let formed = wait_until(Duration::from_secs(15), || {
        let (ra, rb) = (sa().role, sb().role);
        let (ra, rb) = (ra.as_str(), rb.as_str());
        let both_members = sa().members.len() == 2 && sb().members.len() == 2;
        ((ra == "coordinator" && rb == "member") || (ra == "member" && rb == "coordinator"))
            && both_members
    });
    assert!(formed, "no se formó la sesión: A={:?} B={:?}", sa(), sb());

    // 2. El coordinador toma el token de transmisión.
    let (coord, member): (&NetworkEngine, &NetworkEngine) = if sa().role == "coordinator" {
        (&a, &b)
    } else {
        (&b, &a)
    };
    coord.request_transmit();
    let granted = wait_until(Duration::from_secs(5), || {
        sa().transmitter_id == coord.status().device_id
            && sb().transmitter_id == coord.status().device_id
    });
    assert!(granted, "el coordinador no obtuvo el token: {:?}", sa());

    // 3. El miembro pide el token (el coordinador está ocupado) → queda en espera.
    member.request_transmit();
    let waiting = wait_until(Duration::from_secs(5), || {
        coord.status().pending_transmit_requests.contains(&member.status().device_id)
    });
    assert!(waiting, "el miembro no quedó en espera: {:?}", coord.status());

    // 4. El coordinador libera el token → el miembro pasa a transmitir automáticamente.
    coord.release_transmit();
    let handed = wait_until(Duration::from_secs(5), || {
        sa().transmitter_id == member.status().device_id
            && sb().transmitter_id == member.status().device_id
    });
    assert!(handed, "el token no pasó al miembro: {:?} {:?}", sa(), sb());

    a.stop();
    b.stop();
}

fn write_wav(path: &std::path::Path, sample_rate: u32, samples: &[i16], channels: u16) {
    let bytes_per_sample = 2u16;
    let block_align = channels * bytes_per_sample;
    let byte_rate = sample_rate as u32 * block_align as u32;
    let data_len = samples.len() as u32 * bytes_per_sample as u32;
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(b"RIFF").unwrap();
    f.write_all(&(36 + data_len).to_le_bytes()).unwrap();
    f.write_all(b"WAVE").unwrap();
    f.write_all(b"fmt ").unwrap();
    f.write_all(&16u32.to_le_bytes()).unwrap();
    f.write_all(&1u16.to_le_bytes()).unwrap();
    f.write_all(&channels.to_le_bytes()).unwrap();
    f.write_all(&sample_rate.to_le_bytes()).unwrap();
    f.write_all(&byte_rate.to_le_bytes()).unwrap();
    f.write_all(&block_align.to_le_bytes()).unwrap();
    f.write_all(&(16u16).to_le_bytes()).unwrap();
    f.write_all(b"data").unwrap();
    f.write_all(&data_len.to_le_bytes()).unwrap();
    for s in samples {
        f.write_all(&s.to_le_bytes()).unwrap();
    }
}

fn write_tone_wav(dir: &std::path::Path, seconds: usize) -> std::path::PathBuf {
    let wav = dir.join("tone.wav");
    let sample_rate = 48000u32;
    let channels = 2u16;
    let frames = sample_rate as usize * seconds;
    let mut samples = Vec::with_capacity(frames * channels as usize);
    for i in 0..frames {
        let v = ((i as f32) * 440.0 * 2.0 * std::f32::consts::PI / sample_rate as f32)
            .sin()
            .mul_add(16000.0, 0.0) as i16;
        samples.push(v);
        samples.push(v);
    }
    write_wav(&wav, sample_rate, &samples, channels);
    wav
}

#[test]
fn media_stream_flows_from_transmitter_to_member() {
    let _guard = SERIAL.lock().unwrap();
    let dir = std::env::temp_dir().join(format!("weft_media_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let wav = write_tone_wav(&dir, 1);

    let a = NetworkEngine::start_with("Alpha".to_string(), false).unwrap();
    let b = NetworkEngine::start_with("Beta".to_string(), false).unwrap();

    let sa = || a.status();
    let sb = || b.status();

    let formed = wait_until(Duration::from_secs(15), || {
        let sa = sa();
        let sb = sb();
        let (ra, rb) = (sa.role.as_str(), sb.role.as_str());
        let both_members = sa.members.len() == 2 && sb.members.len() == 2;
        ((ra == "coordinator" && rb == "member") || (ra == "member" && rb == "coordinator"))
            && both_members
    });
    assert!(formed, "no se formó la sesión: A={:?} B={:?}", sa(), sb());

    let (coord, member): (&NetworkEngine, &NetworkEngine) = if sa().role == "coordinator" {
        (&a, &b)
    } else {
        (&b, &a)
    };

    // El coordinador obtiene el token y transmite el archivo.
    coord.request_transmit();
    let granted = wait_until(Duration::from_secs(5), || {
        sa().transmitter_id == coord.status().device_id
    });
    assert!(granted, "el coordinador no obtuvo el token: {:?}", sa());

    coord.transmit_file(wav.to_str().unwrap()).expect("transmit_file falló");

    // El miembro recibe todos los paquetes (50 x 20ms).
    let received = wait_until(Duration::from_secs(15), || {
        member.media_stats().map(|m| m.received_packets >= 50).unwrap_or(false)
    });
    assert!(received, "el miembro no recibió el stream: {:?}", member.media_stats());

    // El reloj quedó sincronizado (NTP funcionó).
    let synced = wait_until(Duration::from_secs(10), || {
        member.media_stats().map(|m| m.clock_offset_us != 0).unwrap_or(false)
    });
    assert!(synced, "el miembro no sincronizó el reloj: {:?}", member.media_stats());

    let finished = wait_until(Duration::from_secs(15), || {
        coord.status().transmitter_id.is_empty()
    });
    assert!(finished, "el token no se liberó al terminar: {:?}", coord.status());

    a.stop();
    b.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn receiver_plays_stream_through_cpal() {
    let _guard = SERIAL.lock().unwrap();
    let dir = std::env::temp_dir().join(format!("weft_play_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let wav = write_tone_wav(&dir, 2);

    // El coordinador no necesita audio; el miembro sí lo habilita (cpal).
    // Alpha queda de coordinador primero para que el miembro con audio sea
    // siempre Beta (evita la carrera de bootstrap que dejaba al miembro sin
    // playback).
    let a = NetworkEngine::start_with("Alpha".to_string(), false).unwrap();
    let sa = || a.status();
    let a_is_coord = wait_until(Duration::from_secs(10), || {
        sa().role.as_str() == "coordinator"
    });
    assert!(a_is_coord, "Alpha no quedó de coordinadora: {:?}", sa());

    let b = NetworkEngine::start_with("Beta".to_string(), true).unwrap();
    let sb = || b.status();

    let formed = wait_until(Duration::from_secs(15), || {
        let sa = sa();
        let sb = sb();
        sa.role.as_str() == "coordinator"
            && sb.role.as_str() == "member"
            && sa.members.len() == 2
            && sb.members.len() == 2
    });
    assert!(formed, "no se formó la sesión: A={:?} B={:?}", sa(), sb());

    let coord = &a;
    let member = &b;

    coord.request_transmit();
    let granted = wait_until(Duration::from_secs(5), || {
        sa().transmitter_id == coord.status().device_id
    });
    assert!(granted, "el coordinador no obtuvo el token: {:?}", sa());

    coord.transmit_file(wav.to_str().unwrap()).expect("transmit_file falló");

    // El receptor reproduce: los paquetes se convierten en frames de audio real.
    let played = wait_until(Duration::from_secs(20), || {
        member
            .media_stats()
            .and_then(|m| m.playback)
            .map(|p| p.played_frames > 0 && p.buffered_packets == 0)
            .unwrap_or(false)
    });
    assert!(
        played,
        "el receptor no reprodujo el stream: {:?}",
        member.media_stats()
    );

    a.stop();
    b.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Three devices: the token passes from Beta (file transmitter) to Gamma
/// (member) via a coordinator-negotiated handoff (AskCede/CedeReply), and the
/// coordinator verifies the new transmitter actually streams (no rollback).
#[test]
fn coordinator_handoffs_token_to_requesting_member() {
    let _guard = SERIAL.lock().unwrap();
    let dir = std::env::temp_dir().join(format!("weft_handoff_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let wav = write_tone_wav(&dir, 5);

    let a = NetworkEngine::start_with("Alpha".to_string(), false).unwrap();

    let sa = || a.status();

    // Let Alpha become coordinator first (no peers yet), so Beta and Gamma join
    // a stable session instead of racing the mDNS bootstrap.
    let a_is_coord = wait_until(Duration::from_secs(10), || {
        sa().role.as_str() == "coordinator"
    });
    assert!(a_is_coord, "Alpha no quedó de coordinadora: {:?}", sa());

    let b = NetworkEngine::start_with("Beta".to_string(), false).unwrap();
    let c = NetworkEngine::start_with("Gamma".to_string(), false).unwrap();

    let sb = || b.status();
    let sc = || c.status();

    let formed = wait_until(Duration::from_secs(20), || {
        let roles = [sa().role.as_str().to_string(), sb().role.as_str().to_string(), sc().role.as_str().to_string()];
        let coord_count = roles.iter().filter(|r| r.as_str() == "coordinator").count();
        let member_count = roles.iter().filter(|r| r.as_str() == "member").count();
        let coordinator_sees_all = sa().members.len() == 3;
        coord_count == 1 && member_count == 2 && coordinator_sees_all
    });
    assert!(
        formed,
        "no se formó la sesión de 3: A={:?} B={:?} C={:?}",
        sa(),
        sb(),
        sc()
    );

    // Beta: the future transmitter, auto-cedes when the coordinator asks.
    let cede_req = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cede_flag = cede_req.clone();
    b.set_event_callback(Box::new(move |ev| {
        if ev.kind == "cede_asked" {
            cede_flag.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }));

    // Beta gets the token (token is free) and starts transmitting.
    b.request_transmit();
    let beta_has_token = wait_until(Duration::from_secs(5), || {
        sa().transmitter_id == b.status().device_id
            && sb().transmitter_id == b.status().device_id
            && sc().transmitter_id == b.status().device_id
    });
    assert!(beta_has_token, "Beta no obtuvo el token: {:?}", sa());
    b.transmit_file(wav.to_str().unwrap()).expect("transmit_file falló");

    // The receiver (Alpha, the coordinator) sees Beta's stream arrive.
    let receiving = wait_until(Duration::from_secs(15), || {
        a.media_stats()
            .map(|m| m.received_packets > 0 && m.last_source_id != 0)
            .unwrap_or(false)
    });
    assert!(receiving, "el coordinador no recibió el stream de Beta: {:?}", a.media_stats());

    // Gamma requests the token while Beta holds it -> handoff negotiation.
    c.request_transmit();
    let asked = wait_until(Duration::from_secs(5), || {
        cede_req.load(std::sync::atomic::Ordering::Relaxed)
    });
    assert!(asked, "el coordinador no le preguntó a Beta por el token: {:?}", sa());

    // Beta cedes -> coordinator revokes Beta and grants Gamma.
    b.respond_to_cede(true);
    let gamma_has_token = wait_until(Duration::from_secs(5), || {
        sa().transmitter_id == c.status().device_id
            && sb().transmitter_id == c.status().device_id
            && sc().transmitter_id == c.status().device_id
    });
    assert!(gamma_has_token, "Gamma no recibió el token: A={:?} B={:?} C={:?}", sa(), sb(), sc());

    // Gamma actually streams -> coordinator sees a *new* source and keeps the
    // token on Gamma (no rollback).
    let before_gamma = a.media_stats().map(|m| m.last_source_id).unwrap_or(0);
    c.transmit_file(wav.to_str().unwrap()).expect("transmit_file falló");
    let gamma_streams = wait_until(Duration::from_secs(10), || {
        a.media_stats()
            .map(|m| m.last_source_id != 0 && m.last_source_id != before_gamma)
            .unwrap_or(false)
    });
    assert!(gamma_streams, "el coordinador no vio el stream de Gamma: {:?}", a.media_stats());

    // After the rollback window, the token must still be on Gamma.
    std::thread::sleep(Duration::from_millis(3500));
    let still_gamma = {
        let s = sa();
        s.transmitter_id == c.status().device_id
    };
    assert!(still_gamma, "el token se revirtió por error: {:?}", sa());

    a.stop();
    b.stop();
    c.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

/// If a handed-off transmitter never starts streaming, the coordinator rolls
/// the token back to the previous holder.
#[test]
fn coordinator_rolls_back_token_when_new_transmitter_is_silent() {
    let _guard = SERIAL.lock().unwrap();
    let dir = std::env::temp_dir().join(format!("weft_rollback_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let wav = write_tone_wav(&dir, 5);

    let a = NetworkEngine::start_with("Alpha".to_string(), false).unwrap();
    let sa = || a.status();
    let a_is_coord = wait_until(Duration::from_secs(10), || {
        sa().role.as_str() == "coordinator"
    });
    assert!(a_is_coord, "Alpha no quedó de coordinadora: {:?}", sa());

    let b = NetworkEngine::start_with("Beta".to_string(), false).unwrap();
    let c = NetworkEngine::start_with("Gamma".to_string(), false).unwrap();
    let sb = || b.status();
    let sc = || c.status();

    let formed = wait_until(Duration::from_secs(20), || {
        let roles = [sa().role.as_str().to_string(), sb().role.as_str().to_string(), sc().role.as_str().to_string()];
        let coord_count = roles.iter().filter(|r| r.as_str() == "coordinator").count();
        let member_count = roles.iter().filter(|r| r.as_str() == "member").count();
        let coordinator_sees_all = sa().members.len() == 3;
        coord_count == 1 && member_count == 2 && coordinator_sees_all
    });
    assert!(formed, "no se formó la sesión de 3: {:?} {:?} {:?}", sa(), sb(), sc());

    let cede_req = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cede_flag = cede_req.clone();
    b.set_event_callback(Box::new(move |ev| {
        if ev.kind == "cede_asked" {
            cede_flag.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }));

    // Beta holds the token and streams.
    b.request_transmit();
    let beta_has_token = wait_until(Duration::from_secs(5), || {
        sa().transmitter_id == b.status().device_id
    });
    assert!(beta_has_token, "Beta no obtuvo el token: {:?}", sa());
    b.transmit_file(wav.to_str().unwrap()).expect("transmit_file falló");

    // Gamma requests and Beta cedes.
    c.request_transmit();
    let asked = wait_until(Duration::from_secs(5), || {
        cede_req.load(std::sync::atomic::Ordering::Relaxed)
    });
    assert!(asked, "el coordinador no le preguntó a Beta: {:?}", sa());
    b.respond_to_cede(true);
    let gamma_has_token = wait_until(Duration::from_secs(5), || {
        sa().transmitter_id == c.status().device_id
    });
    assert!(gamma_has_token, "Gamma no recibió el token: {:?}", sa());

    // Gamma never calls transmit_file -> after the rollback window the token
    // returns to Beta.
    let rolled_back = wait_until(Duration::from_secs(8), || {
        sa().transmitter_id == b.status().device_id
    });
    assert!(rolled_back, "el token no volvió a Beta: {:?}", sa());

    a.stop();
    b.stop();
    c.stop();
    let _ = std::fs::remove_dir_all(&dir);
}
