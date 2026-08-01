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

#[test]
fn media_stream_flows_from_transmitter_to_member() {
    let _guard = SERIAL.lock().unwrap();
    let dir = std::env::temp_dir().join(format!("weft_media_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let wav = dir.join("tone.wav");
    let sample_rate = 48000u32;
    let channels = 2u16;
    let frames = sample_rate as usize; // 1 segundo
    let mut samples = Vec::with_capacity(frames * channels as usize);
    for i in 0..frames {
        let v = ((i as f32) * 440.0 * 2.0 * std::f32::consts::PI / sample_rate as f32)
            .sin()
            .mul_add(16000.0, 0.0) as i16;
        samples.push(v);
        samples.push(v);
    }
    write_wav(&wav, sample_rate, &samples, channels);

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
