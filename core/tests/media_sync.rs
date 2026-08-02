use std::io::Write;
use std::net::SocketAddr;
use std::time::Duration;

use weft_core::media::{MediaEngine, MemberMedia};

fn write_wav(path: &std::path::Path, rate: u32, samples: &[i16]) {
    let block_align = 2u16;
    let data_len = (samples.len() * 2) as u32;
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(b"RIFF").unwrap();
    f.write_all(&(36 + data_len).to_le_bytes()).unwrap();
    f.write_all(b"WAVE").unwrap();
    f.write_all(b"fmt ").unwrap();
    f.write_all(&16u32.to_le_bytes()).unwrap();
    f.write_all(&1u16.to_le_bytes()).unwrap();
    f.write_all(&1u16.to_le_bytes()).unwrap();
    f.write_all(&rate.to_le_bytes()).unwrap();
    f.write_all(&(rate * block_align as u32).to_le_bytes()).unwrap();
    f.write_all(&block_align.to_le_bytes()).unwrap();
    f.write_all(&16u16.to_le_bytes()).unwrap();
    f.write_all(b"data").unwrap();
    f.write_all(&data_len.to_le_bytes()).unwrap();
    for s in samples {
        f.write_all(&s.to_le_bytes()).unwrap();
    }
}

fn wait_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    false
}

#[test]
fn transmitter_streams_packets_to_receiver_over_udp() {
    let dir = std::env::temp_dir().join("weft-media-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let wav = dir.join("sync_tone.wav");
    let rate = 48_000u32;
    let samples: Vec<i16> = (0..rate as usize)
        .map(|i| ((i * 7 % 200) as i16).wrapping_sub(100))
        .collect();
    write_wav(&wav, rate, &samples);

    let tx = MediaEngine::new(false).unwrap();
    let rx = MediaEngine::new(false).unwrap();
    let tag = 42u64;
    let rx_addr = SocketAddr::from(([127, 0, 0, 1], rx.media_port()));

    tx.set_session(tag, true);
    rx.set_session(tag, false);
    tx.update_members(vec![MemberMedia {
        device_id: "receiver".to_string(),
        media_addr: rx_addr,
    }]);
    tx.set_transmitter(true);

    tx.transmit_file(wav.to_str().unwrap()).unwrap();

    let tx_path = wav.clone();
    // Expected packets for 1s at 20ms frames.
    let expected = 50u64;
    let done = wait_until(
        || tx.stats().transmitted_packets >= expected,
        Duration::from_secs(5),
    );
    assert!(done, "transmisor no terminó: {:?}", tx.stats());

    let received = wait_until(
        || rx.stats().received_packets >= expected,
        Duration::from_secs(5),
    );
    assert!(
        received,
        "receptor no recibió todos: tx={} rx={:?}",
        tx.stats().transmitted_packets,
        rx.stats()
    );

    let _ = std::fs::remove_file(&tx_path);
}

/// Fase 5: fan-out del plano de medios a 10+ receptores. Un único transmisor
/// envía cada frame a todos los miembros; todos deben recibir el stream
/// completo sin pérdidas de fan-out.
#[test]
fn transmitter_streams_to_ten_receivers() {
    let dir = std::env::temp_dir().join("weft-media-scale");
    std::fs::create_dir_all(&dir).unwrap();
    let wav = dir.join("scale_tone.wav");
    let rate = 48_000u32;
    let samples: Vec<i16> = (0..rate as usize)
        .map(|i| ((i * 11 % 200) as i16).wrapping_sub(100))
        .collect();
    write_wav(&wav, rate, &samples);

    let tx = MediaEngine::new(false).unwrap();
    let tag = 77u64;
    tx.set_session(tag, true);
    tx.set_transmitter(true);

    let rx_count = 10usize;
    let mut rxs: Vec<MediaEngine> = Vec::new();
    let mut members = Vec::new();
    for i in 0..rx_count {
        let rx = MediaEngine::new(false).unwrap();
        rx.set_session(tag, false);
        members.push(MemberMedia {
            device_id: format!("receiver-{i}"),
            media_addr: SocketAddr::from(([127, 0, 0, 1], rx.media_port())),
        });
        rxs.push(rx);
    }
    tx.update_members(members);

    tx.transmit_file(wav.to_str().unwrap()).unwrap();

    let expected = 50u64; // 1s at 20ms frames.
    let done = wait_until(
        || tx.stats().transmitted_packets >= expected,
        Duration::from_secs(10),
    );
    assert!(done, "transmisor no terminó: {:?}", tx.stats());

    for (i, rx) in rxs.iter().enumerate() {
        let received = wait_until(
            || rx.stats().received_packets >= expected,
            Duration::from_secs(10),
        );
        assert!(
            received,
            "receptor {i} no recibió todo: rx={:?}",
            rx.stats()
        );
    }

    let _ = std::fs::remove_file(&wav);
}

/// Fase 5: con un MediaConfig con latencia objetivo más alta (150ms) el
/// transmisor programa los frames más adelante en la línea de sesión y el
/// receptor aún recibe el stream completo.
#[test]
fn configurable_target_latency_still_streams() {
    use weft_core::media::MediaConfig;

    let dir = std::env::temp_dir().join("weft-media-latency");
    std::fs::create_dir_all(&dir).unwrap();
    let wav = dir.join("latency_tone.wav");
    let rate = 48_000u32;
    let samples: Vec<i16> = (0..rate as usize)
        .map(|i| ((i * 5 % 200) as i16).wrapping_sub(100))
        .collect();
    write_wav(&wav, rate, &samples);

    let config = MediaConfig {
        target_latency_us: 150_000,
        ..MediaConfig::default()
    };
    let tx = MediaEngine::new_with_config(false, config).unwrap();
    let rx = MediaEngine::new_with_config(false, config).unwrap();
    let tag = 78u64;
    let rx_addr = SocketAddr::from(([127, 0, 0, 1], rx.media_port()));

    tx.set_session(tag, true);
    rx.set_session(tag, false);
    tx.update_members(vec![MemberMedia {
        device_id: "receiver".to_string(),
        media_addr: rx_addr,
    }]);
    tx.set_transmitter(true);

    tx.transmit_file(wav.to_str().unwrap()).unwrap();

    let expected = 50u64;
    let received = wait_until(
        || rx.stats().received_packets >= expected,
        Duration::from_secs(10),
    );
    assert!(
        received,
        "receptor no recibió con latencia 150ms: tx={} rx={:?}",
        tx.stats().transmitted_packets,
        rx.stats()
    );

    let _ = std::fs::remove_file(&wav);
}
