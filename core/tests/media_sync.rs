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
