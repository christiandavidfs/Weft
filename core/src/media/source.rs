use std::path::Path;

use rubato::{FftFixedOut, Resampler};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{DecoderOptions, CODEC_TYPE_NULL};
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;

use crate::media::packet::AudioPacket;
use crate::media::{CHANNELS, FRAME_SAMPLES, FRAME_US, SAMPLE_RATE};

#[derive(Debug, Clone)]
pub struct PcmFrames {
    pub sample_rate: u32,
    pub channels: u16,
    pub samples: Vec<i16>,
}

/// Decode an audio file into interleaved native PCM via symphonia.
pub fn decode_file_to_pcm(path: &Path) -> Result<PcmFrames, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("no se pudo abrir el archivo: {e}"))?;
    let mss = MediaSourceStream::new(Box::new(file), Default::default());
    let mut hint = Hint::new();
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        hint.with_extension(ext);
    }
    let probed = symphonia::default::get_probe()
        .format(&hint, mss, &FormatOptions::default(), &MetadataOptions::default())
        .map_err(|e| format!("formato no soportado: {e}"))?;
    let mut format = probed.format;

    let track = format
        .tracks()
        .iter()
        .find(|t| t.codec_params.codec != CODEC_TYPE_NULL)
        .ok_or_else(|| "el archivo no tiene pista de audio".to_string())?;
    let track_id = track.id;

    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &DecoderOptions::default())
        .map_err(|e| format!("decoder no disponible: {e}"))?;

    let mut samples: Vec<i16> = Vec::new();
    let mut sample_rate = track.codec_params.sample_rate.unwrap_or(SAMPLE_RATE);
    let mut channels = track.codec_params.channels.map(|c| c.count() as u16).unwrap_or(CHANNELS);

    loop {
        let packet = match format.next_packet() {
            Ok(p) => p,
            Err(SymphoniaError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(SymphoniaError::ResetRequired) => continue,
            Err(e) => return Err(format!("error de decodificación: {e}")),
        };
        if packet.track_id() != track_id {
            continue;
        }
        match decoder.decode(&packet) {
            Ok(decoded) => {
                let spec = *decoded.spec();
                sample_rate = spec.rate;
                channels = spec.channels.count() as u16;
                let mut sbuf = SampleBuffer::<i16>::new(decoded.capacity() as u64, spec);
                sbuf.copy_interleaved_ref(decoded);
                samples.extend_from_slice(sbuf.samples());
            }
            Err(SymphoniaError::DecodeError(_)) => continue,
            Err(e) => return Err(format!("error de decodificación: {e}")),
        }
    }

    if samples.is_empty() {
        return Err("no se decodificó audio".to_string());
    }
    Ok(PcmFrames { sample_rate, channels, samples })
}

/// Downmix to stereo (duplicate mono, keep first two channels otherwise).
fn to_stereo(pcm: &PcmFrames) -> Vec<i16> {
    let ch = pcm.channels as usize;
    let mut out = Vec::with_capacity(pcm.samples.len() * 2 / ch.max(1));
    for frame in pcm.samples.chunks(ch.max(1)) {
        match ch {
            1 => {
                let s = frame[0];
                out.push(s);
                out.push(s);
            }
            2 => {
                out.push(frame[0]);
                out.push(frame[1]);
            }
            _ => {
                out.push(frame[0]);
                out.push(frame[1]);
            }
        }
    }
    out
}

/// Resample interleaved stereo i16 to 48kHz using rubato (FftFixedOut).
fn resample_to_48k(stereo: &[i16], in_rate: u32) -> Result<Vec<i16>, String> {
    if in_rate == SAMPLE_RATE {
        return Ok(stereo.to_vec());
    }
    let chunk_out = 4096usize;
    let mut resampler = FftFixedOut::<f32>::new(
        in_rate as usize,
        SAMPLE_RATE as usize,
        chunk_out,
        2,
        2,
    )
    .map_err(|e| format!("resampler: {e}"))?;

    let left: Vec<f32> = stereo.chunks(2).map(|f| f[0] as f32 / 32768.0).collect();
    let right: Vec<f32> = stereo.chunks(2).map(|f| f[1] as f32 / 32768.0).collect();

    let mut out_left: Vec<i16> = Vec::new();
    let mut out_right: Vec<i16> = Vec::new();

    let mut pos = 0usize;
    loop {
        let need = resampler.input_frames_next();
        if need == 0 {
            break;
        }
        let have = left.len().saturating_sub(pos);
        if have >= need {
            let input = vec![left[pos..pos + need].to_vec(), right[pos..pos + need].to_vec()];
            pos += need;
            let output = resampler.process(&input, None).map_err(|e| format!("resampler: {e}"))?;
            out_left.extend(quantize(&output[0]));
            out_right.extend(quantize(&output[1]));
        } else {
            let input = vec![left[pos..].to_vec(), right[pos..].to_vec()];
            let output = resampler
                .process_partial(Some(&input), None)
                .map_err(|e| format!("resampler: {e}"))?;
            let valid = if have > 0 { output[0].len() * have / need } else { 0 };
            out_left.extend(quantize(&output[0])[..valid].to_vec());
            out_right.extend(quantize(&output[1])[..valid].to_vec());
            break;
        }
    }

    let mut interleaved = Vec::with_capacity(out_left.len() * 2);
    for (l, r) in out_left.into_iter().zip(out_right) {
        interleaved.push(l);
        interleaved.push(r);
    }
    Ok(interleaved)
}

fn quantize(v: &[f32]) -> Vec<i16> {
    v.iter()
        .map(|s| (s.clamp(-1.0, 1.0) * 32767.0) as i16)
        .collect()
}

/// Converts decoded PCM to standard 48k stereo packets with session-clock pts.
pub struct PacketizedSource {
    samples: Vec<i16>,
    session_id: u64,
    base_pts_us: u64,
    start_seq: u32,
    seq: u32,
    pos: usize,
}

impl PacketizedSource {
    pub fn new(pcm: PcmFrames, session_id: u64, base_pts_us: u64, start_seq: u32) -> Result<Self, String> {
        let stereo = to_stereo(&pcm);
        let samples = resample_to_48k(&stereo, pcm.sample_rate)?;
        Ok(Self {
            samples,
            session_id,
            base_pts_us,
            start_seq,
            seq: start_seq,
            pos: 0,
        })
    }

    pub fn sample_rate(&self) -> u32 {
        SAMPLE_RATE
    }

    pub fn channels(&self) -> u16 {
        CHANNELS
    }

    pub fn total_duration_us(&self) -> u64 {
        (self.samples.len() / CHANNELS as usize) as u64 * 1_000_000 / SAMPLE_RATE as u64
    }

    pub fn remaining_frames(&self) -> usize {
        self.samples.len() / CHANNELS as usize - self.pos / CHANNELS as usize
    }

    pub fn next_packet(&mut self) -> Option<AudioPacket> {
        if self.pos >= self.samples.len() {
            return None;
        }
        let frame_cap = FRAME_SAMPLES * CHANNELS as usize;
        let end = (self.pos + frame_cap).min(self.samples.len());
        let chunk = self.samples[self.pos..end].to_vec();
        let seq = self.seq;
        let pts = self.base_pts_us + (seq - self.start_seq) as u64 * FRAME_US;
        self.seq = seq.wrapping_add(1);
        self.pos = end;
        Some(AudioPacket::new(self.session_id, seq, pts, chunk, SAMPLE_RATE, CHANNELS))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_wav(path: &Path, rate: u32, channels: u16, samples: &[i16]) {
        let block_align = channels * 2;
        let data_len = (samples.len() * 2) as u32;
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(b"RIFF").unwrap();
        f.write_all(&(36 + data_len).to_le_bytes()).unwrap();
        f.write_all(b"WAVE").unwrap();
        f.write_all(b"fmt ").unwrap();
        f.write_all(&16u32.to_le_bytes()).unwrap();
        f.write_all(&1u16.to_le_bytes()).unwrap();
        f.write_all(&channels.to_le_bytes()).unwrap();
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

    #[test]
    fn decode_48k_wav_packetizes_contiguously() {
        let dir = std::env::temp_dir().join("weft-tests");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tone.wav");
        let rate = 48_000u32;
        let samples: Vec<i16> = (0..(rate as usize * 3))
            .map(|i| ((i % 200) as i16).wrapping_sub(100))
            .collect();
        write_wav(&path, rate, 1, &samples);

        let pcm = decode_file_to_pcm(&path).unwrap();
        assert_eq!(pcm.sample_rate, rate);
        assert_eq!(pcm.samples.len(), samples.len());

        let mut src = PacketizedSource::new(pcm, 7, 1_000_000, 10).unwrap();
        assert_eq!(src.total_duration_us(), 3_000_000);
        let mut prev_pts = 1_000_000u64;
        let mut seq = 10u32;
        let mut total_frames = 0usize;
        while let Some(pkt) = src.next_packet() {
            assert_eq!(pkt.pts_us, prev_pts);
            prev_pts += FRAME_US;
            assert_eq!(pkt.seq, seq);
            seq += 1;
            assert!(pkt.frames() <= FRAME_SAMPLES);
            total_frames += pkt.frames();
        }
        assert_eq!(total_frames, rate as usize * 3);
        assert_eq!(prev_pts - FRAME_US, 1_000_000 + 3_000_000 - FRAME_US);
    }

    #[test]
    fn decode_44k1_wav_resamples_to_48k() {
        let dir = std::env::temp_dir().join("weft-tests");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tone441.wav");
        let rate = 44_100u32;
        let samples: Vec<i16> = (0..(rate as usize)).map(|_| 42).collect();
        write_wav(&path, rate, 1, &samples);

        let pcm = decode_file_to_pcm(&path).unwrap();
        let mut src = PacketizedSource::new(pcm, 1, 0, 0).unwrap();
        let mut frames = 0usize;
        while let Some(pkt) = src.next_packet() {
            assert!(pkt.is_standard());
            frames += pkt.frames();
        }
        // ~48k frames for 1s of audio.
        assert!((48_000 - frames as i64).abs() < 2_000, "frames {frames}");
    }
}
