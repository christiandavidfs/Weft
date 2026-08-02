use std::collections::VecDeque;

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, SampleFormat, StreamConfig};

use crate::media::{CHANNELS, SAMPLE_RATE};

/// List the names of all input (capture) devices available to cpal.
pub fn input_devices() -> Vec<String> {
    let host = cpal::default_host();
    let Ok(devices) = host.input_devices() else {
        return Vec::new();
    };
    devices
        .filter_map(|d| d.name().ok())
        .filter(|n| !n.is_empty())
        .collect()
}

/// A running capture stream: keeps the cpal `Stream` alive and yields chunks of
/// stereo i16 samples (at the device's sample rate) over a bounded channel.
pub struct CaptureStream {
    pub stream: cpal::Stream,
    pub rx: flume::Receiver<Vec<i16>>,
    pub rate: u32,
}

/// Check (without opening a stream) that the requested input device exists.
/// Used to fail fast before spawning the capture thread.
pub fn check_input_device(device_name: Option<&str>) -> Result<(), String> {
    let host = cpal::default_host();
    match device_name {
        Some(name) => {
            let found = host
                .input_devices()
                .map_err(|e| format!("sin dispositivos de entrada: {e}"))?
                .any(|d| d.name().ok().as_deref() == Some(name));
            if found {
                Ok(())
            } else {
                Err(format!("dispositivo de entrada no encontrado: {name}"))
            }
        }
        None => {
            if host.default_input_device().is_some() {
                Ok(())
            } else {
                Err("no hay dispositivo de entrada por defecto".to_string())
            }
        }
    }
}

/// Open an input device (by name, or the default) and start capturing.
/// The callback runs on the audio thread, so it only downmixes to stereo and
/// pushes a chunk (non-blocking, drops if the queue is full).
pub fn open_capture(device_name: Option<&str>) -> Result<CaptureStream, String> {
    let host = cpal::default_host();
    let device: Device = match device_name {
        Some(name) => host
            .input_devices()
            .map_err(|e| format!("sin dispositivos de entrada: {e}"))?
            .find(|d| d.name().ok().as_deref() == Some(name))
            .ok_or_else(|| format!("dispositivo de entrada no encontrado: {name}"))?,
        None => host
            .default_input_device()
            .ok_or_else(|| "no hay dispositivo de entrada por defecto".to_string())?,
    };

    let mut configs: Vec<_> = device
        .supported_input_configs()
        .map_err(|e| format!("sin configuraciones de entrada: {e}"))?
        .collect();
    configs.sort_by_key(|c| {
        let fmt_i16 = c.sample_format() == SampleFormat::I16;
        let rate_48 = (c.min_sample_rate().0..=c.max_sample_rate().0).contains(&SAMPLE_RATE);
        let ch_2 = c.channels() >= 2;
        (!fmt_i16, !rate_48, !ch_2)
    });
    let supported = configs
        .into_iter()
        .next()
        .ok_or_else(|| "el dispositivo no expone configuraciones".to_string())?;

    let config: StreamConfig = supported
        .try_with_sample_rate(cpal::SampleRate(SAMPLE_RATE))
        .map(|c| c.config())
        .unwrap_or_else(|| supported.with_max_sample_rate().config());
    let rate = config.sample_rate.0;
    let channels = config.channels.max(1) as usize;

    let (tx, rx) = flume::bounded::<Vec<i16>>(8);
    let stream = match supported.sample_format() {
        SampleFormat::I16 => build_stream_i16(&device, &config, channels, tx)?,
        SampleFormat::F32 => build_stream_f32(&device, &config, channels, tx)?,
        SampleFormat::F64 => build_stream_f64(&device, &config, channels, tx)?,
        other => return Err(format!("formato de entrada no soportado: {other}")),
    };
    stream
        .play()
        .map_err(|e| format!("no se pudo iniciar la captura: {e}"))?;

    Ok(CaptureStream { stream, rx, rate })
}

fn build_stream_i16(
    device: &Device,
    config: &StreamConfig,
    channels: usize,
    tx: flume::Sender<Vec<i16>>,
) -> Result<cpal::Stream, String> {
    let err_tx = tx.clone();
    device
        .build_input_stream(
            config,
            move |data: &[i16], _| {
                let chunk = to_stereo_i16_i16(data, channels);
                let _ = tx.try_send(chunk);
            },
            move |_err| {
                let _ = err_tx.try_send(vec![]);
            },
            None,
        )
        .map_err(|e| format!("no se pudo abrir el dispositivo de entrada: {e}"))
}

fn build_stream_f32(
    device: &Device,
    config: &StreamConfig,
    channels: usize,
    tx: flume::Sender<Vec<i16>>,
) -> Result<cpal::Stream, String> {
    let err_tx = tx.clone();
    device
        .build_input_stream(
            config,
            move |data: &[f32], _| {
                let chunk = to_stereo_i16_f32(data, channels);
                let _ = tx.try_send(chunk);
            },
            move |_err| {
                let _ = err_tx.try_send(vec![]);
            },
            None,
        )
        .map_err(|e| format!("no se pudo abrir el dispositivo de entrada: {e}"))
}

fn build_stream_f64(
    device: &Device,
    config: &StreamConfig,
    channels: usize,
    tx: flume::Sender<Vec<i16>>,
) -> Result<cpal::Stream, String> {
    let err_tx = tx.clone();
    device
        .build_input_stream(
            config,
            move |data: &[f64], _| {
                let chunk = to_stereo_i16_f64(data, channels);
                let _ = tx.try_send(chunk);
            },
            move |_err| {
                let _ = err_tx.try_send(vec![]);
            },
            None,
        )
        .map_err(|e| format!("no se pudo abrir el dispositivo de entrada: {e}"))
}

/// Downmix an interleaved input buffer to stereo i16 (mono duplicated, >2ch
/// takes the first two).
#[inline]
fn to_stereo_i16_i16(data: &[i16], channels: usize) -> Vec<i16> {
    let mut out = Vec::with_capacity(data.len() / channels.max(1) * 2);
    for frame in data.chunks(channels.max(1)) {
        match channels {
            1 => {
                let v = frame[0];
                out.push(v);
                out.push(v);
            }
            _ => {
                out.push(frame[0]);
                out.push(frame[1]);
            }
        }
    }
    out
}

#[inline]
fn to_stereo_i16_f32(data: &[f32], channels: usize) -> Vec<i16> {
    let mut out = Vec::with_capacity(data.len() / channels.max(1) * 2);
    for frame in data.chunks(channels.max(1)) {
        match channels {
            1 => {
                let v = (frame[0].clamp(-1.0, 1.0) * 32767.0) as i16;
                out.push(v);
                out.push(v);
            }
            _ => {
                out.push((frame[0].clamp(-1.0, 1.0) * 32767.0) as i16);
                out.push((frame[1].clamp(-1.0, 1.0) * 32767.0) as i16);
            }
        }
    }
    out
}

#[inline]
fn to_stereo_i16_f64(data: &[f64], channels: usize) -> Vec<i16> {
    let mut out = Vec::with_capacity(data.len() / channels.max(1) * 2);
    for frame in data.chunks(channels.max(1)) {
        match channels {
            1 => {
                let v = (frame[0].clamp(-1.0, 1.0) * 32767.0) as i16;
                out.push(v);
                out.push(v);
            }
            _ => {
                out.push((frame[0].clamp(-1.0, 1.0) * 32767.0) as i16);
                out.push((frame[1].clamp(-1.0, 1.0) * 32767.0) as i16);
            }
        }
    }
    out
}

/// Streaming linear-interpolation resampler for interleaved stereo PCM.
/// Used when the capture device doesn't run at 48 kHz.
///
/// `phase` is the fractional position within the current first frame of `buf`
/// (0.0..1.0). After each output the phase advances by `ratio` input frames;
/// when it crosses an integer, those leading frames are drained so `buf[0]`
/// always represents the current input frame.
pub struct StreamResampler {
    ratio: f64,
    buf: VecDeque<i16>,
    phase: f64,
}

impl StreamResampler {
    pub fn new(in_rate: u32, out_rate: u32) -> Self {
        let ratio = in_rate as f64 / out_rate as f64;
        Self {
            ratio,
            buf: VecDeque::new(),
            phase: 0.0,
        }
    }

    pub fn push(&mut self, stereo: &[i16]) {
        self.buf.extend(stereo);
    }

    pub fn input_frames(&self) -> usize {
        self.buf.len() / 2
    }

    /// How many more input frames are needed before `out_frames` output frames
    /// can be produced. Assumes the boundary frame is consumed without
    /// interpolation (see `take`), so it only needs `ceil(last_in)` frames.
    pub fn frames_needed_for(&self, out_frames: usize) -> usize {
        let last_in = self.phase + (out_frames as f64 - 1.0) * self.ratio;
        let needed = last_in.ceil() as usize;
        needed.saturating_sub(self.input_frames())
    }

    /// Produce up to `out_frames` frames (interleaved stereo). Returns fewer if
    /// the input buffer doesn't have enough samples yet. The final output of a
    /// run may use the last available frame without interpolation, so a full
    /// pass-through yields an exact frame count.
    pub fn take(&mut self, out_frames: usize) -> Vec<i16> {
        let mut out = Vec::with_capacity(out_frames * 2);
        while out.len() / 2 < out_frames {
            if self.input_frames() < 1 {
                break;
            }
            let (l, r) = if self.input_frames() >= 2 {
                let frac = self.phase;
                let l = self.buf[0] as f64;
                let r = self.buf[1] as f64;
                let l2 = self.buf[2] as f64;
                let r2 = self.buf[3] as f64;
                (interp(l, l2, frac), interp(r, r2, frac))
            } else {
                (self.buf[0], self.buf[1])
            };
            out.push(l);
            out.push(r);
            self.phase += self.ratio;
            let consumed = self.phase.floor() as usize;
            if consumed > 0 {
                self.buf.drain(..consumed * 2);
                self.phase -= consumed as f64;
            }
        }
        out
    }
}

fn interp(a: f64, b: f64, frac: f64) -> i16 {
    let v = a + (b - a) * frac;
    v.clamp(-32768.0, 32767.0).round() as i16
}

pub fn silence_frame() -> Vec<i16> {
    vec![0i16; crate::media::FRAME_SAMPLES * CHANNELS as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downmix_mono_duplicates() {
        let input = vec![0.5f32, -0.5f32];
        let out = to_stereo_i16_f32(&input, 1);
        assert_eq!(out.len(), 4);
        assert_eq!(out[0], out[1]);
        assert_eq!(out[2], out[3]);
        assert!(out[0] > 16000);
        assert!(out[2] < -16000);
    }

    #[test]
    fn downmix_stereo_keeps_channels() {
        let input = vec![0.5f32, -0.5f32, 0.25f32, 0.0f32];
        let out = to_stereo_i16_f32(&input, 2);
        assert_eq!(out.len(), 4);
        assert_eq!(out[0], (0.5 * 32767.0) as i16);
        assert_eq!(out[1], (-0.5 * 32767.0) as i16);
    }

    #[test]
    fn resampler_44100_to_48000_preserves_duration() {
        let in_rate = 44_100u32;
        let out_rate = 48_000u32;
        let mut r = StreamResampler::new(in_rate, out_rate);
        let signal: Vec<i16> = (0..(in_rate as usize) * 2)
            .map(|i| if i % 2 == 0 { 1000 } else { -1000 })
            .collect();
        r.push(&signal);
        let out = r.take(out_rate as usize);
        assert_eq!(out.len(), out_rate as usize * 2);
        assert!(out.iter().step_by(2).all(|&s| s > 900));
        assert!(out.iter().skip(1).step_by(2).all(|&s| s < -900));
    }

    #[test]
    fn resampler_48000_passthrough() {
        let mut r = StreamResampler::new(48_000, 48_000);
        let signal: Vec<i16> = (0..(48_000usize) * 2).map(|i| (i as i16) % 7).collect();
        r.push(&signal);
        let out = r.take(48_000);
        assert_eq!(out.len(), 48_000 * 2);
    }

    #[test]
    fn resampler_underruns_returns_partial() {
        let mut r = StreamResampler::new(48_000, 48_000);
        r.push(&vec![5i16; 64]);
        let out = r.take(960);
        assert!(out.len() < 960 * 2);
        assert_eq!(out.len() % 2, 0);
        assert!(r.frames_needed_for(960) > 0);
    }
}
