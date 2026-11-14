//! Audio for Zello: the recorder's voice (8 kHz) up to 48 kHz, then Opus,
//! the way the Node connector this plugin replaced did it, which Zello's apps play well.

use std::collections::VecDeque;
use std::f64::consts::PI;

use unsafe_libopus::opus_encoder_ctl;

/// Zello gets 48 kHz audio in 20 ms Opus packets, at 24 kbps.
pub const RATE: u32 = 48_000;
pub const FRAME: usize = 960;
pub const FRAME_MS: u64 = 20;
const BITRATE: i32 = 24_000;
/// `start_stream`'s codec_header: 48000 Hz (little-endian), 1 frame per packet, 20 ms frames.
pub const CODEC_HEADER: &str = "gLsBFA==";

/// Input samples per output phase: a 96-tap filter at 8 kHz.
const TAPS: usize = 16;

/// Raises a sample rate that divides 48 kHz to 48 kHz, with a windowed-sinc
/// anti-imaging filter. (Plain interpolation leaves mirror images of the voice
/// above 4 kHz, which sound tinny and harsh.)
pub struct Upsampler {
    l: usize,
    h: Vec<f64>,
    /// The last input samples, newest first.
    hist: [f64; TAPS],
}

impl Upsampler {
    /// None for a rate that doesn't divide 48 kHz.
    pub fn new(input_rate: u32) -> Option<Upsampler> {
        if input_rate == 0 || input_rate > RATE || !RATE.is_multiple_of(input_rate) {
            return None;
        }
        let l = (RATE / input_rate) as usize;
        let n = l * TAPS;
        let fc = 0.45 * f64::from(input_rate) / f64::from(RATE); // 3.6 kHz for 8 kHz voice
        let h = (0..n)
            .map(|i| {
                let t = i as f64 - (n - 1) as f64 / 2.0;
                let sinc = if t == 0.0 { 2.0 * fc } else { (2.0 * PI * fc * t).sin() / (PI * t) };
                let x = i as f64 / (n - 1) as f64;
                let blackman = 0.42 - 0.5 * (2.0 * PI * x).cos() + 0.08 * (4.0 * PI * x).cos();
                sinc * blackman * l as f64
            })
            .collect();
        Some(Upsampler { l, h, hist: [0.0; TAPS] })
    }

    pub fn push(&mut self, input: &[i16], out: &mut VecDeque<i16>) {
        for &s in input {
            self.hist.rotate_right(1);
            self.hist[0] = f64::from(s);
            for k in 0..self.l {
                let acc: f64 = (0..TAPS).map(|j| self.h[j * self.l + k] * self.hist[j]).sum();
                out.push_back(acc.round().clamp(-32768.0, 32767.0) as i16);
            }
        }
    }

    /// The filter's tail, at the end of a transmission.
    pub fn flush(&mut self, out: &mut VecDeque<i16>) {
        self.push(&[0; TAPS / 2], out);
    }
}

/// An Opus encoder: 48 kHz mono, tuned for voice.
pub struct Encoder(*mut unsafe_libopus::OpusEncoder);

// The encoder is used by one thread at a time (each Zello connection has its own).
unsafe impl Send for Encoder {}

impl Encoder {
    pub fn new() -> Result<Encoder, String> {
        let mut err = 0;
        // SAFETY: valid arguments; the result is checked before use.
        let st = unsafe { unsafe_libopus::opus_encoder_create(RATE as i32, 1, unsafe_libopus::OPUS_APPLICATION_VOIP, &mut err) };
        if st.is_null() || err != unsafe_libopus::OPUS_OK {
            return Err(format!("couldn't start the Opus encoder (error {err})"));
        }
        // SAFETY: `st` is a live encoder.
        unsafe { opus_encoder_ctl!(st, unsafe_libopus::OPUS_SET_BITRATE_REQUEST, BITRATE) };
        Ok(Encoder(st))
    }

    /// Starts afresh, for a new transmission.
    pub fn reset(&mut self) {
        // SAFETY: `self.0` is a live encoder.
        unsafe { opus_encoder_ctl!(self.0, unsafe_libopus::OPUS_RESET_STATE) };
    }

    /// One 20 ms frame ([`FRAME`] samples) as an Opus packet.
    pub fn encode(&mut self, frame: &[i16]) -> Result<Vec<u8>, String> {
        assert_eq!(frame.len(), FRAME);
        let mut out = vec![0u8; 1500];
        // SAFETY: `frame` holds FRAME samples, `out` has room for `out.len()` bytes.
        let n = unsafe { unsafe_libopus::opus_encode(self.0, frame.as_ptr(), FRAME as i32, out.as_mut_ptr(), out.len() as i32) };
        if n < 0 {
            return Err(format!("Opus couldn't encode audio (error {n})"));
        }
        out.truncate(n as usize);
        Ok(out)
    }
}

impl Drop for Encoder {
    fn drop(&mut self) {
        // SAFETY: made by opus_encoder_create, destroyed once.
        unsafe { unsafe_libopus::opus_encoder_destroy(self.0) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tone(hz: f64, rate: u32, n: usize, amp: f64) -> Vec<i16> {
        (0..n).map(|i| (amp * (2.0 * PI * hz * i as f64 / f64::from(rate)).sin()) as i16).collect()
    }

    /// The level of `hz` in `x` (one DFT bin), as an amplitude.
    fn level(x: &[i16], rate: u32, hz: f64) -> f64 {
        let (mut re, mut im) = (0.0, 0.0);
        for (i, &s) in x.iter().enumerate() {
            let a = 2.0 * PI * hz * i as f64 / f64::from(rate);
            re += f64::from(s) * a.cos();
            im += f64::from(s) * a.sin();
        }
        2.0 * (re * re + im * im).sqrt() / x.len() as f64
    }

    #[test]
    fn upsampling_keeps_the_voice_and_drops_its_images() {
        let mut up = Upsampler::new(8000).unwrap();
        let mut out = VecDeque::new();
        up.push(&tone(1000.0, 8000, 8000, 10_000.0), &mut out);
        assert_eq!(out.len(), 48_000);
        let y: Vec<i16> = out.into_iter().skip(1000).collect();
        let voice = level(&y, RATE, 1000.0);
        assert!((voice - 10_000.0).abs() < 500.0, "1 kHz at {voice:.0}, not about 10000");
        // 8 kHz voice mirrors to 7, 9, 15, 17 kHz when it's raised badly.
        for image in [7000.0, 9000.0, 15000.0, 17000.0] {
            let l = level(&y, RATE, image);
            assert!(l < 100.0, "image at {image} Hz is {l:.0}");
        }
    }

    #[test]
    fn rates_that_dont_divide_48_khz_are_refused() {
        assert!(Upsampler::new(8000).is_some());
        assert!(Upsampler::new(16000).is_some());
        assert!(Upsampler::new(22050).is_none());
        assert!(Upsampler::new(0).is_none());
    }

    #[test]
    fn opus_encodes_and_decodes_voice() {
        let mut enc = Encoder::new().unwrap();
        let mut err = 0;
        let dec = unsafe { unsafe_libopus::opus_decoder_create(RATE as i32, 1, &mut err) };
        assert_eq!(err, unsafe_libopus::OPUS_OK);
        let x = tone(1000.0, RATE, FRAME * 50, 8000.0);
        let mut decoded = Vec::new();
        for f in x.chunks(FRAME) {
            let packet = enc.encode(f).unwrap();
            assert!(!packet.is_empty() && packet.len() < 200, "a 20 ms packet at 24 kbps is about 60 bytes, not {}", packet.len());
            let mut pcm = vec![0i16; FRAME];
            let n = unsafe { unsafe_libopus::opus_decode(dec, packet.as_ptr(), packet.len() as i32, pcm.as_mut_ptr(), FRAME as i32, 0) };
            assert_eq!(n, FRAME as i32);
            decoded.extend_from_slice(&pcm);
        }
        unsafe { unsafe_libopus::opus_decoder_destroy(dec) };
        let l = level(&decoded[FRAME * 10..], RATE, 1000.0);
        assert!((l - 8000.0).abs() < 1600.0, "1 kHz came back at {l:.0}, not about 8000");
        enc.reset();
        assert!(enc.encode(&vec![0; FRAME]).is_ok());
    }
}
