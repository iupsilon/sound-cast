//! A small streaming FLAC encoder for the `--codec flac` streams.
//!
//! The stream has no length and no MD5, as a live stream must. Every frame
//! holds [`BLOCK`] frames of 48 kHz stereo 16-bit audio. Each channel is coded
//! with the best of the fixed predictors (orders 0 to 4) and partitioned Rice
//! residuals; the stereo pair is coded as left/right, left/side, side/right or
//! mid/side, whichever is smallest. That is FLAC's "fast" level: about half the
//! size of WAV for music, next to nothing for silence, and cheap on the CPU.

use crate::hub::{CHANNELS, RATE};

/// Frames per FLAC frame: 4096 at 48 kHz is 85 ms.
pub const BLOCK: usize = 4096;
/// Block size code for 4096 in the frame header.
const BLOCK_CODE: u8 = 0b1100;
/// Sample rate code for 48 kHz in the frame header.
const RATE_CODE: u8 = 0b1010;
/// Sample size code for 16 bits in the frame header.
const SIZE_CODE: u8 = 0b100;
const BITS: u32 = 16;
const MAX_ORDER: usize = 4;
const MAX_PARTITION_ORDER: u32 = 6;
/// Largest Rice parameter with the 4-bit parameter field (15 means escape).
const MAX_RICE: u32 = 14;

/// The stream header: the `fLaC` marker and a STREAMINFO block.
pub fn header() -> Vec<u8> {
    let mut out = b"fLaC".to_vec();
    // Last metadata block, type 0 (STREAMINFO), 34 bytes.
    out.extend_from_slice(&[0x80, 0, 0, 34]);
    let mut bits = BitWriter::default();
    bits.put(BLOCK as u64, 16); // minimum block size
    bits.put(BLOCK as u64, 16); // maximum block size
    bits.put(0, 24); // minimum frame size: unknown
    bits.put(0, 24); // maximum frame size: unknown
    bits.put(u64::from(RATE), 20);
    bits.put(u64::from(CHANNELS - 1), 3);
    bits.put(u64::from(BITS - 1), 5);
    bits.put(0, 36); // total samples: unknown, the stream is endless
    out.extend_from_slice(&bits.finish());
    out.extend_from_slice(&[0; 16]); // MD5: unknown
    out
}

/// Turns S16LE stereo PCM into FLAC frames, [`BLOCK`] frames at a time.
#[derive(Default)]
pub struct Encoder {
    left: Vec<i32>,
    right: Vec<i32>,
    frame_number: u64,
    /// The bytes of a PCM frame split between two pushes.
    partial: Vec<u8>,
}

impl Encoder {
    /// Takes PCM and appends the frames it completes to `out`.
    pub fn push(&mut self, pcm: &[u8], out: &mut Vec<u8>) {
        let joined;
        let pcm = if self.partial.is_empty() {
            pcm
        } else {
            self.partial.extend_from_slice(pcm);
            joined = std::mem::take(&mut self.partial);
            &joined[..]
        };
        let (frames, rest) = pcm.as_chunks::<4>();
        self.partial.extend_from_slice(rest);
        for frame in frames {
            self.left.push(i32::from(i16::from_le_bytes([frame[0], frame[1]])));
            self.right.push(i32::from(i16::from_le_bytes([frame[2], frame[3]])));
            if self.left.len() == BLOCK {
                self.encode_block(out);
                self.left.clear();
                self.right.clear();
            }
        }
    }

    fn encode_block(&mut self, out: &mut Vec<u8>) {
        let (left, right) = (&self.left, &self.right);
        let mid: Vec<i32> = left.iter().zip(right).map(|(l, r)| (l + r) >> 1).collect();
        let side: Vec<i32> = left.iter().zip(right).map(|(l, r)| l - r).collect();
        let l = Subframe::best(left, BITS);
        let r = Subframe::best(right, BITS);
        let m = Subframe::best(&mid, BITS);
        let s = Subframe::best(&side, BITS + 1);
        // Channel assignment: 1 left/right, 8 left/side, 9 side/right, 10 mid/side.
        let choices = [
            (0b0001, &l, &r, l.bits + r.bits),
            (0b1000, &l, &s, l.bits + s.bits),
            (0b1001, &s, &r, s.bits + r.bits),
            (0b1010, &m, &s, m.bits + s.bits),
        ];
        let (assignment, first, second, _) = choices.into_iter().min_by_key(|c| c.3).unwrap();

        let start = out.len();
        let mut bits = BitWriter::default();
        bits.put(0b11_1111_1111_1110, 14); // sync code
        bits.put(0, 1); // reserved
        bits.put(0, 1); // fixed block size
        bits.put(u64::from(BLOCK_CODE), 4);
        bits.put(u64::from(RATE_CODE), 4);
        bits.put(assignment, 4);
        bits.put(u64::from(SIZE_CODE), 3);
        bits.put(0, 1); // reserved
        bits.extend(&utf8_number(self.frame_number));
        let header = bits.finish();
        out.extend_from_slice(&header);
        out.push(crc8(&header));

        let mut bits = BitWriter::default();
        first.write(&mut bits);
        second.write(&mut bits);
        out.extend_from_slice(&bits.finish());
        let crc = crc16(&out[start..]);
        out.extend_from_slice(&crc.to_be_bytes());
        self.frame_number += 1;
    }
}

/// How one channel of a block is coded.
struct Subframe<'a> {
    samples: &'a [i32],
    bits_per_sample: u32,
    kind: Kind,
    /// Size of the coded subframe, header included.
    bits: u64,
}

enum Kind {
    Constant,
    Verbatim,
    Fixed { order: usize, residual: Vec<i64>, partition_order: u32, parameters: Vec<u32> },
}

impl<'a> Subframe<'a> {
    /// The smallest coding of `samples`.
    fn best(samples: &'a [i32], bits_per_sample: u32) -> Self {
        let header = 8;
        let bps = u64::from(bits_per_sample);
        if samples.iter().all(|s| *s == samples[0]) {
            return Self { samples, bits_per_sample, kind: Kind::Constant, bits: header + bps };
        }
        let mut best = Self {
            samples,
            bits_per_sample,
            kind: Kind::Verbatim,
            bits: header + bps * samples.len() as u64,
        };
        for order in 0..=MAX_ORDER {
            let residual = fixed_residual(samples, order);
            let (partition_order, parameters, residual_bits) = rice_partitions(&residual, order);
            let bits = header + bps * order as u64 + residual_bits;
            if bits < best.bits {
                best = Self {
                    samples,
                    bits_per_sample,
                    kind: Kind::Fixed { order, residual, partition_order, parameters },
                    bits,
                };
            }
        }
        best
    }

    fn write(&self, out: &mut BitWriter) {
        let bps = self.bits_per_sample;
        match &self.kind {
            Kind::Constant => {
                out.put(0b0000_0000, 8);
                out.put_signed(self.samples[0].into(), bps);
            }
            Kind::Verbatim => {
                out.put(0b0000_0010, 8);
                for sample in self.samples {
                    out.put_signed((*sample).into(), bps);
                }
            }
            Kind::Fixed { order, residual, partition_order, parameters } => {
                // 0, type 001xxx with xxx the order, no wasted bits.
                out.put(0b0001_0000 | (*order as u64) << 1, 8);
                for sample in &self.samples[..*order] {
                    out.put_signed((*sample).into(), bps);
                }
                out.put(0b00, 2); // Rice coding, 4-bit parameters
                out.put(u64::from(*partition_order), 4);
                let size = self.samples.len() >> partition_order;
                let mut position = 0;
                for (index, parameter) in parameters.iter().enumerate() {
                    let count = if index == 0 { size - order } else { size };
                    out.put(u64::from(*parameter), 4);
                    for value in &residual[position..position + count] {
                        out.put_rice(zigzag(*value), *parameter);
                    }
                    position += count;
                }
            }
        }
    }
}

/// The residual of the fixed predictor of `order`, for the samples after the
/// first `order` (which are stored as they are).
fn fixed_residual(samples: &[i32], order: usize) -> Vec<i64> {
    let x = |i: usize| i64::from(samples[i]);
    (order..samples.len())
        .map(|i| match order {
            0 => x(i),
            1 => x(i) - x(i - 1),
            2 => x(i) - 2 * x(i - 1) + x(i - 2),
            3 => x(i) - 3 * x(i - 1) + 3 * x(i - 2) - x(i - 3),
            _ => x(i) - 4 * x(i - 1) + 6 * x(i - 2) - 4 * x(i - 3) + x(i - 4),
        })
        .collect()
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// Picks the partition order and Rice parameters with the fewest bits.
/// Returns them with the size of the residual section, its 6-bit header included.
fn rice_partitions(residual: &[i64], order: usize) -> (u32, Vec<u32>, u64) {
    let block = residual.len() + order;
    let values: Vec<u64> = residual.iter().map(|r| zigzag(*r)).collect();
    let mut best: Option<(u32, Vec<u32>, u64)> = None;
    for partition_order in 0..=MAX_PARTITION_ORDER {
        let size = block >> partition_order;
        if !block.is_multiple_of(1 << partition_order) || size <= order {
            break;
        }
        let mut parameters = Vec::new();
        let mut bits = 6u64;
        let mut position = 0;
        for index in 0..1usize << partition_order {
            let count = if index == 0 { size - order } else { size };
            let (parameter, partition_bits) = best_rice(&values[position..position + count]);
            parameters.push(parameter);
            bits += 4 + partition_bits;
            position += count;
        }
        if best.as_ref().is_none_or(|b| bits < b.2) {
            best = Some((partition_order, parameters, bits));
        }
    }
    best.expect("partition order 0 always fits")
}

/// The Rice parameter with the fewest bits for `values`, and that size.
fn best_rice(values: &[u64]) -> (u32, u64) {
    let size = |k: u32| -> u64 { values.iter().map(|v| (v >> k) + 1 + u64::from(k)).sum() };
    let sum: u64 = values.iter().sum();
    let mean = sum / values.len().max(1) as u64;
    // The optimum is near log2 of the mean: try around it.
    let guess = (u64::BITS - mean.leading_zeros()).min(MAX_RICE);
    (guess.saturating_sub(2)..=(guess + 1).min(MAX_RICE))
        .map(|k| (k, size(k)))
        .min_by_key(|(_, bits)| *bits)
        .unwrap()
}

/// The frame number in FLAC's UTF-8-like coding.
fn utf8_number(value: u64) -> Vec<u8> {
    if value < 0x80 {
        return vec![value as u8];
    }
    // Continuation bytes carry 6 bits each; the first byte has the length prefix.
    let mut bytes = 2;
    while value >= 1 << (5 * bytes + 1) && bytes < 7 {
        bytes += 1;
    }
    let mut out = vec![0u8; bytes];
    let mut rest = value;
    for byte in out.iter_mut().skip(1).rev() {
        *byte = 0x80 | (rest & 0x3f) as u8;
        rest >>= 6;
    }
    let prefix = (0xff00u16 >> bytes) as u8;
    out[0] = prefix | rest as u8;
    out
}

fn crc8(bytes: &[u8]) -> u8 {
    let mut crc = 0u8;
    for byte in bytes {
        crc ^= byte;
        for _ in 0..8 {
            crc = if crc & 0x80 != 0 { crc << 1 ^ 0x07 } else { crc << 1 };
        }
    }
    crc
}

fn crc16(bytes: &[u8]) -> u16 {
    let mut crc = 0u16;
    for byte in bytes {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 { crc << 1 ^ 0x8005 } else { crc << 1 };
        }
    }
    crc
}

/// Writes bits most significant first, padding the last byte with zeros.
#[derive(Default)]
struct BitWriter {
    bytes: Vec<u8>,
    /// Bits not yet written out, in the low `used` bits.
    pending: u64,
    used: u32,
}

impl BitWriter {
    fn put(&mut self, value: u64, bits: u32) {
        debug_assert!(bits <= 56);
        if bits == 0 {
            return;
        }
        self.pending = self.pending << bits | (value & ((1u64 << bits) - 1));
        self.used += bits;
        while self.used >= 8 {
            self.used -= 8;
            self.bytes.push((self.pending >> self.used) as u8);
        }
        self.pending &= (1u64 << self.used) - 1;
    }

    fn put_signed(&mut self, value: i64, bits: u32) {
        self.put(value as u64, bits);
    }

    /// The quotient in unary (zeros closed by a one), then `k` low bits.
    fn put_rice(&mut self, value: u64, k: u32) {
        let mut quotient = value >> k;
        while quotient >= 32 {
            self.put(0, 32);
            quotient -= 32;
        }
        self.put(1, quotient as u32 + 1);
        self.put(value, k);
    }

    fn extend(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.put(u64::from(*byte), 8);
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.used > 0 {
            let pad = 8 - self.used;
            self.put(0, pad);
        }
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pcm(frames: usize, sample: impl Fn(usize) -> (i16, i16)) -> Vec<u8> {
        let mut out = Vec::new();
        for i in 0..frames {
            let (l, r) = sample(i);
            out.extend_from_slice(&l.to_le_bytes());
            out.extend_from_slice(&r.to_le_bytes());
        }
        out
    }

    fn music(i: usize) -> (i16, i16) {
        let t = i as f64 / f64::from(RATE);
        let l = (t * 440.0 * std::f64::consts::TAU).sin() * 9000.0
            + (t * 3100.0 * std::f64::consts::TAU).sin() * 2000.0;
        let r = (t * 660.0 * std::f64::consts::TAU).sin() * 7000.0 + l * 0.3;
        // A little noise, so the predictors have work to do.
        let noise = ((i * 7919) % 61) as f64 - 30.0;
        ((l + noise) as i16, (r - noise) as i16)
    }

    #[test]
    fn utf8_numbers() {
        assert_eq!(utf8_number(0), [0]);
        assert_eq!(utf8_number(0x7f), [0x7f]);
        assert_eq!(utf8_number(0x80), [0xc2, 0x80]);
        assert_eq!(utf8_number(0x7ff), [0xdf, 0xbf]);
        assert_eq!(utf8_number(0x800), [0xe0, 0xa0, 0x80]);
        assert_eq!(utf8_number(0xffff), [0xef, 0xbf, 0xbf]);
        assert_eq!(utf8_number(0x10000), [0xf0, 0x90, 0x80, 0x80]);
    }

    #[test]
    fn crcs_match_the_reference() {
        // The check values of CRC-8 (poly 0x07) and CRC-16/BUYPASS (poly 0x8005).
        assert_eq!(crc8(b"123456789"), 0xf4);
        assert_eq!(crc16(b"123456789"), 0xfee8);
    }

    #[test]
    fn frames_come_in_whole_blocks() {
        let mut encoder = Encoder::default();
        let mut out = Vec::new();
        encoder.push(&pcm(BLOCK - 1, music), &mut out);
        assert!(out.is_empty());
        encoder.push(&pcm(1, music), &mut out);
        assert_eq!(&out[..2], [0xff, 0xf8]);
        assert_eq!(encoder.frame_number, 1);
    }

    #[test]
    fn silence_and_music_compress() {
        let mut encoder = Encoder::default();
        let mut out = Vec::new();
        encoder.push(&pcm(BLOCK, |_| (0, 0)), &mut out);
        assert!(out.len() < 32, "silence took {} bytes", out.len());
        out.clear();
        encoder.push(&pcm(BLOCK, music), &mut out);
        assert!(out.len() < BLOCK * 4 * 6 / 10, "music took {} bytes", out.len());
    }

    /// Decodes with the reference `flac` tool when it is installed.
    #[test]
    fn reference_decoder_accepts_the_stream() {
        let frames = BLOCK * 5;
        let input = pcm(frames, |i| {
            if i < BLOCK {
                (0, 0)
            } else if i < 2 * BLOCK {
                (500, 500)
            } else {
                music(i)
            }
        });
        let mut stream = header();
        let mut encoder = Encoder::default();
        encoder.push(&input, &mut stream);
        let path =
            std::env::temp_dir().join(format!("sound-cast-test-{}.flac", std::process::id()));
        std::fs::write(&path, &stream).unwrap();
        let decoded = std::process::Command::new("flac")
            .args(["-d", "-s", "-c", "--force-raw-format", "--endian=little", "--sign=signed"])
            .arg(&path)
            .output();
        let _ = std::fs::remove_file(&path);
        let Ok(decoded) = decoded else {
            eprintln!("flac not installed: skipped");
            return;
        };
        assert!(decoded.status.success(), "{}", String::from_utf8_lossy(&decoded.stderr));
        assert_eq!(decoded.stdout.len(), input.len());
        assert!(decoded.stdout == input, "decoded audio differs");
    }
}
