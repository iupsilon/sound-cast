//! Fan-out of one room's captured PCM to its HTTP clients.
//!
//! The PipeWire thread hands raw chunks to the hub thread. The hub forwards
//! them to every client and fills the gaps with silence, so a receiver never
//! sees the stream stall while nothing plays on the sink. With no client
//! connected the hub thread sleeps.
//!
//! The hub counts how much time the stream covers. Silence fills only time
//! that has already passed, never time ahead, so audio that resumes does not
//! overlap it and pauses do not add up to a growing delay on the receiver.
//!
//! The buffers go back to the PipeWire thread once published, so its realtime
//! callback finds one ready instead of allocating.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub const RATE: u32 = 48_000;
pub const CHANNELS: u32 = 2;
/// Signed 16-bit little-endian samples.
pub const BYTES_PER_FRAME: u32 = CHANNELS * 2;

/// Chunks the hub may lag behind the PipeWire thread before audio is dropped.
const CAPTURE_QUEUE: usize = 64;
/// Buffers kept for the PipeWire thread to fill.
const SPARE_BUFFERS: usize = CAPTURE_QUEUE + 8;
/// Starting size of a spare buffer: 4096 frames. A larger PipeWire quantum
/// grows a buffer once, and the buffer keeps that size.
const SPARE_CAPACITY: usize = 4096 * BYTES_PER_FRAME as usize;
/// Chunks a client may fall behind before it is dropped (about 5 s).
const CLIENT_QUEUE: usize = 256;
/// Without audio for this long, the hub starts to send silence.
const IDLE_AFTER: Duration = Duration::from_millis(100);
const SILENCE_TICK: Duration = Duration::from_millis(20);

pub type Chunk = Arc<[u8]>;

/// The capture side of a hub, used by the PipeWire thread.
pub struct Capture {
    /// Audio for the hub thread.
    pub audio: SyncSender<Vec<u8>>,
    /// Empty buffers to fill, handed back by the hub thread.
    pub spare: Receiver<Vec<u8>>,
}

pub struct Hub {
    clients: Mutex<Vec<SyncSender<Chunk>>>,
    /// Feeds the hub thread. An empty chunk only wakes it up.
    captured: SyncSender<Vec<u8>>,
    closed: AtomicBool,
}

impl Hub {
    /// Creates a hub and starts its thread.
    pub fn start() -> (Arc<Hub>, Capture) {
        let (captured_tx, captured_rx) = sync_channel(CAPTURE_QUEUE);
        let (spare_tx, spare_rx) = sync_channel(SPARE_BUFFERS);
        for _ in 0..SPARE_BUFFERS {
            let _ = spare_tx.try_send(Vec::with_capacity(SPARE_CAPACITY));
        }
        let hub = Arc::new(Hub {
            clients: Mutex::default(),
            captured: captured_tx.clone(),
            closed: AtomicBool::new(false),
        });
        let worker = hub.clone();
        thread::spawn(move || worker.run(captured_rx, spare_tx));
        (hub, Capture { audio: captured_tx, spare: spare_rx })
    }

    /// Returns the stream for a new client, or `None` when the hub is closed.
    pub fn subscribe(&self) -> Option<Receiver<Chunk>> {
        let (tx, rx) = sync_channel(CLIENT_QUEUE);
        {
            // The closed check and the push share the lock with `close`.
            let mut clients = self.clients.lock().unwrap();
            if self.closed.load(Ordering::Acquire) {
                return None;
            }
            clients.push(tx);
        }
        self.wake();
        Some(rx)
    }

    /// Stops the hub thread and ends the stream of every client.
    pub fn close(&self) {
        let mut clients = self.clients.lock().unwrap();
        self.closed.store(true, Ordering::Release);
        clients.clear();
        drop(clients);
        self.wake();
    }

    fn wake(&self) {
        // A full queue means the thread is busy and will look again by itself.
        let _ = self.captured.try_send(Vec::new());
    }

    fn has_clients(&self) -> bool {
        !self.clients.lock().unwrap().is_empty()
    }

    /// Sends a chunk to every client. A client that is gone or too slow is dropped.
    fn publish(&self, chunk: Chunk) {
        self.clients.lock().unwrap().retain(|tx| match tx.try_send(chunk.clone()) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                eprintln!("hub: client too slow, dropped");
                false
            }
            Err(TrySendError::Disconnected(_)) => false,
        });
    }

    /// Sends silence for the time from `covered` to now. Returns the new `covered`.
    fn fill(&self, covered: Instant) -> Instant {
        let frames = frames_in(Instant::now().saturating_duration_since(covered));
        if frames == 0 {
            return covered;
        }
        self.publish(vec![0u8; frames * BYTES_PER_FRAME as usize].into());
        covered + duration_of(frames)
    }

    fn run(&self, captured: Receiver<Vec<u8>>, spare: SyncSender<Vec<u8>>) {
        // The stream holds audio or silence up to this instant.
        let mut covered = Instant::now();
        let mut idle = false;

        while !self.closed.load(Ordering::Acquire) {
            let received = if !self.has_clients() {
                // Nobody listens: sleep until audio or a new client arrives.
                idle = false;
                let received = captured.recv().map_err(|_| RecvTimeoutError::Disconnected);
                // Nobody heard the time spent asleep: it needs no silence.
                covered = covered.max(Instant::now());
                received
            } else {
                let wait = if idle { SILENCE_TICK } else { IDLE_AFTER };
                captured.recv_timeout((covered + wait).saturating_duration_since(Instant::now()))
            };
            match received {
                // A wake-up, not audio: the silence keeps its pace.
                Ok(pcm) if pcm.is_empty() => {}
                Ok(mut pcm) => {
                    if idle {
                        // The last tick of silence stopped short of now.
                        covered = self.fill(covered);
                        idle = false;
                    }
                    let frames = pcm.len() / BYTES_PER_FRAME as usize;
                    covered = covered.max(Instant::now()) + duration_of(frames);
                    self.publish(pcm.as_slice().into());
                    pcm.clear();
                    let _ = spare.try_send(pcm);
                }
                Err(RecvTimeoutError::Timeout) => {
                    idle = true;
                    covered = self.fill(covered);
                }
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }
}

fn duration_of(frames: usize) -> Duration {
    Duration::from_nanos(frames as u64 * 1_000_000_000 / u64::from(RATE))
}

fn frames_in(duration: Duration) -> usize {
    (duration.as_nanos() * u128::from(RATE) / 1_000_000_000) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Frames a client received within `window`.
    fn frames_received(chunks: &Receiver<Chunk>, window: Duration) -> usize {
        let deadline = Instant::now() + window;
        let mut bytes = 0;
        while let Ok(chunk) = chunks.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            bytes += chunk.len();
        }
        bytes / BYTES_PER_FRAME as usize
    }

    #[test]
    fn silence_and_audio_keep_real_time() {
        let (hub, capture) = Hub::start();
        let chunks = hub.subscribe().unwrap();
        let start = Instant::now();
        // Audio, a pause long enough for silence, audio again.
        for _ in 0..3 {
            let mut pcm = capture.spare.recv().unwrap();
            pcm.resize(960 * BYTES_PER_FRAME as usize, 1);
            capture.audio.send(pcm).unwrap();
            thread::sleep(Duration::from_millis(300));
        }
        let frames = frames_received(&chunks, Duration::from_millis(50));
        let expected = frames_in(start.elapsed());
        hub.close();
        // Never ahead of real time by more than a tick, never far behind.
        assert!(frames <= expected + frames_in(SILENCE_TICK), "{frames} > {expected}");
        assert!(frames + frames_in(IDLE_AFTER + SILENCE_TICK) >= expected, "{frames} < {expected}");
    }

    #[test]
    fn buffers_come_back() {
        let (hub, capture) = Hub::start();
        let mut pcm = capture.spare.recv().unwrap();
        pcm.resize(64, 1);
        capture.audio.send(pcm).unwrap();
        thread::sleep(Duration::from_millis(50));
        let spares = capture.spare.try_iter().count();
        hub.close();
        assert_eq!(spares, SPARE_BUFFERS);
    }
}
