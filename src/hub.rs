//! Fan-out of one room's captured PCM to its HTTP clients.
//!
//! The PipeWire thread hands raw chunks to the hub thread. The hub forwards
//! them to every client and fills the gaps with silence, so a receiver never
//! sees the stream stall while nothing plays on the sink. With no client
//! connected the hub thread sleeps.

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
/// Chunks a client may fall behind before it is dropped (about 5 s).
const CLIENT_QUEUE: usize = 256;
/// Without audio for this long, the hub starts to send silence.
const IDLE_AFTER: Duration = Duration::from_millis(100);
const SILENCE_TICK: Duration = Duration::from_millis(20);

pub type Chunk = Arc<[u8]>;

pub struct Hub {
    clients: Mutex<Vec<SyncSender<Chunk>>>,
    /// Feeds the hub thread. An empty chunk only wakes it up.
    captured: SyncSender<Vec<u8>>,
    closed: AtomicBool,
}

impl Hub {
    /// Creates a hub and starts its thread. The returned sender is the capture side.
    pub fn start() -> (Arc<Hub>, SyncSender<Vec<u8>>) {
        let (captured_tx, captured_rx) = sync_channel(CAPTURE_QUEUE);
        let hub = Arc::new(Hub {
            clients: Mutex::default(),
            captured: captured_tx.clone(),
            closed: AtomicBool::new(false),
        });
        let worker = hub.clone();
        thread::spawn(move || worker.run(captured_rx));
        (hub, captured_tx)
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

    fn run(&self, captured: Receiver<Vec<u8>>) {
        let mut last_audio = Instant::now();
        let mut idle = false;
        let mut next_silence = Instant::now();

        while !self.closed.load(Ordering::Acquire) {
            let received = if !self.has_clients() {
                // Nobody listens: sleep until audio or a new client arrives.
                idle = false;
                captured.recv().map_err(|_| RecvTimeoutError::Disconnected)
            } else if idle {
                captured.recv_timeout(next_silence.saturating_duration_since(Instant::now()))
            } else {
                captured.recv_timeout(IDLE_AFTER)
            };
            match received {
                Ok(pcm) => {
                    idle = false;
                    last_audio = Instant::now();
                    if !pcm.is_empty() {
                        self.publish(pcm.into());
                    }
                }
                Err(RecvTimeoutError::Timeout) if !idle => {
                    // Cover the gap since the last real chunk, then tick.
                    idle = true;
                    self.publish(silence(last_audio.elapsed()));
                    next_silence = Instant::now() + SILENCE_TICK;
                }
                Err(RecvTimeoutError::Timeout) => {
                    self.publish(silence(SILENCE_TICK));
                    next_silence += SILENCE_TICK;
                }
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }
}

fn silence(duration: Duration) -> Chunk {
    let frames = (RATE as u128 * duration.as_millis() / 1000) as usize;
    vec![0u8; frames * BYTES_PER_FRAME as usize].into()
}
