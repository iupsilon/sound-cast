//! sound-cast: play PipeWire audio on Chromecast speakers, one sink per room.

mod castv2;
mod discovery;
mod http;
mod hub;
mod room;
mod sink;
mod status;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use pipewire as pw;

use discovery::Device;
use hub::Hub;
use room::{Control, Room};
use serde_json::json;
use status::{Level, Shared};

const USAGE: &str = "\
usage: sound-cast [--port PORT] [--only NAME]... [--no-cast]

  --port PORT   TCP port of the audio streams (default 3000)
  --only NAME   create a sink only for this Cast device or group (repeatable)
  --no-cast     create the sinks and serve the streams, but never play on a device";

/// A device missing from the network for this long loses its sink.
const LOST_GRACE: Duration = Duration::from_secs(60);
const SWEEP_EVERY: Duration = Duration::from_secs(10);

pub enum Event {
    Found(Device),
    /// The device with this key left the network.
    Lost(String),
    /// A client asked the room with this key to take its device back.
    Resume(String),
    /// A client asked the room with this key to end its idle session now.
    Stop(String),
    Shutdown,
}

struct RoomHandle {
    name: String,
    control: Sender<Control>,
    hub: Arc<Hub>,
    busy: Arc<AtomicBool>,
    lost_at: Option<Instant>,
    thread: JoinHandle<()>,
}

struct Manager {
    port: u16,
    only: Vec<String>,
    cast: bool,
    hubs: http::Hubs,
    sinks: pw::channel::Sender<sink::Command>,
    shared: Arc<Shared>,
    rooms: HashMap<String, RoomHandle>,
}

impl Manager {
    fn run(mut self, events: Receiver<Event>) {
        loop {
            match events.recv_timeout(SWEEP_EVERY) {
                Ok(Event::Found(device)) => self.found(device),
                Ok(Event::Lost(key)) => {
                    if let Some(room) = self.rooms.get_mut(&key)
                        && room.lost_at.is_none()
                    {
                        room.lost_at = Some(Instant::now());
                        self.shared.set_lost(&key, Some(LOST_GRACE));
                        self.shared.event(Level::Warning, "lost", Some(&key), json!({}));
                    }
                }
                Ok(Event::Resume(key)) => {
                    if let Some(room) = self.rooms.get(&key) {
                        let _ = room.control.send(Control::Resume);
                    }
                }
                Ok(Event::Stop(key)) => {
                    if let Some(room) = self.rooms.get(&key) {
                        let _ = room.control.send(Control::Stop);
                    }
                }
                Ok(Event::Shutdown) | Err(RecvTimeoutError::Disconnected) => break,
                Err(RecvTimeoutError::Timeout) => {}
            }
            self.sweep();
        }
        // Every room stops its receiver application before the process exits.
        let keys: Vec<String> = self.rooms.keys().cloned().collect();
        for room in self.rooms.values() {
            let _ = room.control.send(Control::Shutdown);
        }
        for key in keys {
            self.remove(&key);
        }
    }

    fn found(&mut self, device: Device) {
        if !self.only.is_empty() && !self.only.iter().any(|n| n.eq_ignore_ascii_case(&device.name)) {
            return;
        }
        if let Some(room) = self.rooms.get_mut(&device.key) {
            if room.lost_at.take().is_some() {
                self.shared.set_lost(&device.key, None);
                self.shared.event(Level::Info, "back", Some(&device.key), json!({}));
            }
            let _ = room.control.send(Control::Device(device));
            return;
        }

        // A name with no ASCII letter or digit falls back to the key.
        let name = Some(slug(&device.name)).filter(|s| !s.is_empty());
        let mut node_name =
            format!("sound-cast.{}", name.unwrap_or_else(|| device.key.to_ascii_lowercase()));
        if self.rooms.values().any(|r| r.name == node_name) {
            node_name = format!("{node_name}.{:.6}", device.key);
        }
        eprintln!(
            "{}: found at {}, sink {node_name}, stream /stream/{}",
            device.name, device.address, device.key
        );

        let (hub, captured) = Hub::start();
        let (control_tx, control_rx) = channel();
        let busy = Arc::new(AtomicBool::new(false));
        self.hubs.lock().unwrap().insert(device.key.clone(), hub.clone());
        let _ = self.sinks.send(sink::Command::Add {
            key: device.key.clone(),
            node_name: node_name.clone(),
            description: device.name.clone(),
            captured,
            control: control_tx.clone(),
        });
        let key = device.key.clone();
        self.shared.add_room(&key, &device.name, &node_name);
        self.shared.event(Level::Info, "found", Some(&key), json!({}));
        let room = Room::new(
            device,
            self.port,
            control_rx,
            self.sinks.clone(),
            busy.clone(),
            self.shared.clone(),
            self.cast,
        );
        self.rooms.insert(
            key,
            RoomHandle {
                name: node_name,
                control: control_tx,
                hub,
                busy,
                lost_at: None,
                thread: thread::spawn(move || room.run()),
            },
        );
    }

    /// Removes the rooms whose device is long gone and that are not casting.
    fn sweep(&mut self) {
        let gone: Vec<String> = self
            .rooms
            .iter()
            .filter(|(_, room)| {
                room.lost_at.is_some_and(|at| at.elapsed() > LOST_GRACE)
                    && !room.busy.load(Ordering::Acquire)
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in gone {
            self.shared.event(Level::Warning, "removed", Some(&key), json!({}));
            self.remove(&key);
        }
    }

    fn remove(&mut self, key: &str) {
        let Some(room) = self.rooms.remove(key) else {
            return;
        };
        eprintln!("{}: removed", room.name);
        let _ = room.control.send(Control::Shutdown);
        let _ = room.thread.join();
        let _ = self.sinks.send(sink::Command::Remove { key: key.to_string() });
        self.hubs.lock().unwrap().remove(key);
        self.shared.remove_room(key);
        room.hub.close();
    }
}

/// Lowercase ASCII letters and digits, everything else folded into `_`.
fn slug(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
        }
    }
    out.trim_end_matches('_').to_string()
}

/// Blocks SIGINT and SIGTERM in this thread and in every thread spawned after
/// it. The PipeWire loop reads them through a signalfd, which sees a signal
/// only when no thread takes it the default way.
fn block_termination_signals() {
    // SAFETY: the set is initialised by `sigemptyset` before use, and the calls
    // only change the signal mask of the calling thread.
    unsafe {
        let mut set = std::mem::zeroed::<libc::sigset_t>();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}

fn main() -> Result<()> {
    let mut port = 3000u16;
    let mut only = Vec::new();
    let mut cast = true;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => port = args.next().ok_or(anyhow!("--port needs a value"))?.parse()?,
            "--only" => only.push(args.next().ok_or(anyhow!("--only needs a name"))?),
            "--no-cast" => cast = false,
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => bail!("unknown argument {other}\n{USAGE}"),
        }
    }

    block_termination_signals();

    let shared = Shared::new(port, http::new_token()?);
    let hubs = http::Hubs::default();
    http::start(port, hubs.clone(), shared.clone())?;

    let (events_tx, events_rx) = channel();
    // Once the name is owned, systemd (Type=dbus) considers the service started.
    if let Err(error) = status::start(shared.clone(), events_tx.clone()) {
        eprintln!("status: {error:#}; running without the D-Bus interface");
    }
    shared.event(Level::Info, "started", None, json!({ "port": port }));
    let (sinks_tx, sinks_rx) = pw::channel::channel();
    let manager = Manager {
        port,
        only,
        cast,
        hubs,
        sinks: sinks_tx.clone(),
        shared,
        rooms: HashMap::new(),
    };
    let manager = thread::spawn(move || manager.run(events_rx));
    // Discovery failing ends the daemon the same way a signal does, so every
    // room stops its receiver before the process exits.
    let discovery_error = Arc::new(Mutex::new(None));
    {
        let events = events_tx.clone();
        let discovery_error = discovery_error.clone();
        thread::spawn(move || {
            if let Err(error) = discovery::run(events) {
                eprintln!("discovery: {error:#}");
                *discovery_error.lock().unwrap() = Some(error);
                let _ = sinks_tx.send(sink::Command::Quit);
            }
        });
    }

    // Blocks until SIGINT, SIGTERM or a discovery failure.
    let result = sink::run(sinks_rx);

    let _ = events_tx.send(Event::Shutdown);
    let _ = manager.join();
    result?;
    match discovery_error.lock().unwrap().take() {
        Some(error) => Err(error.context("discovery")),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::slug;

    #[test]
    fn slug_keeps_letters_and_digits() {
        assert_eq!(slug("Sala"), "sala");
        assert_eq!(slug("Casse  Camera 2!"), "casse_camera_2");
        assert_eq!(slug("  Però"), "per");
    }
}
