//! sound-cast: play PipeWire audio on Chromecast speakers, one sink per room.

mod castv2;
mod discovery;
mod flac;
mod http;
mod hub;
mod mpris;
mod room;
mod sink;
mod status;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use pipewire as pw;

use discovery::{Device, Fixed};
use http::Codec;
use hub::Hub;
use room::{Control, Room};
use serde_json::json;
use status::{Level, Shared};

const USAGE: &str = "\
usage: sound-cast [OPTION]...
       sound-cast --version

  --port PORT          TCP port of the audio streams (default 8574)
  --only NAME          create a sink only for this Cast device or group (repeatable)
  --exclude NAME       never create a sink for this device or group (repeatable)
  --alias NAME=LABEL   show the device NAME as LABEL, sink name included (repeatable)
  --device NAME=HOST[:PORT]
                       a Cast device mDNS cannot find, at HOST (repeatable)
  --grace SECONDS      keep a session open this long after the audio stops (default 20)
  --codec wav|flac     audio format of the streams (default wav)
  --latency auto|off|MS
                       latency reported to applications for A/V sync (default auto:
                       measured on the device)
  --live-metadata      reload the stream on every track change, so screens show the
                       new title (costs a short gap)
  --no-cast            create the sinks and serve the streams, but never play on a device
  --version            print the version and exit";

/// A device missing from the network for this long loses its sink.
const LOST_GRACE: Duration = Duration::from_secs(60);
const SWEEP_EVERY: Duration = Duration::from_secs(10);
/// How often a `--device` whose host does not resolve is tried again.
const FIXED_RETRY: Duration = Duration::from_secs(60);

/// How the latency reported to PipeWire is chosen.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Latency {
    /// Measured on the device while it plays.
    Auto,
    /// Nothing reported.
    Off,
    Fixed(Duration),
}

/// The options every room shares.
#[derive(Clone, Debug)]
pub struct Settings {
    pub port: u16,
    /// A pause shorter than this keeps the session open.
    pub grace: Duration,
    /// False with `--no-cast`: the rooms never open a session.
    pub cast: bool,
    pub codec: Codec,
    pub latency: Latency,
    pub live_metadata: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            port: 8574,
            grace: Duration::from_secs(20),
            cast: true,
            codec: Codec::Wav,
            latency: Latency::Auto,
            live_metadata: false,
        }
    }
}

/// The options of the manager: which devices get a sink, and under what name.
#[derive(Default)]
struct Filter {
    only: Vec<String>,
    exclude: Vec<String>,
    /// Device name, lowercase, to the label shown instead.
    aliases: HashMap<String, String>,
}

impl Filter {
    /// Applies the alias and returns false when the device gets no sink.
    /// `--only` and `--exclude` match the device name or its alias.
    fn admit(&self, device: &mut Device) -> bool {
        let original = device.name.clone();
        if let Some(label) = self.aliases.get(&original.to_lowercase()) {
            device.name = label.clone();
        }
        let named = |list: &[String]| {
            list.iter()
                .any(|n| n.eq_ignore_ascii_case(&original) || n.eq_ignore_ascii_case(&device.name))
        };
        (self.only.is_empty() || named(&self.only)) && !named(&self.exclude)
    }
}

pub enum Event {
    Found(Device),
    /// The device with this key left the network.
    Lost(String),
    /// A client asked the room with this key to take its device back.
    Resume(String),
    /// A client asked the room with this key to end its idle session now.
    Stop(String),
    /// A client asked the room with this key to open a session without audio.
    Start(String),
    /// A client set the volume or mute of the room with this key.
    Volume {
        key: String,
        level: Option<f32>,
        muted: Option<bool>,
    },
    /// A client asked to look for devices again.
    Rescan,
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
    settings: Arc<Settings>,
    filter: Filter,
    hubs: http::Hubs,
    sinks: pw::channel::Sender<sink::Command>,
    players: Sender<mpris::Command>,
    rescan: Sender<()>,
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
                Ok(Event::Resume(key)) => self.tell(&key, Control::Resume),
                Ok(Event::Stop(key)) => self.tell(&key, Control::Stop),
                Ok(Event::Start(key)) => self.tell(&key, Control::Start),
                Ok(Event::Volume { key, level, muted }) => {
                    // Set on the sink: the sink tells the room, which tells the device.
                    let _ = self.sinks.send(sink::Command::SetVolume { key, level, muted });
                }
                Ok(Event::Rescan) => {
                    self.shared.event(Level::Info, "rescan", None, json!({}));
                    let _ = self.rescan.send(());
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

    fn tell(&self, key: &str, control: Control) {
        if let Some(room) = self.rooms.get(key) {
            let _ = room.control.send(control);
        }
    }

    fn found(&mut self, mut device: Device) {
        if !self.filter.admit(&mut device) {
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
        let base =
            format!("sound-cast.{}", name.unwrap_or_else(|| device.key.to_ascii_lowercase()));
        // Two devices with the same name: the second gets a number, so every room
        // (and D-Bus call by sink name) stays its own.
        let mut node_name = base.clone();
        let mut number = 1;
        while self.rooms.values().any(|r| r.name == node_name) {
            number += 1;
            node_name = format!("{base}.{number}");
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
        self.shared.add_room(&device, &node_name);
        self.shared.event(Level::Info, "found", Some(&key), json!({}));
        let _ = self
            .players
            .send(mpris::Command::Subscribe { key: key.clone(), control: control_tx.clone() });
        let room = Room::new(
            device,
            self.settings.clone(),
            control_rx,
            self.sinks.clone(),
            self.players.clone(),
            busy.clone(),
            self.shared.clone(),
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
        let _ = self.players.send(mpris::Command::Unsubscribe { key: key.to_string() });
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

/// What the command line asks for.
struct Options {
    settings: Settings,
    filter: Filter,
    fixed: Vec<Fixed>,
}

/// Parses the arguments. `None` means the process should exit with success
/// (`--version`, `--help`).
fn parse(args: impl Iterator<Item = String>) -> Result<Option<Options>> {
    let mut settings = Settings::default();
    let mut filter = Filter::default();
    let mut fixed = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let mut value = |what: &str| args.next().ok_or(anyhow!("{arg} needs {what}"));
        match arg.as_str() {
            "--port" => settings.port = value("a port")?.parse().context("--port")?,
            "--only" => filter.only.push(value("a name")?),
            "--exclude" => filter.exclude.push(value("a name")?),
            "--alias" => {
                let text = value("NAME=LABEL")?;
                let (name, label) = text
                    .split_once('=')
                    .filter(|(n, l)| !n.trim().is_empty() && !l.trim().is_empty())
                    .ok_or(anyhow!("--alias needs NAME=LABEL, got {text}"))?;
                filter.aliases.insert(name.trim().to_lowercase(), label.trim().to_string());
            }
            "--device" => fixed.push(Fixed::parse(&value("NAME=HOST[:PORT]")?)?),
            "--grace" => {
                let seconds: u64 = value("seconds")?.parse().context("--grace")?;
                settings.grace = Duration::from_secs(seconds);
            }
            "--codec" => {
                settings.codec = match value("wav or flac")?.as_str() {
                    "wav" => Codec::Wav,
                    "flac" => Codec::Flac,
                    other => bail!("--codec: unknown codec {other}, use wav or flac"),
                }
            }
            "--latency" => {
                settings.latency = match value("auto, off or milliseconds")?.as_str() {
                    "auto" => Latency::Auto,
                    "off" => Latency::Off,
                    ms => Latency::Fixed(Duration::from_millis(
                        ms.parse().context("--latency: use auto, off or milliseconds")?,
                    )),
                }
            }
            "--live-metadata" => settings.live_metadata = true,
            "--no-cast" => settings.cast = false,
            "-V" | "--version" => {
                println!("sound-cast {}", env!("CARGO_PKG_VERSION"));
                return Ok(None);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(None);
            }
            other => bail!("unknown argument {other}\n{USAGE}"),
        }
    }
    Ok(Some(Options { settings, filter, fixed }))
}

/// Reports the devices given by hand, and resolves their hosts again every
/// [`FIXED_RETRY`]: a host that does not resolve yet (the network is not up)
/// comes later, and a name whose address changed (DHCP) moves its room.
fn announce_fixed(fixed: Vec<Fixed>, events: Sender<Event>) {
    if fixed.is_empty() {
        return;
    }
    thread::spawn(move || {
        let mut known: Vec<Option<Device>> = vec![None; fixed.len()];
        loop {
            for (fixed, known) in fixed.iter().zip(known.iter_mut()) {
                match fixed.device() {
                    Ok(device) => {
                        if known.as_ref() != Some(&device) {
                            if events.send(Event::Found(device.clone())).is_err() {
                                return;
                            }
                            *known = Some(device);
                        }
                    }
                    Err(error) => {
                        eprintln!(
                            "--device {}: {error:#}, retry in {}s",
                            fixed.name,
                            FIXED_RETRY.as_secs()
                        );
                    }
                }
            }
            thread::sleep(FIXED_RETRY);
        }
    });
}

fn main() -> Result<()> {
    let Some(Options { settings, filter, fixed }) = parse(std::env::args().skip(1))? else {
        return Ok(());
    };

    block_termination_signals();

    let settings = Arc::new(settings);
    let shared = Shared::new(settings.port, http::new_token()?);
    let hubs = http::Hubs::default();
    http::start(settings.clone(), hubs.clone(), shared.clone())?;

    let (events_tx, events_rx) = channel();
    // Once the name is owned, systemd (Type=dbus) considers the service started.
    if let Err(error) = status::start(shared.clone(), events_tx.clone()) {
        eprintln!("status: {error:#}; running without the D-Bus interface");
    }
    shared.event(Level::Info, "started", None, json!({ "port": settings.port }));
    let players = mpris::start(shared.clone());
    let (sinks_tx, sinks_rx) = pw::channel::channel();
    let (rescan_tx, rescan_rx) = channel();
    let manager = Manager {
        settings,
        filter,
        hubs,
        sinks: sinks_tx.clone(),
        players: players.clone(),
        rescan: rescan_tx,
        shared,
        rooms: HashMap::new(),
    };
    let manager = thread::spawn(move || manager.run(events_rx));
    announce_fixed(fixed, events_tx.clone());
    // Discovery failing ends the daemon the same way a signal does, so every
    // room stops its receiver before the process exits.
    let discovery_error = Arc::new(Mutex::new(None));
    {
        let events = events_tx.clone();
        let discovery_error = discovery_error.clone();
        thread::spawn(move || {
            if let Err(error) = discovery::run(events, rescan_rx) {
                eprintln!("discovery: {error:#}");
                *discovery_error.lock().unwrap() = Some(error);
                let _ = sinks_tx.send(sink::Command::Quit);
            }
        });
    }

    // Blocks until SIGINT, SIGTERM or a discovery failure.
    let result = sink::run(sinks_rx, players);

    let _ = events_tx.send(Event::Shutdown);
    let _ = manager.join();
    result?;
    match discovery_error.lock().unwrap().take() {
        Some(error) => Err(error.context("discovery")),
        None => Ok(()),
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    fn args(text: &str) -> impl Iterator<Item = String> {
        text.split_whitespace().map(str::to_string).collect::<Vec<_>>().into_iter()
    }

    fn device(name: &str) -> Device {
        Device {
            key: "k".into(),
            name: name.into(),
            address: "127.0.0.1:8009".parse().unwrap(),
            model: String::new(),
            screen: false,
            group: false,
            fixed: false,
        }
    }

    #[test]
    fn slug_keeps_letters_and_digits() {
        assert_eq!(slug("Sala"), "sala");
        assert_eq!(slug("Casse  Camera 2!"), "casse_camera_2");
        assert_eq!(slug("  Però"), "per");
    }

    #[test]
    fn options_parse() {
        let o = parse(args(
            "--port 9000 --grace 5 --codec flac --latency 1500 --exclude TV --alias Sala=Soggiorno \
             --device Studio=10.0.0.9 --live-metadata",
        ))
        .unwrap()
        .unwrap();
        assert_eq!(o.settings.port, 9000);
        assert_eq!(o.settings.grace, Duration::from_secs(5));
        assert_eq!(o.settings.codec, Codec::Flac);
        assert_eq!(o.settings.latency, Latency::Fixed(Duration::from_millis(1500)));
        assert!(o.settings.live_metadata);
        assert_eq!(o.fixed.len(), 1);
        assert_eq!(o.filter.aliases["sala"], "Soggiorno");
        assert!(parse(args("--codec mp3")).is_err());
        assert!(parse(args("--alias Sala")).is_err());
        assert!(parse(args("--latency soon")).is_err());
        assert_eq!(parse(args("--latency off")).unwrap().unwrap().settings.latency, Latency::Off);
    }

    #[test]
    fn filter_applies_alias_only_and_exclude() {
        let mut filter = Filter::default();
        filter.aliases.insert("sala".into(), "Soggiorno".into());
        filter.exclude.push("cucina".into());
        let mut sala = device("Sala");
        assert!(filter.admit(&mut sala));
        assert_eq!(sala.name, "Soggiorno");
        assert!(!filter.admit(&mut device("Cucina")));

        filter.only.push("soggiorno".into());
        assert!(filter.admit(&mut device("Sala")));
        assert!(!filter.admit(&mut device("Bagno")));
    }
}
