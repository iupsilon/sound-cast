//! What the daemon is doing, for desktop widgets, published on D-Bus.
//!
//! Service `org.soundcast.Daemon`, object `/org/soundcast/Daemon`, interface
//! `org.soundcast.Daemon1`:
//!
//! - property `Status` (s): JSON snapshot of every room, see [`Shared::status_json`].
//! - property `Events` (s): JSON array of the last [`MAX_EVENTS`] events.
//! - method `Resume(s sink_name)`: take back a device another sender took.
//! - method `Stop(s sink_name)`: end now a session that is only waiting out
//!   its grace period (no audio on the sink).
//! - method `Start(s sink_name)`: open a session now, without waiting for audio.
//! - method `SetVolume(s sink_name, d level)`: 0.0 to 1.0, on the slider scale.
//! - method `SetMute(s sink_name, b muted)`.
//! - method `Rescan()`: ask every device on the network to answer again.
//! - property `Version` (s): the daemon version.
//!
//! Both properties announce their changes with `PropertiesChanged`, at most
//! every [`COALESCE`]. Times are Unix seconds; `now` in `Status` lets a client
//! correct for its own clock.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use zbus::interface;
use zbus::zvariant::Value as Variant;

use crate::Event;
use crate::discovery::Device;
use crate::mpris::Track;

pub const BUS_NAME: &str = "org.soundcast.Daemon";
pub const PATH: &str = "/org/soundcast/Daemon";
pub const INTERFACE: &str = "org.soundcast.Daemon1";

const MAX_EVENTS: usize = 50;
/// Changes closer than this are announced together.
const COALESCE: Duration = Duration::from_millis(100);
/// Buffering spells are counted over this window.
const BUFFERING_WINDOW: f64 = 60.0;

pub fn unix_now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Info,
    Warning,
    Error,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warning => "warning",
            Level::Error => "error",
        }
    }
}

/// What a room is doing. The names are part of the D-Bus API.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// No audio on the sink, nothing cast.
    Ready,
    /// Opening the connection to the device.
    Connecting,
    /// Starting the receiver application.
    Launching,
    /// The device was given the stream and is fetching it.
    Loading,
    Playing,
    Buffering,
    Paused,
    /// The sink went quiet: the session closes at the deadline.
    Lingering,
    /// The last session failed: the next one starts at the deadline.
    Restarting,
    /// Another sender has the device; the room waits for its own audio to restart.
    Busy,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Phase::Ready => "ready",
            Phase::Connecting => "connecting",
            Phase::Launching => "launching",
            Phase::Loading => "loading",
            Phase::Playing => "playing",
            Phase::Buffering => "buffering",
            Phase::Paused => "paused",
            Phase::Lingering => "lingering",
            Phase::Restarting => "restarting",
            Phase::Busy => "busy",
        }
    }
}

struct Room {
    name: String,
    sink_name: String,
    model: String,
    screen: bool,
    group: bool,
    fixed: bool,
    phase: Phase,
    reason: String,
    since: f64,
    deadline: Option<f64>,
    /// Set while the device is missing from the network: the sink goes at this time.
    lost_until: Option<f64>,
    /// HTTP clients fetching the stream.
    listeners: usize,
    buffering: VecDeque<f64>,
    /// 0 to 100, as the device last had it.
    volume: Option<(u32, bool)>,
    /// When the last stream client connected: position 0 of its stream.
    stream_started: Option<Instant>,
    /// The latency reported to PipeWire, in milliseconds.
    latency_ms: Option<u64>,
    /// What plays on the sink, as its media player tells.
    track: Option<Track>,
    /// The local file of the track's cover, served to the device.
    art: Option<PathBuf>,
}

#[derive(Default)]
struct Inner {
    port: u16,
    rooms: BTreeMap<String, Room>,
    events: VecDeque<Value>,
    /// The number of the next event: it grows by one, whatever the clock does.
    next_seq: u64,
}

pub struct Shared {
    /// The secret part of the stream URLs. Never published.
    pub token: String,
    inner: Mutex<Inner>,
    status_dirty: AtomicBool,
    events_dirty: AtomicBool,
    /// Wakes the D-Bus thread. Empty while there is no D-Bus connection.
    wake: Mutex<Option<SyncSender<()>>>,
}

impl Shared {
    pub fn new(port: u16, token: String) -> Arc<Self> {
        Arc::new(Self {
            token,
            inner: Mutex::new(Inner { port, ..Inner::default() }),
            status_dirty: AtomicBool::new(false),
            events_dirty: AtomicBool::new(false),
            wake: Mutex::default(),
        })
    }

    fn changed(&self, status: bool, events: bool) {
        if status {
            self.status_dirty.store(true, Ordering::Release);
        }
        if events {
            self.events_dirty.store(true, Ordering::Release);
        }
        if let Some(wake) = self.wake.lock().unwrap().as_ref() {
            // A full queue already holds a wake-up.
            if let Err(TrySendError::Disconnected(_)) = wake.try_send(()) {
                eprintln!("status: D-Bus thread gone");
            }
        }
    }

    pub fn add_room(&self, device: &Device, sink_name: &str) {
        self.inner.lock().unwrap().rooms.insert(
            device.key.clone(),
            Room {
                name: device.name.clone(),
                sink_name: sink_name.to_string(),
                model: device.model.clone(),
                screen: device.screen,
                group: device.group,
                fixed: device.fixed,
                phase: Phase::Ready,
                reason: String::new(),
                since: unix_now(),
                deadline: None,
                lost_until: None,
                listeners: 0,
                buffering: VecDeque::new(),
                volume: None,
                stream_started: None,
                latency_ms: None,
                track: None,
                art: None,
            },
        );
        self.changed(true, false);
    }

    pub fn remove_room(&self, key: &str) {
        self.inner.lock().unwrap().rooms.remove(key);
        self.changed(true, false);
    }

    /// The key of the room whose sink has this node name.
    pub fn key_of_sink(&self, sink_name: &str) -> Option<String> {
        let inner = self.inner.lock().unwrap();
        inner.rooms.iter().find(|(_, r)| r.sink_name == sink_name).map(|(k, _)| k.clone())
    }

    /// Sets the phase of a room. `deadline` is how long until the phase ends by itself.
    pub fn set_phase(&self, key: &str, phase: Phase, reason: &str, deadline: Option<Duration>) {
        let now = unix_now();
        let mut inner = self.inner.lock().unwrap();
        let Some(room) = inner.rooms.get_mut(key) else {
            return;
        };
        let deadline = deadline.map(|d| now + d.as_secs_f64());
        let same_deadline = match (room.deadline, deadline) {
            (None, None) => true,
            (Some(a), Some(b)) => (a - b).abs() < 0.5,
            _ => false,
        };
        if room.phase == phase && room.reason == reason && same_deadline {
            return;
        }
        if room.phase != phase {
            room.since = now;
            if phase == Phase::Buffering {
                room.buffering.push_back(now);
            }
        }
        room.phase = phase;
        room.reason = reason.to_string();
        room.deadline = deadline;
        drop(inner);
        self.changed(true, false);
    }

    pub fn set_lost(&self, key: &str, until: Option<Duration>) {
        if let Some(room) = self.inner.lock().unwrap().rooms.get_mut(key) {
            room.lost_until = until.map(|d| unix_now() + d.as_secs_f64());
        }
        self.changed(true, false);
    }

    pub fn set_volume(&self, key: &str, level: f32, muted: bool) {
        let volume = Some(((level * 100.0).round() as u32, muted));
        let mut inner = self.inner.lock().unwrap();
        let Some(room) = inner.rooms.get_mut(key) else {
            return;
        };
        if room.volume == volume {
            return;
        }
        room.volume = volume;
        drop(inner);
        self.changed(true, false);
    }

    /// Counts an HTTP client of the room's stream coming (+1) or going (-1).
    pub fn listener(&self, key: &str, delta: isize) {
        if let Some(room) = self.inner.lock().unwrap().rooms.get_mut(key) {
            // The stream starts with the first client after a load, or after all
            // clients left: a second connection (a probe, another client) is not
            // where the device's position counts from.
            if delta > 0 && (room.stream_started.is_none() || room.listeners == 0) {
                room.stream_started = Some(Instant::now());
            }
            room.listeners = room.listeners.saturating_add_signed(delta);
        }
        self.changed(true, false);
    }

    /// When the stream the device plays started: its first client since the
    /// last load.
    pub fn stream_started(&self, key: &str) -> Option<Instant> {
        self.inner.lock().unwrap().rooms.get(key).and_then(|r| r.stream_started)
    }

    /// A new load is on its way: the next client starts the stream anew.
    pub fn reset_stream(&self, key: &str) {
        if let Some(room) = self.inner.lock().unwrap().rooms.get_mut(key) {
            room.stream_started = None;
        }
    }

    pub fn set_latency(&self, key: &str, latency: Option<Duration>) {
        let latency_ms = latency.map(|l| l.as_millis() as u64);
        let mut inner = self.inner.lock().unwrap();
        let Some(room) = inner.rooms.get_mut(key) else {
            return;
        };
        if room.latency_ms == latency_ms {
            return;
        }
        room.latency_ms = latency_ms;
        drop(inner);
        self.changed(true, false);
    }

    /// Sets what plays on the room's sink. A `file://` cover is remembered,
    /// to be served to the device.
    pub fn set_track(&self, key: &str, track: Option<Track>) {
        let mut inner = self.inner.lock().unwrap();
        let Some(room) = inner.rooms.get_mut(key) else {
            return;
        };
        if room.track == track {
            return;
        }
        room.art = track.as_ref().and_then(|t| t.art_file());
        room.track = track;
        drop(inner);
        self.changed(true, false);
    }

    /// The cover file to serve for the room, if its track has a local one.
    pub fn art(&self, key: &str) -> Option<PathBuf> {
        self.inner.lock().unwrap().rooms.get(key).and_then(|r| r.art.clone())
    }

    pub fn listeners(&self, key: &str) -> usize {
        self.inner.lock().unwrap().rooms.get(key).map_or(0, |r| r.listeners)
    }

    /// Records an event. `key` names the room it is about, if any.
    pub fn event(&self, level: Level, code: &str, key: Option<&str>, args: Value) {
        let mut inner = self.inner.lock().unwrap();
        let sink_name = key.and_then(|k| inner.rooms.get(k)).map(|r| r.sink_name.clone());
        let text = match (&sink_name, key.and_then(|k| inner.rooms.get(k))) {
            (Some(_), Some(room)) => format!("{}: {code} {args}", room.name),
            _ => format!("{code} {args}"),
        };
        eprintln!("event: {text}");
        if inner.events.len() == MAX_EVENTS {
            inner.events.pop_front();
        }
        let seq = inner.next_seq;
        inner.next_seq += 1;
        inner.events.push_back(json!({
            "seq": seq,
            "ts": unix_now(),
            "level": level.as_str(),
            "code": code,
            "sink_name": sink_name,
            "args": args,
        }));
        drop(inner);
        self.changed(false, true);
    }

    /// `{"now", "port", "devices": [{"key", "friendly_name", "sink_name", "model",
    /// "screen", "group", "fixed", "phase", "reason", "since", "deadline",
    /// "lost_until", "listeners", "buffering_last_minute", "volume", "muted",
    /// "latency_ms", "track"}]}`. `track` is null or `{"title", "artist",
    /// "album", "art_url", "player", "playing"}`.
    pub fn status_json(&self) -> String {
        let now = unix_now();
        let mut inner = self.inner.lock().unwrap();
        let port = inner.port;
        let devices: Vec<Value> = inner
            .rooms
            .iter_mut()
            .map(|(key, room)| {
                while room.buffering.front().is_some_and(|t| now - t > BUFFERING_WINDOW) {
                    room.buffering.pop_front();
                }
                json!({
                    "key": key,
                    "friendly_name": room.name,
                    "sink_name": room.sink_name,
                    "model": room.model,
                    "screen": room.screen,
                    "group": room.group,
                    "fixed": room.fixed,
                    "phase": room.phase.as_str(),
                    "reason": room.reason,
                    "since": room.since,
                    "deadline": room.deadline,
                    "lost_until": room.lost_until,
                    "listeners": room.listeners,
                    "buffering_last_minute": room.buffering.len(),
                    "volume": room.volume.map(|v| v.0),
                    "muted": room.volume.map(|v| v.1),
                    "latency_ms": room.latency_ms,
                    "track": room.track.as_ref().map(Track::to_json),
                })
            })
            .collect();
        json!({ "now": now, "port": port, "devices": devices }).to_string()
    }

    pub fn events_json(&self) -> String {
        Value::Array(self.inner.lock().unwrap().events.iter().cloned().collect()).to_string()
    }
}

struct Daemon {
    shared: Arc<Shared>,
    events: Sender<Event>,
}

#[interface(name = "org.soundcast.Daemon1")]
impl Daemon {
    #[zbus(property)]
    fn status(&self) -> String {
        self.shared.status_json()
    }

    #[zbus(property)]
    fn events(&self) -> String {
        self.shared.events_json()
    }

    #[zbus(property)]
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }

    /// Takes back a device another sender took, if the room has audio to play.
    fn resume(&self, sink_name: &str) -> zbus::fdo::Result<()> {
        self.send(Event::Resume(self.key(sink_name)?))
    }

    /// Ends now a session that has no audio and waits out its grace period.
    fn stop(&self, sink_name: &str) -> zbus::fdo::Result<()> {
        self.send(Event::Stop(self.key(sink_name)?))
    }

    /// Opens a session now, before any audio: the device is ready when it comes.
    /// Without audio, the session ends after the grace period.
    fn start(&self, sink_name: &str) -> zbus::fdo::Result<()> {
        self.send(Event::Start(self.key(sink_name)?))
    }

    /// Sets the volume of the sink, and so of the device. 0.0 to 1.0.
    fn set_volume(&self, sink_name: &str, level: f64) -> zbus::fdo::Result<()> {
        if !(0.0..=1.0).contains(&level) {
            return Err(zbus::fdo::Error::InvalidArgs(format!("level {level} is not in 0..1")));
        }
        let key = self.key(sink_name)?;
        self.send(Event::Volume { key, level: Some(level as f32), muted: None })
    }

    fn set_mute(&self, sink_name: &str, muted: bool) -> zbus::fdo::Result<()> {
        let key = self.key(sink_name)?;
        self.send(Event::Volume { key, level: None, muted: Some(muted) })
    }

    /// Asks every Cast device on the network to answer again.
    fn rescan(&self) -> zbus::fdo::Result<()> {
        self.send(Event::Rescan)
    }
}

impl Daemon {
    fn key(&self, sink_name: &str) -> zbus::fdo::Result<String> {
        self.shared
            .key_of_sink(sink_name)
            .ok_or_else(|| zbus::fdo::Error::InvalidArgs(format!("no sink {sink_name}")))
    }

    fn send(&self, event: Event) -> zbus::fdo::Result<()> {
        self.events.send(event).map_err(|_| zbus::fdo::Error::Failed("shutting down".into()))
    }
}

/// Connects to the session bus, claims [`BUS_NAME`] and announces changes
/// until the process exits. Returns once the name is owned.
pub fn start(shared: Arc<Shared>, events: Sender<Event>) -> Result<()> {
    let connection = zbus::blocking::connection::Builder::session()
        .context("session bus")?
        .serve_at(PATH, Daemon { shared: shared.clone(), events })
        .context("serve D-Bus object")?
        .name(BUS_NAME)
        .context("claim D-Bus name")?
        .build()
        .with_context(|| format!("connect to the session bus as {BUS_NAME}"))?;

    let (wake_tx, wake_rx) = sync_channel(1);
    *shared.wake.lock().unwrap() = Some(wake_tx);
    thread::spawn(move || announce(connection, shared, wake_rx));
    Ok(())
}

fn announce(connection: zbus::blocking::Connection, shared: Arc<Shared>, wake: Receiver<()>) {
    while wake.recv().is_ok() {
        thread::sleep(COALESCE);
        let mut changed: HashMap<&str, Variant> = HashMap::new();
        if shared.status_dirty.swap(false, Ordering::AcqRel) {
            changed.insert("Status", Variant::from(shared.status_json()));
        }
        if shared.events_dirty.swap(false, Ordering::AcqRel) {
            changed.insert("Events", Variant::from(shared.events_json()));
        }
        if changed.is_empty() {
            continue;
        }
        let body = (INTERFACE, changed, Vec::<String>::new());
        if let Err(error) = connection.emit_signal(
            None::<&str>,
            PATH,
            "org.freedesktop.DBus.Properties",
            "PropertiesChanged",
            &body,
        ) {
            eprintln!("status: PropertiesChanged: {error}");
        }
    }
}
