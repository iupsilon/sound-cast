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
//!
//! Both properties announce their changes with `PropertiesChanged`, at most
//! every [`COALESCE`]. Times are Unix seconds; `now` in `Status` lets a client
//! correct for its own clock.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use zbus::interface;
use zbus::zvariant::Value as Variant;

use crate::Event;

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
}

#[derive(Default)]
struct Inner {
    port: u16,
    rooms: BTreeMap<String, Room>,
    events: VecDeque<Value>,
}

pub struct Shared {
    inner: Mutex<Inner>,
    status_dirty: AtomicBool,
    events_dirty: AtomicBool,
    /// Wakes the D-Bus thread. Empty while there is no D-Bus connection.
    wake: Mutex<Option<SyncSender<()>>>,
}

impl Shared {
    pub fn new(port: u16) -> Arc<Self> {
        Arc::new(Self {
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

    pub fn add_room(&self, key: &str, name: &str, sink_name: &str) {
        self.inner.lock().unwrap().rooms.insert(
            key.to_string(),
            Room {
                name: name.to_string(),
                sink_name: sink_name.to_string(),
                phase: Phase::Ready,
                reason: String::new(),
                since: unix_now(),
                deadline: None,
                lost_until: None,
                listeners: 0,
                buffering: VecDeque::new(),
                volume: None,
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
            room.listeners = room.listeners.saturating_add_signed(delta);
        }
        self.changed(true, false);
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
        inner.events.push_back(json!({
            "ts": unix_now(),
            "level": level.as_str(),
            "code": code,
            "sink_name": sink_name,
            "args": args,
        }));
        drop(inner);
        self.changed(false, true);
    }

    /// `{"now", "port", "devices": [{"key", "friendly_name", "sink_name", "phase",
    /// "reason", "since", "deadline", "lost_until", "listeners",
    /// "buffering_last_minute", "volume", "muted"}]}`
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
                    "phase": room.phase.as_str(),
                    "reason": room.reason,
                    "since": room.since,
                    "deadline": room.deadline,
                    "lost_until": room.lost_until,
                    "listeners": room.listeners,
                    "buffering_last_minute": room.buffering.len(),
                    "volume": room.volume.map(|v| v.0),
                    "muted": room.volume.map(|v| v.1),
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

    /// Takes back a device another sender took, if the room has audio to play.
    fn resume(&self, sink_name: &str) -> zbus::fdo::Result<()> {
        let key = self
            .shared
            .key_of_sink(sink_name)
            .ok_or_else(|| zbus::fdo::Error::InvalidArgs(format!("no sink {sink_name}")))?;
        self.events
            .send(Event::Resume(key))
            .map_err(|_| zbus::fdo::Error::Failed("shutting down".into()))
    }

    /// Ends now a session that has no audio and waits out its grace period.
    fn stop(&self, sink_name: &str) -> zbus::fdo::Result<()> {
        let key = self
            .shared
            .key_of_sink(sink_name)
            .ok_or_else(|| zbus::fdo::Error::InvalidArgs(format!("no sink {sink_name}")))?;
        self.events
            .send(Event::Stop(key))
            .map_err(|_| zbus::fdo::Error::Failed("shutting down".into()))
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
