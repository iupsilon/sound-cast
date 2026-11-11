//! What plays on each sink, from the MPRIS media players on the session bus,
//! and the player controls the Cast devices trigger.
//!
//! PipeWire tells which application streams feed a sink ([`Command::Sources`]).
//! This thread matches them to an MPRIS player: by process id first (the
//! player is the application, or a parent of its audio process, as with
//! browsers), by name otherwise. The player's metadata becomes the room's
//! [`Track`], shown on the device and published on D-Bus. A pause or play on
//! the device is passed to the same player.
//!
//! Players are polled every [`POLL`], and only while some sink has a source.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread;
use std::time::Duration;

use serde_json::{Value, json};
use zbus::blocking::Connection;
use zbus::blocking::fdo::DBusProxy;
use zbus::proxy::CacheProperties;
use zbus::zvariant::OwnedValue;

use crate::room::Control;
use crate::status::Shared;

const POLL: Duration = Duration::from_secs(2);
/// The longest a player may take to answer one call.
const CALL_TIMEOUT: Duration = Duration::from_millis(500);
const PREFIX: &str = "org.mpris.MediaPlayer2.";
const OBJECT: &str = "/org/mpris/MediaPlayer2";
const ROOT: &str = "org.mpris.MediaPlayer2";
const PLAYER: &str = "org.mpris.MediaPlayer2.Player";
/// How far up the process tree a player may be from the audio process.
const MAX_ANCESTORS: usize = 4;

/// An application stream that plays into a sink, as PipeWire describes it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct App {
    pub pid: Option<u32>,
    /// `application.process.binary`, for example `firefox`.
    pub binary: String,
    /// `application.name`, for example `Firefox`.
    pub name: String,
}

pub enum Command {
    /// A room to keep informed with [`Control::Track`].
    Subscribe {
        key: String,
        control: Sender<Control>,
    },
    Unsubscribe {
        key: String,
    },
    /// The applications that play into the room's sink now.
    Sources {
        key: String,
        apps: Vec<App>,
    },
    /// The device paused: pause the player of the room.
    Pause {
        key: String,
    },
    /// The device resumed: resume the player of the room.
    Play {
        key: String,
    },
}

/// What plays on a sink.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Track {
    pub title: String,
    pub artist: String,
    pub album: String,
    /// `mpris:artUrl`: `https://…`, `file://…` or empty.
    pub art_url: String,
    /// The bus name of the player.
    pub player: String,
    /// The player's own name ("Spotify", "Mozilla Firefox").
    pub identity: String,
    pub playing: bool,
}

impl Track {
    pub fn to_json(&self) -> Value {
        json!({
            "title": self.title,
            "artist": self.artist,
            "album": self.album,
            "art_url": self.art_url,
            "player": self.player,
            "identity": self.identity,
            "playing": self.playing,
        })
    }

    /// The local file of the cover, for a `file://` URL.
    /// Only an absolute path on this host counts: `file:///x` or `file://localhost/x`.
    pub fn art_file(&self) -> Option<PathBuf> {
        let rest = self.art_url.strip_prefix("file://")?;
        let path = rest.strip_prefix("localhost").unwrap_or(rest);
        if !path.starts_with('/') {
            return None;
        }
        Some(PathBuf::from(percent_decode(path)?))
    }

    /// True when the change from `self` to `other` is a different track, not
    /// just a pause or a new cover.
    pub fn same_song(&self, other: &Track) -> bool {
        self.title == other.title && self.artist == other.artist && self.album == other.album
    }
}

fn percent_decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// An MPRIS player on the bus.
#[derive(Clone, Debug, Default)]
struct Player {
    bus: String,
    pid: Option<u32>,
    identity: String,
    desktop: String,
    playing: bool,
}

/// Lowercase letters and digits only.
fn normal(text: &str) -> String {
    text.chars().filter(char::is_ascii_alphanumeric).map(|c| c.to_ascii_lowercase()).collect()
}

/// How well a player matches an application: 3 same process, 2 a parent
/// process, 1 same name, 0 not at all.
fn score(app: &App, player: &Player, parent: &dyn Fn(u32) -> Option<u32>) -> u8 {
    if let (Some(app_pid), Some(player_pid)) = (app.pid, player.pid) {
        if app_pid == player_pid {
            return 3;
        }
        let mut pid = app_pid;
        for _ in 0..MAX_ANCESTORS {
            match parent(pid) {
                Some(p) if p > 1 => pid = p,
                _ => break,
            }
            if pid == player_pid {
                return 2;
            }
        }
    }
    // "org.mpris.MediaPlayer2.firefox.instance_1_42" is the player "firefox".
    let short = player.bus.strip_prefix(PREFIX).unwrap_or(&player.bus);
    let short = short.split('.').next().unwrap_or_default();
    let names = [normal(short), normal(&player.desktop), normal(&player.identity)];
    let apps = [normal(&app.binary), normal(&app.name)];
    let similar = |a: &String, b: &String| {
        !a.is_empty()
            && !b.is_empty()
            && (a == b
                || (a.len() >= 4
                    && b.len() >= 4
                    && (a.contains(b.as_str()) || b.contains(a.as_str()))))
    };
    if names.iter().any(|n| apps.iter().any(|a| similar(n, a))) {
        return 1;
    }
    0
}

/// The player that most likely plays the audio of `apps`. A playing player
/// wins over a paused one with the same score.
fn pick<'a>(
    apps: &[App],
    players: &'a [Player],
    parent: &dyn Fn(u32) -> Option<u32>,
) -> Option<&'a Player> {
    players
        .iter()
        .map(|player| {
            (apps.iter().map(|app| score(app, player, parent)).max().unwrap_or(0), player)
        })
        .filter(|(score, _)| *score > 0)
        .max_by_key(|(score, player)| (*score, player.playing))
        .map(|(_, player)| player)
}

/// The parent of a process, from `/proc`.
fn parent_pid(pid: u32) -> Option<u32> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name is in parentheses and may contain spaces.
    let rest = &stat[stat.rfind(')')? + 1..];
    rest.split_whitespace().nth(1)?.parse().ok()
}

struct Room {
    control: Sender<Control>,
    apps: Vec<App>,
    track: Option<Track>,
}

struct Watcher {
    shared: Arc<Shared>,
    rooms: HashMap<String, Room>,
    bus: Option<Connection>,
    /// Set once connecting failed, so the error is logged once.
    bus_failed: bool,
}

/// Starts the thread and returns its command channel.
pub fn start(shared: Arc<Shared>) -> Sender<Command> {
    let (tx, rx) = channel();
    let watcher = Watcher { shared, rooms: HashMap::new(), bus: None, bus_failed: false };
    thread::spawn(move || watcher.run(rx));
    tx
}

impl Watcher {
    fn run(mut self, commands: Receiver<Command>) {
        loop {
            let wait =
                if self.rooms.values().any(|r| !r.apps.is_empty()) { POLL } else { Duration::MAX };
            match commands.recv_timeout(wait) {
                Ok(command) => {
                    let refresh = self.apply(command);
                    // Batch what arrived together before looking at the players.
                    let mut refresh_more = false;
                    while let Ok(command) = commands.try_recv() {
                        refresh_more |= self.apply(command);
                    }
                    if refresh || refresh_more {
                        self.refresh();
                    }
                }
                Err(RecvTimeoutError::Timeout) => self.refresh(),
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Returns true when the tracks need a fresh look.
    fn apply(&mut self, command: Command) -> bool {
        match command {
            Command::Subscribe { key, control } => {
                self.rooms.insert(key, Room { control, apps: Vec::new(), track: None });
                false
            }
            Command::Unsubscribe { key } => {
                self.rooms.remove(&key);
                false
            }
            Command::Sources { key, apps } => {
                if let Some(room) = self.rooms.get_mut(&key) {
                    room.apps = apps;
                }
                true
            }
            Command::Pause { key } => {
                self.call(&key, "Pause");
                false
            }
            Command::Play { key } => {
                self.call(&key, "Play");
                false
            }
        }
    }

    fn bus(&mut self) -> Option<&Connection> {
        if self.bus.is_none() {
            // A frozen player must not stall the watcher: every call has a deadline.
            let bus = zbus::blocking::connection::Builder::session()
                .map(|b| b.method_timeout(CALL_TIMEOUT))
                .and_then(|b| b.build());
            match bus {
                Ok(bus) => self.bus = Some(bus),
                Err(error) => {
                    if !self.bus_failed {
                        eprintln!("mpris: session bus: {error}; no track information");
                        self.bus_failed = true;
                    }
                    return None;
                }
            }
        }
        self.bus.as_ref()
    }

    /// Calls a method of the player of the room, if it has one.
    fn call(&mut self, key: &str, method: &str) {
        let Some(player) =
            self.rooms.get(key).and_then(|r| r.track.as_ref()).map(|t| t.player.clone())
        else {
            return;
        };
        let Some(bus) = self.bus() else {
            return;
        };
        eprintln!("mpris: {method} {player}");
        if let Err(error) =
            bus.call_method(Some(player.as_str()), OBJECT, Some(PLAYER), method, &())
        {
            eprintln!("mpris: {method} {player}: {error}");
        }
    }

    fn players(&mut self) -> Vec<Player> {
        let Some(bus) = self.bus() else {
            return Vec::new();
        };
        let Ok(dbus) = DBusProxy::new(bus) else {
            return Vec::new();
        };
        let names = match dbus.list_names() {
            Ok(names) => names,
            Err(error) => {
                // The bus itself failed (it restarted?): connect again next time.
                eprintln!("mpris: list players: {error}");
                self.bus = None;
                return Vec::new();
            }
        };
        names
            .into_iter()
            .filter(|name| name.starts_with(PREFIX))
            .map(|name| {
                let bus_name = name.to_string();
                let pid = dbus.get_connection_unix_process_id(name.as_ref()).ok();
                let root = property(bus, &bus_name, ROOT, "Identity");
                let desktop = property(bus, &bus_name, ROOT, "DesktopEntry");
                let status = property(bus, &bus_name, PLAYER, "PlaybackStatus");
                Player {
                    pid,
                    identity: root.and_then(|v| String::try_from(v).ok()).unwrap_or_default(),
                    desktop: desktop.and_then(|v| String::try_from(v).ok()).unwrap_or_default(),
                    playing: status.and_then(|v| String::try_from(v).ok()).as_deref()
                        == Some("Playing"),
                    bus: bus_name,
                }
            })
            .collect()
    }

    /// Looks at the players and tells every room whose track changed.
    fn refresh(&mut self) {
        let needed = self.rooms.values().any(|r| !r.apps.is_empty());
        let players = if needed { self.players() } else { Vec::new() };
        let keys: Vec<String> = self.rooms.keys().cloned().collect();
        for key in keys {
            let apps = self.rooms[&key].apps.clone();
            let track = pick(&apps, &players, &parent_pid).and_then(|player| self.track(player));
            let room = self.rooms.get_mut(&key).unwrap();
            if room.track != track {
                match &track {
                    Some(t) => {
                        eprintln!("mpris: {key}: {} - {} ({})", t.artist, t.title, t.identity)
                    }
                    None if room.track.is_some() => eprintln!("mpris: {key}: nothing playing"),
                    None => {}
                }
                room.track = track.clone();
                self.shared.set_track(&key, track.clone());
                let _ = room.control.send(Control::Track(track));
            }
        }
    }

    fn track(&mut self, player: &Player) -> Option<Track> {
        let bus = self.bus()?;
        let metadata = property(bus, &player.bus, PLAYER, "Metadata")?;
        let metadata: HashMap<String, OwnedValue> = metadata.try_into().ok()?;
        let text = |key: &str| {
            metadata
                .get(key)
                .and_then(|v| String::try_from(v.try_clone().ok()?).ok())
                .unwrap_or_default()
        };
        let list = |key: &str| {
            metadata
                .get(key)
                .and_then(|v| Vec::<String>::try_from(v.try_clone().ok()?).ok())
                .map(|v| v.join(", "))
                .unwrap_or_default()
        };
        let track = Track {
            title: text("xesam:title"),
            artist: list("xesam:artist"),
            album: text("xesam:album"),
            art_url: text("mpris:artUrl"),
            player: player.bus.clone(),
            identity: player.identity.clone(),
            playing: player.playing,
        };
        // A player with nothing loaded has no title: that is no track.
        (!track.title.is_empty()).then_some(track)
    }
}

/// Reads one property without caching or watching it.
fn property(
    bus: &Connection,
    destination: &str,
    interface: &str,
    name: &str,
) -> Option<OwnedValue> {
    let proxy = zbus::blocking::proxy::Builder::<zbus::blocking::Proxy>::new(bus)
        .destination(destination.to_string())
        .ok()?
        .path(OBJECT)
        .ok()?
        .interface(interface.to_string())
        .ok()?
        .cache_properties(CacheProperties::No)
        .build()
        .ok()?;
    proxy.get_property(name).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(pid: u32, binary: &str) -> App {
        App { pid: Some(pid), binary: binary.into(), name: String::new() }
    }

    fn player(bus: &str, pid: u32, playing: bool) -> Player {
        Player { bus: format!("{PREFIX}{bus}"), pid: Some(pid), playing, ..Player::default() }
    }

    /// 300 is a child of 200, which is a child of 100.
    fn parent(pid: u32) -> Option<u32> {
        match pid {
            300 => Some(200),
            200 => Some(100),
            100 => Some(1),
            _ => None,
        }
    }

    #[test]
    fn same_process_wins() {
        let players = [player("spotify", 50, false), player("firefox.instance_1_2", 300, false)];
        let picked = pick(&[app(300, "firefox")], &players, &parent).unwrap();
        assert_eq!(picked.pid, Some(300));
    }

    #[test]
    fn parent_process_matches() {
        // Chromium plays from a child process; the player is the browser.
        let players = [player("chromium.instance100", 100, true)];
        assert!(pick(&[app(300, "chromium")], &players, &parent).is_some());
        assert_eq!(score(&app(300, "x"), &players[0], &parent), 2);
    }

    #[test]
    fn name_matches_and_playing_wins() {
        let players =
            [player("vlc", 7, false), player("vlc.instance2", 8, true), player("mpv", 9, true)];
        let picked = pick(&[app(1234, "vlc")], &players, &parent).unwrap();
        assert_eq!(picked.pid, Some(8));
        assert!(pick(&[app(1234, "audacious")], &players, &parent).is_none());
    }

    #[test]
    fn art_files_decode() {
        let track = Track { art_url: "file:///tmp/Cover%20Art/a.png".into(), ..Track::default() };
        assert_eq!(track.art_file(), Some(PathBuf::from("/tmp/Cover Art/a.png")));
        let track = Track { art_url: "https://i.scdn.co/image/x".into(), ..Track::default() };
        assert_eq!(track.art_file(), None);
        let track = Track { art_url: "file://localhost/tmp/a.png".into(), ..Track::default() };
        assert_eq!(track.art_file(), Some(PathBuf::from("/tmp/a.png")));
        let track = Track { art_url: "file://host/tmp/a.png".into(), ..Track::default() };
        assert_eq!(track.art_file(), None);
    }
}
