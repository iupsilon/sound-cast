//! One room: the state machine between a sink and its Cast device.
//!
//! ```text
//! waiting --sink active--> session --sink idle past the grace--> waiting
//!                             |--error--> backoff --> session
//!                             '--another app took the device--> yielded
//! yielded --sink idle--> waiting
//! ```
//!
//! A device taken while the sink is idle (lingering) just ends the session.
//!
//! A room casts only while something plays on its sink, or when a client asks
//! for a session in advance. When another sender takes the device, the room
//! yields and does not take it back until its own audio stops and starts
//! again, or until a client asks to resume.
//!
//! While the device plays, the room asks it now and then how far it got, and
//! reports the difference to real time as the sink latency. A pause or a stop
//! on the device (the Home app, a voice command) goes to the media player that
//! feeds the sink, through [`mpris`].
//!
//! Every change of state is published to [`Shared`] for desktop widgets.

use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use serde_json::{Value, json};

use crate::castv2::{self, Connection, Message};
use crate::discovery::Device;
use crate::http;
use crate::mpris::{self, Track};
use crate::sink;
use crate::status::{Level, Phase as Shown, Shared};
use crate::{Latency, Settings};

/// The default media receiver application.
const APP_ID: &str = "CC1AD845";

/// Time allowed from connection to playback.
const SETUP_TIMEOUT: Duration = Duration::from_secs(20);
const PING_EVERY: Duration = Duration::from_secs(5);
/// Silence from the device for this long means the connection is dead.
const DEAD_AFTER: Duration = Duration::from_secs(15);
/// How long a takeover waits for news that the sink went idle.
const SETTLE: Duration = Duration::from_secs(2);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Volume levels closer than this are the same level.
const SAME_LEVEL: f32 = 0.005;
/// Shortest time between two volume commands to the device.
const VOLUME_EVERY: Duration = Duration::from_millis(100);
/// After a volume command, the device's own reports are ignored for this long:
/// they describe values that are already out of date.
const VOLUME_HOLD: Duration = Duration::from_secs(1);
/// How long stopping a receiver that is still launching waits for its session id.
const STOP_WAIT: Duration = Duration::from_secs(2);
/// How often a playing device is asked for its position.
const LATENCY_EVERY: Duration = Duration::from_secs(5);
/// Positions kept to measure the latency; the median is reported.
const LATENCY_SAMPLES: usize = 5;
/// A measured latency above this is a wrong reading, not a latency.
const LATENCY_MAX: Duration = Duration::from_secs(30);
/// A new latency closer than this to the reported one is not reported.
const LATENCY_STEP: Duration = Duration::from_millis(50);
/// A session paused on the device stays open this long without audio.
const PAUSE_HOLD: Duration = Duration::from_secs(600);
/// With `--live-metadata`, a reload waits for the track to settle this long:
/// players send the title, the cover and the rest in several updates.
const RELOAD_DEBOUNCE: Duration = Duration::from_millis(1500);

pub enum Control {
    /// The sink started or stopped receiving audio.
    Active(bool),
    /// The sink volume changed. `level` is on the slider scale, 1.0 is 100%.
    Volume { level: f32, muted: bool },
    /// Discovery saw the device again, maybe at a new address.
    Device(Device),
    /// Take the device back from another sender.
    Resume,
    /// End the session now if the sink has no audio, without waiting out the grace period.
    Stop,
    /// Open a session now, without waiting for audio.
    Start,
    /// What plays on the sink changed.
    Track(Option<Track>),
    Shutdown,
}

/// Why a session ended without an error.
enum End {
    /// The sink stayed idle past the grace period.
    Idle,
    /// Another sender took the device, or the user stopped playback there.
    Yielded,
    /// A client asked to end the idle session early.
    Stopped,
    Shutdown,
}

enum Phase {
    Launching { request: u64 },
    Loading { transport: String, session: String },
    Playing { transport: String, session: String },
}

#[derive(Clone, Copy, PartialEq)]
struct Volume {
    /// 0.0 to 1.0.
    level: f32,
    muted: bool,
}

impl std::fmt::Display for Volume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.0}%{}", self.level * 100.0, if self.muted { " muted" } else { "" })
    }
}

impl Volume {
    fn same_as(self, other: Volume) -> bool {
        (self.level - other.level).abs() < SAME_LEVEL && self.muted == other.muted
    }
}

pub struct Room {
    device: Device,
    settings: Arc<Settings>,
    control: Receiver<Control>,
    sinks: pipewire::channel::Sender<sink::Command>,
    players: Sender<mpris::Command>,
    /// The volume of the sink.
    volume: Volume,
    /// The volume of the device, once it has reported it in this session.
    device_volume: Option<Volume>,
    volume_sent: Instant,
    /// The last volume the device reported within [`VOLUME_HOLD`]. Taken once
    /// the hold is over, in case the device did not settle where it was told.
    held_volume: Option<Volume>,
    /// True while a Cast session is open. Read by the manager.
    busy: Arc<AtomicBool>,
    shared: Arc<Shared>,
    /// The player state the device last reported in this session.
    player: Shown,
    /// The other application that last ran on the device, by display name.
    other_app: Option<String>,
    /// Set when another sender took the device. Cleared when the sink goes
    /// idle or a client asks to resume.
    yielded: bool,
    active: bool,
    /// A client asked to end the idle session. Ignored while audio plays.
    stop_requested: bool,
    /// A client asked for a session before any audio.
    start_requested: bool,
    idle_since: Option<Instant>,
    backoff: Duration,
    /// What plays on the sink, shown on the device.
    track: Option<Track>,
    /// The track in the media the device last loaded.
    loaded_track: Option<Track>,
    /// With `--live-metadata`: when to look whether the track changed enough to reload.
    reload_at: Option<Instant>,
    /// Load a fresh stream now: the device holds audio that is out of date.
    restart_stream: bool,
    /// A LOAD of ours that replaces the playing media, until the device plays it:
    /// its request id and when it was sent.
    reload_pending: Option<(u64, Instant)>,
    /// The media a reload replaced: its last reports are about the old stream.
    replaced_media: Option<i64>,
    /// The device cancelled a reload: try it again once the debounce is over.
    retry_reload: bool,
    /// The track loaded before the pending reload, back in place if it is cancelled.
    previous_track: Option<Track>,
    /// The media session of the stream on the device, once it reported it.
    media_session: Option<i64>,
    /// The request id of the last position query.
    status_request: Option<u64>,
    /// Latencies measured in this session, newest last.
    latencies: VecDeque<Duration>,
    /// The latency reported to PipeWire.
    latency: Option<Duration>,
    last_position_query: Instant,
    /// Set when the device paused the stream by itself (the user did, there).
    paused_on_device: Option<Instant>,
}

impl Room {
    pub fn new(
        device: Device,
        settings: Arc<Settings>,
        control: Receiver<Control>,
        sinks: pipewire::channel::Sender<sink::Command>,
        players: Sender<mpris::Command>,
        busy: Arc<AtomicBool>,
        shared: Arc<Shared>,
    ) -> Self {
        Self {
            device,
            settings,
            control,
            sinks,
            players,
            volume: Volume { level: 1.0, muted: false },
            device_volume: None,
            volume_sent: Instant::now(),
            held_volume: None,
            busy,
            shared,
            player: Shown::Loading,
            other_app: None,
            yielded: false,
            active: false,
            stop_requested: false,
            start_requested: false,
            idle_since: None,
            backoff: BACKOFF_MIN,
            track: None,
            loaded_track: None,
            reload_at: None,
            restart_stream: false,
            reload_pending: None,
            replaced_media: None,
            retry_reload: false,
            previous_track: None,
            media_session: None,
            status_request: None,
            latencies: VecDeque::new(),
            latency: None,
            last_position_query: Instant::now(),
            paused_on_device: None,
        }
    }

    /// Runs until shutdown.
    pub fn run(mut self) {
        if let Latency::Fixed(latency) = self.settings.latency {
            self.report_latency(latency);
        }
        loop {
            // Wait for audio. A yielded room also waits for the audio to stop first.
            while (!self.active && !self.start_requested) || self.yielded || !self.settings.cast {
                if self.yielded {
                    let app = self.other_app.clone().unwrap_or_default();
                    self.show(Shown::Busy, &app, None);
                } else {
                    self.show(Shown::Ready, "", None);
                }
                match self.control.recv() {
                    Ok(Control::Shutdown) | Err(_) => return,
                    Ok(control) => self.apply(control),
                }
            }

            self.busy.store(true, Ordering::Release);
            let outcome = self.session();
            self.busy.store(false, Ordering::Release);

            // When the audio moves from this room to a group it belongs to, the
            // group can take the device before PipeWire reports this sink idle.
            // Give that report a moment before treating it as a real takeover.
            if matches!(outcome, Ok(End::Yielded)) && self.active && !self.wait(SETTLE) {
                return;
            }

            match outcome {
                Ok(End::Shutdown) => return,
                Ok(End::Stopped) => {
                    self.log("stopped on request, session closed");
                    self.event(Level::Info, "stopped", json!({}));
                }
                Ok(End::Idle) => {
                    self.log("idle, session closed");
                    let seconds = self.settings.grace.as_secs();
                    self.event(Level::Info, "idle_stop", json!({ "seconds": seconds }));
                }
                // Taken while the sink was idle (for example by a speaker group
                // this room belongs to): nothing is lost, the room is just free.
                Ok(End::Yielded) if !self.active => {
                    self.log("device taken by another sender while idle, session closed");
                }
                // Stopped on the device itself, with no other app taking it: the
                // user wants the music to stop, so the player stops too.
                Ok(End::Yielded) if self.other_app.is_none() => {
                    self.log("stopped on the device, pausing the player");
                    self.event(Level::Info, "stopped_on_device", json!({}));
                    let _ =
                        self.players.send(mpris::Command::Pause { key: self.device.key.clone() });
                    self.yielded = true;
                }
                Ok(End::Yielded) => {
                    self.log("device taken by another sender, yielding");
                    self.event(Level::Warning, "yielded", json!({ "app": self.other_app }));
                    self.yielded = true;
                }
                Err(error) => {
                    let text = format!("{error:#}");
                    self.log(&format!("{text}, retry in {:?}", self.backoff));
                    self.event(
                        Level::Warning,
                        "error",
                        json!({ "error": text, "retry": self.backoff.as_secs() }),
                    );
                    self.show(Shown::Restarting, &text, Some(self.backoff));
                    if !self.wait(self.backoff) {
                        return;
                    }
                    self.backoff = (self.backoff * 2).min(BACKOFF_MAX);
                }
            }
        }
    }

    fn show(&self, phase: Shown, reason: &str, deadline: Option<Duration>) {
        self.shared.set_phase(&self.device.key, phase, reason, deadline);
    }

    fn event(&self, level: Level, code: &str, args: serde_json::Value) {
        self.shared.event(level, code, Some(&self.device.key), args);
    }

    /// Publishes the state of the open session.
    fn show_session(&self, phase: &Phase) {
        if let Some(paused) = self.paused_on_device {
            self.show(Shown::Paused, "", Some(PAUSE_HOLD.saturating_sub(paused.elapsed())));
            return;
        }
        if let Some(since) = self.idle_since {
            self.show(
                Shown::Lingering,
                "",
                Some(self.settings.grace.saturating_sub(since.elapsed())),
            );
            return;
        }
        let shown = match phase {
            Phase::Launching { .. } => Shown::Launching,
            Phase::Loading { .. } => Shown::Loading,
            Phase::Playing { .. } => self.player,
        };
        self.show(shown, "", None);
    }

    fn log(&self, text: &str) {
        eprintln!("{}: {text}", self.device.name);
    }

    fn apply(&mut self, control: Control) {
        match control {
            Control::Active(active) => {
                self.active = active;
                self.idle_since = (!active).then(Instant::now);
                if active {
                    self.stop_requested = false;
                    // Played again on the desktop after a pause on the device: the
                    // device still waits, paused on old audio. A fresh stream plays.
                    if self.paused_on_device.take().is_some() {
                        self.log("played again on the desktop, reloading the stream");
                        self.restart_stream = true;
                    }
                } else {
                    self.yielded = false;
                }
            }
            Control::Volume { level, muted } => {
                self.volume = Volume { level: level.clamp(0.0, 1.0), muted };
            }
            Control::Device(device) => self.device = device,
            Control::Resume => {
                if self.yielded {
                    self.event(Level::Info, "resumed", json!({}));
                }
                self.yielded = false;
            }
            Control::Stop => self.stop_requested = !self.active,
            Control::Start if self.active => {}
            Control::Start => {
                if self.busy.load(Ordering::Acquire) {
                    // A session is open: it waits a whole grace period from now.
                    self.idle_since = Some(Instant::now());
                } else {
                    self.start_requested = true;
                    self.yielded = false;
                }
            }
            Control::Track(track) => {
                self.track = track;
                if self.settings.live_metadata {
                    self.reload_at = Some(Instant::now() + RELOAD_DEBOUNCE);
                }
            }
            Control::Shutdown => {}
        }
    }

    /// True when the session has been idle long enough to close.
    fn idle_expired(&self) -> bool {
        let limit = if self.paused_on_device.is_some() {
            PAUSE_HOLD.max(self.settings.grace)
        } else {
            self.settings.grace
        };
        self.idle_since.is_some_and(|since| since.elapsed() > limit)
    }

    fn report_latency(&mut self, latency: Duration) {
        self.log(&format!("latency {} ms", latency.as_millis()));
        self.latency = Some(latency);
        self.shared.set_latency(&self.device.key, Some(latency));
        let _ =
            self.sinks.send(sink::Command::SetLatency { key: self.device.key.clone(), latency });
    }

    /// Takes a position the device reported. The stream started at position 0
    /// when the device connected, and the hub keeps it in real time: what it
    /// sent beyond the position is what the device holds back.
    fn take_position(&mut self, seconds: f64) {
        if self.settings.latency != Latency::Auto || !seconds.is_finite() || seconds < 0.0 {
            return;
        }
        let Some(started) = self.shared.stream_started(&self.device.key) else {
            return;
        };
        let Ok(position) = Duration::try_from_secs_f64(seconds) else {
            return;
        };
        let Some(latency) = started.elapsed().checked_sub(position) else {
            return;
        };
        if latency > LATENCY_MAX {
            return;
        }
        if self.latencies.len() == LATENCY_SAMPLES {
            self.latencies.pop_front();
        }
        self.latencies.push_back(latency);
        let mut sorted: Vec<Duration> = self.latencies.iter().copied().collect();
        sorted.sort();
        let median = sorted[sorted.len() / 2];
        let moved = self.latency.is_none_or(|l| l.abs_diff(median) >= LATENCY_STEP);
        if moved {
            self.report_latency(median);
        }
    }

    /// The media to load: the track playing, or the room name over the logo.
    fn media(&self, base: &str) -> Value {
        let logo =
            json!({ "url": format!("{base}{}", http::LOGO_PATH), "width": 512, "height": 512 });
        let mut metadata = match &self.track {
            Some(track) => {
                let image = if track.art_url.starts_with("http://")
                    || track.art_url.starts_with("https://")
                {
                    json!({ "url": track.art_url })
                } else if let Some(file) = track.art_file() {
                    // The version makes the device fetch a new cover with each track,
                    // also from a player that writes every cover to the same file.
                    let stamp = std::fs::metadata(&file)
                        .map(|m| format!("{:?} {}", m.modified().ok(), m.len()))
                        .unwrap_or_default();
                    let version = fnv(&format!(
                        "{}\n{}\n{}\n{}\n{stamp}",
                        track.art_url, track.title, track.artist, track.album
                    ));
                    let url = format!(
                        "{base}{}/{}/{}?v={version:x}",
                        http::ART_PATH,
                        self.shared.token,
                        self.device.key
                    );
                    json!({ "url": url })
                } else {
                    logo
                };
                json!({
                    "metadataType": 3,
                    "title": track.title,
                    "artist": if track.artist.is_empty() { self.device.name.clone() } else { track.artist.clone() },
                    "images": [image],
                })
            }
            None => json!({
                "metadataType": 3,
                "title": self.device.name,
                "artist": "sound-cast",
                "images": [logo],
            }),
        };
        if let Some(track) = &self.track
            && !track.album.is_empty()
        {
            metadata["albumName"] = json!(track.album);
        }
        json!({
            "contentId": format!("{base}/stream/{}/{}", self.shared.token, self.device.key),
            "contentType": self.settings.codec.content_type(),
            "streamType": "LIVE",
            "metadata": metadata,
        })
    }

    /// Sends the sink volume to the device when the two differ.
    fn push_volume(&mut self, cast: &mut Connection) -> Result<()> {
        // Until the device has reported its volume, it is the one that leads.
        let Some(device) = self.device_volume else {
            return Ok(());
        };
        if self.volume.same_as(device) || self.volume_sent.elapsed() < VOLUME_EVERY {
            return Ok(());
        }
        if (self.volume.level - device.level).abs() >= SAME_LEVEL {
            cast.request(
                castv2::NS_RECEIVER,
                castv2::RECEIVER,
                json!({ "type": "SET_VOLUME", "volume": { "level": self.volume.level } }),
            )?;
        }
        if self.volume.muted != device.muted {
            cast.request(
                castv2::NS_RECEIVER,
                castv2::RECEIVER,
                json!({ "type": "SET_VOLUME", "volume": { "muted": self.volume.muted } }),
            )?;
        }
        self.log(&format!("volume to device: {}", self.volume));
        self.shared.set_volume(&self.device.key, self.volume.level, self.volume.muted);
        self.device_volume = Some(self.volume);
        self.volume_sent = Instant::now();
        // Reports held so far describe the device before this command.
        self.held_volume = None;
        Ok(())
    }

    /// Takes a volume the device reported and moves the sink to it.
    fn pull_volume(&mut self, reported: &serde_json::Value) {
        let (Some(level), Some(muted)) = (reported["level"].as_f64(), reported["muted"].as_bool())
        else {
            return;
        };
        let reported = Volume { level: level as f32, muted };
        if self.device_volume.is_some() && self.volume_sent.elapsed() < VOLUME_HOLD {
            self.held_volume = Some(reported);
            return;
        }
        self.take_device_volume(reported);
    }

    /// Takes the last report held back by [`VOLUME_HOLD`], once the hold is over.
    fn settle_volume(&mut self) {
        if self.volume_sent.elapsed() >= VOLUME_HOLD
            && let Some(reported) = self.held_volume.take()
            && !self.device_volume.is_some_and(|device| device.same_as(reported))
        {
            self.take_device_volume(reported);
        }
    }

    fn take_device_volume(&mut self, reported: Volume) {
        // At the start of a session the device leads the level only. The sink
        // cannot be muted while it plays, so a device left muted is unmuted.
        let target = if self.device_volume.is_none() {
            Volume { level: reported.level, muted: self.volume.muted }
        } else {
            reported
        };
        self.device_volume = Some(reported);
        self.shared.set_volume(&self.device.key, reported.level, reported.muted);
        if !self.volume.same_as(target) {
            self.log(&format!("volume from device: {target}"));
            self.volume = target;
            let _ = self.sinks.send(sink::Command::SetVolume {
                key: self.device.key.clone(),
                level: Some(target.level),
                muted: Some(target.muted),
            });
        }
    }

    /// Sleeps while still following the controls. Returns false on shutdown.
    fn wait(&mut self, duration: Duration) -> bool {
        let deadline = Instant::now() + duration;
        loop {
            match self.control.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(Control::Shutdown) | Err(RecvTimeoutError::Disconnected) => return false,
                Ok(control) => self.apply(control),
                Err(RecvTimeoutError::Timeout) => return true,
            }
        }
    }

    /// One Cast session: connect, launch the receiver, load the stream, then
    /// watch it until the sink goes idle, the device is taken, or it fails.
    fn session(&mut self) -> Result<End> {
        let base = format!(
            "http://{}",
            SocketAddr::new(local_ip_towards(self.device.address)?, self.settings.port)
        );
        self.log(&format!("connecting to {}", self.device.address));
        self.show(Shown::Connecting, "", None);
        self.player = Shown::Loading;
        self.other_app = None;
        self.stop_requested = false;
        self.reload_at = None;
        self.restart_stream = false;
        self.reload_pending = None;
        self.replaced_media = None;
        self.retry_reload = false;
        self.previous_track = None;
        self.status_request = None;
        self.media_session = None;
        self.latencies.clear();
        self.paused_on_device = None;
        // A session opened on request, before audio, counts as idle from now.
        if self.start_requested && !self.active {
            self.idle_since = Some(Instant::now());
        }
        self.start_requested = false;
        let mut cast = Connection::open(self.device.address)?;
        cast.connect(castv2::RECEIVER)?;
        let request = cast.request(
            castv2::NS_RECEIVER,
            castv2::RECEIVER,
            json!({ "type": "LAUNCH", "appId": APP_ID }),
        )?;

        // The device leads at the start of a session: a sink left at 100%
        // must not make a quiet speaker loud.
        self.device_volume = None;
        self.held_volume = None;
        let mut phase = Phase::Launching { request };
        let started = Instant::now();
        let mut last_ping = Instant::now();
        let mut last_heard = Instant::now();

        loop {
            loop {
                match self.control.try_recv() {
                    Ok(Control::Shutdown) | Err(TryRecvError::Disconnected) => {
                        stop(&mut cast, &phase);
                        return Ok(End::Shutdown);
                    }
                    Ok(control) => self.apply(control),
                    Err(TryRecvError::Empty) => break,
                }
            }
            if self.stop_requested && !self.active {
                stop(&mut cast, &phase);
                return Ok(End::Stopped);
            }
            if self.idle_expired() {
                stop(&mut cast, &phase);
                return Ok(End::Idle);
            }
            if last_heard.elapsed() > DEAD_AFTER {
                bail!("no answer from the device");
            }
            if !matches!(phase, Phase::Playing { .. }) && started.elapsed() > SETUP_TIMEOUT {
                stop(&mut cast, &phase);
                if matches!(phase, Phase::Loading { .. })
                    && self.shared.listeners(&self.device.key) == 0
                {
                    // The device never fetched the stream: most likely a firewall.
                    self.event(Level::Error, "not_fetched", json!({ "port": self.settings.port }));
                    bail!("the device did not fetch the stream");
                }
                bail!("playback did not start");
            }
            self.show_session(&phase);
            self.push_volume(&mut cast)?;
            self.settle_volume();
            if last_ping.elapsed() > PING_EVERY {
                cast.send(castv2::NS_HEARTBEAT, castv2::RECEIVER, &json!({ "type": "PING" }))?;
                last_ping = Instant::now();
            }
            if let Some((_, sent)) = self.reload_pending
                && sent.elapsed() > SETUP_TIMEOUT
            {
                stop(&mut cast, &phase);
                bail!("the reloaded stream did not start");
            }
            if let Phase::Playing { transport, session } = &phase {
                // Looked at only when a reload can go out: a change that comes while
                // one is pending waits for it. Not while paused on the device: a
                // reload would start the device again behind the user's back.
                let track_changed = self.reload_pending.is_none()
                    && self.paused_on_device.is_none()
                    && self.reload_at.is_some_and(|at| Instant::now() >= at)
                    && {
                        self.reload_at = None;
                        std::mem::take(&mut self.retry_reload)
                            || match (&self.loaded_track, &self.track) {
                                (Some(old), Some(new)) => !old.same_song(new),
                                (None, Some(_)) => true,
                                _ => false,
                            }
                    };
                // One reload at a time: a LOAD sent before the last one plays is cancelled.
                if self.reload_pending.is_none() && (self.restart_stream || track_changed) {
                    if track_changed {
                        self.log("track changed, reloading the stream");
                    }
                    self.restart_stream = false;
                    self.retry_reload = false;
                    let request = cast.request(
                        castv2::NS_MEDIA,
                        transport,
                        json!({
                            "type": "LOAD",
                            "sessionId": session,
                            "autoplay": true,
                            "currentTime": 0,
                            "media": self.media(&base),
                        }),
                    )?;
                    self.reload_pending = Some((request, Instant::now()));
                    self.previous_track =
                        std::mem::replace(&mut self.loaded_track, self.track.clone());
                    // Positions of the old stream say nothing about the new one.
                    self.latencies.clear();
                    self.replaced_media = self.media_session.take();
                    self.shared.reset_stream(&self.device.key);
                } else if let Some(media_session) = self.media_session
                    && self.player == Shown::Playing
                    && self.last_position_query.elapsed() > LATENCY_EVERY
                {
                    let request = cast.request(
                        castv2::NS_MEDIA,
                        transport,
                        json!({ "type": "GET_STATUS", "mediaSessionId": media_session }),
                    )?;
                    self.status_request = Some(request);
                    self.last_position_query = Instant::now();
                }
            }

            let Some(message) = cast.poll()? else {
                continue;
            };
            last_heard = Instant::now();
            if let Some(end) = self.handle(&mut cast, &mut phase, &message, &base)? {
                return Ok(end);
            }
        }
    }

    /// Reacts to one message. Returns `Some` when the session is over.
    fn handle(
        &mut self,
        cast: &mut Connection,
        phase: &mut Phase,
        message: &Message,
        base: &str,
    ) -> Result<Option<End>> {
        let payload = &message.payload;
        match (message.namespace.as_str(), message.kind()) {
            (castv2::NS_HEARTBEAT, "PING") => {
                cast.send(castv2::NS_HEARTBEAT, &message.source, &json!({ "type": "PONG" }))?;
            }
            (castv2::NS_CONNECTION, "CLOSE") => {
                if message.source == castv2::RECEIVER {
                    bail!("the device closed the connection");
                }
                return Ok(Some(End::Yielded));
            }
            (castv2::NS_RECEIVER, "LAUNCH_ERROR") => {
                bail!("launch refused: {}", payload["reason"]);
            }
            (castv2::NS_RECEIVER, "RECEIVER_STATUS") => {
                self.pull_volume(&payload["status"]["volume"]);
                let apps = payload["status"]["applications"].as_array();
                let apps = apps.map(Vec::as_slice).unwrap_or_default();
                // Another app, or another sender in a receiver like ours, as of this
                // status: the one that ends the session tells who took the device.
                // An idle screen (a TV's backdrop) is no one.
                let ours = match phase {
                    Phase::Loading { session, .. } | Phase::Playing { session, .. } => {
                        Some(session.as_str())
                    }
                    Phase::Launching { .. } => None,
                };
                self.other_app = apps
                    .iter()
                    .find(|app| {
                        !app["isIdleScreen"].as_bool().unwrap_or(false)
                            && (app["appId"] != APP_ID
                                || ours.is_some_and(|ours| app["sessionId"] != ours))
                    })
                    .map(|app| app["displayName"].as_str().unwrap_or("another app").to_string());
                match phase {
                    Phase::Launching { request } => {
                        let ours = apps.iter().find(|app| app["appId"] == APP_ID);
                        if payload["requestId"] == *request
                            && let Some(app) = ours
                            && let (Some(transport), Some(session)) =
                                (app["transportId"].as_str(), app["sessionId"].as_str())
                        {
                            cast.connect(transport)?;
                            cast.request(
                                castv2::NS_MEDIA,
                                transport,
                                json!({
                                    "type": "LOAD",
                                    "sessionId": session,
                                    "autoplay": true,
                                    "currentTime": 0,
                                    "media": self.media(base),
                                }),
                            )?;
                            self.loaded_track = self.track.clone();
                            self.shared.reset_stream(&self.device.key);
                            *phase = Phase::Loading {
                                transport: transport.to_string(),
                                session: session.to_string(),
                            };
                        }
                    }
                    Phase::Loading { session, .. } | Phase::Playing { session, .. } => {
                        if !apps.iter().any(|app| app["sessionId"] == session.as_str()) {
                            return Ok(Some(End::Yielded));
                        }
                    }
                }
            }
            (castv2::NS_MEDIA, "LOAD_FAILED" | "LOAD_CANCELLED" | "INVALID_REQUEST") => {
                let request = payload["requestId"].as_u64();
                // A position query about media that just ended: nothing is wrong.
                if request.is_some() && request == self.status_request {
                    self.status_request = None;
                    self.media_session = None;
                    return Ok(None);
                }
                // Our reload was cancelled: the media it replaced still plays. Tried
                // again later, so the stream and the track shown catch up.
                if message.kind() == "LOAD_CANCELLED" && self.reload_pending.is_some() {
                    self.log("reload cancelled by the device, trying again");
                    self.reload_pending = None;
                    self.media_session = self.replaced_media.take();
                    self.loaded_track = self.previous_track.take();
                    self.retry_reload = true;
                    self.reload_at = Some(Instant::now() + RELOAD_DEBOUNCE);
                    return Ok(None);
                }
                stop(cast, phase);
                bail!("the device refused the stream ({})", message.kind());
            }
            (castv2::NS_MEDIA, "MEDIA_STATUS") => {
                let status = &payload["status"][0];
                let id = status["mediaSessionId"].as_i64();
                let answers_reload =
                    self.reload_pending.is_some_and(|(request, _)| payload["requestId"] == request);
                // The media a reload of ours replaced: it ends INTERRUPTED, before
                // or after the new media plays, and that is not the user stopping it.
                // Only a device that gives the new media the same id gets it back.
                if id.is_some() && id == self.replaced_media {
                    if !answers_reload {
                        return Ok(None);
                    }
                    self.replaced_media = None;
                }
                if self.reload_pending.is_some()
                    && status["playerState"] == "IDLE"
                    && status["idleReason"] == "INTERRUPTED"
                {
                    return Ok(None);
                }
                if let Some(id) = id {
                    self.media_session = Some(id);
                }
                let playing = matches!(phase, Phase::Playing { .. });
                match status["playerState"].as_str() {
                    Some("PAUSED") => {
                        // Paused there, not by us: we never pause. The player follows.
                        if playing
                            && self.player == Shown::Playing
                            && self.paused_on_device.is_none()
                        {
                            self.log("paused on the device, pausing the player");
                            self.event(Level::Info, "paused_on_device", json!({}));
                            self.paused_on_device = Some(Instant::now());
                            let _ = self
                                .players
                                .send(mpris::Command::Pause { key: self.device.key.clone() });
                        }
                        self.player = Shown::Paused;
                    }
                    Some("BUFFERING") => self.player = Shown::Buffering,
                    Some("PLAYING") => {
                        // Our own reload playing is no resume by the user.
                        let reloaded = self.reload_pending.take().is_some();
                        if !reloaded && self.paused_on_device.take().is_some() {
                            self.log("resumed on the device, resuming the player");
                            self.event(Level::Info, "resumed_on_device", json!({}));
                            let _ = self
                                .players
                                .send(mpris::Command::Play { key: self.device.key.clone() });
                            // The device resumes where it paused, as late as the pause
                            // was long: a fresh stream brings it back to real time.
                            self.restart_stream = true;
                        }
                        if let Some(seconds) = status["currentTime"].as_f64() {
                            self.take_position(seconds);
                        }
                        self.player = Shown::Playing;
                        if let Phase::Loading { transport, session } = phase {
                            self.log("playing");
                            self.event(Level::Info, "playing", json!({}));
                            self.backoff = BACKOFF_MIN;
                            *phase = Phase::Playing {
                                transport: std::mem::take(transport),
                                session: std::mem::take(session),
                            };
                        }
                    }
                    Some("IDLE") => match status["idleReason"].as_str() {
                        Some("CANCELLED") => {
                            stop(cast, phase);
                            return Ok(Some(End::Yielded));
                        }
                        // Another sender loaded its media in our receiver.
                        Some("INTERRUPTED") => {
                            self.other_app.get_or_insert_with(|| "another sender".to_string());
                            stop(cast, phase);
                            return Ok(Some(End::Yielded));
                        }
                        Some(reason @ ("FINISHED" | "ERROR")) => {
                            stop(cast, phase);
                            bail!("playback ended ({reason})");
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
            _ => {}
        }
        Ok(None)
    }
}

/// Stops our receiver application, if it was launched. Errors do not matter:
/// the session is being abandoned anyway.
fn stop(cast: &mut Connection, phase: &Phase) {
    match phase {
        Phase::Launching { .. } => {
            // The launch may have gone through without its answer yet: ask
            // the device for the session id, so the receiver does not stay on.
            if let Some(session) = launched_session(cast) {
                stop_session(cast, &session);
            }
        }
        Phase::Loading { transport, session } | Phase::Playing { transport, session } => {
            stop_session(cast, session);
            let _ = cast.send(castv2::NS_CONNECTION, transport, &json!({ "type": "CLOSE" }));
        }
    }
    let _ = cast.send(castv2::NS_CONNECTION, castv2::RECEIVER, &json!({ "type": "CLOSE" }));
}

fn stop_session(cast: &mut Connection, session: &str) {
    let _ = cast.request(
        castv2::NS_RECEIVER,
        castv2::RECEIVER,
        json!({ "type": "STOP", "sessionId": session }),
    );
}

/// The session id of our receiver application, if the device runs it.
/// Waits at most [`STOP_WAIT`] for the answer.
fn launched_session(cast: &mut Connection) -> Option<String> {
    cast.request(castv2::NS_RECEIVER, castv2::RECEIVER, json!({ "type": "GET_STATUS" })).ok()?;
    let deadline = Instant::now() + STOP_WAIT;
    while Instant::now() < deadline {
        let Some(message) = cast.poll().ok()? else {
            continue;
        };
        if message.namespace != castv2::NS_RECEIVER || message.kind() != "RECEIVER_STATUS" {
            continue;
        }
        let apps = message.payload["status"]["applications"].as_array()?;
        let ours = apps.iter().find(|app| app["appId"] == APP_ID)?;
        return ours["sessionId"].as_str().map(str::to_string);
    }
    None
}

/// FNV-1a: a short stable hash for cache-busting URLs.
fn fnv(text: &str) -> u32 {
    text.bytes().fold(0x811c_9dc5, |hash, byte| (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193))
}

/// The local address this host uses to reach `peer`.
fn local_ip_towards(peer: SocketAddr) -> Result<IpAddr> {
    let socket = UdpSocket::bind(if peer.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" })?;
    socket.connect(peer)?;
    Ok(socket.local_addr()?.ip())
}
