//! One room: the state machine between a sink and its Cast device.
//!
//! ```text
//! waiting --sink active--> session --sink idle past GRACE--> waiting
//!                             |--error--> backoff --> session
//!                             '--another app took the device--> yielded
//! yielded --sink idle--> waiting
//! ```
//!
//! A device taken while the sink is idle (lingering) just ends the session.
//!
//! A room casts only while something plays on its sink. When another sender
//! takes the device, the room yields and does not take it back until its own
//! audio stops and starts again, or until a client asks to resume.
//!
//! Every change of state is published to [`Shared`] for desktop widgets.

use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use serde_json::json;

use crate::castv2::{self, Connection, Message};
use crate::discovery::Device;
use crate::sink;
use crate::status::{Level, Phase as Shown, Shared};

/// The default media receiver application.
const APP_ID: &str = "CC1AD845";

/// A pause shorter than this keeps the session open.
const GRACE: Duration = Duration::from_secs(20);
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

pub enum Control {
    /// The sink started or stopped receiving audio.
    Active(bool),
    /// The sink volume changed. `level` is on the slider scale, 1.0 is 100%.
    Volume { level: f32, muted: bool },
    /// Discovery saw the device again, maybe at a new address.
    Device(Device),
    /// Take the device back from another sender.
    Resume,
    /// End the session now if the sink has no audio, without waiting out GRACE.
    Stop,
    Shutdown,
}

/// Why a session ended without an error.
enum End {
    /// The sink stayed idle past [`GRACE`].
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
    port: u16,
    control: Receiver<Control>,
    sinks: pipewire::channel::Sender<sink::Command>,
    /// The volume of the sink.
    volume: Volume,
    /// The volume of the device, once it has reported it in this session.
    device_volume: Option<Volume>,
    volume_sent: Instant,
    /// The last volume the device reported within [`VOLUME_HOLD`]. Taken once
    /// the hold is over, in case the device did not settle where it was told.
    held_volume: Option<Volume>,
    /// False with `--no-cast`: the room never opens a session.
    cast: bool,
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
    idle_since: Option<Instant>,
    backoff: Duration,
}

impl Room {
    pub fn new(
        device: Device,
        port: u16,
        control: Receiver<Control>,
        sinks: pipewire::channel::Sender<sink::Command>,
        busy: Arc<AtomicBool>,
        shared: Arc<Shared>,
        cast: bool,
    ) -> Self {
        Self {
            device,
            port,
            control,
            sinks,
            volume: Volume { level: 1.0, muted: false },
            device_volume: None,
            volume_sent: Instant::now(),
            held_volume: None,
            cast,
            busy,
            shared,
            player: Shown::Loading,
            other_app: None,
            yielded: false,
            active: false,
            stop_requested: false,
            idle_since: None,
            backoff: BACKOFF_MIN,
        }
    }

    /// Runs until shutdown.
    pub fn run(mut self) {
        loop {
            // Wait for audio. A yielded room also waits for the audio to stop first.
            while !self.active || self.yielded || !self.cast {
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
                    self.event(Level::Info, "idle_stop", json!({ "seconds": GRACE.as_secs() }));
                }
                // Taken while the sink was idle (for example by a speaker group
                // this room belongs to): nothing is lost, the room is just free.
                Ok(End::Yielded) if !self.active => {
                    self.log("device taken by another sender while idle, session closed");
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
        if let Some(since) = self.idle_since {
            self.show(Shown::Lingering, "", Some(GRACE.saturating_sub(since.elapsed())));
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
            Control::Shutdown => {}
        }
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
                level: target.level,
                muted: target.muted,
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
        let url = format!(
            "http://{}:{}/stream/{}/{}",
            local_ip_towards(self.device.address)?,
            self.port,
            self.shared.token,
            self.device.key
        );
        self.log(&format!("connecting to {}", self.device.address));
        self.show(Shown::Connecting, "", None);
        self.player = Shown::Loading;
        self.other_app = None;
        self.stop_requested = false;
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
            if self.idle_since.is_some_and(|since| since.elapsed() > GRACE) {
                stop(&mut cast, &phase);
                return Ok(End::Idle);
            }
            if last_heard.elapsed() > DEAD_AFTER {
                bail!("no answer from the device");
            }
            if !matches!(phase, Phase::Playing { .. }) && started.elapsed() > SETUP_TIMEOUT {
                stop(&mut cast, &phase);
                if matches!(phase, Phase::Loading { .. }) && self.shared.listeners(&self.device.key) == 0
                {
                    // The device never fetched the stream: most likely a firewall.
                    self.event(Level::Error, "not_fetched", json!({ "port": self.port }));
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

            let Some(message) = cast.poll()? else {
                continue;
            };
            last_heard = Instant::now();
            if let Some(end) = self.handle(&mut cast, &mut phase, &message, &url)? {
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
        url: &str,
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
                if let Some(other) = apps.iter().find(|app| app["appId"] != APP_ID)
                    && let Some(name) = other["displayName"].as_str()
                {
                    self.other_app = Some(name.to_string());
                }
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
                                    "media": {
                                        "contentId": url,
                                        "contentType": "audio/wav",
                                        "streamType": "LIVE",
                                        "metadata": { "metadataType": 0, "title": "sound-cast" },
                                    },
                                }),
                            )?;
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
                stop(cast, phase);
                bail!("the device refused the stream ({})", message.kind());
            }
            (castv2::NS_MEDIA, "MEDIA_STATUS") => {
                let status = &payload["status"][0];
                match status["playerState"].as_str() {
                    Some(state @ ("BUFFERING" | "PAUSED")) => {
                        self.player =
                            if state == "PAUSED" { Shown::Paused } else { Shown::Buffering };
                    }
                    Some("PLAYING") => {
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
                        Some("CANCELLED" | "INTERRUPTED") => {
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

/// The local address this host uses to reach `peer`.
fn local_ip_towards(peer: SocketAddr) -> Result<IpAddr> {
    let socket = UdpSocket::bind(if peer.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" })?;
    socket.connect(peer)?;
    Ok(socket.local_addr()?.ip())
}
