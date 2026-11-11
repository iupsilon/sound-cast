//! Virtual PipeWire sinks, one per room, owned by this process.
//!
//! Each sink is a native PipeWire node: it exists only while the daemon runs,
//! so a crash leaves nothing behind. Whatever applications play into it arrives
//! in the process callback and leaves it as 48 kHz stereo S16LE.
//!
//! The sink volume is not applied to the audio: it is reported to the room,
//! which sets it on the Cast device. PipeWire's converter does scale the
//! samples, so the process callback divides that gain out again. The samples
//! are taken as 32-bit float to make this lossless.
//!
//! A sink counts as active only while sound arrives in it. PipeWire also runs
//! a sink that nothing plays into, for example when a desktop volume applet
//! records its monitor for a level meter: that must not cast silence to every
//! room.
//!
//! Each sink reports the latency of its Cast device as process latency, so
//! applications that keep video in sync (players, browsers) delay the picture
//! by as much as the device delays the sound.
//!
//! The loop thread also watches the PipeWire graph: which application streams
//! are linked into each sink. It tells the MPRIS watcher, which finds out what
//! they play.
//!
//! PipeWire objects belong to the loop thread. Other threads ask for sinks
//! through [`Command`]s.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::time::Duration;

use anyhow::{Context, Result};
use pipewire as pw;
use pw::core::CoreRc;
use pw::loop_::Signal;
use pw::properties::properties;
use pw::spa;
use pw::stream::{StreamFlags, StreamListener, StreamRc, StreamState};
use spa::param::audio::{AudioFormat, AudioInfoRaw};
use spa::pod::Pod;

use crate::hub::{CHANNELS, Capture, RATE};
use crate::mpris::{self, App};
use crate::room::Control;

pub enum Command {
    Add {
        key: String,
        node_name: String,
        description: String,
        /// Receives the audio played into the sink.
        captured: Capture,
        /// Told when the sink starts and stops receiving audio.
        control: Sender<Control>,
    },
    /// Moves the sink volume, to follow the Cast device or a client. `None`
    /// leaves that control as it is.
    SetVolume {
        key: String,
        /// 0.0 to 1.0, on the scale of the volume slider.
        level: Option<f32>,
        muted: Option<bool>,
    },
    /// Reports how late the device plays what the sink receives.
    SetLatency {
        key: String,
        latency: Duration,
    },
    Remove {
        key: String,
    },
    /// Ends [`run`] as SIGTERM would.
    Quit,
}

/// Below this linear gain the audio is treated as silence.
const MIN_GAIN: f32 = 1e-6;
/// A sample above this level, before the sink volume, is sound (-80 dBFS).
const SOUND_LEVEL: f32 = 1e-4;
/// Silence for this many frames makes the sink inactive.
const SILENCE_FRAMES: u64 = 3 * RATE as u64;
/// How often the applications linked into each sink are looked at.
const SOURCES_EVERY: Duration = Duration::from_secs(1);

/// What the callbacks of one sink need. The process callback runs on the
/// realtime thread and the others on the loop thread, so the shared values
/// are atomics.
struct Tap {
    captured: Capture,
    control: Sender<Control>,
    /// Linear gain the converter applies now, as `f32` bits.
    gain: AtomicU32,
    /// Gain the process callback saw in its previous cycle, as `f32` bits.
    previous_gain: AtomicU32,
    muted: AtomicBool,
    /// True while sound arrives. Changes are sent as [`Control::Active`].
    sounding: AtomicBool,
    /// Frames of silence since the last sound.
    silent_frames: AtomicU64,
}

impl Tap {
    fn new(captured: Capture, control: Sender<Control>) -> Self {
        Self {
            captured,
            control,
            gain: AtomicU32::new(1f32.to_bits()),
            previous_gain: AtomicU32::new(1f32.to_bits()),
            muted: AtomicBool::new(false),
            sounding: AtomicBool::new(false),
            silent_frames: AtomicU64::new(0),
        }
    }

    fn set_sounding(&self, sounding: bool) {
        if self.sounding.swap(sounding, Ordering::Relaxed) != sounding {
            let _ = self.control.send(Control::Active(sounding));
        }
    }

    /// Follows whether the float samples of one cycle carry sound.
    fn watch_level(&self, samples: &[u8]) {
        let gain = f32::from_bits(self.gain.load(Ordering::Relaxed));
        // Muted, the samples are zero whatever plays, and nothing reaches the
        // device either: count it as silence, so a muted sink that keeps
        // running (a level meter on its monitor) does not hold a session open.
        let audible = !self.muted.load(Ordering::Relaxed) && gain >= MIN_GAIN;
        let threshold = SOUND_LEVEL * gain;
        let loud = audible
            && samples.as_chunks::<4>().0.iter().any(|s| f32::from_le_bytes(*s).abs() > threshold);
        if loud {
            self.silent_frames.store(0, Ordering::Relaxed);
            self.set_sounding(true);
        } else {
            let frames = (samples.len() / (4 * CHANNELS as usize)) as u64;
            let silent = self.silent_frames.fetch_add(frames, Ordering::Relaxed) + frames;
            if silent >= SILENCE_FRAMES {
                self.set_sounding(false);
            }
        }
    }

    /// Converts float samples to S16LE at full level, whatever the sink volume.
    /// Fills a spare buffer from the hub: it allocates only when none is left.
    fn to_pcm(&self, samples: &[u8]) -> Vec<u8> {
        let gain = f32::from_bits(self.gain.load(Ordering::Relaxed));
        let previous = f32::from_bits(self.previous_gain.swap(gain.to_bits(), Ordering::Relaxed));
        // The converter and this callback can see a volume change one cycle
        // apart. Dividing by the larger gain makes that cycle quieter, never louder.
        let gain = gain.max(previous);
        let scale = if self.muted.load(Ordering::Relaxed) || gain < MIN_GAIN {
            0.0
        } else {
            f32::from(i16::MAX) / gain
        };
        let mut pcm = self.captured.spare.try_recv().unwrap_or_default();
        pcm.clear();
        pcm.reserve(samples.len() / 2);
        for sample in samples.as_chunks::<4>().0 {
            let value = f32::from_le_bytes(*sample) * scale;
            // `as` saturates, so an over-range sample clips instead of wrapping.
            pcm.extend_from_slice(&(value as i16).to_le_bytes());
        }
        pcm
    }
}

/// A live sink. The listener is declared first so it is dropped first.
struct Sink {
    _listener: StreamListener<Tap>,
    stream: StreamRc,
}

impl Sink {
    fn set_volume(&self, level: Option<f32>, muted: Option<bool>) -> Result<()> {
        if let Some(level) = level {
            // The slider scale is the cube root of the linear gain.
            let gain = level.clamp(0.0, 1.0).powi(3);
            self.stream
                .set_control(spa::sys::SPA_PROP_channelVolumes, &[gain; CHANNELS as usize])?;
        }
        if let Some(muted) = muted {
            self.stream.set_control(spa::sys::SPA_PROP_mute, &[f32::from(u8::from(muted))])?;
        }
        Ok(())
    }

    /// Publishes the latency on the node (process latency) and on its ports.
    /// A sink has nothing downstream to add it to, so the port latency, which
    /// applications read, is set here too.
    fn set_latency(&self, latency: Duration) -> Result<()> {
        let ns = latency.as_nanos().min(i64::MAX as u128) as i64;
        let property =
            |key, value| spa::pod::Property { key, flags: spa::pod::PropertyFlags::empty(), value };
        let process = serialize(spa::pod::Object {
            type_: spa::utils::SpaTypes::ObjectParamProcessLatency.as_raw(),
            id: spa::param::ParamType::ProcessLatency.as_raw(),
            properties: vec![property(
                spa::sys::SPA_PARAM_PROCESS_LATENCY_ns,
                spa::pod::Value::Long(ns),
            )],
        })?;
        let port = serialize(spa::pod::Object {
            type_: spa::utils::SpaTypes::ObjectParamLatency.as_raw(),
            id: spa::param::ParamType::Latency.as_raw(),
            properties: vec![
                property(
                    spa::sys::SPA_PARAM_LATENCY_direction,
                    spa::pod::Value::Id(spa::utils::Id(spa::sys::SPA_DIRECTION_INPUT)),
                ),
                property(spa::sys::SPA_PARAM_LATENCY_minQuantum, spa::pod::Value::Float(0.0)),
                property(spa::sys::SPA_PARAM_LATENCY_maxQuantum, spa::pod::Value::Float(0.0)),
                property(spa::sys::SPA_PARAM_LATENCY_minRate, spa::pod::Value::Int(0)),
                property(spa::sys::SPA_PARAM_LATENCY_maxRate, spa::pod::Value::Int(0)),
                property(spa::sys::SPA_PARAM_LATENCY_minNs, spa::pod::Value::Long(ns)),
                property(spa::sys::SPA_PARAM_LATENCY_maxNs, spa::pod::Value::Long(ns)),
            ],
        })?;
        self.stream.update_params(&mut [
            Pod::from_bytes(&process).context("process latency pod")?,
            Pod::from_bytes(&port).context("latency pod")?,
        ])?;
        Ok(())
    }
}

fn serialize(object: spa::pod::Object) -> Result<Vec<u8>> {
    Ok(spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &spa::pod::Value::Object(object),
    )
    .context("serialize pod")?
    .0
    .into_inner())
}

/// The application streams and links of the PipeWire graph.
#[derive(Default)]
struct Graph {
    /// Application playback streams, by node id, with the id of their client.
    apps: HashMap<u32, (App, Option<u32>)>,
    /// Clients, by id. Native PipeWire applications describe their process on
    /// the client, not on the node. True when the client's process id is the
    /// one the kernel gave for its socket, not one it claims.
    clients: HashMap<u32, (App, bool)>,
    /// Links, by id: (output node, input node).
    links: HashMap<u32, (u32, u32)>,
}

fn app_of(props: &spa::utils::dict::DictRef) -> App {
    App {
        pid: props.get("application.process.id").and_then(|p| p.parse().ok()),
        binary: props.get("application.process.binary").unwrap_or_default().to_string(),
        name: props.get("application.name").unwrap_or_default().to_string(),
    }
}

impl Graph {
    /// Takes a new global. Returns true for an application stream, whose full
    /// properties the caller should fetch for [`Graph::complete`]: the registry
    /// gives only a few of them.
    fn add(&mut self, global: &pw::registry::GlobalObject<&spa::utils::dict::DictRef>) -> bool {
        let Some(props) = global.props else {
            return false;
        };
        match global.type_ {
            pw::types::ObjectType::Node => {
                if !props.get("media.class").is_some_and(|c| c.starts_with("Stream/Output/Audio")) {
                    return false;
                }
                let client = props.get("client.id").and_then(|c| c.parse().ok());
                self.apps.insert(global.id, (app_of(props), client));
                return true;
            }
            pw::types::ObjectType::Client => {
                // A native client's socket credentials beat what it claims (in a
                // Flatpak, its process id inside the sandbox). A PulseAudio client
                // comes through pipewire-pulse, whose own process the socket shows.
                let mut app = app_of(props);
                let checked = props.get("pipewire.sec.pid").and_then(|p| p.parse().ok());
                let native = props.get("client.api") != Some("pipewire-pulse");
                let trusted = native && checked.is_some();
                if trusted {
                    app.pid = checked;
                }
                self.clients.insert(global.id, (app, trusted));
            }
            pw::types::ObjectType::Link => {
                let node = |key| props.get(key).and_then(|n| n.parse().ok());
                if let (Some(output), Some(input)) =
                    (node("link.output.node"), node("link.input.node"))
                {
                    self.links.insert(global.id, (output, input));
                }
            }
            _ => {}
        }
        false
    }

    /// Fills in the application of a stream from the node's full properties.
    fn complete(&mut self, node: u32, props: &spa::utils::dict::DictRef) {
        let full = app_of(props);
        if let Some((app, _)) = self.apps.get_mut(&node) {
            app.pid = full.pid.or(app.pid);
            if !full.binary.is_empty() {
                app.binary = full.binary;
            }
            if !full.name.is_empty() {
                app.name = full.name;
            }
        }
    }

    fn remove(&mut self, id: u32) {
        self.apps.remove(&id);
        self.clients.remove(&id);
        self.links.remove(&id);
    }

    /// The application of a stream node, completed from its client.
    fn app(&self, node: u32) -> Option<App> {
        let (app, client) = self.apps.get(&node)?;
        let mut app = app.clone();
        if let Some((client, trusted)) = client.and_then(|c| self.clients.get(&c)) {
            app.pid = if *trusted { client.pid } else { app.pid.or(client.pid) };
            if app.binary.is_empty() {
                app.binary = client.binary.clone();
            }
            if app.name.is_empty() {
                app.name = client.name.clone();
            }
        }
        Some(app)
    }

    /// The applications linked into the node.
    fn sources(&self, node: u32) -> Vec<App> {
        let mut apps: Vec<App> = self
            .links
            .values()
            .filter(|(_, input)| *input == node)
            .filter_map(|(output, _)| self.app(*output))
            .collect();
        apps.sort_by(|a, b| (a.pid, &a.binary).cmp(&(b.pid, &b.binary)));
        apps.dedup();
        apps
    }
}

/// Runs the PipeWire loop until SIGINT, SIGTERM or [`Command::Quit`]. Every
/// sink is removed on return.
pub fn run(
    commands: pw::channel::Receiver<Command>,
    players: Sender<mpris::Command>,
) -> Result<()> {
    pw::init();

    let mainloop = pw::main_loop::MainLoopRc::new(None).context("PipeWire main loop")?;
    let context = pw::context::ContextRc::new(&mainloop, None).context("PipeWire context")?;
    let core = context.connect_rc(None).context("connect to PipeWire")?;
    let registry = core.get_registry_rc().context("PipeWire registry")?;

    let graph: Rc<RefCell<Graph>> = Rc::default();
    // The application streams bound to read their full properties, by node id.
    let bound: Rc<RefCell<HashMap<u32, (pw::node::Node, pw::node::NodeListener)>>> = Rc::default();
    let _registry = registry
        .add_listener_local()
        .global({
            let graph = graph.clone();
            let bound = bound.clone();
            let registry = registry.downgrade();
            move |global| {
                if !graph.borrow_mut().add(global) {
                    return;
                }
                let Some(registry) = registry.upgrade() else {
                    return;
                };
                let Ok(node) = registry.bind::<pw::node::Node, _>(global) else {
                    return;
                };
                let id = global.id;
                let listener = node
                    .add_listener_local()
                    .info({
                        let graph = Rc::downgrade(&graph);
                        move |info| {
                            if let (Some(graph), Some(props)) = (graph.upgrade(), info.props()) {
                                graph.borrow_mut().complete(id, props);
                            }
                        }
                    })
                    .register();
                bound.borrow_mut().insert(id, (node, listener));
            }
        })
        .global_remove({
            let graph = graph.clone();
            move |id| {
                graph.borrow_mut().remove(id);
                bound.borrow_mut().remove(&id);
            }
        })
        .register();

    let sinks: Rc<RefCell<HashMap<String, Sink>>> = Rc::default();
    // What each room was last told about its sources, and by which node: a
    // room removed and added again between two ticks has a new node and a new
    // listener, which must be told again.
    let told: RefCell<HashMap<String, (u32, Vec<App>)>> = RefCell::default();
    let sources_timer = mainloop.loop_().add_timer({
        let sinks = sinks.clone();
        move |_| {
            let graph = graph.borrow();
            let mut told = told.borrow_mut();
            let sinks = sinks.borrow();
            told.retain(|key, _| sinks.contains_key(key));
            for (key, sink) in sinks.iter() {
                let node = sink.stream.node_id();
                let apps = graph.sources(node);
                if told.get(key).is_none_or(|(n, a)| *n != node || *a != apps) {
                    told.insert(key.clone(), (node, apps.clone()));
                    let _ = players.send(mpris::Command::Sources { key: key.clone(), apps });
                }
            }
        }
    });
    sources_timer.update_timer(Some(SOURCES_EVERY), Some(SOURCES_EVERY)).into_result()?;

    let _commands = commands.attach(mainloop.loop_(), {
        let core = core.clone();
        let mainloop = mainloop.clone();
        let sinks = sinks.clone();
        move |command| match command {
            Command::Add { key, node_name, description, captured, control } => {
                match create(&core, &node_name, &description, Tap::new(captured, control)) {
                    Ok(sink) => {
                        sinks.borrow_mut().insert(key, sink);
                    }
                    Err(error) => eprintln!("sink {node_name}: {error:#}"),
                }
            }
            Command::SetVolume { key, level, muted } => {
                if let Some(sink) = sinks.borrow().get(&key)
                    && let Err(error) = sink.set_volume(level, muted)
                {
                    eprintln!("sink {key}: set volume: {error:#}");
                }
            }
            Command::SetLatency { key, latency } => {
                if let Some(sink) = sinks.borrow().get(&key)
                    && let Err(error) = sink.set_latency(latency)
                {
                    eprintln!("sink {key}: set latency: {error:#}");
                }
            }
            Command::Remove { key } => {
                sinks.borrow_mut().remove(&key);
            }
            Command::Quit => mainloop.quit(),
        }
    });

    let on_int = {
        let mainloop = mainloop.clone();
        move || mainloop.quit()
    };
    let on_term = on_int.clone();
    let _int = mainloop.loop_().add_signal_local(Signal::INT, on_int);
    let _term = mainloop.loop_().add_signal_local(Signal::TERM, on_term);

    mainloop.run();
    Ok(())
}

fn create(core: &CoreRc, node_name: &str, description: &str, tap: Tap) -> Result<Sink> {
    let props = properties! {
        *pw::keys::MEDIA_TYPE => "Audio",
        *pw::keys::MEDIA_CLASS => "Audio/Sink",
        *pw::keys::NODE_NAME => node_name,
        *pw::keys::NODE_DESCRIPTION => description,
    };
    let stream = StreamRc::new(core.clone(), node_name, props).context("create stream")?;

    let listener = stream
        .add_local_listener_with_user_data(tap)
        .state_changed(|_, tap, _, new| {
            // Starting to stream is not enough: sound must arrive (see watch_level).
            if !matches!(new, StreamState::Streaming) {
                tap.set_sounding(false);
            }
        })
        .control_info(|_, tap, id, control| {
            // SAFETY: PipeWire passes a control that is valid for this call,
            // with `n_values` readable floats behind `values`.
            let values = unsafe {
                match control.as_ref() {
                    Some(c) if !c.values.is_null() => {
                        std::slice::from_raw_parts(c.values, c.n_values as usize)
                    }
                    _ => return,
                }
            };
            match id {
                spa::sys::SPA_PROP_channelVolumes => {
                    let gain = values.iter().copied().fold(0.0, f32::max);
                    tap.gain.store(gain.to_bits(), Ordering::Relaxed);
                }
                spa::sys::SPA_PROP_mute => {
                    tap.muted.store(values.first().is_some_and(|v| *v != 0.0), Ordering::Relaxed);
                }
                _ => return,
            }
            let _ = tap.control.send(Control::Volume {
                level: f32::from_bits(tap.gain.load(Ordering::Relaxed)).cbrt(),
                muted: tap.muted.load(Ordering::Relaxed),
            });
        })
        .process(|stream, tap| {
            let Some(mut buffer) = stream.dequeue_buffer() else {
                return;
            };
            let Some(data) = buffer.datas_mut().first_mut() else {
                return;
            };
            let offset = data.chunk().offset() as usize;
            let size = data.chunk().size() as usize;
            if let Some(bytes) = data.data()
                && let Some(samples) = bytes.get(offset..offset + size)
                && !samples.is_empty()
            {
                tap.watch_level(samples);
                // Never block the realtime thread: a full queue drops the chunk.
                let _ = tap.captured.audio.try_send(tap.to_pcm(samples));
            }
        })
        .register()
        .context("register listener")?;

    let mut position = [0u32; spa::sys::SPA_AUDIO_MAX_CHANNELS as usize];
    position[0] = spa::sys::SPA_AUDIO_CHANNEL_FL;
    position[1] = spa::sys::SPA_AUDIO_CHANNEL_FR;

    let mut info = AudioInfoRaw::new();
    info.set_format(AudioFormat::F32LE);
    info.set_rate(RATE);
    info.set_channels(CHANNELS);
    info.set_position(position);

    let object = spa::pod::Object {
        type_: spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
        id: spa::param::ParamType::EnumFormat.as_raw(),
        properties: info.into(),
    };
    let bytes = serialize(object)?;
    let mut params = [Pod::from_bytes(&bytes).context("audio format pod")?];

    // No AUTOCONNECT: the node waits for applications to be routed to it.
    stream
        .connect(
            spa::utils::Direction::Input,
            None,
            StreamFlags::MAP_BUFFERS | StreamFlags::RT_PROCESS,
            &mut params,
        )
        .context("connect stream")?;

    Ok(Sink { _listener: listener, stream })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;

    fn cycle(value: f32, frames: usize) -> Vec<u8> {
        value.to_le_bytes().repeat(frames * CHANNELS as usize)
    }

    #[test]
    fn graph_finds_the_sources_of_a_node() {
        let mut graph = Graph::default();
        let firefox = App { pid: Some(10), binary: "firefox".into(), name: "Firefox".into() };
        graph.apps.insert(5, (firefox.clone(), None));
        graph.links.insert(20, (5, 99));
        graph.links.insert(21, (5, 99)); // the second channel
        graph.links.insert(22, (6, 99)); // not an application stream
        assert_eq!(graph.sources(99), vec![firefox]);
        graph.remove(5);
        assert!(graph.sources(99).is_empty());

        // A native client: the process is on the client.
        let bare = App { pid: None, binary: String::new(), name: "pw-play".into() };
        graph.apps.insert(7, (bare, Some(3)));
        graph.clients.insert(
            3,
            (App { pid: Some(42), binary: "pw-play".into(), name: "pw-play".into() }, true),
        );
        graph.links.insert(23, (7, 99));
        assert_eq!(graph.sources(99)[0].pid, Some(42));
    }

    #[test]
    fn muted_sink_goes_inactive() {
        let (_hub, captured) = crate::hub::Hub::start();
        let (control, controls) = channel();
        let tap = Tap::new(captured, control);

        tap.watch_level(&cycle(0.5, 1024));
        assert!(matches!(controls.try_recv(), Ok(Control::Active(true))));

        tap.muted.store(true, Ordering::Relaxed);
        tap.watch_level(&cycle(0.0, SILENCE_FRAMES as usize));
        assert!(matches!(controls.try_recv(), Ok(Control::Active(false))));
    }
}
