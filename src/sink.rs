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
//! PipeWire objects belong to the loop thread. Other threads ask for sinks
//! through [`Command`]s.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{Sender, SyncSender};

use anyhow::{Context, Result};
use pipewire as pw;
use pw::core::CoreRc;
use pw::loop_::Signal;
use pw::properties::properties;
use pw::spa;
use pw::stream::{StreamFlags, StreamListener, StreamRc, StreamState};
use spa::param::audio::{AudioFormat, AudioInfoRaw};
use spa::pod::Pod;

use crate::hub::{CHANNELS, RATE};
use crate::room::Control;

pub enum Command {
    Add {
        key: String,
        node_name: String,
        description: String,
        /// Receives the audio played into the sink.
        captured: SyncSender<Vec<u8>>,
        /// Told when the sink starts and stops receiving audio.
        control: Sender<Control>,
    },
    /// Moves the sink volume to follow the Cast device.
    SetVolume {
        key: String,
        /// 0.0 to 1.0, on the scale of the volume slider.
        level: f32,
        muted: bool,
    },
    Remove {
        key: String,
    },
}

/// Below this linear gain the audio is treated as silence.
const MIN_GAIN: f32 = 1e-6;
/// A sample above this level, before the sink volume, is sound (-80 dBFS).
const SOUND_LEVEL: f32 = 1e-4;
/// Silence for this many frames makes the sink inactive.
const SILENCE_FRAMES: u64 = 3 * RATE as u64;

/// What the callbacks of one sink need. The process callback runs on the
/// realtime thread and the others on the loop thread, so the shared values
/// are atomics.
struct Tap {
    captured: SyncSender<Vec<u8>>,
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
    fn new(captured: SyncSender<Vec<u8>>, control: Sender<Control>) -> Self {
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
            && samples.chunks_exact(4).any(|sample| {
                f32::from_le_bytes([sample[0], sample[1], sample[2], sample[3]]).abs() > threshold
            });
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
        let mut pcm = Vec::with_capacity(samples.len() / 2);
        for sample in samples.chunks_exact(4) {
            let value = f32::from_le_bytes([sample[0], sample[1], sample[2], sample[3]]) * scale;
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
    fn set_volume(&self, level: f32, muted: bool) -> Result<()> {
        // The slider scale is the cube root of the linear gain.
        let gain = level.clamp(0.0, 1.0).powi(3);
        self.stream.set_control(spa::sys::SPA_PROP_channelVolumes, &[gain; CHANNELS as usize])?;
        self.stream.set_control(spa::sys::SPA_PROP_mute, &[f32::from(u8::from(muted))])?;
        Ok(())
    }
}

/// Runs the PipeWire loop until SIGINT or SIGTERM. Every sink is removed on return.
pub fn run(commands: pw::channel::Receiver<Command>) -> Result<()> {
    pw::init();

    let mainloop = pw::main_loop::MainLoopRc::new(None).context("PipeWire main loop")?;
    let context = pw::context::ContextRc::new(&mainloop, None).context("PipeWire context")?;
    let core = context.connect_rc(None).context("connect to PipeWire")?;

    let sinks: RefCell<HashMap<String, Sink>> = RefCell::default();
    let _commands = commands.attach(mainloop.loop_(), {
        let core = core.clone();
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
            Command::Remove { key } => {
                sinks.borrow_mut().remove(&key);
            }
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
                let _ = tap.captured.try_send(tap.to_pcm(samples));
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
    let bytes = spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &spa::pod::Value::Object(object),
    )
    .context("serialize audio format")?
    .0
    .into_inner();
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
    use std::sync::mpsc::{channel, sync_channel};

    fn cycle(value: f32, frames: usize) -> Vec<u8> {
        value.to_le_bytes().repeat(frames * CHANNELS as usize)
    }

    #[test]
    fn muted_sink_goes_inactive() {
        let (captured, _audio) = sync_channel(1);
        let (control, controls) = channel();
        let tap = Tap::new(captured, control);

        tap.watch_level(&cycle(0.5, 1024));
        assert!(matches!(controls.try_recv(), Ok(Control::Active(true))));

        tap.muted.store(true, Ordering::Relaxed);
        tap.watch_level(&cycle(0.0, SILENCE_FRAMES as usize));
        assert!(matches!(controls.try_recv(), Ok(Control::Active(false))));
    }
}
