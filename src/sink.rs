//! Virtual PipeWire sinks, one per room, owned by this process.
//!
//! Each sink is a native PipeWire node: it exists only while the daemon runs,
//! so a crash leaves nothing behind. Whatever applications play into it arrives
//! in the process callback as 48 kHz stereo S16LE.
//!
//! PipeWire objects belong to the loop thread. Other threads ask for sinks
//! through [`Command`]s.

use std::cell::RefCell;
use std::collections::HashMap;
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
    Remove {
        key: String,
    },
}

/// What the callbacks of one sink need.
struct Tap {
    captured: SyncSender<Vec<u8>>,
    control: Sender<Control>,
}

/// A live sink. The listener is declared first so it is dropped first.
struct Sink {
    _listener: StreamListener<Tap>,
    _stream: StreamRc,
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
                match create(&core, &node_name, &description, Tap { captured, control }) {
                    Ok(sink) => {
                        sinks.borrow_mut().insert(key, sink);
                    }
                    Err(error) => eprintln!("sink {node_name}: {error:#}"),
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
        .state_changed(|_, tap, old, new| {
            let was = matches!(old, StreamState::Streaming);
            let is = matches!(new, StreamState::Streaming);
            if was != is {
                let _ = tap.control.send(Control::Active(is));
            }
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
                && let Some(pcm) = bytes.get(offset..offset + size)
                && !pcm.is_empty()
            {
                // Never block the realtime thread: a full queue drops the chunk.
                let _ = tap.captured.try_send(pcm.to_vec());
            }
        })
        .register()
        .context("register listener")?;

    let mut position = [0u32; spa::sys::SPA_AUDIO_MAX_CHANNELS as usize];
    position[0] = spa::sys::SPA_AUDIO_CHANNEL_FL;
    position[1] = spa::sys::SPA_AUDIO_CHANNEL_FR;

    let mut info = AudioInfoRaw::new();
    info.set_format(AudioFormat::S16LE);
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

    Ok(Sink { _listener: listener, _stream: stream })
}
