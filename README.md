# sound-cast

Plays the audio of a Linux desktop on Chromecast speakers and speaker groups.

sound-cast finds the Cast devices on the network and creates one PipeWire sink
for each ("Sala", "Cucina", a speaker group). Route an application to a sink and
its audio plays in that room. Nothing is cast while nothing plays.

## How it works

- **Sinks**: native PipeWire nodes owned by the daemon. They disappear when it
  stops.
- **Stream**: each sink is served as an endless WAV stream (48 kHz, stereo,
  16 bit) at `http://<host>:<port>/stream/<room>`. Nothing is written to disk.
- **Cast**: when a sink receives audio, the daemon starts the default media
  receiver on the device and loads the stream. After 20 s without audio it stops
  the receiver. If another sender takes the device, the daemon yields until its
  own audio stops and starts again.
- **Volume**: the sink volume and mute are the device's volume and mute. A
  change on either side moves the other. The audio itself is always sent at
  full level. When a session starts, the device's volume wins, so a sink left at
  100% does not make a quiet speaker loud.
- **Discovery**: mDNS, continuous. A device that leaves the network for a minute
  loses its sink.

## Build and run

Needs Rust, the PipeWire development files and `clang`.

```sh
cargo build --release
target/release/sound-cast            # every device on the network
target/release/sound-cast --only Sala
```

Options:

- `--port PORT`: TCP port of the audio streams (default 3000). The Cast devices
  fetch the audio from this port, so the firewall must allow it from the LAN.
- `--only NAME`: create a sink only for this device or group. Repeatable.
- `--no-cast`: create the sinks and serve the streams, but never play on a
  device. For tests.

## Service

```sh
cargo install --path .
cp sound-cast.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now sound-cast
journalctl --user -u sound-cast -f     # the log
```

## D-Bus interface

On the session bus: service `org.soundcast.Daemon`, object `/org/soundcast/Daemon`,
interface `org.soundcast.Daemon1`. The
[Plasma widget](https://github.com/iupsilon/plasma-applet-sound-cast) uses it.

- `Status` (property, JSON string): `{"now", "port", "devices": [...]}`. Each device has
  `key`, `friendly_name`, `sink_name`, `phase`, `reason`, `since`, `deadline`,
  `lost_until`, `listeners` (open stream connections), `buffering_last_minute`,
  `volume` (0-100 or null) and `muted`. Times are Unix seconds.
- Phases: `ready`, `connecting`, `launching`, `loading`, `playing`, `buffering`,
  `paused`, `lingering` (no audio, stops at `deadline`), `restarting` (after an
  error, retries at `deadline`), `busy` (another app took the device; `reason` is
  its name).
- `Events` (property, JSON string): the last 50 events, each
  `{"ts", "level", "code", "sink_name", "args"}`.
- `Resume(s sink_name)` (method): take a device back from the app that took it.

Both properties send `PropertiesChanged`, at most every 100 ms: nothing needs polling.

## Status

Early. Missing: compressed audio (FLAC) and packaging.
