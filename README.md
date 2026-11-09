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

## Status

Early. Missing: volume sync between sink and device, a control interface for a
desktop widget, compressed audio (FLAC), a systemd unit and packaging.
