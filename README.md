<p align="center"><img src="assets/logo.svg" width="128" height="128" alt="sound-cast logo"></p>

# sound-cast

Plays the audio of a Linux desktop on Chromecast speakers and speaker groups.

sound-cast finds the Cast devices on the network and creates one PipeWire sink
for each ("Sala", "Cucina", a speaker group). Route an application to a sink and
its audio plays in that room. Nothing is cast while nothing plays.

![The sound-cast Plasma widget](docs/widget.png)

*The sound-cast Plasma widget, playing on a Google Home speaker.*

## The two parts

sound-cast comes in two repositories. Install the daemon first.

| Part | Repository | What it does | Needed |
|---|---|---|---|
| **Daemon** | [iupsilon/sound-cast](https://github.com/iupsilon/sound-cast) (this one) | Finds the Chromecasts, creates one PipeWire sink per room and streams the audio to them. | Yes |
| **Plasma widget** | [iupsilon/plasma-applet-sound-cast](https://github.com/iupsilon/plasma-applet-sound-cast) | KDE Plasma 6 panel and system tray widget: status of every room, one-click output switching, per-room volume, per-app routing, notifications, service on/off, event log. | No |

Without the widget, any PipeWire mixer (pavucontrol, the Plasma volume applet,
GNOME Settings) can route audio to the sinks.

## Features

- One PipeWire sink per Chromecast, Google/Nest speaker, speaker group or TV.
- Starts casting when audio arrives on a sink and stops 20 s after it ends (`--grace`).
- Sink volume and mute follow the device, both ways.
- Audio/video sync: the delay of each device is measured and reported to the
  applications, so a video player shows the picture when the room plays the sound.
- Now playing: the title, artist and cover of the app's media player (MPRIS) show on
  devices with a screen and in the status.
- Pause, play and stop on the device (or in the Google Home app) pause and resume
  the player on the desktop.
- WAV or FLAC streams (`--codec flac`: about half the bandwidth, still lossless).
- Yields to another app (YouTube, Spotify) that takes the device, and can take it back.
- Devices mDNS cannot find, given by address (`--device`); rooms renamed
  (`--alias`) or left out (`--exclude`).
- IPv4 and IPv6; x86-64 and ARM64 packages.
- A D-Bus interface for status and control (volume, mute, start, rescan), used by the
  Plasma widget.
- No configuration file, nothing written to disk.

## Requirements

- Linux with PipeWire (with `pipewire-pulse` or not) and systemd user services.
- Chromecast devices on the same LAN, reachable through mDNS.
- The Cast devices fetch the audio over HTTP from the computer, on port 8574 by
  default. A firewall must allow it from the LAN, for example:

  ```sh
  sudo ufw allow 8574/tcp
  sudo firewall-cmd --permanent --add-port=8574/tcp && sudo firewall-cmd --reload
  ```

## Install

Download the package for your distribution from the
[latest release](https://github.com/iupsilon/sound-cast/releases/latest).

| Distribution | Package | Install |
|---|---|---|
| Arch Linux, CachyOS, Manjaro | `sound-cast-X.Y.Z-1-x86_64.pkg.tar.zst` | `sudo pacman -U ./sound-cast-*.pkg.tar.zst` |
| Debian 12+, Ubuntu 24.04+ | `sound-cast_X.Y.Z-1_amd64.deb` | `sudo apt install ./sound-cast_*.deb` |
| Fedora | `sound-cast-X.Y.Z-1.x86_64.rpm` | `sudo dnf install ./sound-cast-*.rpm` |
| Other (x86-64) | `sound-cast-X.Y.Z-x86_64-linux` | see [Without a package](#without-a-package) |

On ARM64 (a Raspberry Pi 4/5 with a 64-bit system, an ARM laptop) take the packages
named `aarch64` (Arch, Fedora, the plain binary) or `arm64` (Debian, Ubuntu).
`SHA256SUMS-x86_64` and `SHA256SUMS-aarch64` hold the checksums.

The binary is built on Debian 12, so it runs on that and newer distributions.
It is tested on Arch Linux and CachyOS; the `.deb` and `.rpm` packages are built
the same way but are less tested. Reports are welcome.

On Arch you can also build the package from source with the `PKGBUILD` in
[`packaging/arch`](packaging/arch) (x86-64 and ARM64):

```sh
cd packaging/arch && makepkg -si
```

The same `PKGBUILD` and its `.SRCINFO` are what the AUR package `sound-cast` is
made of, for AUR helpers: `paru -S sound-cast` once it is published.

Then start the service for your user, and enable it at login:

```sh
systemctl --user enable --now sound-cast
journalctl --user -u sound-cast -f     # the log
```

The Chromecasts appear as new outputs within a few seconds.

### Without a package

```sh
install -Dm755 sound-cast-*-x86_64-linux ~/.local/bin/sound-cast
sed 's|^ExecStart=.*|ExecStart=%h/.local/bin/sound-cast|' sound-cast.service \
  > ~/.config/systemd/user/sound-cast.service
systemctl --user daemon-reload
systemctl --user enable --now sound-cast
```

## Usage

Pick a room as the output in any audio mixer, or move a single application's
stream to it. The sinks are named `sound-cast.<room>`, for example
`sound-cast.sala`:

```sh
pactl list short sinks | grep sound-cast
pactl set-default-sink sound-cast.sala
```

Command line options, for running it by hand:

```sh
sound-cast                              # every device on the network
sound-cast --only Sala                  # only this device or group
sound-cast --exclude "Living Room TV" --alias "Google Home Mini=Kitchen"
sound-cast --device Studio=192.168.1.40 --codec flac
```

- `--port PORT`: TCP port of the audio streams (default 8574).
- `--only NAME`: create a sink only for this device or group. Repeatable.
- `--exclude NAME`: never create a sink for this device or group. Repeatable.
- `--alias NAME=LABEL`: show the device `NAME` as `LABEL`, in the sink name too
  (`sound-cast.kitchen`). `--only` and `--exclude` accept either name. Repeatable.
- `--device NAME=HOST[:PORT]`: a Cast device that mDNS cannot find, for example on
  another subnet or behind a router that drops multicast. `HOST` is a name or an
  address (`[fd00::5]:8009` for IPv6 with a port); the port defaults to 8009. A host
  that does not resolve is tried again every minute. Repeatable.
- `--grace SECONDS`: how long a session stays open after the audio stops
  (default 20). A longer one avoids the restart gap between songs or videos.
- `--codec wav|flac`: the format of the streams. `wav` (default) costs about
  1.5 Mbit/s per room; `flac` about half, with the same sound.
- `--latency auto|off|MS`: the playback delay reported to the applications, so
  they keep the picture in sync with the room. `auto` (default) measures it on the
  device, `off` reports none, a number reports that many milliseconds.
- `--live-metadata`: load the stream again when the track changes, so a screen
  shows the new title and cover. Each change costs a gap of a second or two;
  without it the screen shows the track that played when the session started.
  Changes are gathered for 1.5 s, so a title and its cover make one reload. A
  reload the device cancels is tried again; none goes out while the device is
  paused.
- `--no-cast`: create the sinks and serve the streams, but never play on a
  device. For tests.
- `--version`: print the version and exit.

To change the options of the service, override its command line:

```sh
systemctl --user edit sound-cast
# [Service]
# ExecStart=
# ExecStart=/usr/bin/sound-cast --port 8575
```

## How it works

- **Sinks**: native PipeWire nodes owned by the daemon. They disappear when it
  stops.
- **Stream**: each sink is served as an endless WAV or FLAC stream (48 kHz,
  stereo, 16 bit) at `http://<host>:<port>/stream/<token>/<room>`. The token is a
  random secret drawn at startup, so other hosts on the LAN cannot guess the
  URL. Nothing is written to disk. The server listens on IPv6 and IPv4. Another
  program on the port stops the daemon at startup, on either. When the extra
  IPv4 socket cannot be opened for another reason (a host without IPv4), the
  log says why and the server carries on with the IPv6 socket; a host without
  IPv6 is served on IPv4 only.
- **Cast**: when a sink receives audio, the daemon starts the default media
  receiver on the device and loads the stream. After 20 s without audio it stops
  the receiver. If another sender takes the device, the daemon yields until its
  own audio stops and starts again. A device with a screen (a TV) shows the
  room name over the sound-cast logo, served at `http://<host>:<port>/logo.png`.
- **Volume**: the sink volume and mute are the device's volume and mute. A
  change on either side moves the other. The audio itself is always sent at
  full level. When a session starts, the device's level wins, so a sink left at
  100% does not make a quiet speaker loud. Mute follows the sink: a muted sink
  never starts a session, so a device left muted is unmuted.
- **Latency**: a Cast device buffers a few seconds of the stream before it plays.
  While a session runs, the daemon compares how much audio it has sent with the
  position the device reports, and publishes the difference as the sink's
  latency. PipeWire passes it on to the applications (and to PulseAudio clients),
  so a video player delays the picture by as much.
- **Now playing**: the daemon follows which applications feed each sink and finds
  their media player on the session bus (MPRIS) by process. The title, artist,
  album and cover go into the stream's metadata, the status and the log. A local
  cover file is served at `http://<host>:<port>/art/<token>/<room>`.
- **Remote control**: pausing on the device pauses the player and keeps the
  session open for up to 10 minutes; playing on the device (or the player) resumes
  it, with a fresh stream so the room is back in real time. Stopping
  the session from the device (or the Google Home app) pauses the player.
- **Discovery**: mDNS, continuous. A device that leaves the network for a minute
  loses its sink. IPv4 addresses are preferred; a device with only IPv6 is
  reached at its routable address. `Rescan` asks every device to answer again.

## D-Bus interface

On the session bus: service `org.soundcast.Daemon`, object `/org/soundcast/Daemon`,
interface `org.soundcast.Daemon1`. The
[Plasma widget](https://github.com/iupsilon/plasma-applet-sound-cast) uses it.

- `Status` (property, JSON string): `{"now", "port", "devices": [...]}`. Each device has
  `key`, `friendly_name`, `sink_name`, `model`, `screen` (it has a display), `group`
  (a speaker group), `fixed` (given with `--device`), `phase`, `reason`, `since`,
  `deadline`, `lost_until`, `listeners` (open stream connections),
  `buffering_last_minute`, `volume` (0-100 or null), `muted`, `latency_ms` (the
  measured delay, or null) and `track` (null, or `{"title", "artist", "album",
  "art_url", "player", "identity", "playing"}`). Times are Unix seconds.
- Phases: `ready`, `connecting`, `launching`, `loading`, `playing`, `buffering`,
  `paused`, `lingering` (no audio, stops at `deadline`), `restarting` (after an
  error, retries at `deadline`), `busy` (another app took the device; `reason` is
  its name).
- `Events` (property, JSON string): the last 50 events, each
  `{"seq", "ts", "level", "code", "sink_name", "args"}`; `seq` grows by one with each
  event, whatever the clock does. Codes: `started`, `found`, `lost`,
  `back`, `removed`, `rescan`, `playing`, `idle_stop`, `stopped`, `yielded`,
  `resumed`, `error`, `not_fetched`, `paused_on_device`, `resumed_on_device`,
  `stopped_on_device`.
- `Version` (property, string): the daemon's version.
- `Resume(s sink_name)` (method): take a device back from the app that took it.
- `Stop(s sink_name)` (method): end now a session that has no audio and is waiting
  out its grace period.
- `Start(s sink_name)` (method): open a session now, before any audio, so the
  device is ready when it comes. Without audio it ends after the grace period.
- `SetVolume(s sink_name, d level)` (method): set the volume of the sink and the
  device, from 0.0 to 1.0.
- `SetMute(s sink_name, b muted)` (method): mute or unmute the sink and the device.
- `Rescan()` (method): ask every Cast device on the network to answer again.

`Status` and `Events` send `PropertiesChanged`, at most every 100 ms: nothing needs
polling.

```sh
busctl --user get-property org.soundcast.Daemon /org/soundcast/Daemon \
  org.soundcast.Daemon1 Status
busctl --user call org.soundcast.Daemon /org/soundcast/Daemon \
  org.soundcast.Daemon1 SetVolume sd sound-cast.sala 0.3
```

## Build from source

Needs Rust (stable, edition 2024), `clang`, `pkg-config` and the PipeWire
development files:

| Distribution | Build dependencies |
|---|---|
| Arch Linux | `sudo pacman -S --needed rust clang pkgconf libpipewire` |
| Debian, Ubuntu | `sudo apt install build-essential clang libclang-dev pkg-config libpipewire-0.3-dev` (Rust from [rustup](https://rustup.rs)) |
| Fedora | `sudo dnf install gcc clang clang-devel pkgconf pipewire-devel cargo` |

```sh
cargo build --release
target/release/sound-cast --only Sala
cargo test
```

To install it for your user as a service:

```sh
cargo install --path .
cp sound-cast.service ~/.config/systemd/user/
# The colour icon, shown by the Plasma widget in "Add Widgets":
install -Dm644 assets/logo.svg ~/.local/share/icons/hicolor/scalable/apps/sound-cast.svg
systemctl --user daemon-reload
systemctl --user enable --now sound-cast
```

## Build the packages

[`packaging/package.sh`](packaging/package.sh) builds the binary and the `.deb`,
`.rpm` and Arch packages into `dist/`. It needs the build dependencies above and
[nfpm](https://nfpm.goreleaser.com):

```sh
packaging/package.sh
```

The packages are for the machine it runs on, x86-64 or ARM64. The
[Packages workflow](.github/workflows/release.yml) runs it on Debian 12 for both,
on every push. For a tag it also publishes a GitHub release with the packages.

## Releasing

1. Set the new version in `Cargo.toml` and run `cargo build` to update `Cargo.lock`.
2. Add a section for it at the top of [`CHANGELOG.md`](CHANGELOG.md).
3. Commit, tag and push:

   ```sh
   git tag -a v0.2.0 -m "sound-cast 0.2.0"
   git push origin master v0.2.0
   ```

The workflow checks that the tag matches `Cargo.toml`, builds the packages and
publishes the release with the notes from the changelog. For Arch, update
`pkgver` in `packaging/arch/PKGBUILD` and regenerate the `.SRCINFO`:

```sh
cd packaging/arch && makepkg --printsrcinfo > .SRCINFO
```

To publish on the AUR, copy `PKGBUILD` and `.SRCINFO` into a clone of
`ssh://aur@aur.archlinux.org/sound-cast.git`, commit and push.

## Limitations

- WAV costs about 1.5 Mbit/s per room. With many rooms on a weak Wi-Fi, use
  `--codec flac`.
- Rooms are not kept in sync with each other: use a Cast speaker group for
  synchronised playback.
- The latency an application sees is one value per sink. When an app plays on
  several rooms at once, each stream gets its room's delay.
- The track title on a screen changes only with `--live-metadata`.

## License

GPL-3.0-or-later. See [`LICENSE`](LICENSE).
