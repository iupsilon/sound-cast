# Changelog

All notable changes to sound-cast. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Audio/video sync: the delay of each device is measured during playback and
  published as the sink's latency, so applications delay the picture to match.
  `--latency auto|off|MS` chooses measured, none or fixed.
- Now playing: the title, artist, album and cover of the media player (MPRIS) that
  feeds a room go to the device's screen, the status (`track`) and the log.
  `--live-metadata` reloads the stream on a track change so the screen follows:
  once per song, again if the device cancels it, never while paused on the device.
- Pause, play and stop on the device pause and resume the desktop player. A
  session paused on the device stays open for up to 10 minutes.
- FLAC streams with `--codec flac`: lossless, about half the bandwidth of WAV.
- `--exclude NAME`, `--alias NAME=LABEL` and `--grace SECONDS` options.
- `--device NAME=HOST[:PORT]` for Cast devices that mDNS cannot find; a host that
  does not resolve is retried every minute.
- D-Bus: `Start`, `SetVolume`, `SetMute` and `Rescan` methods, the `Version`
  property, and `model`, `screen`, `group`, `fixed`, `latency_ms` and `track` in
  `Status`. New event codes `rescan`, `paused_on_device`, `resumed_on_device` and
  `stopped_on_device`.
- IPv6: the stream server listens on IPv6 and IPv4, and devices with only an IPv6
  address are reached at it.
- ARM64 packages (`.deb`, `.rpm`, Arch and the plain binary), built on an ARM64
  runner. The checksums are now in `SHA256SUMS-x86_64` and `SHA256SUMS-aarch64`.
- `.SRCINFO` next to the `PKGBUILD`, for the AUR; the `PKGBUILD` builds on
  x86-64 and ARM64.
- Events carry a `seq` number that grows by one, whatever the clock does.
- An integration test against a fake Cast receiver.

### Changed

- Two devices with the same name get the sinks `sound-cast.name` and
  `sound-cast.name.2`, instead of a part of the device id.
- The stream server also listens on IPv4 when the system makes IPv6 sockets
  IPv6-only; another program on the IPv4 port stops it at startup.
- The release is published once, with the packages of both architectures.

## [0.1.1] - 2026-11-10

- New logo: a Cast frame around a sound wave. Screens show it behind the room name.
- The packages install the logo as the `sound-cast` icon, which the Plasma widget
  shows in "Add Widgets".

## [0.1.0] - 2026-11-10

First public release.

- One PipeWire sink per Chromecast, speaker group or Cast TV, found through mDNS.
- Casting starts when audio arrives on a sink and stops 20 s after it ends.
- Endless WAV stream per room, kept in real time, behind a random token.
- Sink volume and mute follow the device, both ways.
- Yields to another sender that takes the device; `Resume` takes it back.
- Room name and logo on devices with a screen.
- D-Bus interface `org.soundcast.Daemon1` with `Status`, `Events`, `Resume` and `Stop`.
- systemd user service.
- `--port`, `--only`, `--no-cast` and `--version` options.
- Packages for Arch Linux, Debian/Ubuntu and Fedora, built on every tag.

[Unreleased]: https://github.com/iupsilon/sound-cast/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/iupsilon/sound-cast/releases/tag/v0.1.1
[0.1.0]: https://github.com/iupsilon/sound-cast/releases/tag/v0.1.0
