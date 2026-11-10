# Changelog

All notable changes to sound-cast. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

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

[0.1.1]: https://github.com/iupsilon/sound-cast/releases/tag/v0.1.1
[0.1.0]: https://github.com/iupsilon/sound-cast/releases/tag/v0.1.0
