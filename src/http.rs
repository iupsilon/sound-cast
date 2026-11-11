//! Minimal HTTP server: one endless WAV or FLAC stream per client at
//! `/stream/<token>/<room>`, the logo shown on screens at [`LOGO_PATH`], and
//! the cover of the track playing at `/art/<token>/<room>`, when the player
//! gives it as a local file.
//!
//! The Cast devices fetch the stream without authentication, so the server
//! listens on every interface. The room key is broadcast by mDNS: the token, a
//! random secret drawn at startup, keeps other hosts on the LAN from guessing
//! the URL and listening in.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv6Addr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::Settings;
use crate::flac;
use crate::hub::{BYTES_PER_FRAME, CHANNELS, Hub, RATE};
use crate::status::Shared;

/// The hub of every room, by room key.
pub type Hubs = Arc<Mutex<HashMap<String, Arc<Hub>>>>;

const NOT_FOUND: &[u8] = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const BUSY: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// Where the logo is served. It is public: it reveals nothing, so it needs no token.
pub const LOGO_PATH: &str = "/logo.png";
const LOGO: &[u8] = include_bytes!("../assets/logo.png");

/// Where the cover of a room's track is served, before `/<token>/<room>`.
pub const ART_PATH: &str = "/art";
/// Largest cover file served.
const MAX_ART: u64 = 8 * 1024 * 1024;

/// The audio format of the streams.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Codec {
    Wav,
    Flac,
}

impl Codec {
    pub fn content_type(self) -> &'static str {
        match self {
            Codec::Wav => "audio/wav",
            Codec::Flac => "audio/flac",
        }
    }
}

/// Connections served at the same time. Each one holds a thread.
const MAX_CLIENTS: usize = 32;
/// Longest request line and headers read from a client.
const MAX_REQUEST: u64 = 8 * 1024;

/// A random secret for the stream URLs, as 32 hex digits.
pub fn new_token() -> Result<String> {
    let mut bytes = [0u8; 16];
    // SAFETY: the pointer and length describe `bytes`, which outlives the call.
    let read = unsafe { libc::getrandom(bytes.as_mut_ptr().cast(), bytes.len(), 0) };
    if read != bytes.len() as isize {
        bail!("getrandom: {}", std::io::Error::last_os_error());
    }
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// Compares in a time that does not depend on where the inputs differ.
fn same_secret(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// WAV header for a stream of unknown length.
fn wav_header() -> [u8; 44] {
    let mut h = [0u8; 44];
    h[0..4].copy_from_slice(b"RIFF");
    h[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    h[8..16].copy_from_slice(b"WAVEfmt ");
    h[16..20].copy_from_slice(&16u32.to_le_bytes());
    h[20..22].copy_from_slice(&1u16.to_le_bytes()); // PCM
    h[22..24].copy_from_slice(&(CHANNELS as u16).to_le_bytes());
    h[24..28].copy_from_slice(&RATE.to_le_bytes());
    h[28..32].copy_from_slice(&(RATE * BYTES_PER_FRAME).to_le_bytes());
    h[32..34].copy_from_slice(&(BYTES_PER_FRAME as u16).to_le_bytes());
    h[34..36].copy_from_slice(&16u16.to_le_bytes());
    h[36..40].copy_from_slice(b"data");
    h[40..44].copy_from_slice(&u32::MAX.to_le_bytes());
    h
}

/// The image type of a cover, from its first bytes.
fn image_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG") {
        Some("image/png")
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF8") {
        Some("image/gif")
    } else if bytes.len() > 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

/// The cover of the room's track, if it is a local image file.
fn art(shared: &Shared, key: &str) -> Option<(Vec<u8>, &'static str)> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = shared.art(key)?;
    // The path comes from a media player. A FIFO or a device would block the
    // open: only a regular file is opened, and never in a blocking way.
    if !std::fs::metadata(&path).ok()?.is_file() {
        return None;
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY)
        .open(&path)
        .ok()?;
    if !file.metadata().ok()?.is_file() {
        return None;
    }
    let mut bytes = Vec::new();
    file.take(MAX_ART + 1).read_to_end(&mut bytes).ok()?;
    if bytes.len() as u64 > MAX_ART {
        return None;
    }
    let kind = image_type(&bytes)?;
    Some((bytes, kind))
}

/// The room key of a `<prefix>/<token>/<room>` path with the right token.
/// A query string is ignored.
fn room_of<'a>(path: &'a str, prefix: &str, token: &str) -> Option<&'a str> {
    let path = path.split_once('?').map_or(path, |(p, _)| p);
    path.strip_prefix(prefix)?
        .strip_prefix('/')?
        .split_once('/')
        .filter(|(t, _)| same_secret(t, token))
        .map(|(_, key)| key)
}

fn handle(
    mut socket: TcpStream,
    hubs: &Hubs,
    shared: &Shared,
    codec: Codec,
) -> std::io::Result<()> {
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    socket.set_write_timeout(Some(Duration::from_secs(5)))?;
    socket.set_nodelay(true)?;

    let mut reader = BufReader::new(socket.try_clone()?).take(MAX_REQUEST);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    // Drain the headers.
    let mut line = String::new();
    while reader.read_line(&mut line)? > 2 {
        line.clear();
    }

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    if path == LOGO_PATH && (method == "GET" || method == "HEAD") {
        socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: image/png\r\n\
                 Content-Length: {}\r\n\
                 Cache-Control: max-age=86400\r\n\
                 Connection: close\r\n\r\n",
                LOGO.len()
            )
            .as_bytes(),
        )?;
        return if method == "GET" { socket.write_all(LOGO) } else { Ok(()) };
    }
    if (method == "GET" || method == "HEAD")
        && let Some(key) = room_of(path, ART_PATH, &shared.token)
    {
        let Some((bytes, kind)) = art(shared, key) else {
            return socket.write_all(NOT_FOUND);
        };
        socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: {kind}\r\n\
                 Content-Length: {}\r\n\
                 Cache-Control: max-age=3600\r\n\
                 Connection: close\r\n\r\n",
                bytes.len()
            )
            .as_bytes(),
        )?;
        return if method == "GET" { socket.write_all(&bytes) } else { Ok(()) };
    }
    let key =
        room_of(path, "/stream", &shared.token).filter(|_| method == "GET" || method == "HEAD");
    let Some((key, hub)) = key.and_then(|k| Some((k, hubs.lock().unwrap().get(k).cloned()?)))
    else {
        return socket.write_all(NOT_FOUND);
    };

    // Subscribe before the header goes out, so no audio is lost in between.
    let chunks = if method == "GET" { hub.subscribe() } else { None };
    if method == "GET" && chunks.is_none() {
        return socket.write_all(NOT_FOUND);
    }
    socket.write_all(
        format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: {}\r\n\
             Cache-Control: no-store\r\n\
             Connection: close\r\n\r\n",
            codec.content_type()
        )
        .as_bytes(),
    )?;
    let Some(chunks) = chunks else {
        return Ok(());
    };
    shared.listener(key, 1);
    let result = (|| match codec {
        Codec::Wav => {
            socket.write_all(&wav_header())?;
            for chunk in chunks {
                socket.write_all(&chunk)?;
            }
            Ok(())
        }
        Codec::Flac => {
            socket.write_all(&flac::header())?;
            let mut encoder = flac::Encoder::default();
            let mut frames = Vec::new();
            for chunk in chunks {
                encoder.push(&chunk, &mut frames);
                if !frames.is_empty() {
                    socket.write_all(&frames)?;
                    frames.clear();
                }
            }
            Ok(())
        }
    })();
    shared.listener(key, -1);
    result
}

/// Whether an IPv6 socket takes IPv6 only (`IPV6_V6ONLY`). Unknown counts as yes.
fn v6_only(listener: &TcpListener) -> bool {
    use std::os::fd::AsRawFd;
    let mut value: libc::c_int = 1;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: a valid socket, and a buffer of the size given.
    let result = unsafe {
        libc::getsockopt(
            listener.as_raw_fd(),
            libc::IPPROTO_IPV6,
            libc::IPV6_V6ONLY,
            (&raw mut value).cast(),
            &mut len,
        )
    };
    result != 0 || value != 0
}

/// Binds the port, then serves clients on a background thread each. The IPv6
/// socket takes IPv4 too, unless the system makes IPv6 sockets IPv6-only
/// (`net.ipv6.bindv6only`): then a second socket takes IPv4. Without IPv6 on
/// the host, IPv4 only.
pub fn start(settings: Arc<Settings>, hubs: Hubs, shared: Arc<Shared>) -> Result<()> {
    let (port, codec) = (settings.port, settings.codec);
    let mut listeners = Vec::new();
    match TcpListener::bind((Ipv6Addr::UNSPECIFIED, port)) {
        Ok(v6) => {
            // A dual-stack IPv6 socket already takes IPv4: then the bind fails
            // with "address in use", and that is no conflict. On an IPv6-only
            // socket it is another program on the port, and most devices could
            // not fetch the stream: an error, as without IPv6.
            let dual_stack = !v6_only(&v6);
            listeners.push(v6);
            match TcpListener::bind(("0.0.0.0", port)) {
                Ok(v4) => listeners.push(v4),
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                    if !dual_stack {
                        return Err(error).with_context(|| format!("bind TCP port {port} (IPv4)"));
                    }
                }
                Err(error) => eprintln!("http: IPv4 port {port}: {error}; IPv6 only"),
            }
        }
        // Another program on the port, for IPv6 or for IPv4 on a dual-stack host.
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
            return Err(error).with_context(|| format!("bind TCP port {port}"));
        }
        // No IPv6 on the host.
        Err(_) => listeners.push(
            TcpListener::bind(("0.0.0.0", port))
                .with_context(|| format!("bind TCP port {port}"))?,
        ),
    }
    let clients = Arc::new(AtomicUsize::new(0));
    for listener in listeners {
        serve(listener, hubs.clone(), shared.clone(), clients.clone(), codec);
    }
    Ok(())
}

/// Accepts the clients of one listening socket, on a background thread.
fn serve(
    listener: TcpListener,
    hubs: Hubs,
    shared: Arc<Shared>,
    clients: Arc<AtomicUsize>,
    codec: Codec,
) {
    thread::spawn(move || {
        for mut socket in listener.incoming().flatten() {
            // An IPv4 client on the IPv6 socket shows as ::ffff:a.b.c.d.
            let peer = socket
                .peer_addr()
                .map(|a| match a.ip() {
                    std::net::IpAddr::V6(v6) => {
                        v6.to_ipv4_mapped().map_or(v6.to_string(), |v4| v4.to_string())
                    }
                    ip => ip.to_string(),
                })
                .unwrap_or_default();
            if clients.fetch_add(1, Ordering::AcqRel) >= MAX_CLIENTS {
                clients.fetch_sub(1, Ordering::AcqRel);
                eprintln!("http: {peer} refused, {MAX_CLIENTS} clients already");
                let _ = socket.set_write_timeout(Some(Duration::from_secs(1)));
                let _ = socket.write_all(BUSY);
                continue;
            }
            let hubs = hubs.clone();
            let shared = shared.clone();
            let clients = clients.clone();
            thread::spawn(move || {
                eprintln!("http: {peer} connected");
                match handle(socket, &hubs, &shared, codec) {
                    Ok(()) => eprintln!("http: {peer} closed"),
                    Err(error) => eprintln!("http: {peer} closed ({error})"),
                }
                clients.fetch_sub(1, Ordering::AcqRel);
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v6_only_follows_the_system() {
        let Ok(listener) = TcpListener::bind((Ipv6Addr::UNSPECIFIED, 0)) else { return };
        let Ok(system) = std::fs::read_to_string("/proc/sys/net/ipv6/bindv6only") else { return };
        assert_eq!(v6_only(&listener), system.trim() == "1");
    }

    #[test]
    fn tokens_are_random_hex() {
        let (a, b) = (new_token().unwrap(), new_token().unwrap());
        assert_eq!(a.len(), 32);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn paths_need_the_token() {
        assert_eq!(room_of("/stream/abc/sala", "/stream", "abc"), Some("sala"));
        assert_eq!(room_of("/art/abc/sala?v=3", ART_PATH, "abc"), Some("sala"));
        assert_eq!(room_of("/stream/abd/sala", "/stream", "abc"), None);
        assert_eq!(room_of("/streamx/abc/sala", "/stream", "abc"), None);
        assert_eq!(room_of("/stream/abc", "/stream", "abc"), None);
    }

    #[test]
    fn image_types() {
        assert_eq!(image_type(b"\x89PNG\r\n"), Some("image/png"));
        assert_eq!(image_type(&[0xff, 0xd8, 0xff, 0xe0]), Some("image/jpeg"));
        assert_eq!(image_type(b"<svg"), None);
    }

    #[test]
    fn secrets_compare() {
        assert!(same_secret("abc", "abc"));
        assert!(!same_secret("abc", "abd"));
        assert!(!same_secret("abc", "ab"));
    }
}
