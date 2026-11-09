//! Minimal HTTP server: one endless WAV stream per client at
//! `/stream/<token>/<room>`, and the logo shown on screens at [`LOGO_PATH`].
//!
//! The Cast devices fetch the stream without authentication, so the server
//! listens on every interface. The room key is broadcast by mDNS: the token, a
//! random secret drawn at startup, keeps other hosts on the LAN from guessing
//! the URL and listening in.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};

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

fn handle(mut socket: TcpStream, hubs: &Hubs, shared: &Shared) -> std::io::Result<()> {
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
    let key = path
        .strip_prefix("/stream/")
        .and_then(|rest| rest.split_once('/'))
        .filter(|(token, _)| same_secret(token, &shared.token))
        .map(|(_, key)| key)
        .filter(|_| method == "GET" || method == "HEAD");
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
        b"HTTP/1.1 200 OK\r\n\
          Content-Type: audio/wav\r\n\
          Cache-Control: no-store\r\n\
          Connection: close\r\n\r\n",
    )?;
    let Some(chunks) = chunks else {
        return Ok(());
    };
    shared.listener(key, 1);
    let result = (|| {
        socket.write_all(&wav_header())?;
        for chunk in chunks {
            socket.write_all(&chunk)?;
        }
        Ok(())
    })();
    shared.listener(key, -1);
    result
}

/// Binds the port, then serves clients on a background thread each.
pub fn start(port: u16, hubs: Hubs, shared: Arc<Shared>) -> Result<()> {
    let listener =
        TcpListener::bind(("0.0.0.0", port)).with_context(|| format!("bind TCP port {port}"))?;
    let clients = Arc::new(AtomicUsize::new(0));
    thread::spawn(move || {
        for mut socket in listener.incoming().flatten() {
            let peer = socket.peer_addr().map(|a| a.ip().to_string()).unwrap_or_default();
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
                match handle(socket, &hubs, &shared) {
                    Ok(()) => eprintln!("http: {peer} closed"),
                    Err(error) => eprintln!("http: {peer} closed ({error})"),
                }
                clients.fetch_sub(1, Ordering::AcqRel);
            });
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_random_hex() {
        let (a, b) = (new_token().unwrap(), new_token().unwrap());
        assert_eq!(a.len(), 32);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn secrets_compare() {
        assert!(same_secret("abc", "abc"));
        assert!(!same_secret("abc", "abd"));
        assert!(!same_secret("abc", "ab"));
    }
}
