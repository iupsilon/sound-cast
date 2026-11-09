//! Minimal HTTP server: one endless WAV stream per client at `/stream/<room>`.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::hub::{BYTES_PER_FRAME, CHANNELS, Hub, RATE};

/// The hub of every room, by room key.
pub type Hubs = Arc<Mutex<HashMap<String, Arc<Hub>>>>;

const NOT_FOUND: &[u8] = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

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

fn handle(mut socket: TcpStream, hubs: &Hubs) -> std::io::Result<()> {
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    socket.set_write_timeout(Some(Duration::from_secs(5)))?;
    socket.set_nodelay(true)?;

    let mut reader = BufReader::new(socket.try_clone()?);
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
    let hub = path
        .strip_prefix("/stream/")
        .filter(|_| method == "GET" || method == "HEAD")
        .and_then(|key| hubs.lock().unwrap().get(key).cloned());
    let Some(hub) = hub else {
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
    socket.write_all(&wav_header())?;
    for chunk in chunks {
        socket.write_all(&chunk)?;
    }
    Ok(())
}

/// Binds the port, then serves clients on a background thread each.
pub fn start(port: u16, hubs: Hubs) -> Result<()> {
    let listener =
        TcpListener::bind(("0.0.0.0", port)).with_context(|| format!("bind TCP port {port}"))?;
    thread::spawn(move || {
        for socket in listener.incoming().flatten() {
            let hubs = hubs.clone();
            thread::spawn(move || {
                let peer = socket.peer_addr().map(|a| a.ip().to_string()).unwrap_or_default();
                eprintln!("http: {peer} connected");
                match handle(socket, &hubs) {
                    Ok(()) => eprintln!("http: {peer} closed"),
                    Err(error) => eprintln!("http: {peer} closed ({error})"),
                }
            });
        }
    });
    Ok(())
}
