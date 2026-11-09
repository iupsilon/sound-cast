//! CASTV2 transport: TLS, message framing and the JSON channels.
//!
//! A Cast message is a protobuf `CastMessage` behind a 4-byte big-endian
//! length. Every payload this program needs is JSON text, so the protobuf
//! layer is the few fields below, encoded by hand.
//!
//! [`Connection::poll`] never blocks longer than [`POLL`], so the caller can
//! watch its own deadlines between messages.

use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ClientConnection, DigitallySignedStruct, SignatureScheme, StreamOwned};
use serde_json::{Value, json};

pub const NS_CONNECTION: &str = "urn:x-cast:com.google.cast.tp.connection";
pub const NS_HEARTBEAT: &str = "urn:x-cast:com.google.cast.tp.heartbeat";
pub const NS_RECEIVER: &str = "urn:x-cast:com.google.cast.receiver";
pub const NS_MEDIA: &str = "urn:x-cast:com.google.cast.media";

/// The platform receiver every device runs.
pub const RECEIVER: &str = "receiver-0";
const SENDER: &str = "sender-0";

/// Longest time [`Connection::poll`] waits for data.
pub const POLL: Duration = Duration::from_millis(250);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// The protocol limits a message to 64 KiB.
const MAX_FRAME: usize = 64 * 1024;

#[derive(Debug)]
pub struct Message {
    pub source: String,
    pub namespace: String,
    pub payload: Value,
}

impl Message {
    /// The `type` field of the payload, or an empty string.
    pub fn kind(&self) -> &str {
        self.payload["type"].as_str().unwrap_or("")
    }
}

pub struct Connection {
    tls: StreamOwned<ClientConnection, TcpStream>,
    /// Bytes received and not yet parsed into a message.
    pending: Vec<u8>,
    next_request: u64,
}

impl Connection {
    pub fn open(address: SocketAddr) -> Result<Self> {
        let socket = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)
            .with_context(|| format!("connect to {address}"))?;
        socket.set_nodelay(true)?;
        socket.set_write_timeout(Some(WRITE_TIMEOUT))?;
        socket.set_read_timeout(Some(CONNECT_TIMEOUT))?;

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let schemes = provider.signature_verification_algorithms.supported_schemes();
        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate(schemes)))
            .with_no_client_auth();
        let name = ServerName::IpAddress(address.ip().into());
        let mut tls = StreamOwned::new(ClientConnection::new(Arc::new(config), name)?, socket);

        // Finish the handshake with the long timeout, then switch to polling.
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).context("TLS handshake")?;
        }
        tls.sock.set_read_timeout(Some(POLL))?;

        Ok(Self { tls, pending: Vec::new(), next_request: 1 })
    }

    /// Sends a JSON payload on a namespace.
    pub fn send(&mut self, namespace: &str, destination: &str, payload: &Value) -> Result<()> {
        let body = encode(SENDER, destination, namespace, &payload.to_string());
        let mut frame = (body.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        self.tls.write_all(&frame)?;
        self.tls.flush()?;
        Ok(())
    }

    /// Sends a payload with a fresh `requestId` and returns that id.
    pub fn request(&mut self, namespace: &str, destination: &str, mut payload: Value) -> Result<u64> {
        let id = self.next_request;
        self.next_request += 1;
        payload["requestId"] = json!(id);
        self.send(namespace, destination, &payload)?;
        Ok(id)
    }

    /// Opens a virtual connection to the receiver or to an application.
    pub fn connect(&mut self, destination: &str) -> Result<()> {
        self.send(NS_CONNECTION, destination, &json!({ "type": "CONNECT" }))
    }

    /// Returns the next message, or `None` when none arrived within [`POLL`].
    pub fn poll(&mut self) -> Result<Option<Message>> {
        if let Some(message) = self.take_message()? {
            return Ok(Some(message));
        }
        let mut chunk = [0u8; 4096];
        match self.tls.read(&mut chunk) {
            Ok(0) => bail!("connection closed by the device"),
            Ok(n) => self.pending.extend_from_slice(&chunk[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) => return Err(e.into()),
        }
        self.take_message()
    }

    /// Parses one complete frame out of `pending`, if there is one.
    fn take_message(&mut self) -> Result<Option<Message>> {
        loop {
            let Some(header) = self.pending.first_chunk::<4>() else {
                return Ok(None);
            };
            let length = u32::from_be_bytes(*header) as usize;
            if length > MAX_FRAME {
                bail!("frame of {length} bytes exceeds the protocol limit");
            }
            if self.pending.len() < 4 + length {
                return Ok(None);
            }
            let frame: Vec<u8> = self.pending.drain(..4 + length).skip(4).collect();
            // Binary payloads decode to `None`: skip them.
            if let Some(message) = decode(&frame)? {
                return Ok(Some(message));
            }
        }
    }
}

/// Encodes a `CastMessage` with a text payload.
fn encode(source: &str, destination: &str, namespace: &str, payload: &str) -> Vec<u8> {
    fn text(out: &mut Vec<u8>, field: u8, value: &str) {
        out.push(field << 3 | 2);
        let mut length = value.len();
        while length >= 0x80 {
            out.push(length as u8 | 0x80);
            length >>= 7;
        }
        out.push(length as u8);
        out.extend_from_slice(value.as_bytes());
    }
    let mut out = vec![1 << 3, 0]; // protocol_version = CASTV2_1_0
    text(&mut out, 2, source);
    text(&mut out, 3, destination);
    text(&mut out, 4, namespace);
    out.extend_from_slice(&[5 << 3, 0]); // payload_type = STRING
    text(&mut out, 6, payload);
    out
}

/// Decodes a `CastMessage`. Returns `None` when it has no JSON text payload.
fn decode(mut bytes: &[u8]) -> Result<Option<Message>> {
    fn varint(bytes: &mut &[u8]) -> Result<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let (&byte, rest) = bytes.split_first().ok_or(anyhow!("truncated message"))?;
            *bytes = rest;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        bail!("varint too long")
    }
    fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8]> {
        let (head, rest) = bytes.split_at_checked(length).ok_or(anyhow!("truncated message"))?;
        *bytes = rest;
        Ok(head)
    }

    let (mut source, mut namespace, mut payload) = (None, None, None);
    while !bytes.is_empty() {
        let tag = varint(&mut bytes)?;
        match tag & 7 {
            0 => {
                varint(&mut bytes)?;
            }
            1 => {
                take(&mut bytes, 8)?;
            }
            5 => {
                take(&mut bytes, 4)?;
            }
            2 => {
                let length = varint(&mut bytes)? as usize;
                let value = take(&mut bytes, length)?;
                let slot = match tag >> 3 {
                    2 => &mut source,
                    4 => &mut namespace,
                    6 => &mut payload,
                    _ => continue,
                };
                *slot = Some(std::str::from_utf8(value).context("text field")?);
            }
            other => bail!("unsupported wire type {other}"),
        }
    }
    let (Some(source), Some(namespace), Some(payload)) = (source, namespace, payload) else {
        return Ok(None);
    };
    let Ok(payload) = serde_json::from_str(payload) else {
        return Ok(None);
    };
    Ok(Some(Message { source: source.into(), namespace: namespace.into(), payload }))
}

/// Cast devices present a self-signed certificate: there is no chain to check.
/// The link is encrypted but the device is not authenticated.
#[derive(Debug)]
struct AcceptAnyCertificate(Vec<SignatureScheme>);

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_survives_encode_and_decode() {
        let payload = json!({ "type": "PING", "text": "x".repeat(300) }).to_string();
        let bytes = encode("sender-0", "receiver-0", NS_HEARTBEAT, &payload);
        let message = decode(&bytes).unwrap().unwrap();
        assert_eq!(message.source, "sender-0");
        assert_eq!(message.namespace, NS_HEARTBEAT);
        assert_eq!(message.kind(), "PING");
        assert_eq!(message.payload["text"].as_str().unwrap().len(), 300);
    }

    #[test]
    fn truncated_message_is_an_error() {
        let bytes = encode("a", "b", "c", "{}");
        assert!(decode(&bytes[..bytes.len() - 1]).is_err());
    }
}
