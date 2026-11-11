//! A room against a fake Cast receiver: TLS, CASTV2 framing, the receiver and
//! media channels, and the device fetching the stream over HTTP.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rustls::pki_types::PrivateKeyDer;
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use serde_json::{Value, json};

use crate::discovery::Device;
use crate::hub::Hub;
use crate::room::{Control, Room};
use crate::status::Shared;
use crate::{Settings, castv2, http, mpris};

/// What the fake device was told, in order.
type Log = Arc<Mutex<Vec<String>>>;

/// How the test drives the fake device's player.
enum Script {
    Pause,
    Play,
}

struct Fake {
    address: SocketAddr,
    log: Log,
    script: Sender<Script>,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn frame(namespace: &str, source: &str, payload: &Value) -> Vec<u8> {
    let body = castv2::encode(source, "sender-0", namespace, &payload.to_string());
    let mut out = (body.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

/// Reads the HTTP stream like a device: the response head, then some audio.
/// Returns the first bytes of the body.
fn fetch(url: &str) -> Vec<u8> {
    let rest = url.strip_prefix("http://").unwrap();
    let (host, path) = rest.split_once('/').unwrap();
    let mut socket = TcpStream::connect(host).unwrap();
    write!(socket, "GET /{path} HTTP/1.1\r\nHost: {host}\r\n\r\n").unwrap();
    let mut data = Vec::new();
    let mut chunk = [0u8; 4096];
    while !data.windows(4).any(|w| w == b"\r\n\r\n") || data.len() < 200 {
        let n = socket.read(&mut chunk).unwrap();
        assert!(n > 0, "stream closed early");
        data.extend_from_slice(&chunk[..n]);
    }
    // Keep the connection open and drained, as a playing device does.
    thread::spawn(move || while matches!(socket.read(&mut chunk), Ok(n) if n > 0) {});
    let head_end = data.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    data[head_end..].to_vec()
}

fn serve(
    socket: TcpStream,
    config: Arc<ServerConfig>,
    log: Log,
    script: Arc<Mutex<Receiver<Script>>>,
) {
    let mut tls = StreamOwned::new(ServerConnection::new(config).unwrap(), socket);
    tls.sock.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
    let mut pending = Vec::new();
    let mut fetched_at: Option<Instant> = None;
    // The media playing, by session id; each LOAD makes a new one.
    let mut media: Option<i64> = None;
    let mut chunk = [0u8; 4096];
    loop {
        if let Ok(command) = script.lock().unwrap().try_recv() {
            let state = match command {
                Script::Pause => "PAUSED",
                Script::Play => "PLAYING",
            };
            let position = fetched_at.map_or(0.0, |t| t.elapsed().as_secs_f64() - 0.3);
            let status = json!({ "type": "MEDIA_STATUS", "status": [{
                "mediaSessionId": media, "playerState": state, "currentTime": position }] });
            if tls.write_all(&frame(castv2::NS_MEDIA, "t1", &status)).is_err() {
                return;
            }
        }
        match tls.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => pending.extend_from_slice(&chunk[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => return,
        }
        while pending.len() >= 4 {
            let length = u32::from_be_bytes(pending[..4].try_into().unwrap()) as usize;
            if pending.len() < 4 + length {
                break;
            }
            let body: Vec<u8> = pending.drain(..4 + length).skip(4).collect();
            let message = castv2::decode(&body).unwrap().unwrap();
            let kind = message.kind().to_string();
            log.lock().unwrap().push(kind.clone());
            let request = message.payload["requestId"].clone();
            let reply = match kind.as_str() {
                "LAUNCH" => Some((
                    castv2::NS_RECEIVER,
                    "receiver-0",
                    json!({
                    "type": "RECEIVER_STATUS", "requestId": request,
                    "status": { "volume": { "level": 0.4, "muted": false }, "applications": [
                        { "appId": "CC1AD845", "transportId": "t1", "sessionId": "s1" }] } }),
                )),
                "LOAD" => {
                    // A load over playing media interrupts it, as a real receiver does.
                    if let Some(old) = media {
                        let interrupted = json!({ "type": "MEDIA_STATUS", "status": [{
                            "mediaSessionId": old, "playerState": "IDLE", "idleReason": "INTERRUPTED" }] });
                        if tls.write_all(&frame(castv2::NS_MEDIA, "t1", &interrupted)).is_err() {
                            return;
                        }
                    }
                    let id = media.map_or(7, |old| old + 1);
                    media = Some(id);
                    let media = &message.payload["media"];
                    let body = fetch(media["contentId"].as_str().unwrap());
                    let codec = &body[..4];
                    log.lock().unwrap().push(format!("fetched {}", String::from_utf8_lossy(codec)));
                    log.lock().unwrap().push(format!("title {}", media["metadata"]["title"]));
                    fetched_at = Some(Instant::now());
                    // The device plays 0.3 s behind what it fetched.
                    thread::sleep(Duration::from_millis(300));
                    Some((
                        castv2::NS_MEDIA,
                        "t1",
                        json!({ "type": "MEDIA_STATUS", "requestId": request,
                        "status": [{ "mediaSessionId": id, "playerState": "PLAYING", "currentTime": 0.0 }] }),
                    ))
                }
                "STOP" => {
                    log.lock().unwrap().push(format!("stop {}", message.payload["sessionId"]));
                    None
                }
                _ => None,
            };
            if let Some((namespace, source, payload)) = reply
                && tls.write_all(&frame(namespace, source, &payload)).is_err()
            {
                return;
            }
        }
    }
}

fn fake_device() -> Fake {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
    let key = PrivateKeyDer::Pkcs8(certified.signing_key.serialize_der().into());
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![certified.cert.der().clone()], key)
            .unwrap();
    let config = Arc::new(config);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let log = Log::default();
    let (script_tx, script_rx) = channel();
    let script = Arc::new(Mutex::new(script_rx));
    {
        let log = log.clone();
        thread::spawn(move || {
            for socket in listener.incoming().flatten() {
                let (config, log, script) = (config.clone(), log.clone(), script.clone());
                thread::spawn(move || serve(socket, config, log, script));
            }
        });
    }
    Fake { address, log, script: script_tx }
}

/// Waits until `check` holds, or fails after `timeout`.
fn eventually(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

fn device_status(shared: &Shared) -> Value {
    let status: Value = serde_json::from_str(&shared.status_json()).unwrap();
    status["devices"][0].clone()
}

struct Setup {
    fake: Fake,
    shared: Arc<Shared>,
    control: Sender<Control>,
    players: Receiver<mpris::Command>,
    _sinks: pipewire::channel::Receiver<crate::sink::Command>,
    _hub: Arc<Hub>,
}

fn setup(settings: Settings) -> Setup {
    let fake = fake_device();
    let (hub, _capture) = Hub::start();
    // Another test can take a free port before it is bound: try another one.
    let (settings, shared) = (0..20)
        .find_map(|_| {
            let settings = Arc::new(Settings { port: free_port(), ..settings.clone() });
            let shared = Shared::new(settings.port, http::new_token().unwrap());
            let hubs = http::Hubs::default();
            hubs.lock().unwrap().insert("dev1".into(), hub.clone());
            http::start(settings.clone(), hubs, shared.clone()).ok().map(|()| (settings, shared))
        })
        .expect("a free port");
    let device = Device {
        key: "dev1".into(),
        name: "Studio".into(),
        address: fake.address,
        model: "Fake".into(),
        screen: false,
        group: false,
        fixed: true,
    };
    shared.add_room(&device, "sound-cast.studio");
    let (control_tx, control_rx) = channel();
    let (sinks_tx, sinks_rx) = pipewire::channel::channel();
    let (players_tx, players_rx) = channel();
    let room = Room::new(
        device,
        settings,
        control_rx,
        sinks_tx,
        players_tx,
        Arc::new(Default::default()),
        shared.clone(),
    );
    thread::spawn(move || room.run());
    Setup { fake, shared, control: control_tx, players: players_rx, _sinks: sinks_rx, _hub: hub }
}

#[test]
fn a_session_plays_measures_latency_follows_pauses_and_stops() {
    let s = setup(Settings { grace: Duration::from_secs(1), ..Settings::default() });
    s.control.send(Control::Active(true)).unwrap();
    eventually("playing", Duration::from_secs(10), || {
        device_status(&s.shared)["phase"] == "playing"
    });

    let status = device_status(&s.shared);
    assert_eq!(status["volume"], 40, "the device leads the volume at the start");
    let latency = status["latency_ms"].as_u64().expect("a latency");
    assert!((250..1500).contains(&latency), "latency {latency} ms");
    assert_eq!(status["listeners"], 1);
    {
        let log = s.fake.log.lock().unwrap();
        assert!(log.contains(&"fetched RIFF".to_string()), "{log:?}");
        assert!(log.contains(&"title \"Studio\"".to_string()), "{log:?}");
    }

    // Paused on the device: the player pauses, the session waits.
    s.fake.script.send(Script::Pause).unwrap();
    let command = s.players.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(command, mpris::Command::Pause { ref key } if key == "dev1"));
    s.fake.script.send(Script::Play).unwrap();
    let command = s.players.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(command, mpris::Command::Play { ref key } if key == "dev1"));
    // Resumed late by the pause: a fresh stream, whose old media ends INTERRUPTED
    // without ending the session.
    eventually("reload", Duration::from_secs(5), || loads(&s.fake) == 2);
    thread::sleep(Duration::from_millis(500));
    assert_eq!(device_status(&s.shared)["phase"], "playing");
    assert!(s.players.try_recv().is_err(), "the player was told something");

    // The audio stops: after the grace period the receiver is stopped.
    s.control.send(Control::Active(false)).unwrap();
    eventually("lingering", Duration::from_secs(5), || {
        device_status(&s.shared)["phase"] == "lingering"
    });
    eventually("stop", Duration::from_secs(5), || {
        s.fake.log.lock().unwrap().contains(&"stop \"s1\"".to_string())
    });
    eventually("ready", Duration::from_secs(5), || device_status(&s.shared)["phase"] == "ready");
}

fn loads(fake: &Fake) -> usize {
    fake.log.lock().unwrap().iter().filter(|line| *line == "LOAD").count()
}

#[test]
fn live_metadata_reloads_once_per_song() {
    let s = setup(Settings { live_metadata: true, ..Settings::default() });
    s.control.send(Control::Active(true)).unwrap();
    eventually("playing", Duration::from_secs(10), || {
        device_status(&s.shared)["phase"] == "playing"
    });
    let song = |title: &str, art: &str| mpris::Track {
        title: title.into(),
        art_url: art.into(),
        ..Default::default()
    };
    // The title first, the cover a moment later: one reload, after both.
    s.control.send(Control::Track(Some(song("One", "")))).unwrap();
    thread::sleep(Duration::from_millis(300));
    s.control.send(Control::Track(Some(song("One", "https://example.com/1.png")))).unwrap();
    eventually("reload", Duration::from_secs(5), || {
        s.fake.log.lock().unwrap().contains(&"title \"One\"".to_string())
    });
    assert_eq!(loads(&s.fake), 2);
    // A short gap with no track, then the same song: no reload.
    s.control.send(Control::Track(None)).unwrap();
    s.control.send(Control::Track(Some(song("One", "https://example.com/1.png")))).unwrap();
    thread::sleep(Duration::from_millis(2500));
    assert_eq!(loads(&s.fake), 2);
    assert_eq!(device_status(&s.shared)["phase"], "playing");
    assert!(s.players.try_recv().is_err(), "the player was told something");
}

#[test]
fn start_opens_a_session_without_audio_and_flac_streams() {
    let s = setup(Settings {
        grace: Duration::from_secs(1),
        codec: http::Codec::Flac,
        ..Settings::default()
    });
    s.control
        .send(Control::Track(Some(mpris::Track { title: "Song".into(), ..Default::default() })))
        .unwrap();
    s.control.send(Control::Start).unwrap();
    eventually("stop", Duration::from_secs(10), || {
        s.fake.log.lock().unwrap().contains(&"stop \"s1\"".to_string())
    });
    let log = s.fake.log.lock().unwrap();
    assert!(log.contains(&"fetched fLaC".to_string()), "{log:?}");
    assert!(log.contains(&"title \"Song\"".to_string()), "{log:?}");
}
