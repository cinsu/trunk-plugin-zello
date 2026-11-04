//! One Zello connection: one account logged on to one channel, kept up, its
//! transmissions streamed to the channel one at a time, and how it's going
//! reported as it changes.
//!
//! Each connection runs on its own thread, so the recorder's events are never
//! held up by the network. The protocol is Zello's Channel API: a WebSocket,
//! a `logon` command, then `on_channel_status` events.
//! <https://github.com/zelloptt/zello-channel-api/blob/master/API.md>

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use serde_json::{Value, json};
use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::HandshakeError;
use tungstenite::{Message, WebSocket};

use trunk_recorder_plugin::Outcome;

use crate::audio::{self, Encoder, Upsampler};
use crate::token::TokenMaker;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// How often a connection wakes: to send audio as it's due, answer pings and check for shutdown.
const TICK: Duration = Duration::from_millis(10);
const LOGON_TIMEOUT: Duration = Duration::from_secs(20);
const FIRST_RETRY: Duration = Duration::from_secs(2);
const MAX_RETRY: Duration = Duration::from_secs(60);
/// After Zello refuses a login, wait this long before trying again.
const REFUSED_RETRY: Duration = Duration::from_secs(300);

/// How a connection is doing.
#[derive(Clone, Debug, PartialEq)]
pub enum LinkState {
    Connecting,
    /// Logged in, waiting for the channel.
    LoggedOn,
    Online {
        users: u64,
    },
    /// Lost or unreachable; retrying soon. The text says why, for people.
    Down(String),
    /// Zello said no (wrong password, channel settings, …); retrying slowly.
    Refused(String),
}

/// What a connection tells the plugin.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    State(LinkState),
    /// A transmission is finished with: sent, or why not. `waited` is from
    /// the call's start to Zello taking the stream.
    Done {
        call: u32,
        outcome: Outcome,
        why: String,
        took: Duration,
        waited: Duration,
    },
    /// Transmissions waiting (the one being sent included), and whether one is being sent.
    Load {
        queued: usize,
        streaming: bool,
    },
    /// Something worth a line in the log.
    Note(String),
}

/// What the plugin tells a connection.
#[derive(Debug)]
pub enum Cmd {
    Start { call: u32 },
    Audio { call: u32, rate: u32, samples: Vec<i16> },
    End { call: u32 },
}

/// What a connection needs. The password and token maker stay in here:
/// nothing in this module logs them.
pub struct LinkConfig {
    pub url: String,
    pub channel: String,
    pub username: String,
    pub password: String,
    /// Consumer Zello signs a token for each logon; Zello Work doesn't.
    pub tokens: Option<Arc<TokenMaker>>,
    /// Zello Work: the network, for messages.
    pub network: Option<String>,
    /// Wait this long before the first connection, so several streams don't
    /// all log in at once (Zello limits that).
    pub start_delay: Duration,
}

pub type Report = Arc<dyn Fn(Event) + Send + Sync>;

/// A running connection.
pub struct Link {
    stop: Arc<AtomicBool>,
    tx: Sender<Cmd>,
    thread: Option<JoinHandle<()>>,
}

impl Link {
    pub fn start(cfg: LinkConfig, report: Report) -> Link {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let (tx, rx) = mpsc::channel();
        let thread = thread::spawn(move || run(cfg, Pipeline::new(rx), &flag, &report));
        Link { stop, tx, thread: Some(thread) }
    }

    pub fn send(&self, cmd: Cmd) {
        let _ = self.tx.send(cmd);
    }

    /// Asks it to log off and stop; see [`Link::finished`].
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    pub fn finished(&self) -> bool {
        self.thread.as_ref().is_none_or(|t| t.is_finished())
    }
}

enum Failure {
    Down(String),
    Refused(String),
}

// ---- transmissions waiting to go ----

/// A transmission waits this long at most for its turn (behind others on a
/// busy channel, or for Zello to come back); then it's dropped, not sent late.
const MAX_WAIT: Duration = Duration::from_secs(10);
/// Once a stream has fallen this far behind the radio, the oldest audio is dropped.
const MAX_BEHIND: usize = 3 * audio::RATE as usize;
/// A call that has had no audio for this long is taken to have ended (its end was lost).
const SILENT_END: Duration = Duration::from_secs(3);

struct Tx {
    call: u32,
    created: Instant,
    last_audio: Instant,
    ended: bool,
    rate: u32,
    up: Option<Upsampler>,
    /// 48 kHz audio waiting to be sent.
    pcm: VecDeque<i16>,
}

impl Tx {
    fn new(call: u32) -> Tx {
        let now = Instant::now();
        Tx { call, created: now, last_audio: now, ended: false, rate: 0, up: None, pcm: VecDeque::new() }
    }

    fn end(&mut self) {
        if !self.ended {
            self.ended = true;
            if let Some(up) = &mut self.up {
                up.flush(&mut self.pcm);
            }
        }
    }
}

struct Pipeline {
    rx: Receiver<Cmd>,
    queue: VecDeque<Tx>,
    load: (usize, bool),
    dropped: usize,
    noted_rate: bool,
}

impl Pipeline {
    fn new(rx: Receiver<Cmd>) -> Pipeline {
        Pipeline { rx, queue: VecDeque::new(), load: (0, false), dropped: 0, noted_rate: false }
    }

    /// Takes what the plugin sent.
    fn take(&mut self, report: &Report) {
        while let Ok(cmd) = self.rx.try_recv() {
            match cmd {
                Cmd::Start { call } => {
                    if !self.queue.iter().any(|t| t.call == call) {
                        self.queue.push_back(Tx::new(call));
                    }
                }
                Cmd::Audio { call, rate, samples } => {
                    if !self.queue.iter().any(|t| t.call == call) {
                        self.queue.push_back(Tx::new(call)); // its start was missed
                    }
                    let tx = self.queue.iter_mut().find(|t| t.call == call).expect("just added");
                    if tx.ended {
                        continue;
                    }
                    if tx.rate != rate {
                        tx.rate = rate;
                        tx.up = Upsampler::new(rate);
                        if tx.up.is_none() && !self.noted_rate {
                            self.noted_rate = true;
                            report(Event::Note(format!("audio at {rate} Hz can't be sent: it has to divide into 48 kHz")));
                        }
                    }
                    if let Some(up) = &mut tx.up {
                        up.push(&samples, &mut tx.pcm);
                        tx.last_audio = Instant::now();
                    }
                }
                Cmd::End { call } => {
                    if let Some(tx) = self.queue.iter_mut().find(|t| t.call == call) {
                        tx.end();
                    }
                }
            }
        }
    }

    /// Drops transmissions that waited too long for their turn, all but the
    /// first (`front_busy`: it's being sent), and ends silent ones.
    fn expire(&mut self, front_busy: bool, why: &str, report: &Report) {
        let skip = usize::from(front_busy);
        let mut i = skip;
        while i < self.queue.len() {
            if self.queue[i].created.elapsed() > MAX_WAIT {
                let tx = self.queue.remove(i).expect("in range");
                let waited = tx.created.elapsed();
                report(Event::Done { call: tx.call, outcome: Outcome::Failed, why: why.into(), took: Duration::ZERO, waited });
            } else {
                i += 1;
            }
        }
        for tx in self.queue.iter_mut() {
            if !tx.ended && tx.last_audio.elapsed() > SILENT_END {
                tx.end();
            }
        }
    }

    fn report_load(&mut self, streaming: bool, report: &Report) {
        let load = (self.queue.len(), streaming);
        if load != self.load {
            self.load = load;
            report(Event::Load { queued: load.0, streaming });
        }
    }
}

fn run(cfg: LinkConfig, mut pipeline: Pipeline, stop: &AtomicBool, report: &Report) {
    let mut encoder = match Encoder::new() {
        Ok(e) => e,
        Err(e) => {
            report(Event::State(LinkState::Refused(e)));
            return;
        }
    };
    if !pause(stop, cfg.start_delay, &mut pipeline, report) {
        return;
    }
    let mut backoff = FIRST_RETRY;
    loop {
        let mut online_since = None;
        let failure = match session(&cfg, stop, report, &mut online_since, &mut pipeline, &mut encoder) {
            Ok(()) => return,
            Err(f) => f,
        };
        if online_since.is_some_and(|t: Instant| t.elapsed() > Duration::from_secs(60)) {
            backoff = FIRST_RETRY; // it had been working: try again quickly
        }
        let wait = match failure {
            Failure::Down(why) => {
                let wait = backoff;
                backoff = (backoff * 2).min(MAX_RETRY);
                report(Event::State(LinkState::Down(format!("{why}; trying again in {} s", wait.as_secs()))));
                wait
            }
            Failure::Refused(why) => {
                report(Event::State(LinkState::Refused(format!("{why} (trying again in {} min)", REFUSED_RETRY.as_secs() / 60))));
                REFUSED_RETRY
            }
        };
        if !pause(stop, wait, &mut pipeline, report) {
            return;
        }
    }
}

/// Waits, taking (and timing out) transmissions meanwhile. False if asked to stop.
fn pause(stop: &AtomicBool, d: Duration, pipeline: &mut Pipeline, report: &Report) -> bool {
    let end = Instant::now() + d;
    while Instant::now() < end {
        if stop.load(Ordering::SeqCst) {
            return false;
        }
        pipeline.take(report);
        pipeline.expire(false, "Zello wasn't connected", report);
        pipeline.report_load(false, report);
        thread::sleep(Duration::from_millis(50).min(end - Instant::now()));
    }
    !stop.load(Ordering::SeqCst)
}

// ---- one connection ----

/// Where sending the first waiting transmission is.
enum Sending {
    Idle,
    /// Asked Zello for a stream (`start_stream`), or waiting to ask again
    /// after "channel busy".
    Starting {
        seq: u64,
        asked: Instant,
        busy: u32,
        again_at: Option<Instant>,
    },
    /// Sending: `frames` sent since `clock` (moved on when audio runs short,
    /// so a late burst isn't sent faster than real time).
    Streaming {
        id: u64,
        clock: Instant,
        frames: u64,
        began: Instant,
        waited: Duration,
    },
}

const BUSY_RETRIES: u32 = 8;
const BUSY_WAIT: Duration = Duration::from_millis(750);
const START_TIMEOUT: Duration = Duration::from_secs(5);

/// One connection, from connecting to its end. `Ok` when asked to stop.
fn session(
    cfg: &LinkConfig,
    stop: &AtomicBool,
    report: &Report,
    online_since: &mut Option<Instant>,
    pipeline: &mut Pipeline,
    encoder: &mut Encoder,
) -> Result<(), Failure> {
    report(Event::State(LinkState::Connecting));
    let mut ws = open(cfg)?;
    let mut sending = Sending::Idle;
    let result = converse(cfg, stop, report, online_since, pipeline, encoder, &mut ws, &mut sending);
    if !matches!(sending, Sending::Idle) {
        // Cut off mid-transmission: that one is lost.
        if let Some(tx) = pipeline.queue.pop_front() {
            let why = match &result {
                Ok(()) => "the plugin stopped while sending it",
                Err(_) => "the connection to Zello was lost while sending it",
            };
            report(Event::Done { call: tx.call, outcome: Outcome::Failed, why: why.into(), took: Duration::ZERO, waited: tx.created.elapsed() });
        }
    }
    pipeline.report_load(false, report);
    result
}

#[allow(clippy::too_many_arguments)]
fn converse(
    cfg: &LinkConfig,
    stop: &AtomicBool,
    report: &Report,
    online_since: &mut Option<Instant>,
    pipeline: &mut Pipeline,
    encoder: &mut Encoder,
    ws: &mut WebSocket<Conn>,
    sending: &mut Sending,
) -> Result<(), Failure> {
    let mut logon = json!({
        "command": "logon",
        "seq": 1,
        "username": cfg.username,
        "password": cfg.password,
        "channels": [cfg.channel],
        "version": concat!("tr-pro-zello ", env!("CARGO_PKG_VERSION")),
        "platform_type": "trunk-recorder-pro",
        // Zello Alarms watches clients whose platform name says "Gateway".
        "platform_name": "Trunk Recorder Pro Gateway",
    });
    if let Some(tokens) = &cfg.tokens {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        logon["auth_token"] = tokens.token(now).map_err(Failure::Refused)?.into();
    }
    ws.send(Message::text(logon.to_string())).map_err(|e| Failure::Down(format!("connection lost while logging in ({e})")))?;
    let asked = Instant::now();
    let mut logged_on = false;
    let mut online = false;
    let mut seq: u64 = 1;
    loop {
        if stop.load(Ordering::SeqCst) {
            let _ = ws.close(None);
            let _ = ws.flush();
            return Ok(());
        }
        pipeline.take(report);
        match ws.read() {
            Ok(Message::Text(text)) => {
                let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                if v["seq"] == 1 {
                    if v["success"] == true {
                        logged_on = true;
                        report(Event::State(LinkState::LoggedOn));
                    } else {
                        return Err(refusal(cfg, v["error"].as_str().unwrap_or("no reason given")));
                    }
                } else if v["command"] == "on_channel_status" && same_channel(&v, &cfg.channel) {
                    if v["status"] == "online" {
                        online = true;
                        online_since.get_or_insert_with(Instant::now);
                        report(Event::State(LinkState::Online { users: v["users_online"].as_u64().unwrap_or(0) }));
                    } else if let Some(err) = v["error"].as_str() {
                        return Err(channel_refusal(cfg, err, v["error_type"] == "configuration"));
                    } else {
                        return Err(Failure::Down(format!("Zello took channel '{}' offline", cfg.channel)));
                    }
                } else if v["command"] == "on_error" {
                    report(Event::Note(format!("Zello says: {}", v["error"].as_str().unwrap_or("an error"))));
                } else if let Sending::Starting { seq: want, busy, again_at, .. } = sending
                    && v["seq"].as_u64() == Some(*want)
                {
                    if let Some(id) = v["stream_id"].as_u64().filter(|_| v["success"] == true) {
                        let waited = pipeline.queue.front().map_or(Duration::ZERO, |t| t.created.elapsed());
                        let now = Instant::now();
                        *sending = Sending::Streaming { id, clock: now, frames: 0, began: now, waited };
                    } else {
                        let err = v["error"].as_str().unwrap_or("no reason given").to_string();
                        if err == "channel busy" && *busy < BUSY_RETRIES {
                            *busy += 1;
                            *again_at = Some(Instant::now() + BUSY_WAIT);
                        } else {
                            let why =
                                if err == "channel busy" { "the Zello channel stayed busy".to_string() } else { format!("Zello wouldn't take it ({err})") };
                            fail_front(pipeline, &why, report);
                            *sending = Sending::Idle;
                        }
                    }
                }
                // Other events (others talking, texts, …) aren't ours to act on.
            }
            Ok(Message::Close(_)) => return Err(Failure::Down("Zello closed the connection".into())),
            Ok(_) => {} // pings are answered by tungstenite; voice from others is ignored
            Err(tungstenite::Error::Io(e)) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                let _ = ws.flush(); // sends any pong that's waiting
            }
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                return Err(Failure::Down("the connection to Zello closed".into()));
            }
            Err(e) => return Err(Failure::Down(format!("lost the connection to Zello ({e})"))),
        }
        if !logged_on && asked.elapsed() > LOGON_TIMEOUT {
            return Err(Failure::Down("Zello didn't answer the login".into()));
        }
        let streaming = !matches!(sending, Sending::Idle);
        if online {
            pipeline.expire(streaming, "it waited too long for the Zello channel", report);
            pump(cfg, ws, sending, &mut seq, pipeline, encoder, report)?;
        } else {
            pipeline.expire(false, "Zello wasn't connected", report);
        }
        pipeline.report_load(!matches!(sending, Sending::Idle), report);
    }
}

fn fail_front(pipeline: &mut Pipeline, why: &str, report: &Report) {
    if let Some(tx) = pipeline.queue.pop_front() {
        report(Event::Done { call: tx.call, outcome: Outcome::Failed, why: why.into(), took: Duration::ZERO, waited: tx.created.elapsed() });
    }
}

fn lost(e: tungstenite::Error) -> Failure {
    Failure::Down(format!("lost the connection to Zello ({e})"))
}

/// Moves the first waiting transmission along: asks for a stream, sends what
/// audio is due (one 20 ms packet per 20 ms, as Zello wants it), and ends it.
fn pump(
    cfg: &LinkConfig,
    ws: &mut WebSocket<Conn>,
    sending: &mut Sending,
    seq: &mut u64,
    pipeline: &mut Pipeline,
    encoder: &mut Encoder,
    report: &Report,
) -> Result<(), Failure> {
    let start_stream = |ws: &mut WebSocket<Conn>, seq: u64| {
        let m = json!({ "command": "start_stream", "seq": seq, "type": "audio", "codec": "opus",
                        "codec_header": audio::CODEC_HEADER, "packet_duration": audio::FRAME_MS, "channel": cfg.channel });
        ws.send(Message::text(m.to_string())).map_err(lost)
    };
    match sending {
        Sending::Idle => {
            let Some(tx) = pipeline.queue.front() else { return Ok(()) };
            if tx.pcm.is_empty() {
                if tx.ended {
                    let tx = pipeline.queue.pop_front().expect("front");
                    let waited = tx.created.elapsed();
                    report(Event::Done { call: tx.call, outcome: Outcome::Skipped, why: "no audio".into(), took: Duration::ZERO, waited });
                }
                return Ok(()); // waiting for its audio
            }
            *seq += 1;
            encoder.reset();
            start_stream(ws, *seq)?;
            *sending = Sending::Starting { seq: *seq, asked: Instant::now(), busy: 0, again_at: None };
        }
        Sending::Starting { seq: want, asked, again_at, .. } => {
            if again_at.is_some_and(|t| Instant::now() >= t) {
                *seq += 1;
                *want = *seq;
                *asked = Instant::now();
                *again_at = None;
                start_stream(ws, *seq)?;
            } else if again_at.is_none() && asked.elapsed() > START_TIMEOUT {
                fail_front(pipeline, "Zello didn't answer the request to talk", report);
                *sending = Sending::Idle;
            }
        }
        Sending::Streaming { id, clock, frames, began, waited } => {
            let Some(tx) = pipeline.queue.front_mut() else {
                *sending = Sending::Idle;
                return Ok(());
            };
            if tx.pcm.len() > MAX_BEHIND {
                let n = tx.pcm.len() - MAX_BEHIND;
                tx.pcm.drain(..n);
                pipeline.dropped += n;
                if pipeline.dropped.is_multiple_of(audio::RATE as usize) || pipeline.dropped == n {
                    report(Event::Note(format!(
                        "fell over 3 s behind the radio: dropped {:.1} s of audio so far",
                        pipeline.dropped as f64 / f64::from(audio::RATE)
                    )));
                }
            }
            let due = clock.elapsed().as_millis() as u64 / audio::FRAME_MS + 1;
            while *frames < due {
                let frame: Vec<i16> = if tx.pcm.len() >= audio::FRAME {
                    tx.pcm.drain(..audio::FRAME).collect()
                } else if tx.ended && !tx.pcm.is_empty() {
                    let mut f: Vec<i16> = tx.pcm.drain(..).collect();
                    f.resize(audio::FRAME, 0);
                    f
                } else {
                    // Out of audio for now: hold the clock, so what comes next isn't sent too fast.
                    *clock = Instant::now() - Duration::from_millis(*frames * audio::FRAME_MS);
                    break;
                };
                let opus = encoder.encode(&frame).map_err(Failure::Down)?;
                // {type 1 = audio, stream id, packet id (0)}, big-endian, then the Opus packet.
                let mut packet = Vec::with_capacity(9 + opus.len());
                packet.push(1);
                packet.extend_from_slice(&(*id as u32).to_be_bytes());
                packet.extend_from_slice(&0u32.to_be_bytes());
                packet.extend_from_slice(&opus);
                ws.send(Message::binary(packet)).map_err(lost)?;
                *frames += 1;
            }
            if tx.ended && tx.pcm.is_empty() {
                ws.send(Message::text(json!({ "command": "stop_stream", "stream_id": *id }).to_string())).map_err(lost)?;
                let call = tx.call;
                pipeline.queue.pop_front();
                report(Event::Done { call, outcome: Outcome::Ok, why: String::new(), took: began.elapsed(), waited: *waited });
                *sending = Sending::Idle;
            }
        }
    }
    Ok(())
}

fn same_channel(v: &Value, channel: &str) -> bool {
    v["channel"].as_str().is_none_or(|c| c.eq_ignore_ascii_case(channel))
}

fn refusal(cfg: &LinkConfig, code: &str) -> Failure {
    let who = &cfg.username;
    match code {
        "not authorized" => Failure::Refused(match &cfg.network {
            Some(net) => format!("Zello refused the login for {who}: check the username and password, and that {who} is on network {net}"),
            None => format!("Zello refused the login for {who}: check the username and password, and the Issuer and Private Key"),
        }),
        "internal server error" => Failure::Down("Zello had a server error".into()),
        "not enough params" => Failure::Refused(format!("Zello says the login for {who} is missing something (not enough params)")),
        other => Failure::Refused(format!("Zello refused the login for {who} ({other})")),
    }
}

fn channel_refusal(cfg: &LinkConfig, err: &str, configuration: bool) -> Failure {
    let mut why = format!("Zello won't connect {} to channel '{}': {err}", cfg.username, cfg.channel);
    if err.to_lowercase().contains("password") {
        why.push_str(". The Channel API can't join a channel that has a password: remove the channel's password in Zello");
    } else if configuration {
        why.push_str(". The channel's settings don't let this account connect");
    }
    Failure::Refused(why)
}

/// A TCP connection, encrypted or not (tests use plain `ws://`).
enum Conn {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Conn {
    fn tcp(&self) -> &TcpStream {
        match self {
            Conn::Plain(s) => s,
            Conn::Tls(s) => s.get_ref(),
        }
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Conn::Plain(s) => s.read(buf),
            Conn::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Conn::Plain(s) => s.write(buf),
            Conn::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Plain(s) => s.flush(),
            Conn::Tls(s) => s.flush(),
        }
    }
}

fn tls_config() -> Arc<ClientConfig> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let roots = RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let config = ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .expect("rustls supports its default TLS versions")
                .with_root_certificates(roots)
                .with_no_client_auth();
            Arc::new(config)
        })
        .clone()
}

fn open(cfg: &LinkConfig) -> Result<WebSocket<Conn>, Failure> {
    let request = cfg.url.as_str().into_client_request().map_err(|e| Failure::Refused(format!("Zello's address {} isn't usable ({e})", cfg.url)))?;
    let uri = request.uri().clone();
    let tls = match uri.scheme_str() {
        Some("wss") => true,
        Some("ws") => false,
        _ => return Err(Failure::Refused(format!("Zello's address {} has to start with wss://", cfg.url))),
    };
    let host = uri.host().unwrap_or_default().to_string();
    let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
    let addr = (host.as_str(), port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .ok_or_else(|| Failure::Down(format!("can't look up {host}: is the internet connection up?")))?;
    let sock = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT).map_err(|e| Failure::Down(format!("can't reach {host} ({e})")))?;
    let _ = sock.set_nodelay(true);
    let _ = sock.set_read_timeout(Some(HANDSHAKE_TIMEOUT));
    let _ = sock.set_write_timeout(Some(HANDSHAKE_TIMEOUT));
    let conn = if tls {
        let name = ServerName::try_from(host.clone()).map_err(|_| Failure::Refused(format!("{host} isn't a valid server name")))?;
        let tls = ClientConnection::new(tls_config(), name).map_err(|e| Failure::Down(format!("couldn't start TLS ({e})")))?;
        Conn::Tls(Box::new(StreamOwned::new(tls, sock)))
    } else {
        Conn::Plain(sock)
    };
    let (ws, _) = tungstenite::client(request, conn).map_err(|e| handshake_failure(cfg, &host, e))?;
    let _ = ws.get_ref().tcp().set_read_timeout(Some(TICK));
    Ok(ws)
}

fn handshake_failure(cfg: &LinkConfig, host: &str, e: HandshakeError<tungstenite::ClientHandshake<Conn>>) -> Failure {
    let e = match e {
        HandshakeError::Interrupted(_) => return Failure::Down(format!("{host} didn't answer in time")),
        HandshakeError::Failure(e) => e,
    };
    match e {
        tungstenite::Error::Http(resp) => match resp.status().as_u16() {
            429 => Failure::Down("Zello is limiting logins (HTTP 429)".into()),
            404 if cfg.network.is_some() => {
                Failure::Refused(format!("Zello Work has no network '{}' (HTTP 404): check the network name", cfg.network.as_deref().unwrap_or_default()))
            }
            code => Failure::Down(format!("Zello answered HTTP {code}")),
        },
        tungstenite::Error::Io(e) => Failure::Down(format!("couldn't connect to {host} ({e})")),
        other => Failure::Down(format!("couldn't connect to {host} ({other})")),
    }
}
