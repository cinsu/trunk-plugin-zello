//! One Zello connection: one account logged on to one channel, kept up, its
//! transmissions streamed to the channel one at a time, and how it's going
//! reported as it changes.
//!
//! Each connection runs on its own thread, so the recorder's events are never
//! held up by the network. The protocol is Zello's Channel API: a WebSocket,
//! a `logon` command, then `on_channel_status` events.
//! <https://github.com/zelloptt/zello-channel-api/blob/master/API.md>

use std::collections::{HashMap, VecDeque};
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
/// While nothing is coming from Zello, ping it this often. That keeps an idle
/// connection from being dropped silently along the way, and shows when it was.
const PING_EVERY: Duration = if cfg!(test) { Duration::from_millis(200) } else { Duration::from_secs(20) };
/// Nothing at all from Zello for this long, once it has answered a ping on this
/// connection: the connection is dead, so reconnect before anything needs sending.
const DEAD_AFTER: Duration = if cfg!(test) { Duration::from_millis(700) } else { Duration::from_secs(50) };

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
    /// A text (a page alert) went to a channel, or didn't.
    Posted {
        channel: String,
        ok: bool,
        why: String,
    },
    /// Something worth a line in the log, that's going as it should.
    Info(String),
    /// Something worth a line in the log.
    Note(String),
}

/// What the plugin tells a connection.
#[derive(Debug)]
pub enum Cmd {
    Start {
        call: u32,
        talkgroup: u32,
    },
    Audio {
        call: u32,
        rate: u32,
        samples: Vec<i16>,
    },
    End {
        call: u32,
    },
    /// A text for a channel: `None` is the connection's own channel. `ack`,
    /// when given, hears how it went: the channel, or why it wasn't posted.
    Text {
        channel: Option<String>,
        text: String,
        ack: Option<Ack>,
    },
}

/// What a connection needs. The password and token maker stay in here:
/// nothing in this module logs them.
pub struct LinkConfig {
    pub url: String,
    pub channel: String,
    /// More channels to join, for texts only (page alerts). Only Zello Work
    /// lets one connection join several.
    pub also: Vec<String>,
    pub username: String,
    pub password: String,
    /// Consumer Zello signs a token for each logon; Zello Work doesn't.
    pub tokens: Option<Arc<TokenMaker>>,
    /// Zello Work: the network, for messages.
    pub network: Option<String>,
    /// Wait this long before the first connection, so several streams don't
    /// all log in at once (Zello limits that).
    pub start_delay: Duration,
    /// A transmission waits this long at most for its turn (behind others on
    /// the channel, or for Zello to come back); then it's dropped, not sent late.
    pub max_wait: Duration,
}

pub type Report = Arc<dyn Fn(Event) + Send + Sync>;
pub type Ack = Sender<Result<String, String>>;

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
        let thread = thread::spawn(move || {
            let pipeline = Pipeline::new(rx, cfg.max_wait);
            run(cfg, pipeline, &flag, &report)
        });
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

/// A text waits this long at most for Zello to be connected, and then for its answer.
const TEXT_WAIT: Duration = Duration::from_secs(10);
/// Once a stream has fallen this far behind the radio, the oldest audio is dropped.
const MAX_BEHIND: usize = 3 * audio::RATE as usize;
/// A call that has had no audio for this long is taken to have ended (its end was lost).
const SILENT_END: Duration = Duration::from_secs(3);

struct Tx {
    call: u32,
    talkgroup: u32,
    created: Instant,
    last_audio: Instant,
    ended: bool,
    rate: u32,
    up: Option<Upsampler>,
    /// 48 kHz audio waiting to be sent.
    pcm: VecDeque<i16>,
    /// What this attempt has sent so far, kept so it can be sent again from its
    /// start if the connection drops partway through.
    sent: Vec<i16>,
    /// It was cut off once and is being sent again (only once).
    retried: bool,
    /// When it lost its connection; it may wait Max wait from then for the next.
    retry_since: Option<Instant>,
}

impl Tx {
    fn new(call: u32, talkgroup: u32) -> Tx {
        let now = Instant::now();
        Tx {
            call,
            talkgroup,
            created: now,
            last_audio: now,
            ended: false,
            rate: 0,
            up: None,
            pcm: VecDeque::new(),
            sent: Vec::new(),
            retried: false,
            retry_since: None,
        }
    }

    /// When it started waiting for its turn: its start, or when it was cut off.
    fn waiting_since(&self) -> Instant {
        self.retry_since.unwrap_or(self.created)
    }

    /// Cut off by a lost connection: put back what was sent, to send it all again.
    fn restart(&mut self) {
        let mut all: VecDeque<i16> = self.sent.drain(..).collect();
        all.append(&mut self.pcm);
        self.pcm = all;
        self.retried = true;
        self.retry_since = Some(Instant::now());
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

/// A text waiting to go, or waiting for Zello's answer.
struct Text {
    at: Instant,
    /// `None`: the connection's own channel.
    channel: Option<String>,
    text: String,
    ack: Option<Ack>,
}

impl Text {
    /// It went (`Ok`), or didn't (why): reports it, and answers whoever asked.
    fn done(self, result: Result<String, String>, own: Option<&str>, report: &Report) {
        let channel = self.channel.clone().or_else(|| own.map(str::to_string)).unwrap_or_default();
        match &result {
            Ok(_) => report(Event::Posted { channel: channel.clone(), ok: true, why: String::new() }),
            Err(why) => report(Event::Posted { channel: channel.clone(), ok: false, why: why.clone() }),
        }
        if let Some(ack) = self.ack {
            let _ = ack.send(result.map(|_| channel));
        }
    }
}

/// "talkgroup 101", or "a call" when it isn't known.
fn talkgroup_name(tg: u32) -> String {
    if tg == 0 { "a call".to_string() } else { format!("talkgroup {tg}") }
}

struct Pipeline {
    rx: Receiver<Cmd>,
    queue: VecDeque<Tx>,
    /// Texts waiting to go.
    texts: VecDeque<Text>,
    max_wait: Duration,
    load: (usize, bool),

    noted_rate: bool,
}

impl Pipeline {
    fn new(rx: Receiver<Cmd>, max_wait: Duration) -> Pipeline {
        Pipeline { rx, queue: VecDeque::new(), texts: VecDeque::new(), max_wait, load: (0, false), noted_rate: false }
    }

    /// Takes what the plugin sent.
    fn take(&mut self, report: &Report) {
        while let Ok(cmd) = self.rx.try_recv() {
            match cmd {
                Cmd::Start { call, talkgroup } => {
                    if !self.queue.iter().any(|t| t.call == call) {
                        self.queue.push_back(Tx::new(call, talkgroup));
                    }
                }
                Cmd::Audio { call, rate, samples } => {
                    if !self.queue.iter().any(|t| t.call == call) {
                        self.queue.push_back(Tx::new(call, 0)); // its start was missed
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
                Cmd::Text { channel, text, ack } => self.texts.push_back(Text { at: Instant::now(), channel, text, ack }),
            }
        }
    }

    /// Drops transmissions that waited too long for their turn, all but the
    /// first when it's being sent (`front_busy`), and ends silent ones.
    /// `connected`: Zello is, so they waited behind another transmission.
    fn expire(&mut self, front_busy: bool, connected: bool, report: &Report) {
        let skip = usize::from(front_busy);
        let mut i = skip;
        while i < self.queue.len() {
            if self.queue[i].waiting_since().elapsed() > self.max_wait {
                let tx = self.queue.remove(i).expect("in range");
                let waited = tx.created.elapsed();
                let behind = match self.queue.front() {
                    Some(f) if front_busy && connected => {
                        format!("behind {} (on the channel for {} s)", talkgroup_name(f.talkgroup), f.created.elapsed().as_secs())
                    }
                    _ if connected => "for the Zello channel".to_string(),
                    _ => "for Zello to connect".to_string(),
                };
                let why = format!("{} waited over {} s {behind}", talkgroup_name(tx.talkgroup), self.max_wait.as_secs());
                report(Event::Done { call: tx.call, outcome: Outcome::Failed, why, took: Duration::ZERO, waited });
            } else {
                i += 1;
            }
        }
        for tx in self.queue.iter_mut() {
            if !tx.ended && tx.last_audio.elapsed() > SILENT_END {
                tx.end();
            }
        }
        while self.texts.front().is_some_and(|t| t.at.elapsed() > TEXT_WAIT) {
            let t = self.texts.pop_front().expect("checked");
            t.done(Err("Zello wasn't connected".into()), None, report);
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
        pipeline.expire(false, false, report);
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
    /// so a late burst isn't sent faster than real time). `backlog`: the audio
    /// that piled up while it waited its turn, which is all sent, from its start.
    /// `trimmed`: audio dropped because it fell behind while sending.
    Streaming {
        id: u64,
        clock: Instant,
        frames: u64,
        began: Instant,
        waited: Duration,
        backlog: usize,
        trimmed: usize,
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
    let mut texts_sent = HashMap::new();
    let mut last_heard = Instant::now();
    let result = converse(cfg, stop, report, online_since, pipeline, encoder, &mut ws, &mut sending, &mut texts_sent, &mut last_heard);
    // Say how long Zello had been quiet: a long quiet before a drop points at an idle connection cut along the way.
    let result = result.map_err(|f| match f {
        Failure::Down(why) => Failure::Down(format!("{why}; last heard from Zello {} s before", last_heard.elapsed().as_secs())),
        other => other,
    });
    for (_, t) in texts_sent.drain() {
        t.done(Err("the connection to Zello was lost before it answered".into()), Some(&cfg.channel), report);
    }
    if !matches!(sending, Sending::Idle)
        && let Some(tx) = pipeline.queue.front_mut()
    {
        if result.is_err() && !tx.retried {
            // Cut off by the connection: send it again, from its start, once reconnected.
            tx.restart();
            report(Event::Info(format!(
                "the connection to Zello was lost while sending {}; it will be sent again from its start once reconnected",
                talkgroup_name(tx.talkgroup)
            )));
        } else {
            let why = match &result {
                Ok(()) => "the plugin stopped while sending it",
                Err(_) => "the connection to Zello was lost while sending it, twice",
            };
            let tx = pipeline.queue.pop_front().expect("front");
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
    texts_sent: &mut HashMap<u64, Text>,
    last_heard: &mut Instant,
) -> Result<(), Failure> {
    let mut logon = json!({
        "command": "logon",
        "seq": 1,
        "username": cfg.username,
        "password": cfg.password,
        "channels": channels(cfg),
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
    // Channels online, with their users; texts sent, by seq, to their channel.
    let mut joined: HashMap<String, u64> = HashMap::new();
    let mut noted_no_texts = false;
    // Keepalive: pings sent while quiet; whether Zello has answered one on this connection.
    let mut last_ping = Instant::now();
    let mut pings = 0u32;
    let mut pong_seen = false;
    let mut noted_no_pong = false;
    loop {
        if stop.load(Ordering::SeqCst) {
            let _ = ws.close(None);
            let _ = ws.flush();
            return Ok(());
        }
        pipeline.take(report);
        let read = ws.read();
        if read.is_ok() {
            *last_heard = Instant::now();
        }
        match read {
            Ok(Message::Pong(_)) => pong_seen = true,
            Ok(Message::Text(text)) => {
                let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                if v["seq"] == 1 {
                    if v["success"] == true {
                        logged_on = true;
                        report(Event::State(LinkState::LoggedOn));
                    } else {
                        return Err(refusal(cfg, v["error"].as_str().unwrap_or("no reason given")));
                    }
                } else if v["command"] == "on_channel_status"
                    && let Some(channel) = our_channel(&v, cfg)
                {
                    if v["status"] == "online" {
                        if v["texting_supported"] == false && !noted_no_texts {
                            noted_no_texts = true;
                            report(Event::Note(format!("channel '{channel}' doesn't accept texts, so page alerts can't be posted there")));
                        }
                        joined.insert(channel, v["users_online"].as_u64().unwrap_or(0));
                        if joined.len() == channels(cfg).len() {
                            online = true;
                            online_since.get_or_insert_with(Instant::now);
                            report(Event::State(LinkState::Online { users: joined.values().sum() }));
                        }
                    } else if let Some(err) = v["error"].as_str() {
                        return Err(channel_refusal(cfg, &channel, err, v["error_type"] == "configuration"));
                    } else {
                        return Err(Failure::Down(format!("Zello took channel '{channel}' offline")));
                    }
                } else if let Some(t) = v["seq"].as_u64().and_then(|n| texts_sent.remove(&n)) {
                    let result = if v["success"] == true {
                        Ok(String::new())
                    } else {
                        Err(format!("Zello wouldn't post it ({})", v["error"].as_str().unwrap_or("no reason given")))
                    };
                    t.done(result, Some(&cfg.channel), report);
                } else if v["command"] == "on_error" {
                    report(Event::Note(format!("Zello says: {}", v["error"].as_str().unwrap_or("an error"))));
                } else if let Sending::Starting { seq: want, busy, again_at, .. } = sending
                    && v["seq"].as_u64() == Some(*want)
                {
                    if let Some(id) = v["stream_id"].as_u64().filter(|_| v["success"] == true) {
                        let waited = pipeline.queue.front().map_or(Duration::ZERO, |t| t.created.elapsed());
                        let backlog = pipeline.queue.front().map_or(0, |t| t.pcm.len());
                        let now = Instant::now();
                        *sending = Sending::Streaming { id, clock: now, frames: 0, began: now, waited, backlog, trimmed: 0 };
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
                } else if let Some(err) = v["error"].as_str() {
                    // An error this plugin didn't expect: worth seeing.
                    report(Event::Note(format!("Zello says: {err} ({})", short(&text))));
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
        if logged_on && last_heard.elapsed() >= PING_EVERY && last_ping.elapsed() >= PING_EVERY {
            ws.send(Message::Ping(Default::default())).map_err(lost)?;
            last_ping = Instant::now();
            pings += 1;
        }
        if pong_seen && last_heard.elapsed() > DEAD_AFTER {
            return Err(Failure::Down(format!("Zello stopped answering (nothing for {} s, pings unanswered)", last_heard.elapsed().as_secs())));
        }
        if !pong_seen && !noted_no_pong && pings >= 3 {
            noted_no_pong = true;
            report(Event::Note("Zello isn't answering keepalive pings, so a dropped connection is only noticed when something is sent".into()));
        }
        let streaming = !matches!(sending, Sending::Idle);
        if online {
            while let Some(mut t) = pipeline.texts.pop_front() {
                let channel = t.channel.clone().unwrap_or_else(|| cfg.channel.clone());
                seq += 1;
                let m = json!({ "command": "send_text_message", "seq": seq, "channel": channel, "text": t.text });
                ws.send(Message::text(m.to_string())).map_err(lost)?;
                report(Event::Info(format!("posting \"{}\" to '{channel}'", t.text)));
                t.at = Instant::now(); // now waiting for Zello's answer
                texts_sent.insert(seq, t);
            }
            let late: Vec<u64> = texts_sent.iter().filter(|(_, t)| t.at.elapsed() > TEXT_WAIT).map(|(n, _)| *n).collect();
            for n in late {
                let t = texts_sent.remove(&n).expect("listed");
                t.done(Err(format!("Zello didn't answer within {} s", TEXT_WAIT.as_secs())), Some(&cfg.channel), report);
            }
            pipeline.expire(streaming, true, report);
            pump(cfg, ws, sending, &mut seq, pipeline, encoder, report)?;
        } else {
            pipeline.expire(false, false, report);
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
        Sending::Streaming { id, clock, frames, began, waited, backlog, trimmed } => {
            let Some(tx) = pipeline.queue.front_mut() else {
                *sending = Sending::Idle;
                return Ok(());
            };
            // What waited for its turn is all sent, from the start (who's calling is at
            // the start). Only audio falling behind on top of that, while sending, is dropped.
            let allowed = backlog.saturating_sub(*frames as usize * audio::FRAME) + MAX_BEHIND;
            if tx.pcm.len() > allowed {
                let n = tx.pcm.len() - allowed;
                tx.pcm.drain(..n);
                *trimmed += n;
            }
            let due = clock.elapsed().as_millis() as u64 / audio::FRAME_MS + 1;
            while *frames < due {
                let frame: Vec<i16> = if tx.pcm.len() >= audio::FRAME {
                    tx.pcm.drain(..audio::FRAME).collect()
                } else if tx.ended && !tx.pcm.is_empty() {
                    tx.pcm.drain(..).collect()
                } else {
                    // Out of audio for now: hold the clock, so what comes next isn't sent too fast.
                    *clock = Instant::now() - Duration::from_millis(*frames * audio::FRAME_MS);
                    break;
                };
                tx.sent.extend_from_slice(&frame);
                let mut frame = frame;
                frame.resize(audio::FRAME, 0);
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
                if *trimmed > 0 {
                    report(Event::Note(format!(
                        "{} fell over 3 s behind the radio while it was being sent: dropped {:.1} s of it",
                        talkgroup_name(tx.talkgroup),
                        *trimmed as f64 / f64::from(audio::RATE)
                    )));
                }
                pipeline.queue.pop_front();
                report(Event::Done { call, outcome: Outcome::Ok, why: String::new(), took: began.elapsed(), waited: *waited });
                *sending = Sending::Idle;
            }
        }
    }
    Ok(())
}

/// A message from Zello, shortened for the log.
fn short(text: &str) -> String {
    if text.chars().count() > 200 { format!("{}…", text.chars().take(200).collect::<String>()) } else { text.to_string() }
}

/// The channels a connection joins: its own, then any for texts only.
fn channels(cfg: &LinkConfig) -> Vec<String> {
    std::iter::once(cfg.channel.clone()).chain(cfg.also.iter().cloned()).collect()
}

/// Which of this connection's channels an event is about (its own, when it doesn't say).
fn our_channel(v: &Value, cfg: &LinkConfig) -> Option<String> {
    match v["channel"].as_str() {
        None => Some(cfg.channel.clone()),
        Some(c) => channels(cfg).into_iter().find(|ours| ours.eq_ignore_ascii_case(c)),
    }
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

fn channel_refusal(cfg: &LinkConfig, channel: &str, err: &str, configuration: bool) -> Failure {
    let mut why = format!("Zello won't connect {} to channel '{channel}': {err}", cfg.username);
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
