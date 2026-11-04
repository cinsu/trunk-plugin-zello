//! tr-pro-zello — a Trunk Recorder Pro plugin that streams live radio audio
//! to Zello channels, on consumer Zello or Zello Work.
//!
//! Each stream logs a Zello account on to a channel and keeps it connected.
//! Its system's talkgroup (or all of the system) goes to the channel live, a
//! transmission at a time, and how each stream is doing shows next to the
//! plugin and on its dashboard.

mod audio;
mod token;
mod zello;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trunk_recorder_plugin::{
    AudioChunk, CallInfo, ConcludedCall, Endpoint, EndpointState, Host, Manifest, Metrics, NoConfig, Outcome, Plugin, Setup, State, topic,
};

use token::TokenMaker;
use zello::{Cmd, Event, Link, LinkConfig, LinkState};

/// Which Zello.
#[derive(Serialize, Deserialize, JsonSchema, Default, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "lowercase")]
enum Service {
    /// Zello (the free app)
    #[default]
    Consumer,
    /// Zello Work
    Work,
}

#[derive(Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase", default)]
struct Config {
    /// Zello service
    service: Service,
    /// Issuer
    ///
    /// From developers.zello.com, under Keys.
    #[schemars(extend("x-when" = { "service": "consumer" }))]
    issuer: String,
    /// Private key
    ///
    /// From developers.zello.com, under Keys. Paste all of it, including the
    /// BEGIN and END lines. It's kept in the recorder's settings and never
    /// shown in its log.
    #[schemars(extend("x-secret" = true, "x-multiline" = true, "x-when" = { "service": "consumer" }))]
    private_key: String,
    /// Network
    ///
    /// Your Zello Work network's name: the first part of its address,
    /// network.zellowork.com.
    #[schemars(extend("x-when" = { "service": "work" }))]
    network: String,
    /// Streams
    ///
    /// Each sends a system's talkgroup (or all of it) to a Zello channel,
    /// with a Zello account of its own.
    streams: Vec<StreamConfig>,
    /// For tests: another Zello address. Not in the settings form.
    #[schemars(skip)]
    server: String,
}

/// Stream
#[derive(Serialize, Deserialize, JsonSchema, Default, Clone)]
#[serde(rename_all = "camelCase", default)]
struct StreamConfig {
    /// Name
    ///
    /// What the status and log call it. Empty: the channel's name.
    name: String,
    /// System
    #[schemars(extend("x-system" = true))]
    system: String,
    /// Talkgroup
    ///
    /// Leave it empty to send every talkgroup on the system.
    talkgroup: Option<u32>,
    /// Zello channel
    channel: String,
    /// Zello username
    ///
    /// The account that talks on the channel. Give each stream its own:
    /// Zello keeps one connection per account.
    username: String,
    /// Zello password
    #[schemars(extend("x-secret" = true))]
    password: String,
}

/// A stream, checked and ready to connect.
struct Planned {
    name: String,
    system: String,
    talkgroup: Option<u32>,
    link: LinkConfig,
}

/// Checks the settings; what to connect, or what to fix.
fn plan(c: &Config, systems: &[String]) -> Result<Vec<Planned>, String> {
    if c.streams.is_empty() {
        return Err("Add a stream: the system and talkgroup to send, and the Zello channel and account to send it with.".into());
    }
    let (tokens, network) = match c.service {
        Service::Consumer => (Some(Arc::new(TokenMaker::new(&c.issuer, &c.private_key)?)), None),
        Service::Work => (None, Some(network_name(&c.network)?)),
    };
    let url = match (&network, c.server.trim()) {
        (_, s) if !s.is_empty() => s.to_string(),
        (Some(net), _) => format!("wss://zellowork.io/ws/{net}"),
        (None, _) => "wss://zello.io/ws".to_string(),
    };
    let mut planned = Vec::new();
    for (i, s) in c.streams.iter().enumerate() {
        let name = [s.name.trim(), s.channel.trim()].into_iter().find(|n| !n.is_empty()).map_or_else(|| format!("Stream {}", i + 1), str::to_string);
        if s.system.trim().is_empty() {
            return Err(format!("{name}: choose its system."));
        }
        if !systems.iter().any(|n| n == s.system.trim()) {
            return Err(format!("{name}: the recorder has no system named '{}'.", s.system.trim()));
        }
        for (value, what) in [(&s.channel, "Zello channel"), (&s.username, "Zello username"), (&s.password, "Zello password")] {
            if value.trim().is_empty() {
                return Err(format!("{name}: add its {what}."));
            }
        }
        planned.push(Planned {
            name,
            system: s.system.trim().to_string(),
            talkgroup: s.talkgroup,
            link: LinkConfig {
                url: url.clone(),
                channel: s.channel.trim().to_string(),
                username: s.username.trim().to_string(),
                password: s.password.clone(),
                tokens: tokens.clone(),
                network: network.clone(),
                // Zello limits logins that come all at once.
                start_delay: Duration::from_secs(3 * i as u64),
            },
        });
    }
    Ok(planned)
}

/// "wahkiakum", from what people paste: "Wahkiakum", "wahkiakum.zellowork.com", a URL.
fn network_name(raw: &str) -> Result<String, String> {
    let mut n = raw.trim().to_lowercase();
    for prefix in ["https://", "http://"] {
        if let Some(rest) = n.strip_prefix(prefix) {
            n = rest.to_string();
        }
    }
    let n = n.trim_end_matches('/').trim_end_matches(".zellowork.com").to_string();
    if n.is_empty() {
        return Err("Add your Zello Work network's name.".into());
    }
    if !n.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(format!("'{}' isn't a Zello Work network name: use the first part of its address, network.zellowork.com.", raw.trim()));
    }
    Ok(n)
}

/// What each stream is doing, shown next to the plugin.
struct Board {
    streams: Vec<Entry>,
    shown: String,
    /// Calls sent (or not), waiting to be matched with their recording to report them.
    pending: HashMap<(usize, u32), Pending>,
}

struct Entry {
    name: String,
    channel: String,
    state: LinkState,
    heard: u64,
    sent: u64,
    failed: u64,
    queued: usize,
    streaming: bool,
    /// From a call's start to Zello taking it, ms, smoothed.
    delay_ms: Option<f64>,
    last_ok: Option<f64>,
    last_error: Option<(f64, String)>,
}

#[derive(Default)]
struct Pending {
    since: Option<Instant>,
    path: Option<String>,
    result: Option<(Outcome, String, Duration)>,
}

fn unix_now() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64())
}

impl Board {
    fn line(e: &Entry) -> String {
        let doing = match &e.state {
            LinkState::Connecting => "connecting".to_string(),
            LinkState::LoggedOn => "logged in, waiting for the channel".to_string(),
            LinkState::Online { users } if e.streaming => format!("sending, {users} online"),
            LinkState::Online { users } => format!("connected, {users} online"),
            LinkState::Down(why) | LinkState::Refused(why) => why.clone(),
        };
        let mut counts = Vec::new();
        for (n, what) in [(e.heard, "heard"), (e.sent, "sent"), (e.failed, "failed")] {
            if n > 0 {
                counts.push(format!("{n} {what}"));
            }
        }
        let counts = if counts.is_empty() { String::new() } else { format!(" · {}", counts.join(" · ")) };
        format!("{} → {}: {doing}{counts}", e.name, e.channel)
    }

    /// Shows the status and metrics, when they changed.
    fn publish(&mut self, host: &Host) {
        let state = if self.streams.iter().any(|e| matches!(e.state, LinkState::Refused(_))) {
            State::Error
        } else if self.streams.iter().any(|e| matches!(e.state, LinkState::Down(_))) {
            State::Warning
        } else {
            State::Ok
        };
        let text = self.streams.iter().map(Board::line).collect::<Vec<_>>().join("; ");
        if text == self.shown {
            return;
        }
        self.shown = text.clone();
        host.status(state, text);
        let endpoints = self
            .streams
            .iter()
            .map(|e| Endpoint {
                name: format!("Zello: {}", e.channel),
                state: match e.state {
                    LinkState::Online { .. } if e.failed > 0 && e.last_error.as_ref().map(|x| x.0) > e.last_ok => EndpointState::Degraded,
                    LinkState::Online { .. } => EndpointState::Up,
                    LinkState::Down(_) | LinkState::Refused(_) => EndpointState::Down,
                    _ => EndpointState::Unknown,
                },
                ..Default::default()
            })
            .collect();
        let sum = |f: fn(&Entry) -> u64| self.streams.iter().map(f).sum::<u64>();
        let delays: Vec<f64> = self.streams.iter().filter_map(|e| e.delay_ms).collect();
        let last_error = self.streams.iter().filter_map(|e| e.last_error.clone()).max_by(|a, b| a.0.total_cmp(&b.0));
        let mut m = Metrics {
            endpoints,
            queued: Some(self.streams.iter().map(|e| e.queued as u64).sum()),
            in_flight: Some(self.streams.iter().filter(|e| e.streaming).count() as u64),
            latency_ms: (!delays.is_empty()).then(|| delays.iter().sum::<f64>() / delays.len() as f64),
            last_ok: self.streams.iter().filter_map(|e| e.last_ok).max_by(f64::total_cmp),
            last_error: last_error.as_ref().map(|x| x.0),
            last_error_text: last_error.map(|x| x.1).unwrap_or_default(),
            ..Default::default()
        };
        m.extra.insert("transmissions heard".into(), sum(|e| e.heard).into());
        m.extra.insert("transmissions sent".into(), sum(|e| e.sent).into());
        m.extra.insert("transmissions failed".into(), sum(|e| e.failed).into());
        host.metrics(&m);
    }

    /// Reports a call once both its result and its recording are known.
    fn settle(&mut self, host: &Host, key: (usize, u32)) {
        let ready = self.pending.get(&key).is_some_and(|p| p.path.is_some() && p.result.is_some());
        if ready {
            let p = self.pending.remove(&key).expect("checked");
            let (outcome, why, took) = p.result.expect("checked");
            host.call_result_took(p.path.as_deref().expect("checked"), outcome, why, "", took);
        }
        // Calls that were never recorded have nothing to report against.
        self.pending.retain(|_, p| p.since.is_none_or(|t| t.elapsed() < Duration::from_secs(600)));
    }
}

/// Which stream a system's talkgroup goes to.
struct Route {
    system: String,
    talkgroup: Option<u32>,
    stream: usize,
}

struct TrProZello {
    host: Host,
    board: Arc<Mutex<Board>>,
    routes: Vec<Route>,
    links: Vec<Link>,
    /// Calls being sent, and the streams they go to.
    calls: HashMap<u32, Vec<usize>>,
    /// Calls that just ended, so audio arriving after its end isn't taken for a new call.
    ended: std::collections::VecDeque<u32>,
}

impl TrProZello {
    fn streams_for(&self, short_name: &str, talkgroup: u32) -> Vec<usize> {
        self.routes.iter().filter(|r| r.system == short_name && r.talkgroup.is_none_or(|t| t == talkgroup)).map(|r| r.stream).collect()
    }

    /// A call starts on these streams.
    fn begin(&mut self, call: u32, streams: Vec<usize>) {
        let mut b = self.board.lock().unwrap();
        for &s in &streams {
            self.links[s].send(Cmd::Start { call });
            b.pending.insert((s, call), Pending { since: Some(Instant::now()), ..Default::default() });
        }
        self.calls.insert(call, streams);
    }
}

impl Plugin for TrProZello {
    type Config = Config;
    type SystemConfig = NoConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "Zello".into(),
            subscribe: vec![topic::CALL_START.into(), topic::AUDIO.into(), topic::CALL_END.into(), topic::CALL_CONCLUDED.into()],
            ..trunk_recorder_plugin::manifest!()
        }
    }

    fn start(host: Host, setup: Setup<Config, NoConfig>) -> Result<Self, String> {
        let systems: Vec<String> = setup.systems.iter().map(|s| s.short_name.clone()).collect();
        let planned = plan(&setup.config, &systems)?;
        let users: Vec<&str> = planned.iter().map(|p| p.link.username.as_str()).collect();
        for (i, u) in users.iter().enumerate() {
            if users[..i].contains(u) {
                host.warn(format!("More than one stream uses Zello account {u}. Zello keeps one connection per account, so they'll knock each other off."));
            }
        }
        let service = match setup.config.service {
            Service::Consumer => "Zello".to_string(),
            Service::Work => format!("Zello Work network {}", planned[0].link.network.as_deref().unwrap_or_default()),
        };
        let board = Arc::new(Mutex::new(Board {
            streams: planned
                .iter()
                .map(|p| Entry {
                    name: p.name.clone(),
                    channel: p.link.channel.clone(),
                    state: LinkState::Connecting,
                    heard: 0,
                    sent: 0,
                    failed: 0,
                    queued: 0,
                    streaming: false,
                    delay_ms: None,
                    last_ok: None,
                    last_error: None,
                })
                .collect(),
            shown: String::new(),
            pending: HashMap::new(),
        }));
        board.lock().unwrap().publish(&host);
        let mut routes = Vec::new();
        let mut links = Vec::new();
        for (i, p) in planned.into_iter().enumerate() {
            let tg = p.talkgroup.map_or("every talkgroup".to_string(), |t| format!("talkgroup {t}"));
            host.info(format!("{}: {} {tg} → {service}, channel '{}', as {}", p.name, p.system, p.link.channel, p.link.username));
            routes.push(Route { system: p.system, talkgroup: p.talkgroup, stream: i });
            let (h, b) = (host.clone(), board.clone());
            let report: zello::Report = Arc::new(move |event: Event| {
                let mut b = b.lock().unwrap();
                let e = &mut b.streams[i];
                match event {
                    Event::State(state) => {
                        match &state {
                            LinkState::Online { users } if !matches!(e.state, LinkState::Online { .. }) => {
                                h.info(format!("{}: connected to Zello channel '{}' ({users} online)", e.name, e.channel));
                            }
                            LinkState::Down(why) | LinkState::Refused(why) => h.warn(format!("{}: {why}", e.name)),
                            _ => {}
                        }
                        e.state = state;
                    }
                    Event::Load { queued, streaming } => (e.queued, e.streaming) = (queued, streaming),
                    Event::Note(text) => h.warn(format!("{}: {text}", e.name)),
                    Event::Done { call, outcome, why, took, waited } => {
                        match outcome {
                            Outcome::Ok => {
                                e.sent += 1;
                                e.last_ok = Some(unix_now());
                                let ms = waited.as_secs_f64() * 1000.0;
                                e.delay_ms = Some(e.delay_ms.map_or(ms, |d| d * 0.8 + ms * 0.2));
                            }
                            Outcome::Failed => {
                                e.failed += 1;
                                e.last_error = Some((unix_now(), why.clone()));
                                h.warn(format!("{}: a transmission wasn't sent: {why}", e.name));
                            }
                            Outcome::Skipped => {}
                        }
                        let p = b.pending.entry((i, call)).or_default();
                        p.result = Some((outcome, why, took));
                        b.settle(&h, (i, call));
                    }
                }
                b.publish(&h);
            });
            links.push(Link::start(p.link, report));
        }
        Ok(TrProZello { host, board, routes, links, calls: HashMap::new(), ended: Default::default() })
    }

    fn call_start(&mut self, call: CallInfo) {
        if call.encrypted {
            return;
        }
        let streams = self.streams_for(&call.short_name, call.talkgroup);
        if streams.is_empty() {
            return;
        }
        {
            let mut b = self.board.lock().unwrap();
            for &s in &streams {
                b.streams[s].heard += 1;
            }
            b.publish(&self.host);
        }
        self.begin(call.id, streams);
    }

    fn audio(&mut self, chunk: AudioChunk) {
        if !self.calls.contains_key(&chunk.call_id) {
            if self.ended.contains(&chunk.call_id) {
                return;
            }
            // A call whose start this plugin didn't see (heard on several sites, or
            // the plugin restarted mid-call): route it by its system and talkgroup.
            let streams = self.streams_for(&chunk.short_name, chunk.talkgroup);
            if streams.is_empty() {
                return;
            }
            self.begin(chunk.call_id, streams);
        }
        let samples = chunk.samples();
        for &s in &self.calls[&chunk.call_id] {
            self.links[s].send(Cmd::Audio { call: chunk.call_id, rate: chunk.sample_rate, samples: samples.clone() });
        }
    }

    fn call_end(&mut self, call: CallInfo) {
        if let Some(streams) = self.calls.remove(&call.id) {
            for s in streams {
                self.links[s].send(Cmd::End { call: call.id });
            }
            self.ended.push_back(call.id);
            if self.ended.len() > 64 {
                self.ended.pop_front();
            }
        }
    }

    fn call_concluded(&mut self, call: ConcludedCall) {
        let Ok(id) = u32::try_from(call.call.call_num) else { return };
        let mut b = self.board.lock().unwrap();
        let keys: Vec<(usize, u32)> = b.pending.keys().filter(|k| k.1 == id).copied().collect();
        for key in keys {
            b.pending.get_mut(&key).expect("listed").path = Some(call.path.clone());
            b.settle(&self.host, key);
        }
    }

    fn shutdown(&mut self, grace: Duration) {
        for l in &self.links {
            l.stop();
        }
        let end = Instant::now() + grace.saturating_sub(Duration::from_secs(1));
        while Instant::now() < end && !self.links.iter().all(Link::finished) {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn main() {
    trunk_recorder_plugin::run::<TrProZello>();
}

#[cfg(test)]
mod tests;
