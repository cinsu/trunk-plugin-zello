//! trunk-plugin-zello — a Trunk Recorder Pro plugin that streams live radio audio
//! to Zello channels, on consumer Zello or Zello Work.
//!
//! Each stream logs a Zello account on to a channel and keeps it connected.
//! Its system's talkgroup (or all of the system) goes to the channel live, a
//! transmission at a time, and how each stream is doing shows next to the
//! plugin and on its dashboard. Pages the recorder recognizes (its signals)
//! can post a text to the stream's channel and to other channels.

mod audio;
mod token;
mod zello;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use trunk_recorder_plugin::{
    AudioChunk, CallInfo, ConcludedCall, Contribution, Endpoint, EndpointState, Host, Manifest, Metrics, NoConfig, Outcome, Plugin, Setup, State,
    TonePatternEvent, topic,
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
    /// Page alerts
    ///
    /// When the recorder hears one of these pages (its signals), a text goes
    /// to Zello: "🔔 Page for <name>". Add one here, or with "Zello alert" on
    /// a signal in Setup.
    alerts: Vec<AlertConfig>,
    /// Alert account: username
    ///
    /// Only for alerts to other channels: the Zello account that posts them,
    /// joined to all of those channels. Consumer Zello lets an account join
    /// one channel at a time, so there it can post to one other channel.
    alert_username: String,
    /// Alert account: password
    #[schemars(extend("x-secret" = true))]
    alert_password: String,
    /// For tests: another Zello address. Not in the settings form.
    #[schemars(skip)]
    server: String,
}

/// Stream
#[derive(Serialize, Deserialize, JsonSchema, Clone)]
#[serde(rename_all = "camelCase", default)]
#[schemars(extend("x-title" = "name", "x-summary" = ["{channel|no channel yet}", "talkgroup {talkgroup|all}", "{system|every system}"]))]
struct StreamConfig {
    /// On
    ///
    /// Off: the stream stays here, set up or half set up, but doesn't connect
    /// or send. Turn streams on as they're ready.
    enabled: bool,
    /// Name
    ///
    /// What the status and log call it. Empty: the channel's name.
    name: String,
    /// System
    ///
    /// "Every system" sends all of them.
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
    /// Max wait, seconds
    ///
    /// Zello carries one transmission at a time. One that has to wait longer
    /// than this for its turn (behind another, or for Zello to connect) is
    /// dropped rather than sent late. Longer suits a stream of several
    /// talkgroups; shorter keeps a live channel live.
    #[schemars(range(min = 1, max = 300))]
    max_wait_s: u32,
}

impl Default for StreamConfig {
    fn default() -> Self {
        StreamConfig {
            enabled: true,
            name: String::new(),
            system: String::new(),
            talkgroup: None,
            channel: String::new(),
            username: String::new(),
            password: String::new(),
            max_wait_s: 10,
        }
    }
}

/// Page alert
#[derive(Serialize, Deserialize, JsonSchema, Clone)]
#[serde(rename_all = "camelCase", default)]
#[schemars(extend("x-title" = "name", "x-actions" = [{ "id": "test", "label": "Send a test", "hint": "Posts a test alert where this one would go (save the settings first)" }]))]
struct AlertConfig {
    /// Name
    ///
    /// What the alert says: "🔔 Page for <name>".
    name: String,
    /// Pages
    ///
    /// The signals it's for.
    #[schemars(extend("x-signals" = true))]
    signals: Vec<String>,
    /// Post on the stream's channel
    ///
    /// The channel of each stream that carries the page's talkgroup.
    to_stream: bool,
    /// Also post to
    ///
    /// Other Zello channels, posted to by the alert account.
    channels: Vec<String>,
}

impl Default for AlertConfig {
    fn default() -> Self {
        AlertConfig { name: String::new(), signals: Vec::new(), to_stream: true, channels: Vec::new() }
    }
}

/// An alert, checked.
struct Alert {
    name: String,
    signals: Vec<String>,
    to_stream: bool,
    channels: Vec<String>,
}

/// What to connect, checked.
struct Plan {
    streams: Vec<Planned>,
    alerts: Vec<Alert>,
    /// The alert account's connection, when alerts go to other channels.
    alert_link: Option<LinkConfig>,
    /// Streams that are switched off, by name.
    off: Vec<String>,
}

/// A stream, checked and ready to connect.
struct Planned {
    name: String,
    system: String,
    talkgroup: Option<u32>,
    link: LinkConfig,
}

/// Checks the settings; what to connect, or what to fix.
fn plan(c: &Config, systems: &[String]) -> Result<Plan, String> {
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
    let mut off = Vec::new();
    for (i, s) in c.streams.iter().enumerate() {
        let name = [s.name.trim(), s.channel.trim()].into_iter().find(|n| !n.is_empty()).map_or_else(|| format!("Stream {}", i + 1), str::to_string);
        if !s.enabled {
            // Staged: nothing is checked or connected until it's turned on.
            off.push(name);
            continue;
        }
        // An empty system is Pro's "Every system": the stream takes all of them.
        if !s.system.trim().is_empty() && !systems.iter().any(|n| n == s.system.trim()) {
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
                also: Vec::new(),
                username: s.username.trim().to_string(),
                password: s.password.clone(),
                tokens: tokens.clone(),
                network: network.clone(),
                // Zello limits logins that come all at once.
                start_delay: Duration::from_secs(3 * planned.len() as u64),
                max_wait: Duration::from_secs(u64::from(s.max_wait_s.clamp(1, 300))),
            },
        });
    }
    let mut alerts = Vec::new();
    let mut alert_channels: Vec<String> = Vec::new();
    for (i, a) in c.alerts.iter().enumerate() {
        let name = if a.name.trim().is_empty() { format!("Page alert {}", i + 1) } else { a.name.trim().to_string() };
        let signals: Vec<String> = a.signals.iter().map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect();
        if signals.is_empty() {
            return Err(format!("{name}: tick the page (signal) it's for."));
        }
        let channels: Vec<String> = a.channels.iter().map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect();
        if !a.to_stream && channels.is_empty() {
            return Err(format!("{name}: post it on the stream's channel, or add a channel to post it to."));
        }
        for ch in &channels {
            if !alert_channels.iter().any(|x| x.eq_ignore_ascii_case(ch)) {
                alert_channels.push(ch.clone());
            }
        }
        alerts.push(Alert { name, signals, to_stream: a.to_stream, channels });
    }
    let alert_link = if alert_channels.is_empty() {
        None
    } else {
        if c.alert_username.trim().is_empty() || c.alert_password.trim().is_empty() {
            return Err("Page alerts to other channels need the alert account: add its username and password.".into());
        }
        if c.service == Service::Consumer && alert_channels.len() > 1 {
            return Err(format!(
                "Consumer Zello lets an account join one channel at a time, but the alerts post to {} other channels ({}). Use one, or Zello Work.",
                alert_channels.len(),
                alert_channels.join(", ")
            ));
        }
        Some(LinkConfig {
            url,
            channel: alert_channels[0].clone(),
            also: alert_channels[1..].to_vec(),
            username: c.alert_username.trim().to_string(),
            password: c.alert_password.clone(),
            tokens,
            network,
            start_delay: Duration::from_secs(3 * c.streams.len() as u64),
            max_wait: Duration::from_secs(10),
        })
    };
    Ok(Plan { streams: planned, alerts, alert_link, off })
}

/// "example", from what people paste: "Example", "example.zellowork.com", a URL.
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
    /// How many streams are switched off, for the status line.
    off: usize,
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
    /// Page alerts posted.
    posted: u64,
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
        for (n, what) in [(e.heard, "heard"), (e.sent, "sent"), (e.failed, "failed"), (e.posted, "pages posted")] {
            if n > 0 {
                counts.push(if n == 1 && what == "pages posted" { "1 page posted".to_string() } else { format!("{n} {what}") });
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
        let mut text = self.streams.iter().map(Board::line).collect::<Vec<_>>().join("; ");
        if self.off > 0 {
            let off = if self.off == 1 { "1 stream off".to_string() } else { format!("{} streams off", self.off) };
            text = if text.is_empty() { "All streams are off".to_string() } else { format!("{text}; {off}") };
        }
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
        m.extra.insert("page alerts posted".into(), sum(|e| e.posted).into());
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

struct ZelloStreaming {
    host: Host,
    board: Arc<Mutex<Board>>,
    routes: Vec<Route>,
    links: Vec<Link>,
    alerts: Vec<Alert>,
    /// The alert account's connection: an index into `links`.
    alert_link: Option<usize>,
    /// Calls being sent, and the streams they go to.
    calls: HashMap<u32, Vec<usize>>,
    /// Calls that just ended, so audio arriving after its end isn't taken for a new call.
    ended: std::collections::VecDeque<u32>,
}

impl ZelloStreaming {
    fn streams_for(&self, short_name: &str, talkgroup: u32) -> Vec<usize> {
        self.routes.iter().filter(|r| (r.system.is_empty() || r.system == short_name) && r.talkgroup.is_none_or(|t| t == talkgroup)).map(|r| r.stream).collect()
    }

    /// A stream's (or the alert account's) channel.
    fn links_channel(&self, i: usize) -> String {
        self.board.lock().unwrap().streams[i].channel.clone()
    }

    /// The channels the alert account joined.
    fn alert_channels(&self) -> Vec<String> {
        self.alerts.iter().flat_map(|a| a.channels.iter().cloned()).collect()
    }

    /// Posts a text to streams' channels and other channels; how many it sent.
    fn post(&self, text: &str, streams: &[usize], channels: &[String], ack: Option<&zello::Ack>) -> usize {
        for &s in streams {
            self.links[s].send(Cmd::Text { channel: None, text: text.to_string(), ack: ack.cloned() });
        }
        if let Some(l) = self.alert_link {
            for ch in channels {
                self.links[l].send(Cmd::Text { channel: Some(ch.clone()), text: text.to_string(), ack: ack.cloned() });
            }
        }
        streams.len() + if self.alert_link.is_some() { channels.len() } else { 0 }
    }

    /// A call starts on these streams.
    fn begin(&mut self, call: u32, talkgroup: u32, streams: Vec<usize>) {
        let mut b = self.board.lock().unwrap();
        for &s in &streams {
            self.links[s].send(Cmd::Start { call, talkgroup });
            b.pending.insert((s, call), Pending { since: Some(Instant::now()), ..Default::default() });
        }
        self.calls.insert(call, streams);
    }
}

impl Plugin for ZelloStreaming {
    type Config = Config;
    type SystemConfig = NoConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "Zello Streaming".into(),
            subscribe: vec![topic::CALL_START.into(), topic::AUDIO.into(), topic::CALL_END.into(), topic::CALL_CONCLUDED.into(), topic::TONE_PATTERN.into()],
            // A button on each of a system's signals in Setup: adds a page alert for it.
            contribute: vec![Contribution {
                on: "signal".into(),
                label: "Zello alert".into(),
                hint: "Post a text to Zello when this page is heard".into(),
                append: "alerts".into(),
                item: json!({ "name": "{name}", "signals": ["{signal}"], "toStream": true }),
            }],
            ..trunk_recorder_plugin::manifest!()
        }
    }

    fn start(host: Host, setup: Setup<Config, NoConfig>) -> Result<Self, String> {
        let systems: Vec<String> = setup.systems.iter().map(|s| s.short_name.clone()).collect();
        let Plan { streams: planned, alerts, alert_link, off } = plan(&setup.config, &systems)?;
        for name in &off {
            host.info(format!("{name}: off"));
        }
        let users: Vec<&str> = planned.iter().map(|p| p.link.username.as_str()).collect();
        for (i, u) in users.iter().enumerate() {
            if users[..i].contains(u) {
                host.warn(format!("More than one stream uses Zello account {u}. Zello keeps one connection per account, so they'll knock each other off."));
            }
        }
        let service = match setup.config.service {
            Service::Consumer => "Zello".to_string(),
            Service::Work => format!("Zello Work network {}", network_name(&setup.config.network).unwrap_or_default()),
        };
        let mut entries: Vec<(String, String)> = planned.iter().map(|p| (p.name.clone(), p.link.channel.clone())).collect();
        if let Some(l) = &alert_link {
            entries.push(("Page alerts".into(), channels_label(l)));
        }
        let board = Arc::new(Mutex::new(Board {
            streams: entries
                .into_iter()
                .map(|(name, channel)| Entry {
                    name,
                    channel,
                    state: LinkState::Connecting,
                    heard: 0,
                    sent: 0,
                    failed: 0,
                    posted: 0,
                    queued: 0,
                    streaming: false,
                    delay_ms: None,
                    last_ok: None,
                    last_error: None,
                })
                .collect(),
            shown: String::new(),
            off: off.len(),
            pending: HashMap::new(),
        }));
        board.lock().unwrap().publish(&host);
        let mut routes = Vec::new();
        let mut links = Vec::new();
        for (i, p) in planned.into_iter().enumerate() {
            let tg = p.talkgroup.map_or("every talkgroup".to_string(), |t| format!("talkgroup {t}"));
            let system = if p.system.is_empty() { "every system" } else { p.system.as_str() };
            host.info(format!("{}: {system} {tg} → {service}, channel '{}', as {}", p.name, p.link.channel, p.link.username));
            routes.push(Route { system: p.system, talkgroup: p.talkgroup, stream: i });
            links.push(Link::start(p.link, reporter(i, &host, &board)));
        }
        let alert_link = alert_link.map(|l| {
            let i = links.len();
            host.info(format!("Page alerts: posted to {} as {}", channels_label(&l), l.username));
            links.push(Link::start(l, reporter(i, &host, &board)));
            i
        });
        for a in &alerts {
            let mut to: Vec<String> = a.channels.iter().map(|c| format!("'{c}'")).collect();
            if a.to_stream {
                to.insert(0, "the stream's channel".into());
            }
            host.info(format!("Page alert {}: {} → {}", a.name, a.signals.join(", "), to.join(", ")));
        }
        Ok(ZelloStreaming { host, board, routes, links, alerts, alert_link, calls: HashMap::new(), ended: Default::default() })
    }

    fn tone_pattern(&mut self, t: TonePatternEvent) {
        let heard: Vec<String> = t.pattern.signals.iter().map(|id| format!("{}:{id}", t.short_name)).collect();
        if heard.is_empty() {
            return;
        }
        let streams = self.streams_for(&t.short_name, t.talkgroup);
        let matched: Vec<usize> = (0..self.alerts.len()).filter(|&i| self.alerts[i].signals.iter().any(|s| heard.contains(s))).collect();
        if matched.is_empty() {
            return;
        }
        for i in matched {
            let a = &self.alerts[i];
            let text = format!("🔔 Page for {}", a.name);
            let (to_streams, to_channels) = (if a.to_stream { streams.clone() } else { Vec::new() }, a.channels.clone());
            let mut where_to: Vec<String> = to_streams.iter().map(|&s| format!("'{}'", self.links_channel(s))).collect();
            where_to.extend(to_channels.iter().map(|c| format!("'{c}'")));
            if where_to.is_empty() {
                self.host.warn(format!(
                    "Page heard: {} on {} talkgroup {}, but there's nowhere to post it: no stream carries that talkgroup",
                    a.name, t.short_name, t.talkgroup
                ));
                continue;
            }
            self.host.info(format!("Page heard: {} on {} talkgroup {} → {}", a.name, t.short_name, t.talkgroup, where_to.join(", ")));
            self.post(&text, &to_streams, &to_channels, None);
        }
    }

    fn action(&mut self, a: trunk_recorder_plugin::Action, reply: trunk_recorder_plugin::Reply) {
        if a.action != "test" {
            return reply.fail("This plugin has no such action");
        }
        let alert: AlertConfig = match serde_json::from_value(a.value) {
            Ok(x) => x,
            Err(e) => return reply.fail(format!("That alert couldn't be read ({e})")),
        };
        let systems: Vec<&str> = alert.signals.iter().filter_map(|s| s.split_once(':').map(|x| x.0)).collect();
        if alert.to_stream && systems.is_empty() {
            return reply.fail("Tick the page (signal) it's for first: the test goes to the streams of that page's system.");
        }
        let to_streams: Vec<usize> = if alert.to_stream {
            self.routes.iter().filter(|r| r.system.is_empty() || systems.contains(&r.system.as_str())).map(|r| r.stream).collect()
        } else {
            Vec::new()
        };
        let to_channels: Vec<String> = alert.channels.iter().map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect();
        if let Some(missing) = to_channels.iter().find(|c| !self.alert_channels().iter().any(|x| x.eq_ignore_ascii_case(c))) {
            return reply.fail(format!("'{missing}' isn't in the saved settings yet: save them, then test."));
        }
        if to_streams.is_empty() && to_channels.is_empty() {
            return reply.fail("Nowhere to post it: no stream is on that page's system, and no other channels are listed.");
        }
        let name = if alert.name.trim().is_empty() { "this alert".to_string() } else { alert.name.trim().to_string() };
        let (tx, rx) = std::sync::mpsc::channel();
        let n = self.post(&format!("🔔 Test: Page for {name}"), &to_streams, &to_channels, Some(&tx));
        drop(tx);
        // Zello answers each in a few seconds at most; the reply waits for all of them.
        std::thread::spawn(move || {
            let end = Instant::now() + Duration::from_secs(25);
            let (mut posted, mut failed) = (Vec::new(), Vec::new());
            while posted.len() + failed.len() < n {
                match rx.recv_timeout(end.saturating_duration_since(Instant::now())) {
                    Ok(Ok(channel)) => posted.push(format!("'{channel}'")),
                    Ok(Err(why)) => failed.push(why),
                    Err(_) => {
                        failed.push("no answer from the plugin's Zello connection".into());
                        break;
                    }
                }
            }
            if failed.is_empty() {
                reply.ok(format!("Posted to {}", posted.join(", ")));
            } else if posted.is_empty() {
                reply.fail(format!("Not posted: {}", failed.join("; ")));
            } else {
                reply.fail(format!("Posted to {}, but not everywhere: {}", posted.join(", "), failed.join("; ")));
            }
        });
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
        self.begin(call.id, call.talkgroup, streams);
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
            self.begin(chunk.call_id, chunk.talkgroup, streams);
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

/// What a connection reports, put on the board: `i` is its place there.
fn reporter(i: usize, host: &Host, board: &Arc<Mutex<Board>>) -> zello::Report {
    let (h, b) = (host.clone(), board.clone());
    Arc::new(move |event: Event| {
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
            Event::Info(text) => h.info(format!("{}: {text}", e.name)),
            Event::Posted { channel, ok, why } => {
                if ok {
                    e.posted += 1;
                    h.info(format!("{}: page alert posted to '{channel}'", e.name));
                } else {
                    h.warn(format!("{}: a page alert wasn't posted to '{channel}': {why}", e.name));
                }
            }
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
    })
}

/// "District 2" or "3 channels".
fn channels_label(l: &LinkConfig) -> String {
    if l.also.is_empty() { l.channel.clone() } else { format!("{} channels", l.also.len() + 1) }
}

fn main() {
    trunk_recorder_plugin::run::<ZelloStreaming>();
}

#[cfg(test)]
mod tests;
