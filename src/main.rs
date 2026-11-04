//! tr-pro-zello — a Trunk Recorder Pro plugin that streams live radio audio
//! to Zello channels, on consumer Zello or Zello Work.
//!
//! This version logs each stream's account on to its Zello channel, keeps it
//! connected, and shows how each is doing next to the plugin. It also counts
//! the transmissions each stream will send once audio is added.

mod token;
mod zello;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trunk_recorder_plugin::{CallInfo, Endpoint, EndpointState, Host, Manifest, Metrics, NoConfig, Plugin, Setup, State, topic};

use token::TokenMaker;
use zello::{Link, LinkConfig, LinkState};

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
}

struct Entry {
    name: String,
    channel: String,
    state: LinkState,
    heard: u64,
}

impl Board {
    fn line(e: &Entry) -> String {
        let doing = match &e.state {
            LinkState::Connecting => "connecting".to_string(),
            LinkState::LoggedOn => "logged in, waiting for the channel".to_string(),
            LinkState::Online { users } => format!("connected, {users} online"),
            LinkState::Down(why) | LinkState::Refused(why) => why.clone(),
        };
        let heard = if e.heard > 0 { format!(" · {} heard", e.heard) } else { String::new() };
        format!("{} → {}: {doing}{heard}", e.name, e.channel)
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
                    LinkState::Online { .. } => EndpointState::Up,
                    LinkState::Down(_) | LinkState::Refused(_) => EndpointState::Down,
                    _ => EndpointState::Unknown,
                },
                ..Default::default()
            })
            .collect();
        let heard: u64 = self.streams.iter().map(|e| e.heard).sum();
        let mut m = Metrics { endpoints, ..Default::default() };
        m.extra.insert("transmissions heard".into(), heard.into());
        host.metrics(&m);
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
}

impl Plugin for TrProZello {
    type Config = Config;
    type SystemConfig = NoConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "Zello".into(),
            // Counting transmissions for now; live audio comes next.
            subscribe: vec![topic::CALL_START.into()],
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
            streams: planned.iter().map(|p| Entry { name: p.name.clone(), channel: p.link.channel.clone(), state: LinkState::Connecting, heard: 0 }).collect(),
            shown: String::new(),
        }));
        board.lock().unwrap().publish(&host);
        let mut routes = Vec::new();
        let mut links = Vec::new();
        for (i, p) in planned.into_iter().enumerate() {
            let tg = p.talkgroup.map_or("every talkgroup".to_string(), |t| format!("talkgroup {t}"));
            host.info(format!("{}: {} {tg} → {service}, channel '{}', as {}", p.name, p.system, p.link.channel, p.link.username));
            routes.push(Route { system: p.system, talkgroup: p.talkgroup, stream: i });
            let (h, b) = (host.clone(), board.clone());
            let report: zello::Report = Arc::new(move |state: LinkState| {
                let mut b = b.lock().unwrap();
                let e = &mut b.streams[i];
                match &state {
                    LinkState::Online { users } if !matches!(e.state, LinkState::Online { .. }) => {
                        h.info(format!("{}: connected to Zello channel '{}' ({users} online)", e.name, e.channel));
                    }
                    LinkState::Down(why) | LinkState::Refused(why) => h.warn(format!("{}: {why}", e.name)),
                    _ => {}
                }
                e.state = state;
                b.publish(&h);
            });
            links.push(Link::start(p.link, report));
        }
        Ok(TrProZello { host, board, routes, links })
    }

    fn call_start(&mut self, call: CallInfo) {
        if call.encrypted {
            return;
        }
        let mut b = self.board.lock().unwrap();
        let mut hit = false;
        for r in &self.routes {
            if r.system == call.short_name && r.talkgroup.is_none_or(|t| t == call.talkgroup) {
                b.streams[r.stream].heard += 1;
                hit = true;
            }
        }
        if hit {
            b.publish(&self.host);
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
