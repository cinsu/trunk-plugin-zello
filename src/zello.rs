//! One Zello connection: one account logged on to one channel, kept up, with
//! how it's going reported as it changes.
//!
//! Each connection runs on its own thread, so the recorder's events are never
//! held up by the network. The protocol is Zello's Channel API: a WebSocket,
//! a `logon` command, then `on_channel_status` events.
//! <https://github.com/zelloptt/zello-channel-api/blob/master/API.md>

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use serde_json::{Value, json};
use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::HandshakeError;
use tungstenite::{Message, WebSocket};

use crate::token::TokenMaker;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// How often a connection wakes to answer pings and to check for shutdown.
const TICK: Duration = Duration::from_millis(500);
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

pub type Report = Arc<dyn Fn(LinkState) + Send + Sync>;

/// A running connection.
pub struct Link {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Link {
    pub fn start(cfg: LinkConfig, report: Report) -> Link {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = thread::spawn(move || run(cfg, &flag, &report));
        Link { stop, thread: Some(thread) }
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

fn run(cfg: LinkConfig, stop: &AtomicBool, report: &Report) {
    if !pause(stop, cfg.start_delay) {
        return;
    }
    let mut backoff = FIRST_RETRY;
    loop {
        let mut online_since = None;
        let failure = match session(&cfg, stop, report, &mut online_since) {
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
                report(LinkState::Down(format!("{why}; trying again in {} s", wait.as_secs())));
                wait
            }
            Failure::Refused(why) => {
                report(LinkState::Refused(format!("{why} (trying again in {} min)", REFUSED_RETRY.as_secs() / 60)));
                REFUSED_RETRY
            }
        };
        if !pause(stop, wait) {
            return;
        }
    }
}

/// Sleeps, waking to check `stop`. False if it was asked to stop.
fn pause(stop: &AtomicBool, d: Duration) -> bool {
    let end = Instant::now() + d;
    while Instant::now() < end {
        if stop.load(Ordering::SeqCst) {
            return false;
        }
        thread::sleep(Duration::from_millis(100).min(end - Instant::now()));
    }
    !stop.load(Ordering::SeqCst)
}

/// One connection, from connecting to its end. `Ok` when asked to stop.
fn session(cfg: &LinkConfig, stop: &AtomicBool, report: &Report, online_since: &mut Option<Instant>) -> Result<(), Failure> {
    report(LinkState::Connecting);
    let mut ws = open(cfg)?;
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
    loop {
        if stop.load(Ordering::SeqCst) {
            let _ = ws.close(None);
            let _ = ws.flush();
            return Ok(());
        }
        match ws.read() {
            Ok(Message::Text(text)) => {
                let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                if v["seq"] == 1 {
                    if v["success"] == true {
                        logged_on = true;
                        report(LinkState::LoggedOn);
                    } else {
                        return Err(refusal(cfg, v["error"].as_str().unwrap_or("no reason given")));
                    }
                } else if v["command"] == "on_channel_status" && same_channel(&v, &cfg.channel) {
                    if v["status"] == "online" {
                        online_since.get_or_insert_with(Instant::now);
                        report(LinkState::Online { users: v["users_online"].as_u64().unwrap_or(0) });
                    } else if let Some(err) = v["error"].as_str() {
                        return Err(channel_refusal(cfg, err, v["error_type"] == "configuration"));
                    } else {
                        return Err(Failure::Down(format!("Zello took channel '{}' offline", cfg.channel)));
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
    }
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
