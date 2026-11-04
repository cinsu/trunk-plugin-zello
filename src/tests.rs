//! Tests. The end-to-end ones run the plugin the way the recorder does, against
//! a stand-in Zello server on this computer: they never reach real Zello.

use super::*;
use crate::token::tests::{TEST_PKCS8, pem};
use serde_json::{Value, json};
use std::io::{BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::JoinHandle;
use trunk_recorder_plugin::{HostMessage, testing};

const PASSWORD: &str = "pw-very-secret";

fn stream(talkgroup: Option<u32>) -> Value {
    json!({ "name": "Test", "system": "sys1", "talkgroup": talkgroup, "channel": "Test Channel", "username": "gw-test", "password": PASSWORD })
}

fn work(server: &str) -> Value {
    json!({ "service": "work", "network": "testnet", "streams": [stream(Some(101))], "server": server })
}

fn consumer(server: &str) -> Value {
    json!({ "service": "consumer", "issuer": "test-issuer", "privateKey": pem("PRIVATE KEY", TEST_PKCS8), "streams": [stream(None)], "server": server })
}

fn config(v: Value) -> Config {
    serde_json::from_value(v).unwrap()
}

fn plan_err(v: Value) -> String {
    match plan(&config(v), &["sys1".to_string()]) {
        Ok(_) => panic!("expected the settings to be refused"),
        Err(e) => e,
    }
}

// ---- settings ----

#[test]
fn default_settings_say_what_to_add() {
    assert!(plan_err(json!({})).contains("Add a stream"));
}

#[test]
fn refuses_incomplete_streams() {
    let mut v = work("");
    v["streams"][0]["system"] = json!("nope");
    assert!(plan_err(v).contains("no system named 'nope'"));
    for (field, says) in [("channel", "Zello channel"), ("username", "Zello username"), ("password", "Zello password")] {
        let mut v = work("");
        v["streams"][0][field] = json!(" ");
        assert!(plan_err(v).contains(says), "{field}");
    }
    let mut v = consumer("");
    v["issuer"] = json!("");
    assert!(plan_err(v).contains("Issuer"));
    let mut v = work("");
    v["network"] = json!("");
    assert!(plan_err(v).contains("network's name"));
}

#[test]
fn addresses_for_each_service() {
    let p = plan(&config(work("")), &["sys1".into()]).unwrap();
    assert_eq!(p[0].link.url, "wss://zellowork.io/ws/testnet");
    assert!(p[0].link.tokens.is_none());
    let p = plan(&config(consumer("")), &["sys1".into()]).unwrap();
    assert_eq!(p[0].link.url, "wss://zello.io/ws");
    assert!(p[0].link.tokens.is_some());
}

#[test]
fn network_names_as_people_paste_them() {
    assert_eq!(network_name(" Wahkiakum ").unwrap(), "wahkiakum");
    assert_eq!(network_name("https://wahkiakum.zellowork.com/").unwrap(), "wahkiakum");
    assert!(network_name("my network").is_err());
}

#[test]
fn streams_log_in_a_few_seconds_apart() {
    let mut v = work("");
    v["streams"] = json!([stream(Some(1)), stream(Some(2)), stream(Some(3))]);
    let p = plan(&config(v), &["sys1".into()]).unwrap();
    let delays: Vec<u64> = p.iter().map(|s| s.link.start_delay.as_secs()).collect();
    assert_eq!(delays, [0, 3, 6]);
}

#[test]
fn the_settings_form() {
    let m = trunk_recorder_plugin::describe::<ZelloStream>();
    let m = serde_json::to_value(m).unwrap();
    let props = &m["config"]["properties"];
    assert_eq!(props["privateKey"]["x-secret"], true);
    assert_eq!(props["issuer"]["x-when"], json!({ "service": "consumer" }));
    assert_eq!(props["network"]["x-when"], json!({ "service": "work" }));
    let stream = &props["streams"]["items"]["properties"];
    assert_eq!(stream["password"]["x-secret"], true);
    assert_eq!(stream["system"]["x-system"], true);
    assert!(props.get("server").is_none(), "the test-only address isn't in the form");
    assert_eq!(m["id"], "zello-stream");
}

// ---- a stand-in Zello ----

/// What the stand-in does with a logon: the messages to send back, and whether
/// to hang up afterwards.
type Behaviour = fn(&Value) -> (Vec<Value>, bool);

struct FakeZello {
    url: String,
    logons: Arc<Mutex<Vec<Value>>>,
}

impl FakeZello {
    fn start(behaviour: Behaviour) -> FakeZello {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let logons = Arc::new(Mutex::new(Vec::new()));
        let seen = logons.clone();
        std::thread::spawn(move || {
            for sock in listener.incoming().flatten() {
                let seen = seen.clone();
                std::thread::spawn(move || {
                    let Ok(mut ws) = tungstenite::accept(sock) else { return };
                    let Ok(tungstenite::Message::Text(t)) = ws.read() else { return };
                    let logon: Value = serde_json::from_str(&t).unwrap();
                    seen.lock().unwrap().push(logon.clone());
                    let (replies, hang_up) = behaviour(&logon);
                    for r in replies {
                        let _ = ws.send(tungstenite::Message::text(r.to_string()));
                    }
                    if hang_up {
                        let _ = ws.close(None);
                        let _ = ws.flush();
                        return;
                    }
                    while ws.read().is_ok() {}
                });
            }
        });
        FakeZello { url, logons }
    }

    fn logons(&self) -> Vec<Value> {
        self.logons.lock().unwrap().clone()
    }
}

fn welcome(logon: &Value) -> Vec<Value> {
    vec![
        json!({ "seq": 1, "success": true }),
        json!({ "command": "on_channel_status", "channel": logon["channels"][0], "status": "online", "users_online": 3 }),
    ]
}

// ---- the plugin, run the way the recorder runs it ----

/// Feeds the plugin lines as the test sends them.
struct Lines {
    rx: Receiver<String>,
    buf: Vec<u8>,
    pos: usize,
}

impl Read for Lines {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.buf.len() {
            match self.rx.recv() {
                Ok(line) => (self.buf, self.pos) = ((line + "\n").into_bytes(), 0),
                Err(_) => return Ok(0),
            }
        }
        let n = out.len().min(self.buf.len() - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[derive(Clone, Default)]
struct Said(Arc<Mutex<Vec<u8>>>);

impl Write for Said {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Running {
    tx: Sender<String>,
    said: Said,
    thread: JoinHandle<i32>,
}

impl Running {
    fn start(settings: Value) -> Running {
        let (tx, rx) = channel();
        let said = Said::default();
        let host = Host::to_writer(said.clone());
        let thread = std::thread::spawn(move || trunk_recorder_plugin::serve::<ZelloStream>(BufReader::new(Lines { rx, buf: Vec::new(), pos: 0 }), host));
        let dir = testing::temp_dir("zello-stream");
        let r = Running { tx, said, thread };
        r.send(HostMessage::Hello(testing::hello(&dir, settings)));
        r
    }

    fn send(&self, m: HostMessage) {
        self.tx.send(serde_json::to_string(&m).unwrap()).unwrap();
    }

    fn messages(&self) -> Vec<Value> {
        let bytes = self.said.0.lock().unwrap().clone();
        String::from_utf8_lossy(&bytes).lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
    }

    /// The latest status, once it says `wanted` (waits up to `secs`).
    fn status_saying(&self, wanted: &str, secs: u64) -> (String, String) {
        let end = Instant::now() + Duration::from_secs(secs);
        loop {
            let last = self.messages().into_iter().rev().find(|m| m["type"] == "status");
            if let Some(m) = &last {
                let text = m["message"].as_str().unwrap_or_default().to_string();
                if text.contains(wanted) {
                    return (m["state"].as_str().unwrap_or_default().to_string(), text);
                }
            }
            assert!(Instant::now() < end, "status never said {wanted:?}; last: {last:?}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Stops it as the recorder does; everything it said, and its exit status.
    fn stop(self) -> (Vec<Value>, i32) {
        self.tx.send(r#"{"type":"shutdown","grace_s":5}"#.into()).unwrap();
        drop(self.tx);
        let code = self.thread.join().unwrap();
        let bytes = self.said.0.lock().unwrap().clone();
        let all = String::from_utf8_lossy(&bytes).lines().filter_map(|l| serde_json::from_str(l).ok()).collect();
        (all, code)
    }
}

fn call(talkgroup: u32, encrypted: bool) -> HostMessage {
    HostMessage::CallStart(CallInfo { short_name: "sys1".into(), talkgroup, encrypted, ..Default::default() })
}

#[test]
fn zello_work_connects_and_counts_transmissions() {
    let zello = FakeZello::start(|l| (welcome(l), false));
    let p = Running::start(work(&zello.url));
    let (state, text) = p.status_saying("connected, 3 online", 10);
    assert_eq!(state, "ok");
    assert!(text.starts_with("Test → Test Channel"), "{text}");
    p.send(call(101, false));
    p.send(call(101, false));
    p.send(call(102, false)); // another talkgroup
    p.send(call(101, true)); // encrypted
    p.status_saying("2 heard", 5);
    let (said, code) = p.stop();
    assert_eq!(code, 0);
    let logon = &zello.logons()[0];
    assert_eq!(logon["command"], "logon");
    assert_eq!(logon["username"], "gw-test");
    assert_eq!(logon["password"], PASSWORD);
    assert_eq!(logon["channels"], json!(["Test Channel"]));
    assert!(logon.get("auth_token").is_none(), "Zello Work takes no token");
    assert!(said.iter().any(|m| m["type"] == "metrics"), "reports metrics");
    assert_no_secrets(&said);
}

#[test]
fn consumer_zello_sends_a_token() {
    let zello = FakeZello::start(|l| (welcome(l), false));
    let p = Running::start(consumer(&zello.url));
    p.status_saying("connected", 10);
    let (said, _) = p.stop();
    let token = zello.logons()[0]["auth_token"].as_str().unwrap().to_string();
    assert_eq!(token.split('.').count(), 3);
    assert_no_secrets(&said);
    assert!(!said.iter().any(|m| m.to_string().contains(&token)), "the token isn't logged");
}

#[test]
fn a_wrong_password_is_an_error_that_says_so() {
    let zello = FakeZello::start(|_| (vec![json!({ "seq": 1, "error": "not authorized" })], true));
    let p = Running::start(work(&zello.url));
    let (state, text) = p.status_saying("refused the login", 10);
    assert_eq!(state, "error");
    assert!(text.contains("check the username and password"), "{text}");
    let (said, _) = p.stop();
    assert_no_secrets(&said);
}

#[test]
fn a_passworded_channel_says_to_remove_the_password() {
    let zello = FakeZello::start(|l| {
        let offline = json!({ "command": "on_channel_status", "channel": l["channels"][0], "status": "offline", "error": "invalid password" });
        (vec![json!({ "seq": 1, "success": true }), offline], false)
    });
    let p = Running::start(consumer(&zello.url));
    let (state, text) = p.status_saying("remove the channel's password", 10);
    assert_eq!(state, "error");
    p.stop();
    let _ = text;
}

#[test]
fn reconnects_when_zello_drops_it() {
    let zello = FakeZello::start(|l| (welcome(l), true)); // welcomes, then hangs up
    let p = Running::start(work(&zello.url));
    let end = Instant::now() + Duration::from_secs(10);
    while zello.logons().len() < 2 {
        assert!(Instant::now() < end, "it never logged in again");
        std::thread::sleep(Duration::from_millis(50));
    }
    p.stop();
}

#[test]
fn nobody_answering_is_a_warning() {
    // A port nothing listens on.
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let p = Running::start(work(&format!("ws://127.0.0.1:{port}/ws")));
    let (state, text) = p.status_saying("trying again", 10);
    assert_eq!(state, "warning");
    assert!(text.contains("can't reach"), "{text}");
    p.stop();
}

#[test]
fn bad_settings_stop_it_with_the_reason() {
    let p = Running::start(json!({ "service": "work", "network": "x", "streams": [] }));
    let (said, code) = p.stop();
    assert_eq!(code, trunk_recorder_plugin::EXIT_CONFIG);
    let status = said.iter().rev().find(|m| m["type"] == "status").unwrap();
    assert!(status["message"].as_str().unwrap().contains("Add a stream"));
}

/// Nothing the plugin said holds the password or the private key.
fn assert_no_secrets(said: &[Value]) {
    let key_part: String = TEST_PKCS8.split_whitespace().nth(3).unwrap().to_string();
    for m in said {
        let s = m.to_string();
        assert!(!s.contains(PASSWORD), "password in {s}");
        assert!(!s.contains(&key_part), "private key in {s}");
    }
}
