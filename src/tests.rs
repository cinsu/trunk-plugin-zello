//! Tests. The end-to-end ones run the plugin the way the recorder does, against
//! a stand-in Zello server on this computer: they never reach real Zello.

use super::*;
use crate::token::tests::{TEST_PKCS8, pem};
use serde_json::{Value, json};
use std::io::{BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::JoinHandle;
use trunk_recorder_plugin::{AudioChunk, HostMessage, testing};

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
    let m = trunk_recorder_plugin::describe::<TrProZello>();
    let m = serde_json::to_value(m).unwrap();
    let props = &m["config"]["properties"];
    assert_eq!(props["privateKey"]["x-secret"], true);
    assert_eq!(props["issuer"]["x-when"], json!({ "service": "consumer" }));
    assert_eq!(props["network"]["x-when"], json!({ "service": "work" }));
    let stream = &props["streams"]["items"]["properties"];
    assert_eq!(stream["password"]["x-secret"], true);
    assert_eq!(stream["system"]["x-system"], true);
    assert!(props.get("server").is_none(), "the test-only address isn't in the form");
    assert_eq!(m["id"], "tr-pro-zello");
}

// ---- a stand-in Zello ----

/// What the stand-in does with a logon: the messages to send back, and whether
/// to hang up afterwards.
type Behaviour = fn(&Value) -> (Vec<Value>, bool);

/// How the stand-in answers requests to talk.
#[derive(Clone, Copy, Default)]
struct Talk {
    /// Answer "channel busy" this many times first.
    busy: usize,
    /// Hang up after this many audio packets.
    hang_up_after: Option<usize>,
}

const STREAM_ID: u64 = 4242;

#[derive(Default)]
struct Heard {
    logons: Vec<Value>,
    commands: Vec<Value>,
    packets: Vec<(Instant, Vec<u8>)>,
}

struct FakeZello {
    url: String,
    heard: Arc<Mutex<Heard>>,
}

impl FakeZello {
    fn start(behaviour: Behaviour) -> FakeZello {
        FakeZello::talking(behaviour, Talk::default())
    }

    fn talking(behaviour: Behaviour, talk: Talk) -> FakeZello {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let heard = Arc::new(Mutex::new(Heard::default()));
        let log = heard.clone();
        std::thread::spawn(move || {
            for sock in listener.incoming().flatten() {
                let log = log.clone();
                std::thread::spawn(move || {
                    let Ok(mut ws) = tungstenite::accept(sock) else { return };
                    let Ok(tungstenite::Message::Text(t)) = ws.read() else { return };
                    let logon: Value = serde_json::from_str(&t).unwrap();
                    log.lock().unwrap().logons.push(logon.clone());
                    let (replies, hang_up) = behaviour(&logon);
                    for r in replies {
                        let _ = ws.send(tungstenite::Message::text(r.to_string()));
                    }
                    if hang_up {
                        let _ = ws.close(None);
                        let _ = ws.flush();
                        return;
                    }
                    let mut busy = talk.busy;
                    while let Ok(m) = ws.read() {
                        match m {
                            tungstenite::Message::Text(t) => {
                                let v: Value = serde_json::from_str(&t).unwrap();
                                log.lock().unwrap().commands.push(v.clone());
                                if v["command"] == "start_stream" {
                                    let reply = if busy > 0 {
                                        busy -= 1;
                                        json!({ "seq": v["seq"], "error": "channel busy" })
                                    } else {
                                        json!({ "seq": v["seq"], "success": true, "stream_id": STREAM_ID })
                                    };
                                    let _ = ws.send(tungstenite::Message::text(reply.to_string()));
                                }
                            }
                            tungstenite::Message::Binary(b) => {
                                let n = {
                                    let mut h = log.lock().unwrap();
                                    h.packets.push((Instant::now(), b.to_vec()));
                                    h.packets.len()
                                };
                                if talk.hang_up_after == Some(n) {
                                    return; // drops the connection
                                }
                            }
                            _ => {}
                        }
                    }
                });
            }
        });
        FakeZello { url, heard }
    }

    fn logons(&self) -> Vec<Value> {
        self.heard.lock().unwrap().logons.clone()
    }

    fn commands(&self, which: &str) -> Vec<Value> {
        self.heard.lock().unwrap().commands.iter().filter(|c| c["command"] == which).cloned().collect()
    }

    fn packets(&self) -> Vec<(Instant, Vec<u8>)> {
        self.heard.lock().unwrap().packets.clone()
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
        let thread = std::thread::spawn(move || trunk_recorder_plugin::serve::<TrProZello>(BufReader::new(Lines { rx, buf: Vec::new(), pos: 0 }), host));
        let dir = testing::temp_dir("tr-pro-zello");
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

fn call_numbered(id: u32, talkgroup: u32) -> CallInfo {
    CallInfo { id, short_name: "sys1".into(), talkgroup, recording: true, ..Default::default() }
}

/// `seconds` of a 1 kHz tone at 8 kHz, as the recorder sends live audio (20 ms chunks).
fn audio(id: u32, talkgroup: u32, seconds: f64) -> Vec<HostMessage> {
    let n = (8000.0 * seconds) as usize;
    let tone: Vec<i16> = (0..n).map(|i| (8000.0 * (2.0 * std::f64::consts::PI * 1000.0 * i as f64 / 8000.0).sin()) as i16).collect();
    tone.chunks(160)
        .map(|c| {
            let mut a = AudioChunk::new(id, 0, talkgroup, 8000, c);
            a.short_name = "sys1".into();
            HostMessage::Audio(a)
        })
        .collect()
}

fn concluded(id: u32, talkgroup: u32) -> HostMessage {
    let mut c = trunk_recorder_plugin::ConcludedCall { path: format!("sys1/2026/11/4/{talkgroup}-{id}"), ..Default::default() };
    c.call.call_num = u64::from(id);
    c.call.short_name = "sys1".into();
    c.call.talkgroup = talkgroup;
    HostMessage::CallConcluded(c)
}

/// A whole transmission, as the recorder tells it: start, audio, end, recording.
fn transmission(p: &Running, id: u32, talkgroup: u32, seconds: f64) {
    p.send(HostMessage::CallStart(call_numbered(id, talkgroup)));
    for m in audio(id, talkgroup, seconds) {
        p.send(m);
    }
    p.send(HostMessage::CallEnd(call_numbered(id, talkgroup)));
    p.send(concluded(id, talkgroup));
}

fn results(said: &[Value]) -> Vec<(String, String)> {
    said.iter()
        .filter(|m| m["type"] == "call.result")
        .map(|m| (m["path"].as_str().unwrap_or_default().to_string(), m["outcome"].as_str().unwrap_or_default().to_string()))
        .collect()
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

// ---- sending audio ----

#[test]
fn sends_a_transmission_to_zello_in_real_time() {
    let zello = FakeZello::talking(|l| (welcome(l), false), Talk::default());
    let p = Running::start(work(&zello.url));
    p.status_saying("connected", 10);
    transmission(&p, 7, 101, 1.0);
    p.status_saying("1 sent", 10);
    let (said, _) = p.stop();

    let start = &zello.commands("start_stream")[0];
    assert_eq!(start["channel"], "Test Channel");
    assert_eq!(start["codec"], "opus");
    assert_eq!(start["codec_header"], "gLsBFA==");
    assert_eq!(start["packet_duration"], 20);
    let stop = zello.commands("stop_stream");
    assert_eq!(stop.len(), 1);
    assert_eq!(stop[0]["stream_id"], STREAM_ID);

    let packets = zello.packets();
    // 1 s is 50 packets of 20 ms, plus the filter's short tail.
    assert!((50..=52).contains(&packets.len()), "{} packets", packets.len());
    for (_, b) in &packets {
        assert_eq!(b[0], 1, "audio packet");
        assert_eq!(u32::from_be_bytes([b[1], b[2], b[3], b[4]]), STREAM_ID as u32);
        assert_eq!(&b[5..9], &[0, 0, 0, 0], "packet id");
    }
    // The recorder sent it all at once; Zello got it no faster than real time.
    let spread = packets.last().unwrap().0 - packets[0].0;
    assert!(spread >= Duration::from_millis(900), "sent in {spread:?}");

    // It's still a 1 kHz tone once Zello's apps decode it.
    let mut err = 0;
    let dec = unsafe { unsafe_libopus::opus_decoder_create(48000, 1, &mut err) };
    let mut pcm = Vec::new();
    for (_, b) in &packets {
        let mut out = vec![0i16; 960];
        let n = unsafe { unsafe_libopus::opus_decode(dec, b[9..].as_ptr(), (b.len() - 9) as i32, out.as_mut_ptr(), 960, 0) };
        assert_eq!(n, 960);
        pcm.extend_from_slice(&out);
    }
    unsafe { unsafe_libopus::opus_decoder_destroy(dec) };
    let mid = &pcm[48000 / 4..48000 * 3 / 4];
    let (mut re, mut im) = (0.0f64, 0.0f64);
    for (i, &s) in mid.iter().enumerate() {
        let a = 2.0 * std::f64::consts::PI * 1000.0 * i as f64 / 48000.0;
        re += f64::from(s) * a.cos();
        im += f64::from(s) * a.sin();
    }
    let level = 2.0 * (re * re + im * im).sqrt() / mid.len() as f64;
    assert!((level - 8000.0).abs() < 2000.0, "1 kHz came through at {level:.0}");

    assert_eq!(results(&said), vec![("sys1/2026/11/4/101-7".to_string(), "ok".to_string())]);
    assert!(said.iter().any(|m| m["type"] == "metrics" && m["extra"]["transmissions sent"] == 1));
}

#[test]
fn waits_out_a_busy_channel() {
    let zello = FakeZello::talking(|l| (welcome(l), false), Talk { busy: 2, ..Default::default() });
    let p = Running::start(work(&zello.url));
    p.status_saying("connected", 10);
    transmission(&p, 8, 101, 0.3);
    p.status_saying("1 sent", 10);
    let (said, _) = p.stop();
    assert_eq!(zello.commands("start_stream").len(), 3, "asked again after each busy");
    assert_eq!(results(&said)[0].1, "ok");
}

#[test]
fn gives_up_on_a_channel_that_stays_busy() {
    let zello = FakeZello::talking(|l| (welcome(l), false), Talk { busy: 100, ..Default::default() });
    let p = Running::start(work(&zello.url));
    p.status_saying("connected", 10);
    transmission(&p, 9, 101, 0.3);
    let (state, text) = p.status_saying("1 failed", 15);
    assert_eq!(state, "ok", "the connection itself is fine: {text}");
    let (said, _) = p.stop();
    assert!(zello.packets().is_empty());
    assert_eq!(results(&said)[0].1, "failed");
    assert!(said.iter().any(|m| m["message"].as_str().is_some_and(|t| t.contains("stayed busy"))));
}

#[test]
fn only_its_own_talkgroup_and_nothing_encrypted() {
    let zello = FakeZello::talking(|l| (welcome(l), false), Talk::default());
    let p = Running::start(work(&zello.url));
    p.status_saying("connected", 10);
    transmission(&p, 10, 102, 0.3); // another talkgroup
    let mut enc = call_numbered(11, 101);
    enc.encrypted = true;
    p.send(HostMessage::CallStart(enc));
    std::thread::sleep(Duration::from_millis(600));
    let (said, _) = p.stop();
    assert!(zello.commands("start_stream").is_empty());
    assert!(results(&said).is_empty());
}

#[test]
fn a_dropped_connection_fails_the_transmission_and_reconnects() {
    let zello = FakeZello::talking(|l| (welcome(l), false), Talk { hang_up_after: Some(10), ..Default::default() });
    let p = Running::start(work(&zello.url));
    p.status_saying("connected", 10);
    transmission(&p, 12, 101, 1.0);
    p.status_saying("1 failed", 10);
    let end = Instant::now() + Duration::from_secs(10);
    while zello.logons().len() < 2 {
        assert!(Instant::now() < end, "it never logged in again");
        std::thread::sleep(Duration::from_millis(50));
    }
    let (said, _) = p.stop();
    assert_eq!(results(&said), vec![("sys1/2026/11/4/101-12".to_string(), "failed".to_string())]);
}

#[test]
fn two_transmissions_go_one_after_the_other() {
    let zello = FakeZello::talking(|l| (welcome(l), false), Talk::default());
    let p = Running::start(work(&zello.url));
    p.status_saying("connected", 10);
    transmission(&p, 20, 101, 0.4);
    transmission(&p, 21, 101, 0.4);
    p.status_saying("2 sent", 10);
    let (said, _) = p.stop();
    assert_eq!(zello.commands("start_stream").len(), 2);
    assert_eq!(zello.commands("stop_stream").len(), 2);
    assert_eq!(results(&said).len(), 2);
}

#[test]
fn live_audio_is_never_sent_faster_than_real_time() {
    let zello = FakeZello::talking(|l| (welcome(l), false), Talk::default());
    let p = Running::start(work(&zello.url));
    p.status_saying("connected", 10);
    p.send(HostMessage::CallStart(call_numbered(30, 101)));
    // As the recorder does on the air: 20 ms of audio every 20 ms, with a pause in the middle.
    for (i, m) in audio(30, 101, 1.0).into_iter().enumerate() {
        p.send(m);
        std::thread::sleep(Duration::from_millis(if i == 25 { 300 } else { 20 }));
    }
    p.send(HostMessage::CallEnd(call_numbered(30, 101)));
    p.status_saying("1 sent", 10);
    p.stop();
    let packets = zello.packets();
    assert!(packets.len() >= 50);
    // However the audio arrived, by any moment Zello never has more than real time's worth.
    let t0 = packets[0].0;
    for (n, (t, _)) in packets.iter().enumerate() {
        let allowed = (*t - t0).as_millis() as usize / 20 + 2;
        assert!(n < allowed, "packet {} arrived {:?} after the first: faster than real time", n + 1, *t - t0);
    }
}
