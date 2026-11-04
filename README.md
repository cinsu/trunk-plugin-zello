# Zello for Trunk Recorder Pro

A [Trunk Recorder Pro](https://github.com/TrunkRecorder/trunk-recorder-pro) plugin that streams live
radio audio to Zello channels, on consumer Zello or Zello Work.

A community plugin: not made by, endorsed by or affiliated with Zello.

**Work in progress**, not released yet.

Each stream logs a Zello account on to a channel and sends a system's talkgroup (or all of the
system) to it live: each transmission as it's heard, one at a time, as 48 kHz Opus. When the
channel is busy it waits and asks again; a transmission that can't go out within 10 seconds is
dropped rather than sent late. The plugin's card shows each stream's connection and what it sent
or failed to send, and each recorded call shows whether it went to Zello.

Encrypted calls are never sent.

**Page alerts:** when the recorder hears a page it knows (one of a system's signals), the plugin can
post "🔔 Page for <name>" to the stream's channel, and to other channels with an alert account
(Zello Work lets one account join several; consumer Zello, one). Press **Zello alert** on a signal
in Setup to add one, and **Send a test** on an alert to see it posted. The log says what happened to
every alert: posted, refused (and why), or not answered.

**Max wait:** each stream drops a transmission that has waited longer than this (10 s unless you
change it) for its turn, rather than send it late. The log says which talkgroup it waited behind.

## What it needs

- **Consumer Zello:** an Issuer and Private Key from [developers.zello.com](https://developers.zello.com)
  (Keys), and for each stream a Zello account and a channel it can talk on. The Channel API can't
  join a channel that has a password.
- **Zello Work:** your network's name, and for each stream an account on it that can talk on its channel.

Give each stream its own Zello account: Zello keeps one connection per account.

## Settings

| Setting | |
|---|---|
| Zello service | Zello (the free app) or Zello Work |
| Issuer, Private key | Consumer Zello only |
| Network | Zello Work only |
| Streams | Each: a name, the system, a talkgroup (empty: all of the system), the Zello channel, username and password |
| Page alerts | Each: a name, the pages (signals) it's for, whether it posts on the stream's channel, and other channels to post to |
| Alert account | The username and password that post alerts to other channels |

The private key and passwords are kept in the recorder's settings and never written to its log.

## What it sends where

To Zello only (zello.io, or zellowork.io for Zello Work): each stream's login, with a short-lived
token signed with your Private Key for consumer Zello, the audio of the transmissions on that
stream's talkgroup, and the page alerts you set up. Nothing else, and nowhere else.

Mind what you rebroadcast: a consumer Zello channel can be public, and some agencies don't allow
their traffic to be streamed.

## Building

```sh
cargo build
cargo test
./target/debug/tr-pro-zello --describe
```

## See also

[zellostream](https://github.com/aaknitt/zellostream), a Python script that streams audio to Zello,
including from the original trunk-recorder through its simplestream plugin.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
