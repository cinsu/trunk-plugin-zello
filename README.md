# Zello for Trunk Recorder Pro

A [Trunk Recorder Pro](https://github.com/TrunkRecorder/trunk-recorder-pro) plugin that streams live
radio audio to Zello channels, on consumer Zello or Zello Work.

A community plugin: not made by, endorsed by or affiliated with Zello.

**Work in progress.** It logs each stream's Zello account on to its channel, keeps it connected and
shows how each is doing, and counts the transmissions it will send. It doesn't send audio yet.

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

The private key and passwords are kept in the recorder's settings and never written to its log.

## What it sends where

To Zello only (zello.io, or zellowork.io for Zello Work): each stream's login, with a short-lived
token signed with your Private Key for consumer Zello. Nothing else, and nowhere else.

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
