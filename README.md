# Zello Stream

A [Trunk Recorder Pro](https://github.com/TrunkRecorder/trunk-recorder-pro) plugin that streams live
radio audio to Zello channels, on consumer Zello or Zello Work.

**Work in progress.** It doesn't stream yet: this is the starting point, built from the
[plugin template](https://github.com/TrunkRecorder/trunk-plugin-template).

## Planned

- Live audio of chosen talkgroups to Zello channels, a transmission at a time
- Consumer Zello (Issuer and Private Key) and Zello Work (network, username and password)
- Page alerts from the recorder's signals, with extra channels per signal
- Status in the recorder's plugin list: streams connected, transmissions sent, retries and failures

## Building

```sh
cargo build
cargo test
./target/debug/zello-stream --describe
```

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
