# Changelog

## [0.2.0]

The first release.

- Streams live radio audio to Zello or Zello Work channels: each stream sends a system's talkgroup (or all of
  the system, or every system) to a channel, a transmission at a time, as 48 kHz Opus.
- Page alerts: posts "🔔 Page for <name>" to Zello when the recorder hears a page you've set up, from a
  **Zello alert** button on each signal in Setup, with **Send a test** on each alert.
- Max wait per stream: a transmission that has waited longer than this for a busy channel is dropped rather
  than sent late; one that gets its turn is sent whole, from its start.
- Keepalive pings every 20 s, a dead connection replaced before anything needs sending, and a transmission cut
  off by a lost connection sent again once reconnected.
- An On switch on every stream, so streams can be set up ahead of time and turned on when ready. The stream
  list shows one line per stream; open one to edit it.
- Encrypted calls are never sent. The private key and passwords are never written to the log.
