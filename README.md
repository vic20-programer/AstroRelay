# Astro — architecture & relay server (v0.1)

Compiles clean on `cargo build` (verified against rustc 1.75).

## The core idea

Nothing lives on a server. The relay's only job is to introduce peers to each
other and pass small bits of text between them, then forget about it.

```
        ┌────────────┐        WebSocket (signaling + chat text)
        │ astro-relay │◄──────────────────────────────┐
        └────────────┘                                │
              │  presence / join-channel / signal      │
              ▼                                        ▼
   ┌────────────────┐   WebRTC (data + audio + video)  ┌────────────────┐
   │  Client A       │◄─────────────────────────────────►│  Client B      │
   │  (Tauri app)    │  direct P2P, or via TURN relay    │  (Tauri app)   │
   │  SQLite: history│  if NAT blocks direct connection  │  SQLite: history│
   │  local files    │                                    │  local files   │
   └────────────────┘                                    └────────────────┘
```

- **astro-relay** (this repo): Rust + Axum, one WebSocket route. Tracks who's
  online, who's in which "channel," and relays two kinds of messages:
  chat text (delivered live to everyone in the channel) and WebRTC signaling
  payloads (SDP offers/answers, ICE candidates) passed straight through to a
  named peer. It stores nothing on disk — restart it and all state is gone,
  by design.
- **Voice / video / screenshare**: standard WebRTC. Once two clients swap SDP
  through the relay, media flows directly between them. This is exactly your
  SUPERHOT VR progression (raw UDP → Steam relay → WebSocket relay): start
  with the relay doing signaling, let STUN find a direct path when possible,
  and fall back to a TURN relay (deploy `coturn` separately) only when a
  direct path isn't reachable. TURN relays live media in real time — it
  still never touches a disk.
- **Files**: sent over a WebRTC `RTCDataChannel` straight to the recipient,
  chunked. No size cap because the relay server never buffers or stores the
  bytes — it's not in the transfer path at all once signaling completes.
- **Everything else** (message history, server/channel structure, downloaded
  files) lives in a local SQLite database inside each client's app data
  folder. The relay has no database.

## Why this stack

- **Client: Tauri + Rust backend, web-based UI (React or Svelte).** Tauri's
  webview is a real browser engine (WebView2 on Windows, WebKit on
  Linux/macOS), so the UI layer gets native `RTCPeerConnection` and
  `RTCDataChannel` APIs for free — no need to vendor a WebRTC stack in Rust.
  Rust side handles the SQLite store, file I/O, and OS integration
  (notifications, tray icon, auto-update). Much smaller and snappier than
  Electron, and matches the low-level style of your other projects.
- **Relay: Rust + Axum.** Single static binary, trivial to self-host on a
  $5 VPS or even a Pi, handles thousands of idle WebSocket connections
  without breaking a sweat.
- **TURN: coturn.** Don't write this yourself — it's a solved problem and
  coturn is what Discord, Zoom knockoffs, etc. actually run.

## Running it

```bash
cargo run
# listens on ws://0.0.0.0:7878/ws (or $PORT if set — see Deploying to Render)
```

## Deploying to Render

The relay binds to `$PORT` if it's set (falls back to 7878 locally), which is
what Render's platform requires — that's the only code change this needed.

Two ways to deploy, pick one:

1. **Blueprint (`render.yaml`), least clicking:** push this repo to GitHub,
   then in the Render dashboard use **New → Blueprint** and point it at the
   repo. Render reads `render.yaml`, builds the included `Dockerfile`, and
   stands up the service on the free plan with the `/health` check wired in.
2. **Manual web service:** **New → Web Service** → connect the repo → set
   **Runtime** to `Docker` (it'll auto-detect the `Dockerfile`) → **Create
   Web Service**. Same result, no `render.yaml` needed.

Either way:
- Render gives you a URL like `astro-relay-xxxx.onrender.com`. WebSocket
  clients (the Tauri app) should connect to
  `wss://astro-relay-xxxx.onrender.com/ws` — **`wss://`, not `ws://`**,
  since Render terminates TLS for you.
- Free-plan services spin down after ~15 minutes idle and take a few seconds
  to wake back up on the next connection — fine for a hobby/friends server,
  worth knowing so a "why won't it connect" moment doesn't surprise you.
  A paid instance removes that.
- Nothing about "everything stored on your computer" changes: this service
  still has no database and no disk-backed state, on Render or anywhere
  else.

## Deploying to Northflank (no sleep, still free)

Render's free tier sleeps after 15 minutes idle; Northflank's free "Sandbox"
plan doesn't — same Dockerfile, no code changes beyond what's already here.

1. Push this repo to GitHub (skip if it's already there from the Render setup).
2. In Northflank: **New Project** → **New Service** → connect this repo →
   build type **Dockerfile**.
3. Northflank auto-detects the port from the `EXPOSE 7878` line in the
   Dockerfile and creates a public port for it — just confirm the protocol
   is set to **HTTP** (WebSocket rides on top of an HTTP/1.1 upgrade, so
   this is correct, not TCP). If it doesn't auto-detect, add the port
   manually: `7878`, protocol `HTTP`, public.
4. Deploy. Northflank gives you a `*.code.run` URL (or attach your own
   domain) with a TLS cert already issued — connect with `wss://`, same as
   the Render instructions above.
5. Northflank may ask for a card the first time to confirm you're not a
   bot; the Sandbox plan itself stays $0, and there's no sleep/cold-start
   behavior to work around here.

Unlike Render, Northflank doesn't inject a `$PORT` env var for you — that's
why the Dockerfile's `EXPOSE 7878` matters here specifically: it's what
lets Northflank find the right port automatically, using `main.rs`'s own
7878 fallback rather than an injected one.


## Wire protocol (JSON over the WebSocket)

Client → server:
```json
{"type": "hello", "username": "Mikey"}
{"type": "join_channel", "channel_id": "general"}
{"type": "chat", "channel_id": "general", "content": "hey"}
{"type": "edit_message", "channel_id": "general", "message_id": "<id>", "content": "hey there"}
{"type": "delete_message", "channel_id": "general", "message_id": "<id>"}
{"type": "signal", "to": "<peer-uuid>", "payload": { "sdp": "...", "kind": "offer" }}
```

Server → client:
```json
{"type": "welcome", "user_id": "...", "peers": [...]}
{"type": "peer_joined", "user_id": "...", "username": "..."}
{"type": "chat", "from": "...", "channel_id": "general", "content": "hey", "ts": 1234567890}
{"type": "message_edited", "message_id": "<id>", "channel_id": "general", "content": "hey there"}
{"type": "message_deleted", "message_id": "<id>", "channel_id": "general"}
{"type": "signal", "from": "...", "payload": {...}}
```

`edit_message`/`delete_message` broadcast to every current member of the
channel, same as `chat` — the relay doesn't verify you actually authored
the message you're editing, consistent with the rest of the trust model
here (see the `Hello` doc comment in `main.rs`).

`signal` payloads are opaque JSON — the relay never inspects them, it just
forwards to the named peer. That's where your WebRTC offer/answer/ICE
candidate exchange rides.

## Suggested build order from here

1. **Tauri shell** — bare window + the WebSocket client talking to this relay
   (login/hello, presence list, a single text channel).
2. **SQLite schema** — servers, channels, messages, cached files — everything
   that makes Astro feel persistent even though the relay isn't.
3. **WebRTC voice** — one-to-one call using the `signal` passthrough above,
   then extend to a mesh for group voice channels (fine up to ~6-8 people;
   beyond that you'd eventually want an SFU, but that's a v2 problem).
4. **File transfer over DataChannel** — chunked send/receive with a progress
   UI; this is where "infinite file size" actually comes from.
5. **coturn** deployment for the NAT-fallback case.
6. Video + screenshare reuse the same WebRTC plumbing as voice, just add a
   video/display-capture track.

Want me to scaffold the Tauri client next (steps 1–2)?
