//! Astro relay server.
//!
//! This process holds NO files and NO chat history. It only does three things:
//!   1. Tracks who is online (presence) and which "channels" they're in.
//!   2. Relays chat text and WebRTC signaling (SDP/ICE) between connected peers.
//!   3. Forgets everything about a message the instant it's forwarded.
//!
//! Actual voice/video/screenshare media flows peer-to-peer over WebRTC once
//! signaling completes (falling back to a TURN server, not this one, if a
//! direct P2P path can't be established). File transfers happen the same way,
//! over a WebRTC data channel directly between the two clients, so there is
//! no size limit and nothing ever touches this server's disk.

use axum::{
    body::Bytes,
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use dashmap::{DashMap, DashSet};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::sync::mpsc;
use tower_http::services::ServeDir;
use uuid::Uuid;

#[derive(Clone)]
struct Peer {
    username: String,
    tx: mpsc::UnboundedSender<WsMessage>,
}

struct AppState {
    peers: DashMap<Uuid, Peer>,
    channels: DashMap<String, DashSet<Uuid>>,
    peer_channels: DashMap<Uuid, DashSet<String>>,
    /// Where update files (latest.json + installers) live on disk — served
    /// as-is at GET /update/*. This is the entire "auto-update host": no
    /// GitHub, no separate service, just static files this same process
    /// already serves, published via POST /publish/:filename.
    updates_dir: PathBuf,
    /// Required bearer token for /publish/:filename. Publishing is disabled
    /// entirely (not "open") if this isn't set — see main().
    upload_token: Option<String>,
}

type SharedState = Arc<AppState>;

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg {
    /// `user_id`, when present, is the client's own permanent local identity
    /// (see astro-client's `account` table) — reusing it instead of letting
    /// the relay assign a fresh random one each time is what makes friending
    /// and chat history mean anything across reconnects. NOTE: this is not
    /// authenticated — the relay trusts whatever ID a client claims. Fine
    /// for a hobby/friends server; add signature-based proof of ownership
    /// before this is ever exposed somewhere adversarial.
    Hello { username: String, #[serde(default)] user_id: Option<Uuid> },
    JoinChannel { channel_id: String },
    LeaveChannel { channel_id: String },
    /// The sender assigns this (a client-generated UUID), not the relay —
    /// that's what lets everyone who receives the message agree on what to
    /// call it, so a later edit/delete/reaction (which reference this same
    /// id) actually lands on the right message for every recipient instead
    /// of each client's local copy having its own, unrelated random id.
    Chat { channel_id: String, content: String, message_id: String },
    /// Edit/delete a message you authored — broadcast to every current
    /// member of the channel, the same as `Chat`. Like everything else
    /// here, the relay trusts the claim rather than verifying authorship.
    EditMessage { channel_id: String, message_id: String, content: String },
    DeleteMessage { channel_id: String, message_id: String },
    AddReaction { channel_id: String, message_id: String, emoji: String },
    RemoveReaction { channel_id: String, message_id: String, emoji: String },
    /// Opaque passthrough for WebRTC SDP offers/answers and ICE candidates
    /// (and now also friend-request / friend-accept / avatar payloads — the
    /// relay doesn't care what's inside, it just forwards to `to`).
    Signal { to: Uuid, payload: serde_json::Value },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMsg<'a> {
    Welcome { user_id: Uuid, peers: Vec<PeerInfo> },
    PeerJoined { user_id: Uuid, username: &'a str },
    PeerLeft { user_id: Uuid },
    ChannelJoined { channel_id: &'a str, members: Vec<Uuid> },
    Chat { message_id: &'a str, from: Uuid, channel_id: &'a str, content: &'a str, ts: i64 },
    MessageEdited { message_id: &'a str, channel_id: &'a str, content: &'a str },
    MessageDeleted { message_id: &'a str, channel_id: &'a str },
    ReactionAdded { message_id: &'a str, channel_id: &'a str, emoji: &'a str, user_id: Uuid },
    ReactionRemoved { message_id: &'a str, channel_id: &'a str, emoji: &'a str, user_id: Uuid },
    Signal { from: Uuid, payload: serde_json::Value },
    Error { message: &'a str },
}

#[derive(Serialize, Clone)]
struct PeerInfo {
    user_id: Uuid,
    username: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let updates_dir = std::env::var("UPDATES_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("updates"));
    std::fs::create_dir_all(&updates_dir).expect("could not create updates directory");
    tracing::info!("serving updates from {}", updates_dir.display());

    let upload_token = std::env::var("UPDATE_UPLOAD_TOKEN").ok();
    if upload_token.is_none() {
        tracing::warn!(
            "UPDATE_UPLOAD_TOKEN not set — POST /publish/:filename is disabled until it is"
        );
    }

    let state: SharedState = Arc::new(AppState {
        peers: DashMap::new(),
        channels: DashMap::new(),
        peer_channels: DashMap::new(),
        updates_dir: updates_dir.clone(),
        upload_token,
    });

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(|| async { "ok" }))
        // The entire update host: plain static files, same private URL your
        // app already talks to for chat. Nothing here is listed anywhere —
        // only reachable if you already know the relay's address.
        .nest_service("/update", ServeDir::new(updates_dir))
        .route("/publish/:filename", post(publish_update))
        .layer(axum::extract::DefaultBodyLimit::max(200 * 1024 * 1024))
        .with_state(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7878);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("astro-relay listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// Saves an uploaded release file (installer, .sig, or latest.json) to the
/// updates directory, from which it's immediately servable at
/// GET /update/<filename>. Requires `Authorization: Bearer <UPDATE_UPLOAD_TOKEN>`.
async fn publish_update(
    State(state): State<SharedState>,
    Path(filename): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let authorized = match &state.upload_token {
        Some(expected) => headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| v == format!("Bearer {expected}"))
            .unwrap_or(false),
        None => false,
    };
    if !authorized {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }

    // Bare filename only — no path traversal into the rest of the disk.
    if filename.contains('/') || filename.contains('\\') || filename.contains("..") {
        return (StatusCode::BAD_REQUEST, "invalid filename").into_response();
    }

    let path = state.updates_dir.join(&filename);
    match tokio::fs::write(&path, &body).await {
        Ok(()) => {
            eprintln!("[astro-relay] published update file: {filename} ({} bytes)", body.len());
            (StatusCode::OK, "ok").into_response()
        }
        Err(e) => {
            eprintln!("[astro-relay] failed to write update file {filename}: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "failed to save file").into_response()
        }
    }
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<SharedState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: SharedState) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<WsMessage>();

    // Pumps anything queued for this peer out over the real socket.
    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    let mut user_id = Uuid::new_v4();
    let mut username = format!("user-{}", &user_id.to_string()[..8]);
    let mut registered = false;

    while let Some(Ok(msg)) = ws_rx.next().await {
        let WsMessage::Text(text) = msg else {
            if matches!(msg, WsMessage::Close(_)) {
                break;
            }
            continue;
        };

        let Ok(client_msg) = serde_json::from_str::<ClientMsg>(&text) else {
            send_err(&tx, "could not parse message");
            continue;
        };

        match client_msg {
            ClientMsg::Hello { username: name, user_id: persistent_id } => {
                username = name;
                if let Some(id) = persistent_id {
                    user_id = id;
                }
                let peers: Vec<PeerInfo> = state
                    .peers
                    .iter()
                    .map(|e| PeerInfo {
                        user_id: *e.key(),
                        username: e.value().username.clone(),
                    })
                    .collect();

                state.peers.insert(
                    user_id,
                    Peer {
                        username: username.clone(),
                        tx: tx.clone(),
                    },
                );
                state.peer_channels.insert(user_id, DashSet::new());
                registered = true;

                let _ = send_json(&tx, &ServerMsg::Welcome { user_id, peers });
                broadcast_all(
                    &state,
                    &ServerMsg::PeerJoined { user_id, username: &username },
                    Some(user_id),
                );
            }
            _ if !registered => {
                send_err(&tx, "send hello before anything else");
            }
            ClientMsg::JoinChannel { channel_id } => {
                let members = state.channels.entry(channel_id.clone()).or_default();
                members.insert(user_id);
                if let Some(set) = state.peer_channels.get(&user_id) {
                    set.insert(channel_id.clone());
                }
                let member_ids: Vec<Uuid> = members.iter().map(|m| *m).collect();
                let _ = send_json(
                    &tx,
                    &ServerMsg::ChannelJoined { channel_id: &channel_id, members: member_ids },
                );
            }
            ClientMsg::LeaveChannel { channel_id } => {
                if let Some(members) = state.channels.get(&channel_id) {
                    members.remove(&user_id);
                }
                if let Some(set) = state.peer_channels.get(&user_id) {
                    set.remove(&channel_id);
                }
            }
            ClientMsg::Chat { channel_id, content, message_id } => {
                if let Some(members) = state.channels.get(&channel_id) {
                    let ts = chrono::Utc::now().timestamp_millis();
                    let out = ServerMsg::Chat {
                        message_id: &message_id,
                        from: user_id,
                        channel_id: &channel_id,
                        content: &content,
                        ts,
                    };
                    let payload = serde_json::to_string(&out).unwrap();
                    for member in members.iter() {
                        if let Some(peer) = state.peers.get(&member) {
                            let _ = peer.tx.send(WsMessage::Text(payload.clone()));
                        }
                    }
                } else {
                    send_err(&tx, "not a member of that channel");
                }
            }
            ClientMsg::EditMessage { channel_id, message_id, content } => {
                if let Some(members) = state.channels.get(&channel_id) {
                    let out = ServerMsg::MessageEdited {
                        message_id: &message_id,
                        channel_id: &channel_id,
                        content: &content,
                    };
                    let payload = serde_json::to_string(&out).unwrap();
                    for member in members.iter() {
                        if let Some(peer) = state.peers.get(&member) {
                            let _ = peer.tx.send(WsMessage::Text(payload.clone()));
                        }
                    }
                }
            }
            ClientMsg::DeleteMessage { channel_id, message_id } => {
                if let Some(members) = state.channels.get(&channel_id) {
                    let out = ServerMsg::MessageDeleted {
                        message_id: &message_id,
                        channel_id: &channel_id,
                    };
                    let payload = serde_json::to_string(&out).unwrap();
                    for member in members.iter() {
                        if let Some(peer) = state.peers.get(&member) {
                            let _ = peer.tx.send(WsMessage::Text(payload.clone()));
                        }
                    }
                }
            }
            ClientMsg::AddReaction { channel_id, message_id, emoji } => {
                if let Some(members) = state.channels.get(&channel_id) {
                    let out = ServerMsg::ReactionAdded {
                        message_id: &message_id,
                        channel_id: &channel_id,
                        emoji: &emoji,
                        user_id,
                    };
                    let payload = serde_json::to_string(&out).unwrap();
                    for member in members.iter() {
                        if let Some(peer) = state.peers.get(&member) {
                            let _ = peer.tx.send(WsMessage::Text(payload.clone()));
                        }
                    }
                }
            }
            ClientMsg::RemoveReaction { channel_id, message_id, emoji } => {
                if let Some(members) = state.channels.get(&channel_id) {
                    let out = ServerMsg::ReactionRemoved {
                        message_id: &message_id,
                        channel_id: &channel_id,
                        emoji: &emoji,
                        user_id,
                    };
                    let payload = serde_json::to_string(&out).unwrap();
                    for member in members.iter() {
                        if let Some(peer) = state.peers.get(&member) {
                            let _ = peer.tx.send(WsMessage::Text(payload.clone()));
                        }
                    }
                }
            }
            ClientMsg::Signal { to, payload } => {
                if let Some(peer) = state.peers.get(&to) {
                    let _ = send_json(&peer.tx, &ServerMsg::Signal { from: user_id, payload });
                } else {
                    send_err(&tx, "target peer not connected");
                }
            }
        }
    }

    // Disconnect cleanup — nothing persists past this point.
    state.peers.remove(&user_id);
    if let Some((_, chans)) = state.peer_channels.remove(&user_id) {
        for chan in chans.iter() {
            if let Some(members) = state.channels.get(chan.key()) {
                members.remove(&user_id);
            }
        }
    }
    if registered {
        broadcast_all(&state, &ServerMsg::PeerLeft { user_id }, None);
    }

    send_task.abort();
}

fn send_json<T: Serialize>(tx: &mpsc::UnboundedSender<WsMessage>, msg: &T) -> Result<(), ()> {
    let text = serde_json::to_string(msg).map_err(|_| ())?;
    tx.send(WsMessage::Text(text)).map_err(|_| ())
}

fn send_err(tx: &mpsc::UnboundedSender<WsMessage>, message: &str) {
    let _ = send_json(tx, &ServerMsg::Error { message });
}

fn broadcast_all(state: &SharedState, msg: &ServerMsg, exclude: Option<Uuid>) {
    let text = serde_json::to_string(msg).unwrap();
    for entry in state.peers.iter() {
        if Some(*entry.key()) != exclude {
            let _ = entry.value().tx.send(WsMessage::Text(text.clone()));
        }
    }
}
