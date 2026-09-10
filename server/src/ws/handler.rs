use std::{collections::HashSet, time::{Duration, Instant}};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use uuid::Uuid;

const MUSIC_BOT_ID: Uuid = Uuid::from_u128(1);

/// How often the server pings an idle socket, and how long it tolerates
/// silence before treating the connection as dead. Without this a client
/// that vanishes uncleanly (Wi-Fi drop, laptop sleep, killed process) would
/// sit in `conns` — and stay "online" for everyone — until the OS TCP
/// keepalive eventually reaped it (hours). The .NET/browser client answers
/// WebSocket Pings at the transport layer, so a live-but-idle client keeps
/// itself marked online for free.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

use crate::{
    auth::authenticate_token,
    db,
    state::AppState,
    ws::{
        call_registry::CallOpError,
        hub::ConnMeta,
        protocol::*,
        voice_metrics::VoiceMetrics,
        voice_registry::{HintError, RoomChange, TrackSource},
    },
};

pub async fn ws_upgrade(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    // Forward outbound queue -> actual socket sink. Keeps the socket write
    // half owned by exactly one task, avoiding interleaved writes.
    let forward_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // WS-FR-001: client must send auth.hello within 10s of connecting.
    let hello = tokio::time::timeout(Duration::from_secs(10), receiver.next()).await;
    let authenticated = match hello {
        Ok(Some(Ok(Message::Text(text)))) => match serde_json::from_str::<InboundEnvelope>(&text) {
            Ok(env) if env.op == "auth.hello" => match serde_json::from_value::<AuthHello>(env.data) {
                Ok(hello) => {
                    // `client_version` / `client_platform` are client-controlled
                    // and flow into logs and the debug endpoint — truncate
                    // before storing (SPEC-001 §5, §6.4).
                    // The v2 dialect can be switched off at runtime
                    // (TUPI_VOICE_PROTOCOL_V2=false), which caps negotiation at 1.
                    let server_max = if state.config.voice_protocol_v2 { MAX_SERVER_PROTOCOL } else { 1 };
                    let meta = ConnMeta {
                        protocol_version: hello.protocol_version.min(server_max),
                        client_version: truncate_chars(
                            hello.client_version.as_deref().unwrap_or("unknown"),
                            64,
                        ),
                        client_platform: truncate_chars(
                            hello.client_platform.as_deref().unwrap_or("unknown"),
                            32,
                        ),
                        connected_at: chrono::Utc::now(),
                    };
                    if hello.token == state.config.music_bot_token {
                        Some((music_bot_user(), Uuid::nil(), meta))
                    } else {
                        match authenticate_token(&state.pool, &hello.token).await {
                            Ok((u, s)) => Some((u, s, meta)),
                            Err(error) => {
                                if !matches!(error, crate::error::AppError::Unauthorized) {
                                    tracing::error!(%error, "database error during ws handshake");
                                }
                                None
                            }
                        }
                    }
                }
                Err(_) => None,
            },
            _ => None,
        },
        _ => None,
    };

    let Some((user, _session_id, conn_meta)) = authenticated else {
        let _ = tx.send(Message::Text(
            serde_json::to_string(&OutboundEnvelope::new(
                "auth.rejected",
                serde_json::json!({ "reason": "invalid_or_missing_token" }),
            ))
            .unwrap(),
        ));
        let _ = tx.send(Message::Close(None));
        drop(tx);
        let _ = forward_task.await;
        return;
    };

    let user_id = user.id;
    tracing::info!(
        %user_id,
        protocol_version = conn_meta.protocol_version,
        client_version = %conn_meta.client_version,
        client_platform = %conn_meta.client_platform,
        "ws connected"
    );

    let was_online = state.hub.is_online(user_id).await;
    let connection_id = state.hub.register(user_id, tx.clone(), conn_meta.clone()).await;
    state.advance_presence_epoch(user_id).await;
    let cancelled_offline_grace = state.cancel_offline_grace(user_id).await;

    state
        .hub
        .send_to_connection(
            user_id,
            connection_id,
            OutboundEnvelope::new(
                "auth.ok",
                AuthOk {
                    user_id,
                    username: user.username.clone(),
                    display_name: user.display_name.clone(),
                    livekit_url: state.config.livekit_url.clone(),
                    protocol_version: conn_meta.protocol_version,
                    server_version: crate::SERVER_VERSION.to_string(),
                    features: crate::ws::server_features(&state.config),
                },
            ),
        )
        .await;

    let members = match db::related_member_ids(&state.pool, user_id).await {
        Ok(members) => members,
        Err(error) => {
            tracing::error!(%user_id, %error, "failed to resolve presence snapshot members");
            vec![user_id]
        }
    };
    let mut entries = Vec::with_capacity(members.len());
    for member_id in &members {
        entries.push(PresenceEntry {
            user_id: *member_id,
            status: state.hub.status_for(*member_id).await,
        });
    }
    let snapshot = PresenceSnapshot { users: entries };
    state
        .hub
        .send_to_connection(user_id, connection_id, OutboundEnvelope::new("presence.snapshot", snapshot))
        .await;
    // ACT-FR-005: right after presence.snapshot, hand this connection the
    // current activity of every member who has any (absent = empty list),
    // each game enriched with its lifetime playtime (ACT-FR-032).
    let mut activity_users = state.hub.activities.read().await.snapshot_for(&members);
    for entry in activity_users.iter_mut() {
        enrich_playtime(&state, entry.user_id, &mut entry.activities).await;
    }
    state
        .hub
        .send_to_connection(
            user_id,
            connection_id,
            OutboundEnvelope::new("activity.snapshot", ActivitySnapshot { users: activity_users }),
        )
        .await;
    send_voice_room_state(&state, user_id, connection_id, &[]).await;
    if !was_online && !cancelled_offline_grace {
        broadcast_presence_update(&state, user_id, "online").await;
    }

    // Voice channels this connection is an active participant of. Bound to the
    // socket lifetime: a clean `voice.presence.leave`, a channel switch, or the
    // disconnect path below all evict the user from the community voice roster
    // immediately, instead of waiting on LiveKit's (laggy, best-effort)
    // `participant_left` webhook.
    let mut joined_calls: HashSet<Uuid> = HashSet::new();

    // Application-level heartbeat: any inbound frame (including the Pong the
    // client sends in reply to our Ping) refreshes `last_seen`; if nothing
    // arrives for HEARTBEAT_TIMEOUT we drop the socket so the disconnect
    // path below runs and the user goes offline.
    let mut last_seen = Instant::now();
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            incoming = receiver.next() => {
                let Some(msg) = incoming else { break };
                let msg = match msg {
                    Ok(m) => m,
                    Err(_) => break,
                };
                last_seen = Instant::now();
                match msg {
                    Message::Text(text) => {
                        dispatch(&state, user_id, connection_id, &text, &mut joined_calls).await;
                    }
                    Message::Ping(payload) => {
                        let _ = state
                            .hub
                            .send_to_raw(user_id, Message::Pong(payload))
                            .await;
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = heartbeat.tick() => {
                if last_seen.elapsed() > HEARTBEAT_TIMEOUT {
                    tracing::info!(%user_id, "ws heartbeat timeout; dropping dead connection");
                    break;
                }
                if tx.send(Message::Ping(Vec::new())).is_err() {
                    break;
                }
            }
        }
    }

    // Connection ended: unwind presence + any calls this user was in.
    let was_last_connection = state.hub.unregister(user_id, connection_id).await;
    if !was_last_connection {
        tracing::info!(%user_id, "ws disconnected; another connection remains active");
        let _ = forward_task.await;
        return;
    }
    // INV-A3: the WebSocket is NOT the media transport. Losing it must never
    // evict anyone from voice. Instead, when the last socket goes, schedule a
    // reconcile of each channel this connection had announced — if the person
    // really left LiveKit it removes them, and if they are still in the room
    // (the common blip/deploy case) nothing happens.
    let left_calls: Vec<Uuid> = joined_calls.iter().copied().collect();
    let disconnect_epoch = state.advance_presence_epoch(user_id).await;
    state.begin_offline_grace(user_id).await;
    let delayed_state = state.clone();
    tokio::spawn(async move {
        for channel_id in &left_calls {
            delayed_state.schedule_reconcile(*channel_id, Duration::from_secs(5)).await;
        }
        // Grace window so a page refresh / brief network blip doesn't flap the
        // member list. A genuine disconnect still clears well inside this once
        // the heartbeat above has torn the dead socket down.
        tokio::time::sleep(Duration::from_secs(delayed_state.config.ws_offline_grace_seconds)).await;
        if delayed_state.hub.is_online(user_id).await
            || !delayed_state.presence_epoch_is_current(user_id, disconnect_epoch).await
        {
            return;
        }
        if !delayed_state.finish_offline_grace(user_id).await {
            return;
        }
        // ACT-FR-006: clear this user's activity in the same grace window,
        // and close any playtime rows left open (ACT-FR-031).
        if delayed_state.hub.activities.write().await.clear(user_id) {
            broadcast_activity_update(&delayed_state, user_id, Vec::new()).await;
        }
        // Drop any "busy" override so a later reconnect comes back as plain
        // online; the "offline" broadcast below already covers the visible state.
        delayed_state.hub.clear_status(user_id).await;
        if let Err(error) = db::close_all_game_sessions(&delayed_state.pool, user_id).await {
            tracing::warn!(%user_id, %error, "failed to close game sessions on disconnect");
        }
        broadcast_presence_update(&delayed_state, user_id, "offline").await;
        tracing::info!(%user_id, "ws disconnect grace period elapsed");
    });
    tracing::info!(%user_id, "ws disconnected; grace period started");
    let _ = forward_task.await;
}

/// Keeps at most `max` Unicode scalar values of `s`, on a char boundary.
/// Applied to the client-controlled `client_version` / `client_platform`
/// before they reach logs or the debug endpoint (SPEC-001 §5).
fn truncate_chars(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((idx, _)) => s[..idx].to_string(),
        None => s.to_string(),
    }
}

fn music_bot_user() -> db::User {
    db::User {
        id: MUSIC_BOT_ID, username: "tupi-musica".into(), display_name: "Tupi Música".into(),
        password_hash: String::new(), avatar_color: Some("#5865f2".into()), avatar_storage_path: None,
        avatar_content_type: None, profile_tag: Some("BOT".into()), profile_badge_storage_path: None,
        profile_badge_content_type: None, name_color: Some("#5865f2".into()),
        bio: Some("Bot de música oficial do Tupi. Toque qualquer música ou rádio usando os controles de voz.".into()),
        banner_preset: Some("synthwave".into()), pronouns: Some("ele/bot".into()),
        created_at: chrono::Utc::now(),
    }
}
async fn broadcast_presence_update(state: &AppState, user_id: Uuid, status: &str) {
    match db::related_member_ids(&state.pool, user_id).await {
        Ok(user_ids) => state.hub.broadcast_to(
            &user_ids,
            OutboundEnvelope::new(
                "presence.update",
                PresenceUpdate { user_id, status: status.to_string() },
            ),
        ).await,
        Err(error) => tracing::error!(%user_id, %error, "failed to resolve presence recipients"),
    }
}

async fn broadcast_activity_update(state: &AppState, user_id: Uuid, activities: Vec<Activity>) {
    match db::related_member_ids(&state.pool, user_id).await {
        Ok(user_ids) => {
            state
                .hub
                .broadcast_to(
                    &user_ids,
                    OutboundEnvelope::new("activity.update", ActivityUpdate { user_id, activities }),
                )
                .await
        }
        Err(error) => tracing::error!(%user_id, %error, "failed to resolve activity recipients"),
    }
}

/// ACT-FR-001/003/004: fire-and-forget rich-presence report. Sanitize the
/// client's list, replace what we hold for this user, and broadcast to the
/// community (including the sender, so multi-device clients converge) only
/// when the sanitized state actually changed. On a change we also reconcile
/// the persistent playtime ledger (ACT-FR-031) and enrich each game with its
/// lifetime aggregates before broadcasting (ACT-FR-032).
async fn handle_activity_report(state: &AppState, user_id: Uuid, data: ActivityReport) {
    let (mut activities, dropped) = crate::ws::activity::ActivityRegistry::sanitize(data.activities);
    if dropped > 0 {
        tracing::warn!(%user_id, dropped, "activity.report had items dropped by validation");
    }
    let previous = state.hub.activities.read().await.get(user_id);
    let changed = state.hub.activities.write().await.set(user_id, activities.clone());
    if !changed {
        return;
    }
    reconcile_game_sessions(state, user_id, &previous, &activities).await;
    enrich_playtime(state, user_id, &mut activities).await;
    tracing::debug!(%user_id, count = activities.len(), "activity.update");
    broadcast_activity_update(state, user_id, activities).await;
}

/// Open a playtime row for each game that just appeared in the user's report
/// and close the row for each one that just disappeared (ACT-FR-031).
async fn reconcile_game_sessions(
    state: &AppState,
    user_id: Uuid,
    previous: &[Activity],
    current: &[Activity],
) {
    use crate::ws::activity::game_key;
    use std::collections::HashMap;
    let prev: HashMap<String, ()> =
        previous.iter().filter_map(|a| game_key(a).map(|k| (k, ()))).collect();
    let curr: HashMap<String, String> =
        current.iter().filter_map(|a| game_key(a).map(|k| (k, a.name.clone()))).collect();
    for (key, name) in &curr {
        if !prev.contains_key(key) {
            if let Err(error) = db::open_game_session(&state.pool, user_id, key, name).await {
                tracing::warn!(%user_id, key, %error, "failed to open game session");
            }
        }
    }
    for key in prev.keys() {
        if !curr.contains_key(key) {
            if let Err(error) = db::close_game_session(&state.pool, user_id, key).await {
                tracing::warn!(%user_id, key, %error, "failed to close game session");
            }
        }
    }
}

/// Fill `total_seconds` / `last_played_at` / `is_new` on every `kind ==
/// "playing"` activity from the playtime ledger (ACT-FR-032). `user_id` is
/// the member the activities belong to, not necessarily the viewer.
async fn enrich_playtime(state: &AppState, user_id: Uuid, activities: &mut [Activity]) {
    use crate::ws::activity::game_key;
    for activity in activities.iter_mut() {
        let Some(key) = game_key(activity) else { continue };
        match db::game_stats(&state.pool, user_id, &key).await {
            Ok(stats) => {
                activity.total_seconds = Some(stats.total_seconds);
                activity.last_played_at = stats.last_played_at.map(|time| time.to_rfc3339());
                activity.is_new = Some(stats.is_new);
            }
            Err(error) => tracing::warn!(%user_id, key, %error, "failed to load game stats"),
        }
    }
}

/* Removed mesh call teardown and spectator subscriptions.
async fn teardown_call_membership(
    state: &AppState,
    channel_id: Uuid,
    user_id: Uuid,
    reason: &str,
) {
    let removed_streams = {
        let mut calls = state.hub.calls.write().await;
        if !calls.is_participant(channel_id, user_id) {
            return;
        }
        calls.leave(channel_id, user_id)
    };
    let remaining = state.hub.calls.read().await.participant_ids(channel_id);
    state
        .hub
        .broadcast_to(
            &remaining,
            OutboundEnvelope::new(
                "call.peer_left",
                CallPeerLeft {
                    channel_id,
                    user_id,
                    reason: reason.to_string(),
                    is_bot: user_id == MUSIC_BOT_ID,
                },
            ),
        )
        .await;
    for stream_id in removed_streams {
        state
            .hub
            .broadcast_to(
                &remaining,
                OutboundEnvelope::new(
                    "stream.unpublished",
                    StreamUnpublished { channel_id, stream_id },
                ),
        )
        .await;
    }

    // Pull the music bot only when the LAST human leaves — playback runs on the
    // VPS now, not on whoever ran `/play`, so their leaving must not stop the
    // music for everyone else. (The bot also self-exits on its own idle
    // timeout as a backstop.)
    if user_id != MUSIC_BOT_ID {
        let only_bot_left = remaining == [MUSIC_BOT_ID];
        if only_bot_left {
            state.hub.music_djs.write().await.remove(&channel_id);
            state.hub.send_to(MUSIC_BOT_ID, OutboundEnvelope::new(
                "music.command",
                serde_json::json!({ "command": "stop", "voice_channel_id": channel_id, "reason": "empty" }),
            )).await;
            state.hub.calls.write().await.leave(channel_id, MUSIC_BOT_ID);
        }
    }

    broadcast_voice_roster(state, channel_id).await;
}

/// A disconnecting client may have been spectating streams in channels it
/// never joined (hover previews). Drop it from every stream's viewer set and
/// tell the affected owners so they stop sending.
async fn teardown_spectator_subscriptions(state: &AppState, user_id: Uuid) {
    let affected = {
        let mut calls = state.hub.calls.write().await;
        calls.remove_viewer_globally(user_id)
    };
    for (channel_id, stream_id, owner) in affected {
        state
            .hub
            .send_to(
                owner,
                OutboundEnvelope::new(
                    "stream.unsubscribed",
                    StreamUnsubscribed { channel_id, stream_id, subscriber: user_id },
                ),
            )
            .await;
    }
}
*/

async fn dispatch(
    state: &AppState,
    user_id: Uuid,
    connection_id: Uuid,
    text: &str,
    joined_calls: &mut HashSet<Uuid>,
) {
    let env = match serde_json::from_str::<InboundEnvelope>(text) {
        Ok(e) => e,
        Err(e) => {
            state
                .hub
                .send_to(
                    user_id,
                    OutboundEnvelope::error("bad_request", format!("malformed envelope: {e}"), None),
                )
                .await;
            return;
        }
    };

    macro_rules! parse_or_reject {
        ($ty:ty) => {
            match serde_json::from_value::<$ty>(env.data.clone()) {
                Ok(v) => v,
                Err(e) => {
                    state
                        .hub
                        .send_to(
                            user_id,
                            OutboundEnvelope::error(
                                "bad_request",
                                format!("invalid payload for {}: {e}", env.op),
                                None,
                            ),
                        )
                        .await;
                    return;
                }
            }
        };
    }

    match env.op.as_str() {
        "chat.message.create" => {
            let data: ChatMessageCreate = parse_or_reject!(ChatMessageCreate);
            handle_chat_create(state, user_id, data).await;
        }
        "chat.message.edit" => {
            let data: ChatMessageEdit = parse_or_reject!(ChatMessageEdit);
            handle_chat_edit(state, user_id, data).await;
        }
        "chat.message.delete" => {
            let data: ChatMessageDelete = parse_or_reject!(ChatMessageDelete);
            handle_chat_delete(state, user_id, data).await;
        }
        "activity.report" => {
            let data: ActivityReport = parse_or_reject!(ActivityReport);
            handle_activity_report(state, user_id, data).await;
        }
        "presence.set" => {
            let data: PresenceSet = parse_or_reject!(PresenceSet);
            let status = if data.status == "busy" { "busy" } else { "online" };
            state.hub.set_status(user_id, status).await;
            broadcast_presence_update(state, user_id, status).await;
        }
        "dm.open" => {
            #[derive(serde::Deserialize)]
            struct DmOpenData {
                target_user_id: Uuid,
                #[serde(default)]
                req_id: Option<String>,
            }
            let data: DmOpenData = parse_or_reject!(DmOpenData);
            if user_id != data.target_user_id {
                if let Ok(Some(community_id)) = db::primary_community_for(&state.pool, user_id).await {
                    let topic = if user_id < data.target_user_id {
                        format!("dm:{}:{}", user_id, data.target_user_id)
                    } else {
                        format!("dm:{}:{}", data.target_user_id, user_id)
                    };
                    let existing = sqlx::query_as::<_, crate::db::Channel>("SELECT * FROM channels WHERE community_id = $1 AND topic = $2 LIMIT 1").bind(community_id).bind(&topic).fetch_optional(&state.pool).await;
                    let channel = match existing {
                        Ok(Some(c)) => Some(c),
                        Ok(None) => sqlx::query_as::<_, crate::db::Channel>("INSERT INTO channels (community_id, category_id, name, kind, topic, position) VALUES ($1, NULL, 'dm', 'text', $2, 9999) RETURNING *").bind(community_id).bind(&topic).fetch_one(&state.pool).await.ok(),
                        Err(_) => None,
                    };
                    if let Some(ch) = channel {
                        state.hub.send_to(user_id, OutboundEnvelope::new("dm.opened", serde_json::json!({ "channel": ch, "target_user_id": data.target_user_id, "req_id": data.req_id }))).await;
                    }
                }
            }
        }
        "chat.typing" => {
            let data: ChatTyping = parse_or_reject!(ChatTyping);
            if let Ok(Some(channel)) = db::channel_if_member(&state.pool, data.channel_id, user_id).await {
                if channel.kind == "text" {
                    broadcast_to_community(state, channel.community_id, OutboundEnvelope::new(
                        "chat.typing",
                        serde_json::json!({ "channel_id": data.channel_id, "user_id": user_id }),
                    )).await;
                }
            }
        }
        // ---- voice v2 hint ops (SPEC-005). The v1 ops below translate into
        // the same handlers so the two dialects never drift. ----
        "voice.presence.hint" => {
            let data: VoicePresenceHint = parse_or_reject!(VoicePresenceHint);
            handle_presence_hint(state, user_id, connection_id, data, joined_calls).await;
        }
        "voice.track.hint" => {
            let data: VoiceTrackHint = parse_or_reject!(VoiceTrackHint);
            handle_track_hint(state, user_id, data, joined_calls).await;
        }
        "voice.room.request" => {
            let data: VoiceRoomRequest = parse_or_reject!(VoiceRoomRequest);
            handle_room_request(state, user_id, connection_id, data).await;
        }
        "voice.presence.enter" => {
            let data: VoicePresence = parse_or_reject!(VoicePresence);
            handle_presence_hint(
                state,
                user_id,
                connection_id,
                VoicePresenceHint { channel_id: data.channel_id, state: "joining".into(), participant_sid: None },
                joined_calls,
            )
            .await;
        }
        "voice.presence.leave" => {
            let data: VoicePresence = parse_or_reject!(VoicePresence);
            handle_presence_hint(
                state,
                user_id,
                connection_id,
                VoicePresenceHint { channel_id: data.channel_id, state: "leaving".into(), participant_sid: None },
                joined_calls,
            )
            .await;
        }
        "voice.rooms.request" => {
            handle_room_request(state, user_id, connection_id, VoiceRoomRequest { channel_ids: vec![] }).await;
        }
        "voice.track.published" | "voice.track.unpublished" => {
            let data: VoiceTrack = parse_or_reject!(VoiceTrack);
            let Some(track_sid) = data.track_sid else {
                tracing::info!(
                    event = "voice.registry.track_hint_ignored",
                    %user_id,
                    channel_id = %data.channel_id,
                    outcome = "missing_track_sid"
                );
                return;
            };
            handle_track_hint(
                state,
                user_id,
                VoiceTrackHint {
                    channel_id: data.channel_id,
                    track_sid,
                    source: data.source,
                    state: if env.op == "voice.track.published" { "published".into() } else { "unpublished".into() },
                },
                joined_calls,
            )
            .await;
        }
        // Music has no camera/screen publication to mirror. Its bot-owned
        // playback row remains a control-plane stream so the roster can show
        // the TOCANDO badge while LiveKit delivers the actual audio track.
        "stream.publish" => {
            let data: StreamPublish = parse_or_reject!(StreamPublish);
            handle_music_stream_publish(state, user_id, data).await;
        }
        "stream.unpublish" => {
            let data: StreamUnpublish = parse_or_reject!(StreamUnpublish);
            handle_music_stream_unpublish(state, user_id, data).await;
        }
        "call.state.update" => {
            let data: CallStateUpdate = parse_or_reject!(CallStateUpdate);
            handle_call_state_update(state, user_id, data).await;
        }
        "voice.move_member" => {
            let data: VoiceMoveMember = parse_or_reject!(VoiceMoveMember);
            handle_voice_move_member(state, user_id, data).await;
        }
        "voice.disconnect_member" => {
            let data: VoiceDisconnectMember = parse_or_reject!(VoiceDisconnectMember);
            handle_voice_disconnect_member(state, user_id, data).await;
        }
        "music.command" => {
            let data: MusicCommand = parse_or_reject!(MusicCommand);
            handle_music_command(state, user_id, data).await;
        }
        "music.status" => {
            let data: MusicStatus = parse_or_reject!(MusicStatus);
            handle_music_status(state, user_id, data).await;
        }
        "device.list_changed" => {
            let data: DeviceListChanged = parse_or_reject!(DeviceListChanged);
            tracing::debug!(%user_id, summary = ?data.summary, "device list changed");
        }
        other => {
            state
                .hub
                .send_to(
                    user_id,
                    OutboundEnvelope::error("unknown_op", format!("unknown op: {other}"), None),
                )
                .await;
        }
    }
}

async fn handle_music_command(state: &AppState, user_id: Uuid, data: MusicCommand) {
    const COMMANDS: &[&str] = &["play", "pause", "resume", "skip", "stop", "queue"];
    if !COMMANDS.contains(&data.command.as_str()) {
        state.hub.send_to(user_id, OutboundEnvelope::error("validation_error", "comando de música desconhecido", None)).await;
        return;
    }
    if !state.hub.voice.read().await.is_participant(data.voice_channel_id, user_id) {
        state.hub.send_to(user_id, OutboundEnvelope::error("forbidden", "entre no canal de voz antes de controlar o bot", None)).await;
        return;
    }
    if data.command == "play" && data.query.as_deref().unwrap_or("").trim().is_empty() {
        state.hub.send_to(user_id, OutboundEnvelope::error("validation_error", "use /play <link ou nome>", None)).await;
        return;
    }
    let Ok(Some(reply_channel)) = db::channel_if_member(&state.pool, data.channel_id, user_id).await else {
        state.hub.send_to(user_id, OutboundEnvelope::error("forbidden", "canal de resposta indisponível", None)).await;
        return;
    };
    if reply_channel.kind != "text" {
        state.hub.send_to(user_id, OutboundEnvelope::error("validation_error", "as atualizações do bot exigem um canal de texto", None)).await;
        return;
    }
    if !state.hub.is_online(MUSIC_BOT_ID).await {
        state.hub.send_to(user_id, OutboundEnvelope::error("service_unavailable", "o Tupi Música está iniciando; tente novamente em instantes", None)).await;
        return;
    }
    /* Removed local-DJ bookkeeping: LiveKit webhook owns room lifecycle.
    // Remember who summoned the bot into this voice channel: teardown_call_
    // membership uses it to yank the bot when that DJ leaves, so a finished
    // session can't strand "Tupi Música" in an empty room.
    {
        let mut djs = state.hub.music_djs.write().await;
        match data.command.as_str() {
            "play" => { djs.insert(data.voice_channel_id, user_id); }
            "stop" => { djs.remove(&data.voice_channel_id); }
            _ => {}
        }
    }
    */
    state.hub.send_to(MUSIC_BOT_ID, OutboundEnvelope::new("music.command", serde_json::json!({
        "channel_id": data.channel_id, "voice_channel_id": data.voice_channel_id,
        "command": data.command, "query": data.query, "requested_by": user_id
    }))).await;
}

async fn handle_music_status(state: &AppState, user_id: Uuid, data: MusicStatus) {
    const KINDS: &[&str] = &["loading", "queued", "playing", "paused", "resumed", "skipped", "stopped", "finished", "disconnected", "queue", "error"];
    if user_id != MUSIC_BOT_ID {
        state.hub.send_to(user_id, OutboundEnvelope::error("forbidden", "somente o bot pode publicar estados de música", None)).await;
        return;
    }
    if !KINDS.contains(&data.kind.as_str())
        || data.origin.as_ref().is_some_and(|value| !["text", "spotify", "youtube"].contains(&value.as_str()))
        || data.provider.as_ref().is_some_and(|value| !["cache", "library", "soundcloud", "audius", "youtube"].contains(&value.as_str()))
        || data.title.as_ref().is_some_and(|value| value.len() > 500)
        || data.artist.as_ref().is_some_and(|value| value.len() > 300)
        || data.detail.as_ref().is_some_and(|value| value.len() > 2000)
        || data.image_url.as_ref().is_some_and(|value| value.len() > 2000 || !is_http_url(value))
        || data.source_url.as_ref().is_some_and(|value| value.len() > 2000 || !is_http_url(value))
        || data.collection_name.as_ref().is_some_and(|value| value.len() > 500)
        || data.collection_kind.as_ref().is_some_and(|value| !["album", "playlist"].contains(&value.as_str()))
        || data.items.len() > 10
        || data.items.iter().any(|item| item.title.is_empty() || item.title.len() > 500 || item.artist.as_ref().is_some_and(|value| value.len() > 300))
    {
        state.hub.send_to(user_id, OutboundEnvelope::error("validation_error", "estado de música inválido", None)).await;
        return;
    }
    let channel = match db::channel_by_id(&state.pool, data.channel_id).await {
        Ok(Some(channel)) if channel.kind == "text" => channel,
        _ => {
            state.hub.send_to(user_id, OutboundEnvelope::error("not_found", "canal de texto não encontrado", None)).await;
            return;
        }
    };

    // Persist the card as a real message (author = the music bot's users row,
    // see migration 0011) so it survives a reconnect / reload and loads with
    // channel history, then broadcast it exactly like a chat message. `content`
    // stays empty: the client renders `music_status` as a card, not body text.
    let card = match serde_json::to_value(&data) {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(%error, "failed to serialize music status card");
            return;
        }
    };
    let inserted = sqlx::query_as::<_, (Uuid, chrono::DateTime<chrono::Utc>)>(
        "INSERT INTO messages (channel_id, author_id, content, music_status) \
         VALUES ($1, $2, '', $3) RETURNING id, created_at",
    )
    .bind(channel.id)
    .bind(MUSIC_BOT_ID)
    .bind(&card)
    .fetch_one(&state.pool)
    .await;
    let (message_id, created_at) = match inserted {
        Ok(row) => row,
        Err(error) => {
            tracing::error!(%error, channel_id = %channel.id, "failed to persist music status message");
            return;
        }
    };

    broadcast_to_community(
        state,
        channel.community_id,
        OutboundEnvelope::new(
            "chat.message.created",
            ChatMessageCreated {
                message: MessageDto {
                    id: message_id,
                    channel_id: channel.id,
                    author_id: MUSIC_BOT_ID,
                    content: String::new(),
                    created_at,
                    edited_at: None,
                    attachment_ids: vec![],
                    attachments: vec![],
                    music_status: Some(card),
                },
                in_reply_to: None,
            },
        ),
    )
    .await;
}

fn is_http_url(value: &str) -> bool {
    value.starts_with("https://") || value.starts_with("http://")
}

async fn handle_chat_create(state: &AppState, user_id: Uuid, data: ChatMessageCreate) {
    tracing::info!(%user_id, channel_id = %data.channel_id, has_request_id = data.req_id.is_some(), "chat.message.create received");
    let Ok(Some(channel)) = db::channel_if_member(&state.pool, data.channel_id, user_id).await else {
        state
            .hub
            .send_to(
                user_id,
                OutboundEnvelope::error("forbidden", "not a member of this channel's community", data.req_id.as_deref()),
            )
            .await;
        return;
    };
    if channel.kind != "text" {
        state.hub.send_to(user_id, OutboundEnvelope::error("validation_error", "messages can only be sent to text channels", data.req_id.as_deref())).await;
        return;
    }
    if (data.content.trim().is_empty() && data.attachment_ids.is_empty()) || data.content.len() > 4000 {
        state
            .hub
            .send_to(
                user_id,
                OutboundEnvelope::error("validation_error", "message must be 1..=4000 chars or contain attachments", data.req_id.as_deref()),
            )
            .await;
        return;
    }
    if data.attachment_ids.len() > 10 || data.attachment_ids.iter().collect::<HashSet<_>>().len() != data.attachment_ids.len() {
        state.hub.send_to(user_id, OutboundEnvelope::error("validation_error", "attachments must contain at most 10 unique ids", data.req_id.as_deref())).await;
        return;
    }
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(error) => {
            tracing::error!(%error, "failed to start message transaction");
            state.hub.send_to(user_id, OutboundEnvelope::error("internal_error", "failed to send message", data.req_id.as_deref())).await;
            return;
        }
    };
    // CHAT-FR (retry-safe send): when the client supplies a req_id, a retry
    // after a client-side timeout resolves to the original row instead of
    // inserting a duplicate — see migrations/0002_message_idempotency.sql.
    // `xmax = 0` is the standard Postgres tell for "this RETURNING row came
    // from the INSERT, not the ON CONFLICT DO UPDATE" — used below to skip
    // re-running attachment association (idempotent but pointless on a
    // retry) and, more importantly, to avoid re-broadcasting an already-
    // delivered message to every other client: a retry only means *this*
    // client didn't see its own confirmation, not that nobody did.
    #[derive(sqlx::FromRow)]
    struct CreateMessageRow {
        id: Uuid,
        channel_id: Uuid,
        author_id: Uuid,
        content: String,
        created_at: chrono::DateTime<chrono::Utc>,
        edited_at: Option<chrono::DateTime<chrono::Utc>>,
        is_new_insert: bool,
    }
    let row = sqlx::query_as::<_, CreateMessageRow>(
        "INSERT INTO messages (channel_id, author_id, content, client_req_id) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (channel_id, author_id, client_req_id) WHERE client_req_id IS NOT NULL \
         DO UPDATE SET content = messages.content \
         RETURNING id, channel_id, author_id, content, created_at, edited_at, (xmax = 0) AS is_new_insert",
    )
    .bind(data.channel_id)
    .bind(user_id)
    .bind(&data.content)
    .bind(&data.req_id)
    .fetch_one(&mut *tx)
    .await;

    match row {
        Ok(m) if !m.is_new_insert => {
            let _ = tx.rollback().await;
            let resolved_attachments = if data.attachment_ids.is_empty() {
                vec![]
            } else {
                sqlx::query_as::<_, (Uuid, String, String, i64)>(
                    "SELECT id, filename, content_type, size_bytes FROM attachments WHERE id = ANY($1)",
                )
                .bind(&data.attachment_ids)
                .fetch_all(&state.pool)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|(id, filename, content_type, size_bytes)| crate::routes::messages::MessageAttachment {
                    id,
                    filename,
                    content_type,
                    size_bytes,
                    url: format!("/api/attachments/{id}"),
                })
                .collect()
            };
            state.hub.send_to(user_id, OutboundEnvelope::new(
                "chat.message.created",
                ChatMessageCreated {
                    message: MessageDto {
                        id: m.id,
                        channel_id: m.channel_id,
                        author_id: m.author_id,
                        content: m.content,
                        created_at: m.created_at,
                        edited_at: m.edited_at,
                        attachment_ids: data.attachment_ids,
                        attachments: resolved_attachments,
                        music_status: None,
                    },
                    in_reply_to: data.req_id,
                },
            )).await;
        }
        Ok(m) => {
            let mut resolved_attachments = Vec::new();
            if !data.attachment_ids.is_empty() {
                let associated = sqlx::query(
                    "UPDATE attachments SET message_id = $1 WHERE id = ANY($2) AND uploader_id = $3 AND message_id IS NULL",
                )
                .bind(m.id)
                .bind(&data.attachment_ids)
                .bind(user_id)
                .execute(&mut *tx)
                .await;
                match associated {
                    Ok(result) if result.rows_affected() == data.attachment_ids.len() as u64 => {
                        resolved_attachments = sqlx::query_as::<_, (Uuid, String, String, i64)>(
                            "SELECT id, filename, content_type, size_bytes FROM attachments WHERE id = ANY($1)",
                        )
                        .bind(&data.attachment_ids)
                        .fetch_all(&mut *tx)
                        .await
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(id, filename, content_type, size_bytes)| crate::routes::messages::MessageAttachment {
                            id,
                            filename,
                            content_type,
                            size_bytes,
                            url: format!("/api/attachments/{id}"),
                        })
                        .collect();
                    }
                    Ok(_) => {
                        let _ = tx.rollback().await;
                        state.hub.send_to(user_id, OutboundEnvelope::error("validation_error", "one or more attachments are unavailable", data.req_id.as_deref())).await;
                        return;
                    }
                    Err(error) => {
                        tracing::error!(%error, "failed to associate message attachments");
                        let _ = tx.rollback().await;
                        state.hub.send_to(user_id, OutboundEnvelope::error("internal_error", "failed to send message", data.req_id.as_deref())).await;
                        return;
                    }
                }
            }
            if let Err(error) = tx.commit().await {
                tracing::error!(%error, "failed to commit message transaction");
                state.hub.send_to(user_id, OutboundEnvelope::error("internal_error", "failed to send message", data.req_id.as_deref())).await;
                return;
            }
            broadcast_to_community(state, channel.community_id, OutboundEnvelope::new(
                    "chat.message.created",
                    ChatMessageCreated {
                        message: MessageDto {
                            id: m.id,
                            channel_id: m.channel_id,
                            author_id: m.author_id,
                            content: m.content.clone(),
                            created_at: m.created_at,
                            edited_at: m.edited_at,
                            attachment_ids: data.attachment_ids,
                            attachments: resolved_attachments,
                            music_status: None,
                        },
                        in_reply_to: data.req_id,
                    },
                )).await;
            tracing::info!(%user_id, channel_id = %m.channel_id, "chat.message.created broadcast");
            crate::link_preview::spawn_unfurl_task(
                state.clone(),
                m.id,
                m.channel_id,
                channel.community_id,
                m.content,
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to persist chat message");
            state
                .hub
                .send_to(
                    user_id,
                    OutboundEnvelope::error("internal_error", "failed to send message", data.req_id.as_deref()),
                )
                .await;
        }
    }
}

async fn handle_chat_edit(state: &AppState, user_id: Uuid, data: ChatMessageEdit) {
    if data.content.trim().is_empty() || data.content.len() > 4000 {
        state
            .hub
            .send_to(
                user_id,
                OutboundEnvelope::error("validation_error", "message must be 1..=4000 chars", data.req_id.as_deref()),
            )
            .await;
        return;
    }
    let updated = sqlx::query_as::<_, db::Message>(
        "UPDATE messages SET content = $1, edited_at = now() \
         WHERE id = $2 AND author_id = $3 AND deleted_at IS NULL RETURNING *",
    )
    .bind(&data.content)
    .bind(data.message_id)
    .bind(user_id)
    .fetch_optional(&state.pool)
    .await;

    match updated {
        Ok(Some(m)) => {
            if let Ok(Some(channel)) = db::channel_if_member(&state.pool, m.channel_id, user_id).await {
                broadcast_to_community(state, channel.community_id, OutboundEnvelope::new(
                    "chat.message.edited",
                    ChatMessageEdited {
                        message_id: m.id,
                        content: m.content.clone(),
                        edited_at: m.edited_at,
                        in_reply_to: data.req_id,
                    },
                )).await;
                crate::link_preview::spawn_unfurl_task(
                    state.clone(),
                    m.id,
                    m.channel_id,
                    channel.community_id,
                    m.content,
                );
            }
        }
        Ok(None) => {
            state
                .hub
                .send_to(
                    user_id,
                    OutboundEnvelope::error("forbidden", "message not found or not yours", data.req_id.as_deref()),
                )
                .await;
        }
        Err(e) => tracing::error!(error = %e, "failed to edit message"),
    }
}

async fn handle_chat_delete(state: &AppState, user_id: Uuid, data: ChatMessageDelete) {
    let updated = sqlx::query_as::<_, (Uuid, Uuid)>(
        "UPDATE messages SET deleted_at = now() \
         WHERE id = $1 AND author_id = $2 AND deleted_at IS NULL RETURNING id, channel_id",
    )
    .bind(data.message_id)
    .bind(user_id)
    .fetch_optional(&state.pool)
    .await;

    match updated {
        Ok(Some((message_id, channel_id))) => {
            if let Ok(Some(channel)) = db::channel_if_member(&state.pool, channel_id, user_id).await {
                broadcast_to_community(state, channel.community_id, OutboundEnvelope::new(
                    "chat.message.deleted",
                    ChatMessageDeleted { message_id, channel_id, in_reply_to: data.req_id },
                )).await;
            }
        }
        Ok(None) => {
            state
                .hub
                .send_to(
                    user_id,
                    OutboundEnvelope::error("forbidden", "message not found or not yours", data.req_id.as_deref()),
                )
                .await;
        }
        Err(e) => tracing::error!(error = %e, "failed to delete message"),
    };
}

/* Removed mesh call join and peer snapshot flow.
async fn handle_call_join(
    state: &AppState,
    user_id: Uuid,
    data: CallJoin,
    joined_calls: &mut HashSet<Uuid>,
) {
    let is_bot = user_id == MUSIC_BOT_ID;
    let channel = match db::channel_if_member(&state.pool, data.channel_id, user_id).await {
        Ok(Some(channel)) if channel.kind == "voice" => channel,
        Ok(Some(_)) => {
            state.hub.send_to(user_id, OutboundEnvelope::error("validation_error", "calls can only be joined in voice channels", None)).await;
            return;
        }
        _ if is_bot => match db::channel_by_id(&state.pool, data.channel_id).await {
            Ok(Some(channel)) if channel.kind == "voice" => channel,
            _ => { state.hub.send_to(user_id, OutboundEnvelope::error("not_found", "voice channel not found", None)).await; return; }
        },
        _ => {
        state
            .hub
            .send_to(
                user_id,
                OutboundEnvelope::error("forbidden", "not a member of this channel's community", None),
            )
            .await;
        return;
        }
    };

    let call_is_full = !is_bot && {
        let calls = state.hub.calls.read().await;
        !calls.is_participant(data.channel_id, user_id) && calls.is_full(data.channel_id)
    };
    if call_is_full {
        state.hub.send_to(user_id, OutboundEnvelope::error("call_full", "this voice channel already has 10 participants", None)).await;
        return;
    }

    // A connection can belong to exactly one voice call. Move semantics are
    // intentional: the old roster sees a leave before the new one sees join.
    for previous_channel_id in joined_calls.clone() {
        if previous_channel_id != channel.id {
            joined_calls.remove(&previous_channel_id);
            teardown_call_membership(state, previous_channel_id, user_id, "left").await;
        }
    };

    let snapshot = {
        let mut calls = state.hub.calls.write().await;
        calls.join(data.channel_id, user_id, data.muted, data.deafened, is_bot)
    };
    let snapshot = match snapshot {
        Ok(snapshot) => snapshot,
        Err(CallOpError::CallFull) => {
            state.hub.send_to(user_id, OutboundEnvelope::error("call_full", "this voice channel already has 10 participants", None)).await;
            return;
        }
        Err(_) => {
            state.hub.send_to(user_id, OutboundEnvelope::error("internal_error", "failed to join call", None)).await;
            return;
        }
    };
    state
        .hub
        .send_to(
            user_id,
            OutboundEnvelope::new(
                "call.snapshot",
                CallSnapshot {
                    channel_id: data.channel_id,
                    participants: snapshot.participants,
                    streams: snapshot.streams,
                },
            ),
        )
        .await;

    joined_calls.insert(data.channel_id);

    let others: Vec<Uuid> = state
        .hub
        .calls
        .read()
        .await
        .participant_ids(data.channel_id)
        .into_iter()
        .filter(|id| *id != user_id)
        .collect();
    state
        .hub
        .broadcast_to(
            &others,
            OutboundEnvelope::new(
                "call.peer_joined",
                CallPeerJoined {
                    channel_id: data.channel_id,
                    participant: ParticipantDto {
                        user_id,
                        muted: data.muted,
                        deafened: data.deafened,
                        is_bot,
                    },
                },
            ),
        )
        .await;

    broadcast_voice_roster(state, data.channel_id).await;
}

*/
pub(crate) async fn broadcast_to_community(state: &AppState, community_id: Uuid, event: OutboundEnvelope) {
    match db::community_member_ids(&state.pool, community_id).await {
        Ok(user_ids) => state.hub.broadcast_to(&user_ids, event).await,
        Err(error) => tracing::error!(%community_id, %error, "failed to resolve community realtime recipients"),
    };
}

/// The single emitter for a voice-room change: a versioned `voice.room.delta`
/// to v2 connections, the full `voice.roster` of the affected channel to v1
/// connections — one source, two projections, so the dialects never drift
/// (SPEC-005 §4.5). Every path that mutates the `VoiceRegistry` funnels its
/// `RoomChange` through here.
pub async fn publish_room_change(state: &AppState, change: RoomChange) {
    if change.is_empty() {
        return;
    }
    let Some(community_id) = state.community_of_channel(change.channel_id).await else { return; };
    let Ok(recipients) = state.community_members(community_id).await else { return; };

    // v2: versioned delta.
    let delta = VoiceRoomDelta {
        channel_id: change.channel_id,
        version: change.version_after,
        previous_version: change.version_before,
        participants_added: change.participants_added.iter().map(Into::into).collect(),
        participants_updated: change.participants_updated.iter().map(Into::into).collect(),
        participants_removed: change.participants_removed.clone(),
        tracks_added: change.tracks_added.iter().map(Into::into).collect(),
        tracks_removed: change.tracks_removed.clone(),
        reason: serde_json::to_value(change.reason)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default(),
    };
    VoiceMetrics::bump(&state.voice_metrics.deltas_sent);
    state
        .hub
        .broadcast_to_versioned(&recipients, 2, OutboundEnvelope::new("voice.room.delta", delta))
        .await;

    // v1: the whole channel roster, projected from the same state.
    let (participants, streams) = {
        let voice = state.hub.voice.read().await;
        crate::ws::projection::v1_roster(&voice, change.channel_id)
    };
    state
        .hub
        .broadcast_to_max_version(
            &recipients,
            1,
            OutboundEnvelope::new(
                "voice.roster",
                VoiceRoster { channel_id: change.channel_id, participants, streams },
            ),
        )
        .await;
}

/// Validated `voice.presence.hint` / v1 `voice.presence.enter|leave`. Membership
/// is checked for both `joining` and `leaving` (fixes A2). `leaving` never
/// removes a *confirmed* participant (INV-A1) — it schedules a 2 s directed
/// reconcile instead, and, when the client passed its own matching sid, asks
/// LiveKit to remove it for an immediate exit.
async fn handle_presence_hint(
    state: &AppState,
    user_id: Uuid,
    connection_id: Uuid,
    data: VoicePresenceHint,
    joined_calls: &mut HashSet<Uuid>,
) {
    let allowed = if user_id == MUSIC_BOT_ID {
        matches!(db::channel_by_id(&state.pool, data.channel_id).await, Ok(Some(c)) if c.kind == "voice")
    } else {
        matches!(db::channel_if_member(&state.pool, data.channel_id, user_id).await, Ok(Some(c)) if c.kind == "voice")
    };
    if !allowed {
        tracing::info!(event = "voice.presence.hint", channel_id = %data.channel_id, %user_id, state = %data.state, source = "ws", outcome = "rejected");
        state
            .hub
            .send_to_connection(user_id, connection_id, OutboundEnvelope::error("forbidden", "not allowed to join this voice channel", None))
            .await;
        return;
    }

    match data.state.as_str() {
        "joining" => {
            for previous in joined_calls.clone() {
                if previous == data.channel_id {
                    continue;
                }
                joined_calls.remove(&previous);
                let (change, needs_reconcile) = state.hub.voice.write().await.hint_leaving(previous, user_id);
                if needs_reconcile {
                    state.schedule_reconcile(previous, Duration::from_secs(2)).await;
                }
                publish_room_change(state, change).await;
            }
            joined_calls.insert(data.channel_id);
            let change = state.hub.voice.write().await.hint_joining(
                data.channel_id,
                user_id,
                data.participant_sid.clone(),
                user_id == MUSIC_BOT_ID,
            );
            tracing::info!(
                event = "voice.registry.participant_added",
                channel_id = %data.channel_id,
                %user_id,
                source = "ws",
                participant_sid = data.participant_sid.as_deref().unwrap_or("-"),
                outcome = if change.is_empty() { "noop" } else { "applied" }
            );
            publish_room_change(state, change).await;
        }
        "leaving" => {
            joined_calls.remove(&data.channel_id);

            // A v2 client that passes its own current sid gets an immediate
            // exit: ask LiveKit to remove it, and let the webhook confirm. The
            // sid must match the registered one so a client cannot drop someone
            // else's session.
            let matches_sid = {
                let voice = state.hub.voice.read().await;
                voice
                    .room(data.channel_id)
                    .and_then(|room| room.participants.get(&user_id))
                    .and_then(|p| p.sid.clone())
                    .zip(data.participant_sid.clone())
                    .map(|(registered, informed)| registered == informed)
                    .unwrap_or(false)
            };

            let (change, needs_reconcile) = state.hub.voice.write().await.hint_leaving(data.channel_id, user_id);
            if needs_reconcile {
                state.schedule_reconcile(data.channel_id, Duration::from_secs(2)).await;
                tracing::info!(event = "voice.registry.leave_hint_deferred", channel_id = %data.channel_id, %user_id, source = "ws");
                if matches_sid {
                    let cfg = state.config.clone();
                    let channel = data.channel_id.to_string();
                    let identity = user_id.to_string();
                    tokio::spawn(async move {
                        if let Err(error) = crate::livekit::remove_participant(&cfg, &channel, &identity).await {
                            tracing::warn!(event = "voice.leave.remove_failed", %error);
                        }
                    });
                }
            }
            publish_room_change(state, change).await;
        }
        other => {
            tracing::info!(event = "voice.registry.hint_ignored", %user_id, state = %other);
        }
    }
}

/// Validated `voice.track.hint` / v1 `voice.track.published|unpublished`.
/// `unpublished` now requires the sender to own the track (INV-F1, fixes A1).
async fn handle_track_hint(
    state: &AppState,
    user_id: Uuid,
    data: VoiceTrackHint,
    joined_calls: &HashSet<Uuid>,
) {
    let Some(source) = TrackSource::parse(&data.source) else {
        tracing::info!(event = "voice.registry.track_hint_ignored", %user_id, source = %data.source, outcome = "unknown_source");
        return;
    };
    let published = match data.state.as_str() {
        "published" => true,
        "unpublished" => false,
        _ => return,
    };
    if !joined_calls.contains(&data.channel_id) {
        tracing::info!(event = "voice.registry.track_hint_ignored", %user_id, channel_id = %data.channel_id, outcome = "not_in_channel");
        return;
    }
    let result = state
        .hub
        .voice
        .write()
        .await
        .hint_track(data.channel_id, user_id, data.track_sid.clone(), source, published);
    match result {
        Ok(change) => publish_room_change(state, change).await,
        Err(HintError::NotTrackOwner) => {
            tracing::warn!(event = "voice.registry.track_hint_rejected", %user_id, channel_id = %data.channel_id, track_sid = %data.track_sid, outcome = "not_owner");
            state
                .hub
                .send_to(user_id, OutboundEnvelope::error("forbidden", "you do not own this track", None))
                .await;
        }
        Err(_) => { /* unknown track or not in call: silent */ }
    }
}

/// `voice.room.request` / v1 `voice.rooms.request`: rate-limited, then a full
/// snapshot in the connection's dialect.
async fn handle_room_request(
    state: &AppState,
    user_id: Uuid,
    connection_id: Uuid,
    data: VoiceRoomRequest,
) {
    if !state.allow_room_request(user_id).await {
        VoiceMetrics::bump(&state.voice_metrics.version_gaps_reported);
        state
            .hub
            .send_to_connection(user_id, connection_id, OutboundEnvelope::error("rate_limited", "muitas solicitações de estado", None))
            .await;
        return;
    }
    // Cap the id list so a huge payload cannot turn into a huge `ANY($2)`.
    let mut only = data.channel_ids;
    only.truncate(100);
    send_voice_room_state(state, user_id, connection_id, &only).await;
}

/// Emits a `voice.room.state` (v2) or `voice.rooms` (v1) snapshot for the
/// visible active channels, filtered to `only` when non-empty. Confirms
/// against LiveKit first (throttled) since a requested snapshot means the
/// client distrusts its own state.
async fn send_voice_room_state(
    state: &AppState,
    user_id: Uuid,
    connection_id: Uuid,
    only: &[Uuid],
) {
    if state.should_reconcile_voice(Duration::from_secs(5)).await {
        reconcile_voice_rooms(state).await;
    }
    let active = state.hub.voice.read().await.active_channel_ids();
    let wanted: Vec<Uuid> = if only.is_empty() {
        active
    } else {
        active.into_iter().filter(|id| only.contains(id)).collect()
    };
    let visible = match db::visible_channel_ids(&state.pool, user_id, &wanted).await {
        Ok(list) => list,
        Err(error) => {
            tracing::error!(%user_id, %error, "failed to build voice room state");
            return;
        }
    };

    let protocol = state.hub.connection_protocol(user_id, connection_id).await.unwrap_or(1);
    VoiceMetrics::bump(&state.voice_metrics.snapshots_sent);
    if protocol >= 2 {
        let rooms: Vec<VoiceRoomDto> = {
            let voice = state.hub.voice.read().await;
            visible.iter().filter_map(|id| crate::ws::projection::v2_room(&voice, *id)).collect()
        };
        state
            .hub
            .send_to_connection(
                user_id,
                connection_id,
                OutboundEnvelope::new(
                    "voice.room.state",
                    VoiceRoomState {
                        full: true,
                        channel_ids: (!only.is_empty()).then(|| only.to_vec()),
                        rooms,
                    },
                ),
            )
            .await;
    } else {
        let rooms: Vec<VoiceRoster> = {
            let voice = state.hub.voice.read().await;
            visible
                .iter()
                .map(|id| {
                    let (participants, streams) = crate::ws::projection::v1_roster(&voice, *id);
                    VoiceRoster { channel_id: *id, participants, streams }
                })
                .filter(|r| !r.participants.is_empty())
                .collect()
        };
        state
            .hub
            .send_to_connection(user_id, connection_id, OutboundEnvelope::new("voice.rooms", VoiceRoomsSnapshot { rooms }))
            .await;
    }
}

/// Maps LiveKit's `RoomParticipant` list into the registry's reconcile input.
/// Non-UUID identities and unprojectable track sources are dropped.
fn map_to_reconciled(
    participants: Vec<crate::livekit::RoomParticipant>,
) -> Vec<crate::ws::voice_registry::ReconciledParticipant> {
    participants
        .into_iter()
        .filter_map(|p| {
            Some(crate::ws::voice_registry::ReconciledParticipant {
                user_id: Uuid::parse_str(&p.identity).ok()?,
                sid: p.sid,
                tracks: p
                    .tracks
                    .into_iter()
                    .filter_map(|(track_sid, source, muted)| {
                        Some((
                            track_sid,
                            crate::ws::voice_registry::TrackSource::parse(&source)?,
                            muted,
                        ))
                    })
                    .collect(),
            })
        })
        .collect()
}

/// Confirms ONE channel against LiveKit — used when there is a specific reason
/// to distrust that channel (leave, kick, move, `room_finished`) instead of
/// sweeping every room.
pub async fn reconcile_one_room(state: &AppState, channel_id: Uuid) {
    if state.config.livekit_url.is_none() {
        return;
    }
    VoiceMetrics::bump(&state.voice_metrics.reconciles_run);
    let participants =
        match crate::livekit::room_participants(&state.config, &channel_id.to_string()).await {
            Ok(list) => list,
            Err(error) => {
                VoiceMetrics::bump(&state.voice_metrics.reconciles_failed);
                tracing::warn!(event = "voice.reconcile.failed", %channel_id, %error);
                return;
            }
        };
    let mapped = map_to_reconciled(participants);
    let change = state.hub.voice.write().await.reconcile_room(channel_id, mapped);
    if !change.is_empty() {
        VoiceMetrics::bump(&state.voice_metrics.reconciles_with_drift);
        tracing::warn!(
            event = "voice.reconcile.drift_detected",
            %channel_id,
            scope = "single_room",
            added = change.participants_added.len(),
            removed = change.participants_removed.len(),
        );
    }
    publish_room_change(state, change).await;
}

/// The bot's LiveKit audio publication has no screen/camera counterpart, so
/// its `music` row is retained in the app registry solely for the TOCANDO
/// sidebar badge. The old generic stream handlers were mesh-only and are kept
/// commented below; this narrow handler is the SFU control-plane equivalent.
async fn handle_music_stream_publish(state: &AppState, user_id: Uuid, data: StreamPublish) {
    if user_id != MUSIC_BOT_ID || data.kind != "music" {
        state.hub.send_to(user_id, OutboundEnvelope::error("validation_error", "only the music bot may publish this stream", None)).await;
        return;
    }
    let result = state.hub.calls.write().await.publish(
        data.channel_id,
        user_id,
        data.stream_id,
        data.kind,
        data.label,
        true,
        None,
    );
    if let Err(error) = result {
        let (code, message) = match error {
            CallOpError::NotInCall => ("forbidden", "join the call before publishing"),
            CallOpError::StreamAlreadyPublished => ("conflict", "music is already playing in this channel"),
            _ => ("validation_error", "unable to publish music"),
        };
        state.hub.send_to(user_id, OutboundEnvelope::error(code, message, None)).await;
        return;
    }
    // Mirror the TOCANDO row into the v2 registry so the projected roster
    // shows it (the legacy `CallRegistry.publish` above is kept only until
    // SPEC-018). SPEC-015 replaces this whole path.
    let change = state.hub.voice.write().await.set_music_row(data.channel_id, true);
    publish_room_change(state, change).await;
}

async fn handle_music_stream_unpublish(state: &AppState, user_id: Uuid, data: StreamUnpublish) {
    match state.hub.calls.write().await.unpublish(data.channel_id, user_id, data.stream_id) {
        Ok(_) => {
            let change = state.hub.voice.write().await.set_music_row(data.channel_id, false);
            publish_room_change(state, change).await;
        }
        Err(CallOpError::NotStreamOwner) => {
            state.hub.send_to(user_id, OutboundEnvelope::error("forbidden", "you do not own this stream", None)).await;
        }
        Err(_) => {
            state.hub.send_to(user_id, OutboundEnvelope::error("not_found", "music stream not found", None)).await;
        }
    }
}

/// Pulls LiveKit's authoritative room list and forces the ephemeral registry
/// to match it, rebroadcasting every roster that changed. This is what
/// rebuilds voice presence after a server restart or a dropped webhook —
/// LiveKit never replays `participant_joined` for members already in a room,
/// so the socket-authoritative path alone leaves the sidebar empty until each
/// client happens to re-announce itself.
pub async fn reconcile_voice_rooms(state: &AppState) {
    if state.config.livekit_url.is_none() {
        return;
    }
    let started = Instant::now();
    VoiceMetrics::bump(&state.voice_metrics.reconciles_run);
    tracing::debug!(event = "voice.reconcile.started");

    let snapshot = match crate::livekit::room_snapshot(&state.config).await {
        Ok(rooms) => rooms,
        Err(error) => {
            VoiceMetrics::bump(&state.voice_metrics.reconciles_failed);
            tracing::warn!(event = "voice.reconcile.failed", %error);
            return;
        }
    };
    let rooms_queried = snapshot.len();
    let mapped: Vec<(Uuid, Vec<crate::livekit::RoomParticipant>)> = snapshot
        .into_iter()
        .filter_map(|(room, participants)| {
            let channel_id = Uuid::parse_str(&room).ok()?;
            Some((channel_id, participants))
        })
        .collect();

    let live_channels: std::collections::HashSet<Uuid> =
        mapped.iter().map(|(channel_id, _)| *channel_id).collect();

    let mut changes: Vec<crate::ws::voice_registry::RoomChange> = Vec::new();
    {
        let mut voice = state.hub.voice.write().await;
        for (channel_id, participants) in mapped {
            let change = voice.reconcile_room(channel_id, map_to_reconciled(participants));
            if !change.is_empty() {
                changes.push(change);
            }
        }
        // Channels LiveKit no longer reports at all.
        changes.extend(voice.reconcile_prune(&live_channels).into_iter().filter(|c| !c.is_empty()));
    }

    let duration_ms = started.elapsed().as_millis() as u64;
    VoiceMetrics::set(&state.voice_metrics.last_reconcile_duration_ms, duration_ms);
    VoiceMetrics::set(
        &state.voice_metrics.last_reconcile_at_unix,
        chrono::Utc::now().timestamp().max(0) as u64,
    );
    if changes.is_empty() {
        tracing::debug!(
            event = "voice.reconcile.completed",
            rooms_queried,
            rooms_changed = 0,
            duration_ms
        );
    } else {
        VoiceMetrics::bump(&state.voice_metrics.reconciles_with_drift);
        let channels: Vec<Uuid> = changes.iter().map(|c| c.channel_id).collect();
        tracing::warn!(
            event = "voice.reconcile.drift_detected",
            rooms_queried,
            rooms_changed = changes.len(),
            channels = ?channels,
            duration_ms,
            "reconcile found divergence; a webhook was lost or there was a race"
        );
    }
    for change in changes {
        publish_room_change(state, change).await;
    }
}

async fn handle_call_state_update(
    state: &AppState,
    user_id: Uuid,
    data: CallStateUpdate,
) {
    // Mute/deafen is the one thing the client is authoritative over. Apply it
    // to the v2 registry; the returned `RoomChange` carries the updated row.
    let change = {
        let mut voice = state.hub.voice.write().await;
        voice.set_audio_state(data.channel_id, user_id, data.muted, data.deafened)
    };
    let change = match change {
        Ok(change) => change,
        Err(_) => {
            state.hub.send_to(user_id, OutboundEnvelope::error("forbidden", "join the call before changing participant state", None)).await;
            return;
        }
    };

    // The current (possibly unchanged) state, for the v1 `call.state.update`.
    let (muted, deafened) = {
        let voice = state.hub.voice.read().await;
        voice
            .room(data.channel_id)
            .and_then(|room| room.participants.get(&user_id))
            .map(|p| (p.muted, p.deafened))
            .unwrap_or((false, false))
    };
    let recipients = state.hub.voice.read().await.participant_ids(data.channel_id);
    state.hub.broadcast_to(
        &recipients,
        OutboundEnvelope::new(
            "call.state.update",
            CallStateUpdateEvent { channel_id: data.channel_id, user_id, muted, deafened },
        ),
    ).await;

    // …and the v2 delta (+ v1 roster) via the single emitter.
    publish_room_change(state, change).await;
}

/// A community owner drags another member's sidebar row onto a voice channel.
/// The server only validates the action and tells the target to move — the
/// target's client then sends a normal `call.join`, so channel-membership
/// checks, the previous-call teardown and all RTC setup run through the
/// existing path with nothing special-cased.
async fn handle_voice_move_member(state: &AppState, actor_id: Uuid, data: VoiceMoveMember) {
    let dest = match db::channel_by_id(&state.pool, data.channel_id).await {
        Ok(Some(channel)) if channel.kind == "voice" => channel,
        _ => {
            state.hub.send_to(actor_id, OutboundEnvelope::error("not_found", "voice channel not found", None)).await;
            return;
        }
    };

    // Only the community owner may drag other members around.
    if !matches!(db::is_community_owner(&state.pool, dest.community_id, actor_id).await, Ok(true)) {
        state.hub.send_to(actor_id, OutboundEnvelope::error("forbidden", "only the community owner can move members", None)).await;
        return;
    }

    if data.user_id == actor_id {
        // Moving yourself is just a normal join.
        state.hub.send_to(actor_id, OutboundEnvelope::new(
            "voice.moved", VoiceMoved { channel_id: data.channel_id, moved_by: actor_id },
        )).await;
        return;
    }

    // Target must belong to the destination channel's community — except the
    // music bot, which is a virtual participant with no membership row.
    if data.user_id != MUSIC_BOT_ID
        && !matches!(db::channel_if_member(&state.pool, dest.id, data.user_id).await, Ok(Some(_)))
    {
        state.hub.send_to(actor_id, OutboundEnvelope::error("validation_error", "that member is not in this community", None)).await;
        return;
    }

    // Target must be online and already sitting in some voice call — dragging
    // is for relocating an occupant, not pulling someone into voice cold.
    let source_channel = state.hub.voice.read().await.channel_of(data.user_id);
    let Some(source_channel) = source_channel else {
        state.hub.send_to(actor_id, OutboundEnvelope::error("validation_error", "that member is not in a voice channel", None)).await;
        return;
    };
    if !state.hub.is_online(data.user_id).await {
        state.hub.send_to(actor_id, OutboundEnvelope::error("validation_error", "that member is not in a voice channel", None)).await;
        return;
    }

    state.hub.send_to(data.user_id, OutboundEnvelope::new(
        "voice.moved", VoiceMoved { channel_id: data.channel_id, moved_by: actor_id },
    )).await;
    // The target's own client performs the join/leave; confirm both ends
    // against LiveKit shortly after (I-20).
    state.schedule_reconcile(source_channel, Duration::from_secs(2)).await;
    state.schedule_reconcile(data.channel_id, Duration::from_secs(2)).await;
    tracing::info!(%actor_id, target = %data.user_id, from = %source_channel, to = %data.channel_id, "voice.move_member");
}

/// Kick a member out of a voice channel. Humans: community-owner only. The
/// music bot: any member of that channel's community — and it gets a full
/// reset (stop playback / playlist, leave), not just a removal.
async fn handle_voice_disconnect_member(state: &AppState, actor_id: Uuid, data: VoiceDisconnectMember) {
    let dest = match db::channel_by_id(&state.pool, data.channel_id).await {
        Ok(Some(channel)) if channel.kind == "voice" => channel,
        _ => {
            state.hub.send_to(actor_id, OutboundEnvelope::error("not_found", "voice channel not found", None)).await;
            return;
        }
    };

    let is_bot = data.user_id == MUSIC_BOT_ID;
    let allowed = if is_bot {
        matches!(db::channel_if_member(&state.pool, dest.id, actor_id).await, Ok(Some(_)))
    } else {
        matches!(db::is_community_owner(&state.pool, dest.community_id, actor_id).await, Ok(true))
    };
    if !allowed {
        let msg = if is_bot { "join this community first" } else { "only the community owner can disconnect members" };
        state.hub.send_to(actor_id, OutboundEnvelope::error("forbidden", msg, None)).await;
        return;
    }

    if !state.hub.voice.read().await.is_participant(data.channel_id, data.user_id) {
        state.hub.send_to(actor_id, OutboundEnvelope::error("validation_error", "that member is not in this voice channel", None)).await;
        return;
    }

    if is_bot {
        // Full reset — the bot's `music.command stop` handler stops yt-dlp /
        // ffmpeg / any playlist and leaves the channel.
        state.hub.send_to(MUSIC_BOT_ID, OutboundEnvelope::new(
            "music.command", serde_json::json!({ "command": "stop", "voice_channel_id": data.channel_id, "reason": "disconnected" }),
        )).await;
    } else {
        // The kicked client leaves the call on this event.
        state.hub.send_to(data.user_id, OutboundEnvelope::new(
            "voice.disconnected", VoiceDisconnected { channel_id: data.channel_id, by: actor_id },
        )).await;
    }

    if !is_bot {
        if let Err(error) = crate::livekit::remove_participant(&state.config, &data.channel_id.to_string(), &data.user_id.to_string()).await {
            tracing::warn!(%error, target = %data.user_id, "failed to remove LiveKit participant");
        }
    }
    // INV-A1: the server does not evict locally. LiveKit's `RemoveParticipant`
    // (or the bot's own `music.command stop`) is authoritative; the webhook
    // confirms it, and a directed reconcile in 2 s is the backstop.
    state.schedule_reconcile(data.channel_id, Duration::from_secs(2)).await;
    tracing::info!(%actor_id, target = %data.user_id, channel_id = %data.channel_id, bot = is_bot, "voice.disconnect_member");
}

/* Removed mesh RTC relay and stream subscription handlers.
async fn relay_rtc(
    state: &AppState,
    from: Uuid,
    channel_id: Uuid,
    to: Uuid,
    op: &str,
    payload: impl serde::Serialize,
) {
    let ok = {
        let calls = state.hub.calls.read().await;
        // Either end may be a spectator (subscribed to a stream in this
        // channel) rather than a full call participant — that pairing carries
        // the owner→viewer screen-share offer/answer/ice.
        let endpoint_allowed = |uid| {
            calls.is_participant(channel_id, uid) || calls.is_stream_viewer(channel_id, uid)
        };
        endpoint_allowed(from) && endpoint_allowed(to)
    };
    if !ok {
        state
            .hub
            .send_to(
                from,
                OutboundEnvelope::error(
                    "forbidden",
                    "target is not a participant of this call",
                    None,
                ),
            )
            .await;
        return;
    }
    if !state.hub.is_online(to).await {
        state
            .hub
            .send_to(from, OutboundEnvelope::error("peer_offline", "target is not connected", None))
            .await;
        return;
    }
    let mut payload = match serde_json::to_value(payload) {
        Ok(serde_json::Value::Object(payload)) => payload,
        Ok(_) | Err(_) => {
            tracing::error!(%from, %to, %channel_id, "rtc relay payload was not an object");
            state.hub.send_to(from, OutboundEnvelope::error("internal_error", "failed to relay RTC signal", None)).await;
            return;
        }
    };
    if serde_json::to_vec(&payload).map_or(true, |encoded| encoded.len() > 64 * 1024) {
        state.hub.send_to(from, OutboundEnvelope::error("validation_error", "RTC signal exceeds 64 KiB", None)).await;
        return;
    }
    // Inbound RTC messages name only their target. The recipient needs an
    // authenticated sender identity to route the signal to its PeerController;
    // it is injected server-side rather than trusted from the client payload.
    payload.insert("from".to_string(), serde_json::json!(from));
    state
        .hub
        .send_to(to, OutboundEnvelope::new(op, serde_json::Value::Object(payload)))
        .await;
}

async fn handle_stream_publish(state: &AppState, user_id: Uuid, data: StreamPublish) {
    if data.kind != "screen" && data.kind != "music" && data.kind != "camera" {
        state
            .hub
            .send_to(user_id, OutboundEnvelope::error("validation_error", "only screen, camera and music streams are available", None))
            .await;
        return;
    }
    let is_music = data.kind == "music";
    let result = {
        let mut calls = state.hub.calls.write().await;
        calls.publish(
            data.channel_id,
            user_id,
            data.stream_id,
            data.kind.clone(),
            data.label.clone(),
            data.has_audio,
            data.msid.clone(),
        )
    };
    if let Err(error) = result {
        let (code, message) = match error {
            CallOpError::NotInCall => ("forbidden", "join the call before publishing"),
            CallOpError::StreamAlreadyPublished => ("conflict", "only one stream of each kind per publisher is allowed"),
            _ => ("validation_error", "unable to publish stream"),
        };
        state.hub.send_to(user_id, OutboundEnvelope::error(code, message, None)).await;
        return;
    }
    let participants = state.hub.calls.read().await.participant_ids(data.channel_id);
    state
        .hub
        .broadcast_to(
            &participants,
            OutboundEnvelope::new(
                "stream.published",
                StreamPublished {
                    channel_id: data.channel_id,
                    stream_id: data.stream_id,
                    owner: user_id,
                    kind: data.kind.clone(),
                    label: data.label.clone(),
                    has_audio: data.has_audio,
                    msid: data.msid.clone(),
                },
            ),
        )
        .await;

    // Music is an always-on audio broadcast, unlike a screen preview. Every
    // current peer is subscribed immediately; the local DJ opens the direct
    // WebRTC audio transceiver after receiving this request.
    if is_music {
        for subscriber in participants.into_iter().filter(|id| *id != user_id) {
            state.hub.send_to(subscriber, OutboundEnvelope::new("music.available", serde_json::json!({
                "channel_id": data.channel_id, "stream_id": data.stream_id, "owner": user_id, "label": data.label
            }))).await;
        }
    }

    broadcast_voice_roster(state, data.channel_id).await;
}

async fn handle_stream_unpublish(state: &AppState, user_id: Uuid, data: StreamUnpublish) {
    let result = {
        let mut calls = state.hub.calls.write().await;
        calls.unpublish(data.channel_id, user_id, data.stream_id)
    };
    match result {
        Ok(_viewers) => {
            let participants = state.hub.calls.read().await.participant_ids(data.channel_id);
            state
                .hub
                .broadcast_to(
                    &participants,
                    OutboundEnvelope::new(
                        "stream.unpublished",
                        StreamUnpublished {
                            channel_id: data.channel_id,
                            stream_id: data.stream_id,
                        },
                    ),
                )
                .await;
            broadcast_voice_roster(state, data.channel_id).await;
        }
        Err(CallOpError::NotStreamOwner) => {
            state
                .hub
                .send_to(user_id, OutboundEnvelope::error("forbidden", "you do not own this stream", None))
                .await;
        }
        Err(_) => {
            state
                .hub
                .send_to(user_id, OutboundEnvelope::error("not_found", "stream not found", None))
                .await;
        }
    }
}

async fn handle_stream_subscribe(state: &AppState, user_id: Uuid, data: StreamSubscribe) {
    // A subscriber who is not already in the call may still spectate a stream
    // (hover preview from the sidebar) as long as they belong to the channel's
    // community. Participants are trivially members, so one check covers both.
    let is_participant = state
        .hub
        .calls
        .read()
        .await
        .is_participant(data.channel_id, user_id);
    if !is_participant {
        match db::channel_if_member(&state.pool, data.channel_id, user_id).await {
            Ok(Some(_)) => {}
            _ => {
                state
                    .hub
                    .send_to(user_id, OutboundEnvelope::error("forbidden", "not a member of this channel's community", None))
                    .await;
                return;
            }
        }
    }
    let result = {
        let mut calls = state.hub.calls.write().await;
        calls.subscribe(data.channel_id, user_id, data.stream_id)
    };
    match result {
        Ok(owner) => {
            state
                .hub
                .send_to(
                    owner,
                    OutboundEnvelope::new(
                        "stream.subscription_requested",
                        StreamSubscriptionRequested {
                            channel_id: data.channel_id,
                            stream_id: data.stream_id,
                            subscriber: user_id,
                        },
                    ),
                )
                .await;
        }
        Err(_) => {
            state
                .hub
                .send_to(user_id, OutboundEnvelope::error("not_found", "stream not found or you're not in this call", None))
                .await;
        }
    }
}

async fn handle_stream_unsubscribe(state: &AppState, user_id: Uuid, data: StreamUnsubscribe) {
    let result = {
        let mut calls = state.hub.calls.write().await;
        calls.unsubscribe(data.channel_id, user_id, data.stream_id)
    };
    if let Ok(owner) = result {
        state
            .hub
            .send_to(
                owner,
                OutboundEnvelope::new(
                    "stream.unsubscribed",
                    StreamUnsubscribed {
                        channel_id: data.channel_id,
                        stream_id: data.stream_id,
                        subscriber: user_id,
                    },
                ),
            )
            .await;
    }
}
*/

#[cfg(test)]
mod tests {
    use super::truncate_chars;

    #[test]
    fn truncate_chars_keeps_short_strings_intact() {
        assert_eq!(truncate_chars("1.4.0", 64), "1.4.0");
        assert_eq!(truncate_chars("", 64), "");
    }

    #[test]
    fn truncate_chars_caps_long_strings_at_the_limit() {
        let s = "v".repeat(10_000);
        assert_eq!(truncate_chars(&s, 64).chars().count(), 64);
    }

    #[test]
    fn truncate_chars_never_splits_a_multibyte_char() {
        let s = "é".repeat(50); // 2 bytes per char
        let out = truncate_chars(&s, 10);
        assert_eq!(out.chars().count(), 10);
        assert_eq!(out, "é".repeat(10));
    }
}
