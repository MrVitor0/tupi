use std::time::Duration;

use axum::{extract::{State, Json}, http::{header, HeaderMap}, Json as AxumJson};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    auth::authenticate_token,
    db,
    error::{AppError, AppResult},
    livekit::{self, Mode},
    state::AppState,
    ws::{handler::publish_room_change, voice_metrics::VoiceMetrics, voice_registry::TrackSource},
};

const MUSIC_BOT_ID: Uuid = Uuid::from_u128(1);

#[derive(Deserialize)] pub struct TokenRequest { pub channel_id: Uuid, #[serde(default)] pub mode: Mode }
#[derive(Serialize)] pub struct TokenResponse { pub url: String, pub room: Uuid, pub token: String }

async fn identity_from_headers(state: &AppState, headers: &HeaderMap) -> AppResult<(Uuid, String, bool)> {
    let token = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")).ok_or(AppError::Unauthorized)?;
    if token == state.config.music_bot_token { return Ok((MUSIC_BOT_ID, "Music bot".into(), true)); }
    let (user, _) = authenticate_token(&state.pool, token).await?;
    Ok((user.id, user.display_name, false))
}

/// Logs and counts a token refusal (SPEC-002 §4.3). The `AppError` is returned
/// unchanged — this spec only instruments, it never alters behaviour.
fn refuse_token(state: &AppState, channel_id: Uuid, user_id: Uuid, reason: &'static str, error: AppError) -> AppError {
    VoiceMetrics::bump(&state.voice_metrics.tokens_refused);
    tracing::info!(event = "voice.token.refused", %channel_id, %user_id, reason);
    error
}

pub async fn token(State(state): State<AppState>, headers: HeaderMap, Json(request): Json<TokenRequest>) -> AppResult<AxumJson<TokenResponse>> {
    let (identity, name, is_bot) = identity_from_headers(&state, &headers).await?;
    let channel = db::channel_by_id(&state.pool, request.channel_id)
        .await?
        .ok_or_else(|| refuse_token(&state, request.channel_id, identity, "not_found", AppError::NotFound))?;
    if channel.kind != "voice" {
        return Err(refuse_token(&state, request.channel_id, identity, "not_voice", AppError::Forbidden));
    }
    if !is_bot && db::channel_if_member(&state.pool, request.channel_id, identity).await?.is_none() {
        return Err(refuse_token(&state, request.channel_id, identity, "not_member", AppError::Forbidden));
    }
    // INV-F2: refuse a `participant` token for a channel already at its human
    // cap. Spectators are `hidden` and never counted, so they are never barred.
    if !is_bot && request.mode == Mode::Participant {
        let voice = state.hub.voice.read().await;
        let already_in = voice.is_participant(request.channel_id, identity);
        let full = voice.is_full(request.channel_id);
        drop(voice);
        if !already_in && full {
            return Err(refuse_token(
                &state,
                request.channel_id,
                identity,
                "channel_full",
                AppError::Conflict("este canal de voz já está cheio".into()),
            ));
        }
    }
    let url = state
        .config
        .livekit_url
        .clone()
        .ok_or_else(|| refuse_token(&state, request.channel_id, identity, "livekit_unconfigured", AppError::ServiceUnavailable("LiveKit is not configured".into())))?;
    let token = livekit::access_token(&state.config, &identity.to_string(), &name, &request.channel_id.to_string(), request.mode, serde_json::json!({"is_bot": is_bot}))
        .map_err(|_| refuse_token(&state, request.channel_id, identity, "token_generation_failed", AppError::ServiceUnavailable("LiveKit is not configured".into())))?;
    // Best-effort projection so the sidebar fills in the moment a token is
    // minted. The authoritative lifecycle is the `voice.presence.*` WS pair
    // (socket-lifetime-bound: it evicts on channel switch, explicit leave, and
    // disconnect); this add is just reconciled by it. The bot has no WS
    // presence flow, so for the bot this is the only "joined" signal — it is
    // removed again by the "only the bot is left" check / the webhook.
    // A token only authorizes a possible room connection. Recording presence
    // here made rapid clicks on two channels create two roster entries; the
    // authenticated WebSocket announces presence only after it connects.
    VoiceMetrics::bump(&state.voice_metrics.tokens_issued);
    tracing::info!(
        event = "voice.token.issued",
        channel_id = %request.channel_id,
        user_id = %identity,
        mode = ?request.mode,
        is_bot,
    );
    Ok(AxumJson(TokenResponse { url, room: request.channel_id, token }))
}

pub async fn webhook(State(state): State<AppState>, headers: HeaderMap, body: String) -> AppResult<()> {
    VoiceMetrics::bump(&state.voice_metrics.webhooks_received);
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            VoiceMetrics::bump(&state.voice_metrics.webhooks_rejected);
            tracing::warn!(event = "voice.webhook.rejected", reason = "missing_authorization");
            AppError::Unauthorized
        })?;
    let event = livekit::verify_webhook(&state.config, authorization, &body).map_err(|error| {
        VoiceMetrics::bump(&state.voice_metrics.webhooks_rejected);
        tracing::warn!(event = "voice.webhook.rejected", reason = "signature", %error);
        AppError::Unauthorized
    })?;

    let room_name = event.room.as_ref().map(|r| r.name.clone());
    let identity = event.participant.as_ref().map(|p| p.identity.clone());
    tracing::debug!(
        event = "voice.webhook.received",
        livekit_event = %event.event,
        room = room_name.as_deref().unwrap_or("-"),
        identity = identity.as_deref().unwrap_or("-"),
        track_source = event.track.as_ref().map(|t| t.source.as_str()).unwrap_or("-"),
    );

    // Dedupe: a LiveKit redelivery must not reapply anything (A10).
    let dedupe_key = match &event.id {
        Some(id) => format!("{}:{}", event.event, id),
        None => format!(
            "{}:{}:{}:{}:{}",
            event.event,
            room_name.as_deref().unwrap_or("-"),
            event.participant.as_ref().and_then(|p| p.sid.as_deref()).unwrap_or("-"),
            event.track.as_ref().and_then(|t| t.sid.as_deref()).unwrap_or("-"),
            event.created_at.unwrap_or(0),
        ),
    };
    if state.hub.voice.write().await.is_duplicate_event(&dedupe_key) {
        VoiceMetrics::bump(&state.voice_metrics.webhooks_ignored_duplicate);
        tracing::info!(event = "voice.webhook.ignored", outcome = "ignored_duplicate", key = %dedupe_key);
        return Ok(());
    }

    // A very old event means the webhook stream is lagging: process it, but
    // also force a reconcile of that channel rather than trusting it alone.
    let is_late = event
        .created_at
        .map(|at| (chrono::Utc::now().timestamp() - at) > 60)
        .unwrap_or(false);

    let Some(channel_id) = event.room.as_ref().and_then(|r| Uuid::parse_str(&r.name).ok()) else {
        tracing::info!(event = "voice.webhook.ignored", outcome = "unparsable_room", livekit_event = %event.event);
        return Ok(());
    };

    // Spectators never enter a roster (INV-B3).
    if event
        .participant
        .as_ref()
        .and_then(|p| p.permission)
        .map(|perm| perm.hidden)
        .unwrap_or(false)
    {
        VoiceMetrics::bump(&state.voice_metrics.webhooks_ignored_hidden);
        tracing::debug!(event = "voice.webhook.ignored", outcome = "hidden_participant", %channel_id);
        return Ok(());
    }

    let user_id = event.participant.as_ref().and_then(|p| Uuid::parse_str(&p.identity).ok());
    let participant_sid = event.participant.as_ref().and_then(|p| p.sid.clone());

    let changes = match event.event.as_str() {
        "participant_joined" => match (user_id, participant_sid.clone()) {
            (Some(user), Some(sid)) => {
                let changes = state.hub.voice.write().await.webhook_participant_joined_exclusive(channel_id, user, sid);
                if !changes.is_empty() {
                    VoiceMetrics::bump(&state.voice_metrics.participants_added_by_webhook);
                    tracing::info!(event = "voice.registry.participant_added", %channel_id, user_id = %user, source = "webhook", outcome = "applied");
                }
                changes
            }
            _ => {
                tracing::info!(event = "voice.webhook.ignored", outcome = "missing_sid", livekit_event = "participant_joined", %channel_id);
                Vec::new()
            }
        },
        "participant_left" => match (user_id, participant_sid.clone()) {
            (Some(user), Some(sid)) => {
                let change = state.hub.voice.write().await.webhook_participant_left(channel_id, user, sid);
                if change.is_empty() {
                    VoiceMetrics::bump(&state.voice_metrics.webhooks_ignored_stale);
                    tracing::info!(event = "voice.webhook.ignored", outcome = "ignored_stale", %channel_id, user_id = %user, sid = %participant_sid.as_deref().unwrap_or("-"));
                } else {
                    VoiceMetrics::bump(&state.voice_metrics.participants_removed_by_webhook);
                    tracing::info!(event = "voice.registry.participant_removed", %channel_id, user_id = %user, source = "webhook", outcome = "applied");
                }
                vec![change]
            }
            _ => {
                tracing::info!(event = "voice.webhook.ignored", outcome = "missing_sid", livekit_event = "participant_left", %channel_id);
                Vec::new()
            }
        },
        "track_published" => match (user_id, event.track.as_ref()) {
            (Some(user), Some(track)) => match (track.sid.clone(), TrackSource::parse(&track.source)) {
                (Some(track_sid), Some(source)) => vec![state.hub.voice.write().await.webhook_track_published(channel_id, user, participant_sid.clone(), track_sid, source)],
                _ => {
                    tracing::debug!(event = "voice.webhook.ignored", outcome = "untrackable_publish", %channel_id, track_source = %track.source);
                    Vec::new()
                }
            },
            _ => Vec::new(),
        },
        "track_unpublished" => match event.track.as_ref().and_then(|t| t.sid.clone()) {
            Some(track_sid) => vec![state.hub.voice.write().await.webhook_track_unpublished(channel_id, &track_sid)],
            None => Vec::new(),
        },
        "track_muted" | "track_unmuted" => match event.track.as_ref().and_then(|t| t.sid.clone()) {
            Some(track_sid) => vec![state.hub.voice.write().await.webhook_track_muted(channel_id, &track_sid, event.event == "track_muted")],
            None => Vec::new(),
        },
        // NEVER clear the room here (RC-04). LiveKit can emit this after
        // someone has already rejoined. Confirm against the truth instead.
        "room_finished" => {
            tracing::info!(event = "voice.webhook.ignored", outcome = "room_finished_defers_to_reconcile", %channel_id);
            state.schedule_reconcile(channel_id, Duration::from_millis(500)).await;
            Vec::new()
        }
        "room_started" => Vec::new(),
        other => {
            tracing::debug!(event = "voice.webhook.ignored", outcome = "unhandled_event", livekit_event = %other, %channel_id);
            Vec::new()
        }
    };

    if is_late {
        tracing::warn!(event = "voice.webhook.late", %channel_id, livekit_event = %event.event);
        state.schedule_reconcile(channel_id, Duration::from_secs(1)).await;
    }

    for change in changes {
        publish_room_change(&state, change).await;
    }
    Ok(())
}
