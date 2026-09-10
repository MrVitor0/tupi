//! Wire format for the Tupi signaling/chat/presence WebSocket protocol.
//! Canonical catalog: SDD/09-websocket-protocol.md. Envelope shape:
//! `{ "v": 1, "op": "<namespace>.<action>", "data": { ... } }`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct InboundEnvelope {
    #[allow(dead_code)]
    pub v: u8,
    pub op: String,
    #[serde(default)]
    pub data: Value,
}

#[derive(Debug, Serialize)]
pub struct OutboundEnvelope {
    pub v: u8,
    pub op: String,
    pub data: Value,
}

impl OutboundEnvelope {
    pub fn new(op: &str, data: impl Serialize) -> Self {
        Self {
            v: 1,
            op: op.to_string(),
            data: serde_json::to_value(data).unwrap_or(Value::Null),
        }
    }

    pub fn error(code: &str, message: impl Into<String>, in_reply_to: Option<&str>) -> Self {
        Self::new(
            "error",
            ErrorData {
                code: code.to_string(),
                message: message.into(),
                in_reply_to: in_reply_to.map(|s| s.to_string()),
            },
        )
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorData {
    pub code: String,
    pub message: String,
    pub in_reply_to: Option<String>,
}

// ---- auth.* ----

/// Highest protocol dialect this server speaks. Bump only alongside a
/// documented dialect change in tupi-v2-refactor/05-protocol-spec.md.
///
/// This is `2` from SPEC-001 on, even though SPEC-001 implements nothing of
/// v2: SPEC-005 is what starts emitting the new ops, and the `features` list
/// in `auth.ok` is what actually declares availability. A client that
/// negotiates `protocol_version: 2` but sees an empty `features` MUST operate
/// in the v1 dialect (see 05-protocol-spec.md §1.2).
pub const MAX_SERVER_PROTOCOL: u8 = 2;

#[derive(Debug, Deserialize)]
pub struct AuthHello {
    pub token: String,
    #[serde(default = "default_protocol_version")]
    pub protocol_version: u8,
    #[serde(default)]
    pub client_version: Option<String>,
    #[serde(default)]
    pub client_platform: Option<String>,
}

fn default_protocol_version() -> u8 {
    1
}

#[derive(Debug, Serialize)]
pub struct AuthOk {
    pub user_id: Uuid,
    pub username: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub livekit_url: Option<String>,
    /// Protocol version actually negotiated for this connection:
    /// `min(client, MAX_SERVER_PROTOCOL)`.
    pub protocol_version: u8,
    /// The server's `CARGO_PKG_VERSION`.
    pub server_version: String,
    /// Optional capabilities; the client ignores names it does not know.
    pub features: Vec<String>,
}

// ---- presence.* ----

#[derive(Debug, Serialize, Clone)]
pub struct PresenceEntry {
    pub user_id: Uuid,
    pub status: String, // "online" | "offline"
}

#[derive(Debug, Serialize)]
pub struct PresenceSnapshot {
    pub users: Vec<PresenceEntry>,
}

#[derive(Debug, Serialize, Clone)]
pub struct PresenceUpdate {
    pub user_id: Uuid,
    pub status: String,
}

/// Inbound `presence.set` — a member toggling their own status. v1 accepts
/// `"online"` and `"busy"` (DND); `"busy"` also silences their own
/// new-message notifications, which is a client-side effect.
#[derive(Debug, Deserialize)]
pub struct PresenceSet {
    pub status: String,
}

// ---- activity.* ----
// Ephemeral "rich presence": what a member is playing/listening to outside
// Tupi. Client-detected (native SMTC / process scan), never persisted.
// Catalog: SDD/specs/activity.md.

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Activity {
    pub kind: String, // "playing" | "listening" | "watching" | "browsing"
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>, // RFC3339, opaque to the server
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset_image: Option<String>, // opaque ref resolved by the UI ("steam:<appid>", "att:<hash>", URL)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset_text: Option<String>,

    // ---- server-derived, outbound only (ACT-FR-032). Clients never set
    // these; `sanitize` forces them to None on the way in, and the aggregate
    // step fills them for `kind == "playing"` on the way out. ----
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_played_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_new: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ActivityReport {
    #[serde(default)]
    pub activities: Vec<Activity>,
    #[serde(default)]
    #[allow(dead_code)]
    pub req_id: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ActivityEntry {
    pub user_id: Uuid,
    pub activities: Vec<Activity>,
}

#[derive(Debug, Serialize)]
pub struct ActivitySnapshot {
    pub users: Vec<ActivityEntry>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ActivityUpdate {
    pub user_id: Uuid,
    pub activities: Vec<Activity>,
}

// ---- member.updated / channel.updated ----
// Outbound-only community broadcasts for the lightweight "right-click to
// rename" edits (display name, avatar, channel name). No inbound op — the
// mutations land over REST (routes/profile.rs, routes/channels.rs) and the
// resulting row is fanned out here so every client's sidebar/roster stays
// live without a refetch.

#[derive(Debug, Serialize)]
pub struct MemberUpdated {
    pub user_id: Uuid,
    pub username: String,
    pub display_name: String,
    /// `/api/users/<id>/avatar` when the member has one, else `None`. The
    /// native host inlines this to a `data:` URI before it reaches the UI
    /// (IpcBridge.HandleNetworkEvent), same as it does for bootstrap avatars.
    pub avatar_url: Option<String>,
    pub avatar_color: Option<String>,
    pub profile_tag: Option<String>,
    pub name_color: Option<String>,
    pub bio: Option<String>,
    pub banner_preset: Option<String>,
    pub pronouns: Option<String>,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    pub role: String,
}

#[derive(Debug, Serialize)]
pub struct ChannelUpdated {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub category_id: Option<Uuid>,
}

// ---- chat.* ----

#[derive(Debug, Deserialize)]
pub struct ChatMessageCreate {
    pub channel_id: Uuid,
    pub content: String,
    #[serde(default)]
    pub attachment_ids: Vec<Uuid>,
    #[serde(default)]
    pub req_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChatMessageEdit {
    pub message_id: Uuid,
    pub content: String,
    #[serde(default)]
    pub req_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChatMessageDelete {
    pub message_id: Uuid,
    #[serde(default)]
    pub req_id: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct MessageDto {
    pub id: Uuid,
    pub channel_id: Uuid,
    pub author_id: Uuid,
    pub content: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub edited_at: Option<chrono::DateTime<chrono::Utc>>,
    pub attachment_ids: Vec<Uuid>,
    #[serde(default)]
    pub attachments: Vec<crate::routes::messages::MessageAttachment>,
    /// Present only on the music bot's status cards (persisted `music.status`);
    /// the client renders it as a provider-branded card instead of body text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub music_status: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ChatMessageCreated {
    pub message: MessageDto,
    pub in_reply_to: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ChatMessageEdited {
    pub message_id: Uuid,
    pub content: String,
    pub edited_at: Option<chrono::DateTime<chrono::Utc>>,
    pub in_reply_to: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ChatMessageDeleted {
    pub message_id: Uuid,
    pub channel_id: Uuid,
    pub in_reply_to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkPreviewDto {
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub site_name: Option<String>,
    pub image_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ChatMessagePreviewUpdated {
    pub channel_id: Uuid,
    pub message_id: Uuid,
    pub link_preview: Option<LinkPreviewDto>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ChatTyping {
    pub channel_id: Uuid,
}

// ---- call.* ----

#[derive(Debug, Deserialize)]
pub struct CallStateUpdate {
    pub channel_id: Uuid,
    pub muted: Option<bool>,
    pub deafened: Option<bool>,
}

/// Inbound `voice.presence.enter` / `voice.presence.leave` — a lightweight,
/// socket-lifetime-bound signal that the client is (or is no longer) an active
/// participant of a voice channel. This is the authoritative source for the
/// community voice roster; LiveKit's `participant_*` webhooks only reconcile it.
/// Carries no media semantics — the client already talks to LiveKit directly.
#[derive(Debug, Deserialize)]
pub struct VoicePresence {
    pub channel_id: Uuid,
}

/// Inbound `voice.track.published` / `voice.track.unpublished` — the client
/// reporting that it just (un)published a camera or screen track to LiveKit.
/// This is what tells every other member's sidebar who is sharing, without
/// depending on LiveKit's `track_*` webhooks landing. `track_sid` is LiveKit's
/// publication sid; the client keys a peer's camera feed vs their screen feed
/// off it.
#[derive(Debug, Deserialize)]
pub struct VoiceTrack {
    pub channel_id: Uuid,
    /// `"camera"`, `"screen_share"`, or `"screen_share_audio"`.
    pub source: String,
    #[serde(default)]
    pub track_sid: Option<String>,
}

/// Inbound `voice.move_member` — a community owner dragging another member's
/// row onto a different voice channel in the sidebar. The server only tells
/// the target to move; the target's own client performs the `call.join`, so
/// all the usual join validation and RTC setup is reused unchanged.
#[derive(Debug, Deserialize)]
pub struct VoiceMoveMember {
    /// Who to move.
    pub user_id: Uuid,
    /// Destination voice channel.
    pub channel_id: Uuid,
}

/// Outbound `voice.moved` — delivered only to the member being moved.
#[derive(Debug, Serialize, Clone)]
pub struct VoiceMoved {
    pub channel_id: Uuid,
    pub moved_by: Uuid,
}

/// Inbound `voice.disconnect_member` — kick someone out of a voice channel.
/// Owner-only for humans; any community member may target the music bot
/// (which fully resets: stops playback, playlists, leaves).
#[derive(Debug, Deserialize)]
pub struct VoiceDisconnectMember {
    pub user_id: Uuid,
    pub channel_id: Uuid,
}

/// Outbound `voice.disconnected` — delivered only to the kicked member so
/// their client tears its own call down.
#[derive(Debug, Serialize, Clone)]
pub struct VoiceDisconnected {
    pub channel_id: Uuid,
    pub by: Uuid,
}

#[derive(Debug, Serialize, Clone)]
pub struct ParticipantDto {
    pub user_id: Uuid,
    pub muted: bool,
    pub deafened: bool,
    #[serde(default)]
    pub is_bot: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct StreamDto {
    pub stream_id: Uuid,
    pub owner: Uuid,
    pub kind: String,
    pub label: Option<String>,
    pub has_audio: bool,
    /// The publisher's local `MediaStream.id` for this stream's WebRTC tracks.
    /// Opaque to the server — relayed verbatim so a receiver can map an
    /// incoming video track (which carries the same msid) to the right
    /// `kind` when a peer sends both a screen and a camera at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msid: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CallSnapshot {
    pub channel_id: Uuid,
    pub participants: Vec<ParticipantDto>,
    pub streams: Vec<StreamDto>,
}

#[derive(Debug, Serialize, Clone)]
pub struct CallPeerJoined {
    pub channel_id: Uuid,
    pub participant: ParticipantDto,
}

#[derive(Debug, Serialize, Clone)]
pub struct CallPeerLeft {
    pub channel_id: Uuid,
    pub user_id: Uuid,
    pub reason: String,
    #[serde(default)]
    pub is_bot: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct CallStateUpdateEvent {
    pub channel_id: Uuid,
    pub user_id: Uuid,
    pub muted: bool,
    pub deafened: bool,
}

// ---- voice.roster / voice.rooms ----
// Community-wide, low-frequency projection of who is in each voice channel so
// the sidebar can render occupants live even for channels the viewer has not
// joined. Distinct from call.* (which is scoped to a call's own participants).

#[derive(Debug, Serialize, Clone)]
pub struct VoiceRosterEntry {
    pub user_id: Uuid,
    pub muted: bool,
    pub deafened: bool,
    pub sharing: bool,
    #[serde(default)]
    pub is_bot: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct VoiceRoster {
    pub channel_id: Uuid,
    pub participants: Vec<VoiceRosterEntry>,
    /// Live streams in this channel, so a member who has *not* joined can still
    /// request a hover preview (spectator subscribe — see handler.rs).
    pub streams: Vec<StreamDto>,
}

#[derive(Debug, Serialize, Clone)]
pub struct VoiceRoomsSnapshot {
    pub rooms: Vec<VoiceRoster>,
}

// ---- voice.room.* ---- (protocol v2; see tupi-v2-refactor/05-protocol-spec.md §2)
// SPEC-003 only defines these DTOs. Nothing emits them yet — SPEC-005 does.

#[derive(Debug, Serialize, Clone)]
pub struct VoiceParticipantDto {
    pub user_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub participant_sid: Option<String>,
    pub muted: bool,
    pub deafened: bool,
    pub is_bot: bool,
    pub provisional: bool,
    pub joined_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Clone)]
pub struct VoiceTrackDto {
    pub track_sid: String,
    pub owner: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_sid: Option<String>,
    pub source: String,
    pub muted: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct VoiceRoomDto {
    pub channel_id: Uuid,
    pub version: u64,
    pub participants: Vec<VoiceParticipantDto>,
    pub tracks: Vec<VoiceTrackDto>,
}

#[derive(Debug, Serialize)]
pub struct VoiceRoomState {
    pub full: bool,
    /// `None` means this is a complete community snapshot. A non-empty client
    /// request receives the exact requested scope, including ids for rooms
    /// that are now empty, so the client can merge it without erasing other
    /// channels.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_ids: Option<Vec<Uuid>>,
    pub rooms: Vec<VoiceRoomDto>,
}

#[derive(Debug, Serialize, Clone)]
pub struct VoiceRoomDelta {
    pub channel_id: Uuid,
    pub version: u64,
    pub previous_version: u64,
    pub participants_added: Vec<VoiceParticipantDto>,
    pub participants_updated: Vec<VoiceParticipantDto>,
    pub participants_removed: Vec<Uuid>,
    pub tracks_added: Vec<VoiceTrackDto>,
    pub tracks_removed: Vec<String>,
    pub reason: String,
}

#[derive(Debug, Deserialize)]
pub struct VoiceRoomRequest {
    #[serde(default)]
    pub channel_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct VoicePresenceHint {
    pub channel_id: Uuid,
    /// `"joining"` | `"leaving"`
    pub state: String,
    #[serde(default)]
    pub participant_sid: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct VoiceTrackHint {
    pub channel_id: Uuid,
    pub track_sid: String,
    pub source: String,
    /// `"published"` | `"unpublished"`
    pub state: String,
}

// ---- rtc.* ---- (relayed verbatim between two participants of the same call)

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RtcOffer {
    pub channel_id: Uuid,
    pub to: Uuid,
    pub sdp: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RtcAnswer {
    pub channel_id: Uuid,
    pub to: Uuid,
    pub sdp: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RtcIce {
    pub channel_id: Uuid,
    pub to: Uuid,
    pub candidate: Value,
}

#[derive(Debug, Deserialize)]
pub struct RtcConnectionState {
    pub channel_id: Uuid,
    pub to: Uuid,
    pub state: String,
}

// ---- stream.* ----

#[derive(Debug, Deserialize)]
pub struct StreamPublish {
    pub channel_id: Uuid,
    pub stream_id: Uuid,
    pub kind: String, // "screen" | "camera" | "music"
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub has_audio: bool,
    /// Publisher's local `MediaStream.id` — see `StreamDto::msid`.
    #[serde(default)]
    pub msid: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StreamPublished {
    pub channel_id: Uuid,
    pub stream_id: Uuid,
    pub owner: Uuid,
    pub kind: String,
    pub label: Option<String>,
    pub has_audio: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msid: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StreamUnpublish {
    pub channel_id: Uuid,
    pub stream_id: Uuid,
}

#[derive(Debug, Serialize, Clone)]
pub struct StreamUnpublished {
    pub channel_id: Uuid,
    pub stream_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct StreamSubscribe {
    pub channel_id: Uuid,
    pub stream_id: Uuid,
}

#[derive(Debug, Serialize, Clone)]
pub struct StreamSubscriptionRequested {
    pub channel_id: Uuid,
    pub stream_id: Uuid,
    pub subscriber: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct StreamUnsubscribe {
    pub channel_id: Uuid,
    pub stream_id: Uuid,
}

#[derive(Debug, Serialize, Clone)]
pub struct StreamUnsubscribed {
    pub channel_id: Uuid,
    pub stream_id: Uuid,
    pub subscriber: Uuid,
}

// ---- music.* ---- Local-DJ control plane. Playback is never proxied by the
// server; this just selects the client that owns the WebRTC music track.
#[derive(Debug, Deserialize, Clone)]
pub struct MusicCommand {
    pub channel_id: Uuid, // chat channel where the bot replies
    pub voice_channel_id: Uuid,
    pub command: String, // play | pause | resume | skip | stop | queue
    #[serde(default)]
    pub query: Option<String>,
}

/// Playback state emitted by the authenticated headless bot. The server
/// validates and fans it out to the community that owns `channel_id`; clients
/// render it as a provider-branded status card in that text channel.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MusicStatus {
    pub status_id: Uuid,
    pub channel_id: Uuid,
    pub kind: String,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub artist: Option<String>,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub count: Option<u32>,
    #[serde(default)]
    pub position: Option<u32>,
    #[serde(default)]
    pub queue_size: Option<u32>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub total_duration_ms: Option<u64>,
    #[serde(default)]
    pub eta_ms: Option<u64>,
    #[serde(default)]
    pub image_url: Option<String>,
    #[serde(default)]
    pub source_url: Option<String>,
    #[serde(default)]
    pub collection_name: Option<String>,
    #[serde(default)]
    pub collection_kind: Option<String>,
    #[serde(default)]
    pub requested_by: Option<Uuid>,
    #[serde(default)]
    pub items: Vec<MusicStatusItem>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MusicStatusItem {
    pub title: String,
    #[serde(default)]
    pub artist: Option<String>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
}

// ---- device.* ----

#[derive(Debug, Deserialize)]
pub struct DeviceListChanged {
    #[serde(default)]
    pub summary: Option<String>,
}
