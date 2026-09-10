//! Convergent projection of the LiveKit room state.
//!
//! Authority: LiveKit (see tupi-v2-refactor/03-target-architecture.md §1). This
//! registry is a cache that converges toward it. Confirmed-presence mutations
//! only enter through `webhook_*` and `reconcile_*` (INV-A1). Client hints
//! (`hint_*`) may add a provisional row and may remove *only* a provisional
//! one — never a confirmed participant.
//!
//! SPEC-003 only builds this structure. Nothing wires it up yet: SPEC-004
//! migrates the webhook/reconcile writers onto it, SPEC-005 starts emitting
//! `voice.room.*` from it, SPEC-018 deletes the legacy `CallRegistry`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use super::protocol::{VoiceParticipantDto, VoiceRoomDto, VoiceTrackDto};

pub type ChannelId = Uuid;
pub type UserId = Uuid;
/// LiveKit participant-session SID (e.g. `"PA_xxxxx"`).
pub type ParticipantSid = String;
/// LiveKit track-publication SID (e.g. `"TR_xxxxx"`).
pub type TrackSid = String;

pub const MUSIC_BOT_ID: Uuid = Uuid::from_u128(1);
/// How long a client-announced-only participant survives without LiveKit
/// confirmation (INV-A2).
pub const PROVISIONAL_TTL: Duration = Duration::from_secs(10);
/// Maximum human participants per channel (INV-F2).
pub const MAX_HUMAN_PARTICIPANTS: usize = 10;
/// Fixed capacity of the webhook-dedupe ring.
const SEEN_EVENTS_CAP: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackSource {
    Microphone,
    Camera,
    ScreenShare,
    ScreenShareAudio,
    /// Synthetic row for the music bot; it has no matching LiveKit track and is
    /// the single documented exception to INV-B1.
    Music,
}

impl TrackSource {
    /// Parses the `source` from a LiveKit webhook / ListParticipants. Returns
    /// `None` for sources we do not project (e.g. `"unknown"`).
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "microphone" => Some(Self::Microphone),
            "camera" => Some(Self::Camera),
            "screen_share" => Some(Self::ScreenShare),
            "screen_share_audio" => Some(Self::ScreenShareAudio),
            _ => None,
        }
    }

    pub fn as_wire(&self) -> &'static str {
        match self {
            Self::Microphone => "microphone",
            Self::Camera => "camera",
            Self::ScreenShare => "screen_share",
            Self::ScreenShareAudio => "screen_share_audio",
            Self::Music => "music",
        }
    }
}

#[derive(Debug, Clone)]
pub struct VoiceParticipant {
    pub user_id: UserId,
    /// `None` while provisional (client-announced, not LiveKit-confirmed).
    pub sid: Option<ParticipantSid>,
    pub joined_at: DateTime<Utc>,
    /// When this row became provisional; drives expiry (INV-A2).
    pub provisional_since: Option<Instant>,
    pub muted: bool,
    pub deafened: bool,
    pub is_bot: bool,
}

impl VoiceParticipant {
    pub fn is_provisional(&self) -> bool {
        self.sid.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct VoiceTrack {
    pub sid: TrackSid,
    pub owner: UserId,
    pub owner_sid: Option<ParticipantSid>,
    pub source: TrackSource,
    pub muted: bool,
}

#[derive(Debug)]
pub struct VoiceRoom {
    /// Monotonic per channel; only bumps on an accepted mutation (INV-C2).
    pub version: u64,
    pub participants: HashMap<UserId, VoiceParticipant>,
    /// Keyed by the LiveKit SID, never an invented id (INV-B1).
    pub tracks: HashMap<TrackSid, VoiceTrack>,
    pub reconciled_at: Option<Instant>,
}

impl Default for VoiceRoom {
    fn default() -> Self {
        Self {
            version: 0,
            participants: HashMap::new(),
            tracks: HashMap::new(),
            reconciled_at: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct VoiceRegistry {
    rooms: HashMap<ChannelId, VoiceRoom>,
    /// Recently-processed webhook event ids, for dedupe. Insertion order kept
    /// in `seen_order`; fixed capacity of `SEEN_EVENTS_CAP`.
    seen_events: HashSet<String>,
    seen_order: VecDeque<String>,
}

/// What a mutation changed, so the caller can emit the right delta without
/// recomputing anything. `version_after == version_before` means nothing
/// changed and no delta should be emitted.
#[derive(Debug, Default, Clone)]
pub struct RoomChange {
    pub channel_id: ChannelId,
    pub version_before: u64,
    pub version_after: u64,
    pub participants_added: Vec<VoiceParticipant>,
    pub participants_updated: Vec<VoiceParticipant>,
    pub participants_removed: Vec<UserId>,
    pub tracks_added: Vec<VoiceTrack>,
    pub tracks_removed: Vec<TrackSid>,
    pub reason: ChangeReason,
    /// True when the channel stopped existing (it ran out of participants).
    pub room_closed: bool,
}

impl RoomChange {
    fn empty(channel_id: ChannelId, version: u64) -> Self {
        Self {
            channel_id,
            version_before: version,
            version_after: version,
            reason: ChangeReason::WsStateUpdate,
            ..Default::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.version_after == self.version_before
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ChangeReason {
    #[serde(rename = "webhook.participant_joined")]
    WebhookParticipantJoined,
    #[serde(rename = "webhook.participant_left")]
    WebhookParticipantLeft,
    #[serde(rename = "webhook.track_published")]
    WebhookTrackPublished,
    #[serde(rename = "webhook.track_unpublished")]
    WebhookTrackUnpublished,
    /// Not in 05-protocol-spec.md §2.2's list yet; follows the same dotted
    /// convention. The protocol doc should gain this value.
    #[serde(rename = "webhook.track_muted")]
    WebhookTrackMuted,
    #[serde(rename = "reconcile.added")]
    ReconcileAdded,
    #[serde(rename = "reconcile.removed")]
    ReconcileRemoved,
    #[serde(rename = "reconcile.track_sync")]
    ReconcileTrackSync,
    #[serde(rename = "ws.presence_hint")]
    WsPresenceHint,
    #[serde(rename = "ws.state_update")]
    WsStateUpdate,
    #[serde(rename = "ws.track_hint")]
    WsTrackHint,
    #[serde(rename = "admin.disconnect_member")]
    AdminDisconnectMember,
    #[serde(rename = "channel.deleted")]
    ChannelDeleted,
    #[serde(rename = "provisional.expired")]
    ProvisionalExpired,
}

impl Default for ChangeReason {
    /// Only reached for empty changes, where `reason` is never serialized
    /// (`is_empty()` gates delta emission).
    fn default() -> Self {
        Self::WsStateUpdate
    }
}

#[derive(Debug, PartialEq)]
pub enum HintError {
    NotInCall,
    NotTrackOwner,
    TrackNotFound,
}

pub struct ReconciledParticipant {
    pub user_id: UserId,
    pub sid: ParticipantSid,
    pub tracks: Vec<(TrackSid, TrackSource, bool /* muted */)>,
}

// ---- DTO projection (sorted, deterministic — SPEC-003 §4.6) ----

impl From<&VoiceParticipant> for VoiceParticipantDto {
    fn from(p: &VoiceParticipant) -> Self {
        Self {
            user_id: p.user_id,
            participant_sid: p.sid.clone(),
            muted: p.muted,
            deafened: p.deafened,
            is_bot: p.is_bot,
            provisional: p.is_provisional(),
            joined_at: p.joined_at,
        }
    }
}

impl From<&VoiceTrack> for VoiceTrackDto {
    fn from(t: &VoiceTrack) -> Self {
        Self {
            track_sid: t.sid.clone(),
            owner: t.owner,
            owner_sid: t.owner_sid.clone(),
            source: t.source.as_wire().to_string(),
            muted: t.muted,
        }
    }
}

impl VoiceRoom {
    /// Deterministic projection: participants sorted by `user_id`, tracks by
    /// `track_sid`. Two identical states serialize byte-for-byte identically.
    pub fn to_dto(&self, channel_id: ChannelId) -> VoiceRoomDto {
        let mut participants: Vec<VoiceParticipantDto> =
            self.participants.values().map(VoiceParticipantDto::from).collect();
        participants.sort_by(|a, b| a.user_id.cmp(&b.user_id));
        let mut tracks: Vec<VoiceTrackDto> =
            self.tracks.values().map(VoiceTrackDto::from).collect();
        tracks.sort_by(|a, b| a.track_sid.cmp(&b.track_sid));
        VoiceRoomDto {
            channel_id,
            version: self.version,
            participants,
            tracks,
        }
    }
}

impl VoiceRegistry {
    // ---------- reads ----------

    pub fn room(&self, channel_id: ChannelId) -> Option<&VoiceRoom> {
        self.rooms.get(&channel_id)
    }

    /// Channels with at least one participant (05-protocol-spec.md §2.1).
    pub fn active_channel_ids(&self) -> Vec<ChannelId> {
        self.rooms
            .iter()
            .filter(|(_, room)| !room.participants.is_empty())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Every channel that has a room, including ones down to just a music row.
    /// For the debug endpoint — a room with no participants is itself a bug
    /// worth surfacing.
    pub fn all_channel_ids(&self) -> Vec<ChannelId> {
        self.rooms.keys().copied().collect()
    }

    pub fn version(&self, channel_id: ChannelId) -> u64 {
        self.rooms.get(&channel_id).map_or(0, |room| room.version)
    }

    pub fn is_participant(&self, channel_id: ChannelId, user_id: UserId) -> bool {
        self.rooms
            .get(&channel_id)
            .is_some_and(|room| room.participants.contains_key(&user_id))
    }

    pub fn participant_ids(&self, channel_id: ChannelId) -> Vec<UserId> {
        self.rooms
            .get(&channel_id)
            .map(|room| room.participants.keys().copied().collect())
            .unwrap_or_default()
    }

    /// The channel `user_id` is in, if any. A user is in at most one; if a bug
    /// elsewhere put them in two, this returns the first in iteration order and
    /// callers must not depend on which — `reconcile_room` corrects it.
    pub fn channel_of(&self, user_id: UserId) -> Option<ChannelId> {
        self.rooms
            .iter()
            .find(|(_, room)| room.participants.contains_key(&user_id))
            .map(|(id, _)| *id)
    }

    /// Confirmed, non-bot participants only (INV-F2).
    pub fn human_count(&self, channel_id: ChannelId) -> usize {
        self.rooms.get(&channel_id).map_or(0, |room| {
            room.participants
                .values()
                .filter(|p| !p.is_bot && !p.is_provisional())
                .count()
        })
    }

    pub fn is_full(&self, channel_id: ChannelId) -> bool {
        self.human_count(channel_id) >= MAX_HUMAN_PARTICIPANTS
    }

    /// Sorted, deterministic snapshot for the given channels (those without a
    /// room are skipped).
    pub fn room_dtos(&self, channel_ids: &[ChannelId]) -> Vec<VoiceRoomDto> {
        let mut out: Vec<VoiceRoomDto> = channel_ids
            .iter()
            .filter_map(|id| self.rooms.get(id).map(|room| room.to_dto(*id)))
            .collect();
        out.sort_by(|a, b| a.channel_id.cmp(&b.channel_id));
        out
    }

    // ---------- authoritative mutation: webhook ----------

    /// Inserts or confirms a participant. An existing provisional row for the
    /// same user is confirmed, not duplicated.
    pub fn webhook_participant_joined(
        &mut self,
        channel_id: ChannelId,
        user_id: UserId,
        sid: ParticipantSid,
    ) -> RoomChange {
        let room = self.rooms.entry(channel_id).or_default();
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::WebhookParticipantJoined;

        if let Some(existing) = room.participants.get_mut(&user_id) {
            if existing.sid.as_deref() == Some(sid.as_str()) {
                return change; // exact redelivery
            }
            // Either a reconnection with a new sid or the confirmation of a
            // provisional row.
            existing.sid = Some(sid);
            existing.provisional_since = None;
            let updated = existing.clone();
            change.version_after = bump(room);
            change.participants_updated.push(updated);
        } else {
            let participant = VoiceParticipant {
                user_id,
                sid: Some(sid),
                joined_at: Utc::now(),
                provisional_since: None,
                muted: false,
                deafened: false,
                is_bot: user_id == MUSIC_BOT_ID,
            };
            room.participants.insert(user_id, participant.clone());
            change.version_after = bump(room);
            change.participants_added.push(participant);
        }
        change
    }

    /// Applies an authoritative LiveKit join while preserving the product
    /// invariant that one Tupi identity can occupy only one voice channel.
    ///
    /// LiveKit can deliver the new-room `participant_joined` before the old-room
    /// `participant_left`. Leaving both rows until a later reconcile is what
    /// made a drag visibly duplicate a member. The new join is authoritative,
    /// therefore it safely evicts stale rows from every other room and returns
    /// a delta for each affected channel.
    pub fn webhook_participant_joined_exclusive(
        &mut self,
        channel_id: ChannelId,
        user_id: UserId,
        sid: ParticipantSid,
    ) -> Vec<RoomChange> {
        let previous: Vec<(ChannelId, Option<ParticipantSid>)> = self
            .rooms
            .iter()
            .filter(|(id, room)| **id != channel_id && room.participants.contains_key(&user_id))
            .map(|(id, room)| (*id, room.participants.get(&user_id).and_then(|p| p.sid.clone())))
            .collect();

        let mut changes = Vec::with_capacity(previous.len() + 1);
        for (previous_channel, previous_sid) in previous {
            let change = match previous_sid {
                Some(previous_sid) => self.webhook_participant_left(previous_channel, user_id, previous_sid),
                // A provisional row has no LiveKit SID to validate. The new
                // authoritative join proves that provisional attempt lost.
                None => self.hint_leaving(previous_channel, user_id).0,
            };
            if !change.is_empty() {
                changes.push(change);
            }
        }

        let joined = self.webhook_participant_joined(channel_id, user_id, sid);
        if !joined.is_empty() {
            changes.push(joined);
        }
        changes
    }

    /// Removes a participant only if `sid` matches the registered one (INV-B2).
    /// A mismatched or provisional target returns an empty `RoomChange`.
    pub fn webhook_participant_left(
        &mut self,
        channel_id: ChannelId,
        user_id: UserId,
        sid: ParticipantSid,
    ) -> RoomChange {
        let Some(room) = self.rooms.get_mut(&channel_id) else {
            return RoomChange::empty(channel_id, 0);
        };
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::WebhookParticipantLeft;

        match room.participants.get(&user_id).map(|p| p.sid.clone()) {
            None => return change,                         // not present
            Some(None) => return change,                   // provisional: no session ever existed
            Some(Some(current)) if current != sid => return change, // stale sid (RC-06)
            Some(Some(_)) => {}
        }

        room.participants.remove(&user_id);
        let removed_tracks: Vec<TrackSid> = room
            .tracks
            .values()
            .filter(|t| t.owner == user_id)
            .map(|t| t.sid.clone())
            .collect();
        for sid in &removed_tracks {
            room.tracks.remove(sid);
        }
        change.version_after = bump(room);
        change.participants_removed.push(user_id);
        change.tracks_removed = removed_tracks;
        self.close_if_empty(channel_id, &mut change);
        change
    }

    pub fn webhook_track_published(
        &mut self,
        channel_id: ChannelId,
        user_id: UserId,
        participant_sid: Option<ParticipantSid>,
        track_sid: TrackSid,
        source: TrackSource,
    ) -> RoomChange {
        let room = self.rooms.entry(channel_id).or_default();
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::WebhookTrackPublished;

        if room.tracks.contains_key(&track_sid) {
            return change; // redelivery
        }

        // The track webhook can arrive before the join webhook — insert a
        // provisional owner so the share is never orphaned. The later
        // `participant_joined` confirms it through the normal path.
        if !room.participants.contains_key(&user_id) {
            let provisional = VoiceParticipant {
                user_id,
                sid: participant_sid.clone(),
                joined_at: Utc::now(),
                provisional_since: participant_sid.is_none().then(Instant::now),
                muted: false,
                deafened: false,
                is_bot: user_id == MUSIC_BOT_ID,
            };
            room.participants.insert(user_id, provisional.clone());
            change.version_after = bump(room);
            change.participants_added.push(provisional);
        }

        let owner_sid = participant_sid.or_else(|| {
            room.participants
                .get(&user_id)
                .and_then(|p| p.sid.clone())
        });
        let track = VoiceTrack {
            sid: track_sid.clone(),
            owner: user_id,
            owner_sid,
            source,
            muted: false,
        };
        room.tracks.insert(track_sid, track.clone());
        change.version_after = bump(room);
        change.tracks_added.push(track);
        change
    }

    /// Removes a track by SID. An unknown SID returns an empty `RoomChange`.
    /// No owner/source search — the SID is the key (fixes RC-03).
    pub fn webhook_track_unpublished(
        &mut self,
        channel_id: ChannelId,
        track_sid: &str,
    ) -> RoomChange {
        let Some(room) = self.rooms.get_mut(&channel_id) else {
            return RoomChange::empty(channel_id, 0);
        };
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::WebhookTrackUnpublished;
        if room.tracks.remove(track_sid).is_none() {
            return change;
        }
        change.version_after = bump(room);
        change.tracks_removed.push(track_sid.to_string());
        change
    }

    pub fn webhook_track_muted(
        &mut self,
        channel_id: ChannelId,
        track_sid: &str,
        muted: bool,
    ) -> RoomChange {
        let Some(room) = self.rooms.get_mut(&channel_id) else {
            return RoomChange::empty(channel_id, 0);
        };
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::WebhookTrackMuted;
        let Some(track) = room.tracks.get_mut(track_sid) else {
            return change;
        };
        if track.muted == muted {
            return change;
        }
        track.muted = muted;
        let updated = track.clone();
        change.version_after = bump(room);
        change.tracks_added.push(updated); // resend the whole line
        change
    }

    // ---------- authoritative mutation: reconcile ----------

    /// Forces the channel to mirror exactly what LiveKit reports. Preserves
    /// mute, deafen, `is_bot`, `joined_at` and the synthetic music row (state
    /// only Tupi knows).
    pub fn reconcile_room(
        &mut self,
        channel_id: ChannelId,
        participants: Vec<ReconciledParticipant>,
    ) -> RoomChange {
        let room = self.rooms.entry(channel_id).or_default();
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::ReconcileAdded;

        let live_ids: HashSet<UserId> = participants.iter().map(|p| p.user_id).collect();

        // Remove confirmed locals LiveKit no longer lists; keep still-fresh
        // provisionals.
        let to_remove: Vec<UserId> = room
            .participants
            .values()
            .filter(|p| {
                if live_ids.contains(&p.user_id) {
                    return false;
                }
                match p.provisional_since {
                    None => true, // confirmed and absent -> remove
                    Some(since) => since.elapsed() >= PROVISIONAL_TTL,
                }
            })
            .map(|p| p.user_id)
            .collect();
        for user_id in &to_remove {
            room.participants.remove(user_id);
            let owned: Vec<TrackSid> = room
                .tracks
                .values()
                .filter(|t| t.owner == *user_id)
                .map(|t| t.sid.clone())
                .collect();
            for sid in &owned {
                room.tracks.remove(sid);
            }
            change.participants_removed.push(*user_id);
            change.tracks_removed.extend(owned);
        }

        // Add / update from the live list. Never touch mute/deafen/is_bot/joined_at.
        for p in &participants {
            match room.participants.get_mut(&p.user_id) {
                None => {
                    let participant = VoiceParticipant {
                        user_id: p.user_id,
                        sid: Some(p.sid.clone()),
                        joined_at: Utc::now(),
                        provisional_since: None,
                        muted: false,
                        deafened: false,
                        is_bot: p.user_id == MUSIC_BOT_ID,
                    };
                    room.participants.insert(p.user_id, participant.clone());
                    change.participants_added.push(participant);
                }
                Some(local) if local.sid.as_deref() != Some(p.sid.as_str()) => {
                    local.sid = Some(p.sid.clone());
                    local.provisional_since = None;
                    change.participants_updated.push(local.clone());
                }
                Some(_) => {}
            }
        }

        // Track sync. `Music` rows are never pruned.
        let live_tracks: HashMap<TrackSid, (UserId, ParticipantSid, TrackSource, bool)> = participants
            .iter()
            .flat_map(|p| {
                p.tracks
                    .iter()
                    .map(move |(sid, source, muted)| (sid.clone(), (p.user_id, p.sid.clone(), *source, *muted)))
            })
            .collect();

        let stale_tracks: Vec<TrackSid> = room
            .tracks
            .values()
            .filter(|t| t.source != TrackSource::Music && !live_tracks.contains_key(&t.sid))
            .map(|t| t.sid.clone())
            .collect();
        for sid in &stale_tracks {
            room.tracks.remove(sid);
            change.tracks_removed.push(sid.clone());
        }
        for (sid, (owner, owner_sid, source, muted)) in &live_tracks {
            match room.tracks.get_mut(sid) {
                None => {
                    let track = VoiceTrack {
                        sid: sid.clone(),
                        owner: *owner,
                        owner_sid: Some(owner_sid.clone()),
                        source: *source,
                        muted: *muted,
                    };
                    room.tracks.insert(sid.clone(), track.clone());
                    change.tracks_added.push(track);
                }
                Some(local) if local.muted != *muted => {
                    local.muted = *muted;
                    change.tracks_added.push(local.clone()); // resend the whole line
                }
                Some(_) => {}
            }
        }

        let touched = !change.participants_added.is_empty()
            || !change.participants_updated.is_empty()
            || !change.participants_removed.is_empty()
            || !change.tracks_added.is_empty()
            || !change.tracks_removed.is_empty();
        if touched {
            change.version_after = bump(room);
        }
        room.reconciled_at = Some(Instant::now()); // always, even with no change

        self.close_if_empty(channel_id, &mut change);
        change
    }

    /// Removes channels LiveKit no longer reports at all. Call after
    /// `reconcile_room` for every reported channel.
    pub fn reconcile_prune(&mut self, live_channels: &HashSet<ChannelId>) -> Vec<RoomChange> {
        let dead: Vec<ChannelId> = self
            .rooms
            .keys()
            .filter(|id| !live_channels.contains(id))
            .copied()
            .collect();
        dead.into_iter()
            .map(|channel_id| {
                let mut change = self.close_channel(channel_id);
                change.reason = ChangeReason::ReconcileRemoved;
                change
            })
            .collect()
    }

    // ---------- client hints (non-authoritative) ----------

    /// Inserts a provisional row, or a confirmed one when the client already
    /// knows its sid. Never replaces an already-confirmed participant.
    pub fn hint_joining(
        &mut self,
        channel_id: ChannelId,
        user_id: UserId,
        sid: Option<ParticipantSid>,
        is_bot: bool,
    ) -> RoomChange {
        let room = self.rooms.entry(channel_id).or_default();
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::WsPresenceHint;

        match room.participants.get_mut(&user_id) {
            Some(existing) if !existing.is_provisional() => change, // never override a confirmed row
            Some(existing) => {
                // Provisional already there; the client may now know the sid.
                match sid {
                    Some(sid) if existing.sid.as_deref() != Some(sid.as_str()) => {
                        existing.sid = Some(sid);
                        existing.provisional_since = None;
                        let updated = existing.clone();
                        change.version_after = bump(room);
                        change.participants_updated.push(updated);
                        change
                    }
                    _ => change,
                }
            }
            None => {
                let provisional_since = sid.is_none().then(Instant::now);
                let participant = VoiceParticipant {
                    user_id,
                    sid,
                    joined_at: Utc::now(),
                    provisional_since,
                    muted: false,
                    deafened: false,
                    is_bot: is_bot || user_id == MUSIC_BOT_ID,
                };
                room.participants.insert(user_id, participant.clone());
                change.version_after = bump(room);
                change.participants_added.push(participant);
                change
            }
        }
    }

    /// Removes the participant ONLY if it is provisional (INV-A1). Returns
    /// `(RoomChange, needs_reconcile)`: for a confirmed participant the change
    /// is empty and `needs_reconcile` is `true` (the caller schedules one).
    pub fn hint_leaving(
        &mut self,
        channel_id: ChannelId,
        user_id: UserId,
    ) -> (RoomChange, bool) {
        let Some(room) = self.rooms.get_mut(&channel_id) else {
            return (RoomChange::empty(channel_id, 0), false);
        };
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::WsPresenceHint;

        match room.participants.get(&user_id) {
            None => (change, false),
            Some(p) if !p.is_provisional() => (change, true), // confirmed: caller reconciles
            Some(_) => {
                room.participants.remove(&user_id);
                let owned: Vec<TrackSid> = room
                    .tracks
                    .values()
                    .filter(|t| t.owner == user_id)
                    .map(|t| t.sid.clone())
                    .collect();
                for sid in &owned {
                    room.tracks.remove(sid);
                }
                change.version_after = bump(room);
                change.participants_removed.push(user_id);
                change.tracks_removed = owned;
                self.close_if_empty(channel_id, &mut change);
                (change, false)
            }
        }
    }

    /// Client track hint. `track_sid` is required; `unpublished` validates the
    /// owner (INV-F1).
    pub fn hint_track(
        &mut self,
        channel_id: ChannelId,
        user_id: UserId,
        track_sid: TrackSid,
        source: TrackSource,
        published: bool,
    ) -> Result<RoomChange, HintError> {
        let Some(room) = self.rooms.get_mut(&channel_id) else {
            return Err(if published { HintError::NotInCall } else { HintError::TrackNotFound });
        };
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::WsTrackHint;

        if published {
            let Some(owner) = room.participants.get(&user_id) else {
                return Err(HintError::NotInCall);
            };
            if room.tracks.contains_key(&track_sid) {
                return Ok(change);
            }
            let track = VoiceTrack {
                sid: track_sid.clone(),
                owner: user_id,
                owner_sid: owner.sid.clone(),
                source,
                muted: false,
            };
            room.tracks.insert(track_sid, track.clone());
            change.version_after = bump(room);
            change.tracks_added.push(track);
            Ok(change)
        } else {
            let Some(track) = room.tracks.get(&track_sid) else {
                return Err(HintError::TrackNotFound);
            };
            if track.owner != user_id {
                return Err(HintError::NotTrackOwner);
            }
            room.tracks.remove(&track_sid);
            change.version_after = bump(room);
            change.tracks_removed.push(track_sid);
            Ok(change)
        }
    }

    // ---------- Tupi-only state ----------

    pub fn set_audio_state(
        &mut self,
        channel_id: ChannelId,
        user_id: UserId,
        muted: Option<bool>,
        deafened: Option<bool>,
    ) -> Result<RoomChange, HintError> {
        let Some(room) = self.rooms.get_mut(&channel_id) else {
            return Err(HintError::NotInCall);
        };
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::WsStateUpdate;
        let Some(participant) = room.participants.get_mut(&user_id) else {
            return Err(HintError::NotInCall);
        };
        let mut touched = false;
        if let Some(m) = muted {
            if participant.muted != m {
                participant.muted = m;
                touched = true;
            }
        }
        if let Some(d) = deafened {
            if participant.deafened != d {
                participant.deafened = d;
                touched = true;
            }
        }
        if !touched {
            return Ok(change);
        }
        let updated = participant.clone();
        change.version_after = bump(room);
        change.participants_updated.push(updated);
        Ok(change)
    }

    /// The music bot's synthetic track row (the exception to INV-B1). Keyed by
    /// a stable `music:<channel>` sid so it never collides with a real one.
    pub fn set_music_row(&mut self, channel_id: ChannelId, playing: bool) -> RoomChange {
        let music_sid = format!("music:{channel_id}");
        if playing {
            let room = self.rooms.entry(channel_id).or_default();
            let mut change = RoomChange::empty(channel_id, room.version);
            change.reason = ChangeReason::WsTrackHint;
            if room.tracks.contains_key(&music_sid) {
                return change;
            }
            let track = VoiceTrack {
                sid: music_sid.clone(),
                owner: MUSIC_BOT_ID,
                owner_sid: None,
                source: TrackSource::Music,
                muted: false,
            };
            room.tracks.insert(music_sid, track.clone());
            change.version_after = bump(room);
            change.tracks_added.push(track);
            change
        } else {
            let Some(room) = self.rooms.get_mut(&channel_id) else {
                return RoomChange::empty(channel_id, 0);
            };
            let mut change = RoomChange::empty(channel_id, room.version);
            change.reason = ChangeReason::WsTrackHint;
            if room.tracks.remove(&music_sid).is_none() {
                return change;
            }
            change.version_after = bump(room);
            change.tracks_removed.push(music_sid);
            change
        }
    }

    // ---------- maintenance ----------

    /// Removes expired provisionals (INV-A2). Call on every reconcile tick.
    pub fn expire_provisionals(&mut self) -> Vec<RoomChange> {
        let mut changes = Vec::new();
        let channel_ids: Vec<ChannelId> = self.rooms.keys().copied().collect();
        for channel_id in channel_ids {
            let room = self.rooms.get_mut(&channel_id).expect("channel just enumerated");
            let expired: Vec<UserId> = room
                .participants
                .values()
                .filter(|p| {
                    p.provisional_since
                        .is_some_and(|since| since.elapsed() >= PROVISIONAL_TTL)
                })
                .map(|p| p.user_id)
                .collect();
            if expired.is_empty() {
                continue;
            }
            let mut change = RoomChange::empty(channel_id, room.version);
            change.reason = ChangeReason::ProvisionalExpired;
            for user_id in &expired {
                room.participants.remove(user_id);
                let owned: Vec<TrackSid> = room
                    .tracks
                    .values()
                    .filter(|t| t.owner == *user_id)
                    .map(|t| t.sid.clone())
                    .collect();
                for sid in &owned {
                    room.tracks.remove(sid);
                }
                change.participants_removed.push(*user_id);
                change.tracks_removed.extend(owned);
            }
            change.version_after = bump(room);
            self.close_if_empty(channel_id, &mut change);
            changes.push(change);
        }
        changes
    }

    /// A channel deleted from the database: close the room.
    pub fn close_channel(&mut self, channel_id: ChannelId) -> RoomChange {
        let Some(room) = self.rooms.remove(&channel_id) else {
            return RoomChange::empty(channel_id, 0);
        };
        let mut change = RoomChange::empty(channel_id, room.version);
        change.reason = ChangeReason::ChannelDeleted;
        change.version_after = room.version + 1;
        change.participants_removed = room.participants.keys().copied().collect();
        change.participants_removed.sort();
        change.tracks_removed = room.tracks.keys().cloned().collect();
        change.tracks_removed.sort();
        change.room_closed = true;
        change
    }

    /// Webhook dedupe. `true` = already seen, discard.
    pub fn is_duplicate_event(&mut self, key: &str) -> bool {
        if self.seen_events.contains(key) {
            return true;
        }
        self.seen_events.insert(key.to_string());
        self.seen_order.push_back(key.to_string());
        while self.seen_order.len() > SEEN_EVENTS_CAP {
            if let Some(old) = self.seen_order.pop_front() {
                self.seen_events.remove(&old);
            }
        }
        false
    }

    // ---------- internal ----------

    /// Drops the room (and marks the change `room_closed`) once its last
    /// participant is gone. A room with only the music row and no participants
    /// does not survive — the bot is itself a participant while it plays.
    fn close_if_empty(&mut self, channel_id: ChannelId, change: &mut RoomChange) {
        if change.is_empty() {
            return;
        }
        if self
            .rooms
            .get(&channel_id)
            .is_some_and(|room| room.participants.is_empty())
        {
            self.rooms.remove(&channel_id);
            change.room_closed = true;
        }
    }
}

fn bump(room: &mut VoiceRoom) -> u64 {
    room.version += 1;
    room.version
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uid(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }
    fn chan() -> ChannelId {
        Uuid::from_u128(9000)
    }

    /// Backdates a participant's `provisional_since` so expiry logic can be
    /// tested without sleeping.
    fn age_provisional(reg: &mut VoiceRegistry, channel_id: ChannelId, user_id: UserId) {
        let past = Instant::now()
            .checked_sub(PROVISIONAL_TTL + Duration::from_secs(1))
            .expect("machine uptime exceeds the provisional TTL");
        reg.rooms
            .get_mut(&channel_id)
            .unwrap()
            .participants
            .get_mut(&user_id)
            .unwrap()
            .provisional_since = Some(past);
    }

    // ---- §8: ChangeReason wire values ----

    #[test]
    fn change_reason_serializes_to_protocol_values() {
        let expected = [
            (ChangeReason::WebhookParticipantJoined, "webhook.participant_joined"),
            (ChangeReason::WebhookParticipantLeft, "webhook.participant_left"),
            (ChangeReason::WebhookTrackPublished, "webhook.track_published"),
            (ChangeReason::WebhookTrackUnpublished, "webhook.track_unpublished"),
            (ChangeReason::WebhookTrackMuted, "webhook.track_muted"),
            (ChangeReason::ReconcileAdded, "reconcile.added"),
            (ChangeReason::ReconcileRemoved, "reconcile.removed"),
            (ChangeReason::ReconcileTrackSync, "reconcile.track_sync"),
            (ChangeReason::WsPresenceHint, "ws.presence_hint"),
            (ChangeReason::WsStateUpdate, "ws.state_update"),
            (ChangeReason::WsTrackHint, "ws.track_hint"),
            (ChangeReason::AdminDisconnectMember, "admin.disconnect_member"),
            (ChangeReason::ChannelDeleted, "channel.deleted"),
            (ChangeReason::ProvisionalExpired, "provisional.expired"),
        ];
        for (reason, wire) in expected {
            assert_eq!(serde_json::to_value(reason).unwrap(), serde_json::json!(wire));
        }
    }

    // ---- U-01 / INV-B2 ----

    #[test]
    fn webhook_join_then_stale_left_keeps_participant() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_new".into());
        let change = reg.webhook_participant_left(chan(), uid(1), "PA_old".into());
        assert!(change.is_empty(), "a left with a stale sid must be ignored");
        assert!(reg.is_participant(chan(), uid(1)));
    }

    #[test]
    fn authoritative_join_evicts_same_user_from_the_previous_channel() {
        let mut reg = VoiceRegistry::default();
        let source = chan();
        let destination = Uuid::from_u128(9001);
        reg.webhook_participant_joined(source, uid(1), "PA_source".into());
        reg.webhook_track_published(source, uid(1), Some("PA_source".into()), "TR_source".into(), TrackSource::ScreenShare);

        let changes = reg.webhook_participant_joined_exclusive(destination, uid(1), "PA_destination".into());

        assert_eq!(changes.len(), 2, "the old room and the destination both publish a delta");
        assert!(!reg.is_participant(source, uid(1)));
        assert!(reg.is_participant(destination, uid(1)));
        assert!(reg.room(source).is_none(), "the stale track leaves with the old participant");
    }

    // ---- U-02 ----

    #[test]
    fn webhook_left_with_current_sid_removes_participant_and_tracks() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        reg.webhook_track_published(chan(), uid(1), Some("PA_1".into()), "TR_1".into(), TrackSource::ScreenShare);
        let change = reg.webhook_participant_left(chan(), uid(1), "PA_1".into());
        assert_eq!(change.participants_removed, vec![uid(1)]);
        assert_eq!(change.tracks_removed, vec!["TR_1".to_string()]);
        assert!(change.room_closed, "the last participant left");
        assert!(reg.room(chan()).is_none());
    }

    // ---- U-03 / RC-03 / §7 ----

    #[test]
    fn republish_track_uses_new_sid_and_old_is_removed() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        let v0 = reg.version(chan());
        reg.webhook_track_published(chan(), uid(1), Some("PA_1".into()), "TR_1".into(), TrackSource::ScreenShare);
        reg.webhook_track_unpublished(chan(), "TR_1");
        reg.webhook_track_published(chan(), uid(1), Some("PA_1".into()), "TR_2".into(), TrackSource::ScreenShare);
        let room = reg.room(chan()).unwrap();
        assert_eq!(room.tracks.keys().collect::<Vec<_>>(), vec!["TR_2"]);
        assert_eq!(room.version, v0 + 3, "3 accepted mutations");
    }

    #[test]
    fn out_of_order_republish_keeps_only_the_new_track() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        reg.webhook_track_published(chan(), uid(1), Some("PA_1".into()), "TR_1".into(), TrackSource::ScreenShare);
        // New track published before the old one is torn down.
        reg.webhook_track_published(chan(), uid(1), Some("PA_1".into()), "TR_2".into(), TrackSource::ScreenShare);
        reg.webhook_track_unpublished(chan(), "TR_1");
        let room = reg.room(chan()).unwrap();
        assert_eq!(room.tracks.keys().collect::<Vec<_>>(), vec!["TR_2"]);
    }

    // ---- U-04 ----

    #[test]
    fn track_unpublished_for_unknown_sid_is_noop() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        let v = reg.version(chan());
        let change = reg.webhook_track_unpublished(chan(), "TR_ghost");
        assert!(change.is_empty());
        assert_eq!(reg.version(chan()), v);
    }

    // ---- §4.4 / §6.1: track webhook before join ----

    #[test]
    fn track_published_before_join_creates_provisional_owner() {
        let mut reg = VoiceRegistry::default();
        let change = reg.webhook_track_published(
            chan(),
            uid(1),
            None,
            "TR_1".into(),
            TrackSource::ScreenShare,
        );
        assert_eq!(change.participants_added.len(), 1);
        assert!(change.participants_added[0].is_provisional());
        assert_eq!(change.tracks_added.len(), 1);
        assert_eq!(change.version_after - change.version_before, 2);
    }

    // ---- U-06 / INV-A2 ----

    #[test]
    fn provisional_expires_after_ten_seconds() {
        let mut reg = VoiceRegistry::default();
        reg.hint_joining(chan(), uid(1), None, false);
        age_provisional(&mut reg, chan(), uid(1));
        let changes = reg.expire_provisionals();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].participants_removed, vec![uid(1)]);
        assert!(reg.room(chan()).is_none(), "room emptied and closed");
    }

    #[test]
    fn fresh_provisional_is_not_expired() {
        let mut reg = VoiceRegistry::default();
        reg.hint_joining(chan(), uid(1), None, false);
        assert!(reg.expire_provisionals().is_empty());
        assert!(reg.is_participant(chan(), uid(1)));
    }

    // ---- U-07 ----

    #[test]
    fn provisional_is_confirmed_by_webhook_without_duplicating() {
        let mut reg = VoiceRegistry::default();
        reg.hint_joining(chan(), uid(1), None, false);
        let change = reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        assert_eq!(change.participants_updated.len(), 1);
        assert!(change.participants_added.is_empty());
        let room = reg.room(chan()).unwrap();
        assert_eq!(room.participants.len(), 1);
        assert!(!room.participants[&uid(1)].is_provisional());
    }

    // ---- U-08 / INV-A1 / §7 ----

    #[test]
    fn leaving_hint_does_not_remove_confirmed_participant() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        let (change, needs_reconcile) = reg.hint_leaving(chan(), uid(1));
        assert!(change.is_empty());
        assert!(needs_reconcile);
        assert!(reg.is_participant(chan(), uid(1)));
    }

    // ---- U-09 / §7 ----

    #[test]
    fn leaving_hint_removes_provisional_participant() {
        let mut reg = VoiceRegistry::default();
        reg.hint_joining(chan(), uid(1), None, false);
        let v = reg.version(chan());
        let (change, needs_reconcile) = reg.hint_leaving(chan(), uid(1));
        assert!(!needs_reconcile);
        assert_eq!(change.participants_removed, vec![uid(1)]);
        assert!(change.version_after > v);
    }

    #[test]
    fn joining_hint_never_overrides_a_confirmed_participant() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_real".into());
        let v = reg.version(chan());
        let change = reg.hint_joining(chan(), uid(1), Some("PA_spoofed".into()), false);
        assert!(change.is_empty());
        assert_eq!(
            reg.room(chan()).unwrap().participants[&uid(1)].sid.as_deref(),
            Some("PA_real")
        );
        assert_eq!(reg.version(chan()), v);
    }

    // ---- U-10 (adapted) / §7 ----

    #[test]
    fn reconcile_preserves_mute_deafen_and_music_row() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        reg.set_audio_state(chan(), uid(1), Some(true), Some(true)).unwrap();
        reg.set_music_row(chan(), true);

        reg.reconcile_room(
            chan(),
            vec![ReconciledParticipant { user_id: uid(1), sid: "PA_1".into(), tracks: vec![] }],
        );

        let room = reg.room(chan()).unwrap();
        assert!(room.participants[&uid(1)].muted);
        assert!(room.participants[&uid(1)].deafened);
        assert!(room.tracks.values().any(|t| t.source == TrackSource::Music));
    }

    // ---- U-11 ----

    #[test]
    fn reconcile_removes_participants_livekit_no_longer_lists() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        reg.webhook_participant_joined(chan(), uid(2), "PA_2".into());
        let change = reg.reconcile_room(
            chan(),
            vec![ReconciledParticipant { user_id: uid(1), sid: "PA_1".into(), tracks: vec![] }],
        );
        assert_eq!(change.participants_removed, vec![uid(2)]);
        assert!(!reg.is_participant(chan(), uid(2)));
    }

    // ---- U-12 / INV-C2 / §7 ----

    #[test]
    fn reconcile_is_noop_when_in_sync_and_does_not_bump_version() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        let v = reg.version(chan());
        let change = reg.reconcile_room(
            chan(),
            vec![ReconciledParticipant { user_id: uid(1), sid: "PA_1".into(), tracks: vec![] }],
        );
        assert!(change.is_empty());
        assert_eq!(reg.version(chan()), v);
        assert!(reg.room(chan()).unwrap().reconciled_at.is_some());
    }

    // ---- U-13 / INV-C2 ----

    #[test]
    fn version_increases_by_one_per_accepted_mutation() {
        let mut reg = VoiceRegistry::default();
        assert_eq!(reg.version(chan()), 0);
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        assert_eq!(reg.version(chan()), 1);
        reg.webhook_participant_joined(chan(), uid(2), "PA_2".into());
        assert_eq!(reg.version(chan()), 2);
        reg.set_audio_state(chan(), uid(1), Some(true), None).unwrap();
        assert_eq!(reg.version(chan()), 3);
        // A no-op does not bump.
        reg.set_audio_state(chan(), uid(1), Some(true), None).unwrap();
        assert_eq!(reg.version(chan()), 3);
    }

    // ---- U-15 ----

    #[test]
    fn duplicate_webhook_event_id_is_ignored() {
        let mut reg = VoiceRegistry::default();
        assert!(!reg.is_duplicate_event("evt-1"));
        assert!(reg.is_duplicate_event("evt-1"));
        assert!(!reg.is_duplicate_event("evt-2"));
    }

    #[test]
    fn webhook_dedupe_ring_is_bounded() {
        let mut reg = VoiceRegistry::default();
        for i in 0..(SEEN_EVENTS_CAP + 50) {
            assert!(!reg.is_duplicate_event(&format!("evt-{i}")));
        }
        // The oldest entries have been evicted and are accepted again.
        assert!(!reg.is_duplicate_event("evt-0"));
        // A recent one is still remembered.
        assert!(reg.is_duplicate_event(&format!("evt-{}", SEEN_EVENTS_CAP + 40)));
    }

    // ---- U-16 / §7 ----

    #[test]
    fn music_row_survives_track_reconcile() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        reg.set_music_row(chan(), true);
        reg.reconcile_room(
            chan(),
            vec![ReconciledParticipant {
                user_id: uid(1),
                sid: "PA_1".into(),
                tracks: vec![("TR_cam".into(), TrackSource::Camera, false)],
            }],
        );
        let room = reg.room(chan()).unwrap();
        assert!(room.tracks.values().any(|t| t.source == TrackSource::Music));
        assert!(room.tracks.contains_key("TR_cam"));
    }

    // ---- U-18 / INV-F2 ----

    #[test]
    fn channel_full_is_computed_from_confirmed_non_bot_participants() {
        let mut reg = VoiceRegistry::default();
        for i in 0..MAX_HUMAN_PARTICIPANTS as u128 {
            reg.webhook_participant_joined(chan(), uid(100 + i), format!("PA_{i}"));
        }
        assert!(reg.is_full(chan()));
        assert_eq!(reg.human_count(chan()), MAX_HUMAN_PARTICIPANTS);

        // A provisional and the bot do not count toward the cap.
        let mut reg2 = VoiceRegistry::default();
        reg2.webhook_participant_joined(chan(), MUSIC_BOT_ID, "PA_bot".into());
        reg2.hint_joining(chan(), uid(1), None, false);
        assert_eq!(reg2.human_count(chan()), 0);
        assert!(!reg2.is_full(chan()));
    }

    // ---- §7: hint_track ownership ----

    #[test]
    fn hint_track_unpublish_by_non_owner_is_rejected() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        reg.webhook_participant_joined(chan(), uid(2), "PA_2".into());
        reg.hint_track(chan(), uid(1), "TR_1".into(), TrackSource::ScreenShare, true).unwrap();
        let v = reg.version(chan());

        let err = reg
            .hint_track(chan(), uid(2), "TR_1".into(), TrackSource::ScreenShare, false)
            .unwrap_err();
        assert_eq!(err, HintError::NotTrackOwner);
        assert!(reg.room(chan()).unwrap().tracks.contains_key("TR_1"));
        assert_eq!(reg.version(chan()), v);
    }

    #[test]
    fn hint_track_publish_without_membership_is_rejected() {
        let mut reg = VoiceRegistry::default();
        let err = reg
            .hint_track(chan(), uid(1), "TR_1".into(), TrackSource::ScreenShare, true)
            .unwrap_err();
        assert_eq!(err, HintError::NotInCall);
    }

    // ---- §7: deterministic serialization ----

    #[test]
    fn snapshot_serialization_is_deterministic_regardless_of_insertion_order() {
        let build = |order: [u128; 3]| {
            let mut reg = VoiceRegistry::default();
            for n in order {
                reg.webhook_participant_joined(chan(), uid(n), format!("PA_{n}"));
                reg.webhook_track_published(
                    chan(),
                    uid(n),
                    Some(format!("PA_{n}")),
                    format!("TR_{n}"),
                    TrackSource::ScreenShare,
                );
            }
            reg.room(chan()).unwrap().to_dto(chan())
        };

        let a = build([3, 1, 2]);
        let b = build([1, 2, 3]);

        assert_eq!(
            a.participants.iter().map(|p| p.user_id).collect::<Vec<_>>(),
            vec![uid(1), uid(2), uid(3)],
            "participants must be sorted by user_id"
        );
        assert_eq!(
            a.participants.iter().map(|p| p.user_id).collect::<Vec<_>>(),
            b.participants.iter().map(|p| p.user_id).collect::<Vec<_>>(),
        );
        assert_eq!(
            a.tracks.iter().map(|t| t.track_sid.clone()).collect::<Vec<_>>(),
            b.tracks.iter().map(|t| t.track_sid.clone()).collect::<Vec<_>>(),
        );
        assert!(a.tracks.windows(2).all(|w| w[0].track_sid <= w[1].track_sid));
    }

    // ---- §6.11 / §6.12: no-ops on missing rooms ----

    #[test]
    fn maintenance_ops_on_missing_room_are_empty_and_create_nothing() {
        let mut reg = VoiceRegistry::default();
        assert!(reg.close_channel(chan()).is_empty());
        assert!(reg.set_music_row(chan(), false).is_empty());
        assert!(reg.room(chan()).is_none());
    }

    #[test]
    fn close_channel_reports_everyone_removed_and_closes_room() {
        let mut reg = VoiceRegistry::default();
        reg.webhook_participant_joined(chan(), uid(1), "PA_1".into());
        reg.webhook_participant_joined(chan(), uid(2), "PA_2".into());
        let change = reg.close_channel(chan());
        assert!(change.room_closed);
        assert_eq!(change.participants_removed, vec![uid(1), uid(2)]);
        assert!(reg.room(chan()).is_none());
    }
}
