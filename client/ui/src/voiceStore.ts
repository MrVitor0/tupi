/**
 * Voice state, outside React.
 *
 * Two deliberately different sources (tupi-v2-refactor/03-target-architecture.md §1):
 *
 *   - `rooms`: channels I am NOT in. The server's projection
 *     (voice.room.state / voice.room.delta, or v1 voice.rooms / voice.roster).
 *     Can lag; the per-channel `version` makes loss detectable (INV-C2).
 *
 *   - `session`: the channel I AM in. Derived from the LiveKit Room's
 *     `remoteParticipants` (fed by rtc.ts), with mute/deafen/bot overlaid from
 *     the server projection. It is the same structure the audio comes from, so
 *     a ghost is impossible (INV-C1).
 */
import { useRef } from "react";
import { useSyncExternalStore } from "react";

import { hasFeature } from "./serverInfo";
import { send, subscribe } from "./ipc";
import { logClient, maybeAutoSend } from "./clientLog";

const MUSIC_BOT_ID = "00000000-0000-0000-0000-000000000001";

export type TrackSource = "microphone" | "camera" | "screen_share" | "screen_share_audio" | "music";
const TRACK_SOURCES: readonly TrackSource[] = [
  "microphone",
  "camera",
  "screen_share",
  "screen_share_audio",
  "music",
];

export type RosterParticipant = {
  userId: string;
  participantSid: string | null;
  joinedAt: string;
  muted: boolean;
  deafened: boolean;
  isBot: boolean;
  provisional: boolean;
};

export type RosterTrack = {
  trackSid: string;
  owner: string;
  source: TrackSource;
  muted: boolean;
};

export type RoomProjection = {
  channelId: string;
  version: number;
  participants: RosterParticipant[];
  tracks: RosterTrack[];
};

/** Someone I am actually hearing right now, from LiveKit. */
export type LiveParticipant = {
  userId: string;
  participantSid: string;
  isLocal: boolean;
  /** Server overlay; neutral until the server says otherwise. */
  muted: boolean;
  deafened: boolean;
  isBot: boolean;
};

export type VoiceState = {
  /** Channels I am not in, keyed by channelId. */
  rooms: Record<string, RoomProjection>;
  /** The channel I am in, or null. */
  session: {
    channelId: string | null;
    participants: LiveParticipant[];
  };
  /** User ids currently making sound. A slice of its own so a speaking change
   *  re-renders one row, not the tree (SPEC-013 §4.5). */
  speaking: Set<string>;
};

// ---- wire shapes ----

type ParticipantWire = {
  user_id: string;
  participant_sid?: string | null;
  joined_at?: string;
  muted?: boolean;
  deafened?: boolean;
  is_bot?: boolean;
  provisional?: boolean;
};
type TrackWire = {
  track_sid: string;
  owner: string;
  source: string;
  muted?: boolean;
};
type RoomWire = {
  channel_id: string;
  version: number;
  participants: ParticipantWire[];
  tracks: TrackWire[];
};
type DeltaWire = {
  channel_id: string;
  version: number;
  previous_version: number;
  participants_added: ParticipantWire[];
  participants_updated: ParticipantWire[];
  participants_removed: string[];
  tracks_added: TrackWire[];
  tracks_removed: string[];
  reason?: string;
};
type V1StreamWire = { stream_id: string; owner: string; kind: string; msid?: string | null; has_audio?: boolean };
type V1RoomWire = {
  channel_id: string;
  participants: Array<{ user_id: string; muted?: boolean; deafened?: boolean; is_bot?: boolean; sharing?: boolean }>;
  streams: V1StreamWire[];
};

// ---- module state ----

let state: VoiceState = { rooms: {}, session: { channelId: null, participants: [] }, speaking: new Set<string>() };
const listeners = new Set<(state: VoiceState) => void>();
/** Channels awaiting a snapshot after a version gap; their deltas are dropped
 *  until it lands (protocol §2.2). */
const awaitingSnapshot = new Set<string>();
// `null` means no request has been sent yet. Using zero here delayed the first
// request under fake clocks (and can do the same after a clock correction).
let lastRequestAt: number | null = null;
let pendingSnapshotIds = new Set<string>();
let pendingFullSnapshot = false;
let pendingSnapshotReason = "";
let snapshotRequestTimer: ReturnType<typeof setTimeout> | null = null;
let started = false;
/** The last live-participant list rtc.ts reported, so `emit` can recompute the
 *  session overlay when a delta changes someone's mute state. */
let lastLive: { channelId: string; entries: Array<{ identity: string; sid: string; isLocal: boolean }> } | null = null;

export function getState(): VoiceState {
  return state;
}

/** Test hook only: wipes module state so each case starts clean. */
export function __resetForTest() {
  state = { rooms: {}, session: { channelId: null, participants: [] }, speaking: new Set<string>() };
  listeners.clear();
  awaitingSnapshot.clear();
  lastRequestAt = null;
  pendingSnapshotIds.clear();
  pendingFullSnapshot = false;
  pendingSnapshotReason = "";
  if (snapshotRequestTimer !== null) clearTimeout(snapshotRequestTimer);
  snapshotRequestTimer = null;
  started = false;
  lastLive = null;
}

export function subscribeVoice(listener: (state: VoiceState) => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

function emit() {
  listeners.forEach(listener => listener(state));
}

function toParticipant(wire: ParticipantWire): RosterParticipant {
  return {
    userId: wire.user_id,
    participantSid: wire.participant_sid ?? null,
    joinedAt: wire.joined_at ?? "",
    muted: wire.muted ?? false,
    deafened: wire.deafened ?? false,
    isBot: wire.is_bot ?? wire.user_id === MUSIC_BOT_ID,
    provisional: wire.provisional ?? false,
  };
}

/** Returns `null` for a source the UI can't render — those tracks are dropped. */
function toTrack(wire: TrackWire): RosterTrack | null {
  const source = wire.source as TrackSource;
  if (!TRACK_SOURCES.includes(source)) return null;
  return { trackSid: wire.track_sid, owner: wire.owner, source, muted: wire.muted ?? false };
}

function toProjection(room: RoomWire): RoomProjection {
  const participants = room.participants.map(toParticipant);
  const tracks = room.tracks.map(toTrack).filter((t): t is RosterTrack => t !== null);
  participants.sort((a, b) => a.userId.localeCompare(b.userId));
  tracks.sort((a, b) => a.trackSid.localeCompare(b.trackSid));
  return { channelId: room.channel_id, version: room.version, participants, tracks };
}

function fromV1(room: V1RoomWire): RoomProjection {
  const participants: RosterParticipant[] = (room.participants ?? []).map(p => ({
    userId: p.user_id,
    participantSid: null,
    joinedAt: "",
    muted: p.muted ?? false,
    deafened: p.deafened ?? false,
    isBot: p.is_bot ?? p.user_id === MUSIC_BOT_ID,
    provisional: false,
  }));
  const tracks: RosterTrack[] = [];
  for (const stream of room.streams ?? []) {
    const owner = stream.owner;
    const msid = stream.msid ?? stream.stream_id;
    if (stream.kind === "screen") {
      tracks.push({ trackSid: msid, owner, source: "screen_share", muted: false });
      if (stream.has_audio) {
        tracks.push({ trackSid: `${msid}:audio`, owner, source: "screen_share_audio", muted: false });
      }
    } else if (stream.kind === "camera") {
      tracks.push({ trackSid: msid, owner, source: "camera", muted: false });
    } else if (stream.kind === "music") {
      tracks.push({ trackSid: msid, owner, source: "music", muted: false });
    }
  }
  participants.sort((a, b) => a.userId.localeCompare(b.userId));
  tracks.sort((a, b) => a.trackSid.localeCompare(b.trackSid));
  return { channelId: room.channel_id, version: 0, participants, tracks };
}

// ---- v2 application ----

function applyState(payload: { full: boolean; rooms: RoomWire[]; channel_ids?: string[] }) {
  // An unscoped state is the whole community and replaces local state (including
  // after a server restart). A scoped response is the answer to a targeted
  // `voice.room.request`: merge it and explicitly delete requested rooms the
  // server omitted because they became empty. Treating a scoped answer as global
  // used to wipe unrelated sidebar channels.
  const rooms: Record<string, RoomProjection> = {};
  for (const room of payload.rooms ?? []) rooms[room.channel_id] = toProjection(room);
  const scope = Array.isArray(payload.channel_ids) ? payload.channel_ids : null;
  if (scope) {
    const merged = { ...state.rooms };
    for (const id of scope) {
      delete merged[id];
      awaitingSnapshot.delete(id);
    }
    Object.assign(merged, rooms);
    state = { ...state, rooms: merged };
  } else {
    state = { ...state, rooms };
    awaitingSnapshot.clear();
  }
  recomputeSession();
  emit();
}

function applyOne(room: RoomProjection, delta: DeltaWire): RoomProjection {
  let participants = room.participants;
  let tracks = room.tracks;

  if (delta.participants_removed.length) {
    const removed = new Set(delta.participants_removed);
    participants = participants.filter(p => !removed.has(p.userId));
  }
  if (delta.tracks_removed.length) {
    const removed = new Set(delta.tracks_removed);
    tracks = tracks.filter(t => !removed.has(t.trackSid));
  }
  for (const wire of delta.participants_added) {
    const entry = toParticipant(wire);
    participants = [...participants.filter(p => p.userId !== entry.userId), entry];
  }
  for (const wire of delta.participants_updated) {
    const entry = toParticipant(wire);
    participants = participants.map(p => (p.userId === entry.userId ? entry : p));
  }
  for (const wire of delta.tracks_added) {
    const entry = toTrack(wire);
    if (!entry) continue;
    tracks = [...tracks.filter(t => t.trackSid !== entry.trackSid), entry];
  }

  participants = [...participants].sort((a, b) => a.userId.localeCompare(b.userId));
  tracks = [...tracks].sort((a, b) => a.trackSid.localeCompare(b.trackSid));
  return { channelId: room.channelId, version: delta.version, participants, tracks };
}

function applyDelta(delta: DeltaWire) {
  const channelId = delta.channel_id;
  if (awaitingSnapshot.has(channelId)) return;

  const local = state.rooms[channelId];
  if (!local) {
    requestSnapshot([channelId], "unknown_channel");
    return;
  }
  if (delta.previous_version === local.version) {
    state = { ...state, rooms: { ...state.rooms, [channelId]: applyOne(local, delta) } };
    recomputeSession();
    emit();
    return;
  }
  if (delta.version <= local.version) return; // redelivery
  logClient("voice.version_gap", {
    channel_id: channelId,
    local_version: local.version,
    received_previous: delta.previous_version,
    received_version: delta.version,
  });
  requestSnapshot([channelId], "version_gap");
  maybeAutoSend("version_gap");
}

// ---- v1 application ----

function applyV1Rooms(payload: { rooms: V1RoomWire[] }) {
  const rooms: Record<string, RoomProjection> = {};
  for (const room of payload.rooms ?? []) rooms[room.channel_id] = fromV1(room);
  state = { ...state, rooms };
  recomputeSession();
  emit();
}

function applyV1Roster(payload: V1RoomWire) {
  const projection = fromV1(payload);
  const rooms = { ...state.rooms };
  if (projection.participants.length === 0) delete rooms[payload.channel_id];
  else rooms[payload.channel_id] = projection;
  state = { ...state, rooms };
  recomputeSession();
  emit();
}

// ---- snapshot requests ----

function flushSnapshotRequest() {
  snapshotRequestTimer = null;
  const full = pendingFullSnapshot;
  const channelIds = full ? [] : [...pendingSnapshotIds];
  const reason = pendingSnapshotReason;
  pendingFullSnapshot = false;
  pendingSnapshotIds.clear();
  pendingSnapshotReason = "";
  lastRequestAt = Date.now();
  if (hasFeature("voice.room.v2")) send("voice.room.request", { channel_ids: channelIds });
  else send("voice.rooms.request", {});
  logClient("voice.snapshot_requested", { reason, channels: channelIds.length, full });
}

function requestSnapshot(channelIds: string[], reason: string) {
  for (const id of channelIds) awaitingSnapshot.add(id);
  const now = Date.now();
  if (channelIds.length === 0) pendingFullSnapshot = true;
  else channelIds.forEach(id => pendingSnapshotIds.add(id));
  pendingSnapshotReason = reason;

  const wait = lastRequestAt === null ? 0 : Math.max(0, 2000 - (now - lastRequestAt));
  if (wait === 0) {
    if (snapshotRequestTimer !== null) clearTimeout(snapshotRequestTimer);
    flushSnapshotRequest();
  } else if (snapshotRequestTimer === null) {
    // A second gap can name a different channel. Queue it for a real request;
    // the old implementation merely marked it awaiting forever.
    snapshotRequestTimer = setTimeout(flushSnapshotRequest, wait);
  }
}

/** Asks the server for a full snapshot of every visible channel. Used after a
 *  WebSocket reconnect or a machine wake, when local state may have missed
 *  deltas (SPEC-012). */
export function requestFullSnapshot(reason: string) {
  // The server treats an empty `channel_ids` as "all visible".
  requestSnapshot([], reason);
}

// ---- the session (INV-C1) ----

export function setLiveParticipants(
  channelId: string,
  live: Array<{ identity: string; sid: string; isLocal: boolean }>,
) {
  lastLive = { channelId, entries: live };
  recomputeSession();
  emit();
}

export function clearSession() {
  lastLive = null;
  state = { ...state, session: { channelId: null, participants: [] } };
  emit();
}

/** Rebuilds `state.session` from the last live list + the current server
 *  overlay. Participants come ONLY from `live`; the projection is metadata. */
function recomputeSession() {
  if (!lastLive) {
    if (state.session.channelId !== null || state.session.participants.length) {
      state = { ...state, session: { channelId: null, participants: [] } };
    }
    return;
  }
  const projection = state.rooms[lastLive.channelId];
  const participants: LiveParticipant[] = lastLive.entries.map(entry => {
    const overlay = projection?.participants.find(p => p.userId === entry.identity);
    return {
      userId: entry.identity,
      participantSid: entry.sid,
      isLocal: entry.isLocal,
      muted: overlay?.muted ?? false,
      deafened: overlay?.deafened ?? false,
      isBot: overlay?.isBot ?? entry.identity === MUSIC_BOT_ID,
    };
  });
  participants.sort((a, b) => a.userId.localeCompare(b.userId));
  state = { ...state, session: { channelId: lastLive.channelId, participants } };
}

// ---- boot ----

export function initVoiceStore(): void {
  if (started) return;
  started = true;
  subscribe(event => {
    switch (event.op) {
      case "voice.room.state":
        applyState(event.data);
        break;
      case "voice.room.delta":
        applyDelta(event.data);
        break;
      case "voice.rooms":
        if (!hasFeature("voice.room.v2")) applyV1Rooms(event.data);
        break;
      case "voice.roster":
        if (!hasFeature("voice.room.v2")) applyV1Roster(event.data);
        break;
    }
  });
}

// ---- derived views the React tree consumes (SPEC-008 §4.6) ----

export type StreamInfoView = { stream_id: string; owner: string; kind: string; msid?: string | null };
export type RosterEntryView = { user_id: string; muted: boolean; deafened: boolean; sharing: boolean; is_bot?: boolean };
export type ParticipantView = { user_id: string; muted: boolean; deafened: boolean; is_bot?: boolean };

function kindOf(source: TrackSource): string | null {
  if (source === "screen_share") return "screen";
  if (source === "camera") return "camera";
  if (source === "music") return "music";
  return null;
}

export function roomStreams(projection: RoomProjection | undefined): StreamInfoView[] {
  if (!projection) return [];
  const out: StreamInfoView[] = [];
  for (const track of projection.tracks) {
    const kind = kindOf(track.source);
    if (!kind) continue;
    out.push({ stream_id: track.trackSid, owner: track.owner, kind, msid: track.trackSid });
  }
  return out;
}

/**
 * Produces the community sidebar projection. A person can briefly appear in a
 * source and destination room while LiveKit delivers the two move webhooks in
 * reverse order. Render only their newest session during that window; the
 * registry's authoritative join path removes the stale row shortly after.
 */
export function roomsForDisplay(rooms: Record<string, RoomProjection>): Record<string, RoomProjection> {
  const newest = new Map<string, { channelId: string; joinedAt: string }>();
  for (const room of Object.values(rooms)) {
    for (const participant of room.participants) {
      const current = newest.get(participant.userId);
      if (!current || participant.joinedAt > current.joinedAt || (participant.joinedAt === current.joinedAt && room.channelId > current.channelId)) {
        newest.set(participant.userId, { channelId: room.channelId, joinedAt: participant.joinedAt });
      }
    }
  }

  const out: Record<string, RoomProjection> = {};
  for (const [channelId, room] of Object.entries(rooms)) {
    const participants = room.participants.filter(participant => newest.get(participant.userId)?.channelId === channelId);
    if (participants.length === room.participants.length) {
      out[channelId] = room;
      continue;
    }
    const visibleOwners = new Set(participants.map(participant => participant.userId));
    out[channelId] = { ...room, participants, tracks: room.tracks.filter(track => visibleOwners.has(track.owner)) };
  }
  return out;
}

export function roomRoster(projection: RoomProjection | undefined): RosterEntryView[] {
  if (!projection) return [];
  const sharers = new Set(
    projection.tracks
      .filter(t => t.source === "screen_share" || t.source === "camera")
      .map(t => t.owner),
  );
  return projection.participants.map(p => ({
    user_id: p.userId,
    muted: p.muted,
    deafened: p.deafened,
    sharing: sharers.has(p.userId),
    is_bot: p.isBot,
  }));
}

export function sessionParticipants(session: VoiceState["session"]): ParticipantView[] {
  return session.participants.map(p => ({
    user_id: p.userId,
    muted: p.muted,
    deafened: p.deafened,
    is_bot: p.isBot,
  }));
}

// ---- speaking slice (SPEC-013 §4.5) ----

/** Replaces the "who is speaking" set. Called by rtc.ts on every
 *  ActiveSpeakersChanged and every local speech-monitor transition. */
export function setSpeaking(ids: Set<string>) {
  // Skip the emit when nothing actually changed.
  if (ids.size === state.speaking.size && [...ids].every(id => state.speaking.has(id))) return;
  state = { ...state, speaking: ids };
  emit();
}

// ---- React selectors ----

/**
 * Subscribe to just one slice of the voice state. A selector that returns an
 * object or array MUST pass an `isEqual` — the default `Object.is` would treat
 * every fresh object as a change and defeat the point.
 */
export function useVoiceSelector<T>(
  selector: (state: VoiceState) => T,
  isEqual: (a: T, b: T) => boolean = Object.is,
): T {
  const lastRef = useRef<{ value: T } | null>(null);
  return useSyncExternalStore(
    subscribeVoice,
    () => {
      const next = selector(getState());
      if (lastRef.current && isEqual(lastRef.current.value, next)) return lastRef.current.value;
      lastRef.current = { value: next };
      return next;
    },
    () => selector(getState()),
  );
}

/** True iff this specific user is speaking. Re-renders only its own row. */
export function useIsSpeaking(userId: string): boolean {
  return useVoiceSelector(s => s.speaking.has(userId));
}
