/** SFU media adapter. WebSocket remains control-plane only; media connects directly to LiveKit. */
import { Room, RoomEvent, Track } from "livekit-client";
import type { RemoteVideoTrack } from "livekit-client";
import * as screenPublisher from "./screenPublisher";
import { AudioPipelineManager, type AudioPipelineStatus, type NoiseSuppressionMode } from "./audioPipeline";
import { send, subscribe } from "./ipc";
import * as callSession from "./callSession";
import { hasFeature } from "./serverInfo";
import * as voiceStore from "./voiceStore";
import * as remoteMedia from "./remoteMedia";
export { subscribeRemoteVideos, getRemoteVideos, findRemoteVideo, watchRequestedAt } from "./remoteMedia";
export type { RemoteVideo } from "./remoteMedia";
import { logClient } from "./clientLog";

export type ConnQuality = "good" | "medium" | "poor";
export type { EndReason } from "./callSession";
import type { EndReason } from "./callSession";
type DeviceLists = { audioInputs: MediaDeviceInfo[]; audioOutputs: MediaDeviceInfo[]; videoInputs: MediaDeviceInfo[] };

// The call's lifecycle now lives in `callSession` (SPEC-007). `rtc.ts` reads
// the active room through it and never mutates it directly.
function room(): Room | null { return callSession.activeRoom(); }
function channelId(): string | null { return callSession.snapshot().channelId; }
let controlPlaneSubscription: (() => void) | null = null;
let audioInputDeviceId = storedString("tk.audioInputDeviceId");
let audioOutputDeviceId = storedString("tk.audioOutputDeviceId");
let inputVolume = storedNumber("tk.inputVolume", 1);
let outputVolume = storedNumber("tk.outputVolume", 1);
const cameras = new Set<(stream: MediaStream | null) => void>();
const callEnded = new Set<(reason: EndReason) => void>();
const speakers = new Set<(ids: Set<string>) => void>();
// LiveKit's active-speaker event is authoritative for remote people, but it
// reaches us only after the SFU has received and classified a few audio
// packets. Keep the local microphone level separately so our own speaking
// ring reacts at capture time instead of a second later.
const remoteSpeakers = new Set<string>();
let localSpeakerIdentity: string | null = null;
let localSpeaking = false;
let localSpeakingUntil = 0;
let localAudioContext: AudioContext | null = null;
let localAnalyser: AnalyserNode | null = null;
let localSpeechFrame = 0;
const qualities = new Set<(quality: ConnQuality) => void>();
const mediaErrors = new Set<(message: string) => void>();
const audioPipelineStatusListeners = new Set<(status: AudioPipelineStatus) => void>();
const microphone = new AudioPipelineManager();
let noiseSuppressionMode: NoiseSuppressionMode = storedNoiseSuppressionMode();
let localMuted = false;
const volumes = new Map<string, number>(Object.entries(stored("tk.peerVolumes")));
const screenVolumes = new Map<string, number>(Object.entries(stored("tk.screenVolumes")));
const muted = new Map<string, boolean>(Object.entries(storedBooleans("tk.peerMuted")));
const screenMuted = new Map<string, boolean>(Object.entries(storedBooleans("tk.screenMuted")));
const audio = new Map<string, HTMLAudioElement[]>(), screenAudio = new Map<string, HTMLAudioElement[]>();
/**
 * Who we want to watch, and why. The hover preview and the "AO VIVO" button are
 * two independent reasons for the SAME subscription; without the counter,
 * leaving the hover cancelled the subscription the button had just created
 * (RC-17). The intent also survives a republish (applied from `TrackPublished`).
 */
export type WatchReason = "hover" | "stage";
const watchIntent = new Map<string, Set<WatchReason>>();
let locallyDeafened = false;

function stored(key: string): Record<string, number> { try { return JSON.parse(localStorage.getItem(key) || "{}"); } catch { return {}; } }
function storedBooleans(key: string): Record<string, boolean> {
  try {
    const value = JSON.parse(localStorage.getItem(key) || "{}");
    if (!value || typeof value !== "object") return {};
    const entries: Array<[string, boolean]> = [];
    for (const [id, muted] of Object.entries(value)) if (typeof muted === "boolean") entries.push([id, muted]);
    return Object.fromEntries(entries);
  } catch { return {}; }
}
function storedString(key: string): string | undefined { try { return localStorage.getItem(key) || undefined; } catch { return undefined; } }
function storedNumber(key: string, fallback: number): number {
  try {
    const raw = localStorage.getItem(key);
    if (raw === null) return fallback;
    const value = Number(raw);
    return Number.isFinite(value) ? value : fallback;
  } catch { return fallback; }
}
function storedNoiseSuppressionMode(): NoiseSuppressionMode {
  try {
    const value = localStorage.getItem("tk.noiseSuppressionMode");
    if (value === "rnnoise" || value === "off") return value;
    return "off";
  } catch { return "off"; }
}
function persist(key: string, values: Map<string, number>) { try { localStorage.setItem(key, JSON.stringify(Object.fromEntries(values))); } catch {} }
function persistBooleans(key: string, values: Map<string, boolean>) { try { localStorage.setItem(key, JSON.stringify(Object.fromEntries(values))); } catch {} }
function persistValue(key: string, value: string | number) { try { localStorage.setItem(key, String(value)); } catch {} }

microphone.onStatus(status => audioPipelineStatusListeners.forEach(listener => listener(status)));

function logAudio(event: string, fields: Record<string, unknown> = {}) { console.info(`[audio] ${event}`, fields); }

function emitSpeaking() {
  const ids = new Set(remoteSpeakers);
  if (localSpeaking && localSpeakerIdentity) ids.add(localSpeakerIdentity);
  speakers.forEach(listener => listener(ids));
  // A slice in the store, so a speaking change can re-render one row instead
  // of the whole tree (SPEC-013 §4.5).
  voiceStore.setSpeaking(ids);
}

function stopLocalSpeechMonitor() {
  if (localSpeechFrame) cancelAnimationFrame(localSpeechFrame);
  localSpeechFrame = 0;
  localAnalyser?.disconnect(); localAnalyser = null;
  const context = localAudioContext; localAudioContext = null;
  void context?.close().catch(() => {});
  localSpeakerIdentity = null;
  localSpeaking = false;
  localSpeakingUntil = 0;
  emitSpeaking();
}

function startLocalSpeechMonitor(activeRoom: Room, sessionId: number) {
  stopLocalSpeechMonitor();
  const track = [...activeRoom.localParticipant.audioTrackPublications.values()]
    .map(publication => publication.track)
    .find((candidate): candidate is NonNullable<typeof candidate> => !!candidate);
  if (!track) return;

  try {
    const context = new AudioContext();
    const analyser = context.createAnalyser();
    analyser.fftSize = 512;
    analyser.smoothingTimeConstant = 0.15;
    context.createMediaStreamSource(new MediaStream([track.mediaStreamTrack])).connect(analyser);
    localAudioContext = context;
    localAnalyser = analyser;
    localSpeakerIdentity = activeRoom.localParticipant.identity;
    // A7: the AudioContext and the pending frame are tracked resources, torn
    // down by callSession — never leaked on a failed/superseded join.
    callSession.registerResource(sessionId, () => stopLocalSpeechMonitor());
    void context.resume().catch(() => {});
    const samples = new Uint8Array(analyser.fftSize);
    const tick = () => {
      if (!callSession.isCurrent(sessionId) || localAnalyser !== analyser) return;
      analyser.getByteTimeDomainData(samples);
      let energy = 0;
      for (const sample of samples) { const value = (sample - 128) / 128; energy += value * value; }
      // -36 dB RMS: reacts to normal speech while ignoring the capture floor.
      const speaking = Math.sqrt(energy / samples.length) > 0.016;
      const now = performance.now();
      if (speaking) localSpeakingUntil = now + 180;
      const next = speaking || now < localSpeakingUntil;
      if (next !== localSpeaking) { localSpeaking = next; emitSpeaking(); }
      localSpeechFrame = requestAnimationFrame(tick);
    };
    localSpeechFrame = requestAnimationFrame(tick);
  } catch {
    // The SFU event remains as the no-permission/no-WebAudio fallback.
  }
}

/** Requests a LiveKit token over IPC. Exported for `spectator.ts`, which mints
 *  its own `mode: "spectator"` credential. */
export async function mintCredentials(channel_id: string, mode = "participant") {
  return credentials(channel_id, mode);
}
async function credentials(channel_id: string, mode = "participant") {
  const request_id = crypto.randomUUID();
  return new Promise<{ url: string; token: string }>((resolve, reject) => {
    const timer = setTimeout(() => { off(); reject(new Error("LiveKit token timeout")); }, 10_000);
    const off = subscribe(event => {
      if (event.op !== "livekit.token" || event.data.request_id !== request_id) return;
      clearTimeout(timer); off();
      const url = typeof event.data.url === "string" ? event.data.url.trim() : "";
      const token = typeof event.data.access_token === "string" ? event.data.access_token : "";
      if (!url || !token) { reject(new Error("LiveKit retornou uma credencial incompleta")); return; }
      try { new URL(url); } catch { reject(new Error(`LiveKit retornou URL inválida: ${url}`)); return; }
      resolve({ url, token });
    });
    send("livekit.token.request", { request_id, channel_id, mode });
  });
}

function apply(id: string, isScreen = false) {
  for (const element of (isScreen ? screenAudio : audio).get(id) || []) {
    element.volume = ((isScreen ? screenVolumes : volumes).get(id) ?? 1) * outputVolume;
    element.muted = locallyDeafened || (isScreen ? screenMuted : muted).get(id) === true;
    void setSink(element);
  }
}
async function setSink(element: HTMLMediaElement) {
  const sink = (element as HTMLMediaElement & { setSinkId?: (id: string) => Promise<void> }).setSinkId;
  if (audioOutputDeviceId && sink) await sink.call(element, audioOutputDeviceId).catch(() => {});
}
/**
 * Media-plane wiring only. Connection lifecycle (Disconnected / Reconnecting /
 * Reconnected / teardown) belongs to `callSession` now (SPEC-007). Every
 * handler that touches shared speaking/remote state guards on
 * `callSession.isCurrent(sessionId)` so a stale room can't write.
 */
function bindMedia(room: Room, sessionId: number) {
  // INV-C1: the session participant list is the LiveKit Room's, nothing else.
  // Feed it to the store on every membership change.
  const syncParticipants = () => {
    if (!callSession.isCurrent(sessionId)) return;
    const channel = callSession.snapshot().channelId;
    if (!channel) return;
    voiceStore.setLiveParticipants(channel, [
      { identity: room.localParticipant.identity, sid: room.localParticipant.sid ?? "", isLocal: true },
      ...[...room.remoteParticipants.values()].map(p => ({ identity: p.identity, sid: p.sid, isLocal: false })),
    ]);
  };
  room.on(RoomEvent.ParticipantConnected, syncParticipants);
  room.on(RoomEvent.ParticipantDisconnected, syncParticipants);
  room.on(RoomEvent.Reconnected, () => {
    syncParticipants();
    // A reconnect can restore pre-existing publications without replaying a
    // TrackPublished event. Reapply the user's explicit watch intent.
    for (const ownerId of watchIntent.keys()) applySubscription(ownerId, true);
  });
  room.on(RoomEvent.ConnectionStateChanged, syncParticipants);
  if (sessionId !== 0) {
    syncParticipants();
    callSession.registerResource(sessionId, () => voiceStore.clearSession());
  }

  room.on(RoomEvent.TrackPublished, (publication, participant) => {
    if (publication.source !== Track.Source.ScreenShare) return;
    if (watchIntent.has(participant.identity)) void publication.setSubscribed(true);
  });
  // `TrackPublished` is not replayed for a share that already existed when we
  // joined. This closes the race where "Assistir" is clicked while connecting.
  for (const ownerId of watchIntent.keys()) applySubscription(ownerId, true);
  room.on(RoomEvent.TrackSubscribed, (track, publication, participant) => {
    if (track.kind === Track.Kind.Video) {
      // RC-12: do NOT attach here. The component that displays the video is the
      // one that attaches, so the SDK observes the element the user sees and
      // `adaptiveStream` reports visibility correctly.
      remoteMedia.addRemoteVideo({
        ownerId: participant.identity,
        trackSid: publication.trackSid,
        source: publication.source === Track.Source.Camera ? "camera" : "screen_share",
        track: track as RemoteVideoTrack,
        roomKey: "call",
      });
      logClient("watch.subscribed", { owner: participant.identity, track_sid: publication.trackSid });
      return;
    }
    // Audio: hidden element on the body (adaptiveStream does not manage audio).
    const element = track.attach() as HTMLAudioElement;
    element.autoplay = true; element.style.display = "none"; document.body.appendChild(element);
    const isScreen = publication.source === Track.Source.ScreenShareAudio;
    const sinks = isScreen ? screenAudio : audio;
    sinks.set(participant.identity, [...(sinks.get(participant.identity) || []), element]);
    apply(participant.identity, isScreen);
  });
  room.on(RoomEvent.TrackUnsubscribed, (track, publication, participant) => {
    if (track.kind === Track.Kind.Video) {
      remoteMedia.removeRemoteVideo(publication.trackSid);
      return;
    }
    const detached = track.detach() as HTMLMediaElement[];
    detached.forEach(element => element.remove());
    const isScreen = publication.source === Track.Source.ScreenShareAudio;
    const sinks = isScreen ? screenAudio : audio;
    const remaining = (sinks.get(participant.identity) || []).filter(element => !detached.includes(element));
    if (remaining.length) sinks.set(participant.identity, remaining);
    else sinks.delete(participant.identity);
  });
  room.on(RoomEvent.ActiveSpeakersChanged, list => {
    if (!callSession.isCurrent(sessionId)) return;
    remoteSpeakers.clear();
    list.forEach(participant => remoteSpeakers.add(participant.identity));
    emitSpeaking();
  });
  room.on(RoomEvent.ConnectionQualityChanged, quality => qualities.forEach(listener => listener(quality === "poor" ? "poor" : quality === "good" ? "good" : "medium")));
  room.on(RoomEvent.MediaDevicesError, (error, kind) => {
    const device = kind === "audioinput" ? "microfone" : kind === "audiooutput" ? "saída de áudio" : "dispositivo de mídia";
    mediaErrors.forEach(listener => listener(`Não foi possível usar ${device}: ${error.message}`));
  });
  room.on(RoomEvent.AudioPlaybackStatusChanged, () => {
    if (!room.canPlaybackAudio) mediaErrors.forEach(listener => listener("O aplicativo bloqueou a reprodução de áudio. Clique novamente no canal para ativá-la."));
  });
}

// Tell the server about a local camera/screen publication so every other
// member's sidebar learns who is sharing. Emits the v2 `voice.track.hint` when
// the server advertised `voice.hints`, else the v1 op. `source` matches
// LiveKit's names: "camera" | "screen_share" | "screen_share_audio".
function reportTrack(published: boolean, source: string, trackSid?: string) {
  const channel = channelId();
  if (!channel) return;
  if (hasFeature("voice.hints")) {
    if (!trackSid) return; // v2 requires the sid; the webhook covers the gap
    send("voice.track.hint", {
      channel_id: channel,
      track_sid: trackSid,
      source,
      state: published ? "published" : "unpublished",
    });
    return;
  }
  send(published ? "voice.track.published" : "voice.track.unpublished", {
    channel_id: channel,
    source,
    track_sid: trackSid ?? null,
  });
}

// Presence hint: the v2 op carries our LiveKit session sid (which gives an
// immediate exit on leave, SPEC-005 §4.3); the v1 op does not exist in a
// "hint" form, so it maps to enter/leave.
function sendPresenceHint(channel: string, state: "joining" | "leaving", participantSid?: string) {
  if (hasFeature("voice.hints")) {
    send("voice.presence.hint", { channel_id: channel, state, participant_sid: participantSid ?? null });
  } else {
    send(state === "joining" ? "voice.presence.enter" : "voice.presence.leave", { channel_id: channel });
  }
}

// LiveKit media and the application WebSocket recover independently.  A brief
// loss of the latter must not make an otherwise healthy LiveKit participant
// disappear from the sidebar forever: the server deliberately evicts voice
// presence after its reconnect grace window.  Re-announce the call (and any
// already-published visual tracks) once the control plane comes back.
function restoreControlPlanePresence() {
  const activeRoom = room();
  const channel = channelId();
  if (!activeRoom || !channel) return;

  // The presence hint carries our participant_sid so the server marks us
  // confirmed at once (SPEC-005 §4.3): we only know the sid because the LiveKit
  // connection actually exists.
  sendPresenceHint(channel, "joining", callSession.snapshot().participantSid ?? undefined);
  for (const publication of activeRoom.localParticipant.trackPublications.values()) {
    if (!publication.track) continue;
    if (publication.source === Track.Source.Camera) reportTrack(true, "camera", publication.trackSid);
    if (publication.source === Track.Source.ScreenShare) reportTrack(true, "screen_share", publication.trackSid);
    if (publication.source === Track.Source.ScreenShareAudio) reportTrack(true, "screen_share_audio", publication.trackSid);
  }
  logClient("call.presence.restored", { channel_id: channel });
}

export function init(_: string) {
  // The voice store's single IPC listener is mounted here, once (idempotent).
  voiceStore.initVoiceStore();
  // app.bootstrap may be delivered again after login/reload. Keep exactly one
  // observer so a reconnect does not multiply presence notifications.
  if (controlPlaneSubscription) return;
  controlPlaneSubscription = subscribe(event => {
    if (event.op === "connection.state" && event.data?.state === "connected") {
      restoreControlPlanePresence();
      // Local room state may have missed deltas while the socket was down.
      voiceStore.requestFullSnapshot("ws_reconnected");
    }
  });
}
export async function joinCall(id: string, isMuted: boolean, isDeafened: boolean) {
  locallyDeafened = isDeafened;
  localMuted = isMuted;
  watchIntent.clear();

  await callSession.join(id, {
    credentials: channel => credentials(channel),
    afterConnect: async (activeRoom, sessionId) => {
      bindMedia(activeRoom, sessionId);
      // Some WebViews require a user gesture before they allow remote audio.
      void activeRoom.startAudio().catch(() => {});

      sendPresenceHint(id, "joining", activeRoom.localParticipant.sid ?? undefined);

      await microphone.start(
        { mode: noiseSuppressionMode, deviceId: audioInputDeviceId },
        async (track, pipeline) => {
          if (!callSession.isCurrent(sessionId)) return;
          logAudio("audio.track.publishing", { origin: pipeline.origin, processed: pipeline.isProcessed });
          await activeRoom.localParticipant.publishTrack(track, { source: Track.Source.Microphone });
          logAudio("audio.track.published", { origin: pipeline.origin, processed: pipeline.isProcessed });
          if (localMuted) {
            const publication = [...activeRoom.localParticipant.audioTrackPublications.values()]
              .find(item => item.source === Track.Source.Microphone);
            await publication?.track?.mute();
          }
        },
      );
      // Registered AFTER the speech monitor below? No: the mic must be
      // disposed before the monitor's context closes, and disposers run in
      // reverse registration order — so register the mic first.
      callSession.registerResource(sessionId, () => microphone.dispose());
      callSession.registerResource(sessionId, () => {
        remoteMedia.clearRemoteVideos();
        for (const elements of [...audio.values(), ...screenAudio.values()]) {
          for (const element of elements) { element.pause(); element.srcObject = null; element.remove(); }
        }
        audio.clear();
        screenAudio.clear();
      });
      startLocalSpeechMonitor(activeRoom, sessionId);
    },
    onUnexpectedEnd: reason => {
      callEnded.forEach(listener => listener(reason));
    },
  });
}
export async function leaveCall() {
  const channel = channelId();
  if (channel) sendPresenceHint(channel, "leaving", callSession.snapshot().participantSid ?? undefined);
  watchIntent.clear();
  locallyDeafened = false;
  await callSession.leave();
}
export async function setLocalAudioState(isMuted: boolean, isDeafened: boolean) {
  localMuted = isMuted;
  locallyDeafened = isDeafened;
  audio.forEach((_, id) => apply(id));
  screenAudio.forEach((_, id) => apply(id, true));
  const activeRoom = room();
  const publication = activeRoom && [...activeRoom.localParticipant.audioTrackPublications.values()]
    .find(item => item.source === Track.Source.Microphone);
  if (publication?.track) {
    if (isMuted) await publication.track.mute();
    else await publication.track.unmute();
  }
  if (!isMuted && activeRoom) startLocalSpeechMonitor(activeRoom, callSession.snapshot().id);
}
export async function startCamera(_: string, __: string, deviceId?: string) {
  const activeRoom = room();
  if (!activeRoom) return;
  await activeRoom.localParticipant.setCameraEnabled(true, { deviceId, resolution: { width: 1280, height: 720 } });
  const publication = [...activeRoom.localParticipant.videoTrackPublications.values()].find(item => item.source === Track.Source.Camera);
  cameras.forEach(listener => listener(publication?.track ? new MediaStream([publication.track.mediaStreamTrack]) : null));
  if (publication) reportTrack(true, "camera", publication.trackSid);
}
export async function stopCamera(_: string, __: string) {
  const activeRoom = room();
  const trackSid = activeRoom
    ? [...activeRoom.localParticipant.videoTrackPublications.values()].find(item => item.source === Track.Source.Camera)?.trackSid
    : undefined;
  await activeRoom?.localParticipant.setCameraEnabled(false);
  cameras.forEach(listener => listener(null));
  reportTrack(false, "camera", trackSid);
}
export async function switchCamera(deviceId: string) { await stopCamera("", ""); await startCamera("", "", deviceId); }
export function onLocalCamera(listener: (stream: MediaStream | null) => void) { cameras.add(listener); return () => { cameras.delete(listener); }; }
// Screen sharing now lives entirely in `screenPublisher` (SPEC-010). These
// thin re-exports keep the call sites that still import from `rtc`.
export const publishScreen = screenPublisher.start;
export const unpublishScreen = screenPublisher.stop;
export const switchScreenSource = screenPublisher.switchSource;
export const reconfigureScreen = screenPublisher.reconfigure;
export function getLocalScreenStream(): MediaStream | null {
  return screenPublisher.active()?.stream ?? null;
}
function applySubscription(ownerId: string, subscribed: boolean) {
  const participant = callSession.activeRoom()?.remoteParticipants.get(ownerId);
  if (!participant) return;
  for (const publication of participant.trackPublications.values()) {
    if (publication.source === Track.Source.ScreenShare) void publication.setSubscribed(subscribed);
  }
  if (subscribed) logClient("watch.requested", { owner: ownerId });
}

/** Adds `reason` as a reason to watch `ownerId`'s screen in the active call.
 *  The first reason subscribes; further reasons are no-ops. */
export function watchStream(ownerId: string, reason: WatchReason): void {
  const reasons = watchIntent.get(ownerId) ?? new Set<WatchReason>();
  const hadAny = reasons.size > 0;
  reasons.add(reason);
  watchIntent.set(ownerId, reasons);
  if (!hadAny) applySubscription(ownerId, true);
}

/** Drops `reason`. Only when no reason remains is the subscription cancelled
 *  (RC-17: leaving the hover must not drop the "stage" subscription). */
export function stopWatchingStream(ownerId: string, reason: WatchReason): void {
  const reasons = watchIntent.get(ownerId);
  if (!reasons) return;
  reasons.delete(reason);
  if (reasons.size === 0) {
    watchIntent.delete(ownerId);
    applySubscription(ownerId, false);
  }
}
export function onCallDisconnected(listener: (reason: EndReason) => void) { callEnded.add(listener); return () => { callEnded.delete(listener); }; }
export function onSpeaking(listener: (ids: Set<string>) => void) { speakers.add(listener); return () => { speakers.delete(listener); }; }
export function onConnectionQuality(listener: (quality: ConnQuality) => void) { qualities.add(listener); return () => { qualities.delete(listener); }; }
export function onMediaError(listener: (message: string) => void) { mediaErrors.add(listener); return () => { mediaErrors.delete(listener); }; }
export function setPeerVolume(id: string, value: number) { volumes.set(id, value); persist("tk.peerVolumes", volumes); apply(id); }
export function getPeerVolumes() { return Object.fromEntries(volumes); }
export function setPeerAudioMuted(id: string, value: boolean) { muted.set(id, value); persistBooleans("tk.peerMuted", muted); apply(id); }
export function getPeerAudioMuted() { return Object.fromEntries(muted); }
export function setScreenAudioVolume(id: string, value: number) { screenVolumes.set(id, value); persist("tk.screenVolumes", screenVolumes); apply(id, true); }
export function getScreenAudioVolumes() { return Object.fromEntries(screenVolumes); }
export function setScreenAudioMuted(id: string, value: boolean) { screenMuted.set(id, value); persistBooleans("tk.screenMuted", screenMuted); apply(id, true); }
export async function listCameras(): Promise<MediaDeviceInfo[]> { return (await navigator.mediaDevices.enumerateDevices()).filter(device => device.kind === "videoinput"); }
export async function listAllMediaDevices(): Promise<DeviceLists> { const devices = await navigator.mediaDevices.enumerateDevices(); return { audioInputs: devices.filter(device => device.kind === "audioinput"), audioOutputs: devices.filter(device => device.kind === "audiooutput"), videoInputs: devices.filter(device => device.kind === "videoinput") }; }
export function getAudioInputDeviceId() { return audioInputDeviceId; }
export function getAudioOutputDeviceId() { return audioOutputDeviceId; }
export async function setAudioOutputDevice(deviceId: string) { audioOutputDeviceId = deviceId || undefined; if (deviceId) persistValue("tk.audioOutputDeviceId", deviceId); await room()?.switchActiveDevice("audiooutput", deviceId).catch(() => {}); for (const items of [...audio.values(), ...screenAudio.values()]) for (const element of items) void setSink(element); }
export function getInputVolume() { return inputVolume; }
export function getOutputVolume() { return outputVolume; }
export function setInputVolumeLevel(value: number) { inputVolume = Math.max(0, Math.min(1, value)); persistValue("tk.inputVolume", inputVolume); }
export function setOutputVolumeLevel(value: number) { outputVolume = Math.max(0, Math.min(1, value)); persistValue("tk.outputVolume", outputVolume); for (const id of audio.keys()) apply(id); for (const id of screenAudio.keys()) apply(id, true); }
export function getNoiseSuppressionMode() { return noiseSuppressionMode; }
export function onAudioPipelineStatus(listener: (status: AudioPipelineStatus) => void) { audioPipelineStatusListeners.add(listener); return () => { audioPipelineStatusListeners.delete(listener); }; }
export async function setNoiseSuppressionMode(mode: NoiseSuppressionMode) {
  noiseSuppressionMode = mode;
  try { localStorage.setItem("tk.noiseSuppressionMode", mode); } catch {}
  const activeRoom = room();
  if (!activeRoom || !microphone.current) {
    microphone.setDesiredMode(mode);
    return;
  }
  await microphone.switchMode(mode, async (track, pipeline) => replaceMicrophoneTrack(activeRoom, track, pipeline));
  if (!localMuted) startLocalSpeechMonitor(activeRoom, callSession.snapshot().id);
}
async function replaceMicrophoneTrack(room: Room, track: MediaStreamTrack, pipeline: { origin: string; isProcessed: boolean }) {
  const publication = [...room.localParticipant.audioTrackPublications.values()]
    .find(item => item.source === Track.Source.Microphone);
  if (!publication?.track) {
    logAudio("audio.track.publishing", { origin: pipeline.origin, processed: pipeline.isProcessed });
    await room.localParticipant.publishTrack(track, { source: Track.Source.Microphone });
    logAudio("audio.track.published", { origin: pipeline.origin, processed: pipeline.isProcessed });
    return;
  }
  logAudio("audio.track.replacing", { origin: pipeline.origin, processed: pipeline.isProcessed });
  await publication.track.replaceTrack(track, true);
  if (localMuted) await publication.track.mute();
  logAudio("audio.track.replaced", { origin: pipeline.origin, processed: pipeline.isProcessed });
}
async function unpublishMicrophone(room: Room | null) {
  const publication = room && [...room.localParticipant.audioTrackPublications.values()]
    .find(item => item.source === Track.Source.Microphone);
  if (!publication?.track || !room) return;
  // The pipeline owns stop(); do not let LiveKit stop a source that is about
  // to be disposed by the manager, especially while a replacement is queued.
  await room.localParticipant.unpublishTrack(publication.track, false).catch(() => {});
  logAudio("audio.track.unpublished");
}
export async function setAudioInputDevice(deviceId: string) {
  audioInputDeviceId = deviceId || undefined;
  if (deviceId) persistValue("tk.audioInputDeviceId", deviceId);
  const activeRoom = room();
  if (!activeRoom || !microphone.current) return;
  try {
    logAudio("audio.device.switch.started", { hasDeviceSelection: !!deviceId });
    await microphone.switchDevice(audioInputDeviceId, async (track, pipeline) => replaceMicrophoneTrack(activeRoom, track, pipeline));
    if (!localMuted) startLocalSpeechMonitor(activeRoom, callSession.snapshot().id);
    logAudio("audio.device.switch.completed");
  } catch (error) {
    logAudio("audio.device.switch.failed", { reason: error instanceof Error ? error.message : String(error) });
    throw error;
  }
}
export function ensureChannel(_: string) {}
