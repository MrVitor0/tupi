import { afterEach, describe, expect, it, vi } from "vitest";

const ipc = vi.hoisted(() => {
  let listener: ((event: { op: string; data: any }) => void) | null = null;
  return {
    send: vi.fn(),
    subscribe: vi.fn((l: (event: { op: string; data: any }) => void) => {
      listener = l;
      return () => {};
    }),
    dispatch: (op: string, data: unknown) => listener?.({ op, data }),
  };
});
vi.mock("./ipc", () => ({ send: ipc.send, subscribe: ipc.subscribe }));

const features = vi.hoisted(() => ({ set: new Set<string>(["voice.room.v2"]) }));
vi.mock("./serverInfo", () => ({ hasFeature: (name: string) => features.set.has(name) }));
vi.mock("./clientLog", () => ({ logClient: () => {}, maybeAutoSend: () => {} }));

import * as store from "./voiceStore";

function room(over: Partial<{ channel_id: string; version: number; participants: any[]; tracks: any[] }> = {}) {
  return { channel_id: "ch", version: 1, participants: [], tracks: [], ...over };
}
function delta(over: Partial<any> = {}) {
  return {
    channel_id: "ch",
    version: 2,
    previous_version: 1,
    participants_added: [],
    participants_updated: [],
    participants_removed: [],
    tracks_added: [],
    tracks_removed: [],
    reason: "webhook.participant_joined",
    ...over,
  };
}

afterEach(() => {
  store.__resetForTest();
  ipc.send.mockClear();
  features.set = new Set(["voice.room.v2"]);
  vi.useRealTimers();
});

describe("voiceStore", () => {
  it("applies delta in the fixed order (U-20)", () => {
    store.initVoiceStore();
    ipc.dispatch("voice.room.state", {
      full: true,
      rooms: [room({ participants: [{ user_id: "a" }, { user_id: "b" }] })],
    });
    // Same delta: remove b, add b again with a sid → the add must win (order:
    // removed before added).
    ipc.dispatch(
      "voice.room.delta",
      delta({
        participants_removed: ["b"],
        participants_added: [{ user_id: "b", participant_sid: "PA_b" }],
      }),
    );
    const room2 = store.getState().rooms["ch"];
    expect(room2.version).toBe(2);
    expect(room2.participants.map(p => p.userId)).toEqual(["a", "b"]);
    expect(room2.participants.find(p => p.userId === "b")?.participantSid).toBe("PA_b");
  });

  it("requests a snapshot on a version gap and drops the delta (U-21)", () => {
    store.initVoiceStore();
    ipc.dispatch("voice.room.state", { full: true, rooms: [room({ version: 1 })] });
    ipc.dispatch("voice.room.delta", delta({ previous_version: 5, version: 6 }));
    expect(store.getState().rooms["ch"].version).toBe(1); // not applied
    expect(ipc.send).toHaveBeenCalledWith("voice.room.request", { channel_ids: ["ch"] });
  });

  it("accepts a lower version as a server restart (U-22)", () => {
    store.initVoiceStore();
    ipc.dispatch("voice.room.state", { full: true, rooms: [room({ version: 42, participants: [{ user_id: "a" }] })] });
    ipc.dispatch("voice.room.state", { full: true, rooms: [room({ version: 1, participants: [{ user_id: "b" }] })] });
    expect(store.getState().rooms["ch"].version).toBe(1);
    expect(store.getState().rooms["ch"].participants.map(p => p.userId)).toEqual(["b"]);
  });

  it("merges a scoped snapshot without erasing unrelated voice channels", () => {
    store.initVoiceStore();
    ipc.dispatch("voice.room.state", {
      full: true,
      rooms: [room({ channel_id: "alpha", participants: [{ user_id: "a" }] }), room({ channel_id: "beta", participants: [{ user_id: "b" }] })],
    });

    // The server answered a request for alpha only. Its omission means alpha
    // emptied, not that beta vanished too.
    ipc.dispatch("voice.room.state", { full: true, channel_ids: ["alpha"], rooms: [] });
    expect(store.getState().rooms.alpha).toBeUndefined();
    expect(store.getState().rooms.beta?.participants.map(p => p.userId)).toEqual(["b"]);
  });

  it("unblocks an empty scoped snapshot so a later delta can request recovery", async () => {
    vi.useFakeTimers();
    store.initVoiceStore();
    ipc.dispatch("voice.room.state", { full: true, rooms: [room({ version: 1 })] });
    ipc.dispatch("voice.room.delta", delta({ previous_version: 4, version: 5 }));
    expect(ipc.send).toHaveBeenCalledTimes(1);

    ipc.dispatch("voice.room.state", { full: true, channel_ids: ["ch"], rooms: [] });
    ipc.dispatch("voice.room.delta", delta({ previous_version: 0, version: 1 }));
    // The marker was cleared: this unknown-channel delta starts a new recovery
    // request instead of being silently discarded forever.
    await vi.advanceTimersByTimeAsync(2000);
    expect(ipc.send).toHaveBeenCalledTimes(2);
  });

  it("ignores a duplicate delta (U-23)", () => {
    store.initVoiceStore();
    ipc.dispatch("voice.room.state", { full: true, rooms: [room({ version: 1 })] });
    ipc.dispatch("voice.room.delta", delta({ previous_version: 1, version: 2, participants_added: [{ user_id: "a" }] }));
    ipc.dispatch("voice.room.delta", delta({ previous_version: 1, version: 2, participants_added: [{ user_id: "a" }] }));
    expect(store.getState().rooms["ch"].participants).toHaveLength(1);
    expect(store.getState().rooms["ch"].version).toBe(2);
  });

  it("session participants come only from the live list", () => {
    store.initVoiceStore();
    // The server thinks X is in the channel...
    ipc.dispatch("voice.room.state", { full: true, rooms: [room({ channel_id: "c1", participants: [{ user_id: "x" }] })] });
    // ...but LiveKit only has me.
    store.setLiveParticipants("c1", [{ identity: "me", sid: "PA_me", isLocal: true }]);
    expect(store.getState().session.participants.map(p => p.userId)).toEqual(["me"]);
  });

  it("session overlay picks up mute from a delta (INV-C1 overlay)", () => {
    store.initVoiceStore();
    ipc.dispatch("voice.room.state", {
      full: true,
      rooms: [room({ channel_id: "c1", version: 1, participants: [{ user_id: "a", muted: false }] })],
    });
    store.setLiveParticipants("c1", [
      { identity: "me", sid: "PA_me", isLocal: true },
      { identity: "a", sid: "PA_a", isLocal: false },
    ]);
    expect(store.getState().session.participants.find(p => p.userId === "a")?.muted).toBe(false);

    ipc.dispatch(
      "voice.room.delta",
      delta({ channel_id: "c1", participants_updated: [{ user_id: "a", muted: true }] }),
    );
    expect(store.getState().session.participants.find(p => p.userId === "a")?.muted).toBe(true);
  });

  it("v1 voice.roster converts to the same shape", () => {
    features.set = new Set(); // no voice.room.v2
    store.initVoiceStore();
    ipc.dispatch("voice.roster", {
      channel_id: "c1",
      participants: [{ user_id: "a", muted: true, deafened: false }],
      streams: [{ stream_id: "s1", owner: "a", kind: "screen", msid: "TR_1", has_audio: true }],
    });
    const proj = store.getState().rooms["c1"];
    expect(proj.version).toBe(0);
    expect(proj.participants[0]).toMatchObject({ userId: "a", muted: true, provisional: false });
    expect(proj.tracks.map(t => t.source).sort()).toEqual(["screen_share", "screen_share_audio"]);
    expect(store.roomStreams(proj).map(s => s.kind)).toEqual(["screen"]);
  });

  it("v1 voice.roster with an empty list removes the channel", () => {
    features.set = new Set();
    store.initVoiceStore();
    ipc.dispatch("voice.roster", { channel_id: "c1", participants: [{ user_id: "a" }], streams: [] });
    expect(store.getState().rooms["c1"]).toBeDefined();
    ipc.dispatch("voice.roster", { channel_id: "c1", participants: [], streams: [] });
    expect(store.getState().rooms["c1"]).toBeUndefined();
  });

  it("shows an occupant only in their newest room while a move is converging", () => {
    store.initVoiceStore();
    ipc.dispatch("voice.room.state", {
      full: true,
      rooms: [
        room({ channel_id: "source", participants: [{ user_id: "a", joined_at: "2026-09-02T10:00:00Z" }] }),
        room({ channel_id: "destination", participants: [{ user_id: "a", joined_at: "2026-09-02T10:00:01Z" }] }),
      ],
    });
    const visible = store.roomsForDisplay(store.getState().rooms);
    expect(visible.source.participants).toEqual([]);
    expect(visible.destination.participants.map(p => p.userId)).toEqual(["a"]);
  });

  it("drops a track with an unknown source", () => {
    store.initVoiceStore();
    ipc.dispatch("voice.room.state", {
      full: true,
      rooms: [room({ tracks: [{ track_sid: "TR_1", owner: "a", source: "wat" }, { track_sid: "TR_2", owner: "a", source: "camera" }] })],
    });
    expect(store.getState().rooms["ch"].tracks.map(t => t.trackSid)).toEqual(["TR_2"]);
  });

  it("a delta for an unknown channel requests that channel's snapshot", () => {
    store.initVoiceStore();
    ipc.dispatch("voice.room.delta", delta({ channel_id: "ghost" }));
    expect(ipc.send).toHaveBeenCalledWith("voice.room.request", { channel_ids: ["ghost"] });
    expect(store.getState().rooms["ghost"]).toBeUndefined();
  });

  it("the speaking slice updates without touching rooms (SPEC-013)", () => {
    store.initVoiceStore();
    ipc.dispatch("voice.room.state", { full: true, rooms: [room({ participants: [{ user_id: "a" }] })] });
    const roomsBefore = store.getState().rooms;

    store.setSpeaking(new Set(["a"]));
    expect(store.getState().speaking.has("a")).toBe(true);
    // `rooms` is the same reference — a selector on `rooms` would not re-render.
    expect(store.getState().rooms).toBe(roomsBefore);
  });

  it("setSpeaking is a no-op when the set is unchanged", () => {
    store.initVoiceStore();
    let emits = 0;
    store.subscribeVoice(() => emits++);
    store.setSpeaking(new Set(["a", "b"]));
    const after = emits;
    store.setSpeaking(new Set(["b", "a"])); // same members
    expect(emits).toBe(after);
  });
});
