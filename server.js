// Signaling server for Shift voice calls + voice channel presence.
//
// This process does two things:
//   1. Relays small JSON messages (WebRTC offers/answers/ICE candidates)
//      between two users so their apps can find each other and negotiate
//      a direct peer-to-peer connection. Actual audio never goes through
//      this server.
//   2. Tracks which users are in which voice CHANNEL (room), and
//      broadcasts join/leave events + current state to everyone, so the
//      sidebar can show who's connected in real time.
//
// Run it with: npm install && npm start
// Then point every device's Shift app at ws://<this-machine-ip>:8080
// (see "Serveur d'appel" in the app's user settings).

const { WebSocketServer } = require("ws");
const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// userId -> ws connection
const clients = new Map();

// channelId -> Set of userIds currently in that voice channel
const voiceRooms = new Map();

function send(ws, data) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(data, exceptUserId = null) {
  for (const [userId, ws] of clients.entries()) {
    if (userId !== exceptUserId) send(ws, data);
  }
}

// Turn the Map<channelId, Set<userId>> into the plain object shape
// the client expects: { channelId: [userId, userId, ...] }
function getVoiceStateSnapshot() {
  const snapshot = {};
  for (const [channelId, userIds] of voiceRooms.entries()) {
    snapshot[channelId] = Array.from(userIds);
  }
  return snapshot;
}

function joinRoom(channelId, userId) {
  if (!voiceRooms.has(channelId)) voiceRooms.set(channelId, new Set());
  voiceRooms.get(channelId).add(userId);
}

function leaveRoom(channelId, userId) {
  const room = voiceRooms.get(channelId);
  if (!room) return;
  room.delete(userId);
  if (room.size === 0) voiceRooms.delete(channelId);
}

// Remove a user from whatever voice room they were in (used on
// explicit leaveVoice AND on disconnect), and tell everyone.
function removeUserFromAnyRoom(userId) {
  for (const channelId of voiceRooms.keys()) {
    const room = voiceRooms.get(channelId);
    if (room && room.has(userId)) {
      leaveRoom(channelId, userId);
      broadcast({ type: "userLeftVoice", channelId, userId });
    }
  }
}

wss.on("connection", (ws) => {
  let registeredUserId = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // -------------------------------------------------------------
    // REGISTER
    // -------------------------------------------------------------
    if (msg.type === "register") {
      registeredUserId = msg.userId;
      clients.set(registeredUserId, ws);
      console.log(`[signaling] ${registeredUserId} connected (${clients.size} online)`);

      // Send the newcomer the current state of every voice channel,
      // so they immediately see who's already connected.
      send(ws, { type: "voiceState", voiceMembers: getVoiceStateSnapshot() });
      return;
    }

    // -------------------------------------------------------------
    // JOIN VOICE CHANNEL
    // -------------------------------------------------------------
    if (msg.type === "joinVoice") {
      const { channelId, userId } = msg;
      if (!channelId || !userId) return;

      // A user can only be in one voice channel at a time.
      removeUserFromAnyRoom(userId);

      joinRoom(channelId, userId);
      broadcast({ type: "userJoinedVoice", channelId, userId });
      return;
    }

    // -------------------------------------------------------------
    // LEAVE VOICE CHANNEL
    // -------------------------------------------------------------
    if (msg.type === "leaveVoice") {
      const { channelId, userId } = msg;
      if (!channelId || !userId) return;

      leaveRoom(channelId, userId);
      broadcast({ type: "userLeftVoice", channelId, userId });
      return;
    }

    // -------------------------------------------------------------
    // WEBRTC SIGNALING (offer/answer/ICE) — relayed to a specific user
    // -------------------------------------------------------------
    const target = clients.get(msg.targetUserId);
    if (!target) {
      send(ws, { type: "user-offline", targetUserId: msg.targetUserId });
      return;
    }
    send(target, { ...msg, fromUserId: registeredUserId });
  });

  ws.on("close", () => {
    if (registeredUserId) {
      clients.delete(registeredUserId);
      removeUserFromAnyRoom(registeredUserId);
      console.log(`[signaling] ${registeredUserId} disconnected (${clients.size} online)`);
    }
  });
});

console.log(`Shift signaling server listening on ws://0.0.0.0:${PORT}`);
