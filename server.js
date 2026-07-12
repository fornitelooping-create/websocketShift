// Signaling server for Shift voice calls + voice channel presence/audio.
//
// This process:
//   1. Relays small JSON messages (WebRTC offers/answers/ICE candidates)
//      between users so their apps can find each other and negotiate
//      direct peer-to-peer audio connections. Audio never goes through
//      this server — only the tiny handshake messages do.
//   2. Tracks which users are in which voice CHANNEL (room), and
//      broadcasts join/leave events + current state to everyone.
//
// IMPORTANT: a single user can have MULTIPLE sockets open at once (e.g.
// one from the private-call feature, one from the server voice-channel
// feature). Each userId maps to a SET of sockets, not a single socket.
//
// Run it with: npm install && npm start
// Then point every device's Shift app at ws://<this-machine-ip>:8080
// (see "Serveur d'appel" in the app's user settings).

const { WebSocketServer } = require("ws");
const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// userId -> Set<ws>
const clients = new Map();

// channelId -> Set of userIds currently in that voice channel
const voiceRooms = new Map();

function send(ws, data) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

// Send to every socket belonging to a given user (they may have more than one)
function sendToUser(userId, data) {
  const sockets = clients.get(userId);
  if (!sockets) return false;
  sockets.forEach((ws) => send(ws, data));
  return sockets.size > 0;
}

// Send to every socket of every user except (optionally) one user
function broadcast(data, exceptUserId = null) {
  for (const [userId, sockets] of clients.entries()) {
    if (userId === exceptUserId) continue;
    sockets.forEach((ws) => send(ws, data));
  }
}

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
    // REGISTER (one user can have several sockets registered at once)
    // -------------------------------------------------------------
    if (msg.type === "register") {
      registeredUserId = msg.userId;
      if (!clients.has(registeredUserId)) clients.set(registeredUserId, new Set());
      clients.get(registeredUserId).add(ws);
      console.log(`[signaling] ${registeredUserId} connected (${clients.size} users online)`);

      send(ws, { type: "voiceState", voiceMembers: getVoiceStateSnapshot() });
      return;
    }

    // -------------------------------------------------------------
    // JOIN VOICE CHANNEL
    // -------------------------------------------------------------
    if (msg.type === "joinVoice") {
      const { channelId, userId } = msg;
      if (!channelId || !userId) return;

      removeUserFromAnyRoom(userId); // one voice channel at a time
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
    // EVERYTHING ELSE (call-offer, call-answer, voice-offer,
    // voice-answer, ice-candidate, voice-ice-candidate, ...) is just
    // relayed to its target user, with the sender's id attached.
    // -------------------------------------------------------------
    const delivered = sendToUser(msg.targetUserId, { ...msg, fromUserId: registeredUserId });
    if (!delivered) {
      send(ws, { type: "user-offline", targetUserId: msg.targetUserId });
    }
  });

  ws.on("close", () => {
    if (!registeredUserId) return;
    const sockets = clients.get(registeredUserId);
    if (sockets) {
      sockets.delete(ws);
      if (sockets.size === 0) {
        clients.delete(registeredUserId);
        removeUserFromAnyRoom(registeredUserId);
        console.log(`[signaling] ${registeredUserId} fully disconnected (${clients.size} users online)`);
      }
    }
  });
});

console.log(`Shift signaling server listening on ws://0.0.0.0:${PORT}`);
