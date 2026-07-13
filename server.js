// Shift Signaling Server — Full Version with Voice Channels Support
// ---------------------------------------------------------------
// This server handles:
// - WebRTC signaling (offer/answer/ice)
// - User registration
// - Voice channel presence (join/leave + broadcast + initial snapshot)
// - Clean disconnect handling
// - /health HTTP endpoint, used to keep the free Render instance awake
//   via an external ping (Render free tier sleeps after 15min without
//   any incoming request).
//
// IMPORTANT: a single user can have MULTIPLE sockets open at once (e.g.
// one from the private-call feature, one from the server voice-channel
// feature). Each userId maps to a SET of sockets, not a single socket —
// otherwise whichever connects last silently breaks the other feature.

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;

// Petit serveur HTTP "normal", utilisé pour :
// - le endpoint /health, appelé toutes les ~10min par un service de ping
//   externe (cron-job.org, UptimeRobot...) pour empêcher Render de mettre
//   l'instance en veille.
// - le check de santé que Render fait déjà tout seul sur le port ouvert.
const httpServer = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
    return;
  }
  res.writeHead(404);
  res.end();
});

// Le WebSocketServer s'attache au même serveur HTTP au lieu d'écouter
// son propre port (nécessaire pour que /health fonctionne sur Render).
const wss = new WebSocketServer({ server: httpServer });

// userId -> Set<ws>
const clients = new Map();

// channelId -> Set(userIds)
const voiceRooms = new Map();

function send(ws, data) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

// Envoie à TOUS les sockets ouverts d'un utilisateur (il peut en avoir
// plusieurs : un pour les appels privés, un pour les salons vocaux...).
function sendToUser(userId, data) {
  const sockets = clients.get(userId);
  if (!sockets) return false;
  sockets.forEach((ws) => send(ws, data));
  return sockets.size > 0;
}

function broadcastToRoom(channelId, data, exceptUserId = null) {
  const room = voiceRooms.get(channelId);
  if (!room) return;

  for (const userId of room) {
    if (userId === exceptUserId) continue;
    sendToUser(userId, data);
  }
}

function getVoiceStateSnapshot() {
  const snapshot = {};
  for (const [channelId, userIds] of voiceRooms.entries()) {
    snapshot[channelId] = Array.from(userIds);
  }
  return snapshot;
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
    // 1. REGISTER USER (un utilisateur peut avoir plusieurs sockets)
    // -------------------------------------------------------------
    if (msg.type === "register") {
      registeredUserId = msg.userId;
      if (!clients.has(registeredUserId)) clients.set(registeredUserId, new Set());
      clients.get(registeredUserId).add(ws);
      console.log(`[signaling] ${registeredUserId} connected (${clients.size} users online)`);

      // Envoie l'état actuel de tous les salons vocaux, pour que ce
      // client sache immédiatement qui est déjà connecté où.
      send(ws, { type: "voiceState", voiceMembers: getVoiceStateSnapshot() });
      return;
    }

    // -------------------------------------------------------------
    // 2. JOIN VOICE CHANNEL
    // -------------------------------------------------------------
    if (msg.type === "joinVoice") {
      const { channelId } = msg;

      if (!voiceRooms.has(channelId)) {
        voiceRooms.set(channelId, new Set());
      }

      voiceRooms.get(channelId).add(registeredUserId);

      // Notify others
      broadcastToRoom(channelId, {
        type: "userJoinedVoice",
        channelId,
        userId: registeredUserId
      }, registeredUserId);

      console.log(`[voice] ${registeredUserId} joined ${channelId}`);
      return;
    }

    // -------------------------------------------------------------
    // 3. LEAVE VOICE CHANNEL
    // -------------------------------------------------------------
    if (msg.type === "leaveVoice") {
      const { channelId } = msg;

      const room = voiceRooms.get(channelId);
      if (room) {
        room.delete(registeredUserId);

        broadcastToRoom(channelId, {
          type: "userLeftVoice",
          channelId,
          userId: registeredUserId
        }, registeredUserId);

        console.log(`[voice] ${registeredUserId} left ${channelId}`);
      }
      return;
    }

    // -------------------------------------------------------------
    // 4. WEBRTC SIGNALING (offer/answer/ice) — relayé à TOUS les
    //    sockets du destinataire.
    // -------------------------------------------------------------
    const delivered = sendToUser(msg.targetUserId, { ...msg, fromUserId: registeredUserId });
    if (!delivered) {
      send(ws, { type: "user-offline", targetUserId: msg.targetUserId });
    }
  });

  // -------------------------------------------------------------
  // 5. CLEAN DISCONNECT HANDLING
  // -------------------------------------------------------------
  ws.on("close", () => {
    if (!registeredUserId) return;

    const sockets = clients.get(registeredUserId);
    if (sockets) {
      sockets.delete(ws);
      // Ne considère l'utilisateur comme réellement déconnecté que
      // lorsque son DERNIER socket se ferme (il peut en avoir un autre
      // encore ouvert pour une autre fonctionnalité).
      if (sockets.size > 0) return;
      clients.delete(registeredUserId);
    }

    // Retire l'utilisateur de tous les salons vocaux où il était présent
    // et prévient les autres participants.
    for (const [channelId, room] of voiceRooms.entries()) {
      if (room.has(registeredUserId)) {
        room.delete(registeredUserId);
        broadcastToRoom(channelId, {
          type: "userLeftVoice",
          channelId,
          userId: registeredUserId
        }, registeredUserId);
        if (room.size === 0) voiceRooms.delete(channelId);
      }
    }

    console.log(`[signaling] ${registeredUserId} fully disconnected (${clients.size} users online)`);
  });
});

httpServer.listen(PORT, () => {
  console.log(`[signaling] listening on port ${PORT} (ws + /health)`);
});
