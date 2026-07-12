// Shift Signaling Server — Full Version with Voice Channels Support
// ---------------------------------------------------------------
// This server handles:
// - WebRTC signaling (offer/answer/ice)
// - User registration
// - Voice channel presence (join/leave + broadcast)
// - Clean disconnect handling
// - /health HTTP endpoint, used to keep the free Render instance awake
//   via an external ping (Render free tier sleeps after 15min without
//   any incoming request).

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

// userId -> ws
const clients = new Map();

// channelId -> Set(userIds)
const voiceRooms = new Map();

function send(ws, data) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcastToRoom(channelId, data, exceptUserId = null) {
  const room = voiceRooms.get(channelId);
  if (!room) return;

  for (const userId of room) {
    if (userId === exceptUserId) continue;
    const ws = clients.get(userId);
    send(ws, data);
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
    // 1. REGISTER USER
    // -------------------------------------------------------------
    if (msg.type === "register") {
      registeredUserId = msg.userId;
      clients.set(registeredUserId, ws);
      console.log(`[signaling] ${registeredUserId} connected (${clients.size} online)`);
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
    // 4. WEBRTC SIGNALING (offer/answer/ice)
    // -------------------------------------------------------------
    const target = clients.get(msg.targetUserId);
    if (!target) {
      send(ws, { type: "user-offline", targetUserId: msg.targetUserId });
      return;
    }

    send(target, { ...msg, fromUserId: registeredUserId });
  });

  // -------------------------------------------------------------
  // 5. CLEAN DISCONNECT HANDLING
  // -------------------------------------------------------------
  ws.on("close", () => {
    if (!registeredUserId) return;

    clients.delete(registeredUserId);

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

    console.log(`[signaling] ${registeredUserId} disconnected (${clients.size} online)`);
  });
});

httpServer.listen(PORT, () => {
  console.log(`[signaling] listening on port ${PORT} (ws + /health)`);
});