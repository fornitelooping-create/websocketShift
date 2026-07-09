// Minimal signaling server for Shift voice calls.
//
// This process ONLY relays small JSON messages (WebRTC offers/answers/ICE
// candidates) between two users so their apps can find each other and
// negotiate a direct peer-to-peer connection. Once that connection is
// established, actual audio flows directly between the two devices (or via
// a TURN server if you configure one) — it never goes through this server.
//
// Run it with: npm install && npm start
// Then point every device's Shift app at ws://<this-machine-ip>:8080
// (see "Serveur d'appel" in the app's user settings).

const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// userId -> ws connection
const clients = new Map();

function send(ws, data) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(data));
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

    if (msg.type === "register") {
      registeredUserId = msg.userId;
      clients.set(registeredUserId, ws);
      console.log(`[signaling] ${registeredUserId} connected (${clients.size} online)`);
      return;
    }

    // Every other message type is just relayed to its target, with the
    // sender's id attached so the recipient knows who it's from.
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
      console.log(`[signaling] ${registeredUserId} disconnected (${clients.size} online)`);
    }
  });
});

console.log(`Shift signaling server listening on ws://0.0.0.0:${PORT}`);
