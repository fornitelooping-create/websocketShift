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
const crypto = require("crypto");
const { Readable } = require("stream");
const { WebSocketServer } = require("ws");
const express = require("express");
const multer = require("multer");
const cors = require("cors");

const PORT = process.env.PORT || 8080;

// -------------------------------------------------------------------
// UPLOAD DE FICHIERS (pièces jointes du chat) — via Backblaze B2
// -------------------------------------------------------------------
// Le bucket B2 est PRIVÉ. Ce serveur est le SEUL à connaître les clés B2
// (variables d'environnement, jamais dans le code ni côté client). Le
// client envoie le fichier ici ; ce serveur le pousse vers B2 et renvoie
// une URL de LECTURE qui pointe vers CE serveur (/files/:name), pas vers
// B2 directement — ce serveur agit comme proxy pour le téléchargement,
// ce qui permet de garder le bucket privé tout en servant les fichiers
// normalement dans l'app.
const B2_KEY_ID = process.env.B2_KEY_ID;
const B2_APPLICATION_KEY = process.env.B2_APPLICATION_KEY;
const B2_BUCKET_NAME = process.env.B2_BUCKET_NAME;

const upload = multer({
  storage: multer.memoryStorage(),
  // Limite haute par défaut — largement au-dessus des 50 Mo de Supabase.
  // Ajuste ici si besoin.
  limits: { fileSize: 500 * 1024 * 1024 }
});

// Cache de l'autorisation B2 (le token expire après ~24h ; on le renouvelle
// automatiquement dès qu'un appel échoue avec 401).
let b2Auth = null; // { apiUrl, downloadUrl, authorizationToken, bucketId }

async function b2Authorize() {
  const credentials = Buffer.from(`${B2_KEY_ID}:${B2_APPLICATION_KEY}`).toString("base64");
  const res = await fetch("https://api.backblazeb2.com/b2api/v2/b2_authorize_account", {
    headers: { Authorization: `Basic ${credentials}` }
  });
  if (!res.ok) throw new Error(`b2_authorize_account a échoué (${res.status})`);
  const data = await res.json();
  b2Auth = {
    apiUrl: data.apiUrl,
    downloadUrl: data.downloadUrl,
    authorizationToken: data.authorizationToken,
    bucketId: data.allowed?.bucketId
  };
  return b2Auth;
}

async function getB2Auth() {
  if (!b2Auth) await b2Authorize();
  return b2Auth;
}

function sha1Hex(buffer) {
  return crypto.createHash("sha1").update(buffer).digest("hex");
}

// Upload d'un fichier vers B2. Réessaie une fois avec une nouvelle
// autorisation si le token en cache a expiré (401) ou si l'appel échoue.
async function b2UploadFile(fileName, buffer, contentType, retry = true) {
  try {
    const auth = await getB2Auth();
    const uploadUrlRes = await fetch(`${auth.apiUrl}/b2api/v2/b2_get_upload_url`, {
      method: "POST",
      headers: { Authorization: auth.authorizationToken, "Content-Type": "application/json" },
      body: JSON.stringify({ bucketId: auth.bucketId })
    });
    if (uploadUrlRes.status === 401 && retry) {
      b2Auth = null;
      return b2UploadFile(fileName, buffer, contentType, false);
    }
    if (!uploadUrlRes.ok) throw new Error(`b2_get_upload_url a échoué (${uploadUrlRes.status})`);
    const { uploadUrl, authorizationToken } = await uploadUrlRes.json();

    const res = await fetch(uploadUrl, {
      method: "POST",
      headers: {
        Authorization: authorizationToken,
        "X-Bz-File-Name": encodeURIComponent(fileName),
        "Content-Type": contentType || "b2/x-auto",
        "X-Bz-Content-Sha1": sha1Hex(buffer),
        "Content-Length": buffer.length
      },
      body: buffer
    });
    if (res.status === 401 && retry) {
      b2Auth = null;
      return b2UploadFile(fileName, buffer, contentType, false);
    }
    if (!res.ok) throw new Error(`upload B2 a échoué (${res.status})`);
    return res.json();
  } catch (err) {
    if (retry) {
      b2Auth = null;
      return b2UploadFile(fileName, buffer, contentType, false);
    }
    throw err;
  }
}

// Récupère le contenu d'un fichier B2 (bucket privé — nécessite le token
// d'autorisation). Ce serveur le relaie ensuite au navigateur.
async function b2DownloadFile(fileName, retry = true) {
  const auth = await getB2Auth();
  const url = `${auth.downloadUrl}/file/${encodeURIComponent(B2_BUCKET_NAME)}/${encodeURIComponent(fileName)}`;
  const res = await fetch(url, { headers: { Authorization: auth.authorizationToken } });
  if (res.status === 401 && retry) {
    b2Auth = null;
    return b2DownloadFile(fileName, false);
  }
  if (!res.ok) throw new Error(`téléchargement B2 a échoué (${res.status})`);
  return res;
}

const app = express();
app.use(cors());

app.get(["/health", "/"], (req, res) => {
  res.status(200).type("text/plain").send("ok");
});

app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: "Aucun fichier reçu." });
    if (!B2_KEY_ID || !B2_APPLICATION_KEY || !B2_BUCKET_NAME) {
      return res.status(500).json({ error: "Config B2 manquante côté serveur (variables d'environnement)." });
    }
    const safeName = req.file.originalname.replace(/[^\w.\-]/g, "_");
    const fileName = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}-${safeName}`;
    await b2UploadFile(fileName, req.file.buffer, req.file.mimetype);

    const publicHost = process.env.PUBLIC_URL || `${req.protocol}://${req.get("host")}`;
    res.json({ file_url: `${publicHost}/files/${encodeURIComponent(fileName)}` });
  } catch (err) {
    console.error("[upload] échec:", err);
    res.status(500).json({ error: "Échec de l'upload." });
  }
});

app.get("/files/:name", async (req, res) => {
  try {
    const upstream = await b2DownloadFile(req.params.name);
    res.setHeader("Content-Type", upstream.headers.get("content-type") || "application/octet-stream");
    const contentLength = upstream.headers.get("content-length");
    if (contentLength) res.setHeader("Content-Length", contentLength);
    res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
    Readable.fromWeb(upstream.body).pipe(res);
  } catch (err) {
    console.error("[files] échec:", err);
    res.status(404).send("Fichier introuvable.");
  }
});

app.use((req, res) => res.status(404).send("Not found"));

// Le WebSocketServer s'attache au même serveur HTTP (Express gère /health,
// /upload et /files ; le WS gère la signalisation temps réel).
const httpServer = http.createServer(app);
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

// Diffuse un événement de présence vocale (join/leave) à TOUS les
// utilisateurs connectés au serveur — pas seulement à ceux déjà dans le
// salon vocal concerné. C'est indispensable : un client qui n'a encore
// rejoint aucun salon (ex: il navigue dans les salons texte) a quand
// même besoin de savoir en temps réel qui est connecté où, pour afficher
// la liste des membres dans la sidebar. Ne diffuser qu'aux membres déjà
// présents dans le salon revient à ne jamais notifier personne d'autre,
// puisque le salon est justement vide de leur point de vue.
function broadcastVoicePresence(data, exceptUserId = null) {
  for (const userId of clients.keys()) {
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

      // Notify everyone (not just people already in this room — see
      // broadcastVoicePresence for why).
      broadcastVoicePresence({
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

        broadcastVoicePresence({
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
        broadcastVoicePresence({
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
