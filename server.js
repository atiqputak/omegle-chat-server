"use strict";

const express = require("express");
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({
  server,
  maxPayload: 16 * 1024
});

const waitingUsers = new Set();
const partners = new Map();
const userModes = new Map();

app.get("/", (_req, res) => {
  res.status(200).send("OMEGLE chat server is running.");
});

app.get("/health", (_req, res) => {
  res.status(200).json({
    status: "ok",
    waiting: waitingUsers.size,
    connected: wss.clients.size
  });
});

function send(ws, data) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;

  try {
    ws.send(JSON.stringify(data));
    return true;
  } catch (error) {
    console.error("WebSocket send error:", error.message);
    return false;
  }
}

function removeFromQueue(ws) {
  waitingUsers.delete(ws);
}

function disconnect(ws, message = "partner_left") {
  removeFromQueue(ws);

  const partner = partners.get(ws);
  partners.delete(ws);

  if (partner) {
    partners.delete(partner);
    send(partner, { type: message });
  }
}

function validMode(mode) {
  return mode === "text" || mode === "video";
}

function findPartner(ws, requestedMode) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;

  if (!validMode(requestedMode)) {
    send(ws, { type: "error", message: "invalid_mode" });
    return;
  }

  removeFromQueue(ws);

  if (partners.has(ws)) disconnect(ws);
  userModes.set(ws, requestedMode);

  for (const candidate of waitingUsers) {
    if (
      candidate === ws ||
      candidate.readyState !== WebSocket.OPEN ||
      partners.has(candidate) ||
      userModes.get(candidate) !== requestedMode
    ) {
      continue;
    }

    removeFromQueue(candidate);
    partners.set(ws, candidate);
    partners.set(candidate, ws);

    const firstSent = send(ws, {
      type: "matched",
      initiator: true,
      mode: requestedMode
    });

    const secondSent = send(candidate, {
      type: "matched",
      initiator: false,
      mode: requestedMode
    });

    if (!firstSent || !secondSent) {
      disconnect(ws);
      disconnect(candidate);
      if (candidate.readyState === WebSocket.OPEN && !partners.has(candidate)) {
        waitingUsers.add(candidate);
      }
      continue;
    }

    console.log("Matched users in mode:", requestedMode);
    return;
  }

  waitingUsers.add(ws);
  send(ws, { type: "waiting", mode: requestedMode });
}

function relaySignal(ws, data) {
  const partner = partners.get(ws);

  if (!partner || partner.readyState !== WebSocket.OPEN) {
    send(ws, { type: "error", message: "not_matched" });
    return;
  }

  if (userModes.get(ws) !== "video") {
    send(ws, { type: "error", message: "video_mode_required" });
    return;
  }

  let outgoing;

  if (data.type === "offer" || data.type === "answer") {
    const description = data[data.type] || data.description;

    if (
      !description ||
      typeof description.sdp !== "string" ||
      description.sdp.length > 12000
    ) {
      send(ws, { type: "error", message: "invalid_description" });
      return;
    }

    outgoing = {
      type: data.type,
      [data.type]: {
        type: description.type,
        sdp: description.sdp
      }
    };
  } else {
    const candidate = data.candidate;

    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
      return;
    }

    outgoing = { type: "ice-candidate", candidate };
  }

  send(partner, outgoing);
}

wss.on("connection", (ws) => {
  send(ws, { type: "connected" });

  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch (_) {
      send(ws, { type: "error", message: "invalid_json" });
      return;
    }

    if (!data || typeof data.type !== "string") return;

    switch (data.type) {
      case "find":
        findPartner(ws, data.mode);
        break;

      case "message": {
        if (typeof data.text !== "string") return;

        const text = data.text.trim();
        if (!text || text.length > 1000) return;

        const partner = partners.get(ws);

        if (!partner || partner.readyState !== WebSocket.OPEN) {
          send(ws, { type: "error", message: "not_matched" });
          return;
        }

        send(partner, { type: "message", text });
        break;
      }

      case "offer":
      case "answer":
      case "ice-candidate":
        relaySignal(ws, data);
        break;

      case "next": {
        const mode = userModes.get(ws);
        disconnect(ws);

        if (validMode(mode)) findPartner(ws, mode);
        else send(ws, { type: "error", message: "invalid_mode" });
        break;
      }

      case "end":
        disconnect(ws);
        userModes.delete(ws);
        send(ws, { type: "ended" });
        break;

      default:
        send(ws, { type: "error", message: "unknown_message_type" });
    }
  });

  ws.on("close", () => {
    disconnect(ws);
    userModes.delete(ws);
  });

  ws.on("error", (error) => {
    console.error("WebSocket client error:", error.message);
    disconnect(ws);
    userModes.delete(ws);
  });
});

wss.on("error", (error) => {
  console.error("WebSocket server error:", error);
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`OMEGLE server listening on port ${PORT}`);
});
