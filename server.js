
const express = require("express");
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.get("/", (_req, res) => {
  res.status(200).send("OMEGLE chat server is running.");
});

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

const waitingUsers = new Set();
const partners = new Map();

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function removeFromQueue(ws) {
  waitingUsers.delete(ws);
}

function disconnect(ws, notify = true) {
  removeFromQueue(ws);

  const partner = partners.get(ws);
  partners.delete(ws);

  if (partner) {
    partners.delete(partner);
    if (notify) {
      send(partner, { type: "partner_left" });
    }
  }
}

function findPartner(ws) {
  if (ws.readyState !== WebSocket.OPEN) return;

  // Prevent the same socket from being matched twice.
  if (partners.has(ws)) return;

  removeFromQueue(ws);

  for (const candidate of waitingUsers) {
    if (
      candidate !== ws &&
      candidate.readyState === WebSocket.OPEN &&
      !partners.has(candidate)
    ) {
      removeFromQueue(candidate);

      partners.set(ws, candidate);
      partners.set(candidate, ws);

      send(ws, { type: "matched", initiator: true });
      send(candidate, { type: "matched", initiator: false });
      return;
    }
  }

  waitingUsers.add(ws);
  send(ws, { type: "waiting" });
}

wss.on("connection", (ws) => {
  send(ws, { type: "connected" });

  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      send(ws, { type: "error", message: "invalid_json" });
      return;
    }

    if (!data || typeof data.type !== "string") return;

    if (data.type === "find") {
      if (partners.has(ws)) disconnect(ws);
      findPartner(ws);
      return;
    }

    if (data.type === "message") {
      if (typeof data.text !== "string") return;

      const text = data.text.trim();
      if (!text || text.length > 1000) return;

      const partner = partners.get(ws);

      if (partner && partner.readyState === WebSocket.OPEN) {
        send(partner, { type: "message", text });
      } else {
        send(ws, { type: "error", message: "not_matched" });
      }
      return;
    }

    if (
      data.type === "offer" ||
      data.type === "answer" ||
      data.type === "ice-candidate"
    ) {
      const partner = partners.get(ws);

      if (!partner || partner.readyState !== WebSocket.OPEN) {
        send(ws, { type: "error", message: "not_matched" });
        return;
      }

      if (data.type === "offer" || data.type === "answer") {
        const description =
          data.type === "offer"
            ? (data.offer || data.description)
            : (data.answer || data.description);

        if (
          !description ||
          typeof description.sdp !== "string" ||
          !["offer", "answer"].includes(description.type)
        ) {
          send(ws, { type: "error", message: "invalid_description" });
          return;
        }

        send(partner, {
          type: data.type,
          [data.type]: description
        });
        return;
      }

      if (
        !data.candidate ||
        typeof data.candidate !== "object" ||
        typeof data.candidate.candidate !== "string"
      ) {
        return;
      }

      send(partner, {
        type: "ice-candidate",
        candidate: data.candidate
      });
      return;
    }

    if (data.type === "next") {
      disconnect(ws);
      findPartner(ws);
      return;
    }

    if (data.type === "end") {
      disconnect(ws);
      send(ws, { type: "ended" });
    }
  });

  ws.on("close", () => disconnect(ws));
  ws.on("error", () => disconnect(ws));
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`OMEGLE server listening on port ${PORT}`);
});
