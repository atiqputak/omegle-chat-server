
"use strict";

const express = require("express");
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const app = express();
const server = http.createServer(app);

const wss = new WebSocketServer({
  server,
  maxPayload: 16 * 1024,
  perMessageDeflate: false
});

const waitingUsers = new Set();
const partners = new Map();
const userModes = new Map();

const PORT = process.env.PORT || 3000;

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
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return false;
  }

  try {
    ws.send(JSON.stringify(data));
    return true;
  } catch (error) {
    console.error("WebSocket send error:", error.message);
    return false;
  }
}

function validMode(mode) {
  return mode === "text" || mode === "video";
}

function removeFromQueue(ws) {
  waitingUsers.delete(ws);
}

/*
 * Disconnect a user from their current partner.
 * Optionally place the partner back into the waiting queue.
 */
function disconnect(
  ws,
  { notifyPartner = true, requeuePartner = false } = {}
) {
  removeFromQueue(ws);

  const partner = partners.get(ws);

  if (!partner) {
    return null;
  }

  partners.delete(ws);
  partners.delete(partner);

  if (notifyPartner) {
    send(partner, { type: "partner_left" });
  }

  if (
    requeuePartner &&
    partner.readyState === WebSocket.OPEN
  ) {
    const partnerMode = userModes.get(partner);

    if (validMode(partnerMode)) {
      waitingUsers.add(partner);
      send(partner, {
        type: "waiting",
        mode: partnerMode
      });
    }
  }

  return partner;
}

/*
 * Find a partner using the same chat mode.
 * An excluded user is skipped so that pressing Next
 * does not immediately reconnect the same two people.
 */
function findPartner(ws, requestedMode, excludedPartner = null) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }

  if (!validMode(requestedMode)) {
    send(ws, {
      type: "error",
      message: "invalid_mode"
    });
    return;
  }

  removeFromQueue(ws);

  if (partners.has(ws)) {
    const previousPartner = disconnect(ws, {
      notifyPartner: true,
      requeuePartner: true
    });

    if (previousPartner) {
      excludedPartner = previousPartner;
    }
  }

  userModes.set(ws, requestedMode);

  for (const candidate of Array.from(waitingUsers)) {
    if (
      candidate === ws ||
      candidate === excludedPartner ||
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
      partners.delete(ws);
      partners.delete(candidate);

      if (ws.readyState === WebSocket.OPEN) {
        waitingUsers.add(ws);
      }

      if (candidate.readyState === WebSocket.OPEN) {
        waitingUsers.add(candidate);
      }

      send(ws, {
        type: "waiting",
        mode: requestedMode
      });

      send(candidate, {
        type: "waiting",
        mode: userModes.get(candidate)
      });

      continue;
    }

    console.log("Matched users:", requestedMode);
    return;
  }

  waitingUsers.add(ws);

  send(ws, {
    type: "waiting",
    mode: requestedMode
  });
}

/*
 * Relay WebRTC offers, answers and ICE candidates.
 * The server forwards signaling data; it does not
 * carry the actual video or audio stream.
 */
function relaySignal(ws, data) {
  const partner = partners.get(ws);

  if (!partner || partner.readyState !== WebSocket.OPEN) {
    send(ws, {
      type: "error",
      message: "not_matched"
    });
    return;
  }

  if (userModes.get(ws) !== "video") {
    send(ws, {
      type: "error",
      message: "video_mode_required"
    });
    return;
  }

  if (data.type === "offer" || data.type === "answer") {
    const description = data[data.type] || data.description;

    if (
      !description ||
      typeof description !== "object" ||
      description.type !== data.type ||
      typeof description.sdp !== "string" ||
      description.sdp.length > 12000
    ) {
      send(ws, {
        type: "error",
        message: "invalid_description"
      });
      return;
    }

    send(partner, {
      type: data.type,
      [data.type]: {
        type: description.type,
        sdp: description.sdp
      }
    });

    return;
  }

  if (data.type === "ice-candidate") {
    const candidate = data.candidate;

    if (
      !candidate ||
      typeof candidate !== "object" ||
      Array.isArray(candidate)
    ) {
      return;
    }

    if (
      candidate.candidate !== undefined &&
      typeof candidate.candidate !== "string"
    ) {
      return;
    }

    send(partner, {
      type: "ice-candidate",
      candidate
    });
  }
}

wss.on("connection", (ws) => {
  console.log("Client connected.");

  send(ws, { type: "connected" });

  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch (_error) {
      send(ws, {
        type: "error",
        message: "invalid_json"
      });
      return;
    }

    if (
      !data ||
      typeof data !== "object" ||
      typeof data.type !== "string"
    ) {
      return;
    }

    switch (data.type) {
      case "find": {
        findPartner(ws, data.mode);
        break;
      }

      case "message": {
        if (typeof data.text !== "string") {
          return;
        }

        const text = data.text.trim();

        if (!text || text.length > 1000) {
          return;
        }

        const partner = partners.get(ws);

        if (
          !partner ||
          partner.readyState !== WebSocket.OPEN
        ) {
          send(ws, {
            type: "error",
            message: "not_matched"
          });
          return;
        }

        send(partner, {
          type: "message",
          text
        });

        break;
      }

      case "offer":
      case "answer":
      case "ice-candidate": {
        relaySignal(ws, data);
        break;
      }

      case "next": {
        const mode = userModes.get(ws);

        const previousPartner = disconnect(ws, {
          notifyPartner: true,
          requeuePartner: true
        });

        if (validMode(mode)) {
          findPartner(ws, mode, previousPartner);
        } else {
          send(ws, {
            type: "error",
            message: "invalid_mode"
          });
        }

        break;
      }

      case "end": {
        disconnect(ws, {
          notifyPartner: true,
          requeuePartner: true
        });

        userModes.delete(ws);

        send(ws, { type: "ended" });
        break;
      }

      default: {
        send(ws, {
          type: "error",
          message: "unknown_message_type"
        });
      }
    }
  });

  ws.on("close", () => {
    console.log("Client disconnected.");

    disconnect(ws, {
      notifyPartner: true,
      requeuePartner: true
    });

    userModes.delete(ws);
  });

  ws.on("error", (error) => {
    console.error("WebSocket client error:", error.message);

    disconnect(ws, {
      notifyPartner: true,
      requeuePartner: true
    });

    userModes.delete(ws);
  });
});

wss.on("error", (error) => {
  console.error("WebSocket server error:", error.message);
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`OMEGLE server listening on port ${PORT}`);
});
