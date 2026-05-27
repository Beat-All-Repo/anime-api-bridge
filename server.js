"use strict";

/**
 * anime-api-bridge — server.js
 * Production-grade Express + MongoDB Native Driver + GramJS streaming microservice.
 * Designed for Render Free Tier (512 MB RAM hard ceiling).
 */

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const { MongoClient, ServerApiVersion } = require("mongodb");
const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { Api } = require("telegram/tl");

// ─────────────────────────────────────────────────────────────────────────────
// 1. ENVIRONMENT VARIABLE VALIDATION — fail fast, fail loud
// ─────────────────────────────────────────────────────────────────────────────
const REQUIRED_ENV = [
  "MONGODB_URI",
  "TELEGRAM_API_ID",
  "TELEGRAM_API_HASH",
  "TELEGRAM_BOT_TOKEN",
];

let missingEnv = false;
for (const key of REQUIRED_ENV) {
  if (!process.env[key] || process.env[key].trim() === "") {
    console.error(
      `[FATAL] Missing required environment variable: ${key}. ` +
        `Set it in your .env file or Render dashboard before starting.`
    );
    missingEnv = true;
  }
}
if (missingEnv) {
  process.exit(1);
}

const MONGODB_URI = process.env.MONGODB_URI.trim();
const TELEGRAM_API_ID = parseInt(process.env.TELEGRAM_API_ID.trim(), 10);
const TELEGRAM_API_HASH = process.env.TELEGRAM_API_HASH.trim();
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN.trim();
const PORT = parseInt(process.env.PORT || "3000", 10);
const DB_NAME = process.env.DB_NAME || "anime_db";
const COLLECTION_NAME = "anime_series";

if (isNaN(TELEGRAM_API_ID) || TELEGRAM_API_ID <= 0) {
  console.error(
    "[FATAL] TELEGRAM_API_ID must be a valid positive integer. Check your .env."
  );
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. MODULE-LEVEL SINGLETONS
// ─────────────────────────────────────────────────────────────────────────────
let mongoClient = null;
let db = null;
let animeCollection = null;
let tgClient = null;

// ─────────────────────────────────────────────────────────────────────────────
// 3. EXPRESS APP + CORS CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────────
const app = express();

/**
 * CORS is configured to allow all origins because this API serves video bytes
 * to browser-based players that may be hosted on any domain. Preflight
 * requests for Range-based partial-content streaming must be permitted.
 */
app.use(
  cors({
    origin: "*",
    methods: ["GET", "HEAD", "OPTIONS"],
    allowedHeaders: [
      "Range",
      "Content-Type",
      "Accept",
      "Accept-Encoding",
      "Origin",
      "X-Requested-With",
    ],
    exposedHeaders: [
      "Content-Range",
      "Accept-Ranges",
      "Content-Length",
      "Content-Type",
    ],
    preflightContinue: false,
    optionsSuccessStatus: 204,
  })
);

app.use(express.json({ limit: "1mb" }));

/**
 * Health-check endpoint for Render's keep-alive pings and uptime monitors.
 */
app.get("/health", (_req, res) => {
  res.status(200).json({
    status: "ok",
    uptime: process.uptime(),
    memoryMB: (process.memoryUsage().rss / 1024 / 1024).toFixed(2),
    timestamp: new Date().toISOString(),
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 4. ROUTE A — GET /api/catalog
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns all anime series that have at least one indexed episode.
 * The `episodes` array is intentionally excluded from this projection
 * to minimise response payload — the UI only needs catalogue metadata here.
 */
app.get("/api/catalog", async (_req, res) => {
  try {
    const items = await animeCollection
      .find(
        { total_episodes_indexed: { $gt: 0 } },
        {
          projection: {
            _id: 1,
            anime_id: 1,
            title: 1,
            cover_image: 1,
            total_episodes_indexed: 1,
            last_updated: 1,
          },
        }
      )
      .sort({ last_updated: -1 })
      .toArray();

    return res.status(200).json({ success: true, count: items.length, data: items });
  } catch (err) {
    console.error("[/api/catalog] Database query failed:", err.message);
    return res.status(500).json({
      success: false,
      error: "Internal server error while fetching catalogue.",
    });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 5. ROUTE B — GET /api/series/:slug
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Resolves a complete series document (including full episodes array) by
 * its URL-safe `anime_id` slug. Returns 404 when the slug does not exist.
 */
app.get("/api/series/:slug", async (req, res) => {
  const { slug } = req.params;

  if (!slug || typeof slug !== "string" || slug.trim() === "") {
    return res.status(400).json({ success: false, error: "Invalid slug parameter." });
  }

  try {
    const series = await animeCollection.findOne({ anime_id: slug.trim().toLowerCase() });

    if (!series) {
      return res.status(404).json({
        success: false,
        error: `No series found with anime_id: "${slug}".`,
      });
    }

    return res.status(200).json({ success: true, data: series });
  } catch (err) {
    console.error(`[/api/series/${slug}] Database query failed:`, err.message);
    return res.status(500).json({
      success: false,
      error: "Internal server error while fetching series.",
    });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 6. ROUTE C — GET /stream/telegram/:channelId/:messageId
//    CORE PARTIAL-CONTENT STREAMING ENGINE
// ─────────────────────────────────────────────────────────────────────────────
app.get("/stream/telegram/:channelId/:messageId", async (req, res) => {
  const { channelId, messageId } = req.params;

  // ── 6.1 Validate path parameters ──────────────────────────────────────────
  if (!channelId || !messageId) {
    return res.status(400).json({
      success: false,
      error: "Both channelId and messageId path parameters are required.",
    });
  }

  const parsedMessageId = parseInt(messageId, 10);
  if (isNaN(parsedMessageId) || parsedMessageId <= 0) {
    return res.status(400).json({
      success: false,
      error: "messageId must be a positive integer.",
    });
  }

  // ── 6.2 Enforce Range header — browsers require this for partial content ──
  const rangeHeader = req.headers["range"];
  if (!rangeHeader) {
    return res.status(400).json({
      success: false,
      error:
        "HTTP Range header is required for video streaming. " +
        "Your browser or player must send a Range request (e.g., 'Range: bytes=0-').",
    });
  }

  // ── 6.3 Normalise Channel ID for MTProto PeerChannel ──────────────────────
  /**
   * Telegram channel IDs in Bot API format are prefixed with "-100".
   * MTProto PeerChannel requires only the bare positive numeric ID.
   * We strip "-100" if present, then parse as BigInt to satisfy GramJS's
   * expectation for large channel IDs.
   */
  let normalizedChannelId;
  try {
    let rawId = channelId.toString().trim();
    if (rawId.startsWith("-100")) {
      rawId = rawId.slice(4); // remove "-100" prefix
    } else if (rawId.startsWith("-")) {
      rawId = rawId.slice(1); // remove plain "-" prefix (legacy group IDs)
    }
    normalizedChannelId = BigInt(rawId);
    if (normalizedChannelId <= 0n) {
      throw new Error("Channel ID must resolve to a positive BigInt.");
    }
  } catch (err) {
    return res.status(400).json({
      success: false,
      error: `Invalid channelId format: "${channelId}". ${err.message}`,
    });
  }

  // ── 6.4 Resolve the Telegram message & verify it contains a document ──────
  let tgMessage;
  try {
    const peer = new Api.PeerChannel({ channelId: normalizedChannelId });
    const messages = await tgClient.getMessages(peer, { ids: [parsedMessageId] });

    if (!messages || messages.length === 0 || !messages[0]) {
      return res.status(404).json({
        success: false,
        error: `No Telegram message found for channelId=${channelId}, messageId=${messageId}.`,
      });
    }

    tgMessage = messages[0];

    // Confirm the message carries a streamable document (video file)
    if (!tgMessage.document && !tgMessage.media?.document) {
      return res.status(422).json({
        success: false,
        error:
          `Telegram message ${messageId} in channel ${channelId} does not contain ` +
          `a document/video file. Cannot stream.`,
      });
    }
  } catch (err) {
    console.error(
      `[/stream] Failed to resolve Telegram message (ch=${channelId}, msg=${messageId}):`,
      err.message
    );
    return res.status(502).json({
      success: false,
      error: "Failed to retrieve media metadata from Telegram. " + err.message,
    });
  }

  const document = tgMessage.document || tgMessage.media.document;
  const totalFileSize = Number(document.size);

  // ── 6.5 Parse Range header bytes ──────────────────────────────────────────
  /**
   * Range header format: "bytes=START-END" or "bytes=START-"
   * When END is omitted, browsers expect the server to decide the chunk size.
   * We cap chunks at 5 MB to keep the 512 MB Render heap safe.
   */
  const MAX_CHUNK_BYTES = 5 * 1024 * 1024; // 5 MB
  let startByte, endByte, chunkLength;

  const rangeMatch = rangeHeader.match(/^bytes=(\d+)-(\d*)$/);
  if (!rangeMatch) {
    res.setHeader("Content-Range", `bytes */${totalFileSize}`);
    return res.status(416).json({
      success: false,
      error: `Malformed Range header: "${rangeHeader}". Expected format: bytes=START-END`,
    });
  }

  startByte = parseInt(rangeMatch[1], 10);
  endByte =
    rangeMatch[2] !== ""
      ? parseInt(rangeMatch[2], 10)
      : Math.min(startByte + MAX_CHUNK_BYTES - 1, totalFileSize - 1);

  // Clamp endByte to the actual file boundary
  endByte = Math.min(endByte, totalFileSize - 1);

  if (startByte > endByte || startByte >= totalFileSize || startByte < 0) {
    res.setHeader("Content-Range", `bytes */${totalFileSize}`);
    return res.status(416).json({
      success: false,
      error: `Range ${startByte}-${endByte} is not satisfiable for file of size ${totalFileSize}.`,
    });
  }

  chunkLength = endByte - startByte + 1;

  // ── 6.6 Derive MIME type from Telegram document attributes ────────────────
  let mimeType = document.mimeType || "video/mp4";
  if (!mimeType.startsWith("video/") && !mimeType.startsWith("application/")) {
    mimeType = "video/mp4"; // safe fallback
  }

  // ── 6.7 Send HTTP 206 Partial Content headers ─────────────────────────────
  res.writeHead(206, {
    "Content-Range": `bytes ${startByte}-${endByte}/${totalFileSize}`,
    "Accept-Ranges": "bytes",
    "Content-Length": chunkLength,
    "Content-Type": mimeType,
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
  });

  // ── 6.8 Client-disconnect guard ───────────────────────────────────────────
  /**
   * This flag is the critical memory-leak prevention mechanism.
   * When the browser tab closes, seeks, or switches tracks, the TCP connection
   * is severed and Node fires 'close'. We flip this flag to break out of the
   * GramJS iterDownload loop immediately, preventing further buffer allocation.
   */
  let clientDisconnected = false;
  req.on("close", () => {
    clientDisconnected = true;
    if (!res.writableEnded) {
      res.end();
    }
  });

  // ── 6.9 Stream data via GramJS iterDownload ───────────────────────────────
  /**
   * iterDownload yields Buffer chunks from Telegram's MTProto layer.
   * requestSize is kept at 1 MB to prevent large heap allocations.
   * offset must be aligned to requestSize boundaries; GramJS handles the
   * internal alignment, but we set offset to our startByte so it begins
   * at the correct position.
   *
   * The `limit` parameter restricts the total bytes fetched to exactly
   * `chunkLength` so we do not over-read into the next chunk window.
   */
  try {
    const ITER_REQUEST_SIZE = 1024 * 1024; // 1 MB per MTProto request
    let bytesWritten = 0;

    for await (const chunk of tgClient.iterDownload({
      file: new Api.InputDocumentFileLocation({
        id: document.id,
        accessHash: document.accessHash,
        fileReference: document.fileReference,
        thumbSize: "",
      }),
      requestSize: ITER_REQUEST_SIZE,
      offset: BigInt(startByte),
      limit: chunkLength,
    })) {
      // Abort immediately if the client has disconnected
      if (clientDisconnected) {
        break;
      }

      // Determine how many bytes from this chunk are still needed
      const remaining = chunkLength - bytesWritten;
      if (remaining <= 0) {
        break;
      }

      // Slice the buffer if the final GramJS chunk overshoots our window
      const slice = chunk.length > remaining ? chunk.slice(0, remaining) : chunk;

      // Write synchronously; if the socket has died, stop
      if (res.writableEnded || clientDisconnected) {
        break;
      }

      const canContinue = res.write(slice);
      bytesWritten += slice.length;

      /**
       * Respect backpressure: if the writable buffer is full (canContinue === false),
       * wait for the 'drain' event before resuming to prevent heap overflow.
       */
      if (!canContinue && !clientDisconnected) {
        await new Promise((resolve) => {
          res.once("drain", resolve);
          req.once("close", resolve); // don't hang forever if client drops mid-drain
        });
      }

      if (bytesWritten >= chunkLength) {
        break;
      }
    }
  } catch (err) {
    /**
     * Network timeouts, MTProto errors, and Telegram peer resets are caught here.
     * We do NOT rethrow — doing so would crash the Node process. Instead we log
     * and cleanly end the response if it hasn't been finished already.
     */
    if (!clientDisconnected) {
      console.error(
        `[/stream] Streaming error (ch=${channelId}, msg=${messageId}, ` +
          `range=${startByte}-${endByte}): ${err.message}`
      );
    }
  } finally {
    if (!res.writableEnded) {
      res.end();
    }
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 7. 404 CATCH-ALL FOR UNMATCHED ROUTES
// ─────────────────────────────────────────────────────────────────────────────
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: `Route not found: ${req.method} ${req.originalUrl}`,
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 8. GLOBAL UNCAUGHT EXCEPTION AND UNHANDLED REJECTION GUARDS
//    Prevents silent crashes on Render's free tier from taking down the pod.
// ─────────────────────────────────────────────────────────────────────────────
process.on("uncaughtException", (err) => {
  console.error("[UNCAUGHT EXCEPTION] Process will NOT exit. Error:", err);
});

process.on("unhandledRejection", (reason) => {
  console.error("[UNHANDLED REJECTION] Reason:", reason);
});

// ─────────────────────────────────────────────────────────────────────────────
// 9. GRACEFUL SHUTDOWN — SIGTERM / SIGINT
// ─────────────────────────────────────────────────────────────────────────────
async function gracefulShutdown(signal) {
  console.log(`[SHUTDOWN] Received ${signal}. Closing connections…`);
  try {
    if (tgClient && tgClient.connected) {
      await tgClient.disconnect();
      console.log("[SHUTDOWN] Telegram client disconnected.");
    }
  } catch (err) {
    console.error("[SHUTDOWN] Error disconnecting Telegram client:", err.message);
  }
  try {
    if (mongoClient) {
      await mongoClient.close();
      console.log("[SHUTDOWN] MongoDB client closed.");
    }
  } catch (err) {
    console.error("[SHUTDOWN] Error closing MongoDB client:", err.message);
  }
  process.exit(0);
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

// ─────────────────────────────────────────────────────────────────────────────
// 10. BOOTSTRAP PIPELINE — DB → INDEXES → TELEGRAM → HTTP SERVER
// ─────────────────────────────────────────────────────────────────────────────
async function bootstrap() {
  // ── 10.1 MongoDB ──────────────────────────────────────────────────────────
  console.log("[BOOT] Connecting to MongoDB…");
  mongoClient = new MongoClient(MONGODB_URI, {
    serverApi: {
      version: ServerApiVersion.v1,
      strict: true,
      deprecationErrors: true,
    },
    /**
     * Connection pool tuned for Render free tier:
     * Low pool size prevents excessive memory usage from idle sockets.
     */
    maxPoolSize: 5,
    minPoolSize: 1,
    connectTimeoutMS: 10000,
    socketTimeoutMS: 45000,
  });

  await mongoClient.connect();
  db = mongoClient.db(DB_NAME);
  animeCollection = db.collection(COLLECTION_NAME);
  console.log(`[BOOT] MongoDB connected. Database: "${DB_NAME}", Collection: "${COLLECTION_NAME}"`);

  // ── 10.2 Collection Indexes ───────────────────────────────────────────────
  console.log("[BOOT] Ensuring collection indexes…");

  /**
   * Compound index for catalogue sort queries (newest, most episodes first).
   * Using createIndex is idempotent — safe to call on every boot.
   */
  await animeCollection.createIndex(
    { total_episodes_indexed: -1, last_updated: -1 },
    {
      name: "compound_catalog_sort",
      background: true,
    }
  );

  /**
   * Unique index on anime_id enforces slug uniqueness and provides O(1)
   * resolution for series lookups.
   */
  await animeCollection.createIndex(
    { anime_id: 1 },
    {
      name: "unique_anime_id_slug",
      unique: true,
      background: true,
    }
  );

  console.log("[BOOT] Collection indexes are in place.");

  // ── 10.3 GramJS Telegram Client ───────────────────────────────────────────
  console.log("[BOOT] Initialising GramJS Telegram client…");

  /**
   * An empty StringSession is used because this service authenticates as a
   * Bot via token, not as a user account. GramJS stores no session state.
   */
  tgClient = new TelegramClient(
    new StringSession(""),
    TELEGRAM_API_ID,
    TELEGRAM_API_HASH,
    {
      connectionRetries: 5,
      retryDelay: 2000,
      autoReconnect: true,
      /**
       * Suppress GramJS's default verbose logging to keep Render log streams
       * clean. Errors are still surfaced via the try/catch in route C.
       */
      baseLogger: {
        levels: [],
        getLogger: () => ({
          debug: () => {},
          info: () => {},
          warn: (msg) => console.warn("[GramJS WARN]", msg),
          error: (msg) => console.error("[GramJS ERROR]", msg),
        }),
      },
    }
  );

  await tgClient.start({
    botAuthToken: TELEGRAM_BOT_TOKEN,
  });

  console.log("[BOOT] Telegram client authenticated as bot. MTProto session active.");

  // ── 10.4 HTTP Server ──────────────────────────────────────────────────────
  app.listen(PORT, () => {
    console.log(`[BOOT] anime-api-bridge is live on port ${PORT}`);
    console.log(`[BOOT] Health check → http://localhost:${PORT}/health`);
    console.log(`[BOOT] Catalogue    → http://localhost:${PORT}/api/catalog`);
    console.log(`[BOOT] Series       → http://localhost:${PORT}/api/series/:slug`);
    console.log(`[BOOT] Stream       → http://localhost:${PORT}/stream/telegram/:channelId/:messageId`);
  });
}

bootstrap().catch((err) => {
  console.error("[FATAL] Bootstrap failed:", err);
  process.exit(1);
});
