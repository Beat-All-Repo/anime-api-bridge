"use strict";

/**
 * server.js — anime-api-bridge streaming microservice
 *
 * Fixes & additions in this version:
 *  1. Schema aligned with indexer.js → episode_number, channel_id, message_id
 *  2. Watch route implements priority fallback chain:
 *       Priority 1 → Torrent/Magnet  (Nyaa scraped or caption-provided)
 *       Priority 2 → Telegram stream  (GramJS partial-content HTTP stream)
 *       Priority 3 → Iframe API       (beat-anime-api-backup, prefer as-cdn21.top)
 *  3. New helper: resolveIframeStream(title, episodeNumber, seasonNumber)
 *  4. All previous streaming/validation logic preserved and hardened
 */

require("dotenv").config();

const express = require("express");
const cors    = require("cors");
const { MongoClient, ServerApiVersion } = require("mongodb");
const { TelegramClient } = require("telegram");
const { StringSession }  = require("telegram/sessions");
const { Api }    = require("telegram/tl");
const { Logger } = require("telegram/extensions/Logger");

// ─────────────────────────────────────────────────────────────────────────────
// 1. ENVIRONMENT VALIDATION
// ─────────────────────────────────────────────────────────────────────────────

const REQUIRED_ENV = [
  "MONGODB_URI", "TELEGRAM_API_ID", "TELEGRAM_API_HASH", "TELEGRAM_BOT_TOKEN",
];

let missingEnv = false;
for (const key of REQUIRED_ENV) {
  if (!process.env[key] || process.env[key].trim() === "") {
    console.error(`[FATAL] Missing required env var: ${key}`);
    missingEnv = true;
  }
}
if (missingEnv) process.exit(1);

const MONGODB_URI       = process.env.MONGODB_URI.trim();
const TELEGRAM_API_ID   = parseInt(process.env.TELEGRAM_API_ID.trim(), 10);
const TELEGRAM_API_HASH = process.env.TELEGRAM_API_HASH.trim();
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN.trim();
const PORT              = parseInt(process.env.PORT || "3000", 10);
const DB_NAME           = process.env.DB_NAME || "beataniverse";      // FIX: was "anime_db"
const COLLECTION_NAME   = "anime_series";

// Iframe / external stream API config
const IFRAME_API_BASE   = (process.env.IFRAME_API_URL || "https://beat-anime-api-backup.onrender.com").replace(/\/$/, "");
const IFRAME_CDN_PREFER = process.env.IFRAME_CDN_PREFER || "https://as-cdn21.top";

if (isNaN(TELEGRAM_API_ID) || TELEGRAM_API_ID <= 0) {
  console.error("[FATAL] TELEGRAM_API_ID must be a valid positive integer.");
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. MODULE-LEVEL SINGLETONS
// ─────────────────────────────────────────────────────────────────────────────

let mongoClient     = null;
let db              = null;
let animeCollection = null;
let tgClient        = null;
let tgReady         = false;

// ─────────────────────────────────────────────────────────────────────────────
// 3. EXPRESS + CORS
// ─────────────────────────────────────────────────────────────────────────────

const app = express();

app.use(cors({
  origin: "*",
  methods: ["GET", "HEAD", "OPTIONS"],
  allowedHeaders: ["Range", "Content-Type", "Accept", "Accept-Encoding", "Origin", "X-Requested-With"],
  exposedHeaders: ["Content-Range", "Accept-Ranges", "Content-Length", "Content-Type"],
  preflightContinue: false,
  optionsSuccessStatus: 204,
}));

app.use(express.json({ limit: "1mb" }));

// ─────────────────────────────────────────────────────────────────────────────
// 4. IFRAME API HELPER — resolves embed URLs from beat-anime-api-backup
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Searches the iframe API for this anime, then fetches stream sources.
 * Returns an ordered array of embed URLs, with as-cdn21.top first.
 *
 * API shape (from PrathmeshGOAT/anime-api):
 *   GET /api/search?s={title}&page=1
 *     → { results: { results: [{ anime_id, title, poster }] } }
 *   GET /api/stream?id={anime_id}&season={season}&ep={episode}
 *     → { results: [{ server, embed }] }
 */
async function resolveIframeStream(title, episodeNumber, seasonNumber = 1) {
  const TIMEOUT_MS = 12000;

  /**
   * Helper: fetch with abort timeout.
   */
  async function fetchWithTimeout(url) {
    const ctrl = new AbortController();
    const tid  = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    try {
      const r = await fetch(url, {
        signal: ctrl.signal,
        headers: { "User-Agent": "BeatAniVerse-Bridge/1.0" },
      });
      return r;
    } finally {
      clearTimeout(tid);
    }
  }

  try {
    // Step 1: Search for the anime to get its external anime_id
    const searchUrl = `${IFRAME_API_BASE}/api/search?s=${encodeURIComponent(title)}&page=1`;
    console.log(`[IFRAME] Searching: ${searchUrl}`);

    const searchRes = await fetchWithTimeout(searchUrl);
    if (!searchRes.ok) {
      console.warn(`[IFRAME] Search HTTP ${searchRes.status} for "${title}"`);
      return null;
    }

    const searchJson = await searchRes.json();
    const hits       = searchJson?.results?.results || searchJson?.results || [];

    if (!Array.isArray(hits) || hits.length === 0) {
      console.warn(`[IFRAME] No search results for "${title}"`);
      return null;
    }

    // Pick the best-matching result (first result from the API, which is ranked)
    const hit         = hits[0];
    const externalId  = hit.anime_id || hit.id;
    if (!externalId) {
      console.warn(`[IFRAME] Result has no anime_id for "${title}"`);
      return null;
    }

    console.log(`[IFRAME] Found anime_id: "${externalId}" for "${title}"`);

    // Step 2: Fetch stream servers
    const streamUrl = `${IFRAME_API_BASE}/api/stream?id=${encodeURIComponent(externalId)}&season=${seasonNumber}&ep=${episodeNumber}`;
    console.log(`[IFRAME] Stream URL: ${streamUrl}`);

    const streamRes = await fetchWithTimeout(streamUrl);
    if (!streamRes.ok) {
      console.warn(`[IFRAME] Stream HTTP ${streamRes.status} for ${externalId} S${seasonNumber}E${episodeNumber}`);
      return null;
    }

    const streamJson = await streamRes.json();
    const servers    = streamJson?.results || [];

    if (!Array.isArray(servers) || servers.length === 0) {
      console.warn(`[IFRAME] No stream servers returned for ${externalId} Ep${episodeNumber}`);
      return null;
    }

    // Step 3: Sort — preferred CDN (as-cdn21.top) first, rest follow
    const embeds = servers
      .map((s) => s.embed || s.iframe || s.url || null)
      .filter(Boolean);

    if (embeds.length === 0) return null;

    // Re-sort: entries containing the preferred CDN domain go to the front
    embeds.sort((a, b) => {
      const aPreferred = a.includes(IFRAME_CDN_PREFER) ? -1 : 0;
      const bPreferred = b.includes(IFRAME_CDN_PREFER) ? -1 : 0;
      return aPreferred - bPreferred;
    });

    console.log(`[IFRAME] ✅ Resolved ${embeds.length} server(s). First: ${embeds[0]}`);
    return embeds;

  } catch (err) {
    const reason = err.name === "AbortError" ? "timeout" : err.message;
    console.warn(`[IFRAME] Resolution failed for "${title}" Ep${episodeNumber}: ${reason}`);
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. HEALTH + PING ROUTES
//
// /ping  → plain-text "OK" — use this URL in UptimeRobot (keyword monitor "OK")
//          UptimeRobot free tier pings every 5 min, which is enough to prevent
//          Render free-tier sleep (threshold is 15 min of inactivity).
//          Monitor type : HTTP(s) — Keyword  |  Keyword : OK
//
// /health → full JSON status for dashboards / your own monitoring.
//           Also pings MongoDB so a DB outage shows as "down" in UptimeRobot
//           if you point it here instead.
// ─────────────────────────────────────────────────────────────────────────────

/** Lightweight keep-alive endpoint — UptimeRobot keyword "OK" */
app.get("/ping", (_req, res) => {
  res.setHeader("Content-Type", "text/plain");
  res.status(200).send("OK");
});

/** Full status endpoint — checks DB + Telegram readiness */
app.get("/health", async (_req, res) => {
  let dbOk = false;
  try {
    // A cheap command that confirms the connection is actually alive
    await db.command({ ping: 1 });
    dbOk = true;
  } catch (_) { /* dbOk stays false */ }

  const allOk = dbOk; // tgReady can be false on cold start — don't fail health for it
  res.status(allOk ? 200 : 503).json({
    status:    allOk ? "ok" : "degraded",
    db:        dbOk   ? "connected" : "unreachable",
    telegram:  tgReady ? "ready"   : "connecting",
    uptime:    Math.floor(process.uptime()),
    memoryMB:  (process.memoryUsage().rss / 1024 / 1024).toFixed(1),
    timestamp: new Date().toISOString(),
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 6. ROUTE A — GET /api/catalog
// ─────────────────────────────────────────────────────────────────────────────

app.get("/api/catalog", async (_req, res) => {
  try {
    const items = await animeCollection
      .find(
        { total_episodes_indexed: { $gt: 0 } },
        {
          projection: {
            _id: 1, anime_id: 1, title: 1, cover_image: 1,
            total_episodes_indexed: 1, last_updated: 1,
          },
        }
      )
      .sort({ last_updated: -1 })
      .toArray();

    return res.status(200).json({ success: true, count: items.length, data: items });
  } catch (err) {
    console.error("[/api/catalog] Error:", err.message);
    return res.status(500).json({ success: false, error: "Internal server error fetching catalogue." });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 7. ROUTE B — GET /api/series/:slug
// ─────────────────────────────────────────────────────────────────────────────

app.get("/api/series/:slug", async (req, res) => {
  const { slug } = req.params;
  if (!slug || slug.trim() === "")
    return res.status(400).json({ success: false, error: "Invalid slug." });

  try {
    const series = await animeCollection.findOne({ anime_id: slug.trim().toLowerCase() });
    if (!series)
      return res.status(404).json({ success: false, error: `No series found: "${slug}".` });

    return res.status(200).json({ success: true, data: series });
  } catch (err) {
    console.error(`[/api/series/${slug}] Error:`, err.message);
    return res.status(500).json({ success: false, error: "Internal server error." });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 8. ROUTE C — GET /api/anime/:slug/watch — PRIORITY FALLBACK CHAIN
//
//  Priority 1: Magnet/Torrent  (Nyaa-scraped magnets or caption-provided magnet)
//  Priority 2: Telegram stream (GramJS partial-content via /stream/telegram/...)
//  Priority 3: Iframe API      (beat-anime-api-backup, prefer as-cdn21.top)
// ─────────────────────────────────────────────────────────────────────────────

app.get("/api/anime/:slug/watch", async (req, res) => {
  const { slug }    = req.params;
  const episodeNum  = parseInt(req.query.episode, 10);
  const preferMode  = req.query.prefer || "auto";  // auto | magnet | telegram | iframe

  if (!slug || isNaN(episodeNum)) {
    return res.status(400).json({
      success: false,
      error: "Missing slug or valid 'episode' query parameter.",
    });
  }

  try {
    const series = await animeCollection.findOne({ anime_id: slug.trim().toLowerCase() });
    if (!series)
      return res.status(404).json({ success: false, error: "Anime not found." });

    // FIX: use episode_number (was episode_number in server, episode_num in indexer — now unified)
    const ep = series.episodes?.find((e) => e.episode_number === episodeNum);

    if (!ep || !ep.sources || ep.sources.length === 0) {
      return res.status(404).json({
        success: false,
        error: `Episode ${episodeNum} is not indexed for "${slug}".`,
      });
    }

    const seasonNumber = ep.season_number || 1;

    // ── Gather all sources ─────────────────────────────────────────────────
    // Collect magnets from all sources (scraped Nyaa + caption-provided)
    const allMagnets = [];
    let   telegramSource = null;
    let   captionMagnet  = null;

    for (const src of ep.sources) {
      // Caption-provided magnet
      if (src.magnet && !captionMagnet) captionMagnet = src.magnet;

      // Nyaa-scraped magnets
      if (Array.isArray(src.magnets)) {
        for (const m of src.magnets) {
          if (m.magnet && !allMagnets.some((x) => x.magnet === m.magnet)) {
            allMagnets.push(m);
          }
        }
      }

      // Best Telegram source (prefer highest quality)
      // FIX: field names are now channel_id / message_id
      if (!telegramSource && src.channel_id && src.message_id) {
        telegramSource = src;
      }
    }

    // Sort scraped magnets: Judas first, then more seeders
    const PREFERRED_GROUPS = ["Judas", "NanDesuKa", "SubsPlease", "Erai-raws"];
    allMagnets.sort((a, b) => {
      const ai = PREFERRED_GROUPS.indexOf(a.group ?? "");
      const bi = PREFERRED_GROUPS.indexOf(b.group ?? "");
      const aScore = ai === -1 ? -1 : PREFERRED_GROUPS.length - ai;
      const bScore = bi === -1 ? -1 : PREFERRED_GROUPS.length - bi;
      return bScore - aScore || (b.seeders || 0) - (a.seeders || 0);
    });

    // Add caption magnet at top if it isn't already in the list
    if (captionMagnet && !allMagnets.some((m) => m.magnet === captionMagnet)) {
      allMagnets.unshift({ group: "User-Provided", magnet: captionMagnet, seeders: null, size: null });
    }

    // ── Priority 1: Magnet/Torrent ─────────────────────────────────────────
    if (preferMode !== "telegram" && preferMode !== "iframe" && allMagnets.length > 0) {
      return res.status(200).json({
        success:  true,
        method:   "magnet",
        anime_id: slug,
        episode:  episodeNum,
        season:   seasonNumber,
        magnets:  allMagnets,
        // Convenience: best single magnet for clients that only handle one
        best_magnet: allMagnets[0].magnet,
      });
    }

    // ── Priority 2: Telegram stream ────────────────────────────────────────
    if (preferMode !== "iframe" && telegramSource) {
      if (!tgReady) {
        // Telegram not ready yet — fall through to iframe if we can
        console.warn(`[WATCH] Telegram not ready. Falling through to iframe for "${slug}" Ep${episodeNum}.`);
      } else {
        console.log(`[WATCH] Routing to Telegram stream: ch=${telegramSource.channel_id} msg=${telegramSource.message_id}`);
        return res.redirect(`/stream/telegram/${telegramSource.channel_id}/${telegramSource.message_id}`);
      }
    }

    // ── Priority 3: Iframe API ─────────────────────────────────────────────
    console.log(`[WATCH] Resolving iframe stream for "${series.title}" Ep${episodeNum} S${seasonNumber}…`);
    const embeds = await resolveIframeStream(series.title, episodeNum, seasonNumber);

    if (embeds && embeds.length > 0) {
      return res.status(200).json({
        success:  true,
        method:   "iframe",
        anime_id: slug,
        episode:  episodeNum,
        season:   seasonNumber,
        embeds,
        primary_embed: embeds[0],
      });
    }

    // ── Total failure ──────────────────────────────────────────────────────
    return res.status(503).json({
      success: false,
      error:   "All streaming methods exhausted. No valid source available for this episode.",
      tried:   {
        magnet:   allMagnets.length > 0,
        telegram: !!telegramSource,
        iframe:   true,
      },
    });

  } catch (err) {
    console.error(`[/api/anime/${slug}/watch] Error:`, err.message);
    return res.status(500).json({ success: false, error: "Internal server error." });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 9. ROUTE D — GET /stream/telegram/:channelId/:messageId
//    CORE PARTIAL-CONTENT STREAMING ENGINE (GramJS)
// ─────────────────────────────────────────────────────────────────────────────

app.get("/stream/telegram/:channelId/:messageId", async (req, res) => {
  const { channelId, messageId } = req.params;

  // ── Telegram readiness guard ───────────────────────────────────────────────
  if (!tgReady || !tgClient) {
    res.setHeader("Retry-After", "10");
    return res.status(503).json({
      success: false,
      error: "Telegram client is still connecting. Retry in a few seconds.",
    });
  }

  if (!channelId || !messageId)
    return res.status(400).json({ success: false, error: "channelId and messageId are required." });

  const parsedMessageId = parseInt(messageId, 10);
  if (isNaN(parsedMessageId) || parsedMessageId <= 0)
    return res.status(400).json({ success: false, error: "messageId must be a positive integer." });

  const rangeHeader = req.headers["range"];
  if (!rangeHeader) {
    return res.status(400).json({
      success: false,
      error: "HTTP Range header required for video streaming (e.g. Range: bytes=0-).",
    });
  }

  // ── Normalise channel ID for MTProto ──────────────────────────────────────
  let normalizedChannelId;
  try {
    let rawId = channelId.toString().trim();
    if (rawId.startsWith("-100"))      rawId = rawId.slice(4);
    else if (rawId.startsWith("-"))    rawId = rawId.slice(1);
    normalizedChannelId = BigInt(rawId);
    if (normalizedChannelId <= 0n) throw new Error("Channel ID must resolve to a positive BigInt.");
  } catch (err) {
    return res.status(400).json({ success: false, error: `Invalid channelId: "${channelId}". ${err.message}` });
  }

  // ── Resolve Telegram message ───────────────────────────────────────────────
  let tgMessage;
  try {
    const peer    = new Api.PeerChannel({ channelId: normalizedChannelId });
    const messages = await tgClient.getMessages(peer, { ids: [parsedMessageId] });

    if (!messages || messages.length === 0 || !messages[0]) {
      return res.status(404).json({
        success: false,
        error: `No Telegram message found for ch=${channelId}, msg=${messageId}.`,
      });
    }

    tgMessage = messages[0];

    if (!tgMessage.document && !tgMessage.media?.document) {
      return res.status(422).json({
        success: false,
        error: `Message ${messageId} in channel ${channelId} contains no video document.`,
      });
    }
  } catch (err) {
    console.error(`[/stream] Failed to resolve message (ch=${channelId} msg=${messageId}):`, err.message);
    return res.status(502).json({ success: false, error: `Telegram metadata error: ${err.message}` });
  }

  const document      = tgMessage.document || tgMessage.media.document;
  const totalFileSize = Number(document.size);

  // ── Parse Range header ─────────────────────────────────────────────────────
  const MAX_CHUNK_BYTES = 5 * 1024 * 1024; // 5 MB cap — safe for Render 512 MB heap
  let startByte, endByte, chunkLength;

  const rangeMatch = rangeHeader.match(/^bytes=(\d+)-(\d*)$/);
  if (!rangeMatch) {
    res.setHeader("Content-Range", `bytes */${totalFileSize}`);
    return res.status(416).json({ success: false, error: `Malformed Range header: "${rangeHeader}"` });
  }

  startByte = parseInt(rangeMatch[1], 10);
  endByte   = rangeMatch[2] !== ""
    ? parseInt(rangeMatch[2], 10)
    : Math.min(startByte + MAX_CHUNK_BYTES - 1, totalFileSize - 1);
  endByte   = Math.min(endByte, totalFileSize - 1);

  if (startByte > endByte || startByte >= totalFileSize || startByte < 0) {
    res.setHeader("Content-Range", `bytes */${totalFileSize}`);
    return res.status(416).json({
      success: false,
      error: `Range ${startByte}-${endByte} not satisfiable (file size: ${totalFileSize}).`,
    });
  }

  chunkLength = endByte - startByte + 1;

  // ── MIME type ──────────────────────────────────────────────────────────────
  let mimeType = document.mimeType || "video/mp4";
  if (!mimeType.startsWith("video/") && !mimeType.startsWith("application/"))
    mimeType = "video/mp4";

  // ── Send 206 Partial Content headers ──────────────────────────────────────
  res.writeHead(206, {
    "Content-Range":  `bytes ${startByte}-${endByte}/${totalFileSize}`,
    "Accept-Ranges":  "bytes",
    "Content-Length": chunkLength,
    "Content-Type":   mimeType,
    "Cache-Control":  "no-store",
    "X-Content-Type-Options": "nosniff",
  });

  // ── Client-disconnect guard ────────────────────────────────────────────────
  let clientDisconnected = false;
  req.on("close", () => {
    clientDisconnected = true;
    if (!res.writableEnded) res.end();
  });

  // ── Stream via GramJS iterDownload ─────────────────────────────────────────
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
      if (clientDisconnected) break;

      const remaining = chunkLength - bytesWritten;
      if (remaining <= 0) break;

      const slice = chunk.length > remaining ? chunk.slice(0, remaining) : chunk;

      if (res.writableEnded || clientDisconnected) break;

      const canContinue = res.write(slice);
      bytesWritten += slice.length;

      if (!canContinue && !clientDisconnected) {
        await new Promise((resolve) => {
          res.once("drain", resolve);
          req.once("close", resolve);
        });
      }

      if (bytesWritten >= chunkLength) break;
    }
  } catch (err) {
    if (!clientDisconnected) {
      console.error(
        `[/stream] Error (ch=${channelId} msg=${messageId} range=${startByte}-${endByte}): ${err.message}`
      );
    }
  } finally {
    if (!res.writableEnded) res.end();
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 10. 404 CATCH-ALL
// ─────────────────────────────────────────────────────────────────────────────

app.use((req, res) => {
  res.status(404).json({ success: false, error: `Route not found: ${req.method} ${req.originalUrl}` });
});

// ─────────────────────────────────────────────────────────────────────────────
// 11. GLOBAL GUARDS
// ─────────────────────────────────────────────────────────────────────────────

process.on("uncaughtException",  (err)    => console.error("[UNCAUGHT]", err));
process.on("unhandledRejection", (reason) => console.error("[UNHANDLED]", reason));

// ─────────────────────────────────────────────────────────────────────────────
// 12. GRACEFUL SHUTDOWN
// ─────────────────────────────────────────────────────────────────────────────

async function gracefulShutdown(signal) {
  console.log(`[SHUTDOWN] ${signal} received. Closing connections…`);
  try {
    if (tgClient?.connected) await tgClient.disconnect();
  } catch (e) { console.error("[SHUTDOWN] TG disconnect error:", e.message); }
  try {
    if (mongoClient) await mongoClient.close();
  } catch (e) { console.error("[SHUTDOWN] Mongo close error:", e.message); }
  process.exit(0);
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT",  () => gracefulShutdown("SIGINT"));

// ─────────────────────────────────────────────────────────────────────────────
// 13. TIMEOUT HELPER
// ─────────────────────────────────────────────────────────────────────────────

function withTimeout(promise, ms, label) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`[TIMEOUT] ${label} exceeded ${ms}ms`)), ms);
    promise.then((v) => { clearTimeout(t); resolve(v); }, (e) => { clearTimeout(t); reject(e); });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// 14. BOOTSTRAP PIPELINE
// ─────────────────────────────────────────────────────────────────────────────

async function bootstrap() {
  // ── MongoDB ────────────────────────────────────────────────────────────────
  console.log("[BOOT] Connecting to MongoDB…");
  mongoClient = new MongoClient(MONGODB_URI, {
    serverApi: {
      version: ServerApiVersion.v1,
      strict: true,
      deprecationErrors: true,
    },
    maxPoolSize: 5,
    minPoolSize: 1,
    connectTimeoutMS: 10000,
    socketTimeoutMS: 45000,
  });

  await mongoClient.connect();
  db              = mongoClient.db(DB_NAME);
  animeCollection = db.collection(COLLECTION_NAME);
  console.log(`[BOOT] MongoDB connected → "${DB_NAME}"."${COLLECTION_NAME}"`);

  // ── Indexes (aligned with indexer.js schema) ───────────────────────────────
  await animeCollection.createIndex(
    { total_episodes_indexed: -1, last_updated: -1 },
    { name: "compound_catalog_sort", background: true }
  );
  await animeCollection.createIndex(
    { anime_id: 1 },
    { name: "unique_anime_id_slug", unique: true, background: true }
  );
  console.log("[BOOT] Indexes ensured.");

  // ── HTTP server — bind BEFORE Telegram so Render detects the port ──────────
  await new Promise((resolve) => {
    app.listen(PORT, () => {
      console.log(`[BOOT] HTTP server on port ${PORT}`);
      console.log(`[BOOT]   /ping    ← UptimeRobot keep-alive (keyword: OK)`);
      console.log(`[BOOT]   /health  ← full JSON status`);
      console.log(`[BOOT]   /api/catalog`);
      console.log(`[BOOT]   /api/series/:slug`);
      console.log(`[BOOT]   /api/anime/:slug/watch?episode=N[&prefer=magnet|telegram|iframe]`);
      console.log(`[BOOT]   /stream/telegram/:channelId/:messageId`);
      resolve();
    });
  });

  // ── GramJS in background ───────────────────────────────────────────────────
  console.log("[BOOT] Starting GramJS client (background)…");
  connectTelegramClient();
}

async function connectTelegramClient() {
  const CONNECT_TIMEOUT = 60000;
  const RETRY_DELAY     = 15000;

  while (true) {
    try {
      const gramLogger = new Logger("none");
      const client     = new TelegramClient(
        new StringSession(""),
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH,
        { connectionRetries: 5, retryDelay: 2000, autoReconnect: true, baseLogger: gramLogger }
      );

      await withTimeout(
        client.start({ botAuthToken: TELEGRAM_BOT_TOKEN }),
        CONNECT_TIMEOUT,
        "tgClient.start()"
      );

      tgClient = client;
      tgReady  = true;
      console.log("[TELEGRAM] ✅ MTProto session active.");
      return;

    } catch (err) {
      console.error(`[TELEGRAM] Connection failed: ${err.message}. Retrying in ${RETRY_DELAY / 1000}s…`);
      tgReady = false;
      await new Promise((r) => setTimeout(r, RETRY_DELAY));
    }
  }
}

bootstrap().catch((err) => {
  console.error("[FATAL] Bootstrap failed:", err);
  process.exit(1);
});
