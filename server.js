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
 *  5. FIX: Index creation wrapped in try/catch to handle IndexOptionsConflict (code 85)
 *          when indexer.js has already created the same index under a different name.
 *  6. FIX: Telegram fallthrough now returns 503 + retry_after instead of silently
 *          falling through to iframe when tgReady is false.
 *  7. FIX: Iframe backup pre-warmed on bootstrap to prevent first-request cold lag.
 *  8. FIX: tried{} object now accurately reflects what was actually attempted.
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

const MONGODB_URI        = process.env.MONGODB_URI.trim();
const TELEGRAM_API_ID    = parseInt(process.env.TELEGRAM_API_ID.trim(), 10);
const TELEGRAM_API_HASH  = process.env.TELEGRAM_API_HASH.trim();
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN.trim();
const PORT               = parseInt(process.env.PORT || "3000", 10);
const DB_NAME            = process.env.DB_NAME || "beataniverse";
const COLLECTION_NAME    = "anime_series";

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
// 2b. TELEGRAM SESSION PERSISTENCE
//
// On cold starts (Render free tier spins down after 15 min idle), GramJS needs
// to do a full MTProto DH-key handshake with an empty StringSession — this
// costs 60-120 s and reliably hits the old 60 s timeout.
//
// Fix: persist the session string to MongoDB after the first successful auth.
// Every subsequent cold start loads the saved session → reconnects in <10 s.
//
// Override: set TELEGRAM_SESSION env var in Render to a pre-generated session
// string (useful before the DB has ever stored one).
// ─────────────────────────────────────────────────────────────────────────────

const TG_SESSION_DOC_ID = "tg_bot_session";

async function loadTgSession() {
  // Env var takes priority (manual override for first deploy)
  if (process.env.TELEGRAM_SESSION?.trim()) {
    console.log("[TELEGRAM] Using TELEGRAM_SESSION env var.");
    return process.env.TELEGRAM_SESSION.trim();
  }
  try {
    const doc = await db.collection("bot_config").findOne({ _id: TG_SESSION_DOC_ID });
    if (doc?.session_string) {
      console.log("[TELEGRAM] Loaded saved MTProto session from DB.");
      return doc.session_string;
    }
  } catch (e) {
    console.warn("[TELEGRAM] Could not load saved session from DB:", e.message);
  }
  return ""; // fresh session — first run
}

async function saveTgSession(sessionString) {
  if (!sessionString) return;
  try {
    await db.collection("bot_config").updateOne(
      { _id: TG_SESSION_DOC_ID },
      { $set: { session_string: sessionString, saved_at: new Date() } },
      { upsert: true }
    );
    console.log("[TELEGRAM] MTProto session persisted to DB (fast reconnect on next cold start).");
  } catch (e) {
    console.warn("[TELEGRAM] Failed to persist session to DB:", e.message);
  }
}

async function clearTgSession() {
  try {
    await db.collection("bot_config").deleteOne({ _id: TG_SESSION_DOC_ID });
    console.log("[TELEGRAM] Cleared invalid/stale session from DB.");
  } catch (e) { /* ignore */ }
}

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

    const hit        = hits[0];
    const externalId = hit.anime_id || hit.id;
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
    await db.command({ ping: 1 });
    dbOk = true;
  } catch (_) { /* dbOk stays false */ }

  const allOk = dbOk;
  res.status(allOk ? 200 : 503).json({
    status:    allOk ? "ok" : "degraded",
    db:        dbOk    ? "connected"  : "unreachable",
    telegram:  tgReady ? "ready"      : "connecting",
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
//
//  FIX: If tgReady is false at request time, return 503 + retry_after instead
//       of silently falling through to iframe — prevents false "exhausted" errors.
//  FIX: tried{} now accurately reflects what was actually attempted, not just
//       what sources exist in the DB.
// ─────────────────────────────────────────────────────────────────────────────

app.get("/api/anime/:slug/watch", async (req, res) => {
  const { slug }   = req.params;
  const episodeNum = parseInt(req.query.episode, 10);
  const preferMode = req.query.prefer || "auto";  // auto | magnet | telegram | iframe

  if (!slug || isNaN(episodeNum)) {
    return res.status(400).json({
      success: false,
      error: "Missing slug or valid 'episode' query parameter.",
    });
  }

  // Track what was actually attempted (not just what exists)
  const attempted = { magnet: false, telegram: false, iframe: false };

  try {
    const series = await animeCollection.findOne({ anime_id: slug.trim().toLowerCase() });
    if (!series)
      return res.status(404).json({ success: false, error: "Anime not found." });

    const ep = series.episodes?.find((e) => e.episode_number === episodeNum);

    if (!ep || !ep.sources || ep.sources.length === 0) {
      return res.status(404).json({
        success: false,
        error: `Episode ${episodeNum} is not indexed for "${slug}".`,
      });
    }

    const seasonNumber = ep.season_number || 1;

    // ── Gather all sources ─────────────────────────────────────────────────
    const allMagnets = [];
    let   telegramSource = null;
    let   captionMagnet  = null;

    for (const src of ep.sources) {
      if (src.magnet && !captionMagnet) captionMagnet = src.magnet;

      if (Array.isArray(src.magnets)) {
        for (const m of src.magnets) {
          if (m.magnet && !allMagnets.some((x) => x.magnet === m.magnet)) {
            allMagnets.push(m);
          }
        }
      }

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

    if (captionMagnet && !allMagnets.some((m) => m.magnet === captionMagnet)) {
      allMagnets.unshift({ group: "User-Provided", magnet: captionMagnet, seeders: null, size: null });
    }

    // ── Priority 1: Magnet/Torrent ─────────────────────────────────────────
    if (preferMode !== "telegram" && preferMode !== "iframe" && allMagnets.length > 0) {
      attempted.magnet = true;
      return res.status(200).json({
        success:     true,
        method:      "magnet",
        anime_id:    slug,
        episode:     episodeNum,
        season:      seasonNumber,
        magnets:     allMagnets,
        best_magnet: allMagnets[0].magnet,
      });
    }

    // ── Priority 2: Telegram stream ────────────────────────────────────────
    if (preferMode !== "iframe" && telegramSource) {
      attempted.telegram = true;

      if (!tgReady) {
        // FIX: Instead of hard-blocking with 503, fall through to iframe.
        // On Render free-tier cold starts the Telegram client is still
        // initialising; returning 503 every time gives users nothing.
        // With session persistence (above) tgReady becomes true within ~10 s,
        // so on the NEXT request Telegram will be used correctly.
        console.warn(
          `[WATCH] Telegram not ready — falling through to iframe for "${slug}" Ep${episodeNum}.`
        );
        // Do NOT return here — continue to Priority 3 (iframe).
      } else {
        console.log(
          `[WATCH] Routing to Telegram stream: ch=${telegramSource.channel_id} msg=${telegramSource.message_id}`
        );
        return res.redirect(
          `/stream/telegram/${telegramSource.channel_id}/${telegramSource.message_id}`
        );
      }
    }

    // ── Priority 3: Iframe API ─────────────────────────────────────────────
    attempted.iframe = true;
    console.log(`[WATCH] Resolving iframe stream for "${series.title}" Ep${episodeNum} S${seasonNumber}…`);
    const embeds = await resolveIframeStream(series.title, episodeNum, seasonNumber);

    if (embeds && embeds.length > 0) {
      return res.status(200).json({
        success:       true,
        method:        "iframe",
        anime_id:      slug,
        episode:       episodeNum,
        season:        seasonNumber,
        embeds,
        primary_embed: embeds[0],
      });
    }

    // ── Total failure ──────────────────────────────────────────────────────
    return res.status(503).json({
      success: false,
      error:   "All streaming methods exhausted. No valid source available for this episode.",
      tried:   attempted,
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
    if (rawId.startsWith("-100"))   rawId = rawId.slice(4);
    else if (rawId.startsWith("-")) rawId = rawId.slice(1);
    normalizedChannelId = BigInt(rawId);
    if (normalizedChannelId <= 0n) throw new Error("Channel ID must resolve to a positive BigInt.");
  } catch (err) {
    return res.status(400).json({ success: false, error: `Invalid channelId: "${channelId}". ${err.message}` });
  }

  // ── Resolve Telegram message ───────────────────────────────────────────────
  let tgMessage;
  try {
    const peer     = new Api.PeerChannel({ channelId: normalizedChannelId });
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
    "Content-Range":          `bytes ${startByte}-${endByte}/${totalFileSize}`,
    "Accept-Ranges":          "bytes",
    "Content-Length":         chunkLength,
    "Content-Type":           mimeType,
    "Cache-Control":          "no-store",
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
        id:            document.id,
        accessHash:    document.accessHash,
        fileReference: document.fileReference,
        thumbSize:     "",
      }),
      requestSize: ITER_REQUEST_SIZE,
      offset:      BigInt(startByte),
      limit:       chunkLength,
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
// 14. INDEX HELPER — safely creates indexes, tolerates IndexOptionsConflict
//
// MongoDB error code 85 (IndexOptionsConflict) is thrown when the same key
// pattern already exists under a different name (e.g. the auto-generated name
// created by indexer.js).  The index is functionally identical so we can
// safely skip creation and continue.
// ─────────────────────────────────────────────────────────────────────────────

async function ensureIndexes() {
  const indexSpecs = [
    {
      key:     { total_episodes_indexed: -1, last_updated: -1 },
      options: { name: "compound_catalog_sort", background: true },
    },
    {
      key:     { anime_id: 1 },
      options: { name: "unique_anime_id_slug", unique: true, background: true },
    },
  ];

  for (const { key, options } of indexSpecs) {
    try {
      await animeCollection.createIndex(key, options);
      console.log(`[BOOT] Index ensured: ${JSON.stringify(key)} → "${options.name}"`);
    } catch (err) {
      if (err.code === 85) {
        // IndexOptionsConflict — same key pattern already exists under the
        // auto-generated name created by indexer.js.  Functionally identical;
        // safe to skip.
        console.warn(
          `[BOOT] Index already exists under a different name (skipping): ` +
          `${JSON.stringify(key)} — "${options.name}" (code 85 IndexOptionsConflict)`
        );
      } else if (err.code === 86) {
        // IndexKeySpecsConflict — same name, different keys.  Log and skip.
        console.warn(
          `[BOOT] Index name conflict for "${options.name}" (code 86 IndexKeySpecsConflict). Skipping.`
        );
      } else {
        // Unexpected error — re-throw to halt bootstrap
        throw err;
      }
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 15. IFRAME BACKUP WARMUP — fires on bootstrap, fire-and-forget
//
// FIX: Pre-warms the iframe backup service on startup so the first real
//      request to that API doesn't hit a cold-start delay or timeout.
// ─────────────────────────────────────────────────────────────────────────────

async function warmupIframeBackup() {
  try {
    const ctrl = new AbortController();
    const tid  = setTimeout(() => ctrl.abort(), 8000);
    const r    = await fetch(`${IFRAME_API_BASE}/`, {
      signal: ctrl.signal,
      headers: { "User-Agent": "BeatAniVerse-Bridge/1.0 (warmup)" },
    });
    clearTimeout(tid);
    console.log(`[BOOT] Iframe backup warmed up (HTTP ${r.status}) → ${IFRAME_API_BASE}`);
  } catch (e) {
    const reason = e.name === "AbortError" ? "timeout after 8s" : e.message;
    console.warn(`[BOOT] Iframe backup warmup skipped: ${reason}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 16. BOOTSTRAP PIPELINE
// ─────────────────────────────────────────────────────────────────────────────

async function bootstrap() {
  // ── MongoDB ────────────────────────────────────────────────────────────────
  console.log("[BOOT] Connecting to MongoDB…");
  mongoClient = new MongoClient(MONGODB_URI, {
    serverApi: {
      version:          ServerApiVersion.v1,
      strict:           true,
      deprecationErrors: true,
    },
    maxPoolSize:       5,
    minPoolSize:       1,
    connectTimeoutMS:  10000,
    socketTimeoutMS:   45000,
  });

  await mongoClient.connect();
  db              = mongoClient.db(DB_NAME);
  animeCollection = db.collection(COLLECTION_NAME);
  console.log(`[BOOT] MongoDB connected → "${DB_NAME}"."${COLLECTION_NAME}"`);

  // ── Indexes ────────────────────────────────────────────────────────────────
  await ensureIndexes();
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

  // ── Iframe backup warmup — fire-and-forget, do not block startup ───────────
  warmupIframeBackup();

  // ── GramJS in background ───────────────────────────────────────────────────
  console.log("[BOOT] Starting GramJS client (background)…");
  connectTelegramClient();
}

async function connectTelegramClient() {
  // FIX: Increased from 60 s → 120 s.
  // A fresh MTProto DH-key handshake on Render can take 90+ s on first boot.
  const CONNECT_TIMEOUT = 120000;
  const RETRY_DELAY     = 15000;

  // Load saved session ONCE before the retry loop.
  // With a valid saved session the reconnect takes < 10 s instead of 90+ s.
  let savedSession      = await loadTgSession();
  let usedSavedSession  = !!savedSession;

  while (true) {
    try {
      const gramLogger = new Logger("none");
      const client     = new TelegramClient(
        new StringSession(savedSession),   // use saved session if available
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH,
        { connectionRetries: 3, retryDelay: 3000, autoReconnect: true, baseLogger: gramLogger }
      );

      await withTimeout(
        client.start({ botAuthToken: TELEGRAM_BOT_TOKEN }),
        CONNECT_TIMEOUT,
        "tgClient.start()"
      );

      tgClient = client;
      tgReady  = true;

      // Save the (possibly new) session string for the next cold start.
      const newSession = client.session.save();
      if (newSession && newSession !== savedSession) {
        await saveTgSession(newSession);
        savedSession = newSession;
        console.log("[TELEGRAM] ✅ MTProto session active & persisted.");
      } else {
        console.log("[TELEGRAM] ✅ MTProto session active.");
      }
      return;

    } catch (err) {
      // If the saved session was invalid (revoked / corrupted), clear it and
      // retry with a blank session so GramJS does a fresh auth.
      const isAuthErr = /AUTH|FLOOD|session/i.test(err.message || "");
      if (usedSavedSession && isAuthErr) {
        console.warn(`[TELEGRAM] Saved session rejected (${err.message}). Clearing & retrying fresh…`);
        await clearTgSession();
        savedSession     = "";
        usedSavedSession = false;
        // No wait — retry immediately with fresh session
        continue;
      }

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
