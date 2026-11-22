import express from "express";
import dotenv from "dotenv";
import mongoose from "mongoose";
import http from "http";
import crypto from "crypto";
import { Server } from "socket.io";
import { jwtVerify } from "jose";
import User from "./models/User.model.js";
import Booking from "./models/Booking.model.js";
import { startScheduler } from "./scheduler.js";

dotenv.config();
const port = process.env.PORT || 8000;
const MAX_ACCURACY_M = Number(process.env.MAX_ACCURACY_M) || 2000;
const ARRIVAL_RADIUS_M = Number(process.env.ARRIVAL_RADIUS_M) || 150;
const ARRIVAL_MAX_ACC_M = Number(process.env.ARRIVAL_MAX_ACC_M) || 100;
const JOB_STATUSES = ["confirmed", "arrived", "started", "work_completed"]; // superset of old TRACKABLE
const LABOUR_ROLE = process.env.LABOUR_ROLE || "laber"; // confirm this matches your User model role
const DEBUG = process.env.SOCKET_DEBUG !== "false"; // set SOCKET_DEBUG=false to silence
const log = (...a) => DEBUG && console.log(...a);

if (!process.env.SOCKET_JWT_SECRET || !process.env.SOCKET_EMIT_SECRET) {
  console.error("SOCKET_JWT_SECRET and SOCKET_EMIT_SECRET are required");
  process.exit(1);
}
const JWT_KEY = new TextEncoder().encode(process.env.SOCKET_JWT_SECRET);

const app = express();
app.use(express.json({ limit: "50kb" }));
const server = http.createServer(app);

const origins = (process.env.NEXT_BASE_URL || "")
  .split(",")
  .map((s) => s.trim().replace(/^"|"$/g, "").replace(/\/+$/, ""))
  .filter(Boolean);
log("allowed origins:", origins);

const io = new Server(server, { cors: { origin: origins, credentials: true } });

const secretOk = (given) => {
  const a = Buffer.from(String(given || ""));
  const b = Buffer.from(process.env.SOCKET_EMIT_SECRET);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
};

// health check for the hosting platform (Render / Railway) — also shows the cron scheduler's last runs
let scheduler = null;
app.get("/", (_req, res) => res.json({ ok: true, db: mongoose.connection.name || null, scheduler: scheduler?.status() || { enabled: false } }));

// only the Next.js backend (knows the secret) can push events
app.post("/emit", (req, res) => {
  if (!secretOk(req.headers["x-emit-secret"])) {
    log("emit REJECTED: bad or missing x-emit-secret");
    return res.status(401).json({ success: false });
  }
  const { event, userId, room, data } = req.body || {};
  if (!event) return res.status(400).json({ success: false });
  if (data?.bookingId) for (const [k, v] of activeJobCache) if (v.bookingId === String(data.bookingId) || !v.bookingId) activeJobCache.delete(k);
  if (event === "accept-booking" || event === "booking-update") activeJobCache.clear();
  if (room === "admins") io.to("admins").emit(event, data);
  else if (userId) io.to(`user-${userId}`).emit(event, data);
  else return res.status(400).json({ success: false });
  log("emit:", event, "->", room || `user-${userId}`);
  return res.json({ success: true });
});

const toRad = (d) => (d * Math.PI) / 180;
const distM = (aLat, aLon, bLat, bLon) => {
  const x = Math.sin(toRad(bLat - aLat) / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(toRad(bLon - aLon) / 2) ** 2;
  return 6371000 * 2 * Math.atan2(Math.sqrt(x), Math.sqrt(1 - x));
};
const validCoord = (lat, lon) =>
  Number.isFinite(lat) && Number.isFinite(lon) && Math.abs(lat) <= 90 && Math.abs(lon) <= 180;

// identity comes from the signed token, never from the client
io.use(async (socket, next) => {
  const token = String(socket.handshake.auth?.token || "");
  if (!token) {
    log("auth REJECTED: empty token (client could not get /api/socket/token; see the [socket-token] line in the Next console)");
    return next(new Error("unauthorized"));
  }
  try {
    const { payload } = await jwtVerify(token, JWT_KEY);
    if (!mongoose.isValidObjectId(payload.uid)) {
      log("auth REJECTED: token uid is not a valid ObjectId ->", payload.uid);
      return next(new Error("unauthorized"));
    }
    socket.userId = String(payload.uid);
    socket.role = String(payload.role);
    return next();
  } catch (e) {
    log("auth REJECTED: token invalid ->", e.code || e.message);
    return next(new Error("unauthorized"));
  }
});

const adminsOnline = () => (io.sockets.adapter.rooms.get("admins")?.size || 0) > 0;

const lastSaved = new Map(); // userId -> { t, lat, lon }  (DB write throttle)
const activeJobCache = new Map(); // labourUserId -> { t, bookingId|null }
const lastRoomEmit = new Map();   // labourUserId -> t

// the labour's running live job (cached 20s) so GPS from ANY page reaches the customer's map
async function activeJobOf(userId) {
  const c = activeJobCache.get(userId);
  if (c && Date.now() - c.t < 20000) return c.bookingId;
  const b = await Booking.findOne({ labour: userId, bookingStatus: { $in: ["confirmed", "arrived", "started"] } })
    .select("_id").lean();
  const bookingId = b ? String(b._id) : null;
  activeJobCache.set(userId, { t: Date.now(), bookingId });
  return bookingId;
}

async function saveLocation(userId, lat, lon, minMs = 15000, minM = 30) {
  const now = Date.now();
  const prev = lastSaved.get(userId);
  if (prev && now - prev.t < minMs && distM(prev.lat, prev.lon, lat, lon) < minM) return false;
  lastSaved.set(userId, { t: now, lat, lon });
  await User.updateOne(
    { _id: userId },
    { $set: { location: { type: "Point", coordinates: [lon, lat] }, locationUpdatedAt: new Date(), isOnline: true } }
  );
  return true;
}

io.on("connection", (socket) => {
  socket.data.jobs = new Map(); // bookingId -> { role, site:[lon,lat], status, near, lastEmit }
  socket.join(`user-${socket.userId}`);
  if (socket.role === "admin") socket.join("admins");
  log("socket connected:", socket.id, "role:", socket.role, "user:", socket.userId.slice(-4),
    "origin:", socket.handshake.headers.origin);

  // support chat presence: admins see who is online, users see if support is online
  if (socket.role === "admin") io.emit("support:admin-presence", { online: true });
  else {
    io.to("admins").emit("support:presence", { userId: socket.userId, online: true });
    socket.emit("support:admin-presence", { online: adminsOnline() });
  }

  // ---- ALL handlers are registered BEFORE any await, so early events are never lost ----

  // old clients still emit this; identity is now taken only from the signed token
  socket.on("identity", (claimedId) => {
    if (claimedId && String(claimedId) !== socket.userId)
      log("identity IGNORED: claimed id differs from token id", String(claimedId).slice(-4), socket.userId.slice(-4));
    else log("identity ok:", socket.userId.slice(-4));
  });

  socket.on("update-location", async ({ latitude, longitude, accuracy, heading, speed } = {}) => {
    try {
      if (socket.role !== LABOUR_ROLE)
        return log(`location DROPPED: role "${socket.role}" is not "${LABOUR_ROLE}" (set LABOUR_ROLE in socket .env)`);
      const lat = Number(latitude), lon = Number(longitude);
      if (!validCoord(lat, lon)) return log("location DROPPED: invalid coords", latitude, longitude);
      if (Number(accuracy) > MAX_ACCURACY_M)
        return log(`location DROPPED: accuracy ${accuracy}m > limit ${MAX_ACCURACY_M}m (set MAX_ACCURACY_M in .env)`);
      // live job running? -> customer + admin map get this fix too (max 1 per 2s)
      const jobId = await activeJobOf(socket.userId);
      if (jobId && Date.now() - (lastRoomEmit.get(socket.userId) || 0) > 2000) {
        lastRoomEmit.set(socket.userId, Date.now());
        io.to(`job-${jobId}`).emit("labour-location", {
          latitude: lat, longitude: lon, accuracy: Number(accuracy) || null,
          heading: Number.isFinite(Number(heading)) ? Number(heading) : null,
          speed: Number.isFinite(Number(speed)) ? Number(speed) : null, at: Date.now(),
        });
      }
      if (await saveLocation(socket.userId, lat, lon, jobId ? 8000 : 15000, jobId ? 15 : 30))
        log("location saved:", socket.userId.slice(-4), lat, lon, "acc:", accuracy);
    } catch (e) { console.error("update-location error:", e.message); }
  });

  // only the booking's own customer / labour may join its room
  socket.on("join-job", async (bookingId) => {
    try {
      if (!mongoose.isValidObjectId(bookingId)) return;
      const b = await Booking.findById(bookingId).select("user labour bookingStatus workLocation").lean();
      if (!b || !JOB_STATUSES.includes(b.bookingStatus)) return log("join-job DROPPED: booking missing or status not trackable", String(bookingId));
      const role = String(b.labour) === socket.userId ? "labour"
        : String(b.user) === socket.userId ? "customer"
        : socket.role === "admin" ? "admin" : null;
      if (!role) return log("join-job DROPPED: user is neither labour nor customer of", String(bookingId));
      socket.data.jobs.set(String(bookingId), { role, site: b.workLocation?.coordinates, status: b.bookingStatus, near: 0, lastEmit: 0 });
      socket.join(`job-${bookingId}`);
      log("join-job ok:", role, String(bookingId).slice(-4));
    } catch (e) { console.error("join-job error:", e.message); }
  });

  socket.on("labour-location-update", async ({ bookingId, latitude, longitude, accuracy, status, heading, speed } = {}) => {
    try {
      const job = socket.data.jobs.get(String(bookingId));
      if (!job || job.role !== "labour") return;
      const lat = Number(latitude), lon = Number(longitude);
      if (!validCoord(lat, lon)) return;
      const now = Date.now();
      if (now - job.lastEmit < 1000) return;
      job.lastEmit = now;

      lastRoomEmit.set(socket.userId, now);
      io.to(`job-${bookingId}`).emit("labour-location", {
        latitude: lat, longitude: lon, accuracy: Number(accuracy) || null, status, at: now,
        heading: Number.isFinite(Number(heading)) ? Number(heading) : null,
        speed: Number.isFinite(Number(speed)) ? Number(speed) : null,
      });
      // keep the DB fresh too: the "I've reached" check and the admin map read it
      saveLocation(socket.userId, lat, lon, 8000, 15).catch((e) => console.error("save loc:", e.message));

      // automatic arrival: twice in a row, close to the site, with a good GPS fix
      if (job.status === "confirmed" && job.site && Number(accuracy) <= ARRIVAL_MAX_ACC_M) {
        job.near = distM(lat, lon, job.site[1], job.site[0]) <= ARRIVAL_RADIUS_M ? job.near + 1 : 0;
        if (job.near >= 2) {
          job.status = "checking";
          try {
            const at = new Date();
            const b = await Booking.findOneAndUpdate(
              { _id: bookingId, labour: socket.userId, bookingStatus: "confirmed" },
              { $set: { bookingStatus: "arrived", arrivedAt: at }, $push: { history: { status: "arrived", at, byRole: "system", note: "geofence" } } },
              { returnDocument: "after" }
            ).select("user").lean();
            job.status = b ? "arrived" : "done";
            if (b) {
              io.to(`job-${bookingId}`).emit("booking-update", { bookingId, status: "arrived" });
              io.to(`user-${b.user}`).emit("booking-update", { bookingId, status: "arrived" });
              io.to("admins").emit("admin-booking-update", { bookingId, status: "arrived" });
            }
          } catch (e) {
            job.status = "confirmed"; // DB failed: allow another attempt instead of getting stuck on "checking"
            job.near = 0;
            throw e;
          }
        }
      }
    } catch (e) { console.error("labour-location-update error:", e.message); }
  });

  // ---- support chat (website bubble <-> /admin/chat). Messages are saved by Next API routes and
  //      pushed through /emit; the socket only carries typing + presence (nothing is stored here). ----
  socket.on("support:typing", ({ to, typing } = {}) => {
    const t = !!typing;
    if (socket.role === "admin") {
      if (mongoose.isValidObjectId(to)) io.to(`user-${to}`).emit("support:typing", { from: "admin", typing: t });
    } else {
      io.to("admins").emit("support:typing", { userId: socket.userId, typing: t });
    }
  });

  // admin asks which users are connected right now
  socket.on("support:presence-list", (ack) => {
    if (socket.role !== "admin" || typeof ack !== "function") return;
    const ids = [];
    for (const [name, members] of io.sockets.adapter.rooms) {
      if (name.startsWith("user-") && members.size > 0) ids.push(name.slice(5));
    }
    ack(ids);
  });

  // old chat relay (booking chat now mainly goes through /api/chat/send)
  socket.on("chat-message", (data) => {
    if (!data?.bookingId || !socket.data.jobs.has(String(data.bookingId))) return;
    io.to(`job-${data.bookingId}`).emit("chat-message", { ...data, fromUserId: socket.userId });
  });

  socket.on("disconnect", async (reason) => {
    try {
      log("socket disconnected:", socket.id, reason, "user", socket.userId.slice(-4));
      if (socket.role === "admin") io.emit("support:admin-presence", { online: adminsOnline() });
      const room = io.sockets.adapter.rooms.get(`user-${socket.userId}`);
      if (room && room.size > 0) return; // another tab/device still connected
      if (socket.role !== "admin") io.to("admins").emit("support:presence", { userId: socket.userId, online: false });
      lastSaved.delete(socket.userId);
      await User.updateOne({ _id: socket.userId }, { $set: { socketId: null, isOnline: false } });
    } catch (e) { console.error("disconnect error:", e.message); }
  });

  // ---- mark online (like the old "identity" did, for every role); not awaited, handlers are already live ----
  User.findByIdAndUpdate(
    socket.userId,
    { $set: { socketId: socket.id, isOnline: true } },
    { returnDocument: "after" }
  ).select("role name").lean()
    .then((u) => {
      if (!u) return log("online: NO USER FOUND in this database for id", socket.userId,
        "(socket server may be using a different database than Next)");
      log("online saved:", u.role, u.name);
    })
    .catch((e) => console.error("online update error:", e.message));
});

// MUST be the same DB as the Next app (lib/databaseConnection.js uses "Sport-Ecommerce").
// The URI's own db ("labourapp") has no users, so online/GPS updates were silently lost.
mongoose
  .connect(process.env.MONGODB_URI, { dbName: process.env.MONGODB_DB || "Sport-Ecommerce" })
  .then(() => {
    console.log("db connected:", mongoose.connection.name);
    server.listen(port, () => console.log("server started on", port));
    scheduler = startScheduler({ log: console.log });
  })
  .catch((e) => { console.error("db error", e); process.exit(1); });






