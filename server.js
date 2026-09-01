// MOON -- real-time order chat server.
//
// Replaces WhatsApp/Telegram entirely: when a customer places an order,
// the site creates a session here instead of building a wa.me/t.me deep
// link, then both the customer (on the site) and the admin (on the site
// or the Android app) join the same Socket.io room and exchange
// messages instantly, no page refresh needed.
//
// Deliberately in-memory, no database, no PDF export -- this was scoped
// down on purpose (see project notes) to avoid the operational overhead
// of Firestore for something this size. A restart clears active
// sessions; that's an accepted tradeoff for the simplicity.
//
// Deploy: same place bot2.py already runs (Render.com). This is a
// separate Node service from that Python bot -- they don't share a
// process, only (optionally) the same Render account.

const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');

// ---------------- Push notifications (FCM) ----------------
// Optional and self-disabling: if FIREBASE_SERVICE_ACCOUNT_JSON isn't set
// as an environment variable yet, push notifications are simply skipped
// (a warning is logged once) rather than crashing the server. This lets
// the chat itself work immediately, with push added once Firebase is set
// up, without a redeploy-breaking dependency in between.
let admin = null;
let fcmEnabled = false;
try {
  admin = require('firebase-admin');
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (raw) {
    const serviceAccount = JSON.parse(raw);
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    fcmEnabled = true;
    console.log('[MOON] FCM push notifications enabled.');
  } else {
    console.log('[MOON] FIREBASE_SERVICE_ACCOUNT_JSON not set -- push notifications disabled, chat still works normally.');
  }
} catch (e) {
  console.log('[MOON] firebase-admin not usable yet -- push notifications disabled, chat still works normally.', e.message);
}

// Admin devices that should receive a push when a customer messages --
// registered via POST /api/admin/fcm-token (the Android app calls this
// once it has a token). A Set so the same device re-registering doesn't
// create duplicates.
//
// Unlike active chat sessions (see the file-header comment -- those are
// deliberately in-memory-only), this Set is mirrored to MantleDB, the
// same simple external key-value store the site and the Android app
// already use for other saved data. Device registrations aren't
// transactional the way a chat session is -- there's no reasonable
// sense in which "the server happened to restart" should make an
// admin's phone stop receiving pushes until they think to reopen the
// Admin tab. Render's free/hobby tier spins the service down after a
// period of inactivity and spins a fresh instance back up on the next
// request, wiping any plain in-memory Set -- which is exactly what
// produced the "push arrives from the 2nd message but never the 1st"
// pattern: the very request that wakes the server (a brand new order)
// finds zero registered devices, because nothing yet had a chance to
// re-register on this fresh instance.
const MANTLEDB_BASE = 'https://mantledb.sh/v2';
const MANTLEDB_NAMESPACE = 'moonge-tbilisi-vc7f3q';
const FCM_TOKENS_PATH = 'chat-server-fcm-admin-tokens';
const adminFcmTokens = new Set();

async function loadPersistedFcmTokens() {
  try {
    const resp = await fetch(`${MANTLEDB_BASE}/${MANTLEDB_NAMESPACE}/${FCM_TOKENS_PATH}`);
    if (resp.status === 404) {
      console.log('[MOON] No persisted FCM tokens found yet (first run, or none ever registered).');
      return;
    }
    if (!resp.ok) {
      console.log(`[MOON] Could not load persisted FCM tokens (HTTP ${resp.status}) -- starting with an empty set; devices will need to re-register.`);
      return;
    }
    const data = await resp.json();
    const list = Array.isArray(data.list) ? data.list : [];
    list.forEach(t => adminFcmTokens.add(t));
    console.log(`[MOON] Restored ${list.length} FCM token(s) from persistent storage -- survives this restart, no re-registration needed.`);
  } catch (e) {
    console.log('[MOON] Error loading persisted FCM tokens -- starting with an empty set:', e.message);
  }
}

async function persistFcmTokens() {
  try {
    await fetch(`${MANTLEDB_BASE}/${MANTLEDB_NAMESPACE}/${FCM_TOKENS_PATH}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ list: Array.from(adminFcmTokens) })
    });
  } catch (e) {
    console.log('[MOON] Error persisting FCM tokens (registration still works for this session, just won\'t survive a restart):', e.message);
  }
}
// Kicked off later, awaited immediately before the server starts
// accepting connections (see the bottom of this file) -- this used to
// fire immediately here instead, which left a real (if short) window
// where an incoming request could still find zero tokens if it arrived
// before this finished. Not awaiting it before the server opens for
// requests defeats a good chunk of the point of persisting these at all.

async function notifyAdminsOfNewMessage(orderId, customerName, text) {
  if (!fcmEnabled) {
    console.log(`[MOON] Skipping push for order ${orderId} -- FCM not enabled (no FIREBASE_SERVICE_ACCOUNT_JSON).`);
    return;
  }
  if (adminFcmTokens.size === 0) {
    console.log(`[MOON] Skipping push for order ${orderId} -- FCM is enabled but zero admin devices are registered. The Android app must open the admin screen at least once (which calls POST /api/admin/fcm-token) before this can ever succeed.`);
    return;
  }
  const tokens = Array.from(adminFcmTokens);
  console.log(`[MOON] Sending push for order ${orderId} to ${tokens.length} device(s)...`);
  try {
    const resp = await admin.messaging().sendEachForMulticast({
      tokens,
      notification: {
        title: customerName ? `${customerName} -- შეკვეთა #${orderId}` : `შეკვეთა #${orderId}`,
        body: text.slice(0, 200)
      },
      data: { orderId },
      // Without this, FCM's default Android priority is "normal" --
      // several OEM skins (Xiaomi/MIUI, Samsung, Huawei among them)
      // will delay or drop a normal-priority push entirely once the
      // device is in Doze / the app is backgrounded, which is exactly
      // the "notification never arrives" symptom this addresses.
      // channelId matches MoonFirebaseMessagingService's channel so a
      // notification delivered while the app is fully killed (which
      // bypasses onMessageReceived and is drawn by the OS directly from
      // this payload) still lands in the same channel as one shown from
      // the foreground path, instead of a default/fallback channel.
      android: {
        priority: 'high',
        notification: { channelId: 'moon_order_chat' }
      }
    });
    const successCount = resp.responses.filter(r => r.success).length;
    console.log(`[MOON] Push for order ${orderId}: ${successCount}/${tokens.length} delivered to FCM successfully.`);
    resp.responses.forEach((r, i) => {
      if (!r.success) {
        console.log(`[MOON]   device ...${tokens[i].slice(-12)} failed: ${r.error?.code || r.error?.message}`);
      }
    });
    // Prune tokens the device itself has invalidated (uninstalled app,
    // token rotated, etc.) so the set doesn't grow with dead entries.
    let pruned = false;
    resp.responses.forEach((r, i) => {
      if (!r.success && (r.error?.code === 'messaging/registration-token-not-registered')) {
        adminFcmTokens.delete(tokens[i]);
        pruned = true;
      }
    });
    if (pruned) persistFcmTokens();
  } catch (e) {
    console.log('[MOON] FCM send failed (chat itself is unaffected):', e.message);
  }
}

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// orderId -> { customer, order, messages: [...], status: 'active'|'completed', createdAt }
const sessions = new Map();

function publicSession(orderId, session) {
  return {
    orderId,
    customer: session.customer,
    order: session.order,
    messages: session.messages,
    status: session.status,
    createdAt: session.createdAt
  };
}

// ---------------- REST ----------------

// Called by the site the moment a customer places an order (any of the
// site's 5 order types -- main download, weekly/monthly subscription,
// paid edit, cheap package). This is the direct replacement for
// building a wa.me/t.me link.
app.post('/api/sessions', (req, res) => {
  const { orderId, customer, order, firstMessage } = req.body;
  if (!orderId || !customer) {
    return res.status(400).json({ error: 'orderId and customer are required' });
  }
  const session = {
    customer, // { discordId, username, avatarUrl }
    order,    // { type, tiktok, price, details, ... } -- whatever the site already built for the old WA/TG message text
    messages: firstMessage ? [firstMessage] : [],
    status: 'active',
    createdAt: Date.now()
  };
  sessions.set(orderId, session);
  io.to('admin_room').emit('new_session', publicSession(orderId, session));
  // Without this, the FIRST message of a new order (this one, embedded
  // in session creation) never triggered a push at all -- only messages
  // sent afterward via the send_message socket handler did. That made it
  // look like push "only works starting from the second message," when
  // really the very first one was just never wired to notifyAdminsOfNewMessage.
  if (firstMessage) {
    notifyAdminsOfNewMessage(orderId, customer?.username, firstMessage.text || '');
  }
  res.json({ ok: true, orderId });
});

// Looked up right after Discord login (and on page load, if already
// logged in) so a customer who closes the chat -- or leaves and comes
// back later, even on a different device -- can get straight back into
// it instead of it just vanishing. Declared BEFORE /api/sessions/:orderId
// below, since Express would otherwise match "by-customer" as if it
// were an :orderId value.
app.get('/api/sessions/by-customer/:discordId', (req, res) => {
  const discordId = req.params.discordId;
  const active = [];
  for (const [orderId, session] of sessions.entries()) {
    if (session.status === 'active' && session.customer?.discordId === discordId) {
      active.push(publicSession(orderId, session));
    }
  }
  active.sort((a, b) => b.createdAt - a.createdAt);
  res.json(active);
});

app.get('/api/sessions/:orderId', (req, res) => {
  const session = sessions.get(req.params.orderId);
  if (!session) return res.status(404).json({ error: 'not found' });
  res.json(publicSession(req.params.orderId, session));
});

// Admin panel / Android app: list every currently-active chat.
app.get('/api/sessions', (req, res) => {
  const active = [];
  for (const [orderId, session] of sessions.entries()) {
    if (session.status === 'active') active.push(publicSession(orderId, session));
  }
  active.sort((a, b) => b.createdAt - a.createdAt);
  res.json(active);
});

app.get('/health', (req, res) => res.json({ ok: true, activeSessions: sessions.size, fcmEnabled, registeredAdminDevices: adminFcmTokens.size }));

// Called once by the Android admin app after it obtains its FCM
// registration token, so a new customer message can reach the admin's
// phone even while the app is backgrounded or the site tab is closed.
app.post('/api/admin/fcm-token', (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'token is required' });
  const isNew = !adminFcmTokens.has(token);
  adminFcmTokens.add(token);
  console.log(`[MOON] FCM token ${isNew ? 'registered' : 're-registered'} (...${token.slice(-12)}). Total devices: ${adminFcmTokens.size}. fcmEnabled=${fcmEnabled}`);
  if (isNew) persistFcmTokens();
  res.json({ ok: true, fcmEnabled });
});

// ---------------- Socket.io ----------------

io.on('connection', (socket) => {
  socket.on('join_order', ({ orderId }) => {
    if (!orderId || !sessions.has(orderId)) return;
    socket.join(orderId);
    socket.data.orderId = orderId;
  });

  // The admin panel (site or Android app) joins this room once to
  // receive "a new order came in" / "a customer replied" notifications
  // for every session, without needing to join each order room
  // individually.
  socket.on('join_admin', () => {
    socket.join('admin_room');
  });

  socket.on('send_message', ({ orderId, message }) => {
    const session = sessions.get(orderId);
    if (!session || session.status !== 'active' || !message) return;
    const stored = {
      from: message.from, // 'customer' | 'admin'
      text: String(message.text || '').slice(0, 2000),
      ts: Date.now()
    };
    session.messages.push(stored);
    io.to(orderId).emit('new_message', stored);
    // Lets the admin side show an unread badge / trigger a push
    // notification even if it hasn't opened this specific order's room.
    io.to('admin_room').emit('admin_notify', {
      orderId,
      message: stored,
      customer: session.customer
    });
    // Real push (FCM), for when the admin app is backgrounded or the
    // site tab is closed entirely -- the socket-based admin_notify above
    // only reaches a currently-open tab/app. Only fires for customer
    // messages; the admin doesn't need a push for their own reply.
    if (stored.from === 'customer') {
      notifyAdminsOfNewMessage(orderId, session.customer?.username, stored.text);
    }
  });

  // Admin panel's "Complete order" button -- closes the chat and tells
  // the customer's tab to return to the homepage.
  socket.on('complete_order', ({ orderId }) => {
    const session = sessions.get(orderId);
    if (!session) return;
    session.status = 'completed';
    io.to(orderId).emit('order_completed');
  });

  socket.on('disconnect', () => {});
});

const PORT = process.env.PORT || 3000;
(async () => {
  // Awaited so the server doesn't open for requests -- including the
  // very request that wakes it from a Render free-tier spin-down, which
  // is often a brand new order -- until previously-registered devices
  // are back in adminFcmTokens. See loadPersistedFcmTokens's own comment
  // for why this specific ordering is what the fix actually depends on.
  await loadPersistedFcmTokens();
  server.listen(PORT, () => {
    console.log(`MOON chat server listening on port ${PORT}`);
  });
})();
