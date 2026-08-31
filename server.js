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
  res.json({ ok: true, orderId });
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

app.get('/health', (req, res) => res.json({ ok: true, activeSessions: sessions.size }));

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
server.listen(PORT, () => {
  console.log(`MOON chat server listening on port ${PORT}`);
});
