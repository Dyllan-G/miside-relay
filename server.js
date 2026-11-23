// MiSide browser relay: host-and-join rooms, forwards messages between players.
// Usage: node server.js [port]   (default 8080)
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = Number(process.argv[2] || process.env.PORT || 8080);
const MAX_PLAYERS = 4;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I
const MAX_MESSAGE_BYTES = 64 * 1024;

const rooms = new Map(); // code -> { code, host: id, members: Map<id, ws> }
let nextId = 1;

function makeCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < 5; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
}

function send(ws, msg) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
}

function broadcast(room, msg, exceptId) {
  for (const [id, ws] of room.members) if (id !== exceptId) send(ws, msg);
}

function leave(ws) {
  const room = ws.room;
  if (!room) return;
  room.members.delete(ws.pid);
  ws.room = null;
  broadcast(room, { t: 'left', id: ws.pid });
  if (room.members.size === 0) { rooms.delete(room.code); return; }
  if (room.host === ws.pid) {
    // Host left: close the room so clients don't run a stale world.
    broadcast(room, { t: 'closed', reason: 'host left' });
    for (const [, m] of room.members) { m.room = null; }
    rooms.delete(room.code);
  }
}

const wss = new WebSocketServer({ port: PORT, maxPayload: MAX_MESSAGE_BYTES });
console.log(`MiSide relay listening on ws://localhost:${PORT}`);

wss.on('connection', (ws) => {
  ws.pid = 'p' + (nextId++);
  ws.room = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  send(ws, { t: 'hello', id: ws.pid });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(String(raw)); } catch { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'host') {
      if (ws.room) leave(ws);
      const code = makeCode();
      const room = { code, host: ws.pid, members: new Map([[ws.pid, ws]]) };
      rooms.set(code, room);
      ws.room = room;
      send(ws, { t: 'joined', code, id: ws.pid, host: true, players: [ws.pid] });
      return;
    }

    if (m.t === 'join') {
      const code = String(m.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) return send(ws, { t: 'error', msg: 'No room with that code' });
      if (room.members.size >= MAX_PLAYERS) return send(ws, { t: 'error', msg: 'Room is full' });
      if (ws.room) leave(ws);
      room.members.set(ws.pid, ws);
      ws.room = room;
      const players = [...room.members.keys()];
      send(ws, { t: 'joined', code, id: ws.pid, host: false, hostId: room.host, players });
      broadcast(room, { t: 'joined_peer', id: ws.pid }, ws.pid);
      return;
    }

    if (m.t === 'leave') { leave(ws); return send(ws, { t: 'left_self' }); }

    // Anything else is game data: relay it to the rest of the room.
    if (ws.room) {
      // Mark the sender so receivers know who it came from.
      broadcast(ws.room, { t: 'relay', from: ws.pid, data: m }, ws.pid);
    }
  });

  ws.on('error', () => leave(ws));
  ws.on('close', () => leave(ws));
});

// Drop connections that stop answering pings (dead Wi-Fi, sleeping laptops).
const pinger = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 15000);
wss.on('close', () => clearInterval(pinger));
