// Siege Tactics relay server. Players connect with WebSockets (wss:// on port 443),
// which school and work networks almost always allow, and the server passes
// messages between the two devices. No game logic lives here.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const GRACE_MS = 12000; // how long a dropped player can come back and keep their links

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' });
  res.end('Siege Tactics relay is running. Peers online: ' + peers.size + '\n');
});
const wss = new WebSocketServer({ server, maxPayload: 2 * 1024 * 1024 });

const peers = new Map(); // id -> { id, ws, timer, links:Set<"otherId|cid"> }
const rid = () => Math.random().toString(36).slice(2, 10);
const send = (ws, o) => { try { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); } catch (_) {} };
const sendTo = (id, o) => { const p = peers.get(id); if (p && p.ws) send(p.ws, o); };

function dropLinks(p) {
  for (const l of p.links) {
    const [other, cid] = l.split('|');
    sendTo(other, { op: 'close', from: p.id, cid });
    const o = peers.get(other); if (o) o.links.delete(p.id + '|' + cid);
  }
  p.links.clear();
}
function removePeer(p) { clearTimeout(p.timer); dropLinks(p); if (peers.get(p.id) === p) peers.delete(p.id); }

wss.on('connection', (ws) => {
  let me = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (_) { return; }
    if (!m || typeof m.op !== 'string') return;
    if (m.op === 'reg') {
      let id = typeof m.id === 'string' && /^[\w-]{3,64}$/.test(m.id) ? m.id : null;
      if (id) {
        const ex = peers.get(id);
        if (ex && ex.ws && ex.ws.readyState === 1 && !ex.dead) return send(ws, { op: 'err', type: 'unavailable-id' });
        if (ex && ex.dead) { clearTimeout(ex.timer); ex.dead = false; ex.ws = ws; me = ex; return send(ws, { op: 'open', id, resumed: 1 }); }
      } else { do { id = rid(); } while (peers.has(id)); }
      me = { id, ws, links: new Set(), dead: false, timer: null };
      peers.set(id, me);
      return send(ws, { op: 'open', id });
    }
    if (!me) return;
    if (m.op === 'conn') {
      const t = peers.get(m.to);
      if (!t || t.dead) return send(ws, { op: 'err', type: 'peer-unavailable', cid: m.cid });
      me.links.add(m.to + '|' + m.cid); t.links.add(me.id + '|' + m.cid);
      return send(t.ws, { op: 'conn', from: me.id, cid: m.cid });
    }
    if (m.op === 'ack' || m.op === 'data' || m.op === 'close') {
      if (!me.links.has(m.to + '|' + m.cid)) return;
      if (m.op === 'close') { me.links.delete(m.to + '|' + m.cid); const o = peers.get(m.to); if (o) o.links.delete(me.id + '|' + m.cid); }
      return sendTo(m.to, { op: m.op, from: me.id, cid: m.cid, d: m.d });
    }
  });
  ws.on('close', () => {
    if (!me || me.ws !== ws) return;
    me.dead = true;
    me.timer = setTimeout(() => removePeer(me), GRACE_MS);
  });
  ws.on('error', () => {});
});

// keep connections alive through proxies and detect dead sockets
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false; try { ws.ping(); } catch (_) {}
  });
}, 25000);

server.listen(PORT, () => console.log('Siege relay listening on ' + PORT));
