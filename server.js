// Siege Tactics relay server - ONE file, no libraries, no package.json needed.
// Run with:  node server.js
// Players connect with WebSockets (wss on 443), which school/work wifi allows,
// and this server passes messages between the two devices.
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const GRACE_MS = 12000;
const MAX = 2 * 1024 * 1024;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const peers = new Map();
const rid = () => Math.random().toString(36).slice(2, 10);

/* ---------- minimal WebSocket (RFC 6455) ---------- */
function frame(op, payload) {
  const len = payload.length;
  let h;
  if (len < 126) h = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([h, payload]);
}
class WS {
  constructor(sock) { this.sock = sock; this.buf = Buffer.alloc(0); this.open = true; this.msgs = []; this.fragOp = 0; this.frag = []; this.alive = true; this.onmessage = null; this.onclose = null;
    sock.on('data', (d) => this.data(d)); sock.on('close', () => this.end()); sock.on('error', () => this.end()); sock.setNoDelay(true); }
  send(str) { if (this.open) try { this.sock.write(frame(1, Buffer.from(str))); } catch (_) {} }
  ping() { if (this.open) try { this.sock.write(frame(9, Buffer.alloc(0))); } catch (_) {} }
  end() { if (!this.open) return; this.open = false; try { this.sock.destroy(); } catch (_) {} if (this.onclose) this.onclose(); }
  data(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 15, masked = !!(b[1] & 0x80); let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > MAX) return this.end();
      const need = off + (masked ? 4 : 0) + len; if (b.length < need) return;
      let p = b.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const k = b.subarray(off, off + 4); p = Buffer.from(p); for (let i = 0; i < p.length; i++) p[i] ^= k[i & 3]; }
      this.buf = b.subarray(need);
      if (op === 8) { try { this.sock.write(frame(8, Buffer.alloc(0))); } catch (_) {} return this.end(); }
      if (op === 9) { try { this.sock.write(frame(10, p)); } catch (_) {} continue; }
      if (op === 10) { this.alive = true; continue; }
      if (op === 1 || op === 2) { this.fragOp = op; this.frag = [p]; } else if (op === 0) this.frag.push(p);
      if (fin && this.frag.length) { const m = Buffer.concat(this.frag).toString('utf8'); this.frag = []; if (this.onmessage) this.onmessage(m); }
    }
  }
}

/* ---------- relay logic ---------- */
const sendTo = (id, o) => { const p = peers.get(id); if (p && p.ws && !p.dead) p.ws.send(JSON.stringify(o)); };
function dropLinks(p) {
  for (const l of p.links) {
    const [other, cid] = l.split('|');
    sendTo(other, { op: 'close', from: p.id, cid });
    const o = peers.get(other); if (o) o.links.delete(p.id + '|' + cid);
  }
  p.links.clear();
}
function removePeer(p) { clearTimeout(p.timer); dropLinks(p); if (peers.get(p.id) === p) peers.delete(p.id); }

function onSocket(ws) {
  let me = null;
  ws.onmessage = (raw) => {
    let m; try { m = JSON.parse(raw); } catch (_) { return; }
    if (!m || typeof m.op !== 'string') return;
    if (m.op === 'reg') {
      let id = typeof m.id === 'string' && /^[\w-]{3,64}$/.test(m.id) ? m.id : null;
      if (id) {
        const ex = peers.get(id);
        if (ex && !ex.dead) return ws.send(JSON.stringify({ op: 'err', type: 'unavailable-id' }));
        if (ex && ex.dead) { clearTimeout(ex.timer); ex.dead = false; ex.ws = ws; me = ex; return ws.send(JSON.stringify({ op: 'open', id, resumed: 1 })); }
      } else { do { id = rid(); } while (peers.has(id)); }
      me = { id, ws, links: new Set(), dead: false, timer: null };
      peers.set(id, me);
      return ws.send(JSON.stringify({ op: 'open', id }));
    }
    if (!me) return;
    if (m.op === 'conn') {
      const t = peers.get(m.to);
      if (!t || t.dead) return ws.send(JSON.stringify({ op: 'err', type: 'peer-unavailable', cid: m.cid }));
      me.links.add(m.to + '|' + m.cid); t.links.add(me.id + '|' + m.cid);
      return t.ws.send(JSON.stringify({ op: 'conn', from: me.id, cid: m.cid }));
    }
    if (m.op === 'ack' || m.op === 'data' || m.op === 'close') {
      if (!me.links.has(m.to + '|' + m.cid)) return;
      if (m.op === 'close') { me.links.delete(m.to + '|' + m.cid); const o = peers.get(m.to); if (o) o.links.delete(me.id + '|' + m.cid); }
      sendTo(m.to, { op: m.op, from: me.id, cid: m.cid, d: m.d });
    }
  };
  ws.onclose = () => { if (!me || me.ws !== ws) return; me.dead = true; me.timer = setTimeout(() => removePeer(me), GRACE_MS); };
}

const clients = new Set();
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' });
  res.end('Siege Tactics relay is running. Peers online: ' + peers.size + '\n');
});
server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  const ws = new WS(sock); clients.add(ws);
  const oc = ws.end.bind(ws); ws.cleanup = () => clients.delete(ws);
  sock.on('close', () => clients.delete(ws));
  onSocket(ws);
});
setInterval(() => { clients.forEach((ws) => { if (!ws.alive) return ws.end(); ws.alive = false; ws.ping(); }); }, 25000);
server.listen(PORT, () => console.log('Siege relay listening on ' + PORT));
