// WebSocket-шлюз для Godot-клиента (протокол v2) — работает параллельно с Socket.IO на том же порту.
// Каждое WS-соединение оборачивается в объект с API сокета Socket.IO (id, emit, on, join, leave),
// поэтому все игровые обработчики server-multiplayer.js общие для браузера и Godot.
//
// Кадр: JSON-массив [event, data]. Первым сообщением клиент шлёт ["hello", {protocol, client, token?}],
// сервер отвечает ["welcome", {id, token, protocol, resumed}].
// Обрыв связи: игрок «паркуется» на GRACE_MS (место, очки, оружие сохраняются), переподключение
// с тем же token возвращает его в ту же комнату под тем же id.
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PROTOCOL = 2;
const GRACE_MS = 30000;         // сколько ждать переподключения
const HEARTBEAT_MS = 3000;      // ping уровня WebSocket; два пропуска — соединение считается мёртвым
const STALE_MS = 12000;         // тишина от клиента (он шлёт ping раз в 2 с) — связь потеряна
const MAX_MSG = 64 * 1024;
const RATE_PER_SEC = 120;       // защита от флуда: событий в секунду на соединение

class WsSocket {
  constructor(id, token, gw) {
    this.id = id; this.token = token; this.gw = gw; this.godot = true;
    this.ws = null; this.handlers = Object.create(null); this.rooms = new Set();
    this.parked = null; this.closed = false;
  }
  on(ev, fn) { (this.handlers[ev] = this.handlers[ev] || []).push(fn); }
  emit(ev, data) {
    if (!this.ws || this.ws.readyState !== 1) return;
    if (this.ws.bufferedAmount > 512 * 1024 && ev === 'gameState') return;   // как volatile: старый кадр не копим
    try { this.ws.send(JSON.stringify([ev, data === undefined ? null : data])); } catch (e) { /* обрыв */ }
  }
  get volatile() { return this; }
  join(code) { this.rooms.add(code); this.gw.roomSet(code).add(this); }
  leave(code) { this.rooms.delete(code); const s = this.gw.rooms.get(code); if (s) { s.delete(this); if (!s.size) this.gw.rooms.delete(code); } }
  fire(ev, data) { for (const fn of this.handlers[ev] || []) { try { fn(data); } catch (e) { console.error('WS handler', ev, e); } } }
}

/**
 * hooks.onConnection(socket) — повесить игровые обработчики (общие с Socket.IO).
 * hooks.onPark(socket) — игрок потерял связь (ещё в комнате). hooks.onResume(socket) — вернулся.
 */
function attachWs(server, hooks) {
  const gw = {
    rooms: new Map(),        // код комнаты → Set<WsSocket>
    clients: new Map(),      // id → WsSocket
    byToken: new Map(),      // token → WsSocket
    roomSet(code) { let s = this.rooms.get(code); if (!s) this.rooms.set(code, s = new Set()); return s; },
    // рассылка WS-участникам комнаты (или одному клиенту, если target — его id), кроме except
    emit(target, ev, data, except) {
      const direct = this.clients.get(target);
      if (direct) { if (direct.id !== except) direct.emit(ev, data); return; }
      const s = this.rooms.get(target);
      if (s) for (const c of s) if (c.id !== except) c.emit(ev, data);
    },
  };
  const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_MSG });
  server.on('upgrade', (req, sock, head) => {
    let path = '/';
    try { path = new URL(req.url, 'http://x').pathname; } catch (e) { /* плохой url */ }
    if (path !== '/ws') return;   // остальное (в т.ч. /socket.io/) обрабатывает Socket.IO
    wss.handleUpgrade(req, sock, head, ws => wss.emit('connection', ws, req));
  });

  wss.on('connection', ws => {
    let sock = null;
    let alive = true, budget = RATE_PER_SEC, budgetAt = Date.now(), lastMsg = Date.now();
    ws.on('pong', () => { alive = true; });
    const beat = setInterval(() => {
      // Прокси хостинга (Render) сам отвечает на ping WebSocket, поэтому обрыв видно только по тишине:
      // клиент шлёт ["ping"] каждые 2 с — молчит дольше STALE_MS, значит связи нет.
      if (!alive || Date.now() - lastMsg > STALE_MS) { ws.terminate(); return; }
      alive = false;
      try { ws.ping(); } catch (e) { /* закрыт */ }
    }, HEARTBEAT_MS);

    ws.on('message', raw => {
      const now = Date.now();
      lastMsg = now;
      if (now - budgetAt >= 1000) { budget = RATE_PER_SEC; budgetAt = now; }
      if (--budget < 0) return;
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
      if (!Array.isArray(msg) || typeof msg[0] !== 'string') return;
      const [ev, data] = msg;
      if (!sock) {
        if (ev !== 'hello') return;
        sock = handshake(ws, data || {});
        return;
      }
      if (ev === 'ping') { sock.emit('pong', data); return; }   // RTT и контроль связи на клиенте
      if (ev === 'hello' || ev === 'disconnect') return;
      sock.fire(ev, data && typeof data === 'object' ? data : (data === null ? undefined : data));
    });

    ws.on('close', () => {
      clearInterval(beat);
      if (sock && sock.ws === ws) park(sock);
    });
    ws.on('error', () => { /* close придёт следом */ });
  });

  function handshake(ws, hello) {
    const token = typeof hello.token === 'string' ? hello.token.slice(0, 64) : '';
    let sock = token && gw.byToken.get(token);
    let resumed = false;
    if (sock && !sock.closed) {
      // переподключение: тот же id, та же комната
      if (sock.ws && sock.ws !== ws) { try { sock.ws.terminate(); } catch (e) { /* уже закрыт */ } }
      if (sock.parked) { clearTimeout(sock.parked); sock.parked = null; }
      sock.ws = ws;
      resumed = true;
    } else {
      const id = 'g' + crypto.randomBytes(6).toString('hex');
      const tok = crypto.randomBytes(16).toString('hex');
      sock = new WsSocket(id, tok, gw);
      sock.ws = ws;
      gw.clients.set(id, sock);
      gw.byToken.set(tok, sock);
      hooks.onConnection(sock);
    }
    sock.emit('welcome', { id: sock.id, token: sock.token, protocol: PROTOCOL, resumed });
    if (resumed) hooks.onResume(sock);
    return sock;
  }

  function park(sock) {
    if (sock.closed || sock.parked) return;
    sock.ws = null;
    if (!hooks.onPark(sock)) { finish(sock); return; }   // не в комнате — ждать нечего
    sock.parked = setTimeout(() => finish(sock), GRACE_MS);
  }

  function finish(sock) {
    if (sock.closed) return;
    sock.closed = true; sock.parked = null;
    sock.fire('disconnect');
    for (const code of [...sock.rooms]) sock.leave(code);
    gw.clients.delete(sock.id);
    gw.byToken.delete(sock.token);
  }

  return gw;
}

module.exports = { attachWs, PROTOCOL, GRACE_MS };
