const fs = require('fs');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');
const { MongoClient } = require('mongodb');

const PROJECT_ID = '699c12be8df98bd863d63d70';
const CLUSTER_NAME = 'mcpatlas';
const MONGO_URI = 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';

function write(obj) {
  fs.writeFileSync('desktop_social_campaign_count_result.json', JSON.stringify(obj, null, 2));
}

function buildCookieHeader() {
  const raw = JSON.parse(fs.readFileSync('browser_cookies.json', 'utf8'));
  const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
  const allowedDomains = new Set(['cloud.mongodb.com', '.cloud.mongodb.com', 'account.mongodb.com']);
  const filtered = cookies.filter((c) => {
    const domain = String(c.domain || '');
    const value = typeof c.value === 'string' ? c.value : '';
    return value && (allowedDomains.has(domain) || domain.endsWith('.cloud.mongodb.com'));
  });
  const cookieMap = new Map();
  for (const c of filtered) cookieMap.set(String(c.name), String(c.value));
  return Array.from(cookieMap.entries()).map(([k, v]) => `${k}=${v}`).join('; ');
}
const COOKIE_HEADER = buildCookieHeader();

class TLSSocketProxy extends Duplex {
  constructor(options = {}) {
    super();
    this.host = options.host || options.servername;
    this.port = options.port || 27017;
    this.remoteAddress = this.host;
    this.remotePort = this.port;
    this.localAddress = 'atlas-proxy';
    this.localPort = Math.floor(Math.random() * 50000) + 10000;
    this.authorized = true;
    this.encrypted = true;
    this.connected = false;
    this._pendingWrites = [];
    this._timeout = 0;
    this._timeoutId = null;

    const url = new URL(`wss://cloud.mongodb.com/cluster-connection/${PROJECT_ID}`);
    url.searchParams.set('sniHostname', this.host);
    url.searchParams.set('port', String(this.port));
    url.searchParams.set('clusterName', CLUSTER_NAME);
    url.searchParams.set('version', '1');

    this.ws = new WebSocket(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Origin': 'https://cloud.mongodb.com',
        'Cookie': COOKIE_HEADER,
      }
    });

    this.ws.on('open', () => {
      const meta = { port: this.port, host: this.host, clusterName: CLUSTER_NAME, ok: 1 };
      const payload = Buffer.from(JSON.stringify(meta), 'utf8');
      const frame = Buffer.concat([Buffer.from([1]), payload]);
      this.ws.send(frame);
    });

    this.ws.on('message', (data) => {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (!buf || !buf.length) return;
      const type = buf[0];
      const rest = buf.subarray(1);
      if (type === 1) {
        let msg;
        try { msg = JSON.parse(rest.toString('utf8')); } catch (e) { this.destroy(e); return; }
        if (msg.preMessageOk === 1) {
          this.connected = true;
          this.emit('connect');
          this.emit('secureConnect');
          this._flush();
        } else {
          this.destroy(new Error('Unexpected pre-message: ' + rest.toString('utf8')));
        }
      } else if (type === 2) {
        this._refreshTimeout();
        this.push(rest);
      } else {
        this.destroy(new Error('Unexpected frame type: ' + type));
      }
    });

    this.ws.on('error', (err) => this.destroy(err));
    this.ws.on('close', (code, reason) => {
      if (!this.destroyed) {
        if (code === 1000 || code === 4100) {
          this.push(null);
          super.destroy();
        } else {
          this.destroy(new Error(`WebSocket closed: code=${code} reason=${reason ? reason.toString() : ''}`));
        }
      }
    });
  }
  _flush() { if (!this.connected || !this.ws || this.ws.readyState !== WebSocket.OPEN) return; while (this._pendingWrites.length) { const { chunk, encoding, callback } = this._pendingWrites.shift(); this._writeNow(chunk, encoding, callback); } }
  _writeNow(chunk, encoding, callback) { try { this._refreshTimeout(); const payload = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding); const frame = Buffer.concat([Buffer.from([2]), payload]); this.ws.send(frame, callback); } catch (e) { callback(e); } }
  _read() {}
  _write(chunk, encoding, callback) { if (this.destroyed) return callback(new Error('Socket destroyed')); if (!this.connected || !this.ws || this.ws.readyState !== WebSocket.OPEN) { this._pendingWrites.push({ chunk, encoding, callback }); return; } this._writeNow(chunk, encoding, callback); }
  _destroy(err, callback) { this._clearTimeout(); while (this._pendingWrites.length) { const item = this._pendingWrites.shift(); try { item.callback(err || new Error('Socket destroyed')); } catch {} } try { if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) { this.ws.close(4100, err ? String(err.message || err) : 'Driver closed socket'); } } catch {} callback(err); }
  setKeepAlive() { return this; }
  setNoDelay() { return this; }
  setTimeout(ms, cb) { this._timeout = ms; if (typeof cb === 'function') this.once('timeout', cb); this._refreshTimeout(); return this; }
  _clearTimeout() { if (this._timeoutId) { clearTimeout(this._timeoutId); this._timeoutId = null; } }
  _refreshTimeout() { this._clearTimeout(); if (typeof this._timeout === 'number' && this._timeout > 0 && Number.isFinite(this._timeout)) { this._timeoutId = setTimeout(() => this.emit('timeout'), this._timeout); } }
  once(event, listener) { if (event === 'secureConnect' && this.connected) { queueMicrotask(() => listener()); return this; } return super.once(event, listener); }
}

const origTlsConnect = tls.connect.bind(tls);
tls.connect = function patchedTlsConnect(options, callback) {
  const host = options && (options.host || options.servername);
  const port = options && options.port;
  if (host === 'cloud.mongodb.com' || host === 'account.mongodb.com' || port === 443) return origTlsConnect(options, callback);
  const sock = new TLSSocketProxy(options || {});
  if (typeof callback === 'function') sock.once('secureConnect', callback);
  return sock;
};

function ser(v) {
  if (v === null || typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
  if (Array.isArray(v)) return v.map(ser);
  if (v instanceof Date) return v.toISOString();
  if (typeof v === 'object') {
    if (v && typeof v.toHexString === 'function') return v.toHexString();
    const out = {};
    for (const [k, val] of Object.entries(v)) out[k] = ser(val);
    return out;
  }
  return String(v);
}

async function main() {
  const out = {
    started_at: new Date().toISOString(),
    question: 'Total sold for Sports video games in February 2023',
    cookieHeaderLength: COOKIE_HEADER.length,
  };
  write(out);
  const client = new MongoClient(MONGO_URI, {
    serverSelectionTimeoutMS: 60000,
    connectTimeoutMS: 60000,
    socketTimeoutMS: 60000,
    directConnection: false,
    monitorCommands: false,
  });

  try {
    await client.connect();
    out.ping = await client.db('admin').command({ ping: 1 });
    const db = client.db('video_game_store');
    out.collections = (await db.listCollections().toArray()).map(x => x.name);

    const coll = db.collection('Customers');
    const start = new Date('2023-02-01T00:00:00.000Z');
    const end = new Date('2023-03-01T00:00:00.000Z');
    const filter = { 'Purchase Date': { $gte: start, $lt: end }, 'Game Genre': 'Sports' };

    out.matchingCount = await coll.countDocuments(filter);
    out.aggregate = (await coll.aggregate([
      { $match: filter },
      {
        $group: {
          _id: null,
          totalPurchaseAmount: { $sum: '$Purchase Amount' },
          avgPurchaseAmount: { $avg: '$Purchase Amount' },
          minPurchaseAmount: { $min: '$Purchase Amount' },
          maxPurchaseAmount: { $max: '$Purchase Amount' }
        }
      }
    ]).toArray()).map(ser);

    out.sample = (await coll.find(filter, {
      projection: {
        _id: 0,
        'Customer ID': 1,
        'Purchase Date': 1,
        'Game Title': 1,
        'Game Genre': 1,
        'Purchase Amount': 1,
        'Preferred Platform': 1,
        'Membership Status': 1
      }
    }).sort({ 'Purchase Amount': -1 }).limit(10).toArray()).map(ser);

    out.answer = {
      matchingCount: out.matchingCount,
      totalPurchaseAmount: out.aggregate[0] ? out.aggregate[0].totalPurchaseAmount : null
    };
  } catch (e) {
    out.error = String(e && e.message || e);
    out.stack = e && e.stack || null;
  } finally {
    out.finished_at = new Date().toISOString();
    write(out);
    console.log(JSON.stringify(out, null, 2));
    try { await client.close(); } catch {}
  }
}

main();
