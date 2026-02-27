
const fs = require('fs');
const { MongoClient } = require('mongodb');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');

const PROJECT_ID = '699c12be8df98bd863d63d70';
const CLUSTER_NAME = 'mcpatlas';
const MONGO_URI = 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';

const result = {
  projectId: PROJECT_ID,
  clusterName: CLUSTER_NAME,
  mongoUri: MONGO_URI,
};

function writeResult() {
  try { fs.mkdirSync('outputs', { recursive: true }); } catch {}
  fs.writeFileSync('proxy_sat_test.json', JSON.stringify(result, null, 2));
}

process.on('uncaughtException', (err) => {
  result.uncaughtException = String(err && err.message || err);
  result.uncaughtStack = err && err.stack || null;
  try { writeResult(); } catch {}
  process.exit(1);
});
process.on('unhandledRejection', (err) => {
  result.unhandledRejection = String(err && err.message || err);
  result.unhandledRejectionStack = err && err.stack || null;
  try { writeResult(); } catch {}
  process.exit(1);
});

function buildCookieHeader() {
  const raw = JSON.parse(fs.readFileSync('browser_cookies.json', 'utf8'));
  const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
  const allowedDomains = new Set(['cloud.mongodb.com', '.cloud.mongodb.com']);
  const filtered = cookies.filter((c) => {
    const domain = String(c.domain || '');
    const value = typeof c.value === 'string' ? c.value : '';
    return value && allowedDomains.has(domain);
  });
  const cookieMap = new Map();
  let cloudSrt = null;
  for (const c of filtered) {
    const name = String(c.name);
    const value = String(c.value);
    cookieMap.set(name, value);
    if (name === '__Secure-mdb-srt') cloudSrt = value;
  }
  if (cloudSrt) {
    cookieMap.set('__Secure-mdb-sat', cloudSrt);
    result.satOverriddenFromSrt = true;
  }
  result.cookieNames = Array.from(cookieMap.keys());
  return Array.from(cookieMap.entries()).map(([k, v]) => `${k}=${v}`).join('; ');
}

const COOKIE_HEADER = buildCookieHeader();
result.cookieHeaderLength = COOKIE_HEADER.length;

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
    result.proxyUrlExample = url.toString();

    const headers = {
      'User-Agent': 'Mozilla/5.0',
      'Origin': 'https://cloud.mongodb.com',
      'Cookie': COOKIE_HEADER,
    };

    this.ws = new WebSocket(url, { headers });

    this.ws.on('open', () => {
      result.wsOpened = (result.wsOpened || 0) + 1;
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
        const text = rest.toString('utf8');
        let msg;
        try { msg = JSON.parse(text); } catch {
          result.proxyPreMessageRaw = text;
          this.destroy(new Error('Invalid JSON from proxy: ' + text));
          return;
        }
        result.proxyPreMessage = msg;
        if (msg.preMessageOk === 1) {
          this.connected = true;
          this.emit('connect');
          this.emit('secureConnect');
          this._flushPendingWrites();
        } else {
          this.destroy(new Error('Unexpected proxy pre-message: ' + text));
        }
      } else if (type === 2) {
        this._refreshTimeout();
        this.push(rest);
      } else {
        this.destroy(new Error('Unexpected proxy frame type: ' + type));
      }
    });

    this.ws.on('error', (err) => {
      result.wsError = String(err && err.message || err);
      this.destroy(err);
    });
    this.ws.on('close', (code, reason) => {
      result.wsClose = { code, reason: reason ? reason.toString() : '' };
      if (!this.destroyed) {
        const normalish = code === 1000 || code === 4100;
        if (normalish) {
          this.push(null);
          super.destroy();
        } else {
          this.destroy(new Error(`WebSocket closed: code=${code} reason=${reason ? reason.toString() : ''}`));
        }
      }
    });
  }

  _flushPendingWrites() {
    if (!this.connected || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    while (this._pendingWrites.length) {
      const { chunk, encoding, callback } = this._pendingWrites.shift();
      this._writeNow(chunk, encoding, callback);
    }
  }

  _writeNow(chunk, encoding, callback) {
    try {
      this._refreshTimeout();
      const payload = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding);
      const frame = Buffer.concat([Buffer.from([2]), payload]);
      this.ws.send(frame, callback);
    } catch (e) {
      callback(e);
    }
  }
  _read() {}
  _write(chunk, encoding, callback) {
    if (this.destroyed) return callback(new Error('Socket destroyed'));
    if (!this.connected || !this.ws || this.ws.readyState !== WebSocket.OPEN) {
      this._pendingWrites.push({ chunk, encoding, callback });
      return;
    }
    this._writeNow(chunk, encoding, callback);
  }
  _destroy(err, callback) {
    this._clearTimeout();
    while (this._pendingWrites.length) {
      const item = this._pendingWrites.shift();
      try { item.callback(err || new Error('Socket destroyed')); } catch {}
    }
    try {
      if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
        this.ws.close(4100, err ? String(err.message || err) : 'Driver closed socket');
      }
    } catch {}
    callback(err);
  }
  setKeepAlive() { return this; }
  setNoDelay() { return this; }
  setTimeout(ms, cb) {
    this._timeout = ms;
    if (typeof cb === 'function') this.once('timeout', cb);
    this._refreshTimeout();
    return this;
  }
  _clearTimeout() {
    if (this._timeoutId) {
      clearTimeout(this._timeoutId);
      this._timeoutId = null;
    }
  }
  _refreshTimeout() {
    this._clearTimeout();
    if (typeof this._timeout === 'number' && this._timeout > 0 && Number.isFinite(this._timeout)) {
      this._timeoutId = setTimeout(() => this.emit('timeout'), this._timeout);
    }
  }
  once(event, listener) {
    if (event === 'secureConnect' && this.connected) {
      queueMicrotask(() => listener());
      return this;
    }
    return super.once(event, listener);
  }
}

const origTlsConnect = tls.connect.bind(tls);
tls.connect = function patchedTlsConnect(options, callback) {
  const host = options && (options.host || options.servername);
  const port = options && options.port;
  if (host === 'cloud.mongodb.com' || host === 'account.mongodb.com' || port === 443) {
    return origTlsConnect(options, callback);
  }
  const sock = new TLSSocketProxy(options || {});
  if (typeof callback === 'function') sock.once('secureConnect', callback);
  return sock;
};

async function main() {
  const client = new MongoClient(MONGO_URI, {
    serverSelectionTimeoutMS: 30000,
    connectTimeoutMS: 30000,
    socketTimeoutMS: 30000,
    directConnection: false,
    monitorCommands: false,
  });
  try {
    await client.connect();
    result.connected = true;
    result.ping = await client.db('admin').command({ ping: 1 });
    const dbsResp = await client.db('admin').admin().listDatabases();
    result.databases = (dbsResp.databases || []).map((d) => ({
      name: d.name,
      sizeOnDisk: d.sizeOnDisk,
      empty: d.empty
    }));
    for (const dbMeta of result.databases) {
      if (['admin','local','config'].includes(dbMeta.name)) continue;
      const cols = await client.db(dbMeta.name).listCollections().toArray();
      dbMeta.collections = cols.map(c => c.name);
    }
  } catch (e) {
    result.connected = false;
    result.error = String(e && e.message || e);
    result.stack = e && e.stack || null;
  } finally {
    try { await client.close(); } catch {}
    writeResult();
  }
}

main();
