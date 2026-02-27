
const { MongoClient } = require('mongodb');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');

const PROJECT_ID = process.env.PROJECT_ID || '699c12be8df98bd863d63d70';
const CLUSTER_NAME = process.env.CLUSTER_NAME || 'mcpatlas';
const COOKIE_HEADER = process.env.CLOUD_COOKIES || '';

const MONGO_URI =
  'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';

class TLSSocketProxy extends Duplex {
  constructor(options = {}) {
    super();
    this.host = options.host || options.servername;
    this.port = options.port || 27017;
    this.remoteAddress = this.host;
    this.remotePort = this.port;
    this.localAddress = 'atlas-proxy';
    this.localPort = Math.floor(Math.random() * 50000) + 10000;
    this.connected = false;
    this.authorized = true;
    this._timeout = 0;
    this._timeoutId = null;
    this._pendingWrites = [];
    this._wsOpen = false;

    const url = new URL(`wss://cloud.mongodb.com/cluster-connection/${PROJECT_ID}`);
    url.searchParams.set('sniHostname', this.host);
    url.searchParams.set('port', String(this.port));
    url.searchParams.set('clusterName', CLUSTER_NAME);
    url.searchParams.set('version', '1');

    this.ws = new WebSocket(url.toString(), {
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Origin': 'https://cloud.mongodb.com',
        'Cookie': COOKIE_HEADER,
      },
    });

    this.ws.on('open', () => {
      this._wsOpen = true;
      const meta = { port: this.port, host: this.host, clusterName: CLUSTER_NAME, ok: 1 };
      const payload = Buffer.from(JSON.stringify(meta), 'utf8');
      this.ws.send(Buffer.concat([Buffer.from([1]), payload]));
    });

    this.ws.on('message', (data) => {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (!buf || buf.length === 0) return;
      const type = buf[0];
      const rest = buf.subarray(1);
      if (type === 1) {
        let msg;
        try { msg = JSON.parse(rest.toString('utf8')); } catch (e) {
          this.destroy(new Error('Invalid JSON from proxy'));
          return;
        }
        if (msg.preMessageOk === 1) {
          this.connected = true;
          this.emit('connect');
          this.emit('secureConnect');
          this._flushPendingWrites();
        } else {
          this.destroy(new Error('Unexpected pre-message JSON: ' + JSON.stringify(msg)));
        }
      } else if (type === 2) {
        this._refreshTimeout();
        this.push(rest);
      } else {
        this.destroy(new Error('Unexpected proxy frame type: ' + type));
      }
    });

    this.ws.on('error', (err) => this.destroy(err));
    this.ws.on('close', (code, reason) => {
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
    if (!this.connected || !this._wsOpen || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
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
    if (!this.connected || !this._wsOpen || !this.ws || this.ws.readyState !== WebSocket.OPEN) {
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

function ser(v) {
  if (v == null) return v;
  if (v instanceof Date) return v.toISOString();
  if (Array.isArray(v)) return v.map(ser);
  if (typeof v === 'object') {
    if (typeof v.toHexString === 'function') return v.toHexString();
    const out = {};
    for (const [k, val] of Object.entries(v)) out[k] = ser(val);
    return out;
  }
  return v;
}

async function getBtcCloseUSD(dateStr) {
  const end = Math.floor(Date.parse(dateStr + 'T23:59:59Z') / 1000);
  const url = `https://min-api.cryptocompare.com/data/v2/histoday?fsym=BTC&tsym=USD&limit=1&toTs=${end}`;
  const resp = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
  const text = await resp.text();
  let data;
  try { data = JSON.parse(text); } catch {
    throw new Error('Failed to parse CryptoCompare response: ' + text.slice(0, 300));
  }
  if (!resp.ok || !data || data.Response === 'Error') {
    throw new Error('CryptoCompare error: ' + text.slice(0, 500));
  }
  const rows = (((data || {}).Data || {}).Data) || [];
  if (!rows.length) throw new Error('No BTC history rows returned');
  let row = rows.find(r => {
    const d = new Date(r.time * 1000).toISOString().slice(0, 10);
    return d === dateStr;
  }) || rows[rows.length - 1];
  if (row.close == null) throw new Error('BTC close missing in response');
  return {
    close_usd: row.close,
    open_usd: row.open,
    high_usd: row.high,
    low_usd: row.low,
    unix_time: row.time,
    source_url: url,
  };
}

async function main() {
  const out = {
    question: 'fuel cost for first delivery using 10L/100km and gas price = 1% BTC close on order date',
    cookieHeaderLength: COOKIE_HEADER.length,
  };

  let client;
  try {
    client = new MongoClient(MONGO_URI, {
      serverSelectionTimeoutMS: 30000,
      connectTimeoutMS: 30000,
      socketTimeoutMS: 30000,
      directConnection: false,
      monitorCommands: false,
    });
    await client.connect();
    out.ping = await client.db('admin').command({ ping: 1 });

    const coll = client.db('video_game_store').collection('Delivery Logistics');
    const first = await coll.find({}).sort({ 'Order Date': 1, _id: 1 }).limit(1).next();
    if (!first) throw new Error('No delivery records found');

    const firstSer = ser(first);
    out.first_delivery = firstSer;

    const dateStr = String(firstSer['Order Date']).slice(0, 10);
    out.order_date = dateStr;
    const distanceKm = Number(first['Delivery Distance (km)']);
    out.delivery_distance_km = distanceKm;
    const liters = distanceKm * 10 / 100;
    out.fuel_liters = liters;

    const btc = await getBtcCloseUSD(dateStr);
    out.bitcoin = btc;

    const gasPrice = btc.close_usd * 0.01;
    out.gas_price_per_liter_usd = gasPrice;
    out.fuel_cost_usd = liters * gasPrice;

    out.answer = {
      order_id: firstSer['Order ID'],
      order_date: dateStr,
      delivery_distance_km: distanceKm,
      fuel_liters: Number(liters.toFixed(6)),
      bitcoin_close_usd: btc.close_usd,
      gas_price_per_liter_usd: Number(gasPrice.toFixed(6)),
      fuel_cost_usd: Number((liters * gasPrice).toFixed(6)),
    };
  } catch (e) {
    out.error = String(e && e.message || e);
    out.stack = e && e.stack || null;
  } finally {
    try { if (client) await client.close(); } catch {}
  }

  console.log(JSON.stringify(out, null, 2));
}

main();
