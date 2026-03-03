
const fs = require('fs');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');
const { MongoClient } = require('mongodb');

const PROJECT_ID = '699c12be8df98bd863d63d70';
const CLUSTER_NAME = 'mcpatlas';
const MONGO_URI = 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';
const RESULT_PATH = 'desktop_social_campaign_count_result.json';

function write(obj) {
  fs.writeFileSync(RESULT_PATH, JSON.stringify(obj, null, 2));
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

function ser(v, depth = 0) {
  if (v === null || typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
  if (v instanceof Date) return v.toISOString();
  if (Array.isArray(v)) return v.slice(0, 10).map(x => ser(x, depth + 1));
  if (v && typeof v === 'object') {
    if (typeof v.toHexString === 'function') return v.toHexString();
    const out = {};
    for (const [k, val] of Object.entries(v).slice(0, 80)) out[k] = ser(val, depth + 1);
    return out;
  }
  return String(v);
}
function norm(s) { return String(s ?? '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim(); }
function normKey(s) { return norm(s).replace(/ /g, ''); }
function parseNum(v) {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  const s = String(v ?? '').replace(/[$,%]/g, '').replace(/,/g, '').trim();
  if (/^[-+]?\d+(?:\.\d+)?$/.test(s)) return Number(s);
  return null;
}
function parseDate(v) {
  if (v instanceof Date && !isNaN(v)) return v;
  if (typeof v === 'number' && Number.isFinite(v)) {
    if (v > 1e12) { const d = new Date(v); if (!isNaN(d)) return d; }
    if (v > 1e9) { const d = new Date(v * 1000); if (!isNaN(d)) return d; }
    if (v >= 1900 && v <= 2100) return new Date(Date.UTC(Math.trunc(v), 0, 1));
  }
  const s = String(v ?? '').trim();
  if (!s) return null;
  let d = new Date(s);
  if (!isNaN(d)) return d;
  let m = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
  if (m) return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
  if (m) return new Date(Date.UTC(Number(m[3]), Number(m[1]) - 1, Number(m[2])));
  m = s.match(/\b(19|20)\d{2}\b/);
  if (m) return new Date(Date.UTC(Number(m[0]), 0, 1));
  return null;
}
function mobileValue(v) {
  const s = norm(v);
  return ['mobile','phone','smartphone','ios','android'].some(tok => s.split(' ').includes(tok) || s === tok || s.includes(tok));
}
function flatten(obj, prefix = '', out = {}) {
  if (obj instanceof Date) { out[prefix || 'value'] = obj; return out; }
  if (Array.isArray(obj)) {
    out[prefix || 'value'] = obj.slice(0, 10).map(ser);
    obj.slice(0, 5).forEach((item, i) => flatten(item, `${prefix}[${i}]`, out));
    return out;
  }
  if (obj && typeof obj === 'object') {
    if (typeof obj.toHexString === 'function') { out[prefix || 'value'] = obj.toHexString(); return out; }
    for (const [k, v] of Object.entries(obj)) {
      const p = prefix ? `${prefix}.${k}` : String(k);
      flatten(v, p, out);
    }
    return out;
  }
  out[prefix || 'value'] = obj;
  return out;
}
function scoreName(dbName, collName) {
  const nk = normKey(`${dbName} ${collName}`);
  let score = 0;
  if (nk.includes('campaign')) score += 15;
  if (nk.includes('advert')) score += 12;
  if (nk.includes('marketing')) score += 10;
  if (nk.includes('digital')) score += 6;
  if (nk.includes('social')) score += 5;
  if (nk.includes('ad')) score += 4;
  return score;
}
function pickBest(flat, kind, context) {
  let best = null;
  for (const [path, value] of Object.entries(flat)) {
    const nk = normKey(path);
    let score = 0;
    let parsed = null;
    if (kind === 'date') {
      const d = parseDate(value);
      if (!d) continue;
      if (nk.includes('campaignstartdate')) score += 300;
      if (nk.includes('startdate')) score += 260;
      if (nk.includes('campaignstart')) score += 250;
      if (nk.includes('start')) score += 160;
      if (nk.includes('launch')) score += 140;
      if (nk.includes('begin')) score += 120;
      if (score === 0 && nk.includes('date') && /campaign|advert|marketing|ad/.test(context)) score += 60;
      if (nk.includes('enddate') || nk.includes('updated') || nk.includes('created')) score -= 100;
      parsed = d;
    } else if (kind === 'device') {
      const text = String(value ?? '');
      if (!text) continue;
      if (nk.includes('targetdevice')) score += 260;
      if (nk === 'device' || nk.endsWith('device') || nk.includes('device')) score += 220;
      if (nk.includes('platform')) score += 120;
      if (nk.includes('channel')) score += 20;
      if (mobileValue(text)) score += 40;
      parsed = text;
    } else if (kind === 'cpc') {
      const num = parseNum(value);
      if (num === null) continue;
      if (nk === 'cpc' || nk.endsWith('cpc')) score += 160;
      if (nk.includes('costperclick') || nk.includes('averagecpc') || nk.includes('avgcpc')) score += 150;
      if (nk.includes('clickcost')) score += 120;
      if (nk.includes('cpm') || nk.includes('ctr')) score -= 80;
      parsed = num;
    } else {
      continue;
    }
    if (score <= 0) continue;
    const cand = { path, value, parsed, score };
    if (!best || cand.score > best.score) best = cand;
  }
  return best;
}
function extractMatches(doc, dbName, collName) {
  const flat = flatten(doc);
  const context = norm(`${dbName} ${collName} ${Object.keys(flat).slice(0, 20).join(' ')}`);
  const date = pickBest(flat, 'date', context);
  if (!date) return { matches: [], flat };
  const d = date.parsed;
  if (d.getUTCFullYear() !== 2023 || d.getUTCMonth() !== 11) return { matches: [], flat };
  const device = pickBest(flat, 'device', context);
  const cpc = pickBest(flat, 'cpc', context);
  const matches = [];
  for (const [path, value] of Object.entries(flat)) {
    const nk = normKey(path);
    const num = parseNum(value);
    if (num === null) continue;
    if ((nk.includes('cpc') || nk.includes('costperclick') || nk.includes('avgcpc')) && nk.includes('mobile')) {
      matches.push({ cpc: num, source: 'mobile_path', cpcPath: path, deviceValue: 'mobile', datePath: date.path });
    }
  }
  if (device && mobileValue(device.value) && cpc) {
    matches.push({ cpc: cpc.parsed, source: 'device_field', cpcPath: cpc.path, devicePath: device.path, deviceValue: ser(device.value), datePath: date.path });
  }
  const seen = new Set();
  const uniq = [];
  for (const m of matches) {
    const key = `${m.source}|${m.cpcPath}|${m.devicePath || ''}`;
    if (!seen.has(key)) { seen.add(key); uniq.push(m); }
  }
  return { matches: uniq, flat, context, bestDate: date, bestDevice: device, bestCpc: cpc };
}

async function main() {
  const out = { started_at: new Date().toISOString(), question: 'Average CPC from mobile devices for campaigns started in December 2023', cookieHeaderLength: COOKIE_HEADER.length };
  write(out);
  const client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 60000, connectTimeoutMS: 60000, socketTimeoutMS: 60000, directConnection: false, monitorCommands: false });
  try {
    await client.connect();
    out.ping = await client.db('admin').command({ ping: 1 });
    const dbNames = (await client.db('admin').admin().listDatabases()).databases.map(x => x.name).filter(x => !['admin','local','config'].includes(x));
    out.databases = dbNames;
    const collectionStats = [];
    const answers = [];
    for (const dbName of dbNames) {
      const cols = await client.db(dbName).listCollections().toArray();
      for (const meta of cols) {
        const collName = meta.name;
        const coll = client.db(dbName).collection(collName);
        let scanned = 0;
        let matches = [];
        let firstDoc = null;
        try {
          const cursor = coll.find({}, { batchSize: 100 });
          for await (const doc of cursor) {
            scanned += 1;
            if (!firstDoc) firstDoc = ser(doc);
            if (scanned > 5000) break;
            const info = extractMatches(doc, dbName, collName);
            for (const m of info.matches) {
              matches.push({ db: dbName, collection: collName, score: scoreName(dbName, collName), sample: ser(doc), ...m });
            }
          }
        } catch (e) {
          collectionStats.push({ db: dbName, collection: collName, scanned, error: String(e && e.message || e), nameScore: scoreName(dbName, collName), firstDoc });
          continue;
        }
        collectionStats.push({ db: dbName, collection: collName, scanned, matchCount: matches.length, nameScore: scoreName(dbName, collName), firstDoc, sampleMatches: matches.slice(0,3) });
        if (matches.length) {
          const avg = matches.reduce((s, m) => s + Number(m.cpc), 0) / matches.length;
          answers.push({ db: dbName, collection: collName, matchCount: matches.length, averageCpc: Number(avg.toFixed(3)), nameScore: scoreName(dbName, collName), sampleMatches: matches.slice(0,5) });
        }
      }
    }
    collectionStats.sort((a,b) => (b.matchCount || 0) - (a.matchCount || 0) || (b.nameScore || 0) - (a.nameScore || 0) || `${a.db}.${a.collection}`.localeCompare(`${b.db}.${b.collection}`));
    answers.sort((a,b) => (b.matchCount - a.matchCount) || (b.nameScore - a.nameScore) || `${a.db}.${a.collection}`.localeCompare(`${b.db}.${b.collection}`));
    out.collectionStats = collectionStats;
    out.answers = answers;
    out.answer = answers[0] || null;
  } catch (e) {
    out.error = String(e && e.message || e);
    out.stack = e && e.stack || null;
  } finally {
    out.finished_at = new Date().toISOString();
    write(out);
    console.log(JSON.stringify(out.answer || { error: out.error || 'no answer', databases: out.databases }, null, 2));
    try { await client.close(); } catch {}
  }
}
main();
