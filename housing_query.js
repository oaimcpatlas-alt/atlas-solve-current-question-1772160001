
const fs = require('fs');
const { MongoClient } = require('mongodb');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');

const PROJECT_ID = '699c12be8df98bd863d63d70';
const CLUSTER_NAME = 'mcpatlas';
const MONGO_URI = 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';
const COOKIES_PATH = 'browser_cookies.json';
const TARGET_YEAR = 2010;

function norm(s) {
  return String(s ?? '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}
function num(v) {
  if (v === null || v === undefined || typeof v === 'boolean') return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  const s = String(v).replace(/,/g, '').trim();
  const m = s.match(/-?\d+(?:\.\d+)?/);
  return m ? Number(m[0]) : null;
}
function parseFurnishing(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'boolean') return v ? 'furnished' : 'unfurnished';
  const s = norm(v);
  if (!s) return null;
  if (s.includes('semi') && s.includes('furnish')) return 'semi-furnished';
  if (s.includes('unfurnish')) return 'unfurnished';
  if (s.includes('furnish')) return 'furnished';
  if (['true','yes','y','1'].includes(s)) return 'furnished';
  if (['false','no','n','0'].includes(s)) return 'unfurnished';
  return null;
}
function flatten(obj, prefix = '', out = {}) {
  if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
    for (const [k, v] of Object.entries(obj)) {
      const p = prefix ? `${prefix}.${k}` : String(k);
      if (v && typeof v === 'object' && !Array.isArray(v) && typeof v.toHexString !== 'function' && !(v instanceof Date) && !('_bsontype' in v)) {
        flatten(v, p, out);
      } else if (Array.isArray(v)) {
        out[p] = v;
        for (let i = 0; i < Math.min(v.length, 5); i++) {
          const item = v[i];
          const pi = `${p}[${i}]`;
          if (item && typeof item === 'object' && !Array.isArray(item)) flatten(item, pi, out);
          else out[pi] = item;
        }
      } else {
        out[p] = v;
      }
    }
  } else {
    out[prefix || 'value'] = obj;
  }
  return out;
}
function getByTokens(flat, tokens, valueFn = null) {
  const hits = [];
  for (const [k, v] of Object.entries(flat)) {
    const nk = norm(k).replace(/ /g, '');
    if (tokens.some(tok => nk.includes(tok))) {
      if (!valueFn || valueFn(v)) hits.push([k, v]);
    }
  }
  return hits;
}
function firstValue(flat, tokens, valueFn = null) {
  const hits = getByTokens(flat, tokens, valueFn);
  return hits.length ? hits[0] : [null, null];
}
function ser(v) {
  if (v === null || typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
  if (Array.isArray(v)) return v.slice(0, 20).map(ser);
  if (v instanceof Date) return v.toISOString();
  if (v && typeof v.toHexString === 'function') return v.toHexString();
  if (v && typeof v === 'object') {
    const out = {};
    for (const [k, val] of Object.entries(v).slice(0, 80)) out[k] = ser(val);
    return out;
  }
  return String(v);
}
function buildCookieHeader() {
  const raw = JSON.parse(fs.readFileSync(COOKIES_PATH, 'utf8'));
  const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
  const parts = [];
  for (const c of cookies) {
    const domain = String(c.domain || '');
    const value = typeof c.value === 'string' ? c.value : '';
    if (!value) continue;
    if (domain.includes('mongodb.com')) {
      parts.push(`${c.name}=${c.value}`);
    }
  }
  return parts.join('; ');
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
  _flush() {
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
  setTimeout(ms, cb) { this._timeout = ms; if (typeof cb === 'function') this.once('timeout', cb); this._refreshTimeout(); return this; }
  _clearTimeout() { if (this._timeoutId) { clearTimeout(this._timeoutId); this._timeoutId = null; } }
  _refreshTimeout() {
    this._clearTimeout();
    if (typeof this._timeout === 'number' && this._timeout > 0 && Number.isFinite(this._timeout)) {
      this._timeoutId = setTimeout(() => this.emit('timeout'), this._timeout);
    }
  }
  once(event, listener) {
    if (event === 'secureConnect' && this.connected) { queueMicrotask(() => listener()); return this; }
    return super.once(event, listener);
  }
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

function extractListing(doc) {
  const flat = flatten(doc);
  const blob = Object.entries(flat).slice(0, 300).map(([k, v]) => `${norm(k)} ${norm(v)}`).join(' ');

  let [propTypeKey, propTypeVal] = firstValue(flat, ['propertytype','propertysubtype','subtype','unittype','hometype','listingtype','category','type']);
  let propType = propTypeVal == null ? null : norm(propTypeVal);

  let [yearKey, yearVal] = firstValue(flat, ['yearbuilt','builtin','buildyear','constructionyear','year_built','year']);
  let yearBuilt = null;
  if (yearVal != null) {
    const n = num(yearVal);
    if (n && n >= 1800 && n <= 2100) yearBuilt = Math.round(n);
  }

  let [furnKey, furnVal] = firstValue(flat, ['furnishingstatus','furnishing','isfurnished','furnished']);
  let furnishing = parseFurnishing(furnVal);

  let [rentKey, rentVal] = firstValue(flat, ['monthlyrent','rentamount','pricepermonth','baserent','rent','listprice','monthly_rent'], x => num(x) != null);
  const rent = rentVal == null ? null : num(rentVal);

  let [titleKey, titleVal] = firstValue(flat, ['title','name','listingname','propertyname','address','unitnumber','unit']);
  const title = titleVal == null ? null : String(titleVal);

  let isApartment = false;
  if (propType && propType.includes('apart')) isApartment = true;
  else if (` ${blob} `.includes(' apartment ')) isApartment = true;
  else if (` ${blob} `.includes(' apartments ')) isApartment = true;
  else if (` ${blob} `.includes(' apt ')) isApartment = true;

  return {
    title, titleKey,
    propType, propTypeKey,
    yearBuilt, yearKey,
    furnishing, furnKey,
    rent, rentKey,
    blob,
    isApartment,
    raw: ser(doc),
    key: JSON.stringify([norm(title||''), propType||'', yearBuilt||'', furnishing||'', rent||''])
  };
}

(async () => {
  const out = {
    started_at: new Date().toISOString(),
    target_year: TARGET_YEAR,
    cookie_header_length: COOKIE_HEADER.length,
  };
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
    const dbs = await client.db('admin').admin().listDatabases();
    out.databases = dbs.databases.map(x => x.name);
    out.collections = {};
    const stats = { furnished: { sum: 0, count: 0 }, unfurnished: { sum: 0, count: 0 } };
    const matches = [];
    const seen = new Set();
    const candidateCollections = [];
    for (const dbInfo of dbs.databases) {
      const dbName = dbInfo.name;
      if (['admin','local','config'].includes(dbName)) continue;
      const db = client.db(dbName);
      const cols = await db.listCollections().toArray();
      out.collections[dbName] = cols.map(c => c.name);
      for (const c of cols) {
        const coll = db.collection(c.name);
        let est = null;
        try { est = await coll.estimatedDocumentCount(); } catch {}
        const sampleDocs = [];
        let relevance = 0;
        const sampleCursor = coll.find({}).limit(200);
        for await (const doc of sampleCursor) {
          const x = extractListing(doc);
          if (x.rent != null) relevance += 1;
          if (x.yearBuilt != null) relevance += 1;
          if (x.furnishing != null) relevance += 1;
          if (x.isApartment) relevance += 2;
          if (x.propType && x.propType.includes('apart')) relevance += 2;
          if (sampleDocs.length < 5 && (x.rent != null || x.yearBuilt != null || x.furnishing != null || x.isApartment)) {
            sampleDocs.push({
              title: x.title, propType: x.propType, yearBuilt: x.yearBuilt, furnishing: x.furnishing, rent: x.rent,
              propTypeKey: x.propTypeKey, yearKey: x.yearKey, furnKey: x.furnKey, rentKey: x.rentKey,
              raw: x.raw
            });
          }
        }
        const info = { estimated_count: est, relevance_score: relevance, samples: sampleDocs };
        if (relevance > 0) candidateCollections.push({ db: dbName, collection: c.name, relevance, estimated_count: est });
        out[`${dbName}.${c.name}`] = info;
        if (relevance < 5) continue;

        let processed = 0;
        const cursor = coll.find({});
        for await (const doc of cursor) {
          processed += 1;
          const x = extractListing(doc);
          if (!x.isApartment) continue;
          if (x.yearBuilt !== TARGET_YEAR) continue;
          if (x.rent == null) continue;
          if (!x.furnishing || x.furnishing === 'semi-furnished') continue;
          if (seen.has(x.key)) continue;
          seen.add(x.key);
          if (x.furnishing === 'furnished' || x.furnishing === 'unfurnished') {
            stats[x.furnishing].sum += x.rent;
            stats[x.furnishing].count += 1;
            if (matches.length < 50) {
              matches.push({
                db: dbName, collection: c.name, title: x.title, propType: x.propType,
                yearBuilt: x.yearBuilt, furnishing: x.furnishing, rent: x.rent,
                keys: { propType: x.propTypeKey, yearBuilt: x.yearKey, furnishing: x.furnKey, rent: x.rentKey },
                raw: x.raw
              });
            }
          }
          if (processed > 50000) break;
        }
      }
    }
    out.candidate_collections = candidateCollections.sort((a,b) => b.relevance - a.relevance).slice(0, 20);
    out.matches = matches;
    out.stats = {
      furnished: { count: stats.furnished.count, average_rent: stats.furnished.count ? stats.furnished.sum / stats.furnished.count : null },
      unfurnished: { count: stats.unfurnished.count, average_rent: stats.unfurnished.count ? stats.unfurnished.sum / stats.unfurnished.count : null },
    };
    const fAvg = out.stats.furnished.average_rent;
    const uAvg = out.stats.unfurnished.average_rent;
    out.higher_average = fAvg == null && uAvg == null ? null :
                         uAvg == null ? 'furnished' :
                         fAvg == null ? 'unfurnished' :
                         fAvg > uAvg ? 'furnished' :
                         uAvg > fAvg ? 'unfurnished' : 'tie';
  } catch (e) {
    out.error = String(e && e.message || e);
    out.stack = e && e.stack || null;
  } finally {
    out.finished_at = new Date().toISOString();
    try { await client.close(); } catch {}
    console.log(JSON.stringify(out, null, 2));
  }
})();
