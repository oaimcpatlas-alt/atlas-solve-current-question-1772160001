
const fs = require('fs');
const { MongoClient } = require('mongodb');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');

const PROJECT_ID = process.env.PROJECT_ID || '699c12be8df98bd863d63d70';
const CLUSTER_NAME = process.env.CLUSTER_NAME || 'mcpatlas';
const MONGO_URI = process.env.MONGO_PROXY_URI || 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';

function simple(v) {
  if (v == null) return v;
  if (v instanceof Date) return v.toISOString();
  if (Array.isArray(v)) return v.slice(0, 10).map(simple);
  if (typeof v === 'object') {
    if (v._bsontype === 'ObjectId' && typeof v.toString === 'function') return v.toString();
    const out = {};
    for (const [k,val] of Object.entries(v).slice(0, 80)) out[k] = simple(val);
    return out;
  }
  return v;
}

function buildCookieHeader() {
  const raw = JSON.parse(fs.readFileSync('fresh_browser_cookies.json', 'utf8'));
  const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
  const filtered = cookies.filter(c => String(c.domain || '').includes('mongodb.com') && c.value);
  // Ensure sat exists if only srt is present by mirroring srt into sat as fallback
  const map = new Map(filtered.map(c => [String(c.name), String(c.value)]));
  if (!map.has('__Secure-mdb-sat') && map.has('__Secure-mdb-srt')) {
    map.set('__Secure-mdb-sat', map.get('__Secure-mdb-srt'));
  }
  return Array.from(map.entries()).map(([k,v]) => `${k}=${v}`).join('; ');
}

const COOKIE_HEADER = buildCookieHeader();
const result = { cookieHeaderLength: COOKIE_HEADER.length };

if (!COOKIE_HEADER) {
  result.error = 'no cookies';
  fs.writeFileSync('query_employee_392_result.json', JSON.stringify(result, null, 2));
  console.log(JSON.stringify(result));
  process.exit(0);
}

function stripAccents(s) {
  return String(s ?? '').normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
}
function norm(s) {
  return stripAccents(String(s ?? '')).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}
function normKey(s) {
  return norm(s).replace(/ /g, '');
}
function toNumber(v) {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const m = v.replace(/,/g, '').match(/^-?\d+(?:\.\d+)?$/);
    if (m) return Number(m[0]);
  }
  return null;
}
function parseDate(v) {
  if (v == null) return null;
  if (v instanceof Date && !isNaN(v)) return v;
  if (typeof v === 'number' && Number.isFinite(v)) {
    if (v > 1e12) {
      const d = new Date(v);
      if (!isNaN(d)) return d;
    }
    if (v > 1e9) {
      const d = new Date(v * 1000);
      if (!isNaN(d)) return d;
    }
    return null;
  }
  const s = String(v).trim();
  if (!s) return null;
  let d = new Date(s);
  if (!isNaN(d)) return d;
  let m = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
  if (m) {
    d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
    if (!isNaN(d)) return d;
  }
  m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
  if (m) {
    d = new Date(Date.UTC(Number(m[3]), Number(m[1]) - 1, Number(m[2])));
    if (!isNaN(d)) return d;
  }
  m = s.match(/^(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})$/);
  if (m) {
    d = new Date(`${m[1]} ${m[2]} ${m[3]} UTC`);
    if (!isNaN(d)) return d;
  }
  return null;
}
function flatten(obj, prefix = '', out = {}) {
  if (obj instanceof Date) {
    out[prefix || 'value'] = obj;
    return out;
  }
  if (Array.isArray(obj)) {
    out[prefix || 'value'] = obj.slice(0, 10).map(simple);
    obj.slice(0, 5).forEach((item, i) => flatten(item, `${prefix}[${i}]`, out));
    return out;
  }
  if (obj && typeof obj === 'object') {
    if (obj._bsontype === 'ObjectId' && typeof obj.toString === 'function') {
      out[prefix || 'value'] = obj.toString();
      return out;
    }
    for (const [k, v] of Object.entries(obj)) {
      const p = prefix ? `${prefix}.${k}` : String(k);
      flatten(v, p, out);
    }
    return out;
  }
  out[prefix || 'value'] = obj;
  return out;
}

class TLSSocketProxy extends Duplex {
  constructor(options = {}) {
    super();
    this.host = options.host || options.servername;
    this.port = options.port || 27017;
    this.remoteAddress = this.host;
    this.remotePort = this.port;
    this.localAddress = 'gh-runner';
    this.localPort = Math.floor(Math.random() * 50000) + 10000;
    this.authorized = true;
    this.encrypted = true;
    this.connected = false;
    this._pendingWrites = [];
    this._timeout = 0;
    this._timeoutId = null;
    this._wsOpen = false;

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
      },
    });

    this.ws.on('open', () => {
      this._wsOpen = true;
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
        try { msg = JSON.parse(rest.toString('utf8')); } catch (e) {
          this.destroy(new Error('Invalid JSON from proxy: ' + rest.toString('utf8')));
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
    } catch (e) { callback(e); }
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
  _clearTimeout() { if (this._timeoutId) { clearTimeout(this._timeoutId); this._timeoutId = null; } }
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

function contextScore(dbName, collName) {
  const s = `${norm(dbName)} ${norm(collName)}`;
  let score = 0;
  if (/\bcustomer\b/.test(s)) score += 5;
  if (/\bfeedback\b/.test(s)) score += 5;
  if (/\bsatisfaction\b/.test(s)) score += 5;
  if (/\bsurvey\b/.test(s)) score += 4;
  if (/\breview\b/.test(s)) score += 4;
  if (/\bservice\b/.test(s)) score += 3;
  return score;
}
function employeeFieldScore(nk) {
  let s = 0;
  if (nk.includes('employee')) s += 8;
  if (nk.includes('staff')) s += 6;
  if (nk.includes('advisor')) s += 6;
  if (nk.includes('agent')) s += 5;
  if (nk.includes('representative')) s += 5;
  if (nk.includes('rep')) s += 4;
  if (nk.includes('technician') || nk.includes('tech')) s += 5;
  if (nk.endsWith('id')) s += 1;
  return s;
}
function serviceOrderScore(nk) {
  let s = 0;
  if (nk.includes('service') && nk.includes('number')) s += 10;
  if (nk.includes('service') && nk.includes('count')) s += 10;
  if (nk.includes('service') && nk.includes('sequence')) s += 10;
  if (nk.includes('service') && nk.includes('order')) s += 10;
  if (nk.includes('visit') && nk.includes('number')) s += 9;
  if (nk.includes('appointment') && nk.includes('number')) s += 8;
  if (nk.includes('ordinal')) s += 8;
  if (nk.includes('sequence')) s += 7;
  if (nk.includes('count')) s += 4;
  if (nk.includes('number')) s += 3;
  return s;
}
function dateFieldScore(nk) {
  let s = 0;
  if (nk.includes('service') && nk.includes('date')) s += 10;
  if (nk.includes('visit') && nk.includes('date')) s += 9;
  if (nk.includes('appointment') && nk.includes('date')) s += 8;
  if (nk.includes('feedback') && nk.includes('date')) s += 7;
  if (nk.endsWith('date') || nk.includes('date')) s += 5;
  if (nk.includes('time') || nk.includes('created') || nk.includes('submitted')) s += 3;
  return s;
}
function hasFeedbackSignals(flat) {
  const keys = Object.keys(flat).slice(0, 100).map(normKey).join(' ');
  return /comment|feedback|satisfaction|rating|review|survey|customer/.test(keys);
}

async function main() {
  const client = new MongoClient(MONGO_URI, {
    serverSelectionTimeoutMS: 45000,
    connectTimeoutMS: 45000,
    socketTimeoutMS: 45000,
    directConnection: false,
    monitorCommands: false,
  });
  try {
    await client.connect();
    result.connected = true;
    try { result.ping = await client.db('admin').command({ ping: 1 }); } catch {}
    const admin = client.db('admin').admin();
    const dbInfo = await admin.listDatabases();
    const dbNames = (dbInfo.databases || []).map(d => d.name).filter(n => !['admin','config','local'].includes(n));
    result.databases = dbNames;
    result.collectionSummaries = {};
    const collectionCandidates = [];

    for (const dbName of dbNames) {
      const db = client.db(dbName);
      const cols = await db.listCollections().toArray();
      result.collectionSummaries[dbName] = {};
      for (const c of cols) {
        const collName = c.name;
        const coll = db.collection(collName);
        const summary = { scanned: 0, matchedEmployee392: 0, ctxScore: contextScore(dbName, collName), examples: [] };
        result.collectionSummaries[dbName][collName] = summary;
        const employeeMatches = [];

        try {
          const cursor = coll.find({});
          for await (const doc of cursor) {
            summary.scanned += 1;
            const flat = flatten(doc);
            if (summary.scanned === 1) {
              summary.sampleKeys = Object.keys(flat).slice(0, 80);
              summary.feedbackSignals = hasFeedbackSignals(flat);
            }

            const empFieldCandidates = [];
            const orderFieldCandidates = [];
            const dateFieldCandidates = [];
            for (const [path, v] of Object.entries(flat)) {
              const nk = normKey(path);
              const num = toNumber(v);
              const eScore = employeeFieldScore(nk);
              if (eScore > 0 && num === 392) {
                empFieldCandidates.push({ path, value: simple(v), score: eScore });
              }
              const oScore = serviceOrderScore(nk);
              if (oScore > 0 && num != null) {
                orderFieldCandidates.push({ path, value: simple(v), num, score: oScore });
              }
              const d = parseDate(v);
              const dScore = dateFieldScore(nk);
              if (d && dScore > 0) {
                dateFieldCandidates.push({ path, value: simple(v), iso: d.toISOString(), ms: d.getTime(), score: dScore });
              }
            }

            if (empFieldCandidates.length) {
              summary.matchedEmployee392 += 1;
              empFieldCandidates.sort((a,b) => b.score - a.score || a.path.length - b.path.length);
              orderFieldCandidates.sort((a,b) => b.score - a.score || a.path.length - b.path.length);
              dateFieldCandidates.sort((a,b) => b.score - a.score || a.path.length - b.path.length);
              const sample = {
                empField: empFieldCandidates[0],
                orderField: orderFieldCandidates[0] || null,
                dateField: dateFieldCandidates[0] || null,
                doc: simple(doc),
              };
              employeeMatches.push(sample);
              if (summary.examples.length < 3) summary.examples.push(sample);
            }

            if (summary.scanned >= 20000) break;
          }
        } catch (e) {
          summary.error = String(e && e.message || e);
        }

        if (employeeMatches.length) {
          // group likely match quality
          const bestOrderPathCounts = {};
          const bestDatePathCounts = {};
          for (const m of employeeMatches) {
            if (m.orderField?.path) bestOrderPathCounts[m.orderField.path] = (bestOrderPathCounts[m.orderField.path] || 0) + 1;
            if (m.dateField?.path) bestDatePathCounts[m.dateField.path] = (bestDatePathCounts[m.dateField.path] || 0) + 1;
          }
          const preferredOrderPath = Object.entries(bestOrderPathCounts).sort((a,b) => b[1]-a[1])[0]?.[0] || null;
          const preferredDatePath = Object.entries(bestDatePathCounts).sort((a,b) => b[1]-a[1])[0]?.[0] || null;

          let directFourth = null;
          if (preferredOrderPath) {
            const candidates = employeeMatches.filter(m => m.orderField && m.orderField.path === preferredOrderPath);
            const hit = candidates.find(m => m.orderField.num === 4);
            if (hit) directFourth = hit;
          }

          let chronologicalFourth = null;
          if (preferredDatePath) {
            const dated = employeeMatches
              .filter(m => m.dateField && m.dateField.path === preferredDatePath)
              .sort((a,b) => a.dateField.ms - b.dateField.ms);
            if (dated.length >= 4) chronologicalFourth = dated[3];
          } else {
            const dated = employeeMatches
              .filter(m => m.dateField)
              .sort((a,b) => a.dateField.ms - b.dateField.ms);
            if (dated.length >= 4) chronologicalFourth = dated[3];
          }

          const overallScore =
            summary.ctxScore * 10 +
            (summary.feedbackSignals ? 15 : 0) +
            employeeMatches.length * 4 +
            (directFourth ? 40 : 0) +
            (chronologicalFourth ? 20 : 0);

          collectionCandidates.push({
            db: dbName,
            collection: collName,
            scanned: summary.scanned,
            feedbackSignals: !!summary.feedbackSignals,
            ctxScore: summary.ctxScore,
            matchCount: employeeMatches.length,
            preferredOrderPath,
            preferredDatePath,
            directFourth,
            chronologicalFourth,
            examples: summary.examples,
            score: overallScore,
          });
        }
      }
    }

    collectionCandidates.sort((a,b) => b.score - a.score || b.matchCount - a.matchCount);
    result.candidates = collectionCandidates.slice(0, 20);

    const best = collectionCandidates[0] || null;
    if (best) {
      const chosen = best.directFourth || best.chronologicalFourth || null;
      result.best = {
        db: best.db,
        collection: best.collection,
        score: best.score,
        matchCount: best.matchCount,
        preferredOrderPath: best.preferredOrderPath,
        preferredDatePath: best.preferredDatePath,
        chosenDate: chosen?.dateField?.iso || null,
        chosenDateField: chosen?.dateField?.path || null,
        chosenOrder: chosen?.orderField?.num || null,
        chosenOrderField: chosen?.orderField?.path || null,
        chosenDoc: chosen?.doc || null,
      };
    }
  } catch (e) {
    result.error = String(e && e.message || e);
    result.stack = e && e.stack || null;
  } finally {
    try { await client.close(); } catch {}
    fs.writeFileSync('query_employee_392_result.json', JSON.stringify(result, null, 2));
    console.log(JSON.stringify({connected: result.connected, best: result.best, error: result.error, topCandidates: result.candidates?.slice(0,3)}, null, 2));
  }
}

main();
