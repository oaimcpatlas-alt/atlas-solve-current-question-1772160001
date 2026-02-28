const { MongoClient } = require('mongodb');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');

const PROJECT_ID = process.env.PROJECT_ID || '699c12be8df98bd863d63d70';
const CLUSTER_NAME = process.env.CLUSTER_NAME || 'mcpatlas';
const COOKIE_HEADER = process.env.CLOUD_COOKIES || '';
const WS_VERSION = process.env.WS_VERSION || '1';
const DEBUG = process.env.DEBUG_PROXY === '1';

function dlog(...args) {
  if (DEBUG) console.error('[proxy]', ...args);
}

class TLSSocketProxy extends Duplex {
  constructor(options = {}) {
    super();
    this.options = options;
    this.host = options.host || options.servername;
    this.port = options.port || 27017;
    this.remoteAddress = this.host;
    this.remotePort = this.port;
    this.localAddress = 'mybrowser';
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
    url.searchParams.set('version', WS_VERSION);
    this.url = url.toString();

    const headers = {
      'User-Agent': 'Mozilla/5.0',
      'Origin': 'https://cloud.mongodb.com',
    };
    if (COOKIE_HEADER) headers['Cookie'] = COOKIE_HEADER;

    dlog('opening proxied ws', this.url);
    this.ws = new WebSocket(this.url, { headers });

    this.ws.on('open', () => {
      dlog('ws open', this.host, this.port);
      this._wsOpen = true;
      if (WS_VERSION === '2') {
        this.connected = true;
        this.emit('connect');
        this.emit('secureConnect');
        this._flushPendingWrites();
      } else {
        const meta = { port: this.port, host: this.host, clusterName: CLUSTER_NAME, ok: 1 };
        const payload = Buffer.from(JSON.stringify(meta), 'utf8');
        const frame = Buffer.concat([Buffer.from([1]), payload]);
        this.ws.send(frame);
      }
    });

    this.ws.on('message', (data) => {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (!buf || buf.length === 0) return;
      const type = buf[0];
      const rest = buf.subarray(1);
      if (type === 1) {
        const text = rest.toString('utf8');
        let msg;
        try { msg = JSON.parse(text); } catch {
          this.destroy(new Error('Invalid JSON from proxy: ' + text));
          return;
        }
        if (msg.preMessageOk === 1) {
          this.connected = true;
          this.emit('connect');
          this.emit('secureConnect');
          this._flushPendingWrites();
        } else {
          this.destroy(new Error('Unexpected pre-message JSON: ' + text));
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
          this.destroy(new Error(`WebSocket closed: code=${code} reason=${reason}`));
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
      if (WS_VERSION === '2') {
        callback(new Error('WS version 2 not implemented'));
        return;
      }
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
  setKeepAlive(enable, initialDelay) { this._keepAlive = enable; this._keepAliveDelay = initialDelay; return this; }
  setNoDelay(noDelay) { this._noDelay = noDelay; return this; }
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

function stripAccents(s) {
  return String(s ?? '').normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
}
function norm(s) {
  return stripAccents(String(s ?? '')).toLowerCase();
}
function normKey(s) {
  return norm(s).replace(/[^a-z0-9]+/g, '');
}
function parseDate(v) {
  if (v instanceof Date && !Number.isNaN(v.getTime())) return v;
  if (typeof v === 'number' && Number.isFinite(v)) {
    if (v > 1e12) {
      const d = new Date(v);
      if (!Number.isNaN(d.getTime())) return d;
    }
    if (v > 1e9) {
      const d = new Date(v * 1000);
      if (!Number.isNaN(d.getTime())) return d;
    }
    if (v >= 1000 && v <= 3000) return new Date(Date.UTC(Math.trunc(v), 0, 1));
    return null;
  }
  if (v && typeof v === 'object' && !Array.isArray(v)) {
    if ('$date' in v) return parseDate(v.$date);
    return null;
  }
  if (typeof v !== 'string') return null;
  const s = v.trim();
  if (!s || s.length > 100) return null;
  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) return d;
  const m = s.match(/((?:19|20)\d{2})[-/](\d{1,2})[-/](\d{1,2})/);
  if (m) {
    const dt = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
    if (!Number.isNaN(dt.getTime())) return dt;
  }
  return null;
}
function flatten(obj, prefix = '', out = {}) {
  if (obj == null || obj instanceof Date) {
    out[prefix || 'value'] = obj;
    return out;
  }
  if (Array.isArray(obj)) {
    out[prefix || 'value'] = obj;
    obj.slice(0, 8).forEach((item, idx) => flatten(item, `${prefix}[${idx}]`, out));
    return out;
  }
  if (typeof obj === 'object') {
    if (typeof obj.toHexString === 'function') {
      out[prefix || 'value'] = obj.toHexString();
      return out;
    }
    const entries = Object.entries(obj);
    if (!entries.length) {
      out[prefix || 'value'] = obj;
      return out;
    }
    for (const [k, v] of entries.slice(0, 120)) {
      const p = prefix ? `${prefix}.${k}` : String(k);
      if (v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Date) && typeof v.toHexString !== 'function') {
        flatten(v, p, out);
      } else {
        out[p] = v;
      }
    }
    return out;
  }
  out[prefix || 'value'] = obj;
  return out;
}
function serialize(v) {
  if (v instanceof Date) return v.toISOString();
  if (Array.isArray(v)) return v.slice(0, 20).map(serialize);
  if (v && typeof v === 'object') {
    if (typeof v.toHexString === 'function') return v.toHexString();
    const out = {};
    for (const [k, val] of Object.entries(v).slice(0, 80)) out[k] = serialize(val);
    return out;
  }
  return v;
}
function quarter(dt) {
  const month = dt.getUTCMonth() + 1;
  return Math.floor((month - 1) / 3) + 1;
}
function isDeptField(nk) {
  return /(department|dept|team|function|division|unit|area|businessunit|section|lineofbusiness)/.test(nk);
}
function isJoinField(nk) {
  if (/(termination|separation|resign|quit|depart|leave|enddate|lastday|offboard|fired|dismiss)/.test(nk)) return false;
  return /(join|joining|joindate|dateofjoining|hire|hired|hiredate|datehired|startdate|employmentdate|onboard|onboarding|commence|commencement)/.test(nk);
}
function isEmployeeField(nk) {
  return /(employee|employeeid|empid|staffid|personnel|worker|firstname|lastname|fullname|email|position|title|salary)/.test(nk);
}
function mapDept(val) {
  const s = norm(val).replace(/\s+/g, ' ').trim();
  if (!s) return null;
  if (s === 'hr' || s === 'human resources' || s === 'human resource' || s.includes('human resources')) return 'HR';
  if (s === 'sales' || s.includes('sales')) return 'Sales';
  if (
    s === 'tech' ||
    s === 'technology' ||
    s === 'it' ||
    s === 'information technology' ||
    s === 'engineering' ||
    s.includes('technology') ||
    s.includes('information technology') ||
    s.includes('software engineering') ||
    s.includes('engineering')
  ) return 'Tech';
  return null;
}
function collectionHintScore(dbName, collName) {
  const s = normKey(`${dbName} ${collName}`);
  let score = 0;
  if (/(employee|employees|staff|personnel|worker|workers|people)/.test(s)) score += 8;
  if (/(hr|humanresources)/.test(s)) score += 4;
  if (/(join|onboard|hire)/.test(s)) score += 4;
  return score;
}
function detectDoc(doc, dbName, collName) {
  const flat = flatten(doc);
  let dept = null;
  let joinDate = null;
  let employeeish = false;
  const evidence = { dept: [], dates: [], employee: [] };
  const joinKeys = new Set();
  const deptKeys = new Set();

  for (const [path, v] of Object.entries(flat)) {
    const nk = normKey(path);
    if (isEmployeeField(nk)) {
      employeeish = true;
      if (evidence.employee.length < 4) evidence.employee.push({ path, value: serialize(v) });
    }

    if (isDeptField(nk)) {
      const mapped = mapDept(v);
      if (mapped) {
        dept = dept || mapped;
        deptKeys.add(path);
        if (evidence.dept.length < 4) evidence.dept.push({ path, value: serialize(v), mapped });
      }
    }

    if (!joinDate && isJoinField(nk)) {
      const dt = parseDate(v);
      if (dt) {
        joinDate = dt;
        joinKeys.add(path);
        if (evidence.dates.length < 4) evidence.dates.push({ path, value: serialize(v), iso: dt.toISOString() });
      }
    }
  }

  return {
    dept,
    joinDate,
    employeeish,
    joinKeys: [...joinKeys],
    deptKeys: [...deptKeys],
    evidence,
    flatKeys: Object.keys(flat).slice(0, 80),
  };
}

async function scanCollectionSample(coll, dbName, collName, sampleLimit = 500) {
  const summary = {
    db: dbName,
    collection: collName,
    sampled: 0,
    estimatedCount: null,
    counts: { HR: 0, Sales: 0, Tech: 0 },
    matches: 0,
    uniqueDepts: [],
    joinFields: [],
    deptFields: [],
    employeeishDocs: 0,
    topFields: [],
    score: 0,
    samples: [],
  };
  const fieldCounts = {};
  const joinFieldCounts = {};
  const deptFieldCounts = {};
  const deptsSeen = new Set();

  try { summary.estimatedCount = await coll.estimatedDocumentCount(); } catch {}
  let cursor;
  try {
    cursor = coll.find({}).limit(sampleLimit);
  } catch (e) {
    summary.error = String(e && e.message || e);
    return summary;
  }

  for await (const doc of cursor) {
    summary.sampled += 1;
    const found = detectDoc(doc, dbName, collName);
    for (const key of found.flatKeys) fieldCounts[key] = (fieldCounts[key] || 0) + 1;
    for (const key of found.joinKeys) joinFieldCounts[key] = (joinFieldCounts[key] || 0) + 1;
    for (const key of found.deptKeys) deptFieldCounts[key] = (deptFieldCounts[key] || 0) + 1;
    if (found.employeeish) summary.employeeishDocs += 1;
    if (found.dept) deptsSeen.add(found.dept);

    if (found.dept && found.joinDate && found.joinDate.getUTCFullYear() === 2017 && quarter(found.joinDate) === 1) {
      summary.matches += 1;
      summary.counts[found.dept] += 1;
      if (summary.samples.length < 8) {
        summary.samples.push({
          date: found.joinDate.toISOString(),
          dept: found.dept,
          evidence: found.evidence,
          doc: serialize(doc),
        });
      }
    }
  }

  summary.uniqueDepts = [...deptsSeen].sort();
  summary.joinFields = Object.entries(joinFieldCounts).sort((a, b) => b[1] - a[1]).slice(0, 10);
  summary.deptFields = Object.entries(deptFieldCounts).sort((a, b) => b[1] - a[1]).slice(0, 10);
  summary.topFields = Object.entries(fieldCounts).sort((a, b) => b[1] - a[1]).slice(0, 12);

  const total = summary.counts.HR + summary.counts.Sales + summary.counts.Tech;
  summary.score =
    total * 100 +
    summary.uniqueDepts.length * 20 +
    summary.joinFields.length * 8 +
    summary.deptFields.length * 8 +
    (summary.employeeishDocs > 0 ? 10 : 0) +
    collectionHintScore(dbName, collName);

  return summary;
}

async function scanCollectionFull(coll, dbName, collName) {
  const out = {
    db: dbName,
    collection: collName,
    scanned: 0,
    counts: { HR: 0, Sales: 0, Tech: 0 },
    total: 0,
    samples: [],
    joinFieldCounts: {},
    deptFieldCounts: {},
  };
  const cursor = coll.find({});
  for await (const doc of cursor) {
    out.scanned += 1;
    const found = detectDoc(doc, dbName, collName);
    for (const key of found.joinKeys) out.joinFieldCounts[key] = (out.joinFieldCounts[key] || 0) + 1;
    for (const key of found.deptKeys) out.deptFieldCounts[key] = (out.deptFieldCounts[key] || 0) + 1;
    if (found.dept && found.joinDate && found.joinDate.getUTCFullYear() === 2017 && quarter(found.joinDate) === 1) {
      out.counts[found.dept] += 1;
      out.total += 1;
      if (out.samples.length < 15) {
        out.samples.push({
          date: found.joinDate.toISOString(),
          dept: found.dept,
          evidence: found.evidence,
          doc: serialize(doc),
        });
      }
    }
  }
  out.joinFieldCounts = Object.entries(out.joinFieldCounts).sort((a, b) => b[1] - a[1]).slice(0, 12);
  out.deptFieldCounts = Object.entries(out.deptFieldCounts).sort((a, b) => b[1] - a[1]).slice(0, 12);
  return out;
}

async function main() {
  const out = {
    target: {
      year: 2017,
      quarter: 1,
      departments: ['HR', 'Sales', 'Tech'],
    },
  };

  const uri = process.env.MONGO_URI || 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';
  out.uri = uri;
  out.cookieHeaderPresent = Boolean(COOKIE_HEADER);
  const client = new MongoClient(uri, {
    serverSelectionTimeoutMS: 30000,
    connectTimeoutMS: 30000,
    socketTimeoutMS: 30000,
    directConnection: false,
    monitorCommands: false,
  });

  try {
    await client.connect();
    out.ping = await client.db('admin').command({ ping: 1 });
    const dbInfo = await client.db().admin().listDatabases();
    const dbNames = (dbInfo.databases || []).map((x) => x.name).filter((x) => !['admin', 'local', 'config'].includes(x));
    out.databases = dbNames;
    out.discovery = [];

    for (const dbName of dbNames) {
      const db = client.db(dbName);
      const collections = await db.listCollections().toArray();
      for (const cinfo of collections) {
        const collName = cinfo.name;
        const coll = db.collection(collName);
        const summary = await scanCollectionSample(coll, dbName, collName, 500);
        out.discovery.push(summary);
      }
    }

    out.discovery.sort((a, b) => (b.score || 0) - (a.score || 0));
    out.topCandidates = out.discovery.slice(0, 8);

    const viable = out.discovery.filter((x) => (x.score || 0) > 0 || (x.matches || 0) > 0);
    if (!viable.length) {
      out.answer = null;
      out.error = 'No viable candidate collections found';
      return;
    }

    // Fully scan the top 2 candidates if close, else just top 1.
    const toScan = [viable[0]];
    if (viable[1] && (viable[1].score || 0) >= (viable[0].score || 0) - 40) {
      toScan.push(viable[1]);
    }
    out.fullScans = [];
    for (const cand of toScan) {
      const db = client.db(cand.db);
      const coll = db.collection(cand.collection);
      const full = await scanCollectionFull(coll, cand.db, cand.collection);
      full.baseScore = cand.score;
      out.fullScans.push(full);
    }

    out.fullScans.sort((a, b) => {
      if ((b.total || 0) !== (a.total || 0)) return (b.total || 0) - (a.total || 0);
      return (b.baseScore || 0) - (a.baseScore || 0);
    });

    const best = out.fullScans[0];
    out.bestCollection = {
      db: best.db,
      collection: best.collection,
      scanned: best.scanned,
      joinFieldCounts: best.joinFieldCounts,
      deptFieldCounts: best.deptFieldCounts,
    };
    out.answer = {
      HR: best.counts.HR,
      Sales: best.counts.Sales,
      Tech: best.counts.Tech,
      total: best.total,
      basedOn: `${best.db}.${best.collection}`,
    };
  } catch (e) {
    out.error = String((e && e.message) || e);
    out.stack = (e && e.stack) || null;
    process.exitCode = 1;
  } finally {
    try { await client.close(); } catch {}
    console.log(JSON.stringify(out, null, 2));
  }
}

main();
