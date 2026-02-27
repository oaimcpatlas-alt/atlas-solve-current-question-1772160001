
const fs = require('fs');
const { MongoClient } = require('mongodb');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');

const PROJECT_ID = '699c12be8df98bd863d63d70';
const CLUSTER_NAME = 'mcpatlas';
const MONGO_URI = 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';
const DEBUG = process.env.DEBUG_PROXY === '1';

function dlog(...args) {
  if (DEBUG) console.error('[proxy]', ...args);
}

function writeResult(obj) {
  fs.writeFileSync('hr_custom_result3.json', JSON.stringify(obj, null, 2));
}

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

    const headers = {
      'User-Agent': 'Mozilla/5.0',
      'Origin': 'https://cloud.mongodb.com',
      'Cookie': COOKIE_HEADER,
    };

    dlog('opening proxied ws', url.toString());
    this.ws = new WebSocket(url, { headers });

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
          this.destroy(new Error('Unexpected proxy pre-message: ' + text));
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

function stripAccents(s) {
  return String(s ?? '').normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
}
function norm(s) {
  return stripAccents(String(s ?? '')).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}
function normKey(s) {
  return norm(s).replace(/ /g, '');
}
function flatten(value, prefix = '', out = {}) {
  if (value instanceof Date) {
    out[prefix || 'value'] = value;
    return out;
  }
  if (Array.isArray(value)) {
    out[prefix || 'value'] = value;
    value.slice(0, 5).forEach((item, idx) => flatten(item, `${prefix}[${idx}]`, out));
    return out;
  }
  if (value && typeof value === 'object') {
    for (const [k, v] of Object.entries(value)) {
      const p = prefix ? `${prefix}.${k}` : String(k);
      flatten(v, p, out);
    }
    return out;
  }
  out[prefix || 'value'] = value;
  return out;
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
    return null;
  }
  if (typeof v !== 'string') return null;
  const s = v.trim();
  if (!s) return null;
  const direct = new Date(s);
  if (!Number.isNaN(direct.getTime())) return direct;
  const m = s.match(/\b(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})\b/);
  if (m) {
    const mm = Number(m[1]);
    const dd = Number(m[2]);
    const yyyy = Number(m[3]);
    if (mm >= 1 && mm <= 12 && dd >= 1 && dd <= 31) {
      const d = new Date(Date.UTC(yyyy, mm - 1, dd));
      if (!Number.isNaN(d.getTime())) return d;
    }
  }
  return null;
}
function toNumber(v) {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const m = v.replace(/,/g, '').match(/-?\d+(?:\.\d+)?/);
    if (m) return Number(m[0]);
  }
  return null;
}
function yearFromValue(v) {
  if (typeof v === 'number' && Number.isFinite(v) && v >= 1900 && v <= 3000) return Math.trunc(v);
  const s = String(v ?? '');
  const m = s.match(/\b(19|20)\d{2}\b/);
  return m ? Number(m[0]) : null;
}
function quarterFromValue(v) {
  if (typeof v === 'number' && Number.isFinite(v) && v >= 1 && v <= 4) return Math.trunc(v);
  const s = norm(v);
  if (!s) return null;
  const qm = s.match(/(?:^| )q([1-4])(?: |$)/);
  if (qm) return Number(qm[1]);
  if (s === '1' || s === 'first' || s.includes('first quarter') || s === 'quarter 1') return 1;
  if (s === '2' || s === 'second' || s.includes('second quarter') || s === 'quarter 2') return 2;
  if (s === '3' || s === 'third' || s.includes('third quarter') || s === 'quarter 3') return 3;
  if (s === '4' || s === 'fourth' || s.includes('fourth quarter') || s === 'quarter 4') return 4;
  return null;
}
function isHrValue(v) {
  const s = norm(v);
  return s === 'hr' || s === 'human resources' || s === 'human resource' || s === 'humanresources';
}
function boolish(v) {
  if (typeof v === 'boolean') return v;
  const s = norm(v);
  if (['true', 'yes', 'y', '1', 'active'].includes(s)) return true;
  if (['false', 'no', 'n', '0', 'inactive'].includes(s)) return false;
  return null;
}
function statusSuggestsLeft(v) {
  if (typeof v === 'boolean') return v === true;
  const s = norm(v);
  return [
    'left',
    'terminated',
    'resigned',
    'quit',
    'separated',
    'inactive',
    'former',
    'offboarded'
  ].includes(s) || s.includes('left company') || s.includes('termination') || s.includes('resign');
}
function dateFieldScore(nk) {
  let score = 0;
  const strong = [
    'terminationdate',
    'terminatedate',
    'dateoftermination',
    'exitdate',
    'dateleft',
    'leftdate',
    'dateofleaving',
    'leavingdate',
    'separationdate',
    'resignationdate',
    'departuredate',
    'lastworkingday',
    'lastday',
    'offboarddate',
  ];
  if (strong.some((k) => nk.includes(k))) score += 100;
  if (nk.includes('termination') && nk.includes('date')) score += 90;
  if (nk.includes('left') && nk.includes('date')) score += 85;
  if (nk.includes('exit') && nk.includes('date')) score += 85;
  if (nk.includes('leave') && nk.includes('date')) score += 80;
  if (nk.includes('enddate')) score += 60;
  if (nk.endsWith('endedon') || nk.endsWith('endon')) score += 60;
  return score;
}
function countFieldScore(nk) {
  if (nk.includes('rate') || nk.includes('percent') || nk.includes('pct')) return 0;
  let score = 0;
  if (nk.includes('employeesleft')) score += 120;
  if (nk.includes('employeeleft')) score += 120;
  if (nk.includes('leftcount') || nk.includes('countleft')) score += 110;
  if (nk.includes('departurecount') || nk.includes('departurescount')) score += 110;
  if (nk.includes('departures')) score += 100;
  if (nk.includes('leavers')) score += 100;
  if (nk.includes('terminationcount')) score += 100;
  if (nk.includes('terminationtotal')) score += 95;
  if (nk.includes('attritioncount')) score += 100;
  if (nk.includes('attritiontotal')) score += 95;
  if (nk.includes('terminations')) score += 85;
  return score;
}
function personKeyFromFlat(flat) {
  const preferred = [
    'employeeid',
    'employee_id',
    'empid',
    'staffid',
    'personid',
    'workerid',
    'userid',
    'email',
    '_id',
    'id',
  ];
  for (const key of preferred) {
    for (const [path, v] of Object.entries(flat)) {
      const nk = normKey(path);
      if (nk === key || nk.endsWith(key)) {
        if (v !== null && v !== undefined && String(v) !== '') {
          return `${nk}:${String(v)}`;
        }
      }
    }
  }
  const nameBits = [];
  for (const [path, v] of Object.entries(flat)) {
    const nk = normKey(path);
    if ((nk.endsWith('firstname') || nk.endsWith('lastname') || nk.endsWith('fullname') || nk.endsWith('name')) && typeof v === 'string') {
      nameBits.push(v.trim());
    }
  }
  if (nameBits.length) return 'name:' + nameBits.join('|');
  return null;
}

function analyzeDoc(doc, ctx) {
  const flat = flatten(doc);
  const collNorm = norm(`${ctx.dbName} ${ctx.collName}`);

  let deptFieldSeen = false;
  let deptHr = false;
  let exitDateFieldSeen = false;
  let leaveStatusFieldSeen = false;
  let yearFieldSeen = false;
  let quarterFieldSeen = false;
  let leftCountFieldSeen = false;
  let employeeSignals = 0;

  const exitDateCandidates = [];
  const yearCandidates = [];
  const quarterCandidates = [];
  const leftCountCandidates = [];
  let leaveIndicator = false;
  const evidence = [];

  for (const [path, v] of Object.entries(flat)) {
    const nk = normKey(path);

    if (nk.includes('employee') || nk.includes('staff') || nk.includes('personnel') || nk.endsWith('firstname') || nk.endsWith('lastname') || nk.endsWith('fullname')) {
      employeeSignals += 1;
    }

    if (nk.includes('department') || nk === 'dept' || nk.endsWith('dept')) {
      deptFieldSeen = true;
      if (isHrValue(v)) {
        deptHr = true;
        if (evidence.length < 10) evidence.push({ type: 'department', path, value: v });
      }
    }

    let dfs = dateFieldScore(nk);
    if (dfs === 0 && (nk === 'left' || nk === 'terminated' || nk === 'resigned' || nk.endsWith('left') || nk.endsWith('terminated') || nk.endsWith('resigned'))) {
      dfs = 70;
    }
    if (dfs > 0) {
      exitDateFieldSeen = true;
      const d = parseDate(v);
      if (d) {
        exitDateCandidates.push({ path, value: v, score: dfs, iso: d.toISOString(), date: d });
      }
    }

    if (
      nk === 'year' || nk.endsWith('year') || nk.includes('fiscalyear') ||
      nk === 'calendaryear' || nk.endsWith('calendaryear')
    ) {
      yearFieldSeen = true;
      const y = yearFromValue(v);
      if (y !== null) yearCandidates.push({ path, value: v, score: nk === 'year' ? 20 : 10, year: y });
    }

    if (
      nk === 'quarter' || nk.endsWith('quarter') || nk === 'qtr' || nk.endsWith('qtr')
    ) {
      quarterFieldSeen = true;
      const q = quarterFromValue(v);
      if (q !== null) quarterCandidates.push({ path, value: v, score: nk === 'quarter' ? 20 : 10, quarter: q });
    }

    const cfs = countFieldScore(nk);
    if (cfs > 0) {
      leftCountFieldSeen = true;
      const n = toNumber(v);
      if (n !== null) leftCountCandidates.push({ path, value: v, score: cfs, count: n });
    }

    if (
      nk.includes('status') || nk.includes('state') || nk.endsWith('left') ||
      nk.includes('terminated') || nk.includes('resigned') || nk.includes('attrition') ||
      nk === 'left' || nk === 'terminated'
    ) {
      leaveStatusFieldSeen = true;
      const b = boolish(v);
      if (b === true || statusSuggestsLeft(v)) {
        leaveIndicator = true;
        if (evidence.length < 10) evidence.push({ type: 'leaveFlag', path, value: v });
      }
    }
  }

  if (/employee|employees|staff|personnel|worker|workers|attrition|turnover|termination|separation|offboard|hr/.test(collNorm)) {
    employeeSignals += 1;
  }

  exitDateCandidates.sort((a, b) => b.score - a.score);
  yearCandidates.sort((a, b) => b.score - a.score);
  quarterCandidates.sort((a, b) => b.score - a.score);
  leftCountCandidates.sort((a, b) => b.score - a.score);

  const bestExitDate = exitDateCandidates[0] || null;
  const bestYear = yearCandidates[0] || null;
  const bestQuarter = quarterCandidates[0] || null;
  const bestCount = leftCountCandidates[0] || null;

  let period = null;
  if (bestExitDate) {
    const d = bestExitDate.date;
    period = {
      source: 'exitDate',
      path: bestExitDate.path,
      year: d.getUTCFullYear(),
      quarter: Math.floor(d.getUTCMonth() / 3) + 1,
      iso: bestExitDate.iso,
    };
  } else if (bestYear && bestQuarter) {
    period = {
      source: 'yearQuarter',
      yearPath: bestYear.path,
      quarterPath: bestQuarter.path,
      year: bestYear.year,
      quarter: bestQuarter.quarter,
    };
  }

  const quantity = bestCount ? bestCount.count : (bestExitDate || leaveIndicator ? 1 : 0);
  const isDeparture = !!bestCount || !!bestExitDate || leaveIndicator;

  if (bestExitDate && evidence.length < 10) evidence.push({ type: 'exitDate', path: bestExitDate.path, value: bestExitDate.value, iso: bestExitDate.iso });
  if (bestCount && evidence.length < 10) evidence.push({ type: 'leftCount', path: bestCount.path, value: bestCount.value, count: bestCount.count });
  if (bestYear && bestQuarter && evidence.length < 10) {
    evidence.push({ type: 'yearQuarter', yearPath: bestYear.path, year: bestYear.year, quarterPath: bestQuarter.path, quarter: bestQuarter.quarter });
  }

  return {
    deptFieldSeen,
    deptHr,
    exitDateFieldSeen,
    leaveStatusFieldSeen,
    yearFieldSeen,
    quarterFieldSeen,
    leftCountFieldSeen,
    employeeSignals,
    period,
    quantity,
    isDeparture,
    personKey: personKeyFromFlat(flat),
    evidence,
  };
}

async function inspectCollection(col, dbName, collName) {
  const info = {
    sampleDocs: 0,
    sampleFlags: {
      deptFieldSeen: false,
      exitDateFieldSeen: false,
      leaveStatusFieldSeen: false,
      yearFieldSeen: false,
      quarterFieldSeen: false,
      leftCountFieldSeen: false,
      employeeSignals: 0,
    },
    sampleMatches: [],
    sampleHrDocs: 0,
  };

  const cursor = col.find({}, { limit: 60 });
  for await (const doc of cursor) {
    info.sampleDocs += 1;
    const analyzed = analyzeDoc(doc, { dbName, collName });
    if (analyzed.deptFieldSeen) info.sampleFlags.deptFieldSeen = true;
    if (analyzed.exitDateFieldSeen) info.sampleFlags.exitDateFieldSeen = true;
    if (analyzed.leaveStatusFieldSeen) info.sampleFlags.leaveStatusFieldSeen = true;
    if (analyzed.yearFieldSeen) info.sampleFlags.yearFieldSeen = true;
    if (analyzed.quarterFieldSeen) info.sampleFlags.quarterFieldSeen = true;
    if (analyzed.leftCountFieldSeen) info.sampleFlags.leftCountFieldSeen = true;
    if (analyzed.employeeSignals) info.sampleFlags.employeeSignals += analyzed.employeeSignals;
    if (analyzed.deptHr) info.sampleHrDocs += 1;
    if (analyzed.deptHr && analyzed.period && analyzed.period.year === 2022 && (analyzed.period.quarter === 1 || analyzed.period.quarter === 3)) {
      if (info.sampleMatches.length < 3) {
        info.sampleMatches.push({
          quantity: analyzed.quantity,
          period: analyzed.period,
          evidence: analyzed.evidence,
        });
      }
    }
  }

  let score = 0;
  if (info.sampleFlags.deptFieldSeen) score += 4;
  if (info.sampleFlags.exitDateFieldSeen) score += 4;
  if (info.sampleFlags.yearFieldSeen && info.sampleFlags.quarterFieldSeen) score += 3;
  if (info.sampleFlags.leftCountFieldSeen) score += 3;
  if (info.sampleFlags.leaveStatusFieldSeen) score += 2;
  if (info.sampleFlags.employeeSignals > 0) score += 2;
  if (/employee|employees|staff|personnel|worker|workers|attrition|turnover|termination|separation|offboard|hr/.test(norm(`${dbName} ${collName}`))) score += 2;
  if (info.sampleHrDocs > 0) score += 1;
  info.sampleScore = score;
  return info;
}

async function fullScanCollection(col, dbName, collName) {
  const out = {
    scanned: 0,
    q1: 0,
    q3: 0,
    total: 0,
    mode: null,
    sampleMatches: [],
    uniquePersonsCounted: 0,
  };
  const seenPersons = new Set();
  const cursor = col.find({});
  for await (const doc of cursor) {
    out.scanned += 1;
    const analyzed = analyzeDoc(doc, { dbName, collName });
    if (!analyzed.deptHr || !analyzed.isDeparture || !analyzed.period) continue;
    if (analyzed.period.year !== 2022) continue;
    if (analyzed.period.quarter !== 1 && analyzed.period.quarter !== 3) continue;
    let qty = analyzed.quantity;
    if (!(typeof qty === 'number' && Number.isFinite(qty) && qty > 0)) continue;

    if (qty === 1 && analyzed.personKey) {
      if (seenPersons.has(analyzed.personKey)) continue;
      seenPersons.add(analyzed.personKey);
      out.uniquePersonsCounted = seenPersons.size;
    }

    if (analyzed.quantity !== 1) out.mode = out.mode || 'aggregate';
    else out.mode = out.mode || 'rowCount';

    if (analyzed.period.quarter === 1) out.q1 += qty;
    if (analyzed.period.quarter === 3) out.q3 += qty;
    out.total += qty;

    if (out.sampleMatches.length < 5) {
      out.sampleMatches.push({
        quantity: qty,
        period: analyzed.period,
        personKey: analyzed.personKey,
        evidence: analyzed.evidence,
      });
    }
  }
  return out;
}

async function main() {
  const result = {
    cookieHeaderLength: COOKIE_HEADER.length,
    mongoUri: MONGO_URI,
    databases: [],
    candidates: [],
    collections: {},
    answer: null,
  };

  const client = new MongoClient(MONGO_URI, {
    serverSelectionTimeoutMS: 30000,
    connectTimeoutMS: 30000,
    socketTimeoutMS: 30000,
    directConnection: false,
    monitorCommands: false,
  });

  try {
    await client.connect();
    result.ping = await client.db('admin').command({ ping: 1 });

    const dbsResp = await client.db('admin').admin().listDatabases();
    const dbNames = (dbsResp.databases || []).map((d) => d.name).filter((n) => !['admin', 'local', 'config'].includes(n));
    result.databases = dbNames;

    for (const dbName of dbNames) {
      result.collections[dbName] = {};
      const db = client.db(dbName);
      let collections = [];
      try {
        collections = await db.listCollections().toArray();
      } catch (e) {
        result.collections[dbName]._error = String(e && e.message || e);
        continue;
      }

      for (const c of collections) {
        const collName = c.name;
        const col = db.collection(collName);
        let inspect;
        try {
          inspect = await inspectCollection(col, dbName, collName);
        } catch (e) {
          result.collections[dbName][collName] = { inspectError: String(e && e.message || e) };
          continue;
        }
        result.collections[dbName][collName] = { inspect };
        result.candidates.push({ dbName, collName, score: inspect.sampleScore, inspect });
      }
    }

    result.candidates.sort((a, b) => b.score - a.score);
    const selected = result.candidates.filter((c) => c.score >= 6).slice(0, 6);
    result.selected = selected.map((c) => ({ dbName: c.dbName, collName: c.collName, score: c.score }));

    const positive = [];
    for (const c of selected) {
      const db = client.db(c.dbName);
      const col = db.collection(c.collName);
      try {
        const scan = await fullScanCollection(col, c.dbName, c.collName);
        result.collections[c.dbName][c.collName].scan = scan;
        if (scan.total > 0) positive.push({ dbName: c.dbName, collName: c.collName, score: c.score, scan });
      } catch (e) {
        result.collections[c.dbName][c.collName].scanError = String(e && e.message || e);
      }
      writeResult(result);
    }

    positive.sort((a, b) => (b.scan.total - a.scan.total) || (b.score - a.score));
    result.positive = positive.map((p) => ({
      dbName: p.dbName,
      collName: p.collName,
      score: p.score,
      q1: p.scan.q1,
      q3: p.scan.q3,
      total: p.scan.total,
      mode: p.scan.mode,
      scanned: p.scan.scanned,
    }));

    if (positive.length) {
      const best = positive[0];
      result.answer = {
        bestCollection: { dbName: best.dbName, collName: best.collName, score: best.score, mode: best.scan.mode },
        q1: best.scan.q1,
        q3: best.scan.q3,
        total: best.scan.total,
        note: 'Best single collection chosen by highest positive total, then score.',
      };
    } else {
      result.answer = {
        q1: 0,
        q3: 0,
        total: 0,
        note: 'No positive matches found in scanned candidate collections.',
      };
    }
  } catch (e) {
    result.error = String(e && e.message || e);
    result.stack = e && e.stack || null;
  } finally {
    try { await client.close(); } catch {}
    writeResult(result);
  }
}

main();
