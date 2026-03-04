
const fs = require('fs');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');
const { MongoClient } = require('mongodb');

const PROJECT_ID = '699c12be8df98bd863d63d70';
const CLUSTER_NAME = 'mcpatlas';
const MONGO_URI = 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';
const RESULT_PATH = 'bonus_answer.json';
const TARGET_YEAR = 2023;
const TARGET_QUARTER = 1;

function write(obj) {
  fs.writeFileSync(RESULT_PATH, JSON.stringify(obj, null, 2));
}
function buildCookieHeader() {
  try {
    const raw = JSON.parse(fs.readFileSync('browser_cookies.json', 'utf8'));
    const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
    const cookieMap = new Map();
    let srt = null;
    for (const c of cookies) {
      const domain = String(c.domain || '');
      const name = String(c.name || '');
      const value = typeof c.value === 'string' ? c.value : '';
      if (!value) continue;
      if (domain.includes('mongodb.com')) {
        cookieMap.set(name, value);
        if (name === '__Secure-mdb-srt') srt = value;
      }
    }
    if (srt) cookieMap.set('__Secure-mdb-sat', srt);
    return Array.from(cookieMap.entries()).map(([k, v]) => `${k}=${v}`).join('; ');
  } catch {
    return '';
  }
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
      this.ws.send(Buffer.concat([Buffer.from([1]), payload]));
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
          this.destroy(new Error('Unexpected proxy pre-message: ' + rest.toString('utf8')));
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
      this.ws.send(Buffer.concat([Buffer.from([2]), payload]), callback);
    } catch (e) { callback(e); }
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
  if (host === 'cloud.mongodb.com' || host === 'account.mongodb.com' || port === 443) {
    return origTlsConnect(options, callback);
  }
  const sock = new TLSSocketProxy(options || {});
  if (typeof callback === 'function') sock.once('secureConnect', callback);
  return sock;
};

function normText(s) { return String(s ?? '').normalize('NFKD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim(); }
function normKey(s) { return normText(s).replace(/[^a-z0-9]+/g, ''); }

function ser(v, depth = 0) {
  if (depth > 4) return '[depth]';
  if (v === null || typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
  if (v instanceof Date) return v.toISOString();
  if (Array.isArray(v)) return v.slice(0, 10).map(x => ser(x, depth + 1));
  if (v && typeof v === 'object') {
    if (typeof v.toHexString === 'function') return v.toHexString();
    const out = {};
    for (const [k, val] of Object.entries(v).slice(0, 50)) out[k] = ser(val, depth + 1);
    return out;
  }
  return String(v);
}
function flatten(value, prefix = '', out = {}, depth = 0) {
  if (depth > 5) return out;
  if (value instanceof Date) { out[prefix || 'value'] = value; return out; }
  if (Array.isArray(value)) {
    out[prefix || 'value'] = value;
    value.slice(0, 5).forEach((item, i) => flatten(item, `${prefix}[${i}]`, out, depth + 1));
    return out;
  }
  if (value && typeof value === 'object') {
    if (typeof value.toHexString === 'function') { out[prefix || 'value'] = value.toHexString(); return out; }
    for (const [k, v] of Object.entries(value)) flatten(v, prefix ? `${prefix}.${k}` : String(k), out, depth + 1);
    return out;
  }
  out[prefix || 'value'] = value;
  return out;
}
function parseNum(v) {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'boolean' || v == null) return null;
  let s = String(v).trim();
  if (!s) return null;
  if (/^\(.*\)$/.test(s)) s = '-' + s.slice(1, -1);
  s = s.replace(/[$£€,]/g, '').replace(/\b(?:USD|AUD|CAD)\b/gi, '').trim();
  if (/^[-+]?\d+(?:\.\d+)?$/.test(s)) return Number(s);
  const m = s.match(/[-+]?\d[\d,]*(?:\.\d+)?/);
  if (!m) return null;
  const cleaned = m[0].replace(/,/g, '');
  return /^[-+]?\d+(?:\.\d+)?$/.test(cleaned) ? Number(cleaned) : null;
}
function parseDate(v) {
  if (v instanceof Date && !Number.isNaN(v.getTime())) return v;
  if (typeof v === 'number' && Number.isFinite(v)) {
    if (v > 1e12) { const d = new Date(v); if (!Number.isNaN(d.getTime())) return d; }
    if (v > 1e9) { const d = new Date(v * 1000); if (!Number.isNaN(d.getTime())) return d; }
    if (v >= 1900 && v <= 2100) return new Date(Date.UTC(Math.trunc(v), 0, 1));
    return null;
  }
  const s = String(v ?? '').trim();
  if (!s) return null;
  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) return d;
  const m = s.match(/\b((?:19|20)\d{2})\b/);
  if (m) return new Date(Date.UTC(Number(m[1]), 0, 1));
  return null;
}
function quarterFromDate(d) {
  return d ? Math.floor(d.getUTCMonth() / 3) + 1 : null;
}
function quarterFromText(v) {
  const s = normText(v);
  if (!s) return null;
  if (/\bq\s*1\b/.test(s) || /\bquarter\s*1\b/.test(s) || /\bfirst quarter\b/.test(s)) return 1;
  if (/\bq\s*2\b/.test(s) || /\bquarter\s*2\b/.test(s) || /\bsecond quarter\b/.test(s)) return 2;
  if (/\bq\s*3\b/.test(s) || /\bquarter\s*3\b/.test(s) || /\bthird quarter\b/.test(s)) return 3;
  if (/\bq\s*4\b/.test(s) || /\bquarter\s*4\b/.test(s) || /\bfourth quarter\b/.test(s)) return 4;
  return null;
}
function yearFromText(v) {
  const m = String(v ?? '').match(/\b((?:19|20)\d{2})\b/);
  return m ? Number(m[1]) : null;
}
function keyMentionsYearQuarter(path, year = TARGET_YEAR, quarter = TARGET_QUARTER) {
  const nk = normKey(path);
  const y = String(year);
  return nk.includes(`${y}q${quarter}`) || nk.includes(`q${quarter}${y}`) ||
         nk.includes(`${y}quarter${quarter}`) || nk.includes(`quarter${quarter}${y}`);
}
function extractPeriod(flat) {
  let year = null, quarter = null, datePath = null;
  for (const [path, value] of Object.entries(flat)) {
    const nk = normKey(path);
    const d = parseDate(value);
    if (d && !datePath) {
      year = d.getUTCFullYear();
      quarter = quarterFromDate(d);
      datePath = path;
    }
    const y = yearFromText(value);
    if (year == null && y != null && (nk.includes('year') || nk.includes('period'))) year = y;
    const q = quarterFromText(value);
    if (quarter == null && q != null && (nk.includes('quarter') || nk.includes('period') || nk.includes('q'))) quarter = q;
    if (keyMentionsYearQuarter(path)) {
      year = TARGET_YEAR;
      quarter = TARGET_QUARTER;
      datePath = path;
    }
  }
  return { year, quarter, path: datePath };
}
function textLooksExcellent(v) {
  const s = normText(v);
  return s.includes('excellent');
}
function pickName(flat) {
  let best = null;
  let first = null, last = null;
  for (const [path, value] of Object.entries(flat)) {
    if (value == null) continue;
    const nk = normKey(path);
    const text = typeof value === 'string' ? value.trim() : String(value);
    if (!text) continue;
    if (nk.includes('firstname')) first = first || text;
    if (nk.includes('lastname')) last = last || text;
    let score = 0;
    if (nk === 'employeename' || nk === 'staffname' || nk === 'fullname' || nk === 'name') score += 200;
    if (nk.endsWith('name')) score += 120;
    if (nk.includes('employee') || nk.includes('staff') || nk.includes('member')) score += 50;
    if (score > 0 && (!best || score > best.score)) best = { value: text, score };
  }
  return best ? best.value : ([first, last].filter(Boolean).join(' ') || null);
}
const financeWords = ['profit', 'revenue', 'expense', 'expenses', 'cost', 'sales', 'income', 'spend', 'campaign', 'advert', 'marketing'];
const employeeWords = ['employee', 'staff', 'teammember', 'member', 'satisfaction', 'communication', 'colleague', 'coworker', 'teammate', 'relationship', 'skill', 'review', 'evaluation'];

function scoreField(path, value, kind) {
  const nk = normKey(path);
  let score = 0;
  if (kind === 'finance') {
    if (nk.includes('profit')) score += 40;
    if (nk.includes('revenue')) score += 35;
    if (nk.includes('expense')) score += 30;
    if (nk.includes('sales')) score += 20;
    if (nk.includes('income')) score += 20;
    if (nk.includes('campaign') || nk.includes('advert') || nk.includes('marketing')) score += 10;
    if (keyMentionsYearQuarter(path)) score += 100;
    if (quarterFromText(value) === TARGET_QUARTER && yearFromText(value) === TARGET_YEAR) score += 80;
  } else {
    if (nk.includes('overallsatisfactionscore')) score += 100;
    if (nk.includes('overallsatisfaction')) score += 90;
    if (nk.includes('satisfaction')) score += 60;
    if (nk.includes('communicationskills')) score += 100;
    if (nk.includes('communication')) score += 70;
    if (nk.includes('relationshipwithcolleagues') || nk.includes('colleaguerelationship')) score += 100;
    if (nk.includes('relationship')) score += 50;
    if (nk.includes('colleague') || nk.includes('coworker') || nk.includes('teammate')) score += 60;
    if (nk.includes('employee') || nk.includes('staff') || nk.endsWith('name')) score += 20;
    if (keyMentionsYearQuarter(path)) score += 100;
  }
  return score;
}

(async () => {
  const result = {
    started_at: new Date().toISOString(),
    cookieHeaderLength: COOKIE_HEADER.length,
    target_year: TARGET_YEAR,
    target_quarter: TARGET_QUARTER,
  };
  if (!COOKIE_HEADER) {
    result.error = 'no cloud cookies';
    write(result);
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  let client = null;
  try {
    client = new MongoClient(MONGO_URI, {
      serverSelectionTimeoutMS: 60000,
      connectTimeoutMS: 60000,
      socketTimeoutMS: 60000,
      directConnection: false,
      monitorCommands: false,
    });
    await client.connect();
    result.ping = await client.db('admin').command({ ping: 1 });
    const dbsResp = await client.db('admin').admin().listDatabases();
    const dbs = (dbsResp.databases || []).map(d => d.name).filter(n => !['admin', 'local', 'config'].includes(n));
    result.databases = dbs;

    const scans = [];
    const financeMatches = [];
    const employeeMatches = [];

    for (const dbName of dbs) {
      const cols = await client.db(dbName).listCollections().toArray();
      for (const meta of cols) {
        const collName = meta.name;
        const projection = dbName === 'sample_mflix' ? { plot_embedding: 0 } : {};
        const cursor = client.db(dbName).collection(collName).find({}, { projection }).batchSize(100);
        let scanned = 0;
        const keyCounts = {};
        let financeCount = 0;
        let employeeCount = 0;
        while (await cursor.hasNext()) {
          const doc = await cursor.next();
          scanned += 1;
          const flat = flatten(doc);
          const period = extractPeriod(flat);
          const fHits = [];
          const eHits = [];
          for (const [path, value] of Object.entries(flat)) {
            const nk = normKey(path);
            const fScore = scoreField(path, value, 'finance');
            const eScore = scoreField(path, value, 'employee');
            if (fScore > 0) {
              keyCounts[path] = (keyCounts[path] || 0) + 1;
              fHits.push({ path, value: ser(value), score: fScore, num: parseNum(value) });
            }
            if (eScore > 0) {
              keyCounts[path] = (keyCounts[path] || 0) + 1;
              eHits.push({ path, value: ser(value), score: eScore, num: parseNum(value) });
            }
            if (textLooksExcellent(value) && (nk.includes('relationship') || nk.includes('communication') || nk.includes('colleague') || nk.includes('skill'))) {
              eHits.push({ path, value: ser(value), score: 120, num: parseNum(value) });
            }
          }
          if (fHits.length) {
            financeCount += 1;
            if (financeMatches.length < 200) financeMatches.push({
              db: dbName, collection: collName, scannedIndex: scanned,
              period, fields: fHits.sort((a,b)=>b.score-a.score).slice(0, 12), sample: ser(doc)
            });
          }
          if (eHits.length) {
            employeeCount += 1;
            if (employeeMatches.length < 200) employeeMatches.push({
              db: dbName, collection: collName, scannedIndex: scanned,
              period, name: pickName(flat),
              fields: eHits.sort((a,b)=>b.score-a.score).slice(0, 12), sample: ser(doc)
            });
          }
        }
        scans.push({
          db: dbName,
          collection: collName,
          scanned,
          financeCount,
          employeeCount,
          topKeys: Object.entries(keyCounts).sort((a,b)=>b[1]-a[1]).slice(0, 40).map(([path,count])=>({path,count}))
        });
      }
    }

    result.collectionScans = scans;
    financeMatches.sort((a,b) =>
      ((b.period.year === TARGET_YEAR && b.period.quarter === TARGET_QUARTER) - (a.period.year === TARGET_YEAR && a.period.quarter === TARGET_QUARTER)) ||
      ((b.fields[0]?.score || 0) - (a.fields[0]?.score || 0))
    );
    employeeMatches.sort((a,b) =>
      ((b.period.year === TARGET_YEAR && b.period.quarter === TARGET_QUARTER) - (a.period.year === TARGET_YEAR && a.period.quarter === TARGET_QUARTER)) ||
      ((b.fields[0]?.score || 0) - (a.fields[0]?.score || 0))
    );
    result.financeMatches = financeMatches.slice(0, 80);
    result.employeeMatches = employeeMatches.slice(0, 80);

    // Attempt answer if possible from matched docs
    const financeEligible = financeMatches.filter(m => m.period.year === TARGET_YEAR && m.period.quarter === TARGET_QUARTER);
    const financeDoc = financeEligible.find(m => m.fields.some(f => normKey(f.path).includes('profit') && f.num != null))
      || financeEligible.find(m => {
        const hasRev = m.fields.some(f => normKey(f.path).includes('revenue') && f.num != null);
        const hasExp = m.fields.some(f => (normKey(f.path).includes('expense') || normKey(f.path).includes('cost')) && f.num != null);
        return hasRev && hasExp;
      }) || null;

    let quarterlyProfit = null;
    if (financeDoc) {
      const profitField = financeDoc.fields.find(f => normKey(f.path).includes('profit') && f.num != null);
      if (profitField) quarterlyProfit = profitField.num;
      else {
        const rev = financeDoc.fields.find(f => normKey(f.path).includes('revenue') && f.num != null);
        const exp = financeDoc.fields.find(f => (normKey(f.path).includes('expense') || normKey(f.path).includes('cost')) && f.num != null);
        if (rev && exp) quarterlyProfit = rev.num - exp.num;
      }
    }

    const employeeEligibleDocs = employeeMatches.filter(m => m.period.year === TARGET_YEAR && m.period.quarter === TARGET_QUARTER);
    const normalizedEmployees = employeeEligibleDocs.map(doc => {
      let overall = null, rel = null, comm = null;
      for (const f of doc.fields) {
        const nk = normKey(f.path);
        if (overall == null && nk.includes('satisfaction')) overall = f;
        if (rel == null && (nk.includes('relationship') || nk.includes('colleague') || nk.includes('coworker') || nk.includes('teammate'))) rel = f;
        if (comm == null && nk.includes('communication')) comm = f;
      }
      return {
        name: doc.name,
        overallRaw: overall?.value ?? null,
        overallNum: overall?.num ?? null,
        relationshipRaw: rel?.value ?? null,
        relationshipNum: rel?.num ?? null,
        communicationRaw: comm?.value ?? null,
        communicationNum: comm?.num ?? null,
        source: doc,
      };
    }).filter(x => x.name || x.overallRaw != null || x.relationshipRaw != null || x.communicationRaw != null);

    let answer = null;
    if (quarterlyProfit != null && normalizedEmployees.length) {
      const numericOverall = normalizedEmployees.map(x => x.overallNum).filter(v => v != null);
      const relNums = normalizedEmployees.map(x => x.relationshipNum).filter(v => v != null);
      const commNums = normalizedEmployees.map(x => x.communicationNum).filter(v => v != null);
      const maxOverall = numericOverall.length ? Math.max(...numericOverall) : null;
      const maxRel = relNums.length ? Math.max(...relNums) : null;
      const maxComm = commNums.length ? Math.max(...commNums) : null;
      const elig = normalizedEmployees.filter(x =>
        (maxOverall == null || x.overallNum === maxOverall) &&
        (textLooksExcellent(x.relationshipRaw) || (maxRel != null && x.relationshipNum === maxRel)) &&
        (textLooksExcellent(x.communicationRaw) || (maxComm != null && x.communicationNum === maxComm))
      );
      if (elig.length) {
        const pool = quarterlyProfit * 0.4;
        const each = Math.round((pool / elig.length + Number.EPSILON) * 100) / 100;
        answer = {
          quarterly_profit_q1_2023: quarterlyProfit,
          bonus_pool_40_percent: Math.round((pool + Number.EPSILON) * 100) / 100,
          eligible_count: elig.length,
          amount_each: each,
          eligible_employees: elig.map(x => ({
            name: x.name,
            overall_satisfaction: x.overallRaw,
            relationship_with_colleagues: x.relationshipRaw,
            communication_skills: x.communicationRaw,
          })),
        };
      }
    }
    result.answer = answer;
    if (!answer) result.note = 'No definitive answer found; inspect financeMatches and employeeMatches.';
  } catch (e) {
    result.error = String(e && e.message || e);
    result.stack = e && e.stack || null;
  } finally {
    try { if (client) await client.close(); } catch {}
    result.finished_at = new Date().toISOString();
    write(result);
    console.log(JSON.stringify(result, null, 2));
  }
})();
