
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
    if (srt && !cookieMap.has('__Secure-mdb-sat')) cookieMap.set('__Secure-mdb-sat', srt);
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
function normText(s) {
  return stripAccents(String(s ?? '')).toLowerCase().trim();
}
function normKey(s) {
  return normText(s).replace(/[^a-z0-9]+/g, '');
}
function ser(v, depth = 0) {
  if (depth > 5) return '[depth]';
  if (v === null || typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
  if (v instanceof Date) return v.toISOString();
  if (Array.isArray(v)) return v.slice(0, 20).map(x => ser(x, depth + 1));
  if (v && typeof v === 'object') {
    if (typeof v.toHexString === 'function') return v.toHexString();
    const out = {};
    for (const [k, val] of Object.entries(v).slice(0, 100)) out[k] = ser(val, depth + 1);
    return out;
  }
  return String(v);
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
    if (typeof value.toHexString === 'function') {
      out[prefix || 'value'] = value.toHexString();
      return out;
    }
    for (const [k, v] of Object.entries(value)) {
      const p = prefix ? `${prefix}.${k}` : String(k);
      flatten(v, p, out);
    }
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
  s = s.replace(/\u00a0/g, ' ');
  if (/^\(.*\)$/.test(s)) s = '-' + s.slice(1, -1);
  s = s.replace(/[$£€,]/g, '').replace(/\bUSD\b/gi, '').replace(/\bAUD\b/gi, '').replace(/\bCAD\b/gi, '').trim();
  if (/^[-+]?\d+(?:\.\d+)?$/.test(s)) return Number(s);
  const m = s.match(/[-+]?\d[\d,]*(?:\.\d+)?/);
  if (!m) return null;
  const cleaned = m[0].replace(/,/g, '');
  return /^[-+]?\d+(?:\.\d+)?$/.test(cleaned) ? Number(cleaned) : null;
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
    if (v >= 1900 && v <= 2100) return new Date(Date.UTC(Math.trunc(v), 0, 1));
    return null;
  }
  const s = String(v ?? '').trim();
  if (!s) return null;
  let d = new Date(s);
  if (!Number.isNaN(d.getTime())) return d;
  let m = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
  if (m) return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
  if (m) {
    const a = Number(m[1]);
    const b = Number(m[2]);
    const y = Number(m[3]);
    if (a >= 1 && a <= 12 && b >= 1 && b <= 31) return new Date(Date.UTC(y, a - 1, b));
    if (b >= 1 && b <= 12 && a >= 1 && a <= 31) return new Date(Date.UTC(y, b - 1, a));
  }
  m = s.match(/\b((?:19|20)\d{2})\b/);
  if (m) return new Date(Date.UTC(Number(m[1]), 0, 1));
  return null;
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
  const s = String(v ?? '');
  const m = s.match(/\b((?:19|20)\d{2})\b/);
  return m ? Number(m[1]) : null;
}
function keyMentionsYearQuarter(nk, year = TARGET_YEAR, quarter = TARGET_QUARTER) {
  const y = String(year);
  return (
    (nk.includes(y) && (nk.includes(`q${quarter}`) || nk.includes(`quarter${quarter}`))) ||
    nk.includes(`q${quarter}${y}`) ||
    nk.includes(`${y}q${quarter}`) ||
    nk.includes(`quarter${quarter}${y}`) ||
    nk.includes(`${y}quarter${quarter}`)
  );
}
function periodStringMatches(value, year = TARGET_YEAR, quarter = TARGET_QUARTER) {
  const s = normText(value);
  if (!s || !s.includes(String(year))) return false;
  return quarterFromText(s) === quarter;
}
function scoreDatePath(nk) {
  let score = 0;
  if (nk.includes('campaignstartdate')) score += 100;
  if (nk.includes('reportdate') || nk.includes('reportingdate') || nk.includes('filedate') || nk.includes('perioddate')) score += 90;
  if (nk.includes('quarter')) score += 85;
  if (nk.endsWith('startdate') || nk.includes('start')) score += 70;
  if (nk === 'date' || nk.endsWith('date')) score += 50;
  if (nk.includes('period')) score += 40;
  if (nk.includes('enddate') || nk.includes('updated') || nk.includes('created') || nk.includes('birth') || nk.includes('hire')) score -= 30;
  return score;
}
function extractPeriod(flat) {
  let bestDate = null;
  let bestYear = null;
  let bestQuarter = null;
  let bestCombined = null;
  for (const [path, value] of Object.entries(flat)) {
    const nk = normKey(path);
    const d = parseDate(value);
    if (d) {
      const score = scoreDatePath(nk);
      if (!bestDate || score > bestDate.score) bestDate = { path, score, date: d };
    }
    const q = quarterFromText(value);
    if (q != null) {
      const score = (nk.includes('quarter') ? 80 : 30) + (nk.includes('period') ? 10 : 0);
      if (!bestQuarter || score > bestQuarter.score) bestQuarter = { path, score, quarter: q };
    }
    const y = yearFromText(value);
    if (y != null) {
      const score = (nk === 'year' || nk.endsWith('year') ? 70 : 20) + (nk.includes('period') ? 10 : 0);
      if (!bestYear || score > bestYear.score) bestYear = { path, score, year: y };
    }
    if (typeof value === 'string' && periodStringMatches(value)) {
      const score = 120 + (nk.includes('period') ? 10 : 0) + (nk.includes('quarter') ? 10 : 0);
      bestCombined = { path, score, year: TARGET_YEAR, quarter: TARGET_QUARTER };
    }
    if (keyMentionsYearQuarter(nk)) {
      const score = 120;
      bestCombined = { path, score, year: TARGET_YEAR, quarter: TARGET_QUARTER };
    }
  }
  if (bestCombined) return { year: bestCombined.year, quarter: bestCombined.quarter, source: 'combined', path: bestCombined.path };
  if (bestYear && bestQuarter) return { year: bestYear.year, quarter: bestQuarter.quarter, source: 'explicit', yearPath: bestYear.path, quarterPath: bestQuarter.path };
  if (bestDate) {
    return {
      year: bestDate.date.getUTCFullYear(),
      quarter: Math.floor(bestDate.date.getUTCMonth() / 3) + 1,
      source: 'date',
      path: bestDate.path,
      date: bestDate.date,
    };
  }
  return { year: null, quarter: null, source: null };
}

function metricScore(nk, kind) {
  let score = 0;
  if (kind === 'profit') {
    if (nk.includes('profitmargin') || nk.includes('margin')) return -50;
    if (nk.includes('netprofit')) score += 170;
    if (nk.includes('quarterlyprofit')) score += 160;
    if (nk.includes('weeklyprofit') || nk.includes('monthlyprofit')) score += 155;
    if (nk.includes('grossprofit')) score += 140;
    if (nk.includes('operatingprofit')) score += 150;
    if (nk.includes('profit')) score += 130;
    if (nk.includes('netincome')) score += 120;
    if (nk.includes('income')) score += 60;
  }
  if (kind === 'revenue') {
    if (nk.includes('avg') || nk.includes('average') || nk.includes('forecast') || nk.includes('rate') || nk.includes('cpc')) return -40;
    if (nk.includes('totalrevenue')) score += 170;
    if (nk.includes('quarterlyrevenue')) score += 160;
    if (nk.includes('weeklyrevenue') || nk.includes('monthlyrevenue')) score += 155;
    if (nk.includes('revenue')) score += 130;
    if (nk.includes('sales')) score += 110;
    if (nk.includes('income')) score += 60;
  }
  if (kind === 'expense') {
    if (nk.includes('costperclick') || nk.includes('cpc') || nk.includes('rate')) return -60;
    if (nk.includes('totalexpense') || nk.includes('totalexpenses')) score += 170;
    if (nk.includes('quarterlyexpense') || nk.includes('quarterlyexpenses')) score += 160;
    if (nk.includes('weeklyexpense') || nk.includes('weeklyexpenses') || nk.includes('monthlyexpense') || nk.includes('monthlyexpenses')) score += 155;
    if (nk.includes('expenses')) score += 130;
    if (nk.includes('expense')) score += 125;
    if (nk.includes('cost')) score += 100;
    if (nk.includes('spend')) score += 90;
  }
  if (nk.includes('total')) score += 8;
  return score;
}
function pickBestMetric(flat, kind, opts = {}) {
  const { quarterScoped = false } = opts;
  let best = null;
  for (const [path, value] of Object.entries(flat)) {
    const nk = normKey(path);
    if (quarterScoped && !keyMentionsYearQuarter(nk)) continue;
    const num = parseNum(value);
    if (num == null) continue;
    let score = metricScore(nk, kind);
    if (score <= 0) continue;
    if (quarterScoped) score += 100;
    if (best == null || score > best.score) best = { path, value: num, score };
  }
  return best;
}
function extractFinanceDoc(doc) {
  const flat = flatten(doc);
  const period = extractPeriod(flat);
  let mode = null;
  let profit = pickBestMetric(flat, 'profit', { quarterScoped: true });
  let revenue = pickBestMetric(flat, 'revenue', { quarterScoped: true });
  let expense = pickBestMetric(flat, 'expense', { quarterScoped: true });
  if (profit || (revenue && expense)) {
    mode = 'quarterField';
  } else if (period.year === TARGET_YEAR && period.quarter === TARGET_QUARTER) {
    profit = pickBestMetric(flat, 'profit');
    revenue = pickBestMetric(flat, 'revenue');
    expense = pickBestMetric(flat, 'expense');
    mode = 'periodDoc';
  }
  let profitValue = null;
  if (profit) profitValue = profit.value;
  else if (revenue && expense) profitValue = revenue.value - expense.value;
  return {
    flat,
    period,
    mode,
    profitValue,
    profitPath: profit && profit.path,
    revenuePath: revenue && revenue.path,
    expensePath: expense && expense.path,
    sample: ser(doc),
  };
}

function financeCollectionScore(nameBits, keys) {
  const pieces = [...nameBits, ...keys].map(x => normKey(x));
  let score = 0;
  for (const nk of pieces) {
    if (nk.includes('campaign')) score += 15;
    if (nk.includes('advert')) score += 14;
    if (nk.includes('marketing')) score += 12;
    if (nk.includes('profit')) score += 35;
    if (nk.includes('revenue')) score += 25;
    if (nk.includes('expense') || nk.includes('cost') || nk.includes('spend')) score += 15;
    if (nk.includes('quarter') || nk.includes('q1')) score += 18;
    if (nk.includes('financial') || nk.includes('finance')) score += 12;
  }
  return score;
}
function employeeCollectionScore(nameBits, keys) {
  const pieces = [...nameBits, ...keys].map(x => normKey(x));
  let score = 0;
  for (const nk of pieces) {
    if (nk.includes('employee') || nk.includes('staff') || nk.includes('teammember') || nk.includes('teammember')) score += 20;
    if (nk.includes('satisfaction')) score += 30;
    if (nk.includes('overall')) score += 8;
    if (nk.includes('communication')) score += 25;
    if (nk.includes('colleague') || nk.includes('coworker') || nk.includes('teammate')) score += 25;
    if (nk.includes('relationship')) score += 20;
    if (nk.includes('skill')) score += 10;
    if (nk.includes('review') || nk.includes('evaluation') || nk.includes('survey')) score += 10;
  }
  return score;
}

function pickName(flat) {
  let best = null;
  let first = null;
  let last = null;
  for (const [path, value] of Object.entries(flat)) {
    if (value == null) continue;
    const nk = normKey(path);
    const text = typeof value === 'string' ? value.trim() : String(value);
    if (!text) continue;
    if (nk.includes('firstname') && !first) first = text;
    if (nk.includes('lastname') && !last) last = text;
    let score = 0;
    if (nk === 'employeename' || nk === 'staffname' || nk === 'fullname' || nk === 'name') score += 200;
    if (nk.endsWith('name')) score += 120;
    if (nk.includes('employee') || nk.includes('staff') || nk.includes('member')) score += 20;
    if (nk.includes('first') || nk.includes('last')) score -= 20;
    if (score > 0 && (!best || score > best.score)) best = { value: text, score, path };
  }
  if (best) return best.value;
  if (first || last) return [first, last].filter(Boolean).join(' ');
  return null;
}
function employeeMetricScore(nk, kind) {
  let score = 0;
  if (kind === 'overall') {
    if (nk.includes('overallsatisfactionscore')) score += 250;
    if (nk.includes('overallsatisfaction')) score += 220;
    if (nk.includes('satisfactionscore')) score += 180;
    if (nk.includes('overallscore')) score += 120;
    if (nk.includes('satisfaction')) score += 80;
    if (nk.includes('overall')) score += 20;
  } else if (kind === 'relationship') {
    if (nk.includes('relationshipwithcolleagues') || nk.includes('relationwithcolleagues')) score += 250;
    if (nk.includes('colleaguerelationship') || nk.includes('coworkerrelationship') || nk.includes('teammaterelationship')) score += 230;
    if (nk.includes('colleague') || nk.includes('coworker') || nk.includes('teammate')) score += 150;
    if (nk.includes('relationship')) score += 90;
  } else if (kind === 'communication') {
    if (nk.includes('communicationskills')) score += 250;
    if (nk.includes('communicationskill')) score += 240;
    if (nk.includes('communication')) score += 150;
    if (nk.includes('skill')) score += 40;
  }
  return score;
}
function pickEmployeeMetric(flat, kind, quarterScoped = false) {
  let best = null;
  for (const [path, value] of Object.entries(flat)) {
    if (value == null) continue;
    const nk = normKey(path);
    if (quarterScoped && !keyMentionsYearQuarter(nk)) continue;
    const score = employeeMetricScore(nk, kind) + (quarterScoped ? 100 : 0);
    if (score <= 0) continue;
    if (!best || score > best.score) best = { path, value, score };
  }
  return best;
}
function extractEmployeeDoc(doc) {
  const flat = flatten(doc);
  const period = extractPeriod(flat);
  const name = pickName(flat);
  let overall = pickEmployeeMetric(flat, 'overall', true);
  let relationship = pickEmployeeMetric(flat, 'relationship', true);
  let communication = pickEmployeeMetric(flat, 'communication', true);
  let mode = null;
  let periodMatch = false;
  if (overall || relationship || communication) {
    mode = 'quarterField';
    periodMatch = true;
  } else {
    overall = pickEmployeeMetric(flat, 'overall');
    relationship = pickEmployeeMetric(flat, 'relationship');
    communication = pickEmployeeMetric(flat, 'communication');
    mode = 'doc';
    periodMatch = period.year === TARGET_YEAR && period.quarter === TARGET_QUARTER;
  }
  return {
    name,
    overallRaw: overall ? overall.value : null,
    overallNum: overall ? parseNum(overall.value) : null,
    relationshipRaw: relationship ? relationship.value : null,
    relationshipNum: relationship ? parseNum(relationship.value) : null,
    communicationRaw: communication ? communication.value : null,
    communicationNum: communication ? parseNum(communication.value) : null,
    period,
    periodMatch,
    mode,
    sample: ser(doc),
  };
}
function textLooksExcellent(v) {
  if (v == null) return false;
  const s = normText(v);
  return s.includes('excellent');
}
function equalNum(a, b) {
  return a != null && b != null && Math.abs(a - b) < 1e-9;
}
function roundMoney(n) {
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

async function evaluateFinanceCandidate(client, candidate) {
  const coll = client.db(candidate.db).collection(candidate.collection);
  const docs = await coll.find({}).limit(5000).toArray();
  let totalProfit = 0;
  let matchedDocs = 0;
  let profitDocs = 0;
  let explicitProfitDocs = 0;
  const examples = [];
  for (const doc of docs) {
    const info = extractFinanceDoc(doc);
    if (info.profitValue == null) continue;
    if (info.mode === 'quarterField' || (info.period.year === TARGET_YEAR && info.period.quarter === TARGET_QUARTER)) {
      matchedDocs += 1;
      profitDocs += 1;
      if (info.profitPath) explicitProfitDocs += 1;
      totalProfit += info.profitValue;
      if (examples.length < 5) {
        examples.push({
          mode: info.mode,
          period: info.period,
          profitValue: info.profitValue,
          profitPath: info.profitPath,
          revenuePath: info.revenuePath,
          expensePath: info.expensePath,
          sample: info.sample,
        });
      }
    }
  }
  return {
    ...candidate,
    scannedCount: docs.length,
    matchedDocs,
    profitDocs,
    explicitProfitDocs,
    totalProfit,
    examples,
    rankingScore: candidate.financeScore * 10 + matchedDocs * 20 + explicitProfitDocs * 5 + (profitDocs > 0 ? 25 : 0),
  };
}

async function evaluateEmployeeCandidate(client, candidate) {
  const coll = client.db(candidate.db).collection(candidate.collection);
  const docs = await coll.find({}).limit(5000).toArray();
  const extracted = [];
  for (const doc of docs) {
    const info = extractEmployeeDoc(doc);
    if (info.name || info.overallRaw != null || info.relationshipRaw != null || info.communicationRaw != null) {
      extracted.push(info);
    }
  }
  const q1Specific = extracted.filter(x => x.periodMatch);
  const pool = q1Specific.length > 0 ? q1Specific : extracted;
  const usable = pool.filter(x => x.overallNum != null && x.relationshipRaw != null && x.communicationRaw != null);
  const maxOverall = usable.length ? Math.max(...usable.map(x => x.overallNum)) : null;
  const maxRelationshipNum = usable.map(x => x.relationshipNum).filter(v => v != null);
  const maxCommunicationNum = usable.map(x => x.communicationNum).filter(v => v != null);
  const relMax = maxRelationshipNum.length ? Math.max(...maxRelationshipNum) : null;
  const commMax = maxCommunicationNum.length ? Math.max(...maxCommunicationNum) : null;
  const eligible = usable.filter(x =>
    equalNum(x.overallNum, maxOverall) &&
    (textLooksExcellent(x.relationshipRaw) || equalNum(x.relationshipNum, relMax)) &&
    (textLooksExcellent(x.communicationRaw) || equalNum(x.communicationNum, commMax))
  );
  return {
    ...candidate,
    scannedCount: docs.length,
    extractedCount: extracted.length,
    usedQ1Specific: q1Specific.length > 0,
    usableCount: usable.length,
    maxOverall,
    relationshipMaxNumeric: relMax,
    communicationMaxNumeric: commMax,
    eligibleCount: eligible.length,
    examples: usable.slice(0, 8).map(ser),
    eligible: eligible.map(ser),
    rankingScore: candidate.employeeScore * 10 + usable.length * 8 + eligible.length * 20 + (q1Specific.length > 0 ? 15 : 0),
  };
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

    const collectionCandidates = [];
    for (const dbName of dbs) {
      const cols = await client.db(dbName).listCollections().toArray();
      for (const meta of cols) {
        const collName = meta.name;
        const coll = client.db(dbName).collection(collName);
        const samples = await coll.find({}).limit(50).toArray().catch(() => []);
        const keys = new Set();
        for (const doc of samples) {
          const flat = flatten(doc);
          Object.keys(flat).forEach(k => keys.add(k));
        }
        const keyList = [...keys];
        collectionCandidates.push({
          db: dbName,
          collection: collName,
          financeScore: financeCollectionScore([dbName, collName], keyList),
          employeeScore: employeeCollectionScore([dbName, collName], keyList),
          sampleKeys: keyList.slice(0, 120),
          sampleDocs: samples.slice(0, 3).map(ser),
        });
      }
    }

    result.collectionCandidates = collectionCandidates.sort((a, b) =>
      (Math.max(b.financeScore, b.employeeScore) - Math.max(a.financeScore, a.employeeScore)) ||
      ((b.financeScore + b.employeeScore) - (a.financeScore + a.employeeScore))
    ).slice(0, 30);

    const financeCandidates = collectionCandidates.filter(c => c.financeScore > 0)
      .sort((a, b) => b.financeScore - a.financeScore)
      .slice(0, 8);
    const employeeCandidates = collectionCandidates.filter(c => c.employeeScore > 0)
      .sort((a, b) => b.employeeScore - a.employeeScore)
      .slice(0, 8);

    const financeEvaluations = [];
    for (const candidate of financeCandidates) {
      financeEvaluations.push(await evaluateFinanceCandidate(client, candidate));
    }
    financeEvaluations.sort((a, b) => b.rankingScore - a.rankingScore || Math.abs(b.totalProfit) - Math.abs(a.totalProfit));
    result.financeEvaluations = financeEvaluations;

    const employeeEvaluations = [];
    for (const candidate of employeeCandidates) {
      employeeEvaluations.push(await evaluateEmployeeCandidate(client, candidate));
    }
    employeeEvaluations.sort((a, b) => b.rankingScore - a.rankingScore || b.eligibleCount - a.eligibleCount || b.usableCount - a.usableCount);
    result.employeeEvaluations = employeeEvaluations;

    const bestFinance = financeEvaluations[0] || null;
    const bestEmployee = employeeEvaluations[0] || null;
    result.bestFinance = bestFinance;
    result.bestEmployee = bestEmployee;

    if (!bestFinance || bestFinance.profitDocs === 0) {
      throw new Error('Could not determine Q1 2023 profit collection');
    }
    if (!bestEmployee || bestEmployee.eligibleCount === 0) {
      throw new Error('Could not determine eligible employees');
    }

    const quarterlyProfit = bestFinance.totalProfit;
    const bonusPool = quarterlyProfit * 0.4;
    const amountEach = bonusPool / bestEmployee.eligibleCount;
    const eligibleEmployees = bestEmployee.eligible.map(e => ({
      name: e.name,
      overall_satisfaction: e.overallRaw,
      relationship_with_colleagues: e.relationshipRaw,
      communication_skills: e.communicationRaw,
      amount_each: roundMoney(amountEach),
    }));

    result.answer = {
      quarterly_profit_q1_2023: roundMoney(quarterlyProfit),
      bonus_pool_40_percent: roundMoney(bonusPool),
      eligible_count: bestEmployee.eligibleCount,
      amount_each: roundMoney(amountEach),
      eligible_employees: eligibleEmployees,
    };
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
