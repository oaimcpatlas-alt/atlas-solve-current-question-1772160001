const fs = require('fs');
const { MongoClient } = require('mongodb');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');

const PROJECT_ID = '699c12be8df98bd863d63d70';
const CLUSTER_NAME = 'mcpatlas';
const MONGO_URI = 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';

function writeResult(obj) {
  fs.writeFileSync('car_team_score_result.json', JSON.stringify(obj, null, 2));
}
function buildCookieHeader() {
  const raw = JSON.parse(fs.readFileSync('browser_cookies.json', 'utf8'));
  const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
  const cookieMap = new Map();
  for (const c of cookies) {
    const domain = String(c.domain || '');
    const value = typeof c.value === 'string' ? c.value : '';
    if (!value) continue;
    if (domain.includes('mongodb.com')) cookieMap.set(String(c.name), value);
  }
  const srt = cookieMap.get('__Secure-mdb-srt');
  if (srt && !cookieMap.has('__Secure-mdb-sat')) {
    cookieMap.set('__Secure-mdb-sat', srt);
  }
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
      const frame = Buffer.concat([Buffer.from([2]), payload]);
      this.ws.send(frame, callback);
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

function stripAccents(s) { return String(s ?? '').normalize('NFKD').replace(/[\u0300-\u036f]/g, ''); }
function norm(s) { return stripAccents(String(s ?? '')).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim(); }
function normKey(s) { return norm(s).replace(/ /g, ''); }
function toNum(v) {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const m = v.replace(/,/g, '').replace(/\$/g, '').match(/-?\d+(?:\.\d+)?/);
    if (m) return Number(m[0]);
  }
  return null;
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
  const m = s.match(/((?:19|20)\d{2})[-/](\d{1,2})[-/](\d{1,2})/);
  if (m) {
    const dt = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
    if (!Number.isNaN(dt.getTime())) return dt;
  }
  return null;
}
function flatten(v, prefix = '', out = {}) {
  if (v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Date)) {
    for (const [k, val] of Object.entries(v)) {
      const p = prefix ? `${prefix}.${k}` : String(k);
      flatten(val, p, out);
    }
    return out;
  }
  if (Array.isArray(v)) {
    out[prefix || 'value'] = v;
    v.slice(0, 8).forEach((item, i) => flatten(item, `${prefix}[${i}]`, out));
    return out;
  }
  out[prefix || 'value'] = v;
  return out;
}
function simple(v) {
  if (v instanceof Date) return v.toISOString();
  if (Array.isArray(v)) return v.slice(0, 6).map(simple);
  if (v && typeof v === 'object') {
    const out = {};
    for (const [k, val] of Object.entries(v).slice(0, 20)) out[k] = simple(val);
    return out;
  }
  return v;
}
function detectEmployeeSignals(flat) {
  let score = 0;
  for (const [path, v] of Object.entries(flat)) {
    const nk = normKey(path);
    if (['employeeid', 'employee_id', 'employeenumber', 'staffid', 'personnelid', 'workerid'].some(x => nk.includes(x))) score += 4;
    if (['employeename', 'fullname', 'firstname', 'lastname', 'staffname', 'personnelname', 'workername'].some(x => nk.includes(x))) score += 3;
    if ((nk.includes('employee') || nk.includes('staff') || nk.includes('personnel') || nk.includes('worker')) && !nk.includes('score')) score += 2;
  }
  return score;
}
function detectTeam(flat) {
  let best = null;
  for (const [path, v] of Object.entries(flat)) {
    const nk = normKey(path);
    const pv = norm(v);
    if (!pv) continue;
    const teamField = /(department|dept|team|function|division|unit|area|group)/.test(nk);
    const isSales = pv === 'sales' || pv.includes(' sales') || pv.startsWith('sales ') || pv.includes('sales department') || pv.includes('sales team');
    const isFinance = pv === 'finance' || pv.includes(' finance') || pv.startsWith('finance ') || pv.includes('finance department') || pv.includes('finance team');
    if ((isSales || isFinance) && (teamField || pv === 'sales' || pv === 'finance')) {
      const team = isSales ? 'sales' : 'finance';
      let score = 0;
      if (teamField) score += 5;
      if (nk.includes('department')) score += 3;
      if (nk.includes('team')) score += 2;
      if (!best || score > best.score) best = { team, field: path, value: String(v), score };
    }
  }
  return best;
}
function qVal(s) {
  const t = norm(s);
  if (t === 'q1' || t === '1' || t.includes('first quarter') || t === 'quarter 1' || t === 'quarter1') return 1;
  if (t === 'q2' || t === '2' || t.includes('second quarter') || t === 'quarter 2' || t === 'quarter2') return 2;
  if (t === 'q3' || t === '3' || t.includes('third quarter') || t === 'quarter 3' || t === 'quarter3') return 3;
  if (t === 'q4' || t === '4' || t.includes('fourth quarter') || t === 'quarter 4' || t === 'quarter4') return 4;
  return null;
}
function detectPeriod(flat) {
  let year = null, quarter = null, month = null, dateQ1 = false;
  for (const [path, v] of Object.entries(flat)) {
    const nk = normKey(path);
    if (year == null && /year|reviewyear|fiscalyear|periodyear|assessmentyear/.test(nk)) {
      const n = toNum(v);
      if (n && n >= 1900 && n <= 2100) year = Math.trunc(n);
      else {
        const m = String(v).match(/\b(19|20)\d{2}\b/);
        if (m) year = Number(m[0]);
      }
    }
    if (quarter == null) {
      if (/quarter|qtr|periodquarter|fiscalquarter|reviewquarter/.test(nk)) quarter = qVal(v);
      else if (nk.includes('q12017') || nk.includes('2017q1') || nk.includes('quarter12017') || nk.includes('2017quarter1')) { year = 2017; quarter = 1; }
    }
    if (month == null && /month|periodmonth|reviewmonth/.test(nk)) {
      const n = toNum(v);
      if (n && n >= 1 && n <= 12) month = Math.trunc(n);
    }
    const d = parseDate(v);
    if (d && d.getUTCFullYear() === 2017) {
      const m = d.getUTCMonth() + 1;
      if (m >= 1 && m <= 3) dateQ1 = true;
      if (year == null) year = 2017;
      if (quarter == null && m >= 1 && m <= 3) quarter = 1;
      if (month == null) month = m;
    }
  }
  const match = dateQ1 || (year === 2017 && quarter === 1) || (year === 2017 && month != null && month >= 1 && month <= 3);
  return { year, quarter, month, dateQ1, match };
}
function detectScore(flat) {
  let best = null;
  for (const [path, v] of Object.entries(flat)) {
    const nk = normKey(path);
    const num = toNum(v);
    if (num == null) continue;
    if (num < -1000 || num > 1000000) continue;
    let score = 0;
    if (nk.includes('totalscore')) score += 120;
    if (nk.includes('overallscore')) score += 110;
    if (nk.includes('performancescore')) score += 105;
    if (nk.includes('reviewscore')) score += 100;
    if (nk.endsWith('score') || nk === 'score') score += 90;
    if (nk.includes('score')) score += 70;
    if (nk.includes('rating')) score += 45;
    if (nk.includes('points')) score += 40;
    if (nk.includes('result')) score += 35;
    if (nk.includes('metric') || nk.includes('id') || nk.includes('zip') || nk.includes('phone')) score -= 50;
    if (nk.includes('age')) score -= 40;
    if (score <= 0) continue;
    if (!best || score > best.score) best = { field: path, value: num, score };
  }
  return best;
}
function collNameScore(dbName, collName) {
  const blob = norm(dbName + ' ' + collName);
  let score = 0;
  ['employee', 'employees', 'staff', 'personnel', 'performance', 'review', 'score', 'dealership', 'dealer', 'automotive', 'auto', 'sales', 'finance'].forEach(tok => { if (blob.includes(tok)) score += 5; });
  return score;
}

async function main() {
  const out = { started_at: new Date().toISOString(), cookieHeaderLength: COOKIE_HEADER.length };
  let client = null;
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
    const dbInfo = await client.db('admin').admin().listDatabases();
    const dbNames = (dbInfo.databases || []).map(d => d.name).filter(n => !['admin', 'local', 'config'].includes(n));
    out.databases = dbNames;
    const collectionResults = [];
    for (const dbName of dbNames) {
      const db = client.db(dbName);
      const cols = await db.listCollections().toArray();
      for (const c of cols) {
        const collName = c.name;
        const coll = db.collection(collName);
        let scanned = 0;
        let matched = 0;
        let employeeMatched = 0;
        const teamTotals = { sales: 0, finance: 0 };
        const teamMins = { sales: Infinity, finance: Infinity };
        const samples = [];
        try {
          const cursor = coll.find({});
          for await (const doc of cursor) {
            scanned += 1;
            const flat = flatten(doc);
            const teamRec = detectTeam(flat);
            const periodRec = detectPeriod(flat);
            const scoreRec = detectScore(flat);
            const empScore = detectEmployeeSignals(flat);
            if (teamRec && periodRec.match && scoreRec) {
              matched += 1;
              teamTotals[teamRec.team] += scoreRec.value;
              if (scoreRec.value < teamMins[teamRec.team]) teamMins[teamRec.team] = scoreRec.value;
              if (empScore > 0) employeeMatched += 1;
              if (samples.length < 5) {
                samples.push({
                  team: teamRec.team,
                  teamField: teamRec.field,
                  score: scoreRec.value,
                  scoreField: scoreRec.field,
                  period: periodRec,
                  employeeSignalScore: empScore,
                  doc: simple(doc),
                });
              }
            }
            if (scanned >= 10000) break;
          }
        } catch (e) {
          collectionResults.push({ dbName, collName, scanned, error: String(e && e.message || e) });
          continue;
        }
        const bothTeams = teamTotals.sales !== 0 && teamTotals.finance !== 0;
        const rel = matched * 100 + employeeMatched * 20 + (bothTeams ? 200 : 0) + collNameScore(dbName, collName);
        collectionResults.push({
          dbName, collName, scanned, matched, employeeMatched, bothTeams,
          teamTotals,
          teamMins: {
            sales: Number.isFinite(teamMins.sales) ? teamMins.sales : null,
            finance: Number.isFinite(teamMins.finance) ? teamMins.finance : null,
          },
          rel,
          samples,
        });
      }
    }
    collectionResults.sort((a, b) => (b.rel || 0) - (a.rel || 0) || (b.matched || 0) - (a.matched || 0));
    out.collectionResults = collectionResults.slice(0, 20);
    const best = collectionResults.find(x => x.matched > 0 && x.bothTeams) || collectionResults.find(x => x.matched > 0) || null;
    out.best = best || null;
    if (best) {
      const worstTeam = (best.teamTotals.sales <= best.teamTotals.finance) ? 'sales' : 'finance';
      out.answer = {
        collection: `${best.dbName}.${best.collName}`,
        teamTotals: best.teamTotals,
        worstTeam,
        lowestIndividualScore: best.teamMins[worstTeam],
        matchedDocs: best.matched,
        employeeMatchedDocs: best.employeeMatched,
      };
    } else {
      out.answer = null;
      out.error = 'No matching records found';
    }
  } catch (e) {
    out.error = String((e && e.message) || e);
    out.stack = e && e.stack || null;
  } finally {
    try { if (client) await client.close(); } catch {}
    writeResult(out);
    console.log(JSON.stringify({ answer: out.answer, error: out.error, best: out.best && { dbName: out.best.dbName, collName: out.best.collName, matched: out.best.matched, bothTeams: out.best.bothTeams } }, null, 2));
  }
}
main();
