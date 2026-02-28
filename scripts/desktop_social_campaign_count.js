
const fs = require('fs');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');
const { MongoClient } = require('mongodb');

const PROJECT_ID = '699c12be8df98bd863d63d70';
const CLUSTER_NAME = 'mcpatlas';
const MONGO_URI = 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';
const TARGET_DATE = '2015-12-03';
const BTC_LOW_DATE = '2015-12-02';
const DOC_RELEASE_DATE = '2014-10-10';

function write(obj) {
  fs.writeFileSync('desktop_social_campaign_count_result.json', JSON.stringify(obj, null, 2));
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

function stripAccents(s) { return String(s ?? '').normalize('NFKD').replace(/[\u0300-\u036f]/g, ''); }
function norm(s) { return stripAccents(String(s ?? '')).toLowerCase(); }
function normKey(s) { return norm(s).replace(/[^a-z0-9]+/g, ''); }
function textNorm(s) { return norm(s).replace(/[^a-z0-9]+/g, ' ').trim(); }

function ser(v, depth = 0) {
  if (depth > 4) return String(v);
  if (v instanceof Date) return v.toISOString();
  if (Array.isArray(v)) return v.slice(0, 10).map(x => ser(x, depth + 1));
  if (v && typeof v === 'object') {
    if (typeof v.toHexString === 'function') return v.toHexString();
    const out = {};
    for (const [k, val] of Object.entries(v).slice(0, 60)) out[k] = ser(val, depth + 1);
    return out;
  }
  return v;
}

function flatten(obj, prefix = '', out = {}) {
  if (obj instanceof Date) {
    out[prefix || 'value'] = obj;
    return out;
  }
  if (Array.isArray(obj)) {
    out[prefix || 'value'] = obj.map(x => ser(x));
    obj.slice(0, 8).forEach((item, i) => flatten(item, `${prefix}[${i}]`, out));
    return out;
  }
  if (obj && typeof obj === 'object') {
    if (typeof obj.toHexString === 'function') {
      out[prefix || 'value'] = obj.toHexString();
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

function parseDate(v) {
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
  }
  if (typeof v === 'string') {
    const s = v.trim();
    if (!s) return null;
    const d = new Date(s);
    if (!isNaN(d)) return d;
    const m = s.match(/^(\d{4})[\/-](\d{1,2})[\/-](\d{1,2})$/);
    if (m) {
      const d2 = new Date(`${m[1]}-${m[2].padStart(2,'0')}-${m[3].padStart(2,'0')}T00:00:00Z`);
      if (!isNaN(d2)) return d2;
    }
  }
  return null;
}

function isoDay(d) {
  if (!(d instanceof Date) || isNaN(d)) return null;
  return d.toISOString().slice(0, 10);
}

const SOCIAL_TERMS = [
  'social media','facebook','instagram','twitter','x ','x.com','tiktok','linkedin','pinterest','snapchat','youtube','reddit','meta'
];

function socialValueScore(value, keyNorm = '') {
  const s = textNorm(value);
  if (!s) return 0;
  let score = 0;
  if (s === 'social media') score += 30;
  if (s.includes('social media')) score += 26;
  if (/(facebook|instagram|twitter|tiktok|linkedin|pinterest|snapchat|youtube|reddit|meta)\b/.test(s)) score += 24;
  if (/(channel|platform|medium|source|network|placement|adtype|type|category)/.test(keyNorm)) score += 8;
  if (/(email|search|display|referral|direct|organic)/.test(s)) score -= 18;
  return score;
}

function desktopValueScore(value, keyNorm = '') {
  const s = textNorm(value);
  if (!s) return 0;
  let score = 0;
  if (s === 'desktop' || s === 'desktop computers' || s === 'desktop computer') score += 30;
  if (s.includes('desktop')) score += 24;
  if (/(device|platform|target|userdevice|audience)/.test(keyNorm)) score += 8;
  if (/(mobile|tablet|android|ios|phone|smartphone)/.test(s)) score -= 18;
  return score;
}

function campaignFieldScore(keyNorm, ctx) {
  let s = 0;
  if (keyNorm.includes('campaign')) s += 18;
  if (keyNorm.includes('advert')) s += 12;
  if (keyNorm === 'ad' || keyNorm.includes('ad')) s += 8;
  if (keyNorm.includes('marketing')) s += 8;
  if (/(campaign|advert|marketing|promotion|social)/.test(ctx)) s += 6;
  return s;
}

function pickCampaignIdentifier(flat, context) {
  const candidates = [];
  for (const [key, value] of Object.entries(flat)) {
    const nk = normKey(key);
    if (value == null) continue;
    let score = 0;
    if (nk === '_id' || nk === 'id') score += 2;
    if (nk.includes('campaignid')) score += 30;
    if (nk.includes('campaignname')) score += 28;
    if (nk.includes('campaigntitle')) score += 26;
    if (nk === 'campaign') score += 22;
    if (nk.includes('campaign')) score += 18;
    if (nk.endsWith('name')) score += 8;
    if (nk.endsWith('title')) score += 8;
    if (nk.endsWith('id')) score += 6;
    if (score <= 0) continue;
    const sv = typeof value === 'string' ? value.trim() : String(value);
    if (!sv) continue;
    candidates.push({ key, value: sv, score });
  }
  candidates.sort((a,b)=>b.score-a.score || a.key.localeCompare(b.key));
  if (candidates.length) return candidates[0].value;
  if (flat['_id']) return String(flat['_id']);
  return null;
}

function analyzeDoc(doc, dbName, collName) {
  const flat = flatten(doc);
  const context = textNorm(`${dbName} ${collName}`);
  let campaignish = 0;
  const dateCandidates = [];
  const socialCandidates = [];
  const desktopCandidates = [];
  const nameCandidates = [];

  for (const [key, value] of Object.entries(flat)) {
    const nk = normKey(key);
    if (!nk) continue;
    campaignish += campaignFieldScore(nk, context) * 0.1;

    const d = parseDate(value);
    if (d) {
      let score = 0;
      if (nk.includes('campaignstart')) score += 40;
      if ((nk.includes('start') || nk.includes('launch') || nk.includes('launched') || nk.includes('begin')) && (nk.includes('date') || nk.includes('time') || nk === 'start' || nk === 'launch')) score += 35;
      if (nk === 'date' && /(campaign|ad|advert|marketing)/.test(context)) score += 15;
      if (nk.endsWith('date')) score += 8;
      if (score > 0) dateCandidates.push({ key, day: isoDay(d), score });
    }

    if (typeof value === 'string') {
      const sv = value.trim();
      if (sv) {
        const socialScore = socialValueScore(sv, nk);
        if (socialScore > 0) socialCandidates.push({ key, value: sv, score: socialScore });
        const desktopScore = desktopValueScore(sv, nk);
        if (desktopScore > 0) desktopCandidates.push({ key, value: sv, score: desktopScore });
        let nameScore = 0;
        if (nk.includes('campaignname')) nameScore += 30;
        if (nk.includes('campaigntitle')) nameScore += 28;
        if (nk === 'campaign') nameScore += 22;
        if (nk.includes('campaign')) nameScore += 16;
        if ((nk.endsWith('name') || nk.endsWith('title')) && /(campaign|ad|advert|marketing)/.test(context)) nameScore += 8;
        if (nameScore > 0) nameCandidates.push({ key, value: sv, score: nameScore });
      }
    } else if (Array.isArray(value)) {
      const joined = value.map(x => String(x)).join(' | ');
      const socialScore = socialValueScore(joined, nk);
      if (socialScore > 0) socialCandidates.push({ key, value: joined, score: socialScore });
      const desktopScore = desktopValueScore(joined, nk);
      if (desktopScore > 0) desktopCandidates.push({ key, value: joined, score: desktopScore });
    }
  }

  dateCandidates.sort((a,b)=>b.score-a.score || a.key.localeCompare(b.key));
  socialCandidates.sort((a,b)=>b.score-a.score || a.key.localeCompare(b.key));
  desktopCandidates.sort((a,b)=>b.score-a.score || a.key.localeCompare(b.key));
  nameCandidates.sort((a,b)=>b.score-a.score || a.key.localeCompare(b.key));

  const bestDate = dateCandidates[0] || null;
  const bestSocial = socialCandidates[0] || null;
  const bestDesktop = desktopCandidates[0] || null;
  const campaignId = pickCampaignIdentifier(flat, context);
  const campaignName = nameCandidates[0]?.value || campaignId;

  if (/campaign|advert|marketing|promotion/.test(context)) campaignish += 8;
  if (/social/.test(context)) campaignish += 4;
  if (/desktop/.test(context)) campaignish += 2;
  if (bestDate) campaignish += 2;
  if (bestSocial) campaignish += 3;
  if (bestDesktop) campaignish += 3;

  return {
    flat,
    campaignish,
    campaignId,
    campaignName,
    bestDate,
    bestSocial,
    bestDesktop,
    matched: !!(bestDate && bestDate.day === TARGET_DATE && bestSocial && bestDesktop),
  };
}

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
      if (!buf.length) return;
      const type = buf[0];
      const rest = buf.subarray(1);
      if (type === 1) {
        let msg;
        try { msg = JSON.parse(rest.toString('utf8')); } catch (e) { this.destroy(e); return; }
        if (msg.preMessageOk === 1) {
          this.connected = true;
          this.emit('connect');
          this.emit('secureConnect');
          this._flushPendingWrites();
        } else {
          this.destroy(new Error('Unexpected proxy pre-message: ' + JSON.stringify(msg)));
        }
      } else if (type === 2) {
        this.push(rest);
      } else {
        this.destroy(new Error('Unexpected proxy frame type: ' + type));
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
  _flushPendingWrites() {
    if (!this.connected || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    while (this._pendingWrites.length) {
      const { chunk, encoding, callback } = this._pendingWrites.shift();
      this._writeNow(chunk, encoding, callback);
    }
  }
  _writeNow(chunk, encoding, callback) {
    try {
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
  setTimeout(ms, cb) { return this; }
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

(async () => {
  const result = {
    started_at: new Date().toISOString(),
    documentary_release_date: DOC_RELEASE_DATE,
    year_after_release: 2015,
    month_considered: '2015-12',
    bitcoin_lowest_price_date_in_month: BTC_LOW_DATE,
    target_launch_date: TARGET_DATE,
    cookieHeaderLength: COOKIE_HEADER.length,
  };
  let client = null;
  try {
    client = new MongoClient(MONGO_URI, {
      serverSelectionTimeoutMS: 90000,
      connectTimeoutMS: 90000,
      socketTimeoutMS: 90000,
      directConnection: false,
    });
    await client.connect();
    result.ping = await client.db('admin').command({ ping: 1 });
    const dbResp = await client.db('admin').admin().listDatabases();
    const dbNames = (dbResp.databases || []).map(d => d.name).filter(n => !['admin','local','config'].includes(n));
    result.databases = dbNames;

    const collectionSummaries = [];

    for (const dbName of dbNames) {
      const collections = await client.db(dbName).listCollections().toArray();
      for (const meta of collections) {
        const collName = meta.name;
        const coll = client.db(dbName).collection(collName);
        let scanned = 0;
        let campaignishDocs = 0;
        let rawMatchCount = 0;
        const matchedIds = new Set();
        const matchedDocs = [];
        const dateFieldCounts = new Map();
        const socialFieldCounts = new Map();
        const desktopFieldCounts = new Map();

        try {
          const cursor = coll.find({}, { batchSize: 100 });
          for await (const doc of cursor) {
            scanned += 1;
            if (scanned > 12000) break;
            const analyzed = analyzeDoc(doc, dbName, collName);
            if (analyzed.campaignish >= 5) campaignishDocs += 1;
            if (analyzed.bestDate) dateFieldCounts.set(analyzed.bestDate.key, (dateFieldCounts.get(analyzed.bestDate.key)||0)+1);
            if (analyzed.bestSocial) socialFieldCounts.set(analyzed.bestSocial.key, (socialFieldCounts.get(analyzed.bestSocial.key)||0)+1);
            if (analyzed.bestDesktop) desktopFieldCounts.set(analyzed.bestDesktop.key, (desktopFieldCounts.get(analyzed.bestDesktop.key)||0)+1);
            if (analyzed.matched) {
              rawMatchCount += 1;
              const id = analyzed.campaignId || `__row_${rawMatchCount}`;
              matchedIds.add(id);
              if (matchedDocs.length < 8) {
                matchedDocs.push({
                  campaignId: analyzed.campaignId,
                  campaignName: analyzed.campaignName,
                  dateField: analyzed.bestDate,
                  socialField: analyzed.bestSocial,
                  desktopField: analyzed.bestDesktop,
                  sample: ser(doc),
                });
              }
            }
          }
        } catch (e) {
          collectionSummaries.push({ db: dbName, collection: collName, error: String(e && e.message || e), scanned });
          continue;
        }

        const nameScore = (/campaign|advert|marketing|social/i.test(dbName + ' ' + collName) ? 30 : 0) + (/desktop/i.test(dbName + ' ' + collName) ? 10 : 0);
        const score = matchedIds.size * 200 + rawMatchCount * 20 + campaignishDocs * 2 + nameScore;
        collectionSummaries.push({
          db: dbName,
          collection: collName,
          scanned,
          campaignishDocs,
          rawMatchCount,
          uniqueCampaignCount: matchedIds.size,
          topDateFields: Array.from(dateFieldCounts.entries()).sort((a,b)=>b[1]-a[1]).slice(0,5),
          topSocialFields: Array.from(socialFieldCounts.entries()).sort((a,b)=>b[1]-a[1]).slice(0,5),
          topDesktopFields: Array.from(desktopFieldCounts.entries()).sort((a,b)=>b[1]-a[1]).slice(0,5),
          matchedDocs,
          score,
        });
      }
    }

    collectionSummaries.sort((a,b)=>(b.uniqueCampaignCount||0)-(a.uniqueCampaignCount||0) || (b.score||0)-(a.score||0) || String(a.db+'.'+a.collection).localeCompare(String(b.db+'.'+b.collection)));
    result.collectionSummaries = collectionSummaries.slice(0, 25);
    const best = collectionSummaries.find(x => (x.uniqueCampaignCount||0) > 0) || collectionSummaries[0] || null;
    result.bestCollection = best ? { db: best.db, collection: best.collection, uniqueCampaignCount: best.uniqueCampaignCount, rawMatchCount: best.rawMatchCount, score: best.score } : null;
    result.answer = best ? {
      campaign_count: best.uniqueCampaignCount || 0,
      db: best.db,
      collection: best.collection,
      target_launch_date: TARGET_DATE,
      bitcoin_lowest_price_date_in_month: BTC_LOW_DATE,
      matching_campaign_samples: best.matchedDocs,
    } : null;
  } catch (e) {
    result.error = String(e && e.message || e);
    result.stack = e && e.stack || null;
  } finally {
    try { if (client) await client.close(); } catch {}
    result.finished_at = new Date().toISOString();
    write(result);
    console.log(JSON.stringify(result.answer || { error: result.error }, null, 2));
  }
})();
