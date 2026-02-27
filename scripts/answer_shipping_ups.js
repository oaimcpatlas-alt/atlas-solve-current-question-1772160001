
const { MongoClient } = require('mongodb');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');

const PROJECT_ID = process.env.PROJECT_ID || '699c12be8df98bd863d63d70';
const CLUSTER_NAME = process.env.CLUSTER_NAME || 'mcpatlas';
const COOKIE_HEADER = process.env.CLOUD_COOKIES || '';
const WS_VERSION = process.env.WS_VERSION || '1';

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
    url.searchParams.set('version', WS_VERSION);
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
      const frame = Buffer.concat([Buffer.from([1]), payload]);
      this.ws.send(frame);
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

function stripAccents(s) {
  return String(s ?? '').normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
}
function norm(s) {
  return stripAccents(String(s ?? '')).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}
function normKey(s) {
  return norm(s).replace(/ /g, '');
}
function scalar(v) {
  if (v == null) return v;
  if (v instanceof Date) return v.toISOString();
  if (Array.isArray(v)) return v.slice(0, 8).map(scalar);
  if (typeof v === 'object') {
    if (typeof v.toHexString === 'function') return v.toHexString();
    const out = {};
    const entries = Object.entries(v).slice(0, 30);
    for (const [k, val] of entries) out[k] = scalar(val);
    return out;
  }
  return v;
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
    for (const [k, v] of Object.entries(value).slice(0, 50)) {
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
  const m = s.match(/\b(20\d{2}|19\d{2})[-/](\d{1,2})[-/](\d{1,2})\b/);
  if (m) {
    const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
    if (!Number.isNaN(d.getTime())) return d;
  }
  const m2 = s.match(/\b(\d{1,2})[-/](\d{1,2})[-/](20\d{2}|19\d{2})\b/);
  if (m2) {
    const d = new Date(Date.UTC(Number(m2[3]), Number(m2[1]) - 1, Number(m2[2])));
    if (!Number.isNaN(d.getTime())) return d;
  }
  return null;
}
function toNumber(v) {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const s = v.replace(/,/g, '').trim();
    if (/^-?\d+(?:\.\d+)?$/.test(s)) return Number(s);
    const m = s.match(/-?\d+(?:\.\d+)?/);
    if (m) return Number(m[0]);
  }
  return null;
}
function isUpsValue(v) {
  const s = norm(v);
  return /\bups\b/.test(s) || s.includes('united parcel');
}
function carrierFieldScore(k) {
  const nk = normKey(k);
  let s = 0;
  if (nk.includes('carrier')) s += 8;
  if (nk.includes('courier')) s += 8;
  if (nk.includes('shipper')) s += 7;
  if (nk.includes('shippingprovider')) s += 9;
  if (nk.includes('deliveryservice')) s += 8;
  if (nk.includes('shippingmethod')) s += 6;
  if (nk.includes('shipmentmethod')) s += 6;
  if (nk === 'service') s += 1;
  return s;
}
function distanceFieldScore(k) {
  const nk = normKey(k);
  let s = 0;
  if (nk.includes('distancetraveled')) s += 15;
  if (nk.includes('distancetravelled')) s += 15;
  if (nk.includes('shippingdistance')) s += 14;
  if (nk.includes('shipmentdistance')) s += 14;
  if (nk.includes('deliverydistance')) s += 14;
  if (nk.includes('routedistance')) s += 12;
  if (nk.includes('traveldistance')) s += 12;
  if (nk.includes('distance')) s += 10;
  if (nk.includes('milestravel')) s += 11;
  if (nk.includes('mileage')) s += 8;
  if (nk.includes('miles')) s += 7;
  if (nk.endsWith('mile')) s += 6;
  if (nk.includes('km')) s += 6;
  return s;
}
function dateFieldScore(k) {
  const nk = normKey(k);
  let s = 0;
  if (nk === 'date') s += 5;
  if (nk.includes('orderdate')) s += 14;
  if (nk.includes('orderedat')) s += 12;
  if (nk.includes('purchasedat')) s += 11;
  if (nk.includes('purchasedate')) s += 11;
  if (nk.includes('shipmentdate')) s += 13;
  if (nk.includes('shippingdate')) s += 13;
  if (nk.includes('shipdate')) s += 12;
  if (nk.includes('deliverydate')) s += 12;
  if (nk.includes('createdat')) s += 8;
  if (nk.includes('createdon')) s += 8;
  if (nk.includes('timestamp')) s += 5;
  if (nk.includes('date')) s += 4;
  if (nk.includes('time')) s += 1;
  return s;
}
function idFieldLike(k) {
  const nk = normKey(k);
  return nk === '_id' || nk === 'id' || nk.endsWith('id') || nk.includes('sku') || nk.includes('itemnumber');
}
function videoTextScore(s) {
  const t = norm(s);
  let score = 0;
  if (t.includes('video game')) score += 14;
  if (t.includes('videogame')) score += 14;
  if (t.includes('game store')) score += 10;
  if (t.includes('gaming')) score += 6;
  if (/\bplaystation\b/.test(t)) score += 8;
  if (/\bxbox\b/.test(t)) score += 8;
  if (/\bnintendo\b/.test(t)) score += 8;
  if (/\bswitch\b/.test(t)) score += 6;
  if (/\bpc game\b/.test(t)) score += 6;
  if (/\bconsole\b/.test(t)) score += 4;
  if (/\bgenre\b/.test(t)) score += 2;
  if (/\bplatform\b/.test(t)) score += 2;
  if (/\bgame\b/.test(t)) score += 2;
  return score;
}
function orderContextScore(dbName, collName, keys, textVals) {
  const base = norm(`${dbName} ${collName} ${keys.slice(0, 50).join(' ')} ${textVals.slice(0, 20).join(' ')}`);
  let s = 0;
  for (const kw of ['order', 'orders', 'purchase', 'purchases', 'transaction', 'transactions', 'shipment', 'shipments', 'shipping', 'delivery', 'deliveries']) {
    if (base.includes(kw)) s += 4;
  }
  if (base.includes('customer')) s += 2;
  if (base.includes('tracking')) s += 2;
  if (base.includes('address')) s += 1;
  if (base.includes('store') || base.includes('shop')) s += 1;
  return s;
}
function unitFromKey(k) {
  const nk = normKey(k);
  if (nk.includes('km') || nk.includes('kilometer')) return 'km';
  if (nk.includes('mile') || nk.includes('miles') || nk.includes('mileage')) return 'mi';
  return null;
}

async function main() {
  const uri = 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';
  const out = {
    question: 'Average distance for January 2021 video game orders shipped by UPS',
    cloudCookiesPresent: !!COOKIE_HEADER,
    uri,
  };
  if (!COOKIE_HEADER) {
    out.error = 'missing CLOUD_COOKIES';
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  const client = new MongoClient(uri, {
    serverSelectionTimeoutMS: 60000,
    connectTimeoutMS: 60000,
    socketTimeoutMS: 60000,
    directConnection: false,
    monitorCommands: false,
  });

  try {
    await client.connect();
    out.connected = true;
    out.ping = await client.db('admin').command({ ping: 1 });

    const dbList = await client.db('admin').admin().listDatabases();
    const dbNames = dbList.databases.map((d) => d.name).filter((n) => !['admin', 'config', 'local'].includes(n));
    out.databases = dbNames;

    const productIndexById = new Map();
    const productIndexByName = new Map();
    const productProfiles = [];
    const candidateRows = [];
    const collectionSummaries = [];

    for (const dbName of dbNames) {
      const db = client.db(dbName);
      let colls = [];
      try {
        colls = await db.listCollections({}, { nameOnly: false }).toArray();
      } catch (e) {
        collectionSummaries.push({ db: dbName, error: String(e && e.message || e) });
        continue;
      }

      for (const c of colls) {
        const collName = c.name;
        const coll = db.collection(collName);
        let scanned = 0;
        const sampleDocs = [];
        const topKeys = {};
        let localCandidateCount = 0;
        let localProductProfiles = 0;

        try {
          const cursor = coll.find({}).limit(5000);
          for await (const doc of cursor) {
            scanned += 1;
            if (sampleDocs.length < 2) sampleDocs.push(scalar(doc));

            const flat = flatten(doc);
            const entries = Object.entries(flat).slice(0, 400);
            const keys = entries.map(([k]) => k);
            for (const k of keys.slice(0, 150)) topKeys[k] = (topKeys[k] || 0) + 1;

            const textVals = [];
            const idRefs = [];
            const nameVals = [];
            const categoryVals = [];
            const platformVals = [];
            const dates = [];
            const carriers = [];
            const distances = [];
            let priceish = 0;

            for (const [k, raw] of entries) {
              const nk = normKey(k);
              const vals = Array.isArray(raw) ? raw.slice(0, 8) : [raw];
              for (const v of vals) {
                if (v == null) continue;
                const serial = scalar(v);

                if (typeof serial === 'string') {
                  const sv = serial.trim();
                  if (sv && sv.length <= 120) textVals.push(sv);
                  if (idFieldLike(k) && sv && sv.length <= 80) idRefs.push(sv);
                  if (['name', 'title', 'product', 'item', 'game'].some(tag => nk.includes(tag)) && sv && sv.length <= 120) nameVals.push(sv);
                  if (['category', 'genre', 'type', 'department', 'class'].some(tag => nk.includes(tag)) && sv && sv.length <= 120) categoryVals.push(sv);
                  if (['platform', 'console', 'system'].some(tag => nk.includes(tag)) && sv && sv.length <= 120) platformVals.push(sv);
                  const cScore = carrierFieldScore(k) + (isUpsValue(sv) ? 5 : 0);
                  if (isUpsValue(sv) && cScore > 0) carriers.push({ field: k, value: sv, score: cScore });
                  else if (isUpsValue(sv)) carriers.push({ field: k, value: sv, score: 3 });
                }

                const ds = dateFieldScore(k);
                const dt = parseDate(v);
                if (dt && dt.getUTCFullYear() === 2021 && dt.getUTCMonth() === 0) {
                  dates.push({ field: k, value: dt.toISOString(), score: ds || 1 });
                }

                const num = toNumber(v);
                if (num != null) {
                  const dScore = distanceFieldScore(k);
                  if (dScore > 0 && num > 0 && num < 100000) {
                    distances.push({ field: k, value: num, score: dScore, unit: unitFromKey(k) });
                  }
                  if (nk.includes('price') || nk.includes('cost') || nk.includes('amount') || nk.includes('total')) {
                    priceish += 1;
                  }
                  if (idFieldLike(k) && String(num).length <= 18) idRefs.push(String(Math.trunc(num)));
                }
              }
            }

            const baseText = `${dbName} ${collName} ${keys.slice(0, 80).join(' ')} ${textVals.slice(0, 30).join(' | ')}`;
            const vScoreDirect = videoTextScore(baseText)
              + videoTextScore(categoryVals.join(' | '))
              + videoTextScore(platformVals.join(' | '));

            const productScore =
              (nameVals.length ? 3 : 0) +
              (categoryVals.length ? 3 : 0) +
              (platformVals.length ? 4 : 0) +
              (priceish ? 1 : 0) +
              videoTextScore(`${dbName} ${collName}`) / 2 +
              (norm(`${dbName} ${collName}`).includes('product') ? 3 : 0) +
              (norm(`${dbName} ${collName}`).includes('inventory') ? 2 : 0) +
              (norm(`${dbName} ${collName}`).includes('catalog') ? 2 : 0);

            if ((productScore >= 6 || vScoreDirect >= 8) && (nameVals.length || categoryVals.length || platformVals.length)) {
              const profile = {
                db: dbName,
                collection: collName,
                vScore: vScoreDirect,
                idRefs: [...new Set(idRefs)].slice(0, 20),
                nameVals: [...new Set(nameVals)].slice(0, 10),
                categoryVals: [...new Set(categoryVals)].slice(0, 10),
                platformVals: [...new Set(platformVals)].slice(0, 10),
                sample: scalar(doc),
              };
              productProfiles.push(profile);
              localProductProfiles += 1;
              for (const id of profile.idRefs) {
                if (!id) continue;
                if (!productIndexById.has(id)) productIndexById.set(id, []);
                const arr = productIndexById.get(id);
                if (arr.length < 8) arr.push(profile);
              }
              for (const nm of profile.nameVals) {
                const key = normKey(nm);
                if (!key) continue;
                if (!productIndexByName.has(key)) productIndexByName.set(key, []);
                const arr = productIndexByName.get(key);
                if (arr.length < 8) arr.push(profile);
              }
            }

            if (dates.length && carriers.length && distances.length) {
              localCandidateCount += 1;
              dates.sort((a, b) => b.score - a.score);
              carriers.sort((a, b) => b.score - a.score);
              distances.sort((a, b) => b.score - a.score || a.value - b.value);
              candidateRows.push({
                source: { db: dbName, collection: collName },
                orderScore: orderContextScore(dbName, collName, keys, textVals),
                directVideoScore: vScoreDirect,
                dates: dates.slice(0, 5),
                carrier: carriers[0],
                distance: distances[0],
                allDistances: distances.slice(0, 5),
                idRefs: [...new Set(idRefs)].slice(0, 20),
                nameVals: [...new Set(nameVals)].slice(0, 10),
                categoryVals: [...new Set(categoryVals)].slice(0, 10),
                platformVals: [...new Set(platformVals)].slice(0, 10),
                rawDoc: scalar(doc),
              });
            }
          }

          const topKeyList = Object.entries(topKeys).sort((a, b) => b[1] - a[1]).slice(0, 25);
          const summaryScore = orderContextScore(dbName, collName, topKeyList.map(([k]) => k), []);
          collectionSummaries.push({
            db: dbName,
            collection: collName,
            scanned,
            candidateRows: localCandidateCount,
            productProfiles: localProductProfiles,
            summaryScore,
            topKeys: topKeyList,
            sampleDocs,
          });
        } catch (e) {
          collectionSummaries.push({
            db: dbName,
            collection: collName,
            scanned,
            error: String(e && e.message || e),
            sampleDocs,
          });
        }
      }
    }

    out.scannedCollections = collectionSummaries.length;
    out.topCollectionSummaries = collectionSummaries
      .sort((a, b) => ((b.candidateRows || 0) + (b.productProfiles || 0) + (b.summaryScore || 0)) - ((a.candidateRows || 0) + (a.productProfiles || 0) + (a.summaryScore || 0)))
      .slice(0, 40);
    out.rawCandidateRowCount = candidateRows.length;
    out.productProfileCount = productProfiles.length;

    const enrichedRows = candidateRows.map((row) => {
      const linked = [];
      for (const id of row.idRefs || []) {
        for (const prof of (productIndexById.get(id) || [])) linked.push(prof);
      }
      for (const nm of row.nameVals || []) {
        const key = normKey(nm);
        for (const prof of (productIndexByName.get(key) || [])) linked.push(prof);
      }
      const seen = new Set();
      const dedup = [];
      for (const prof of linked) {
        const sig = `${prof.db}.${prof.collection}:${JSON.stringify(prof.idRefs)}:${JSON.stringify(prof.nameVals)}`;
        if (!seen.has(sig)) {
          seen.add(sig);
          dedup.push(prof);
        }
      }
      let linkedVideoScore = 0;
      for (const prof of dedup) linkedVideoScore = Math.max(linkedVideoScore, prof.vScore || 0);
      const allVideoEvidence = [
        row.directVideoScore || 0,
        linkedVideoScore || 0,
        videoTextScore(`${row.source.db} ${row.source.collection}`),
        videoTextScore((row.categoryVals || []).join(' | ')),
        videoTextScore((row.platformVals || []).join(' | ')),
      ];
      const videoScore = Math.max(...allVideoEvidence);
      const totalScore =
        (row.orderScore || 0) +
        videoScore +
        (row.idRefs && row.idRefs.length ? 1 : 0) +
        (row.distance && row.distance.score ? row.distance.score : 0) +
        (row.carrier && row.carrier.score ? row.carrier.score : 0);

      return {
        ...row,
        linkedProfiles: dedup.slice(0, 5),
        linkedVideoScore,
        videoScore,
        totalScore,
      };
    });

    enrichedRows.sort((a, b) => b.totalScore - a.totalScore);
    out.topRows = enrichedRows.slice(0, 30);

    const groups = new Map();
    for (const row of enrichedRows) {
      // prefer rows with at least some genuine order and video game signal
      if ((row.orderScore || 0) < 2) continue;
      if ((row.videoScore || 0) < 4) continue;
      const unit = row.distance && row.distance.unit || null;
      const key = [
        row.source.db,
        row.source.collection,
        row.distance ? row.distance.field : '',
        row.carrier ? row.carrier.field : '',
        unit || '',
      ].join('|');
      if (!groups.has(key)) {
        groups.set(key, {
          source: row.source,
          distanceField: row.distance ? row.distance.field : null,
          carrierField: row.carrier ? row.carrier.field : null,
          unit,
          distances: [],
          rows: [],
          avgVideoScore: 0,
          avgOrderScore: 0,
        });
      }
      const g = groups.get(key);
      g.distances.push(row.distance.value);
      if (g.rows.length < 10) g.rows.push(row);
      g.avgVideoScore += row.videoScore || 0;
      g.avgOrderScore += row.orderScore || 0;
    }

    const candidateGroups = [];
    for (const g of groups.values()) {
      const count = g.distances.length;
      if (!count) continue;
      const sum = g.distances.reduce((a, b) => a + b, 0);
      const avg = sum / count;
      const min = Math.min(...g.distances);
      const max = Math.max(...g.distances);
      candidateGroups.push({
        source: g.source,
        distanceField: g.distanceField,
        carrierField: g.carrierField,
        unit: g.unit,
        count,
        averageDistance: avg,
        minDistance: min,
        maxDistance: max,
        avgVideoScore: g.avgVideoScore / count,
        avgOrderScore: g.avgOrderScore / count,
        sampleRows: g.rows,
      });
    }

    candidateGroups.sort((a, b) => {
      const scoreA = (a.avgVideoScore * 5) + (a.avgOrderScore * 3) + Math.min(a.count, 20);
      const scoreB = (b.avgVideoScore * 5) + (b.avgOrderScore * 3) + Math.min(b.count, 20);
      return scoreB - scoreA;
    });
    out.candidateGroups = candidateGroups.slice(0, 15);

    const answer = candidateGroups[0] || null;
    out.answer = answer ? {
      courier: 'UPS',
      source: answer.source,
      distanceField: answer.distanceField,
      carrierField: answer.carrierField,
      unit: answer.unit,
      documentCount: answer.count,
      averageDistance: Number(answer.averageDistance.toFixed(6)),
      minDistance: answer.minDistance,
      maxDistance: answer.maxDistance,
    } : null;

    out.explanation = answer
      ? `Best matching collection ${answer.source.db}.${answer.source.collection}, ${answer.count} January 2021 UPS rows, average distance ${Number(answer.averageDistance.toFixed(6))}${answer.unit ? ' ' + answer.unit : ''}.`
      : 'No qualifying candidate group found.';
  } catch (e) {
    out.error = String(e && e.message || e);
    out.stack = e && e.stack || null;
  } finally {
    try { await client.close(); } catch {}
  }

  console.log(JSON.stringify(out, null, 2));
}

main();
