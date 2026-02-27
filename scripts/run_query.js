
const fs = require('fs');
const WebSocket = require('ws');

function writeResult(obj){ fs.writeFileSync('workflow_result.json', JSON.stringify(obj, null, 2)); }

function buildCookieHeader(allowedDomains) {
  try {
    const raw = JSON.parse(fs.readFileSync('browser_cookies.json','utf8'));
    const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
    const parts = [];
    for (const c of cookies) {
      const d = String(c.domain || '');
      const v = typeof c.value === 'string' ? c.value : '';
      if (!v) continue;
      if (allowedDomains.some((ad) => d === ad || d.endsWith(ad))) {
        parts.push(`${c.name}=${c.value}`);
      }
    }
    return parts.join('; ');
  } catch {
    return '';
  }
}

async function fetchManual(url, headers) {
  const resp = await fetch(url, {
    method: 'GET',
    headers,
    redirect: 'manual',
  });
  const text = await resp.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}
  return {
    status: resp.status,
    headers: Object.fromEntries(resp.headers.entries()),
    body: text.slice(0, 2000),
    json,
  };
}

function testWs(cookieHeader) {
  return new Promise((resolve) => {
    const host = 'ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net';
    const url = new URL('wss://cloud.mongodb.com/cluster-connection/699c12be8df98bd863d63d70');
    url.searchParams.set('sniHostname', host);
    url.searchParams.set('port', '27017');
    url.searchParams.set('clusterName', 'mcpatlas');
    url.searchParams.set('version', '1');
    const out = { url: url.toString() };
    const headers = {
      'User-Agent': 'Mozilla/5.0',
      'Origin': 'https://cloud.mongodb.com',
    };
    if (cookieHeader) headers['Cookie'] = cookieHeader;
    const ws = new WebSocket(url, { headers });
    let done = false;
    function finish() {
      if (done) return;
      done = true;
      try { ws.close(); } catch {}
      resolve(out);
    }
    ws.on('open', () => {
      out.open = true;
      const meta = { port: 27017, host, clusterName: 'mcpatlas', ok: 1 };
      const payload = Buffer.from(JSON.stringify(meta), 'utf8');
      const frame = Buffer.concat([Buffer.from([1]), payload]);
      ws.send(frame);
    });
    ws.on('message', (data) => {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      out.message = {
        len: buf.length,
        type: buf[0],
        textPrefix: buf.subarray(1, 200).toString('utf8'),
        hexPrefix: buf.subarray(0, 50).toString('hex'),
      };
      finish();
    });
    ws.on('unexpected-response', (_req, res) => {
      out.unexpected = {
        statusCode: res.statusCode,
        headers: res.headers,
      };
      finish();
    });
    ws.on('error', (err) => {
      out.error = String(err && err.message || err);
      finish();
    });
    ws.on('close', (code, reason) => {
      out.close = { code, reason: reason.toString() };
      setTimeout(finish, 50);
    });
    setTimeout(() => {
      out.timeout = true;
      finish();
    }, 10000);
  });
}

async function main() {
  const result = { triedAt: new Date().toISOString() };
  try {
    const accountCookieHeader = buildCookieHeader(['account.mongodb.com', '.account.mongodb.com', '.mongodb.com']);
    const cloudCookieHeader = buildCookieHeader(['cloud.mongodb.com', '.cloud.mongodb.com', '.mongodb.com']);
    result.accountCookieHeaderLength = accountCookieHeader.length;
    result.cloudCookieHeaderLength = cloudCookieHeader.length;

    const cloudHeaders = {
      'Accept': 'application/json',
      'User-Agent': 'Mozilla/5.0',
    };
    if (cloudCookieHeader) cloudHeaders['Cookie'] = cloudCookieHeader;

    result.overview = await fetchManual('https://cloud.mongodb.com/v2/699c12be8df98bd863d63d70#/overview', cloudHeaders);
    result.connectionInfo = await fetchManual('https://cloud.mongodb.com/explorer/v1/groups/699c12be8df98bd863d63d70/clusters/connectionInfo', cloudHeaders);
    result.nds = await fetchManual('https://cloud.mongodb.com/nds/699c12be8df98bd863d63d70', cloudHeaders);
    result.wsProbe = await testWs(cloudCookieHeader);
  } catch (e) {
    result.error = String(e && e.stack || e);
  } finally {
    writeResult(result);
  }
}
main();
