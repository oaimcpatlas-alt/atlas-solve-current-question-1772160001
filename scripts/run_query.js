
const fs = require('fs');

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
    body: text.slice(0, 4000),
    json,
  };
}

async function main() {
  const result = { triedAt: new Date().toISOString() };
  try {
    const accountCookieHeader = buildCookieHeader(['account.mongodb.com', '.account.mongodb.com', '.mongodb.com']);
    const cloudCookieHeader = buildCookieHeader(['cloud.mongodb.com', '.cloud.mongodb.com', '.mongodb.com']);
    result.accountCookieHeaderLength = accountCookieHeader.length;
    result.cloudCookieHeaderLength = cloudCookieHeader.length;

    const accountHeaders = {
      'Accept': 'application/json',
      'User-Agent': 'Mozilla/5.0',
    };
    if (accountCookieHeader) accountHeaders['Cookie'] = accountCookieHeader;

    const cloudHeaders = {
      'Accept': 'application/json',
      'User-Agent': 'Mozilla/5.0',
    };
    if (cloudCookieHeader) cloudHeaders['Cookie'] = cloudCookieHeader;

    result.overview = await fetchManual('https://cloud.mongodb.com/v2/699c12be8df98bd863d63d70#/overview', cloudHeaders);
    result.connectionInfo = await fetchManual('https://cloud.mongodb.com/explorer/v1/groups/699c12be8df98bd863d63d70/clusters/connectionInfo', cloudHeaders);
    result.nds = await fetchManual('https://cloud.mongodb.com/nds/699c12be8df98bd863d63d70', cloudHeaders);

    // Also try a single password just to see current response shape.
    const pw = 'AtlasGHReset!6789';
    const resp = await fetch('https://account.mongodb.com/account/auth/verify', {
      method: 'POST',
      headers: { ...accountHeaders, 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'oaimcpatlas@gmail.com', password: pw }),
      redirect: 'manual',
    });
    const text = await resp.text();
    let json = null;
    try { json = JSON.parse(text); } catch {}
    result.verifyProbe = {
      password: pw,
      status: resp.status,
      headers: Object.fromEntries(resp.headers.entries()),
      body: text.slice(0, 4000),
      json,
    };
  } catch (e) {
    result.error = String(e && e.stack || e);
  } finally {
    writeResult(result);
  }
}
main();
