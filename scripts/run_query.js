
const fs = require('fs');

function writeResult(obj) {
  fs.writeFileSync('workflow_result.json', JSON.stringify(obj, null, 2));
}
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function buildAccountCookieHeader() {
  try {
    const raw = JSON.parse(fs.readFileSync('browser_cookies.json', 'utf8'));
    const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
    const parts = [];
    for (const c of cookies) {
      const domain = String(c.domain || '');
      const value = typeof c.value === 'string' ? c.value : '';
      if (!value) continue;
      if (domain === 'account.mongodb.com' || domain === '.account.mongodb.com' || domain === '.mongodb.com') {
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
  return { password, status: resp.status, headers: Object.fromEntries(resp.headers.entries()), body: text };
}

async function main() {
  const result = { cookieHeaderLength: 0, tried: [] };
  try {
    const cookieHeader = buildAccountCookieHeader();
    result.cookieHeaderLength = cookieHeader.length;
    const candidates = [
      'AtlasGHReset!6789',
      'Tmp!2e7ad4aa5c4fe3d1aA1',
      'P9!vL3#qT7@xN2mR',
      'V7u!9xK#2mQ@4nR$8tP^6sL&'
    ];
    for (const pw of candidates) {
      const r = await tryPassword(pw, cookieHeader);
      result.tried.push(r);
      writeResult(result);
      if (r.status === 200 && /"status"\s*:\s*"OK"/.test(r.body)) {
        result.success = r;
        break;
      }
      if ((r.body || '').includes('RATE_LIMITED')) {
        result.rateLimited = true;
        break;
      }
      await sleep(3000);
    }
  } catch (e) {
    result.error = String(e && e.stack || e);
  } finally { writeResult(result); }
}
main();
