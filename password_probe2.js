
const fs = require('fs');

const USERNAME = 'oaimcpatlas@gmail.com';
const CANDIDATES = ["VM5!YXAtnZLkAkl@U9MGVb0q0w", "VQ3!V%6f6=DUBT@E6VTkjvR5iy", "AtlasFixed!24680Aa"];

function write(obj) {
  fs.writeFileSync('password_probe2_result.json', JSON.stringify(obj, null, 2));
}

function buildAccountCookieHeader() {
  try {
    const raw = JSON.parse(fs.readFileSync('browser_cookies.json', 'utf8'));
    const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
    const allowed = new Set(['account.mongodb.com']);
    const keepNames = new Set([
      'remember-user-device',
      'user-device',
      '__Secure-mdb-sat',
      '__Secure-mdb-srt',
      'accounta-prod',
      'okta-oauth-state',
      'okta-oauth-nonce',
      'oidc-init-user-prod'
    ]);
    const parts = [];
    for (const c of cookies) {
      if (allowed.has(String(c.domain || '')) && keepNames.has(String(c.name || '')) && typeof c.value === 'string' && c.value) {
        parts.push(`${c.name}=${c.value}`);
      }
    }
    return parts.join('; ');
  } catch {
    return '';
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function tryPw(pw, cookieHeader) {
  const resp = await fetch('https://account.mongodb.com/account/auth/verify', {
    method: 'POST',
    redirect: 'manual',
    headers: {
      'User-Agent': 'Mozilla/5.0',
      'Content-Type': 'application/json',
      'Cookie': cookieHeader,
    },
    body: JSON.stringify({ username: USERNAME, password: pw }),
  });
  const text = await resp.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}
  return {
    password: pw,
    status: resp.status,
    headers: Object.fromEntries(resp.headers.entries()),
    body: text,
    json,
  };
}

async function main() {
  const result = {
    started_at: new Date().toISOString(),
    cookieHeaderLength: 0,
    attempts: []
  };
  try {
    const cookieHeader = buildAccountCookieHeader();
    result.cookieHeaderLength = cookieHeader.length;
    for (const pw of CANDIDATES) {
      const out = await tryPw(pw, cookieHeader);
      result.attempts.push(out);
      write(result);
      if (out.status === 200 && out.json && out.json.status === 'OK') {
        result.success = out;
        break;
      }
      const err = out.json && out.json.errorCode;
      if (err === 'RATE_LIMITED') {
        await sleep(45000);
      } else {
        await sleep(5000);
      }
    }
  } catch (e) {
    result.error = String(e && e.message || e);
    result.stack = e && e.stack || null;
  } finally {
    result.finished_at = new Date().toISOString();
    write(result);
  }
}

main();
