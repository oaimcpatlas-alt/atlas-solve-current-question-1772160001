
const fs = require('fs');

const PASSWORD = "Qz9!SolveNow#1772174889";
const USERNAME = 'oaimcpatlas@gmail.com';

function write(obj) {
  fs.writeFileSync('password_probe_result.json', JSON.stringify(obj, null, 2));
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

async function main() {
  const result = {
    started_at: new Date().toISOString(),
    password: PASSWORD,
    cookieHeaderLength: 0
  };
  try {
    const cookieHeader = buildAccountCookieHeader();
    result.cookieHeaderLength = cookieHeader.length;
    const resp = await fetch('https://account.mongodb.com/account/auth/verify', {
      method: 'POST',
      redirect: 'manual',
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Content-Type': 'application/json',
        'Cookie': cookieHeader,
      },
      body: JSON.stringify({ username: USERNAME, password: PASSWORD }),
    });
    const text = await resp.text();
    let json = null;
    try { json = JSON.parse(text); } catch {}
    result.verify = {
      status: resp.status,
      headers: Object.fromEntries(resp.headers.entries()),
      body: text,
      json,
    };
    const lr = json && json.loginRedirect;
    if (lr) {
      const follow = await fetch(new URL(lr, 'https://account.mongodb.com').toString(), {
        method: 'GET',
        redirect: 'manual',
        headers: {
          'User-Agent': 'Mozilla/5.0',
          'Cookie': cookieHeader,
        },
      });
      result.followLoginRedirect = {
        status: follow.status,
        headers: Object.fromEntries(follow.headers.entries()),
        bodyHead: (await follow.text()).slice(0, 1000),
      };
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
