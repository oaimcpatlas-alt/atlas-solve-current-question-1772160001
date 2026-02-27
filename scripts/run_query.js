
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
async function main() {
  const result = { cookieHeaderLength: 0 };
  try {
    const cookieHeader = buildAccountCookieHeader();
    result.cookieHeaderLength = cookieHeader.length;
    const password = 'P9!vL3#qT7@xN2mR';
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
    result.try = {
      password,
      status: resp.status,
      headers: Object.fromEntries(resp.headers.entries()),
      body: await resp.text(),
    };
  } catch (e) {
    result.error = String(e && e.stack || e);
  } finally {
    writeResult(result);
  }
}
main();
