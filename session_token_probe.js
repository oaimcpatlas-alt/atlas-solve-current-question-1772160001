
const fs = require('fs');

const LOGIN_URL = "https://auth.mongodb.com/oauth2/default/v1/authorize?client_id=0oa2h8np9ryZR4Xat297&redirect_uri=https%3A%2F%2Faccount.mongodb.com%2Faccount%2Foidc%2Fcallback&response_type=code&response_mode=query&scope=openid+email&state=%7B%22stateNonce%22%3A%227ac314bf-7346-4b1c-8ef6-087292241148%22%7D&nonce=7226ca6c-25c0-4e7c-bdb6-858db3afef74&sessionToken=20111iTBfo3iYJ2_Pw9jY6Gu4P0bs1beQxpvRcOYBCTQV1JC224SArB";
const PROJECT_ID = '699c12be8df98bd863d63d70';

const seedCookies = [{"name": "okta-oauth-state", "value": "%7B%22stateNonce%22%3A%227ac314bf-7346-4b1c-8ef6-087292241148%22%7D", "domain": "account.mongodb.com", "path": "/"}, {"name": "okta-oauth-nonce", "value": "7226ca6c-25c0-4e7c-bdb6-858db3afef74", "domain": "account.mongodb.com", "path": "/"}, {"name": "oidc-init-user-prod", "value": "672a6fb2df247d1d0dabda3c3482b4af9b9faba5a46f5e8cd20b01d3c4e50954-73eb7f272444d9add0eb26b6dd760ead4b17c80f381523eddb32746579a6619479933dc63dc1855d5c19f4a39193a5580866edd278016db04c6eb8a3c468828a", "domain": "account.mongodb.com", "path": "/"}];

function write(obj) {
  fs.writeFileSync('session_token_probe_result.json', JSON.stringify(obj, null, 2));
}

function parseSetCookie(setCookieValue) {
  // very simple parser: first name=value up to ;
  const first = String(setCookieValue || '').split(';')[0];
  const eq = first.indexOf('=');
  if (eq === -1) return null;
  return { name: first.slice(0, eq), value: first.slice(eq + 1) };
}

async function main() {
  const result = { loginUrl: LOGIN_URL };
  const jar = new Map();

  // seed with fresh auth-flow cookies
  for (const c of seedCookies) {
    jar.set(c.name, c.value);
  }

  // add a couple of device-related cookies from browser_cookies.json if present
  try {
    const raw = JSON.parse(fs.readFileSync('browser_cookies.json', 'utf8'));
    const cookies = Array.isArray(raw.cookies) ? raw.cookies : [];
    for (const c of cookies) {
      if (['remember-user-device', 'user-device'].includes(c.name) && typeof c.value === 'string' && c.value) {
        jar.set(c.name, c.value);
      }
    }
  } catch (e) {
    result.browserCookieReadError = String(e && e.message || e);
  }

  function cookieHeader() {
    return Array.from(jar.entries()).map(([k,v]) => `${k}=${v}`).join('; ');
  }

  async function step(url, label) {
    const resp = await fetch(url, {
      method: 'GET',
      redirect: 'manual',
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Cookie': cookieHeader(),
      },
    });
    let setCookies = [];
    try {
      if (typeof resp.headers.getSetCookie === 'function') {
        setCookies = resp.headers.getSetCookie();
      } else {
        const sc = resp.headers.get('set-cookie');
        if (sc) setCookies = [sc];
      }
    } catch {}
    const added = [];
    for (const rawSet of setCookies) {
      const parsed = parseSetCookie(rawSet);
      if (parsed) {
        jar.set(parsed.name, parsed.value);
        added.push(parsed.name);
      }
    }
    const body = await resp.text();
    const out = {
      label,
      url,
      status: resp.status,
      headers: Object.fromEntries(resp.headers.entries()),
      setCookieNames: added,
      bodyHead: body.slice(0, 1200),
    };
    result.steps = result.steps || [];
    result.steps.push(out);
    return out;
  }

  try {
    const s1 = await step(LOGIN_URL, 'loginRedirect');
    const next1 = s1.headers.location;
    if (next1) {
      const s2 = await step(next1, 'redirect1');
      const next2 = s2.headers.location;
      if (next2) {
        const absolute2 = next2.startsWith('http') ? next2 : new URL(next2, next1).toString();
        const s3 = await step(absolute2, 'redirect2');
        const next3 = s3.headers.location;
        if (next3) {
          const absolute3 = next3.startsWith('http') ? next3 : new URL(next3, absolute2).toString();
          await step(absolute3, 'redirect3');
        }
      }
    }

    result.jarNames = Array.from(jar.keys());

    // Probe account and cloud after following redirects
    const acct = await fetch('https://account.mongodb.com/account/profile/info', {
      method: 'GET',
      redirect: 'manual',
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Cookie': cookieHeader(),
      },
    });
    result.accountProfile = {
      status: acct.status,
      headers: Object.fromEntries(acct.headers.entries()),
      bodyHead: (await acct.text()).slice(0, 800),
    };

    const cloud = await fetch('https://cloud.mongodb.com/v2/' + PROJECT_ID + '#/overview', {
      method: 'GET',
      redirect: 'manual',
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Cookie': cookieHeader(),
      },
    });
    result.cloudOverview = {
      status: cloud.status,
      headers: Object.fromEntries(cloud.headers.entries()),
      bodyHead: (await cloud.text()).slice(0, 800),
    };

    const conn = await fetch(`https://cloud.mongodb.com/explorer/v1/groups/${PROJECT_ID}/clusters/connectionInfo`, {
      method: 'GET',
      redirect: 'manual',
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Cookie': cookieHeader(),
      },
    });
    result.connectionInfo = {
      status: conn.status,
      headers: Object.fromEntries(conn.headers.entries()),
      bodyHead: (await conn.text()).slice(0, 800),
    };
  } catch (e) {
    result.error = String(e && e.message || e);
    result.stack = e && e.stack || null;
  } finally {
    write(result);
  }
}

main();
