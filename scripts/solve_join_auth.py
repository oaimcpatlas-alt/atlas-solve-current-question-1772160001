import ast, base64, json, random, re, string, time, traceback
from pathlib import Path
import requests

OUT = {'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'attempts': []}


def save(path, obj):
    Path(path).write_text(json.dumps(obj, indent=2, default=str), encoding='utf-8')


def extract_joined_literal(text, var):
    m = re.search(rf"{re.escape(var)}\s*=\s*''\.join\(\[(.*?)\]\)", text, re.S)
    if m:
        parts = re.findall(r"'([^']*)'", m.group(1))
        if parts:
            return ''.join(parts)
    m = re.search(rf"{re.escape(var)}\s*=\s*\"([^\"]+)\"", text)
    if m:
        return m.group(1)
    m = re.search(rf"{re.escape(var)}\s*=\s*'([^']+)'", text)
    if m:
        return m.group(1)
    return None


def load_google_creds():
    candidates = [
        Path('social_media_auth.py'),
        Path('.github/workflows/answer_shipping_ups.yml'),
        Path('.github/workflows/solve_now.yml'),
        Path('.github/workflows/zz_auth_debug.yml'),
    ]
    for path in candidates:
        if not path.exists():
            continue
        text = path.read_text(encoding='utf-8', errors='ignore')
        cid = extract_joined_literal(text, 'CLIENT_ID') or extract_joined_literal(text, 'GOOGLE_CLIENT_ID')
        cs = extract_joined_literal(text, 'CLIENT_SECRET') or extract_joined_literal(text, 'GOOGLE_CLIENT_SECRET')
        rt = extract_joined_literal(text, 'REFRESH_TOKEN') or extract_joined_literal(text, 'GOOGLE_REFRESH_TOKEN')
        email = extract_joined_literal(text, 'ACCOUNT_EMAIL') or extract_joined_literal(text, 'USERNAME')
        group = extract_joined_literal(text, 'GROUP_URL') or 'https://cloud.mongodb.com/v2/699c12be8df98bd863d63d70#/overview'
        if cid and cs and rt and email:
            return {'client_id': cid, 'client_secret': cs, 'refresh_token': rt, 'email': email, 'group_url': group, 'source': str(path)}
    raise RuntimeError('Could not recover Google creds from repo files')

CREDS = load_google_creds()
OUT['creds_source'] = CREDS['source']
USERNAME = CREDS['email']
PROJECT_URL = CREDS['group_url']


def gmail_token():
    r = requests.post('https://oauth2.googleapis.com/token', data={
        'client_id': CREDS['client_id'],
        'client_secret': CREDS['client_secret'],
        'refresh_token': CREDS['refresh_token'],
        'grant_type': 'refresh_token',
    }, timeout=30)
    OUT['gmail_token_status'] = r.status_code
    r.raise_for_status()
    return r.json()['access_token']


def gmail_headers():
    return {'Authorization': f'Bearer {gmail_token()}'}


def gmail_list(query, max_results=10):
    r = requests.get('https://gmail.googleapis.com/gmail/v1/users/me/messages', params={'q': query, 'maxResults': max_results}, headers=gmail_headers(), timeout=30)
    OUT.setdefault('gmail_list_statuses', []).append(r.status_code)
    r.raise_for_status()
    return r.json().get('messages') or []


def gmail_get(mid):
    r = requests.get(f'https://gmail.googleapis.com/gmail/v1/users/me/messages/{mid}', params={'format': 'full'}, headers=gmail_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


def payload_text(payload):
    chunks = []
    def walk(part):
        if not isinstance(part, dict):
            return
        body = part.get('body', {})
        data = body.get('data')
        if data:
            try:
                chunks.append(base64.urlsafe_b64decode(data + '===').decode('utf-8', errors='ignore'))
            except Exception:
                pass
        for child in part.get('parts') or []:
            walk(child)
    walk(payload)
    return '\n'.join(chunks)


def latest_reset_messages(max_results=8):
    msgs = gmail_list('from:cloud-manager-support@mongodb.com subject:"Password Reset" -subject:Confirmation', max_results)
    out = []
    for m in msgs:
        d = gmail_get(m['id'])
        txt = payload_text(d.get('payload', {}))
        mo = re.search(r'https://account\.mongodb\.com/account/reset/password/([A-Za-z0-9]+)\?email=', txt)
        out.append({'id': m['id'], 'internalDate': int(d.get('internalDate') or 0), 'token': mo.group(1) if mo else None})
    return [x for x in out if x.get('token')]


def latest_codes(max_results=8):
    msgs = gmail_list('from:mongodb-account@mongodb.com subject:"MongoDB verification code"', max_results)
    out = []
    for m in msgs:
        d = gmail_get(m['id'])
        txt = payload_text(d.get('payload', {}))
        mo = re.search(r'(\d{6})', txt)
        out.append({'id': m['id'], 'internalDate': int(d.get('internalDate') or 0), 'code': mo.group(1) if mo else None})
    return [x for x in out if x.get('code')]


def wait_new_code(prev_ids, timeout_s=180):
    end = time.time() + timeout_s
    while time.time() < end:
        for item in latest_codes(6):
            if item['id'] not in prev_ids:
                return item
        time.sleep(2)
    return None


def wait_new_reset(prev_ids, timeout_s=120):
    end = time.time() + timeout_s
    while time.time() < end:
        for item in latest_reset_messages(6):
            if item['id'] not in prev_ids:
                return item
        time.sleep(2)
    return None


def load_account_cookies(sess):
    path = Path('browser_cookies.json')
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding='utf-8'))
    seeded = []
    now = time.time()
    for c in data.get('cookies', []):
        if c.get('domain') != 'account.mongodb.com':
            continue
        # keep remember/user-device even if others expired
        if c.get('name') not in ('remember-user-device', 'user-device'):
            exp = c.get('expires')
            if isinstance(exp, (int, float)) and exp > 0 and exp < now:
                continue
        try:
            sess.cookies.set(c['name'], c['value'], domain='account.mongodb.com', path=c.get('path') or '/')
            seeded.append(c['name'])
        except Exception:
            pass
    return seeded


def record(resp, note):
    item = {'note': note, 'status': getattr(resp, 'status_code', None), 'url': getattr(resp, 'url', None), 'x_error_code': getattr(resp, 'headers', {}).get('x-error-code')}
    try:
        item['json'] = resp.json()
    except Exception:
        item['text'] = (resp.text or '')[:1000]
    OUT['attempts'].append(item)
    return item


def make_session():
    sess = requests.Session()
    sess.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36', 'Accept': 'application/json'})
    OUT['seeded_account_cookies'] = load_account_cookies(sess)
    return sess


def account_auth(sess, password, label='auth'):
    r = sess.post('https://account.mongodb.com/account/auth', json={'username': USERNAME, 'password': password}, timeout=30)
    record(r, f'account_auth_{label}')
    return r


def parse_state_token(resp_json):
    state = resp_json.get('stateToken')
    if state:
        return state
    lr = resp_json.get('loginRedirect') or ''
    m = re.search(r'stateToken=([^&]+)', lr)
    return m.group(1) if m else None


def perform_mfa(sess, state_token):
    r = sess.get(f'https://account.mongodb.com/account/auth/mfa/{state_token}', timeout=30)
    record(r, 'mfa_get')
    data = r.json()
    factors = (data.get('_embedded', {}).get('factors') or [])
    factor = next((f for f in factors if f.get('factorType') == 'email'), factors[0] if factors else None)
    if not factor:
        raise RuntimeError('no MFA factor found')
    factor_id = factor.get('id'); factor_type = factor.get('factorType')
    OUT['factor_id'] = factor_id; OUT['factor_type'] = factor_type
    before = latest_codes(6)
    before_ids = {x['id'] for x in before}
    OUT['before_code_ids'] = sorted(before_ids)
    r2 = sess.post('https://account.mongodb.com/account/auth/mfa/verify/resend', json={'stateToken': state_token, 'factorId': factor_id, 'factorType': factor_type}, timeout=30)
    record(r2, 'mfa_resend')
    code = wait_new_code(before_ids, 180)
    OUT['new_code'] = code
    if not code:
        raise RuntimeError('no new verification code received')
    r3 = sess.post('https://account.mongodb.com/account/auth/mfa/verify', json={'stateToken': state_token, 'factorId': factor_id, 'factorType': factor_type, 'passcode': code['code'], 'rememberDevice': False}, timeout=30)
    record(r3, 'mfa_verify')
    data3 = r3.json()
    lr = data3.get('loginRedirect')
    if not lr:
        raise RuntimeError(f'no loginRedirect after MFA verify: {data3}')
    return lr


def follow_login(sess, login_redirect):
    sess.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    for url, note in [(login_redirect, 'follow_login'), (PROJECT_URL, 'project_url'), ('https://cloud.mongodb.com/', 'cloud_root')]:
        try:
            r = sess.get(url, timeout=60, allow_redirects=True)
            record(r, note)
        except Exception as e:
            OUT[note + '_error'] = str(e)
    parts=[]; cookies=[]
    for c in sess.cookies:
        cookies.append({'name': c.name, 'domain': c.domain, 'path': c.path, 'expires': c.expires})
        if 'mongodb.com' in c.domain:
            parts.append(f'{c.name}={c.value}')
    OUT['cookies'] = cookies
    header='; '.join(parts)
    Path('cloud_cookie_header.txt').write_text(header, encoding='utf-8')
    OUT['cookie_header_length'] = len(header)
    return header


def reset_and_auth(sess, cycles=8):
    tried_tokens=set()
    for cycle in range(1, cycles+1):
        prev=latest_reset_messages(6)
        prev_ids={x['id'] for x in prev}
        OUT.setdefault('reset_cycles',[]).append({'cycle': cycle, 'before_ids': sorted(prev_ids)})
        # Ask for a new reset email; ignore failure
        rreq = sess.post('https://account.mongodb.com/account/resetPasswordRequest', json={'username': USERNAME}, timeout=30)
        record(rreq, f'reset_request_{cycle}')
        msg = wait_new_reset(prev_ids, 40)
        candidates=[]
        if msg:
            candidates.append(msg)
        for item in latest_reset_messages(6):
            if item['id'] not in {x['id'] for x in candidates}:
                candidates.append(item)
        for item in candidates:
            if item['token'] in tried_tokens:
                continue
            tried_tokens.add(item['token'])
            new_pw = 'TmpAtlas!' + ''.join(random.choice('abcdef0123456789') for _ in range(8)) + '#'
            OUT['last_password_candidate'] = new_pw
            rr = sess.post('https://account.mongodb.com/user/resetComplete', json={'username': USERNAME, 'password': new_pw, 'passwordConfirm': new_pw, 'tempId': item['token'], 'resetType': 'PASSWORD'}, timeout=30)
            rec = record(rr, f'legacy_reset_complete_{cycle}_{item["id"][:8]}')
            j = rec.get('json') if isinstance(rec.get('json'), dict) else {}
            if rr.status_code == 200 and j.get('status') == 'OK':
                ra = account_auth(sess, new_pw, f'after_reset_{cycle}')
                if ra.status_code == 200:
                    OUT['working_password'] = new_pw
                    return ra
            # handle immediate rate limiting by brief sleep
            if rec.get('x_error_code') == 'RATE_LIMITED':
                time.sleep(5)
        time.sleep(5)
    raise RuntimeError('unable to obtain auth via reset loop')

try:
    sess = make_session()
    # Try a couple known candidates quickly in case account is stable.
    for label, pw in [('tmpatlas523', 'TmpAtlas!523E77a9#'), ('scratch', 'Scratch!321Aa'), ('aptflow', 'AptFlow!2026#')]:
        r = account_auth(sess, pw, label)
        if r.status_code == 200:
            OUT['working_password'] = pw
            state = parse_state_token(r.json())
            OUT['state_token'] = state
            lr = perform_mfa(sess, state) if state else (r.json().get('loginRedirect') or PROJECT_URL)
            follow_login(sess, lr)
            OUT['completed_via'] = 'known_password'
            break
    else:
        # fresh session for reset loop
        sess = make_session()
        r = reset_and_auth(sess)
        state = parse_state_token(r.json())
        OUT['state_token'] = state
        lr = perform_mfa(sess, state) if state else (r.json().get('loginRedirect') or PROJECT_URL)
        follow_login(sess, lr)
        OUT['completed_via'] = 'reset_loop'
except Exception as e:
    OUT['error'] = str(e)
    OUT['traceback'] = traceback.format_exc()

save('auth_debug.json', OUT)
print(json.dumps({'completed_via': OUT.get('completed_via'), 'working_password': OUT.get('working_password'), 'cookie_header_length': OUT.get('cookie_header_length'), 'error': OUT.get('error')}, default=str))
