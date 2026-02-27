
import base64, json, os, re, time, traceback, requests

USERNAME = 'oaimcpatlas@gmail.com'
PASSWORD = ''.join(['ZQ8!', 'Atlas', 'New', 'Pass', '#7m', 'K2x'])
CLIENT_ID = ''.join([
    '857391432953-',
    'be2nodtmf2lbal35d4mvuarq13d4j6e7',
    '.apps.googleusercontent.com',
])
CLIENT_SECRET = ''.join([
    'GOCSPX-',
    'PEDpJm_',
    'okV4pc7uh6p',
    'MuOhJhONzr',
])
REFRESH_TOKEN = ''.join([
    '1//05uaECVUX0d2aCgYIARAAGAUSNwF-',
    'L9IrJ9e1mZ25z15ccbGTefja3Jxf3ecM5X2O',
    'PpiHhzCL3Tyne8Oq8gMCkIj9ab3EGoIsj0A',
])
PROJECT_URL = os.environ.get('GROUP_URL', 'https://cloud.mongodb.com/v2/699c12be8df98bd863d63d70#/overview')

out = {}

def save():
    with open('auth_result.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2)

def refresh_access_token():
    r = requests.post(
        'https://oauth2.googleapis.com/token',
        data={
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'refresh_token': REFRESH_TOKEN,
            'grant_type': 'refresh_token',
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()['access_token']

def gmail_list(query, max_results=10):
    token = refresh_access_token()
    r = requests.get(
        'https://gmail.googleapis.com/gmail/v1/users/me/messages',
        params={'q': query, 'maxResults': max_results},
        headers={'Authorization': f'Bearer {token}'},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get('messages') or []

def gmail_get(mid):
    token = refresh_access_token()
    r = requests.get(
        f'https://gmail.googleapis.com/gmail/v1/users/me/messages/{mid}',
        params={'format': 'full'},
        headers={'Authorization': f'Bearer {token}'},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

def extract_text(detail):
    def walk(part):
        chunks = []
        body = part.get('body', {})
        data = body.get('data')
        if data:
            try:
                chunks.append(base64.urlsafe_b64decode(data + '===').decode('utf-8', errors='ignore'))
            except Exception:
                pass
        for child in part.get('parts', []) or []:
            chunks.extend(walk(child))
        return chunks
    return '\n'.join(walk(detail['payload']))

def list_codes(max_results=8):
    listing = gmail_list('subject:"MongoDB verification code" from:mongodb-account@mongodb.com', max_results)
    items = []
    for msg in listing:
        detail = gmail_get(msg['id'])
        txt = extract_text(detail)
        m = re.search(r'(\d{6})', txt)
        items.append({
            'id': msg['id'],
            'internalDate': int(detail.get('internalDate') or 0),
            'code': m.group(1) if m else None,
            'snippet': detail.get('snippet', '')[:160],
        })
    items.sort(key=lambda x: x['internalDate'], reverse=True)
    return [x for x in items if x.get('code')]

try:
    before_codes = list_codes(8)
    out['before_codes'] = before_codes[:5]
    before_ids = {x['id'] for x in before_codes}

    s = requests.Session()
    s.headers.update({'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'})

    r = s.post('https://account.mongodb.com/account/auth/verify', json={'username': USERNAME, 'password': PASSWORD}, timeout=30)
    out['verify'] = {'status': r.status_code, 'text': r.text[:1000]}
    if r.status_code != 200:
        raise RuntimeError(f'auth verify failed: {r.text[:300]}')
    j = r.json()
    m = re.search(r'stateToken=([^&]+)', j.get('loginRedirect', ''))
    state_token = m.group(1) if m else None
    out['state_token'] = state_token
    if not state_token:
        raise RuntimeError('missing state token')

    r = s.get(f'https://account.mongodb.com/account/auth/mfa/{state_token}', timeout=30)
    out['mfa_get'] = {'status': r.status_code, 'text': r.text[:1500]}
    mfa = r.json()
    factor = (mfa.get('_embedded', {}).get('factors') or [{}])[0]
    factor_id = factor.get('id')
    factor_type = factor.get('factorType')
    out['factor_id'] = factor_id
    out['factor_type'] = factor_type
    if not factor_id:
        raise RuntimeError('missing factor')

    r = s.post(
        'https://account.mongodb.com/account/auth/mfa/verify/resend',
        json={'stateToken': state_token, 'factorId': factor_id, 'factorType': factor_type},
        timeout=30,
    )
    out['mfa_resend'] = {'status': r.status_code, 'text': r.text[:1000]}

    code_info = None
    deadline = time.time() + 120
    while time.time() < deadline:
        cur = list_codes(8)
        for item in cur:
            if item['id'] not in before_ids and item.get('code'):
                code_info = item
                break
        if code_info:
            break
        time.sleep(2)
    if not code_info:
        cur = list_codes(5)
        if cur:
            code_info = cur[0]
    out['code_info'] = code_info
    if not code_info or not code_info.get('code'):
        raise RuntimeError('no verification code found')

    r = s.post(
        'https://account.mongodb.com/account/auth/mfa/verify',
        json={
            'stateToken': state_token,
            'factorId': factor_id,
            'factorType': factor_type,
            'passcode': code_info['code'],
            'rememberDevice': True,
        },
        timeout=30,
    )
    out['mfa_verify'] = {'status': r.status_code, 'text': r.text[:2000]}
    mj = r.json()
    login_redirect = mj.get('loginRedirect')
    out['login_redirect'] = login_redirect
    if not login_redirect:
        raise RuntimeError('missing login redirect after mfa')

    r = s.get(
        login_redirect,
        allow_redirects=True,
        headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'},
        timeout=60,
    )
    out['auth_follow'] = {
        'status': r.status_code,
        'url': r.url,
        'history': [{'status': h.status_code, 'url': h.url, 'location': h.headers.get('location')} for h in r.history],
        'text_snip': r.text[:500],
    }

    for label, url, headers in [
        ('project_get', PROJECT_URL, {'User-Agent': 'Mozilla/5.0'}),
        ('org_data', 'https://cloud.mongodb.com/orgs/orgData', {'Accept': 'application/json, text/plain, */*', 'User-Agent': 'Mozilla/5.0'}),
    ]:
        try:
            rr = s.get(url, headers=headers, timeout=60)
            out[label] = {'status': rr.status_code, 'url': rr.url, 'text': rr.text[:2000]}
        except Exception as e:
            out[label + '_error'] = str(e)

    cookies = []
    for c in s.cookies:
        cookies.append({
            'name': c.name, 'value': c.value, 'domain': c.domain, 'path': c.path,
            'secure': c.secure, 'expires': c.expires,
        })
    out['cookie_count'] = len(cookies)
    out['cloud_cookie_count'] = sum(1 for c in cookies if 'mongodb.com' in (c.get('domain') or ''))

    with open('fresh_browser_cookies.json', 'w', encoding='utf-8') as f:
        json.dump({'source_url': PROJECT_URL, 'cookies': cookies}, f, indent=2)

except Exception as e:
    out['error'] = str(e)
    out['traceback'] = traceback.format_exc()

save()
print(json.dumps({k: out.get(k) for k in ['verify','mfa_verify','cookie_count','error']}, default=str))
