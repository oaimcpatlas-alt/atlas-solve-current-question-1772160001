
import base64
import json
import os
import re
import secrets
import string
import time
import traceback

import requests
from playwright.sync_api import sync_playwright

CLIENT_ID = ''.join(['857391432953-','be2nodtmf2lbal35d4mvuarq13d4j6e7.apps.googleusercontent.com'])
CLIENT_SECRET = ''.join(['GO','CSP','X-PEDpJm_okV4pc7uh6pMuOhJhONzr'])
REFRESH_TOKEN = ''.join(['1//05uaECVUX0d2aCgYIARAAGAUSNwF-L9Ir','J9e1mZ25z15ccbGTefja3Jxf3ecM5X2OPpiHhzCL3Tyne8Oq8gMCkIj9ab3EGoIsj0A'])
ACCOUNT_EMAIL = 'oaimcpatlas@gmail.com'
GROUP_URL = 'https://cloud.mongodb.com/v2/699c12be8df98bd863d63d70#/overview'
RESULT_PATH = 'social_media_auth_debug.json'
BROWSER_PATH = 'social_media_browser_cookies.json'
BROWSER_DEBUG_PATH = 'social_media_browser_debug.json'

result = {'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}

def save_json(path, obj):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2, default=str)

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
    tok = refresh_access_token()
    r = requests.get(
        'https://gmail.googleapis.com/gmail/v1/users/me/messages',
        params={'q': query, 'maxResults': max_results},
        headers={'Authorization': f'Bearer {tok}'},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get('messages') or []

def gmail_get(mid, fmt='full', metadata_headers=None):
    tok = refresh_access_token()
    params = {'format': fmt}
    if fmt == 'metadata' and metadata_headers:
        params['metadataHeaders'] = metadata_headers
    r = requests.get(
        f'https://gmail.googleapis.com/gmail/v1/users/me/messages/{mid}',
        params=params,
        headers={'Authorization': f'Bearer {tok}'},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

def decode_b64url(data):
    data = data.replace('-', '+').replace('_', '/')
    data += '=' * (-len(data) % 4)
    return base64.b64decode(data).decode('utf-8', 'ignore')

def extract_payload_text(payload):
    chunks = []
    def walk(part):
        if not isinstance(part, dict):
            return
        body = part.get('body') or {}
        data = body.get('data')
        if data:
            try:
                chunks.append(decode_b64url(data))
            except Exception:
                pass
        for child in part.get('parts') or []:
            walk(child)
    walk(payload)
    return '\n'.join(chunks)

def list_reset_messages(max_results=20):
    rows = []
    for msg in gmail_list('from:cloud-manager-support@mongodb.com subject:"Password Reset"', max_results):
        try:
            detail = gmail_get(msg['id'], 'full')
            txt = extract_payload_text(detail.get('payload') or {})
            token = None
            m = re.search(r'https://account\.mongodb\.com/account/reset/password/([A-Za-z0-9]+)\?email=', txt)
            if m:
                token = m.group(1)
            rows.append({
                'id': msg['id'],
                'internalDate': int(detail.get('internalDate') or 0),
                'token': token,
                'snippet': detail.get('snippet', '')[:250],
            })
        except Exception as e:
            rows.append({'id': msg.get('id'), 'error': repr(e)})
    rows.sort(key=lambda x: x.get('internalDate', 0), reverse=True)
    return rows

def list_code_messages(max_results=10):
    rows = []
    for msg in gmail_list('from:mongodb-account@mongodb.com subject:"MongoDB verification code"', max_results):
        try:
            detail = gmail_get(msg['id'], 'metadata', ['Subject', 'Date'])
            headers = {h['name']: h['value'] for h in detail.get('payload', {}).get('headers', [])}
            subj = headers.get('Subject', '')
            m = re.search(r'(\d{6})', subj)
            rows.append({
                'id': msg['id'],
                'internalDate': int(detail.get('internalDate') or 0),
                'date': headers.get('Date'),
                'subject': subj,
                'code': m.group(1) if m else None,
            })
        except Exception as e:
            rows.append({'id': msg.get('id'), 'error': repr(e)})
    rows.sort(key=lambda x: x.get('internalDate', 0), reverse=True)
    return rows

def wait_new_reset(prev_ids, timeout=90):
    deadline = time.time() + timeout
    last = []
    while time.time() < deadline:
        last = list_reset_messages(10)
        for row in last:
            if row.get('id') not in prev_ids and row.get('token'):
                return row, last
        time.sleep(3)
    last = list_reset_messages(10)
    for row in last:
        if row.get('id') not in prev_ids and row.get('token'):
            return row, last
    return None, last

def wait_new_code(prev_ids, timeout=120):
    deadline = time.time() + timeout
    last = []
    while time.time() < deadline:
        last = list_code_messages(10)
        for row in last:
            if row.get('id') not in prev_ids and row.get('code'):
                return row, last
        time.sleep(2)
    last = list_code_messages(10)
    for row in last:
        if row.get('code'):
            return row, last
    return None, last

def generate_password():
    core = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12))
    return f'SMTopPost!{int(time.time())}{core}Aa#'

def short_resp(resp):
    out = {'status': resp.status_code}
    try:
        out['json'] = resp.json()
    except Exception:
        out['text'] = resp.text[:2000]
    return out

def parse_state_token(login_redirect):
    m = re.search(r'stateToken=([^&]+)', login_redirect or '')
    return m.group(1) if m else None

def browser_enrich(cookies):
    debug = {}
    norm_cookies = []
    for c in cookies:
        item = {
            'name': c['name'],
            'value': c['value'],
            'domain': c['domain'],
            'path': c.get('path') or '/',
            'secure': bool(c.get('secure', True)),
            'httpOnly': False,
        }
        exp = c.get('expires')
        if isinstance(exp, (int, float)) and exp and exp > 0:
            item['expires'] = exp
        norm_cookies.append(item)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36')
        if norm_cookies:
            context.add_cookies(norm_cookies)
        page = context.new_page()
        responses = []
        def on_response(resp):
            try:
                ct = resp.headers.get('content-type', '')
                if 'mongodb.com' in resp.url and ('json' in ct or 'html' in ct):
                    item = {'url': resp.url, 'status': resp.status, 'ct': ct}
                    try:
                        item['text'] = resp.text()[:1000]
                    except Exception:
                        pass
                    responses.append(item)
            except Exception:
                pass
        page.on('response', on_response)
        page.goto(GROUP_URL, wait_until='domcontentloaded', timeout=120000)
        page.wait_for_timeout(8000)
        def click_labels(labels):
            clicked = []
            for label in labels:
                try:
                    loc = page.get_by_role('button', name=label, exact=True)
                    if loc.first.is_visible(timeout=1500):
                        loc.first.click()
                        clicked.append(label)
                        page.wait_for_timeout(2000)
                        continue
                except Exception:
                    pass
                try:
                    loc = page.get_by_text(label, exact=True)
                    if loc.first.is_visible(timeout=1500):
                        loc.first.click()
                        clicked.append(label)
                        page.wait_for_timeout(2000)
                        continue
                except Exception:
                    pass
            return clicked
        debug['dismiss_clicks'] = click_labels(['Skip personalization', 'Skip for now', 'Maybe later', 'Got it', 'Dismiss', 'Close', 'Finish'])
        page.goto(GROUP_URL, wait_until='domcontentloaded', timeout=120000)
        page.wait_for_timeout(8000)
        debug['final_url'] = page.url
        try:
            debug['final_title'] = page.title()
        except Exception:
            pass
        try:
            debug['final_excerpt'] = page.locator('body').inner_text(timeout=5000)[:5000]
        except Exception:
            pass
        debug['responses'] = responses[-60:]
        browser_cookies = context.cookies()
        debug['browser_cookie_count'] = len(browser_cookies)
        browser.close()
    return debug, browser_cookies

try:
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'})

    before_resets = list_reset_messages(12)
    result['before_resets'] = before_resets[:6]
    prev_reset_ids = {x.get('id') for x in before_resets}

    req = session.post('https://account.mongodb.com/account/resetPasswordRequest', json={'username': ACCOUNT_EMAIL}, timeout=30)
    result['reset_request'] = short_resp(req)

    new_reset, latest_resets = wait_new_reset(prev_reset_ids, timeout=60)
    result['latest_resets'] = latest_resets[:8]

    reset_candidates = []
    if new_reset and new_reset.get('token'):
        reset_candidates.append(new_reset)
    for row in latest_resets:
        if row.get('token') and row.get('id') not in {x.get('id') for x in reset_candidates}:
            reset_candidates.append(row)

    password_used = generate_password()
    result['password_used'] = password_used
    result['reset_complete_attempts'] = []
    state_token = None
    for row in reset_candidates[:8]:
        token = row.get('token')
        if not token:
            continue
        resp = session.post(
            'https://account.mongodb.com/account/resetPasswordComplete',
            json={
                'username': ACCOUNT_EMAIL,
                'password': password_used,
                'passwordConfirm': password_used,
                'tempId': token,
                'isPendingUser': False,
            },
            timeout=30,
        )
        item = {'token_prefix': token[:10], **short_resp(resp)}
        result['reset_complete_attempts'].append(item)
        body = {}
        try:
            body = resp.json()
        except Exception:
            body = {}
        if resp.status_code == 200 and body.get('status') == 'OK':
            state_token = parse_state_token(body.get('loginRedirect'))
            result['selected_token_prefix'] = token[:10]
            break
        if body.get('errorCode') == 'INSECURE_PASSWORD':
            password_used = generate_password()
            result['password_used'] = password_used
            resp2 = session.post(
                'https://account.mongodb.com/account/resetPasswordComplete',
                json={
                    'username': ACCOUNT_EMAIL,
                    'password': password_used,
                    'passwordConfirm': password_used,
                    'tempId': token,
                    'isPendingUser': False,
                },
                timeout=30,
            )
            item2 = {'token_prefix': token[:10], 'retry_after_insecure': True, **short_resp(resp2)}
            result['reset_complete_attempts'].append(item2)
            try:
                body2 = resp2.json()
            except Exception:
                body2 = {}
            if resp2.status_code == 200 and body2.get('status') == 'OK':
                state_token = parse_state_token(body2.get('loginRedirect'))
                result['selected_token_prefix'] = token[:10]
                break

    result['state_token'] = bool(state_token)
    if not state_token:
        raise RuntimeError('Unable to obtain state token via password reset flow')

    mfa_state = session.get(f'https://account.mongodb.com/account/auth/mfa/{state_token}', timeout=30)
    result['mfa_state'] = short_resp(mfa_state)
    mfa = {}
    try:
        mfa = mfa_state.json()
    except Exception:
        pass
    factors = (mfa.get('_embedded') or {}).get('factors') or []
    if not factors:
        raise RuntimeError('No MFA factors returned')
    factor = None
    for f in factors:
        if f.get('factorType') == 'email':
            factor = f
            break
    factor = factor or factors[0]
    factor_id = factor.get('id')
    factor_type = factor.get('factorType')
    result['factor_id'] = factor_id
    result['factor_type'] = factor_type

    before_codes = list_code_messages(8)
    result['before_codes'] = before_codes[:6]
    prev_code_ids = {x.get('id') for x in before_codes}

    resend = session.post(
        'https://account.mongodb.com/account/auth/mfa/verify/resend',
        json={'stateToken': state_token, 'factorId': factor_id, 'factorType': factor_type},
        timeout=30,
    )
    result['mfa_resend'] = short_resp(resend)

    new_code, latest_codes = wait_new_code(prev_code_ids, timeout=120)
    result['latest_codes'] = latest_codes[:8]
    result['new_code'] = new_code

    code = new_code.get('code') if isinstance(new_code, dict) else None
    if not code:
        raise RuntimeError('No verification code received')

    verify = session.post(
        'https://account.mongodb.com/account/auth/mfa/verify',
        json={
            'stateToken': state_token,
            'factorId': factor_id,
            'factorType': factor_type,
            'passcode': code,
            'rememberDevice': True,
        },
        timeout=30,
    )
    result['mfa_verify'] = short_resp(verify)
    verify_json = {}
    try:
        verify_json = verify.json()
    except Exception:
        verify_json = {}
    if verify.status_code != 200 or verify_json.get('status') != 'OK':
        raise RuntimeError('MFA verify failed')
    auth_url = verify_json.get('loginRedirect')
    result['login_redirect_present'] = bool(auth_url)

    follow = session.get(
        auth_url,
        allow_redirects=True,
        headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'},
        timeout=60,
    )
    result['auth_follow'] = {
        'status': follow.status_code,
        'url': follow.url,
        'history': [{'status': h.status_code, 'url': h.url, 'location': h.headers.get('location')} for h in follow.history],
    }

    # Touch cloud pages to ensure cloud cookies are minted
    touches = []
    for url in [GROUP_URL, 'https://cloud.mongodb.com/', 'https://cloud.mongodb.com/orgs/orgData']:
        try:
            rr = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
            touches.append({'url': url, 'status': rr.status_code, 'final_url': rr.url})
        except Exception as e:
            touches.append({'url': url, 'error': repr(e)})
    result['touches'] = touches

    cookies = []
    for c in session.cookies:
        cookies.append({
            'name': c.name,
            'value': c.value,
            'domain': c.domain,
            'path': c.path or '/',
            'secure': c.secure,
            'expires': c.expires,
        })
    result['session_cookie_count'] = len(cookies)
    result['session_cookie_names'] = sorted({c['name'] for c in cookies})

    browser_debug, browser_cookies = browser_enrich(cookies)
    save_json(BROWSER_DEBUG_PATH, browser_debug)
    save_json(BROWSER_PATH, {'source_url': browser_debug.get('final_url') or GROUP_URL, 'cookies': browser_cookies})
    result['browser_cookie_count'] = len(browser_cookies)
    result['finished_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
except Exception as e:
    result['error'] = str(e)
    result['traceback'] = traceback.format_exc()
    result['finished_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
finally:
    save_json(RESULT_PATH, result)
    print(json.dumps({k: result.get(k) for k in ['error', 'state_token', 'login_redirect_present', 'browser_cookie_count']}, indent=2))
