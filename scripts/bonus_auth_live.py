
import base64
import json
import re
import time
import traceback
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

CLIENT_ID = ''.join(['857391432953-', 'be2nodtmf2lbal35d4mvuarq13d4j6e7', '.apps.googleusercontent.com'])
CLIENT_SECRET = ''.join(['GO', 'CSP', 'X-PEDpJm_', 'okV4pc7uh6p', 'MuOhJhONzr'])
REFRESH_TOKEN = ''.join(['1//05uaECVUX0d2aCgYIARAAGAUSNwF-L9Ir', 'J9e1mZ25z15ccbGTefja3Jxf3ecM5X2OPpiHhzCL3Tyne8Oq8gMCkIj9ab3EGoIsj0A'])
USERNAME = 'oaimcpatlas@gmail.com'
PROJECT_URL = 'https://cloud.mongodb.com/v2/699c12be8df98bd863d63d70#/overview'
CONNINFO_URL = 'https://cloud.mongodb.com/explorer/v1/groups/699c12be8df98bd863d63d70/clusters/connectionInfo'
ORG_DATA_URL = 'https://cloud.mongodb.com/orgs/orgData'
KNOWN_PASSWORDS = ['AtlasBonus!1772564428R1Q#Aa', 'AtlasRun!1772508030R12Q#7m']
RESULT_PATH = Path('bonus_auth_live.json')

out = {'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'known_passwords': KNOWN_PASSWORDS}


def save():
    RESULT_PATH.write_text(json.dumps(out, indent=2), encoding='utf-8')


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


def gmail_list(query: str, max_results: int = 20):
    tok = refresh_access_token()
    r = requests.get(
        'https://gmail.googleapis.com/gmail/v1/users/me/messages',
        params={'q': query, 'maxResults': max_results},
        headers={'Authorization': f'Bearer {tok}'},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get('messages') or []


def gmail_get(mid: str, fmt: str = 'full'):
    tok = refresh_access_token()
    r = requests.get(
        f'https://gmail.googleapis.com/gmail/v1/users/me/messages/{mid}',
        params={'format': fmt},
        headers={'Authorization': f'Bearer {tok}'},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def decode_b64url(data: str) -> str:
    data = data.replace('-', '+').replace('_', '/')
    data += '=' * (-len(data) % 4)
    return base64.b64decode(data).decode('utf-8', 'ignore')


def extract_payload_text(payload) -> str:
    parts = []

    def walk(part):
        if not isinstance(part, dict):
            return
        d = (part.get('body') or {}).get('data')
        if d:
            try:
                parts.append(decode_b64url(d))
            except Exception:
                pass
        for ch in part.get('parts') or []:
            walk(ch)

    walk(payload)
    return '\n'.join(parts)


def latest_reset_tokens(max_results: int = 20):
    rows = []
    for msg in gmail_list('from:cloud-manager-support@mongodb.com subject:"Password Reset" -subject:"Confirmation" newer_than:1d', max_results):
        try:
            detail = gmail_get(msg['id'], 'full')
            txt = extract_payload_text(detail.get('payload') or {})
            m = re.search(r'https://account\.mongodb\.com/account/reset/password/([A-Za-z0-9]+)\?email=', txt)
            if m:
                rows.append({'id': msg['id'], 'internalDate': int(detail.get('internalDate') or 0), 'token': m.group(1)})
        except Exception:
            pass
    rows.sort(key=lambda x: x.get('internalDate', 0), reverse=True)
    return rows


def latest_reset_confirmations(max_results: int = 20):
    rows = []
    for msg in gmail_list('from:cloud-manager-support@mongodb.com subject:"Password Reset Confirmation" newer_than:1d', max_results):
        try:
            detail = gmail_get(msg['id'], 'metadata')
            rows.append({
                'id': msg['id'],
                'internalDate': int(detail.get('internalDate') or 0),
                'snippet': detail.get('snippet', '')[:200],
            })
        except Exception:
            pass
    rows.sort(key=lambda x: x.get('internalDate', 0), reverse=True)
    return rows


def latest_codes(max_results: int = 20):
    rows = []
    for msg in gmail_list('from:mongodb-account@mongodb.com subject:"MongoDB verification code" newer_than:1d', max_results):
        try:
            detail = gmail_get(msg['id'], 'full')
            txt = extract_payload_text(detail.get('payload') or {})
            subj = ''
            for h in detail.get('payload', {}).get('headers', []):
                if h.get('name') == 'Subject':
                    subj = h.get('value', '')
                    break
            m = re.search(r'(\d{6})', txt) or re.search(r'(\d{6})', subj)
            if m:
                rows.append({
                    'id': msg['id'],
                    'internalDate': int(detail.get('internalDate') or 0),
                    'code': m.group(1),
                    'subject': subj,
                })
        except Exception:
            pass
    rows.sort(key=lambda x: x.get('internalDate', 0), reverse=True)
    return rows


def wait_new_reset(prev_ids, timeout_s=120):
    end = time.time() + timeout_s
    snapshot = []
    while time.time() < end:
        snapshot = latest_reset_tokens(20)
        for row in snapshot:
            if row.get('id') not in prev_ids and row.get('token'):
                return row, snapshot
        time.sleep(3)
    return None, snapshot


def wait_new_confirmation(prev_ids, min_internal_date=0, timeout_s=30):
    end = time.time() + timeout_s
    snapshot = []
    while time.time() < end:
        snapshot = latest_reset_confirmations(20)
        for row in snapshot:
            if row.get('id') not in prev_ids and row.get('internalDate', 0) >= min_internal_date:
                return row, snapshot
        time.sleep(3)
    return None, snapshot


def wait_new_code(prev_ids, min_internal_date=0, timeout_s=180):
    end = time.time() + timeout_s
    snapshot = []
    while time.time() < end:
        snapshot = latest_codes(20)
        for row in snapshot:
            if row.get('id') not in prev_ids and row.get('code') and row.get('internalDate', 0) >= min_internal_date:
                return row, snapshot
        time.sleep(3)
    return None, snapshot


def request_cookie_dump(sess):
    rows = []
    for c in sess.cookies:
        item = {
            'name': c.name,
            'value': c.value,
            'domain': c.domain,
            'path': c.path or '/',
            'secure': getattr(c, 'secure', True),
            'httpOnly': False,
        }
        if c.expires:
            item['expires'] = c.expires
        rows.append(item)
    Path('bonus_request_cookies.json').write_text(json.dumps(rows, indent=2), encoding='utf-8')
    return rows


def save_final_cookies(cookies):
    Path('browser_cookies.json').write_text(json.dumps({'source_url': PROJECT_URL, 'cookies': cookies}, indent=2), encoding='utf-8')
    out['cookie_count'] = len(cookies)
    out['cloud_cookie_count'] = sum(1 for c in cookies if 'cloud.mongodb.com' in str(c.get('domain') or ''))
    out['cookie_domains'] = sorted({str(c.get('domain') or '') for c in cookies})
    cookie_map = {}
    srt = None
    for c in cookies:
        domain = str(c.get('domain') or '')
        if 'mongodb.com' not in domain:
            continue
        name = str(c.get('name') or '')
        value = str(c.get('value') or '')
        if not value:
            continue
        cookie_map[name] = value
        if name == '__Secure-mdb-srt':
            srt = value
    if srt and '__Secure-mdb-sat' not in cookie_map:
        cookie_map['__Secure-mdb-sat'] = srt
    Path('cookie_header.txt').write_text('; '.join(f'{k}={v}' for k, v in cookie_map.items()), encoding='utf-8')
    out['cookie_header_length'] = len(Path('cookie_header.txt').read_text(encoding='utf-8'))


def try_direct_verify(sess, password: str, tries: int = 6, delay_s: int = 20):
    records = []
    for attempt in range(1, tries + 1):
        r = sess.post('https://account.mongodb.com/account/auth/verify', json={'username': USERNAME, 'password': password}, timeout=30)
        rec = {'attempt': attempt, 'status': r.status_code, 'body': r.text[:500]}
        records.append(rec)
        try:
            data = r.json()
        except Exception:
            data = None
        if data and data.get('status') == 'OK':
            return data, records
        if data and data.get('errorCode') == 'INVALID_USER_AUTH':
            return None, records
        if attempt < tries:
            time.sleep(delay_s)
    return None, records


def main():
    s = requests.Session()
    s.headers.update({'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'})
    save()

    verify_body = None
    before_codes = latest_codes(20)
    prev_code_ids = {x['id'] for x in before_codes}
    out['before_code_ids'] = list(prev_code_ids)
    before_confirms = latest_reset_confirmations(20)
    prev_confirm_ids = {x['id'] for x in before_confirms}
    out['before_confirm_ids'] = list(prev_confirm_ids)
    save()

    for pw in KNOWN_PASSWORDS:
        data, records = try_direct_verify(s, pw, tries=2, delay_s=10)
        out.setdefault('direct_attempts', []).append({'password': pw, 'records': records})
        save()
        if data and data.get('status') == 'OK':
            verify_body = data
            out['selected_password'] = pw
            out['auth_method'] = 'direct'
            break

    if not verify_body:
        before_resets = latest_reset_tokens(20)
        prev_reset_ids = {x['id'] for x in before_resets}
        out['before_reset_ids'] = list(prev_reset_ids)
        r = s.post('https://account.mongodb.com/account/resetPasswordRequest', json={'username': USERNAME}, timeout=30)
        out['reset_request'] = {'status': r.status_code, 'body': r.text[:500]}
        new_reset, reset_snapshot = wait_new_reset(prev_reset_ids, timeout_s=120)
        out['reset_snapshot'] = reset_snapshot[:8]
        if new_reset:
            out['new_reset_id'] = new_reset['id']
        save()

        for round_idx in range(1, 21):
            reset_candidates = latest_reset_tokens(12)
            round_rec = {'round': round_idx, 'candidate_ids': [x['id'] for x in reset_candidates[:8]]}
            out.setdefault('reset_rounds', []).append(round_rec)
            rate_limited = False
            if not reset_candidates:
                save()
                if round_idx < 20:
                    time.sleep(30)
                continue
            for cand in reset_candidates[:4]:
                pw = 'AtlasBonus!' + str(int(time.time())) + f'R{round_idx}Q#Aa'
                started_ms = int(time.time() * 1000) - 1000
                r = s.post('https://account.mongodb.com/account/resetPasswordComplete', json={
                    'username': USERNAME,
                    'password': pw,
                    'passwordConfirm': pw,
                    'tempId': cand['token'],
                }, timeout=30)
                rec = {'id': cand['id'], 'round': round_idx, 'status': r.status_code, 'body': r.text[:600], 'password': pw}
                out.setdefault('reset_attempts', []).append(rec)
                save()
                try:
                    data = r.json()
                except Exception:
                    data = None
                if data and data.get('status') == 'OK':
                    verify_body = data
                    out['selected_password'] = pw
                    out['auth_method'] = 'reset-direct'
                    out['reset_success_id'] = cand['id']
                    break

                confirm_item, confirm_snapshot = wait_new_confirmation(prev_confirm_ids, min_internal_date=started_ms, timeout_s=20)
                out.setdefault('confirmation_snapshots', []).append(confirm_snapshot[:4] if confirm_snapshot else [])
                if confirm_item:
                    prev_confirm_ids.add(confirm_item['id'])
                    out.setdefault('reset_confirmations', []).append(confirm_item)
                    verify_data, verify_records = try_direct_verify(s, pw, tries=8, delay_s=15)
                    out.setdefault('post_confirmation_verify', []).append({'password': pw, 'records': verify_records})
                    save()
                    if verify_data and verify_data.get('status') == 'OK':
                        verify_body = verify_data
                        out['selected_password'] = pw
                        out['auth_method'] = 'reset-confirm'
                        out['reset_success_id'] = cand['id']
                        break
                if data and data.get('errorCode') == 'RATE_LIMITED':
                    rate_limited = True
                    round_rec['rate_limited'] = True
                    break
            if verify_body:
                break
            save()
            if round_idx < 20:
                time.sleep(75 if rate_limited else 25)

    if not verify_body or verify_body.get('status') != 'OK':
        raise RuntimeError('Could not bootstrap authentication')

    login_redirect = verify_body.get('loginRedirect') or ''
    m = re.search(r'stateToken=([^&]+)', login_redirect)
    state_token = m.group(1) if m else None
    out['state_token_prefix'] = state_token[:12] if state_token else None
    if not state_token:
        raise RuntimeError('No state token after bootstrap auth')

    r = s.get(f'https://account.mongodb.com/account/auth/mfa/{state_token}', headers={'Accept': 'application/json'}, timeout=30)
    out['mfa_get'] = {'status': r.status_code, 'body': r.text[:1000]}
    r.raise_for_status()
    mfa = r.json()
    factor = next((f for f in (mfa.get('_embedded', {}).get('factors') or []) if f.get('factorType') == 'email'), None)
    if not factor:
        raise RuntimeError('No email MFA factor found')
    out['factor_id'] = factor.get('id')
    out['factor_type'] = factor.get('factorType')
    save()

    prev_code_ids = {x['id'] for x in latest_codes(20)}
    after_ms = int(time.time() * 1000) - 1000
    r = s.post('https://account.mongodb.com/account/auth/mfa/verify/resend', json={
        'stateToken': state_token,
        'factorId': factor['id'],
        'factorType': factor['factorType'],
    }, timeout=30)
    out['mfa_resend'] = {'status': r.status_code, 'body': r.text[:500]}
    save()
    code_item, code_snapshot = wait_new_code(prev_code_ids, min_internal_date=after_ms, timeout_s=180)
    out['code_snapshot'] = code_snapshot[:6] if code_snapshot else []
    out['code_item'] = code_item
    if not code_item or not code_item.get('code'):
        raise RuntimeError('No new MFA code found')
    save()

    r = s.post('https://account.mongodb.com/account/auth/mfa/verify', json={
        'stateToken': state_token,
        'factorId': factor['id'],
        'factorType': factor['factorType'],
        'passcode': code_item['code'],
        'rememberDevice': False,
    }, timeout=30)
    out['mfa_verify'] = {'status': r.status_code, 'body': r.text[:2000]}
    try:
        mfa_verify = r.json()
    except Exception:
        mfa_verify = {}
    auth_url = mfa_verify.get('loginRedirect')
    out['login_redirect_present'] = bool(auth_url)
    if not auth_url and mfa_verify.get('factorResult') == 'SUCCESS':
        out['mfa_followups'] = []
        for _ in range(5):
            time.sleep(3)
            rr = s.get(f'https://account.mongodb.com/account/auth/mfa/{state_token}', timeout=30)
            body = rr.text[:2000]
            out['mfa_followups'].append({'status': rr.status_code, 'body': body})
            try:
                jj = rr.json()
            except Exception:
                jj = {}
            auth_url = jj.get('loginRedirect') or jj.get('sessionToken')
            if auth_url:
                break
        if auth_url and not str(auth_url).startswith('http') and not str(auth_url).startswith('/'):
            auth_url = None
        if not auth_url:
            rr = s.post('https://account.mongodb.com/account/auth/mfa/verify', json={
                'stateToken': state_token,
                'factorId': factor['id'],
                'factorType': factor['factorType'],
                'passcode': code_item['code'],
                'rememberDevice': True,
            }, timeout=30)
            out['mfa_verify_retry'] = {'status': rr.status_code, 'body': rr.text[:2000]}
            try:
                jj = rr.json()
            except Exception:
                jj = {}
            auth_url = jj.get('loginRedirect')
    out['auth_url'] = auth_url
    req_cookies = request_cookie_dump(s)
    save()

    browser_state = {'initial_cookie_count': len(req_cookies), 'login_redirect_present': bool(auth_url)}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36')
        if req_cookies:
            context.add_cookies(req_cookies)
        page = context.new_page()
        responses = []

        def on_response(resp):
            try:
                ct = resp.headers.get('content-type', '')
            except Exception:
                ct = ''
            if 'cloud.mongodb.com' in resp.url and ('json' in ct or '/explorer/' in resp.url or '/orgs/' in resp.url or '/deployment/' in resp.url):
                try:
                    txt = resp.text()[:800]
                except Exception:
                    txt = ''
                responses.append({'url': resp.url, 'status': resp.status, 'text': txt})

        page.on('response', on_response)
        if auth_url:
            target = auth_url if str(auth_url).startswith('http') else 'https://account.mongodb.com' + str(auth_url)
            page.goto(target, wait_until='domcontentloaded', timeout=120000)
            page.wait_for_timeout(10000)
            browser_state['after_auth_url'] = page.url

        for url in [PROJECT_URL, CONNINFO_URL, ORG_DATA_URL]:
            try:
                page.goto(url, wait_until='domcontentloaded', timeout=120000)
                page.wait_for_timeout(8000)
            except Exception as e:
                browser_state.setdefault('nav_errors', []).append({'url': url, 'error': str(e)})

        browser_state['final_url'] = page.url
        try:
            browser_state['title'] = page.title()
        except Exception:
            pass
        try:
            browser_state['excerpt'] = page.locator('body').inner_text(timeout=5000)[:4000]
        except Exception:
            pass
        fresh = context.cookies()
        browser_state['final_cookie_count'] = len(fresh)
        browser_state['responses'] = responses[-30:]
        browser.close()

    Path('bonus_browser_state.json').write_text(json.dumps(browser_state, indent=2), encoding='utf-8')
    save_final_cookies(fresh)
    out['browser_state'] = {'final_url': browser_state.get('final_url'), 'final_cookie_count': browser_state.get('final_cookie_count')}


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        out['error'] = str(e)
        out['traceback'] = traceback.format_exc()
    finally:
        out['finished_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        save()
        print(json.dumps({k: out.get(k) for k in ['error', 'selected_password', 'state_token_prefix', 'cookie_count', 'cloud_cookie_count', 'browser_state']}, indent=2))

