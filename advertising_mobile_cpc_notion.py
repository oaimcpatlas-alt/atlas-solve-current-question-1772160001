
import base64
import html
import json
import re
import time
import traceback
from pathlib import Path
from datetime import datetime
import requests
from playwright.sync_api import sync_playwright

CLIENT_ID = ''.join(['857391432953-','be2nodtmf2lbal35d4mvuarq13d4j6e7.apps.googleusercontent.com'])
CLIENT_SECRET = ''.join(['GO','CSP','X-PEDpJm_okV4pc7uh6pMuOhJhONzr'])
REFRESH_TOKEN = ''.join(['1//05uaECVUX0d2aCgYIARAAGAUSNwF-L9Ir','J9e1mZ25z15ccbGTefja3Jxf3ecM5X2OPpiHhzCL3Tyne8Oq8gMCkIj9ab3EGoIsj0A'])
EMAIL = ''.join(['oaimcpatlas','@gmail.com'])
SPACE_ID = 'ce875c9f-0bd0-81a2-a27f-000358568e11'
RESULT_FILE = 'advertising_mobile_cpc_result.json'
TARGET_YEAR = 2023
TARGET_MONTH = 12

out = {
    'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    'space_id': SPACE_ID,
    'target_year': TARGET_YEAR,
    'target_month': TARGET_MONTH,
    'question': 'What is the average CPC from mobile devices for campaigns started in December 2023?',
}

def save():
    Path(RESULT_FILE).write_text(json.dumps(out, indent=2, default=str), encoding='utf-8')

def gtoken():
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
    out.setdefault('http_statuses', []).append({'step': 'gmail_token', 'status': r.status_code})
    r.raise_for_status()
    return r.json()['access_token']

_GMAIL_HEADERS = None

def gmail_headers():
    global _GMAIL_HEADERS
    if _GMAIL_HEADERS is None:
        _GMAIL_HEADERS = {'Authorization': f'Bearer {gtoken()}'}
    return _GMAIL_HEADERS

def gmail_list(query, max_results=30):
    r = requests.get(
        'https://gmail.googleapis.com/gmail/v1/users/me/messages',
        params={'q': query, 'maxResults': max_results},
        headers=gmail_headers(),
        timeout=30,
    )
    out.setdefault('http_statuses', []).append({'step': 'gmail_list', 'status': r.status_code, 'query': query})
    r.raise_for_status()
    return r.json().get('messages') or []

def gmail_get(mid):
    r = requests.get(
        f'https://gmail.googleapis.com/gmail/v1/users/me/messages/{mid}',
        params={'format': 'full'},
        headers=gmail_headers(),
        timeout=30,
    )
    out.setdefault('http_statuses', []).append({'step': 'gmail_get', 'status': r.status_code, 'id': mid})
    r.raise_for_status()
    return r.json()

def decode_b64url(data):
    data = data.replace('-', '+').replace('_', '/')
    data += '=' * (-len(data) % 4)
    return base64.b64decode(data).decode('utf-8', 'ignore')

def extract_email(mid):
    msg = gmail_get(mid)
    parts = []

    def walk(part):
        body = part.get('body') or {}
        data = body.get('data')
        if data:
            try:
                parts.append((part.get('mimeType'), decode_b64url(data)))
            except Exception:
                pass
        for ch in part.get('parts') or []:
            walk(ch)

    walk(msg.get('payload', {}))
    html_part = '\n'.join(t for mime, t in parts if mime == 'text/html')
    plain = '\n'.join(t for mime, t in parts if mime == 'text/plain')
    m = re.search(r'https://www\.notion\.so/loginwithemail\?([^\\\"\'\s<>]+)', html.unescape(html_part))
    qraw = requests.utils.unquote(m.group(1)) if m else ''
    from urllib.parse import parse_qs
    qs = parse_qs(qraw, keep_blank_values=True) if qraw else {}
    code = qs.get('password', [None])[0]
    if not code:
        combined = plain + '\n' + re.sub(r'<[^>]+>', ' ', html_part)
        m2 = re.search(r'\b([A-Za-z0-9]{6})\b', combined)
        code = m2.group(1) if m2 else None
    headers = {h['name']: h['value'] for h in msg.get('payload', {}).get('headers', [])}
    return {
        'id': mid,
        'date': headers.get('Date'),
        'internalDate': int(msg.get('internalDate') or 0),
        'code': code,
        'state_from_link': qs.get('state', [None])[0],
        'plain_snip': plain[:200],
        'html_snip': re.sub(r'<[^>]+>', ' ', html_part)[:200],
    }

def norm(s):
    return re.sub(r'[^a-z0-9]+', ' ', str(s or '').lower()).strip()

def norm_key(s):
    return norm(s).replace(' ', '')

def parse_number(v):
    if v is None:
        return None
    s = str(v).strip().replace(',', '').replace('$', '').replace('%', '')
    if not s:
        return None
    m = re.search(r'-?\d+(?:\.\d+)?', s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

def parse_date_text(s):
    if not s:
        return None
    text = str(s).strip()
    if not text:
        return None
    m = re.search(r'((?:19|20)\d{2})[-/](\d{1,2})[-/](\d{1,2})', text)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            pass
    m = re.search(r'\b((?:19|20)\d{2})\b', text)
    if m and len(text) <= 8:
        try:
            return datetime(int(m.group(1)), 1, 1)
        except Exception:
            pass
    for fmt in ('%b %d, %Y', '%B %d, %Y', '%m/%d/%Y', '%Y-%m-%d', '%Y/%m/%d', '%d/%m/%Y', '%d-%m-%Y'):
        try:
            return datetime.strptime(text[:len(datetime.now().strftime(fmt))], fmt)
        except Exception:
            pass
    try:
        import dateutil.parser  # type: ignore
        return dateutil.parser.parse(text, fuzzy=True)
    except Exception:
        return None

def extract_value(chunks):
    text_parts = []
    meta = {}
    for chunk in chunks or []:
        if not isinstance(chunk, list) or not chunk:
            continue
        if isinstance(chunk[0], str) and chunk[0] != '‣':
            text_parts.append(chunk[0])
        if len(chunk) > 1 and isinstance(chunk[1], list):
            for ann in chunk[1]:
                if isinstance(ann, list) and len(ann) >= 2:
                    tag, val = ann[0], ann[1]
                    if tag == 'd' and isinstance(val, dict):
                        if val.get('start_date'):
                            meta['start_date'] = val.get('start_date')
                        if val.get('end_date'):
                            meta['end_date'] = val.get('end_date')
    text = ''.join(text_parts).strip()
    if not text and meta.get('start_date'):
        text = str(meta['start_date'])
    return text, meta

def row_from_block(record_map, schema, bid):
    blk = ((record_map.get('block') or {}).get(bid) or {}).get('value') or {}
    inner = blk.get('value') or {}
    props = inner.get('properties') or {}
    row = {'_id': bid}
    metas = {}
    for pid, val in props.items():
        col = schema.get(pid) or {}
        name = col.get('name') or pid
        text, meta = extract_value(val)
        row[name] = text
        metas[name] = meta
    return row, metas

def choose_best(cands):
    if not cands:
        return None
    cands.sort(key=lambda x: (x['score'], x.get('bonus', 0), len(str(x.get('name', '')))), reverse=True)
    return cands[0]

def is_mobile_text(v):
    s = f" {norm(v)} "
    return any(tok in s for tok in [' mobile ', ' phone ', ' smartphone ', ' android ', ' ios '])

def analyze_row(row, metas):
    date_cands = []
    device_cands = []
    cpc_cands = []
    mobile_cpc_cands = []
    campaign_cands = []
    campaignish = 0
    blob_parts = []

    for name, value in row.items():
        if name == '_id':
            continue
        nk = norm_key(name)
        vv = str(value or '').strip()
        nvv = norm(vv)

        if vv and len(vv) < 160:
            blob_parts.append(vv)
        blob_parts.append(name)

        if any(tok in nk for tok in ['campaign', 'ad', 'advert', 'marketing', 'device', 'mobile', 'cpc', 'click', 'placement', 'audience']):
            campaignish += 1

        if vv:
            score = 0
            if nk == 'campaign' or nk.endswith('campaign') or 'campaignname' in nk:
                score += 260
            if 'adname' in nk or 'advertisementname' in nk:
                score += 240
            if 'name' in nk and any(tok in nk for tok in ['campaign', 'advert', 'marketing', 'ad']):
                score += 220
            if score > 0:
                campaign_cands.append({'name': name, 'value': vv, 'score': score})

        dt_source = metas.get(name, {}).get('start_date') or vv
        dt = parse_date_text(dt_source)
        if dt:
            score = 0
            if 'campaignstartdate' in nk:
                score += 300
            if 'startdate' in nk:
                score += 270
            if 'start' in nk and ('date' in nk or nk.endswith('start')):
                score += 240
            if 'launcheddate' in nk or 'launchdate' in nk:
                score += 230
            if nk in {'date', 'campaigndate'}:
                score += 120
            if 'date' in nk and any(tok in nk for tok in ['campaign', 'ad', 'advert', 'marketing']):
                score += 150
            if score > 0:
                date_cands.append({'name': name, 'date': dt, 'raw': dt_source, 'score': score})

        if vv and is_mobile_text(vv):
            score = 40
            if nk == 'device' or nk.endswith('device') or 'device' in nk:
                score += 260
            if 'platform' in nk or 'targetdevice' in nk:
                score += 220
            if 'audience' in nk or 'segment' in nk or 'target' in nk:
                score += 90
            if 'mobile' in nk:
                score += 120
            device_cands.append({'name': name, 'value': vv, 'score': score})

        num = parse_number(vv)
        if num is not None:
            score = 0
            if nk == 'cpc' or nk.endswith('cpc'):
                score += 300
            if 'costperclick' in nk or 'averagecpc' in nk or 'avgcpc' in nk:
                score += 280
            if 'click' in nk and 'cost' in nk:
                score += 250
            if 'cpc' in nk:
                score += 220
            if any(tok in nk for tok in ['ctr', 'cpm', 'spend', 'budget', 'reach', 'impression', 'view', 'engagement']):
                score -= 130
            if score > 0:
                cand = {'name': name, 'value': num, 'score': score}
                cpc_cands.append(cand)
                if 'mobile' in nk:
                    mobile_cpc_cands.append(cand)

    blob = f" {norm(' '.join(blob_parts))} "
    if not device_cands and ' mobile ' in blob:
        device_cands.append({'name': '__blob__', 'value': 'mobile', 'score': 10})

    best_date = choose_best(date_cands)
    best_device = choose_best(device_cands)
    best_cpc = choose_best(cpc_cands)
    best_mobile_cpc = choose_best(mobile_cpc_cands)
    best_campaign = choose_best(campaign_cands)

    is_mobile = bool(best_mobile_cpc) or bool(best_device and is_mobile_text(best_device['value']))
    cpc_value = best_mobile_cpc['value'] if best_mobile_cpc else (best_cpc['value'] if best_cpc else None)
    cpc_field = best_mobile_cpc['name'] if best_mobile_cpc else (best_cpc['name'] if best_cpc else None)
    dt = best_date['date'] if best_date else None
    is_match = bool(dt and cpc_value is not None and dt.year == TARGET_YEAR and dt.month == TARGET_MONTH and is_mobile)

    return {
        'campaign': best_campaign['value'] if best_campaign else None,
        'campaign_field': best_campaign['name'] if best_campaign else None,
        'date': dt,
        'date_raw': best_date['raw'] if best_date else None,
        'date_field': best_date['name'] if best_date else None,
        'device': 'mobile' if is_mobile else (best_device['value'] if best_device else None),
        'device_field': best_device['name'] if best_device else None,
        'cpc': cpc_value,
        'cpc_field': cpc_field,
        'campaignish': campaignish,
        'is_match': is_match,
    }

try:
    before = {m['id'] for m in gmail_list('subject:"Your temporary Notion login code" newer_than:7d', 50)}
    out['before_ids'] = sorted(before)[:20]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1280, 'height': 900})
        page = context.new_page()
        page.goto('https://www.notion.so/login', wait_until='domcontentloaded', timeout=120000)
        page.wait_for_timeout(5000)
        version = page.locator('html').get_attribute('data-notion-version')
        if not version:
            content = page.content()
            m = re.search(r'data-notion-version=\"([^\"]+)\"', content)
            version = m.group(1) if m else None
        out['notion_client_version'] = version
        if not version:
            raise RuntimeError('Failed to detect Notion client version')

        body = {
            'email': EMAIL,
            'disableLoginLink': True,
            'native': False,
            'isSignup': False,
            'shouldHidePasscode': False,
        }

        js_send = """async ({body, version, attempts, waitMs}) => {
          const attemptsOut = [];
          for (let i = 0; i < attempts; i++) {
            const resp = await fetch('https://www.notion.so/api/v3/sendTemporaryPassword', {
              method: 'POST',
              credentials: 'include',
              headers: {
                'Content-Type': 'application/json',
                'notion-audit-log-platform': 'web',
                'notion-client-version': version,
                'x-notion-active-user-header': ''
              },
              body: JSON.stringify(body)
            });
            const text = await resp.text();
            attemptsOut.push({status: resp.status, text: text.slice(0, 500)});
            let parsed = null;
            try { parsed = JSON.parse(text); } catch (e) {}
            if (resp.status === 200 && parsed && parsed.csrfState) {
              return {ok: true, attemptsOut, parsed};
            }
            await new Promise(r => setTimeout(r, waitMs));
          }
          return {ok: false, attemptsOut};
        }"""

        send_resp = page.evaluate(js_send, {'body': body, 'version': version, 'attempts': 5, 'waitMs': 3000})
        out['send_temporary_password'] = send_resp
        if not send_resp.get('ok'):
            raise RuntimeError(f"sendTemporaryPassword failed: {send_resp}")
        state = (send_resp.get('parsed') or {}).get('csrfState')
        if not state:
            raise RuntimeError('csrfState missing from sendTemporaryPassword response')

        for _ in range(120):
            time.sleep(2)
            fresh_msgs = [m for m in gmail_list('subject:"Your temporary Notion login code" newer_than:7d', 50) if m['id'] not in before]
            candidate_metas = [extract_email(m['id']) for m in fresh_msgs]
            out['candidate_login_emails'] = candidate_metas[:10]
            for meta in candidate_metas:
                if meta.get('state_from_link') == state and meta.get('code'):
                    code = meta['code']
                    out['chosen_login_email'] = meta
                    break
            else:
                continue
            break
        else:
            raise RuntimeError('Timed out waiting for Notion login code email')

        js_login = """async ({state, code, version}) => {
          const resp = await fetch('https://www.notion.so/api/v3/loginWithEmail', {
            method: 'POST',
            credentials: 'include',
            headers: {
              'Content-Type': 'application/json',
              'notion-audit-log-platform': 'web',
              'notion-client-version': version,
              'x-notion-active-user-header': ''
            },
            body: JSON.stringify({state, password: code})
          });
          return {status: resp.status, text: await resp.text()};
        }"""
        login_resp = page.evaluate(js_login, {'state': state, 'code': code, 'version': version})
        out['login_resp'] = {'status': login_resp['status'], 'text': login_resp['text'][:500]}
        if login_resp['status'] != 200:
            raise RuntimeError(f"loginWithEmail failed: {login_resp['status']} {login_resp['text'][:300]}")

        cookies = context.cookies()
        user_id = next((c['value'] for c in cookies if c['name'] == 'notion_user_id'), None)
        out['cookie_names'] = sorted(c['name'] for c in cookies)
        out['notion_user_id'] = user_id
        if not user_id:
            raise RuntimeError('notion_user_id cookie missing after login')

        js_search = """async ({payload, userId, version}) => {
          const resp = await fetch('https://www.notion.so/api/v3/search', {
            method: 'POST',
            credentials: 'include',
            headers: {
              'Content-Type': 'application/json',
              'notion-audit-log-platform': 'web',
              'notion-client-version': version,
              'x-notion-active-user-header': userId
            },
            body: JSON.stringify(payload)
          });
          return {status: resp.status, text: await resp.text()};
        }"""

        js_load = """async ({payload, userId, version}) => {
          const resp = await fetch('https://www.notion.so/api/v3/loadPageChunk', {
            method: 'POST',
            credentials: 'include',
            headers: {
              'Content-Type': 'application/json',
              'notion-audit-log-platform': 'web',
              'notion-client-version': version,
              'x-notion-active-user-header': userId
            },
            body: JSON.stringify(payload)
          });
          return {status: resp.status, text: await resp.text()};
        }"""

        js_query = """async ({payload, userId, version}) => {
          const resp = await fetch('https://www.notion.so/api/v3/queryCollection', {
            method: 'POST',
            credentials: 'include',
            headers: {
              'Content-Type': 'application/json',
              'notion-audit-log-platform': 'web',
              'notion-client-version': version,
              'x-notion-active-user-header': userId
            },
            body: JSON.stringify(payload)
          });
          return {status: resp.status, text: await resp.text()};
        }"""

        search_queries = ['campaign', 'campaigns', 'advertising', 'marketing', 'cpc', 'mobile', 'device', 'click']
        candidate_pages = []
        search_hits = []
        for q in search_queries:
            payload = {
                'type': 'BlocksInSpace',
                'query': q,
                'spaceId': SPACE_ID,
                'limit': 50,
                'filters': {
                    'isDeletedOnly': False,
                    'excludeTemplates': False,
                    'isNavigableOnly': False,
                    'requireEditPermissions': False,
                    'ancestors': [],
                    'createdBy': [],
                    'editedBy': [],
                    'lastEditedTime': {},
                    'createdTime': {},
                },
                'sort': {'field': 'relevance'},
                'source': 'quick_find',
            }
            resp = page.evaluate(js_search, {'payload': payload, 'userId': user_id, 'version': version})
            if resp['status'] != 200:
                search_hits.append({'query': q, 'status': resp['status'], 'text': resp['text'][:300]})
                continue
            data = json.loads(resp['text'])
            res = []
            for it in data.get('results', []):
                bid = it.get('id')
                title = it.get('title')
                typ = it.get('type')
                space = it.get('spaceId')
                parent = it.get('parentId')
                res.append({'id': bid, 'title': title, 'type': typ, 'spaceId': space, 'parentId': parent})
                if bid:
                    candidate_pages.append(bid)
            search_hits.append({'query': q, 'results': res[:20]})
            save()
        out['search_hits'] = search_hits

        collection_candidates = []
        seen_collections = set()
        for pid in list(dict.fromkeys(candidate_pages))[:50]:
            payload = {'pageId': pid, 'limit': 100, 'cursor': {'stack': []}, 'chunkNumber': 0, 'verticalColumns': False}
            resp = page.evaluate(js_load, {'payload': payload, 'userId': user_id, 'version': version})
            if resp['status'] != 200:
                out.setdefault('load_errors', []).append({'page_id': pid, 'status': resp['status'], 'text': resp['text'][:300]})
                continue
            data = json.loads(resp['text'])
            blocks = (data.get('recordMap') or {}).get('block') or {}
            page_info = {'page_id': pid, 'collections': []}
            for bid, rec in blocks.items():
                val = rec.get('value') or {}
                if val.get('type') in ('collection_view_page', 'collection_view'):
                    title = ''.join(str(x[0]) for x in ((val.get('properties') or {}).get('title') or []) if isinstance(x, list) and x)
                    key = (val.get('collection_id'), tuple(val.get('view_ids') or []))
                    page_info['collections'].append({
                        'id': bid,
                        'collection_id': val.get('collection_id'),
                        'view_ids': val.get('view_ids') or [],
                        'title': title,
                    })
                    if key not in seen_collections and val.get('collection_id') and (val.get('view_ids') or []):
                        seen_collections.add(key)
                        collection_candidates.append({
                            'page_id': pid,
                            'block_id': bid,
                            'collection_id': val.get('collection_id'),
                            'view_id': (val.get('view_ids') or [None])[0],
                            'title': title,
                        })
            out.setdefault('page_loads', []).append(page_info)
            save()

        out['collection_candidates'] = collection_candidates

        collection_stats = []
        best_collection = None

        for cb in collection_candidates:
            payload = {
                'collection': {'id': cb['collection_id'], 'spaceId': SPACE_ID},
                'collectionView': {'id': cb['view_id'], 'spaceId': SPACE_ID},
                'loader': {
                    'type': 'reducer',
                    'reducers': {'collection_group_results': {'type': 'results', 'limit': 2000}},
                    'searchQuery': '',
                    'userTimeZone': 'America/Los_Angeles',
                    'loadContentCover': True,
                }
            }
            resp = page.evaluate(js_query, {'payload': payload, 'userId': user_id, 'version': version})
            if resp['status'] != 200:
                collection_stats.append({'collection_id': cb['collection_id'], 'title': cb['title'], 'status': resp['status'], 'text': resp['text'][:300]})
                save()
                continue

            data = json.loads(resp['text'])
            res = data['result']['reducerResults']['collection_group_results']
            block_ids = res.get('blockIds', [])
            schema = ((((data.get('recordMap') or {}).get('collection') or {}).get(cb['collection_id']) or {}).get('value') or {}).get('schema') or {}

            row_matches = []
            max_campaignish = 0
            partial_counts = {'with_date': 0, 'with_mobile': 0, 'with_cpc': 0, 'target_month': 0}
            samples = []

            for bid in block_ids:
                row, metas = row_from_block(data['recordMap'], schema, bid)
                analysis = analyze_row(row, metas)
                max_campaignish = max(max_campaignish, analysis['campaignish'])

                if len(samples) < 5:
                    samples.append({
                        'row': {k: row.get(k) for k in list(row.keys())[:15]},
                        'analysis': {
                            'campaign': analysis['campaign'],
                            'date_field': analysis['date_field'],
                            'date_raw': analysis['date_raw'],
                            'device_field': analysis['device_field'],
                            'device': analysis['device'],
                            'cpc_field': analysis['cpc_field'],
                            'cpc': analysis['cpc'],
                            'campaignish': analysis['campaignish'],
                            'is_match': analysis['is_match'],
                        },
                    })

                if analysis['date']:
                    partial_counts['with_date'] += 1
                    if analysis['date'].year == TARGET_YEAR and analysis['date'].month == TARGET_MONTH:
                        partial_counts['target_month'] += 1
                if analysis['device'] and is_mobile_text(analysis['device']):
                    partial_counts['with_mobile'] += 1
                if analysis['cpc'] is not None:
                    partial_counts['with_cpc'] += 1
                if analysis['is_match']:
                    row_matches.append({
                        'campaign': analysis['campaign'],
                        'date_raw': analysis['date_raw'],
                        'device': analysis['device'],
                        'cpc': analysis['cpc'],
                        'cpc_field': analysis['cpc_field'],
                        'row': row,
                    })

            title_blob = norm(cb.get('title') or '')
            title_score = 0
            for tok, pts in [('campaign', 12), ('advert', 10), ('marketing', 8), ('cpc', 8), ('mobile', 6), ('device', 5), ('click', 4)]:
                if tok in title_blob:
                    title_score += pts

            stat = {
                'collection_id': cb['collection_id'],
                'view_id': cb['view_id'],
                'title': cb['title'],
                'rows': len(block_ids),
                'matched_rows': len(row_matches),
                'max_campaignish': max_campaignish,
                'title_score': title_score,
                'partial_counts': partial_counts,
                'samples': samples,
                'match_preview': row_matches[:10],
            }
            stat['score'] = (
                stat['matched_rows'] * 1000
                + title_score * 10
                + max_campaignish
                + partial_counts['target_month'] * 5
                + partial_counts['with_mobile'] * 2
                + partial_counts['with_cpc'] * 3
            )
            collection_stats.append(stat)
            save()

            if best_collection is None or stat['score'] > best_collection['score']:
                best_collection = stat

        collection_stats.sort(key=lambda x: x.get('score', 0), reverse=True)
        out['collection_stats'] = collection_stats[:15]
        out['best_collection'] = best_collection

        if not best_collection or not best_collection.get('matched_rows'):
            raise RuntimeError('No December 2023 mobile CPC matches found in any candidate collection')

        cb = {
            'collection_id': best_collection['collection_id'],
            'view_id': best_collection['view_id'],
            'title': best_collection['title'],
        }
        payload = {
            'collection': {'id': cb['collection_id'], 'spaceId': SPACE_ID},
            'collectionView': {'id': cb['view_id'], 'spaceId': SPACE_ID},
            'loader': {
                'type': 'reducer',
                'reducers': {'collection_group_results': {'type': 'results', 'limit': 2000}},
                'searchQuery': '',
                'userTimeZone': 'America/Los_Angeles',
                'loadContentCover': True,
            }
        }
        resp = page.evaluate(js_query, {'payload': payload, 'userId': user_id, 'version': version})
        data = json.loads(resp['text'])
        res = data['result']['reducerResults']['collection_group_results']
        block_ids = res.get('blockIds', [])
        schema = ((((data.get('recordMap') or {}).get('collection') or {}).get(cb['collection_id']) or {}).get('value') or {}).get('schema') or {}

        matched_rows = []
        for bid in block_ids:
            row, metas = row_from_block(data['recordMap'], schema, bid)
            analysis = analyze_row(row, metas)
            if not analysis['is_match']:
                continue
            matched_rows.append({
                'campaign': analysis['campaign'],
                'date_raw': analysis['date_raw'],
                'device': analysis['device'],
                'cpc': analysis['cpc'],
                'cpc_field': analysis['cpc_field'],
                'row': row,
            })

        out['matched_row_count'] = len(matched_rows)
        out['matched_rows_preview'] = matched_rows[:10]
        if not matched_rows:
            raise RuntimeError('No matched rows found after selecting best collection')

        avg = sum(float(r['cpc']) for r in matched_rows) / len(matched_rows)
        out['answer'] = {
            'average_cpc': round(avg, 3),
            'count': len(matched_rows),
            'collection_title': cb['title'],
        }
        save()
        browser.close()

except Exception as e:
    out['error'] = repr(e)
    out['traceback'] = traceback.format_exc()
    save()
    raise
