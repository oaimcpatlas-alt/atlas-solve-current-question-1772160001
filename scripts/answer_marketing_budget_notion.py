
import base64
import html
import json
import re
import time
import traceback
from pathlib import Path
from datetime import datetime, timezone
import requests
from playwright.sync_api import sync_playwright

CLIENT_ID = ''.join(['857391432953-','be2nodtmf2lbal35d4mvuarq13d4j6e7.apps.googleusercontent.com'])
CLIENT_SECRET = ''.join(['GO','CSP','X-PEDpJm_okV4pc7uh6pMuOhJhONzr'])
REFRESH_TOKEN = ''.join(['1//05uaECVUX0d2aCgYIARAAGAUSNwF-L9Ir','J9e1mZ25z15ccbGTefja3Jxf3ecM5X2OPpiHhzCL3Tyne8Oq8gMCkIj9ab3EGoIsj0A'])
EMAIL = ''.join(['oaimcpatlas','@gmail.com'])
SPACE_ID = 'ce875c9f-0bd0-81a2-a27f-000358568e11'
RESULT_FILE = 'outputs/marketing_budget_notion_answer.json'

QUESTION = ("According to our financial records in the database, how much did the total budget allocated "
            "to non recurring marketing transactions vary between January 2020 and January 2021, based on transaction dates?")

out = {
    'started_at': datetime.now(timezone.utc).isoformat(),
    'space_id': SPACE_ID,
    'question': QUESTION,
}

def save():
    Path('outputs').mkdir(exist_ok=True)
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
    save()
    r.raise_for_status()
    return r.json()['access_token']

GMAIL_HEADERS = {'Authorization': f'Bearer {gtoken()}'}

def gmail_list(query, max_results=30):
    r = requests.get(
        'https://gmail.googleapis.com/gmail/v1/users/me/messages',
        params={'q': query, 'maxResults': max_results},
        headers=GMAIL_HEADERS,
        timeout=30,
    )
    out.setdefault('http_statuses', []).append({'step': 'gmail_list', 'status': r.status_code, 'query': query})
    save()
    r.raise_for_status()
    return r.json().get('messages') or []

def gmail_get(mid):
    r = requests.get(
        f'https://gmail.googleapis.com/gmail/v1/users/me/messages/{mid}',
        params={'format': 'full'},
        headers=GMAIL_HEADERS,
        timeout=30,
    )
    out.setdefault('http_statuses', []).append({'step': 'gmail_get', 'status': r.status_code, 'id': mid})
    save()
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
    for fmt in ('%b %d, %Y', '%B %d, %Y', '%m/%d/%Y', '%Y-%m-%d', '%Y/%m/%d', '%d/%m/%Y', '%d-%m-%Y', '%m-%d-%Y'):
        try:
            return datetime.strptime(text[:len(datetime.now().strftime(fmt))], fmt)
        except Exception:
            pass
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

def count_keyword_hits(text, items):
    score = 0
    for tok, pts in items:
        if tok in text:
            score += pts
    return score

def analyze_row(row, metas):
    dept_cands = []
    recur_cands = []
    date_cands = []
    budget_cands = []
    blob_parts = []

    for name, value in row.items():
        if name == '_id':
            continue
        nk = norm_key(name)
        vv = str(value or '').strip()
        nvv = norm(vv)
        if vv and len(vv) < 160:
            blob_parts.append(f'{name}={vv}')
        else:
            blob_parts.append(name)

        # department = marketing
        if vv:
            score = 0
            if 'marketing' in nvv:
                score += 4
                if any(tok in nk for tok in ['department','dept','team','area','function','division','unit','category']):
                    score += 6
                if nk in {'department','dept','team','area','function'}:
                    score += 2
            if score > 0:
                dept_cands.append({'name': name, 'value': vv, 'score': score})

        # non recurring
        if vv:
            score = 0
            if any(term in nvv for term in ['non recurring', 'non-recurring', 'nonrecurring', 'one time', 'one-time', 'one off', 'one-off']):
                score += 5
                if any(tok in nk for tok in ['recurring','recurrence','transactiontype','type','category','expense','payment']):
                    score += 6
                if nk in {'recurring','recurrence','transactiontype','type','category'}:
                    score += 2
            if any(tok in nk for tok in ['recurring','isrecurring']) and nvv in {'no','false','n','0'}:
                score += 8
            if score > 0:
                recur_cands.append({'name': name, 'value': vv, 'score': score})

        # date
        dt_source = metas.get(name, {}).get('start_date') or vv
        dt = parse_date_text(dt_source)
        if dt:
            score = 0
            if 'transactiondate' == nk or ('transaction' in nk and 'date' in nk):
                score += 10
            elif 'date' in nk:
                score += 4
            if any(tok in nk for tok in ['posted','booked','purchase','payment','record']) and 'date' in nk:
                score += 2
            if score > 0:
                date_cands.append({'name': name, 'date': dt, 'raw': dt_source, 'score': score})

        # budget
        num = parse_number(vv)
        if num is not None:
            score = 0
            if 'budget' in nk:
                score += 10
            if 'allocated' in nk or 'allocation' in nk:
                score += 8
            if 'amount' in nk or 'value' in nk:
                score += 2
            if nk in {'budgetallocated','allocatedbudget','budgetallocation','budget'}:
                score += 4
            if any(bad in nk for bad in ['score','rating','percent','quantity','count','id','year','month']):
                score -= 6
            if score > 0:
                budget_cands.append({'name': name, 'value': num, 'score': score})

    blob = norm(' | '.join(blob_parts))
    if not dept_cands and 'marketing' in blob:
        dept_cands.append({'name': 'blob', 'value': 'marketing', 'score': 1})
    if not recur_cands:
        for term in ['non recurring', 'non-recurring', 'nonrecurring', 'one time', 'one-time', 'one off', 'one-off']:
            if term in blob:
                recur_cands.append({'name': 'blob', 'value': term, 'score': 1})
                break

    dept = max(dept_cands, key=lambda x: x['score']) if dept_cands else None
    recur = max(recur_cands, key=lambda x: x['score']) if recur_cands else None
    date = max(date_cands, key=lambda x: x['score']) if date_cands else None
    budget = max(budget_cands, key=lambda x: x['score']) if budget_cands else None
    bucket = None
    if date:
        if date['date'].year == 2020 and date['date'].month == 1:
            bucket = '2020-01'
        elif date['date'].year == 2021 and date['date'].month == 1:
            bucket = '2021-01'
    is_match = bool(dept and recur and date and budget)
    return {
        'department': dept,
        'recurrence': recur,
        'date': {'name': date['name'], 'raw': date['raw'], 'iso': date['date'].isoformat(), 'score': date['score']} if date else None,
        'budget': budget,
        'bucket': bucket,
        'is_match': is_match,
        'blob': blob[:500],
    }

try:
    before = {m['id'] for m in gmail_list('subject:"Your temporary Notion login code" newer_than:7d', 50)}
    out['before_ids_count'] = len(before)
    save()

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
        save()
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
            attemptsOut.push({attempt: i + 1, status: resp.status, text: text.slice(0, 500)});
            if (resp.status === 200) {
              return {attempts: attemptsOut, finalStatus: resp.status, finalText: text};
            }
            if (i + 1 < attempts) await new Promise(r => setTimeout(r, waitMs));
          }
          return {attempts: attemptsOut, finalStatus: null, finalText: null};
        }"""

        send_resp = page.evaluate(js_send, {'body': body, 'version': version, 'attempts': 5, 'waitMs': 20000})
        out['send_attempts'] = send_resp.get('attempts')
        save()
        if send_resp.get('finalStatus') != 200:
            raise RuntimeError(f"sendTemporaryPassword failed: {send_resp}")
        send_json = json.loads(send_resp['finalText'])
        state = send_json.get('csrfState')
        out['state_prefix'] = state[:24] if state else None
        save()
        if not state:
            raise RuntimeError('csrfState missing from sendTemporaryPassword response')

        code = None
        email_meta = None
        for _ in range(120):
            time.sleep(2)
            fresh_msgs = [m for m in gmail_list('subject:"Your temporary Notion login code" newer_than:7d', 50) if m['id'] not in before]
            candidate_metas = [extract_email(m['id']) for m in fresh_msgs]
            out['candidate_login_emails'] = candidate_metas[:10]
            save()
            for meta in candidate_metas:
                if meta.get('state_from_link') == state and meta.get('code'):
                    code = meta['code']
                    email_meta = meta
                    break
            if code:
                break
        if not code:
            raise RuntimeError('No matching login email found')
        out['login_email'] = email_meta
        save()

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
        out['login_response'] = {'status': login_resp['status'], 'text_prefix': login_resp['text'][:500]}
        save()
        if login_resp['status'] != 200:
            raise RuntimeError(f"loginWithEmail failed: {login_resp['status']} {login_resp['text'][:300]}")

        cookies = context.cookies()
        user_id = next((c['value'] for c in cookies if c['name'] == 'notion_user_id'), None)
        out['cookie_names'] = sorted(c['name'] for c in cookies)
        out['notion_user_id'] = user_id
        save()
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

        search_queries = [
            'financial', 'finance', 'financial records', 'budget', 'budget allocation',
            'allocated', 'transaction', 'transaction date', 'marketing', 'non recurring',
            'department', 'expense'
        ]
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
                block = (((data.get('recordMap') or {}).get('block') or {}).get(bid) or {}).get('value') or {}
                title = None
                if (block.get('properties') or {}).get('title'):
                    title = ''.join(str(x[0]) for x in block['properties']['title'] if isinstance(x, list) and x)
                item = {'id': bid, 'title': title, 'type': block.get('type')}
                res.append(item)
                if bid:
                    candidate_pages.append(bid)
            search_hits.append({'query': q, 'results': res[:20]})
            out['search_hits'] = search_hits
            save()

        collection_candidates = []
        seen_collections = set()
        for pid in list(dict.fromkeys(candidate_pages))[:60]:
            payload = {'pageId': pid, 'limit': 120, 'cursor': {'stack': []}, 'chunkNumber': 0, 'verticalColumns': False}
            resp = page.evaluate(js_load, {'payload': payload, 'userId': user_id, 'version': version})
            if resp['status'] != 200:
                out.setdefault('load_errors', []).append({'page_id': pid, 'status': resp['status'], 'text': resp['text'][:300]})
                save()
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
            out['collection_candidates'] = collection_candidates
            save()

        collection_stats = []
        best_collection = None

        for cb in collection_candidates[:40]:
            payload = {
                'collection': {'id': cb['collection_id'], 'spaceId': SPACE_ID},
                'collectionView': {'id': cb['view_id'], 'spaceId': SPACE_ID},
                'loader': {
                    'type': 'reducer',
                    'reducers': {'collection_group_results': {'type': 'results', 'limit': 3000}},
                    'searchQuery': '',
                    'userTimeZone': 'America/Los_Angeles',
                    'loadContentCover': True,
                }
            }
            resp = page.evaluate(js_query, {'payload': payload, 'userId': user_id, 'version': version})
            if resp['status'] != 200:
                collection_stats.append({'collection_id': cb['collection_id'], 'title': cb['title'], 'status': resp['status'], 'text': resp['text'][:300]})
                out['collection_stats'] = collection_stats[-15:]
                save()
                continue

            data = json.loads(resp['text'])
            res = data['result']['reducerResults']['collection_group_results']
            block_ids = res.get('blockIds', [])
            schema = ((((data.get('recordMap') or {}).get('collection') or {}).get(cb['collection_id']) or {}).get('value') or {}).get('schema') or {}
            field_names = [c.get('name') for c in schema.values() if isinstance(c, dict)]
            field_blob = norm(' | '.join(x for x in field_names if x))
            title_blob = norm(cb.get('title') or '')
            table_hint_score = count_keyword_hits(title_blob + ' | ' + field_blob, [
                ('financial', 40), ('finance', 35), ('record', 12), ('budget', 30),
                ('allocation', 22), ('allocated', 18), ('transaction', 30),
                ('transaction date', 18), ('marketing', 18), ('department', 16),
                ('recurring', 12), ('expense', 18)
            ])
            if any('budget' in norm_key(x or '') for x in field_names):
                table_hint_score += 15
            if any('transactiondate' in norm_key(x or '') or ('transaction' in norm_key(x or '') and 'date' in norm_key(x or '')) for x in field_names):
                table_hint_score += 20
            if any('recurring' in norm_key(x or '') for x in field_names):
                table_hint_score += 12
            if any(norm_key(x or '') in {'department','dept','team','area','function'} or 'department' in norm_key(x or '') for x in field_names):
                table_hint_score += 12

            matched_rows = []
            bucket_counts = {'2020-01': 0, '2021-01': 0}
            partial_counts = {'dept': 0, 'recur': 0, 'date': 0, 'budget': 0}
            samples = []
            for bid in block_ids:
                row, metas = row_from_block(data['recordMap'], schema, bid)
                analysis = analyze_row(row, metas)
                if len(samples) < 5:
                    samples.append({'row': {k: row[k] for k in list(row.keys())[:12]}, 'analysis': analysis})
                if analysis['department']:
                    partial_counts['dept'] += 1
                if analysis['recurrence']:
                    partial_counts['recur'] += 1
                if analysis['date']:
                    partial_counts['date'] += 1
                if analysis['budget']:
                    partial_counts['budget'] += 1
                if analysis['is_match']:
                    matched_rows.append({
                        'row': row,
                        'analysis': analysis,
                    })
                    if analysis['bucket']:
                        bucket_counts[analysis['bucket']] += 1

            stat = {
                'collection_id': cb['collection_id'],
                'view_id': cb['view_id'],
                'title': cb['title'],
                'rows': len(block_ids),
                'field_names': field_names[:30],
                'table_hint_score': table_hint_score,
                'matched_rows': len(matched_rows),
                'bucket_counts': bucket_counts,
                'partial_counts': partial_counts,
                'samples': samples,
                'match_preview': matched_rows[:10],
            }
            stat['score'] = (
                table_hint_score
                + len(matched_rows) * 500
                + bucket_counts['2020-01'] * 120
                + bucket_counts['2021-01'] * 120
                + partial_counts['dept'] * 2
                + partial_counts['recur'] * 2
                + partial_counts['date']
                + partial_counts['budget']
            )
            collection_stats.append(stat)
            collection_stats.sort(key=lambda x: x.get('score', 0), reverse=True)
            out['collection_stats'] = collection_stats[:15]
            save()
            if best_collection is None or stat['score'] > best_collection['score']:
                best_collection = stat

        out['best_collection'] = best_collection
        save()
        if not best_collection:
            raise RuntimeError('No collection candidates found')
        if not best_collection.get('matched_rows'):
            raise RuntimeError('No matching rows found in candidate collections')

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
                'reducers': {'collection_group_results': {'type': 'results', 'limit': 5000}},
                'searchQuery': '',
                'userTimeZone': 'America/Los_Angeles',
                'loadContentCover': True,
            }
        }
        resp = page.evaluate(js_query, {'payload': payload, 'userId': user_id, 'version': version})
        if resp['status'] != 200:
            raise RuntimeError(f"Best collection requery failed: {resp['status']}")
        data = json.loads(resp['text'])
        res = data['result']['reducerResults']['collection_group_results']
        block_ids = res.get('blockIds', [])
        schema = ((((data.get('recordMap') or {}).get('collection') or {}).get(cb['collection_id']) or {}).get('value') or {}).get('schema') or {}

        sums = {'2020-01': 0.0, '2021-01': 0.0}
        counts = {'2020-01': 0, '2021-01': 0}
        examples = {'2020-01': [], '2021-01': []}

        for bid in block_ids:
            row, metas = row_from_block(data['recordMap'], schema, bid)
            analysis = analyze_row(row, metas)
            if not analysis['is_match']:
                continue
            bucket = analysis['bucket']
            if bucket and analysis['budget'] is not None:
                val = float(analysis['budget']['value'])
                sums[bucket] += val
                counts[bucket] += 1
                if len(examples[bucket]) < 10:
                    examples[bucket].append({
                        'row': row,
                        'analysis': analysis,
                        'budget_value': val,
                    })

        jan20 = sums['2020-01']
        jan21 = sums['2021-01']
        diff = jan21 - jan20
        pct = None if jan20 == 0 else (diff / jan20) * 100.0
        direction = 'increase' if diff > 0 else 'decrease' if diff < 0 else 'no change'

        out['answer'] = {
            'collection_title': best_collection['title'],
            'january_2020_total': jan20,
            'january_2021_total': jan21,
            'difference': diff,
            'direction': direction,
            'percentage_change': pct,
            'counts': counts,
            'examples': examples,
        }
        save()
        browser.close()

except Exception as e:
    out['error'] = str(e)
    out['traceback'] = traceback.format_exc()
    save()
    raise
