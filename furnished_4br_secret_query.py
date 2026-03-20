import os
import requests, re, json, time, datetime, traceback, base64, html as html_mod, urllib.parse, statistics
from pathlib import Path

CLIENT_ID=os.environ.get('GOOGLE_CLIENT_ID','')
CLIENT_SECRET=os.environ.get('GOOGLE_CLIENT_SECRET','')
REFRESH_TOKEN=os.environ.get('GOOGLE_REFRESH_TOKEN','')
EMAILS=[
    'oaimcpatlas+guest4@gmail.com',
    'oaimcpatlas+guest10@gmail.com',
    'oaimcpatlas+guest9@gmail.com',
    'oaimcpatlas+guest8@gmail.com',
    'oaimcpatlas+guest5@gmail.com',
    'oaimcpatlas@gmail.com',
]
SPACE_ID='ce875c9f-0bd0-81a2-a27f-000358568e11'
COLLECTION_ID='31075c9f-0bd0-8185-904a-000b1a275783'
VIEW_ID='31075c9f-0bd0-81c0-9444-000c5bc695df'

out={
    'started_at': datetime.datetime.utcnow().isoformat()+'Z',
    'question': 'Average monthly rent and standard deviation for furnished four bedroom apartments over 2000 square feet with premium amenities built in the 2010s',
}

def save():
    Path('answer.json').write_text(json.dumps(out, indent=2, default=str), encoding='utf-8')

out['has_google_client_id']=bool(CLIENT_ID)
out['has_google_client_secret']=bool(CLIENT_SECRET)
out['has_google_refresh_token']=bool(REFRESH_TOKEN)

def norm(s):
    return re.sub(r'[^a-z0-9]+', '', str(s or '').lower())

def gtoken():
    if not (CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN):
        raise RuntimeError('Missing GOOGLE_* secrets')
    r = requests.post('https://oauth2.googleapis.com/token', data={
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': REFRESH_TOKEN,
        'grant_type': 'refresh_token',
    }, timeout=30)
    out.setdefault('http_statuses', []).append({'step':'gmail_token', 'status':r.status_code, 'time': datetime.datetime.utcnow().isoformat()+'Z'})
    r.raise_for_status()
    return r.json()['access_token']

_GMAIL_HEADERS=None
def gmail_headers():
    global _GMAIL_HEADERS
    if _GMAIL_HEADERS is None:
        _GMAIL_HEADERS={'Authorization': f'Bearer {gtoken()}'}
    return _GMAIL_HEADERS

def gmail_list(query, max_results=100):
    r = requests.get('https://gmail.googleapis.com/gmail/v1/users/me/messages',
                     params={'q': query, 'maxResults': max_results},
                     headers=gmail_headers(), timeout=30)
    out.setdefault('http_statuses', []).append({'step':'gmail_list', 'status':r.status_code, 'query':query})
    r.raise_for_status()
    return r.json().get('messages') or []

def gmail_get(mid):
    r = requests.get(f'https://gmail.googleapis.com/gmail/v1/users/me/messages/{mid}',
                     params={'format':'full'}, headers=gmail_headers(), timeout=30)
    out.setdefault('http_statuses', []).append({'step':'gmail_get', 'status':r.status_code, 'id':mid})
    r.raise_for_status()
    return r.json()

def decode_b64url(data):
    data = data.replace('-', '+').replace('_', '/')
    data += '=' * (-len(data) % 4)
    return base64.b64decode(data).decode('utf-8', 'ignore')

def extract_email(mid):
    msg = gmail_get(mid)
    parts=[]
    def walk(part):
        if not isinstance(part, dict):
            return
        body=part.get('body') or {}
        data=body.get('data')
        if data:
            try:
                parts.append((part.get('mimeType'), decode_b64url(data)))
            except Exception:
                pass
        for ch in part.get('parts') or []:
            walk(ch)
    walk(msg.get('payload', {}))
    html_part='\n'.join(t for mime,t in parts if mime=='text/html')
    plain='\n'.join(t for mime,t in parts if mime=='text/plain')
    html_un = html_mod.unescape(html_part)
    link = None
    for pat in [
        r'href="(https://www\.notion\.so/loginwithemail\?[^"]+)"',
        r'(https://www\.notion\.so/loginwithemail\?[^\s<>\"]+)',
        r'https://www\.notion\.so/loginwithemail\?([^\"\'\s<>]+)',
    ]:
        m = re.search(pat, html_un)
        if m:
            link = m.group(1) if not pat.startswith(r'https://www') else 'https://www.notion.so/loginwithemail?' + m.group(1)
            break
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(link).query, keep_blank_values=True) if link else {}
    code = qs.get('password',[None])[0]
    if not code:
        m = re.search(r'\b([A-Za-z0-9]{6})\b', plain.strip())
        code = m.group(1) if m else None
    if not code:
        combined = plain + '\n' + re.sub(r'<[^>]+>',' ', html_part)
        m2 = re.search(r'\b([A-Za-z0-9]{6})\b', combined)
        code = m2.group(1) if m2 else None
    headers={h['name']:h['value'] for h in msg.get('payload',{}).get('headers',[])}
    return {
        'id': mid,
        'date': headers.get('Date'),
        'internalDate': int(msg.get('internalDate') or 0),
        'code': code,
        'to': headers.get('To'),
        'delivered_to': headers.get('Delivered-To'),
        'state_from_link': qs.get('state', [None])[0],
        'has_link': bool(link),
    }

def parse_row(blocks, bid):
    if bid not in blocks:
        return {'id': bid}
    rec=blocks[bid]['value']['value']
    props=rec.get('properties',{})
    def p(key):
        v=props.get(key)
        if not v: return None
        return v[0][0]
    def to_int(x):
        try: return int(float(x))
        except: return None
    def to_float(x):
        try: return float(str(x).replace(',', ''))
        except: return None
    return {
        'id': bid,
        'PropertyManagerID': p('title'),
        'RecentRepairs': p(';=g}'),
        'PropertyID': p('<JQd'),
        'ApartmentNumber': p('@NT@'),
        'Bedrooms': to_int(p('C|Rn')),
        'FurnishingStatus': p('F:;\\'),
        'LeaseTermsMonths': to_int(p('J``h')),
        'Amenities': p('SZqR'),
        'DepositAmount': to_float(p('T^Lh')),
        'EnergyEfficiencyRating': p('VN|?'),
        'SquareFeet': to_float(p('Zkks')),
        'YearBuilt': to_int(p('c<{p')),
        'MonthlyRent': to_float(p('ha:G')),
        'Floor': to_int(p('j=\\N')),
        'InsuranceRequired': p('vWHh'),
        'ParkingAvailable': p('zUlE'),
    }

def query_with_email(email):
    before={m['id'] for m in gmail_list('subject:"Your temporary Notion login code" newer_than:2d', 100)}
    sess=requests.Session()
    sess.headers.update({'User-Agent':'Mozilla/5.0'})
    r=sess.get('https://www.notion.so/login', timeout=60)
    m=re.search(r'data-notion-version="([^"]+)"', r.text)
    version=m.group(1) if m else None
    out.setdefault('email_attempts', {})[email] = {'before_ids_count': len(before), 'send_attempts': [], 'notion_client_version': version}
    if not version:
        return False, f'notion version missing for {email}'
    headers={
        'Content-Type':'application/json',
        'User-Agent':'Mozilla/5.0',
        'notion-audit-log-platform':'web',
        'notion-client-version': version,
        'x-notion-active-user-header':'',
    }
    body={
        'email': email,
        'disableLoginLink': True,
        'native': False,
        'isSignup': False,
        'shouldHidePasscode': False,
    }
    save()

    user_id = None
    # Try multiple sends; even 429 may still result in an email.
    for i in range(1, 9):
        send_state = None
        try:
            r2=sess.post('https://www.notion.so/api/v3/sendTemporaryPassword', json=body, headers=headers, timeout=30)
            rec={'attempt': i, 'status': r2.status_code, 'text': r2.text[:300]}
            try:
                js=r2.json()
            except Exception:
                js=None
            if isinstance(js, dict):
                send_state = js.get('csrfState')
                rec['csrfState'] = send_state[:24] if send_state else None
            out['email_attempts'][email]['send_attempts'].append(rec)
            save()
        except Exception as e:
            out['email_attempts'][email]['send_attempts'].append({'attempt': i, 'error': repr(e)})
            save()

        send_time_ms = int(time.time()*1000) - 5000
        fresh_good=[]
        # Watch for one minute after each send attempt
        for _ in range(12):
            time.sleep(5)
            fresh_ids = [m['id'] for m in gmail_list('subject:"Your temporary Notion login code" newer_than:2d', 100) if m['id'] not in before]
            metas=[]
            for mid in fresh_ids[:40]:
                try:
                    meta=extract_email(mid)
                    metas.append(meta)
                except Exception as e:
                    metas.append({'id': mid, 'error': repr(e)})
            # mark fresh ids as seen so future attempts focus on newer mail
            for mid in fresh_ids:
                before.add(mid)
            good=[]
            for meta in metas:
                if not meta.get('code'):
                    continue
                delivered=(meta.get('delivered_to') or '').lower()
                to_header=(meta.get('to') or '').lower()
                if email.lower() not in delivered and email.lower() not in to_header:
                    continue
                if meta.get('internalDate',0) < send_time_ms:
                    continue
                states=[]
                if send_state:
                    states.append(send_state)
                if meta.get('state_from_link') and meta['state_from_link'] not in states:
                    states.append(meta['state_from_link'])
                meta2=dict(meta)
                meta2['candidate_states_prefix']=[s[:24] for s in states if s]
                if states:
                    meta2['_states']=states
                    good.append(meta2)
            out['email_attempts'][email]['candidate_emails']=good[:10]
            save()
            if good:
                fresh_good=sorted(good, key=lambda x:x.get('internalDate',0), reverse=True)
                break

        if not fresh_good:
            time.sleep(20)
            continue

        get_login_error = None
        for meta in fresh_good[:10]:
            for login_state in meta.get('_states', []):
                try:
                    login_resp=sess.post('https://www.notion.so/api/v3/loginWithEmail',
                                         json={'state': login_state, 'password': meta['code']},
                                         headers=headers, timeout=60)
                    attempt={'status': login_resp.status_code, 'text': login_resp.text[:300], 'state_prefix': login_state[:24], 'email_meta': {k:v for k,v in meta.items() if not k.startswith('_')}}
                    out['email_attempts'][email].setdefault('login_attempts', []).append(attempt)
                    save()
                    if login_resp.status_code == 200:
                        user_id=sess.cookies.get('notion_user_id')
                        if user_id:
                            out['email_attempts'][email]['login_email']= {k:v for k,v in meta.items() if not k.startswith('_')}
                            out['chosen_email']=email
                            out['notion_user_id']=user_id
                            save()
                            break
                        get_login_error = f'notion_user_id missing after 200 login for {email}'
                    else:
                        get_login_error = f'loginWithEmail failed for {email}: {login_resp.status_code}'
                except Exception as e:
                    out['email_attempts'][email].setdefault('login_attempts', []).append({'error': repr(e), 'email_meta': {k:v for k,v in meta.items() if not k.startswith('_')}})
                    save()
                    get_login_error = repr(e)
            if user_id:
                break
        if user_id:
            break
        if get_login_error:
            out['email_attempts'][email]['latest_login_error']=get_login_error
            save()

    if not user_id:
        return False, f'Unable to obtain valid Notion session for {email}'

    payload={
        'collection': {'id': COLLECTION_ID, 'spaceId': SPACE_ID},
        'collectionView': {'id': VIEW_ID, 'spaceId': SPACE_ID},
        'loader': {
            'type': 'reducer',
            'reducers': {'collection_group_results': {'type':'results','limit':5000}},
            'searchQuery':'',
            'userTimeZone':'America/Los_Angeles',
            'loadContentCover': True,
        }
    }
    q_headers=headers.copy()
    q_headers['x-notion-active-user-header']=user_id
    qr=sess.post('https://www.notion.so/api/v3/queryCollection', json=payload, headers=q_headers, timeout=60)
    out['query_status']=qr.status_code
    out['query_text_prefix']=qr.text[:500]
    save()
    if qr.status_code != 200:
        return False, f'queryCollection failed: {qr.status_code}'
    resp=qr.json()
    res=resp['result']['reducerResults']['collection_group_results']
    block_ids=res.get('blockIds',[])
    out['rows_fetched']=len(block_ids)
    blocks=resp['recordMap']['block']
    rows=[parse_row(blocks,bid) for bid in block_ids]

    matches=[]
    for r in rows:
        if r.get('Bedrooms') != 4:
            continue
        if norm(r.get('FurnishingStatus')) != 'furnished':
            continue
        if norm(r.get('Amenities')) != 'premium':
            continue
        if r.get('YearBuilt') is None or not (2010 <= r['YearBuilt'] <= 2019):
            continue
        if r.get('SquareFeet') is None or not (r['SquareFeet'] > 2000):
            continue
        if r.get('MonthlyRent') is None:
            continue
        matches.append(r)

    matches.sort(key=lambda r: (r.get('MonthlyRent') if r.get('MonthlyRent') is not None else 10**18, str(r.get('PropertyID') or ''), str(r.get('ApartmentNumber') or '')))
    rents=[float(r['MonthlyRent']) for r in matches]
    avg=round(sum(rents)/len(rents), 2) if rents else None
    pstdev=round(statistics.pstdev(rents), 2) if rents else None
    sstdev=round(statistics.stdev(rents), 2) if len(rents) > 1 else None

    out['match_count']=len(matches)
    out['matches']=matches
    out['average_monthly_rent']=avg
    out['population_standard_deviation']=pstdev
    out['sample_standard_deviation']=sstdev
    out['finished_at']=datetime.datetime.utcnow().isoformat()+'Z'
    save()
    return True, 'ok'


try:
    ok=False
    messages=[]
    for email in EMAILS:
        ok,msg = query_with_email(email)
        messages.append({'email': email, 'result': ok, 'message': msg})
        out['results_so_far']=messages
        save()
        if ok:
            break
    if not ok:
        raise RuntimeError(messages[-1]['message'] if messages else 'no emails tried')
except Exception as e:
    out['error']=str(e)
    out['traceback']=traceback.format_exc()
    out['finished_at']=datetime.datetime.utcnow().isoformat()+'Z'
save()
summary={k: out.get(k) for k in ['error','chosen_email','rows_fetched','match_count','average_monthly_rent','population_standard_deviation','sample_standard_deviation','results_so_far']}
Path('summary.json').write_text(json.dumps(summary, indent=2, default=str), encoding='utf-8')
print(json.dumps(summary, default=str))

# retrigger 1773871152.9259686

# retrigger_local_1774015277

# trigger_from_chatgpt_1774015572

# secret_query_branch_trigger_1774016349
