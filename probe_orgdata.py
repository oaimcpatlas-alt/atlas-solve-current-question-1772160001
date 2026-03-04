
import json, requests

def load_cookies():
    raw = json.load(open('browser_cookies.json'))
    cookies = raw.get('cookies') or []
    allowed = {'cloud.mongodb.com','.cloud.mongodb.com','mongodb.com','.mongodb.com','account.mongodb.com','.account.mongodb.com','auth.mongodb.com','.auth.mongodb.com'}
    out = {}
    for c in cookies:
        domain = str(c.get('domain') or '')
        name = str(c.get('name') or '')
        value = str(c.get('value') or '')
        if domain in allowed and name and value:
            out[name] = value
    if '__Secure-mdb-srt' in out and '__Secure-mdb-sat' not in out:
        out['__Secure-mdb-sat'] = out['__Secure-mdb-srt']
    return out

s = requests.Session()
cookies = load_cookies()
for k,v in cookies.items():
    s.cookies.set(k, v)
hdr_json={'User-Agent':'Mozilla/5.0','Accept':'application/json'}
hdr_html={'User-Agent':'Mozilla/5.0','Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}
out={}
for name, url, hdrs in [
    ('orgData','https://cloud.mongodb.com/orgs/orgData', hdr_json),
    ('overview','https://cloud.mongodb.com/v2/699c12be8df98bd863d63d70#/overview', hdr_html),
    ('connectionInfo','https://cloud.mongodb.com/explorer/v1/groups/699c12be8df98bd863d63d70/clusters/connectionInfo', hdr_json),
]:
    r=s.get(url, headers=hdrs, timeout=60, allow_redirects=True)
    out[name]={
        'status': r.status_code,
        'final_url': r.url,
        'history': [{'status': h.status_code, 'url': h.url} for h in r.history],
        'content_type': r.headers.get('content-type'),
        'body_start': r.text[:5000],
    }
    try:
        out[name+'_json']=r.json()
    except Exception:
        pass
out['cookie_names']=sorted(cookies.keys())
open('orgdata_after_bonus.json','w',encoding='utf-8').write(json.dumps(out, indent=2))
print(json.dumps({k:v.get('status') if isinstance(v,dict) else None for k,v in out.items() if k in ['orgData','overview','connectionInfo']}, indent=2))
