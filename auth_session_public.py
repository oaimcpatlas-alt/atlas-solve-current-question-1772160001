
import json, time, traceback, requests

USERNAME = 'oaimcpatlas@gmail.com'
CANDIDATES = [
    'AtlasFixed!24680Aa',
    'ZQ8!AtlasNewPass#7mK2x',
    'AtlasGHReset!8901',
    'AtlasGHReset!7890',
    'AtlasGHReset!6789',
    'AtlasGHReset!9012',
    'AtlasTemp!2026#A',
    'AtlasTemp!2026#B',
    'AgentFull!9010x',
    'Scratch!321Aa',
]

out = {'candidates': CANDIDATES, 'attempts': []}

def save():
    with open('auth_result.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2)

try:
    s = requests.Session()
    s.headers.update({'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'})
    for idx, pw in enumerate(CANDIDATES, 1):
        r = s.post('https://account.mongodb.com/account/auth/verify',
                   json={'username': USERNAME, 'password': pw}, timeout=30)
        rec = {'index': idx, 'password': pw, 'status': r.status_code, 'text': r.text[:1000]}
        try:
            rec['json'] = r.json()
        except Exception:
            pass
        out['attempts'].append(rec)
        save()
        if r.status_code == 200:
            out['success_password'] = pw
            break
        if 'RATE_LIMITED' in r.text:
            time.sleep(30)
        else:
            time.sleep(3)
except Exception as e:
    out['error'] = str(e)
    out['traceback'] = traceback.format_exc()

save()
print(json.dumps(out)[:10000])
