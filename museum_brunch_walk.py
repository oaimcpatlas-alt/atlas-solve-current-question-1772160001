import json, os, re, traceback, requests
from datetime import datetime

OUT = {"started_at": datetime.utcnow().isoformat() + "Z"}

def refresh_access_token():
    r = requests.post(
        'https://oauth2.googleapis.com/token',
        data={
            'client_id': os.environ.get('GOOGLE_CLIENT_ID',''),
            'client_secret': os.environ.get('GOOGLE_CLIENT_SECRET',''),
            'refresh_token': os.environ.get('GOOGLE_REFRESH_TOKEN',''),
            'grant_type': 'refresh_token',
        },
        timeout=30,
    )
    OUT['token_status'] = r.status_code
    try:
        OUT['token_body'] = r.json()
    except Exception:
        OUT['token_text'] = r.text[:2000]
    r.raise_for_status()
    return r.json()['access_token']

def list_events(token, time_min, time_max):
    headers = {'Authorization': f'Bearer {token}'}
    params = {
        'calendarId': 'primary',
        'timeMin': time_min,
        'timeMax': time_max,
        'singleEvents': 'true',
        'orderBy': 'startTime',
        'maxResults': '250',
    }
    r = requests.get(
        'https://www.googleapis.com/calendar/v3/calendars/primary/events',
        headers=headers,
        params=params,
        timeout=30,
    )
    OUT['events_status'] = r.status_code
    body = r.json()
    OUT['events_raw_count'] = len(body.get('items') or [])
    r.raise_for_status()
    return body.get('items') or []

def ser_event(ev):
    return {
        'id': ev.get('id'),
        'summary': ev.get('summary'),
        'location': ev.get('location'),
        'description': (ev.get('description') or '')[:1000],
        'start': (ev.get('start') or {}).get('dateTime') or (ev.get('start') or {}).get('date'),
        'end': (ev.get('end') or {}).get('dateTime') or (ev.get('end') or {}).get('date'),
        'htmlLink': ev.get('htmlLink'),
    }

MUSEUM_RE = re.compile(r'\b(museum|met|moma|guggenheim|gallery|exhibit|exhibition|art)\b', re.I)
BRUNCH_RE = re.compile(r'\b(brunch|breakfast|restaurant|cafe|café|bistro|diner)\b', re.I)

def same_day(event):
    start = (event.get('start') or {}).get('dateTime') or (event.get('start') or {}).get('date') or ''
    return start[:10]

try:
    token = refresh_access_token()
    events = list_events(token, '2025-07-16T00:00:00Z', '2025-08-01T00:00:00Z')
    OUT['events'] = [ser_event(e) for e in events]
    museum_events = []
    by_day = {}
    for ev in events:
        day = same_day(ev)
        by_day.setdefault(day, []).append(ev)
        hay = ' '.join([ev.get('summary',''), ev.get('location',''), ev.get('description','')])
        if MUSEUM_RE.search(hay):
            museum_events.append(ev)

    OUT['museum_events'] = [ser_event(e) for e in museum_events]
    pairings = []
    for mev in museum_events:
        day = same_day(mev)
        day_events = by_day.get(day, [])
        mev_start = (mev.get('start') or {}).get('dateTime') or ''
        candidates = []
        for ev in day_events:
            if ev is mev:
                continue
            ev_start = (ev.get('start') or {}).get('dateTime') or ''
            if ev_start and mev_start and ev_start < mev_start:
                hay = ' '.join([ev.get('summary',''), ev.get('location',''), ev.get('description','')])
                score = 0
                if BRUNCH_RE.search(hay): score += 5
                if ev.get('location'): score += 2
                if 'food' in hay.lower() or 'eat' in hay.lower(): score += 1
                candidates.append((score, ev_start, ev))
        candidates.sort(key=lambda t: (-t[0], t[1]))
        pairings.append({
            'museum_event': ser_event(mev),
            'earlier_events_same_day': [ser_event(ev) for _score,_start,ev in candidates[:10]],
            'best_candidate': ser_event(candidates[0][2]) if candidates else None,
        })
    OUT['pairings'] = pairings
except Exception as e:
    OUT['error'] = repr(e)
    OUT['traceback'] = traceback.format_exc()

with open('museum_brunch_walk_result.json', 'w', encoding='utf-8') as f:
    json.dump(OUT, f, indent=2)
print(json.dumps(OUT, indent=2)[:80000])
