
import os, json, urllib.request, urllib.error, urllib.parse, traceback, re, math
from datetime import datetime, timezone

QUESTION = ("I'm auditing how the marketing departments non recurring budget allocations have changed over the years. "
            "According to our financial records in the database, how much did the total budget allocated to non recurring "
            "marketing transactions vary between January 2020 and January 2021, depending on the transaction dates? "
            "Was it an increase or decrease, and by what percentage?")

OUT = {
    "question": QUESTION,
    "started_at": datetime.now(timezone.utc).isoformat(),
    "token_lengths": {k: len(os.environ.get(k, "")) for k in ["AIRTABLE_API_KEY", "AIRTABLE_PAT", "AIRTABLE_TOKEN"]},
    "token_attempts": [],
    "bases": [],
    "table_scan": [],
    "selected": None,
    "full_scan": None,
    "answer": None,
}

RESULT_PATH = os.path.join("outputs", "marketing_nonrecurring_budget_answer.json")

def save():
    os.makedirs("outputs", exist_ok=True)
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(OUT, f, indent=2, ensure_ascii=False, default=str)

def req_json(url, token, params=None):
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": "python",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = resp.read()
    return json.loads(body.decode("utf-8"))

def try_tokens():
    for name in ["AIRTABLE_API_KEY", "AIRTABLE_PAT", "AIRTABLE_TOKEN"]:
        token = os.environ.get(name, "")
        if not token:
            OUT["token_attempts"].append({"name": name, "status": "missing"})
            continue
        try:
            data = req_json("https://api.airtable.com/v0/meta/bases", token)
            bases = data.get("bases", [])
            OUT["token_attempts"].append({"name": name, "status": "ok", "base_count": len(bases)})
            save()
            return token, bases
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8")
            except Exception:
                body = ""
            OUT["token_attempts"].append({"name": name, "status": e.code, "body": body[:500]})
        except Exception as e:
            OUT["token_attempts"].append({"name": name, "status": "error", "error": repr(e)})
        save()
    return None, []

def get_tables(token, base_id):
    return req_json(f"https://api.airtable.com/v0/meta/bases/{base_id}/tables", token).get("tables", [])

def iter_records(token, base_id, table_name, max_pages=None):
    offset = None
    pages = 0
    encoded_table = urllib.parse.quote(table_name, safe="")
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = req_json(f"https://api.airtable.com/v0/{base_id}/{encoded_table}", token, params=params)
        for rec in data.get("records", []):
            yield rec
        offset = data.get("offset")
        pages += 1
        if not offset:
            break
        if max_pages is not None and pages >= max_pages:
            break

def norm(s):
    return re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()

def norm_key(s):
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())

def to_number(v):
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", "").replace("$", "").replace("€", "").replace("£", "").replace("%", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

def parse_date(v):
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        # unlikely Airtable numeric date
        return None
    s = str(v).strip()
    if not s:
        return None
    ss = s.replace("Z", "+00:00")
    for val in [ss, s]:
        try:
            if "T" in val:
                return datetime.fromisoformat(val)
            if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                return datetime.strptime(val, "%Y-%m-%d")
        except Exception:
            pass
    for fmt in [
        "%m/%d/%Y", "%d/%m/%Y", "%m/%d/%y", "%d/%m/%y",
        "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%m-%d-%Y", "%d-%m-%Y",
        "%b %d %Y", "%B %d %Y", "%d %b %Y", "%d %B %Y",
        "%b %d, %Y", "%B %d, %Y", "%d %b, %Y", "%d %B, %Y",
    ]:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def flatten_value(v, prefix=""):
    out = {}
    if isinstance(v, dict):
        for k, vv in v.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            out.update(flatten_value(vv, p))
    elif isinstance(v, list):
        if not v:
            out[prefix] = ""
        else:
            # Add joined text for easier detection + individual items
            joined = ", ".join("" if x is None else str(x) for x in v)
            out[prefix] = joined
            for i, vv in enumerate(v):
                p = f"{prefix}[{i}]"
                out.update(flatten_value(vv, p))
    else:
        out[prefix] = v
    return out

def table_meta_score(table):
    score = 0
    tname = norm(table.get("name"))
    keys = [norm(f.get("name")) for f in table.get("fields", [])]
    blob = " | ".join([tname] + keys)
    for tok, pts in [
        ("financial", 50), ("finance", 45), ("budget", 45), ("transaction", 45),
        ("expense", 40), ("records", 18), ("allocation", 25), ("allocated", 20),
        ("department", 18), ("marketing", 16), ("recurring", 22)
    ]:
        if tok in blob:
            score += pts
    if any("budget" in k for k in keys):
        score += 30
    if any("transaction" in k and "date" in k for k in keys):
        score += 35
    if any("department" in k or "dept" in k or "team" in k or "area" in k or "function" in k for k in keys):
        score += 18
    if any("recurring" in k or "recurrence" in k for k in keys):
        score += 25
    return score

def score_text_match(nk, nv, true_terms, field_terms=()):
    if not nv:
        return 0
    score = 0
    if any(term in nv for term in true_terms):
        score += 3
        if any(term in nk for term in field_terms):
            score += 4
        if nk in field_terms:
            score += 2
    return score

def analyze_record(rec):
    flat = flatten_value(rec.get("fields", {}))
    dept_best = None
    recur_best = None
    date_best = None
    budget_best = None
    blob_parts = []

    for path, v in flat.items():
        if path == "":
            continue
        nk = norm_key(path)
        nv = norm(v)
        if nv:
            blob_parts.append(f"{path}={v}")

        # department / marketing
        dscore = 0
        if "marketing" in nv:
            dscore += 2
            if any(tok in nk for tok in ["department", "dept", "team", "area", "function", "division", "unit", "category"]):
                dscore += 5
            if nk in {"department", "dept", "team", "area", "function"}:
                dscore += 2
        if dscore > 0:
            cand = {"field": path, "value": v, "score": dscore}
            if not dept_best or cand["score"] > dept_best["score"]:
                dept_best = cand

        # non recurring
        rscore = 0
        non_terms = ["non recurring", "non-recurring", "nonrecurring", "one time", "one-time", "one off", "one-off"]
        if any(term in nv for term in non_terms):
            rscore += 3
            if any(tok in nk for tok in ["recurring", "recurrence", "transactiontype", "type", "category", "expense", "payment"]):
                rscore += 4
            if nk in {"recurring", "recurrence", "transactiontype", "type", "category"}:
                rscore += 2
        # handle boolean-ish "No" in recurring field
        if any(tok in nk for tok in ["recurring", "isrecurring"]) and nv in {"no", "false", "n", "0"}:
            rscore += 5
        if rscore > 0:
            cand = {"field": path, "value": v, "score": rscore}
            if not recur_best or cand["score"] > recur_best["score"]:
                recur_best = cand

        # date
        dt = parse_date(v)
        if dt:
            score = 0
            if "transaction" in nk and "date" in nk:
                score += 10
            elif "date" in nk:
                score += 4
            if any(tok in nk for tok in ["posted", "booked", "created", "payment"]) and "date" in nk:
                score += 1
            if score > 0:
                cand = {"field": path, "value": str(v), "date": dt, "score": score}
                if not date_best or cand["score"] > date_best["score"]:
                    date_best = cand

        # budget amount
        num = to_number(v)
        if num is not None:
            score = 0
            if "budget" in nk:
                score += 10
            if "allocated" in nk or "allocation" in nk:
                score += 8
            if nk in {"budget", "budgetallocated", "allocatedbudget", "budgetallocation"}:
                score += 4
            if "amount" in nk and score > 0:
                score += 2
            if any(bad in nk for bad in ["score", "rating", "percent", "quantity", "count", "id", "phone", "zip", "year"]):
                score -= 6
            if score > 0:
                cand = {"field": path, "value": num, "raw": v, "score": score}
                if not budget_best or cand["score"] > budget_best["score"]:
                    budget_best = cand

    blob = norm(" | ".join(blob_parts))
    if not dept_best and "marketing" in blob:
        dept_best = {"field": "blob", "value": "marketing", "score": 1}
    if not recur_best:
        for term in ["non recurring", "non-recurring", "nonrecurring", "one time", "one-time", "one off", "one-off"]:
            if term in blob:
                recur_best = {"field": "blob", "value": term, "score": 1}
                break

    match = bool(dept_best and recur_best and date_best and budget_best)
    target_bucket = None
    if date_best:
        dt = date_best["date"]
        if dt.year == 2020 and dt.month == 1:
            target_bucket = "2020-01"
        elif dt.year == 2021 and dt.month == 1:
            target_bucket = "2021-01"

    return {
        "department": dept_best,
        "recurrence": recur_best,
        "date": {
            "field": date_best["field"],
            "value": date_best["value"],
            "iso": date_best["date"].isoformat(),
            "score": date_best["score"],
        } if date_best else None,
        "budget": budget_best,
        "match": match,
        "target_bucket": target_bucket,
        "sample_blob": " | ".join(blob_parts)[:800],
    }

def scan_table_sample(token, base, table):
    meta_score = table_meta_score(table)
    scan = {
        "base_id": base.get("id"),
        "base_name": base.get("name"),
        "table_id": table.get("id"),
        "table_name": table.get("name"),
        "meta_score": meta_score,
        "field_names": [f.get("name") for f in table.get("fields", [])],
        "sample_records": 0,
        "matched_records": 0,
        "bucket_hits": {"2020-01": 0, "2021-01": 0},
        "sample_sums": {"2020-01": 0.0, "2021-01": 0.0},
        "examples": [],
    }

    should_sample = meta_score >= 20
    if not should_sample:
        # still sample if table name/fields mention any single major concept
        fn_blob = " | ".join(scan["field_names"]).lower()
        should_sample = any(tok in (table.get("name","").lower() + " " + fn_blob) for tok in ["budget", "transaction", "financial", "expense", "marketing"])
    if not should_sample:
        scan["score"] = meta_score
        return scan

    try:
        for rec in iter_records(token, base["id"], table["name"], max_pages=1):
            scan["sample_records"] += 1
            analysis = analyze_record(rec)
            if analysis["match"]:
                scan["matched_records"] += 1
                if len(scan["examples"]) < 5:
                    scan["examples"].append({
                        "id": rec.get("id"),
                        "fields": rec.get("fields"),
                        "analysis": analysis,
                    })
            if analysis["target_bucket"] and analysis["budget"]:
                scan["bucket_hits"][analysis["target_bucket"]] += 1
                scan["sample_sums"][analysis["target_bucket"]] += float(analysis["budget"]["value"])
            if scan["sample_records"] >= 100:
                break
    except Exception as e:
        scan["sample_error"] = repr(e)

    scan["score"] = (
        meta_score
        + scan["matched_records"] * 80
        + (15 if scan["bucket_hits"]["2020-01"] else 0)
        + (15 if scan["bucket_hits"]["2021-01"] else 0)
    )
    return scan

def full_compute(token, selected):
    base_id = selected["base_id"]
    table_name = selected["table_name"]
    result = {
        "base_id": base_id,
        "base_name": selected["base_name"],
        "table_name": table_name,
        "rows_scanned": 0,
        "matched_rows": 0,
        "sums": {"2020-01": 0.0, "2021-01": 0.0},
        "match_buckets": {"2020-01": 0, "2021-01": 0},
        "examples": {"2020-01": [], "2021-01": []},
    }

    for rec in iter_records(token, base_id, table_name, max_pages=None):
        result["rows_scanned"] += 1
        analysis = analyze_record(rec)
        if not analysis["match"]:
            continue
        result["matched_rows"] += 1
        bucket = analysis["target_bucket"]
        if bucket and analysis["budget"] is not None:
            val = float(analysis["budget"]["value"])
            result["sums"][bucket] += val
            result["match_buckets"][bucket] += 1
            if len(result["examples"][bucket]) < 10:
                result["examples"][bucket].append({
                    "id": rec.get("id"),
                    "fields": rec.get("fields"),
                    "analysis": analysis,
                    "budget_value": val,
                })

    jan20 = result["sums"]["2020-01"]
    jan21 = result["sums"]["2021-01"]
    diff = jan21 - jan20
    direction = "increase" if diff > 0 else "decrease" if diff < 0 else "no change"
    pct_change = None
    if jan20 != 0:
        pct_change = (diff / jan20) * 100.0

    result["computed"] = {
        "january_2020_total": jan20,
        "january_2021_total": jan21,
        "difference": diff,
        "direction": direction,
        "percentage_change": pct_change,
    }
    return result

def main():
    save()
    token, bases = try_tokens()
    if not token:
        raise RuntimeError("No working Airtable token")
    OUT["bases"] = [{"id": b.get("id"), "name": b.get("name"), "permissionLevel": b.get("permissionLevel")} for b in bases]
    save()

    for base in bases:
        try:
            tables = get_tables(token, base["id"])
        except Exception as e:
            OUT.setdefault("base_errors", []).append({"base_id": base.get("id"), "base_name": base.get("name"), "error": repr(e)})
            save()
            continue
        for table in tables:
            scan = scan_table_sample(token, base, table)
            OUT["table_scan"].append(scan)
            OUT["table_scan"] = sorted(OUT["table_scan"], key=lambda x: x.get("score", 0), reverse=True)[:120]
            save()

    if not OUT["table_scan"]:
        raise RuntimeError("No tables scanned")

    best = sorted(OUT["table_scan"], key=lambda x: x.get("score", 0), reverse=True)[0]
    OUT["selected"] = best
    save()

    full = full_compute(token, best)
    OUT["full_scan"] = full

    comp = full["computed"]
    OUT["answer"] = {
        "base_name": full["base_name"],
        "table_name": full["table_name"],
        "january_2020_total": round(comp["january_2020_total"], 2),
        "january_2021_total": round(comp["january_2021_total"], 2),
        "difference": round(comp["difference"], 2),
        "direction": comp["direction"],
        "percentage_change": None if comp["percentage_change"] is None else round(comp["percentage_change"], 2),
        "matched_rows": full["matched_rows"],
        "bucket_counts": full["match_buckets"],
    }
    save()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        OUT["error"] = str(e)
        OUT["traceback"] = traceback.format_exc()
        save()
        raise
