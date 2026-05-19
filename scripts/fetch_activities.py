#!/usr/bin/env python3
"""
DriveGO Activities ETL: fetch from two complementary sources, filter to
a 180-day forward window, clean descriptions, output unified JSON.

Sources
-------
* TDX Tourism Activity API
    - National coverage (12 cities with real data), all event categories
      (festivals, marathons, expos, ...). Taipei has near-zero data.
* travel.taipei Open API (Events/Activity)
    - Taipei-only, exhibition-focused. Very high description quality.
    - Requires Chrome TLS impersonation via curl_cffi to bypass the
      Cloudflare bot filter sitting in front of the API.

Output : ../data/activities.json (relative to this script's directory)

Usage
-----
    python3 drivego_fetch_activities.py

Env required (loaded from .env in same dir, or runtime env in CI):
    TDX_CLIENT_ID
    TDX_CLIENT_SECRET
"""

import html
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# curl_cffi is only needed for travel.taipei. If unavailable, that source
# is skipped and the TDX pipeline still runs.
try:
    from curl_cffi import requests as curl_requests   # type: ignore
    _HAS_CURL_CFFI = True
except ImportError:
    _HAS_CURL_CFFI = False


SCRIPT_DIR = Path(__file__).resolve().parent
OUT_PATH = SCRIPT_DIR.parent / "data" / "activities.json"

# ---- TDX -------------------------------------------------------------
TDX_TOKEN_URL = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
TDX_API_URL   = "https://tdx.transportdata.tw/api/basic/v2/Tourism/Activity"
TDX_PAGE_SIZE = 500

# ---- travel.taipei ---------------------------------------------------
TT_API_URL     = "https://www.travel.taipei/open-api/zh-tw/Events/Activity"
TT_IMPERSONATE = "chrome120"
TT_PAGE_SIZE   = 30   # API-defined, not configurable

# ---- Shared ----------------------------------------------------------
WINDOW_DAYS          = 180
PLACEHOLDER_END_YEAR = 2030
TAIPEI_TZ            = timezone(timedelta(hours=8))

DESCRIPTION_BLOCKLIST = {
    "", "-", "—", "N/A", "n/a", "無", "無。", "(空)",
    "詳見官網", "詳見活動官網", "請見活動官網", "詳見主辦單位官網",
    "to see the official site",
}
MIN_DESCRIPTION_LENGTH = 20


# ======================================================================
# TDX fetch + normalize
# ======================================================================

def tdx_get_token() -> str:
    cid  = os.environ.get("TDX_CLIENT_ID")
    csec = os.environ.get("TDX_CLIENT_SECRET")
    if not cid or not csec:
        print("ERROR: TDX_CLIENT_ID / TDX_CLIENT_SECRET missing", file=sys.stderr)
        sys.exit(1)
    resp = requests.post(TDX_TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     cid,
        "client_secret": csec,
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def tdx_fetch_all(token: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    out, skip = [], 0
    while True:
        resp = requests.get(
            TDX_API_URL,
            headers=headers,
            params={"$top": TDX_PAGE_SIZE, "$skip": skip, "$format": "JSON"},
            timeout=60,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        out.extend(batch)
        if len(batch) < TDX_PAGE_SIZE:
            break
        skip += TDX_PAGE_SIZE
    return out


def tdx_normalize(raw: dict):
    if not raw.get("ActivityID") or not raw.get("ActivityName"):
        return None
    s_time = raw.get("StartTime") or ""
    e_time = raw.get("EndTime") or ""
    if len(s_time) < 10 or len(e_time) < 10:
        return None
    pos = raw.get("Position") or {}
    lat = pos.get("PositionLat")
    lng = pos.get("PositionLon")
    if lat is None or lng is None:
        return None
    return {
        "id":          f"tdx-{raw['ActivityID']}",
        "source":      "tdx",
        "name":        raw["ActivityName"].strip(),
        "city":        (raw.get("City") or "").strip() or None,
        "startDate":   s_time[:10],
        "endDate":     e_time[:10],
        "lat":         float(lat),
        "lng":         float(lng),
        "description": clean_description(raw.get("Description")),
        "organizer":   (raw.get("Organizer") or "").strip() or None,
        "phone":       (raw.get("Phone") or "").strip() or None,
        "category":    (raw.get("Class1") or "").strip() or None,
        "detailUrl":   None,
    }


# ======================================================================
# travel.taipei fetch + normalize
# ======================================================================

def tt_fetch_all() -> list[dict]:
    """Paginate travel.taipei Events/Activity. Window is sent to the API
    directly via begin/end params so we don't waste bandwidth."""
    if not _HAS_CURL_CFFI:
        print("      (curl_cffi unavailable — skipping travel.taipei)")
        return []
    today      = datetime.now(TAIPEI_TZ).date()
    window_end = today + timedelta(days=WINDOW_DAYS)
    out, page = [], 1
    while True:
        params = {
            "begin": today.isoformat(),
            "end":   window_end.isoformat(),
            "page":  page,
        }
        try:
            r = curl_requests.get(
                TT_API_URL,
                params=params,
                impersonate=TT_IMPERSONATE,
                headers={"Accept": "application/json"},
                timeout=30,
            )
        except Exception as e:
            print(f"      page {page} request failed: {e}")
            break
        if r.status_code != 200:
            print(f"      page {page}: HTTP {r.status_code}, stop")
            break
        payload = r.json()
        batch = payload.get("data") or []
        total = payload.get("total") or 0
        out.extend(batch)
        if not batch or len(out) >= total or len(batch) < TT_PAGE_SIZE:
            break
        page += 1
    return out


def _tt_parse_date(s: str) -> str | None:
    """travel.taipei dates look like '2026-04-21 00:00:00 +08:00'. We
    only need the date portion. Returns 'YYYY-MM-DD' or None."""
    if not s or len(s) < 10:
        return None
    return s[:10]


def tt_normalize(raw: dict):
    if not raw.get("id") or not raw.get("title"):
        return None
    s_date = _tt_parse_date(raw.get("begin") or "")
    e_date = _tt_parse_date(raw.get("end") or "")
    if not s_date or not e_date:
        return None
    # Coordinates are strings ("25.1024"). 0/empty are non-locatable.
    try:
        lat = float(raw.get("nlat") or 0)
        lng = float(raw.get("elong") or 0)
    except (TypeError, ValueError):
        return None
    if lat == 0 or lng == 0:
        return None
    return {
        "id":          f"tt-{raw['id']}",
        "source":      "travel.taipei",
        "name":        raw["title"].strip(),
        "city":        "臺北市",
        "startDate":   s_date,
        "endDate":     e_date,
        "lat":         lat,
        "lng":         lng,
        "description": clean_description(raw.get("description")),
        "organizer":   (raw.get("organizer") or "").strip() or None,
        "phone":       (raw.get("tel") or "").strip() or None,
        "category":    None,   # travel.taipei has no analogue to Class1
        "detailUrl":   (raw.get("url") or "").strip() or None,
    }


# ======================================================================
# Shared helpers
# ======================================================================

def clean_description(raw):
    """Strip HTML, decode entities, drop placeholder filler, enforce
    a minimum length. Returns cleaned string or None."""
    if not raw:
        return None
    s = raw.strip()
    if s in DESCRIPTION_BLOCKLIST:
        return None
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    s = re.sub(r"[ \t]+", " ", s).strip()
    s = re.sub(r"\n{3,}", "\n\n", s)
    if len(s) < MIN_DESCRIPTION_LENGTH:
        return None
    return s


def filter_window(items: list[dict]) -> list[dict]:
    today      = datetime.now(TAIPEI_TZ).date()
    window_end = today + timedelta(days=WINDOW_DAYS)
    out = []
    for a in items:
        if a["startDate"] > window_end.isoformat():
            continue
        if a["endDate"] < today.isoformat():
            continue
        if a["endDate"][:4] >= str(PLACEHOLDER_END_YEAR):
            continue
        out.append(a)
    return out


def print_summary(items: list[dict]) -> None:
    total = len(items)
    if total == 0:
        print("  (empty)")
        return
    with_desc = sum(1 for a in items if a.get("description"))
    by_source: dict[str, int] = {}
    by_city:   dict[str, int] = {}
    for a in items:
        by_source[a["source"]] = by_source.get(a["source"], 0) + 1
        c = a.get("city") or "(未標)"
        by_city[c] = by_city.get(c, 0) + 1
    print(f"  total:            {total}")
    print(f"  with description: {with_desc} ({with_desc*100//total}%)")
    print(f"  by source:")
    for s, n in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"    {s:<14} {n}")
    print(f"  by city:")
    for c, n in sorted(by_city.items(), key=lambda x: -x[1]):
        print(f"    {c:<8} {n}")


# ======================================================================
# Entry point
# ======================================================================

def main():
    load_dotenv(SCRIPT_DIR / ".env")
    print("=== DriveGO Activities ETL ===\n")

    # --- TDX ---------------------------------------------------------
    print("[1/5] Fetch TDX (token + paginated)...")
    token = tdx_get_token()
    tdx_raw = tdx_fetch_all(token)
    print(f"      retrieved: {len(tdx_raw)}\n")

    print("[2/5] Normalize TDX...")
    tdx_norm = [n for n in (tdx_normalize(a) for a in tdx_raw) if n is not None]
    print(f"      kept: {len(tdx_norm)} (dropped {len(tdx_raw) - len(tdx_norm)})\n")

    # --- travel.taipei -----------------------------------------------
    print("[3/5] Fetch travel.taipei (curl_cffi → bypass Cloudflare)...")
    tt_raw = tt_fetch_all()
    print(f"      retrieved: {len(tt_raw)}\n")

    print("[4/5] Normalize travel.taipei...")
    tt_norm = [n for n in (tt_normalize(a) for a in tt_raw) if n is not None]
    print(f"      kept: {len(tt_norm)} (dropped {len(tt_raw) - len(tt_norm)})")

    # If the fetch came back empty (almost always because Cloudflare
    # blocked the data-center IP on a CI runner), preserve the Taipei
    # records that the previous run wrote. Their window filter still
    # applies, so anything past its endDate falls off naturally.
    if not tt_norm and OUT_PATH.exists():
        try:
            previous = json.loads(OUT_PATH.read_text(encoding="utf-8"))
            preserved = [a for a in previous.get("activities", [])
                         if a.get("source") == "travel.taipei"]
            if preserved:
                tt_norm = preserved
                print(f"      (preserved {len(preserved)} Taipei records "
                      f"from previous run)")
        except Exception as e:
            print(f"      (could not read previous JSON: {e})")
    print()

    # --- Merge + window filter + write -------------------------------
    print(f"[5/5] Merge & filter (next {WINDOW_DAYS} days, drop placeholders)...")
    merged = filter_window(tdx_norm + tt_norm)
    merged.sort(key=lambda a: (a["startDate"], a.get("city") or "", a["name"]))
    print(f"      final: {len(merged)}\n")

    payload = {
        "version":     2,
        "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "windowDays":  WINDOW_DAYS,
        "count":       len(merged),
        "sources":     ["TDX Tourism Activity", "travel.taipei Open API"],
        "activities":  merged,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"      wrote {OUT_PATH} ({size_kb:.1f} KB)\n")

    print("=== Summary ===")
    print_summary(merged)
    print("\nDone.")


if __name__ == "__main__":
    main()
