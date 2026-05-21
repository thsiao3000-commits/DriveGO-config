#!/usr/bin/env python3
"""
DriveGO Activities ETL: fetch from two complementary sources, filter to
a 180-day forward window, clean descriptions, output unified JSON.

Sources
-------
* TDX Tourism Activity API
    - National coverage (12 cities with real data), all event categories
      (festivals, marathons, expos, ...). Taipei has near-zero data.
    - Server-side OAuth. On a fetch failure (expired credentials, API
      outage) the previous TDX records are preserved so the CI run
      stays green; the stale sourcesFreshness.tdx timestamp is the
      signal that the credentials need renewing.
* travel.taipei Open API (Events/Activity)
    - Taipei-only, exhibition-focused. Very high description quality.
    - Requires Chrome TLS impersonation via curl_cffi to bypass the
      Cloudflare bot filter, AND is blocked from data-center IPs even
      after impersonation. Falls back to "preserve previous" on CI.
* travel.taipei Open API (Events/Calendar)
    - Taipei-only, festival/seasonal events INCLUDING future-starting
      ones (跨年, 馬拉松, 藝術節 …). Officially documented in Swagger,
      same DB as the internal /api/zh-tw/event endpoint but stable.
    - Returns the full small set (~33 entries) without paging tricks;
      the begin/end query params filter by posted date which is the
      opposite of what we want, so we don't pass them.
    - Also Cloudflare-blocked from CI runners; same preserve fallback
      as the Activity endpoint.

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
TT_ACTIVITY_URL = "https://www.travel.taipei/open-api/zh-tw/Events/Activity"
TT_CALENDAR_URL = "https://www.travel.taipei/open-api/zh-tw/Events/Calendar"
TT_IMPERSONATE  = "chrome120"
TT_PAGE_SIZE    = 30   # both /Events/* endpoints paginate at 30/page

# ---- Shared ----------------------------------------------------------
WINDOW_DAYS          = 240   # ~8 months — captures full year of Taipei festivals
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
        # Raise (not sys.exit) so the caller's preserve fallback can
        # catch it and keep the run green — see the TDX block in main().
        raise RuntimeError("TDX_CLIENT_ID / TDX_CLIENT_SECRET missing")
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

def tt_activity_fetch_all() -> list[dict]:
    """Paginate travel.taipei /open-api/.../Events/Activity (展演).
    Blocked from data-center IPs — caller falls back to preserve."""
    if not _HAS_CURL_CFFI:
        print("      (curl_cffi unavailable — skipping)")
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
                TT_ACTIVITY_URL,
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


def tt_calendar_fetch_all() -> list[dict]:
    """Paginate travel.taipei /open-api/.../Events/Calendar (節慶年曆).
    Official endpoint. We don't pass begin/end because the API
    semantics filter by `posted` date, not the activity date — passing
    a future window returns nothing useful. Just grab all pages and
    let the post-merge filter_window do its job."""
    if not _HAS_CURL_CFFI:
        print("      (curl_cffi unavailable — skipping)")
        return []
    out, page = [], 1
    while True:
        try:
            r = curl_requests.get(
                TT_CALENDAR_URL,
                params={"page": page},
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
        if not batch:
            break
        out.extend(batch)
        if len(out) >= total or len(batch) < TT_PAGE_SIZE:
            break
        page += 1
    return out


def _tt_parse_date(s: str) -> str | None:
    """travel.taipei date fields. /open-api gives '2026-04-21 00:00:00
    +08:00'; /api/event gives '2026-04-21'. Both safe to slice."""
    if not s or len(s) < 10:
        return None
    return s[:10]


def tt_activity_normalize(raw: dict):
    """Schema for /open-api/.../Events/Activity records."""
    if not raw.get("id") or not raw.get("title"):
        return None
    s_date = _tt_parse_date(raw.get("begin") or "")
    e_date = _tt_parse_date(raw.get("end") or "")
    if not s_date or not e_date:
        return None
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
        "category":    None,
        "detailUrl":   (raw.get("url") or "").strip() or None,
    }


def tt_calendar_normalize(raw: dict):
    """Schema for /open-api/.../Events/Calendar records. Same shape as
    Events/Activity (id, title, begin/end with +08:00, nlat/elong as
    string floats, url as the detail-page link)."""
    if not raw.get("id") or not raw.get("title"):
        return None
    s_date = _tt_parse_date(raw.get("begin") or "")
    e_date = _tt_parse_date(raw.get("end") or "")
    if not s_date or not e_date:
        return None
    try:
        lat = float(raw.get("nlat") or 0)
        lng = float(raw.get("elong") or 0)
    except (TypeError, ValueError):
        return None
    if lat == 0 or lng == 0:
        return None
    return {
        # Keep the tt-event-* prefix and travel.taipei.event source so
        # the preserve-from-previous fallback continues to match
        # records written by the prior /api/event integration.
        "id":          f"tt-event-{raw['id']}",
        "source":      "travel.taipei.event",
        "name":        raw["title"].strip(),
        "city":        "臺北市",
        "startDate":   s_date,
        "endDate":     e_date,
        "lat":         lat,
        "lng":         lng,
        "description": clean_description(raw.get("description")),
        "organizer":   None,   # Calendar doesn't carry organizer
        "phone":       (raw.get("tel") or "").strip() or None,
        "category":    "節慶活動",
        "detailUrl":   (raw.get("url") or "").strip() or None,
    }


# ======================================================================
# Shared helpers
# ======================================================================

def clean_description(raw):
    """Strip HTML, decode entities, drop placeholder filler, enforce
    a minimum length. Returns cleaned string or None.

    Order matters: <style> and <script> blocks must be removed AS A
    WHOLE before the generic tag-stripping pass, otherwise their text
    content (CSS rules / JS code) leaks into the final output. Some
    travel.taipei records embed a full webview <style> block."""
    if not raw:
        return None
    s = raw.strip()
    if s in DESCRIPTION_BLOCKLIST:
        return None
    # 1. Drop <style>…</style> and <script>…</script> entirely.
    s = re.sub(r"<style\b[^>]*>.*?</style\s*>",  "", s, flags=re.IGNORECASE | re.DOTALL)
    s = re.sub(r"<script\b[^>]*>.*?</script\s*>", "", s, flags=re.IGNORECASE | re.DOTALL)
    # 2. <br> → newline so the paragraph structure survives.
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    # 3. Strip everything else that looks like a tag.
    s = re.sub(r"<[^>]+>", "", s)
    # 4. Decode HTML entities.
    s = html.unescape(s)
    # 5. Collapse whitespace.
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


def _previous_payload() -> dict:
    """Read the previously-written activities.json once, cache for
    re-use. Returns {} if missing/malformed."""
    if not OUT_PATH.exists():
        return {}
    try:
        return json.loads(OUT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _preserved_from_previous(source: str) -> list[dict]:
    """Records tagged with `source` from the prior JSON. Used so a CI
    run that can't reach travel.taipei doesn't wipe Taipei data the
    local Mac had populated."""
    return [a for a in _previous_payload().get("activities", [])
            if a.get("source") == source]


def _previous_freshness() -> dict[str, str]:
    """Per-source last-successfully-fetched timestamps from the prior
    JSON. Used to carry timestamps across preserve fallbacks."""
    return _previous_payload().get("sourcesFreshness") or {}


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

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    prev_freshness = _previous_freshness()
    freshness: dict[str, str] = {}

    # --- TDX ---------------------------------------------------------
    # TDX has no public CI block (unlike travel.taipei), but its OAuth
    # credentials can expire and the API can have outages. Treat any
    # failure the same way as the travel.taipei block: preserve the
    # previous TDX slice and keep the run green, rather than wiping the
    # nationwide activities or e-mailing a red run every day. The stale
    # `sourcesFreshness.tdx` timestamp is the durable signal that a fix
    # is needed — see the README "Operations" section.
    print("[1/6] Fetch TDX (token + paginated)...")
    try:
        token = tdx_get_token()
        tdx_raw = tdx_fetch_all(token)
        print(f"      retrieved: {len(tdx_raw)}\n")
    except Exception as e:
        print(f"      ⚠️  TDX fetch FAILED: {e}", file=sys.stderr)
        print( "      → preserving previous TDX records; run stays green\n")
        tdx_raw = []

    print("[2/6] Normalize TDX...")
    if tdx_raw:
        tdx_norm = [n for n in (tdx_normalize(a) for a in tdx_raw) if n is not None]
        freshness["tdx"] = now_iso
        print(f"      kept: {len(tdx_norm)} (dropped {len(tdx_raw) - len(tdx_norm)})\n")
    else:
        tdx_norm = _preserved_from_previous("tdx")
        freshness["tdx"] = prev_freshness.get("tdx", "unknown")
        print(f"      (preserved {len(tdx_norm)} from previous run)\n")

    # --- travel.taipei /open-api/.../Events/Activity (展演) -----------
    print("[3/6] Fetch travel.taipei Events/Activity (curl_cffi)...")
    tt_activity_raw = tt_activity_fetch_all()
    tt_activity_norm = [n for n in (tt_activity_normalize(a)
                                    for a in tt_activity_raw) if n is not None]
    print(f"      retrieved: {len(tt_activity_raw)}  kept: {len(tt_activity_norm)}")
    if tt_activity_raw:
        freshness["travel.taipei"] = now_iso
    else:
        freshness["travel.taipei"] = prev_freshness.get(
            "travel.taipei", "unknown")
        preserved = _preserved_from_previous("travel.taipei")
        if preserved:
            tt_activity_norm = preserved
            print(f"      (preserved {len(preserved)} from previous run)")
    print()

    # --- travel.taipei /open-api/.../Events/Calendar (節慶) -----------
    print("[4/6] Fetch travel.taipei Events/Calendar (curl_cffi)...")
    tt_event_raw = tt_calendar_fetch_all()
    tt_event_norm = [n for n in (tt_calendar_normalize(a)
                                  for a in tt_event_raw) if n is not None]
    print(f"      retrieved: {len(tt_event_raw)}  kept: {len(tt_event_norm)}")
    if tt_event_raw:
        freshness["travel.taipei.event"] = now_iso
    else:
        freshness["travel.taipei.event"] = prev_freshness.get(
            "travel.taipei.event", "unknown")
        preserved = _preserved_from_previous("travel.taipei.event")
        if preserved:
            tt_event_norm = preserved
            print(f"      (preserved {len(preserved)} from previous run)")
    print()

    # --- Merge + window filter + write -------------------------------
    print(f"[5/6] Merge & filter (next {WINDOW_DAYS} days, drop placeholders)...")
    merged = filter_window(tdx_norm + tt_activity_norm + tt_event_norm)
    merged.sort(key=lambda a: (a["startDate"], a.get("city") or "", a["name"]))
    print(f"      final: {len(merged)}\n")

    print(f"[6/6] Write {OUT_PATH}")
    payload = {
        "version":           2,
        "generatedAt":       now_iso,
        "windowDays":        WINDOW_DAYS,
        "count":             len(merged),
        # `source` is the legacy v1 top-level field. The v1.0.5 binary
        # shipped before we made it Optional in ActivitiesPayload, so
        # without this string the bundle JSON refuses to decode and
        # the live App Store version 看不到任何活動. KEEP IT until
        # v1.0.6 has been out long enough that no one runs v1.0.5.
        "source":            "TDX + travel.taipei",
        "sources":           [
            "TDX Tourism Activity",
            "travel.taipei Open API (Events/Activity)",
            "travel.taipei Open API (Events/Calendar)",
        ],
        "sourcesFreshness":  freshness,
        "activities":        merged,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"      wrote {size_kb:.1f} KB\n")

    print("=== Summary ===")
    print_summary(merged)
    print("\nDone.")


if __name__ == "__main__":
    main()
