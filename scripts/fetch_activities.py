#!/usr/bin/env python3
"""
DriveGO Activities ETL: Fetch TDX Tourism Activity, filter to a 180-day
forward window, clean descriptions, output unified JSON for the iOS app.

Source : TDX Tourism Activity API
Output : ../data/activities.json (relative to this script's directory)

Usage:
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

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_PATH = SCRIPT_DIR.parent / "data" / "activities.json"

TOKEN_URL = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
API_URL   = "https://tdx.transportdata.tw/api/basic/v2/Tourism/Activity"

PAGE_SIZE            = 500
WINDOW_DAYS          = 180
PLACEHOLDER_END_YEAR = 2030

TAIPEI_TZ = timezone(timedelta(hours=8))

DESCRIPTION_BLOCKLIST = {
    "", "-", "—", "N/A", "n/a", "無", "無。", "(空)",
    "詳見官網", "詳見活動官網", "請見活動官網", "詳見主辦單位官網",
    "to see the official site",
}
MIN_DESCRIPTION_LENGTH = 20


def get_token() -> str:
    cid  = os.environ.get("TDX_CLIENT_ID")
    csec = os.environ.get("TDX_CLIENT_SECRET")
    if not cid or not csec:
        print("ERROR: TDX_CLIENT_ID / TDX_CLIENT_SECRET missing in env", file=sys.stderr)
        sys.exit(1)
    resp = requests.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     cid,
        "client_secret": csec,
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_all(token: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    out, skip = [], 0
    while True:
        resp = requests.get(
            API_URL,
            headers=headers,
            params={"$top": PAGE_SIZE, "$skip": skip, "$format": "JSON"},
            timeout=60,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        out.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return out


def clean_description(raw):
    if not raw:
        return None
    s = raw.strip()
    if s in DESCRIPTION_BLOCKLIST:
        return None
    # Strip HTML before unescaping, so any entities embedded in tag
    # attributes don't leak. Decode <br> to newlines first so the
    # paragraph structure survives the tag stripping.
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    # html.unescape handles every named entity (&bull; &middot; &hellip;
    # &ldquo; &rdquo; etc.) plus numeric forms (&#8226; &#x2022;).
    s = html.unescape(s)
    s = re.sub(r"[ \t]+", " ", s).strip()
    s = re.sub(r"\n{3,}", "\n\n", s)
    if len(s) < MIN_DESCRIPTION_LENGTH:
        return None
    return s


def normalize(raw: dict):
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
        "id":          raw["ActivityID"],
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
    }


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
    by_city: dict[str, int] = {}
    for a in items:
        c = a.get("city") or "(未標)"
        by_city[c] = by_city.get(c, 0) + 1
    print(f"  total:            {total}")
    print(f"  with description: {with_desc} ({with_desc*100//total}%)")
    print(f"  by city:")
    for c, n in sorted(by_city.items(), key=lambda x: -x[1]):
        print(f"    {c:<8} {n}")


def main():
    load_dotenv(SCRIPT_DIR / ".env")
    print("=== DriveGO TDX Activities ETL ===\n")

    print("[1/4] Get TDX token...")
    token = get_token()
    print("      OK\n")

    print("[2/4] Fetch all Activity records...")
    raw = fetch_all(token)
    print(f"      retrieved: {len(raw)}\n")

    print(f"[3/4] Normalize & filter (next {WINDOW_DAYS} days, drop placeholders)...")
    normalized = [n for n in (normalize(a) for a in raw) if n is not None]
    dropped    = len(raw) - len(normalized)
    filtered   = filter_window(normalized)
    filtered.sort(key=lambda a: (a["startDate"], a.get("city") or ""))
    print(f"      normalized: {len(normalized)}  (dropped {dropped} for missing fields)")
    print(f"      after window filter: {len(filtered)}\n")

    print(f"[4/4] Write {OUT_PATH}")
    payload = {
        "version":      1,
        "generatedAt":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "windowDays":   WINDOW_DAYS,
        "count":        len(filtered),
        "source":       "TDX Tourism Activity",
        "activities":   filtered,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"      wrote {size_kb:.1f} KB\n")

    print("=== Summary ===")
    print_summary(filtered)
    print("\nDone.")


if __name__ == "__main__":
    main()
