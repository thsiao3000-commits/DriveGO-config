#!/usr/bin/env python3
"""
One-off probe: from the current host's IP, can we reach
travel.taipei's internal `/api/...` endpoint? Used to compare
against the known-blocked `/open-api/...` endpoint and decide whether
the internal API needs the same "preserve previous" treatment.
"""

import sys

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    print("curl_cffi not installed", file=sys.stderr)
    sys.exit(1)

PROBES = [
    ("open-api / Events/Activity",
     "https://www.travel.taipei/open-api/zh-tw/Events/Activity?begin=2026-05-19&end=2026-11-15&page=1"),
    ("open-api / Events/Calendar",
     "https://www.travel.taipei/open-api/zh-tw/Events/Calendar?page=1"),
    ("internal /api/zh-tw/event",
     "https://www.travel.taipei/api/zh-tw/event"),
    ("internal /api/zh-tw/activity",
     "https://www.travel.taipei/api/zh-tw/activity?page=1"),
]

for label, url in PROBES:
    try:
        r = curl_requests.get(url, impersonate="chrome120",
                              headers={"Accept": "application/json"}, timeout=15)
        status = r.status_code
        # 嘗試 parse JSON
        try:
            payload = r.json()
            if isinstance(payload, dict):
                count = (payload.get("total")
                         or payload.get("dataCount")
                         or len(payload.get("data") or []))
                summary = f"JSON OK, count={count}"
            else:
                summary = f"got {type(payload).__name__}"
        except Exception:
            summary = f"non-JSON ({len(r.text)}B body, looks like {'Cloudflare' if 'Just a moment' in r.text else '?'})"
        print(f"  {label:35s}  HTTP {status}  → {summary}")
    except Exception as e:
        print(f"  {label:35s}  EXCEPTION  → {e}")
