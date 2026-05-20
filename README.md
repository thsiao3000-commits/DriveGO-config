# DriveGO-config

Public configuration and data files consumed by the DriveGO iOS app.

## Contents

- `data/activities.json` — Taiwan tourism activities for the next 240 days.
- `scripts/fetch_activities.py` — the ETL that produces the JSON.
- `scripts/update-activities.sh` — manual one-shot refresh helper.
- `.github/workflows/update-activities.yml` — daily cron that runs the ETL.
- `.github/workflows/probe.yml` — ad-hoc reachability probe (debugging only).

## Data sources

| Source | Coverage | Access |
|---|---|---|
| [TDX Tourism Activity](https://tdx.transportdata.tw/) | 12 cities nationwide, all event types. Near-zero Taipei data. | OAuth client credentials, server-side only |
| travel.taipei `/open-api/{lang}/Events/Activity` | Taipei exhibitions (展演) | Public, Cloudflare-gated |
| travel.taipei `/open-api/{lang}/Events/Calendar` | Taipei festivals (節慶) — incl. future-dated | Public, Cloudflare-gated |

Both are open government data. TDX is under the Government Open Data
License; travel.taipei's Open Data Notice asks integrators to cite the
source, which the app does in Settings → Data Sources and in the
App Store listing.

---

## travel.taipei integration — technical notes

The hard part of this project was not the data; it was getting *to*
the data. travel.taipei sits behind Cloudflare, and Cloudflare decides
who is allowed through. Everything below is the result of trial and
error — keep it in mind before changing the pipeline.

### The core obstacle: Cloudflare bot management

travel.taipei is an **open** API (no API key) but **not unprotected**.
Cloudflare in front of it does DDoS mitigation, per-IP rate limiting,
edge caching, and — the part that bit us — **bot detection**.

Bot detection is driven mostly by the **TLS fingerprint (JA3/JA4)**.
The TLS `ClientHello` a client sends — its cipher list, extensions,
and their ordering — is determined by the TLS library, not by any
header you can set. Cloudflare maps fingerprints to known clients.

Observed behaviour:

| Caller | TLS fingerprint | IP type | Result |
|---|---|---|---|
| `curl` | curl/OpenSSL | any | 403 — "CLI tool" |
| `curl_cffi` (impersonate=chrome120) | Chrome | residential | 200 |
| `curl_cffi` (impersonate=chrome120) | data-center (GitHub Actions) | — | 403 — IP reputation |
| iOS `URLSession` | Apple Network framework | residential / cellular | 200 |

Two conclusions:

1. **A genuine end-user device passes cleanly.** iOS `URLSession`
   produces an Apple TLS fingerprint from a consumer ISP / cellular
   IP — exactly the "real user" profile Cloudflare admits. This is
   the *intended* way to consume the Open API.
2. **A server cannot.** Even with Chrome TLS impersonation, a
   data-center IP is rejected. There is no free way around this; it
   is by design.

### Two access paths, and why both exist

```
┌─ TDX ────────────────────────────────────────────────┐
│  Needs OAuth client_id / client_secret → server-side  │
│  only. Runs in the GitHub Actions cron.               │
└───────────────────────────────────────────────────────┘

┌─ travel.taipei, path A: the ETL (this repo) ──────────┐
│  scripts/fetch_activities.py uses curl_cffi to        │
│  impersonate Chrome. Works from a developer's Mac     │
│  (residential IP); BLOCKED from GitHub Actions.       │
│  → On CI the fetch fails and the previous Taipei      │
│    records are preserved (see "preserve fallback").   │
└───────────────────────────────────────────────────────┘

┌─ travel.taipei, path B: the app (DriveGO repo) ───────┐
│  TaipeiLiveService.swift calls the Open API directly  │
│  with URLSession on every launch. Each user's device  │
│  is its own residential/cellular IP → passes.         │
│  This is the PRIMARY source for Taipei at runtime.    │
└───────────────────────────────────────────────────────┘
```

At runtime the app merges them: TDX (+ everything non-Taipei) comes
from this repo's JSON; Taipei is replaced by the live `URLSession`
fetch whenever it succeeds, and falls back to the JSON's Taipei
records (then the bundled snapshot) when it doesn't.

### The "preserve fallback"

Because the ETL's travel.taipei fetch fails on CI, a naive cron run
would overwrite `activities.json` with **zero** Taipei records every
night. To prevent that, `fetch_activities.py`:

1. Reads the previous `activities.json` before writing.
2. If a travel.taipei source returns nothing, it re-uses ("preserves")
   that source's records from the previous file.
3. The window filter still applies, so genuinely expired records drop
   off naturally.

Net effect: the daily CI cron keeps TDX fresh and leaves the Taipei
slice untouched. The Taipei slice is only *actually* refreshed when a
developer runs `update-activities.sh` locally (residential IP).

### Why the app still needs this repo's Taipei data

Path B (live `URLSession`) is primary, but the JSON Taipei records
remain the fallback for:

- First launch before the live fetch completes.
- No network / travel.taipei or Cloudflare outage.
- Networks where Cloudflare is stricter (some overseas/VPN IPs).

So `update-activities.sh` is still worth running roughly monthly, and
always before cutting an App Store build, to keep that fallback — and
the bundled snapshot in the app — reasonably current.

### Endpoint quirks

- **`Events/Activity`** — `begin`/`end` query params filter by the
  activity date; we pass a today→+240d window.
- **`Events/Calendar`** — `begin`/`end` filter by the **posted** date
  (when the record was published), *not* the event date. Passing a
  future window returns nothing. We omit them and filter client-side.
- Both paginate at 30 records/page.
- Coordinates arrive as **strings** (`"25.1024"`).
- Field names contain upstream typos kept as-is: `distric`,
  `co_rganizer`.
- Descriptions are HTML and sometimes embed a whole `<style>` block —
  strip `<style>`/`<script>` *with their contents* before the generic
  tag pass, then decode entities.

### Schema / payload

`activities.json` top level:

```jsonc
{
  "version": 2,
  "generatedAt": "ISO-8601",
  "windowDays": 240,
  "count": 339,
  "source": "TDX + travel.taipei",          // legacy string — keep it
  "sources": ["...", "...", "..."],           // human-readable list
  "sourcesFreshness": { "tdx": "ISO", ... },  // last successful fetch
  "activities": [ /* Activity records */ ]
}
```

> **Do not remove the top-level `source` string.** The v1.0.5 build on
> the App Store predates the Optional-`source` fix and fails to decode
> the payload without it. Keep it until v1.0.5 is no longer in use.

Each activity record carries `source` = `"tdx"` / `"travel.taipei"` /
`"travel.taipei.event"`, plus optional `detailUrl`.

## Notes

This repo contains derived public data only. It does not contain
DriveGO source code or any credentials. API secrets used by the ETL
live in GitHub Actions Secrets and never appear in the repo.
