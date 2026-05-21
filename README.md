# DriveGO-config

Public configuration and data files consumed by the DriveGO iOS app.

## Contents

- `data/activities.json` — Taiwan tourism activities for the next 240 days.
- `scripts/fetch_activities.py` — the ETL that produces the JSON.
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

### Refreshing before a release

`update-activities.sh` is **not** in this repo — it lives in the DriveGO
app repo at `DriveGO/Scripts/update-activities.sh` (it needs to write to
both repos). It has two modes:

```bash
./Scripts/update-activities.sh           # refresh the published JSON only
                                         # — routine, run roughly monthly
./Scripts/update-activities.sh --bundle  # ALSO refresh the app's bundled
                                         # snapshot — REQUIRED before every
                                         # App Store release
```

Both modes run the ETL locally (a developer's residential IP gets past
Cloudflare), `git pull` this repo, then commit and push the new
`data/activities.json`. The `--bundle` flag additionally overwrites
`DriveGO/Resources/activities.json` — the snapshot compiled into the app
binary.

**Always run the `--bundle` form before cutting a new App Store build.**
The bundled snapshot is the runtime fallback of last resort; if it is
stale, a fresh install shows out-of-date Taipei activities until its first
live `URLSession` fetch lands (and shows *only* stale data while offline).
The daily CI cron never refreshes the Taipei slice — Cloudflare blocks the
data-center IP — so the *only* thing that moves Taipei data, in both the
published JSON and the bundled snapshot, is a developer running this
script from a residential IP. After it finishes, commit the refreshed
`DriveGO/Resources/activities.json` in the DriveGO repo as part of the
release; the script does not commit the snapshot for you.

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

---

# 中文說明

DriveGO iOS App 使用的公開設定與資料檔。

## 內容

- `data/activities.json` — 未來 240 天的台灣觀光活動。
- `scripts/fetch_activities.py` — 產生 JSON 的 ETL 腳本。
- `.github/workflows/update-activities.yml` — 每日跑 ETL 的排程。
- `.github/workflows/probe.yml` — 臨時連線探針（僅除錯用）。

## 資料來源

| 來源 | 涵蓋範圍 | 存取方式 |
|---|---|---|
| [TDX 觀光活動](https://tdx.transportdata.tw/) | 全台 12 縣市、所有活動類型。臺北市近乎 0 筆。 | OAuth client credentials，僅伺服器端 |
| travel.taipei `/open-api/{lang}/Events/Activity` | 臺北市展演 | 公開、Cloudflare 防護 |
| travel.taipei `/open-api/{lang}/Events/Calendar` | 臺北市節慶（含未來日期） | 公開、Cloudflare 防護 |

兩者皆為政府開放資料。TDX 採用「政府資料開放授權條款」；臺北旅遊網的「資料開放宣告」要求利用時註明出處，App 已在「設定 → 資料來源」與 App Store 說明中標示。

---

## travel.taipei 整合 — 技術筆記

這個專案的難點不是資料本身，而是「**怎麼拿到**」資料。travel.taipei 在 Cloudflare 後面，由 Cloudflare 決定誰能通過。以下全是試錯換來的經驗 —— 改動 pipeline 前請先讀。

### 核心障礙：Cloudflare bot 管理

travel.taipei 是**開放** API（不需 API key），但**並非不設防**。前面的 Cloudflare 做 DDoS 緩解、per-IP 速率限制、邊緣快取，還有讓我們踩雷的 —— **bot 偵測**。

bot 偵測主要靠 **TLS 指紋（JA3/JA4）**。client 送出的 TLS `ClientHello` —— 它的加密套件清單、擴充功能、以及排列順序 —— 由 TLS 函式庫決定，**不是任何 header 設得了的**。Cloudflare 把指紋對應到已知的 client。

實測行為：

| 呼叫端 | TLS 指紋 | IP 類型 | 結果 |
|---|---|---|---|
| `curl` | curl/OpenSSL | 任何 | 403 — 「CLI 工具」 |
| `curl_cffi`（impersonate=chrome120） | Chrome | 住宅 | 200 |
| `curl_cffi`（impersonate=chrome120） | 機房（GitHub Actions） | — | 403 — IP 信譽 |
| iOS `URLSession` | Apple Network framework | 住宅 / 電信 | 200 |

兩個結論：

1. **真實使用者裝置可乾淨通過。** iOS `URLSession` 從消費級 ISP / 電信 IP 發出 Apple 的 TLS 指紋 —— 正是 Cloudflare 放行的「真實使用者」輪廓。這是 Open API 的*預期*使用方式。
2. **伺服器不行。** 即使用 Chrome TLS 偽裝，機房 IP 仍被拒。沒有免費的繞法，這是刻意設計。

### 兩條存取路徑，以及為何兩條都要

```
┌─ TDX ────────────────────────────────────────────────┐
│  需要 OAuth client_id / client_secret → 僅伺服器端。   │
│  在 GitHub Actions cron 內執行。                       │
└───────────────────────────────────────────────────────┘

┌─ travel.taipei，路徑 A：ETL（此 repo）────────────────┐
│  scripts/fetch_activities.py 用 curl_cffi 偽裝 Chrome。│
│  從開發者 Mac（住宅 IP）可行；從 GitHub Actions 被擋。  │
│  → CI 上 fetch 失敗時，沿用前一次的 Taipei 紀錄         │
│    （見「preserve fallback」）。                       │
└───────────────────────────────────────────────────────┘

┌─ travel.taipei，路徑 B：App（DriveGO repo）───────────┐
│  TaipeiLiveService.swift 每次 App 啟動時用 URLSession  │
│  直接打 Open API。每位使用者裝置都是自己的住宅/電信     │
│  IP → 通過。這是執行時臺北市的「主要」資料來源。        │
└───────────────────────────────────────────────────────┘
```

執行時 App 把兩者合併：TDX（及所有非臺北資料）來自此 repo 的 JSON；臺北市則在 live `URLSession` fetch 成功時用它取代，失敗時 fallback 到 JSON 內的臺北紀錄（再不行則用內建快照）。

### 「preserve fallback」

因為 ETL 的 travel.taipei fetch 在 CI 上會失敗，天真的 cron 每晚會把 `activities.json` 的台北紀錄洗成 **0 筆**。為了避免這件事，`fetch_activities.py`：

1. 寫入前先讀取前一份 `activities.json`。
2. 若某個 travel.taipei 來源回傳空的，就「保留（preserve）」前一份檔案裡該來源的紀錄。
3. 時間窗口過濾仍會套用，所以真正過期的紀錄會自然消失。

淨效果：每日 CI cron 保持 TDX 新鮮、台北那段原封不動。台北那段只有在開發者本機跑 `update-activities.sh`（住宅 IP）時才**真正**更新。

### 為何 App 仍需要此 repo 的台北資料

路徑 B（live `URLSession`）是主要來源，但 JSON 內的台北紀錄仍是以下情況的 fallback：

- live fetch 完成前的首次啟動。
- 沒網路 / travel.taipei 或 Cloudflare 中斷。
- Cloudflare 較嚴格的網路（部分海外 / VPN IP）。

所以 `update-activities.sh` 仍值得大約每月跑一次，且每次要做 App Store build 之前一定要跑，讓 fallback —— 以及 App 內建的快照 —— 維持夠新。

### 發版前更新

`update-activities.sh` **不在**此 repo —— 它放在 DriveGO App repo 的
`DriveGO/Scripts/update-activities.sh`（因為它需要同時寫入兩個 repo）。它有兩種模式：

```bash
./Scripts/update-activities.sh           # 只更新已發佈的 JSON
                                         # —— 例行作業，大約每月跑一次
./Scripts/update-activities.sh --bundle  # 連同 App 內建快照一起更新
                                         # —— 每次 App Store 發版前必跑
```

兩種模式都會在本機跑 ETL（開發者的住宅 IP 能通過 Cloudflare）、`git pull` 此 repo，
再 commit 並 push 新的 `data/activities.json`。`--bundle` 旗標還會額外覆寫
`DriveGO/Resources/activities.json` —— 也就是編譯進 App 二進位檔的快照。

**每次要做新的 App Store build 之前，一定要跑 `--bundle` 形式。**
內建快照是執行時最後一道 fallback；如果它過舊，全新安裝的 App 在第一次 live
`URLSession` 抓取成功前都會顯示過時的臺北活動（離線時更是只剩這份舊資料）。每日的
CI cron 永遠不會更新臺北那段 —— Cloudflare 擋掉機房 IP —— 所以**唯一**能讓臺北資料
（不論是已發佈的 JSON 還是內建快照）更新的，就是開發者用住宅 IP 跑這個腳本。腳本跑完
後，請在 DriveGO repo 把更新後的 `DriveGO/Resources/activities.json` commit 起來
作為發版的一部分；腳本不會幫你 commit 快照。

### Endpoint 陷阱

- **`Events/Activity`** — `begin`/`end` query 參數依活動日期過濾；我們送「今天→+240 天」的窗口。
- **`Events/Calendar`** — `begin`/`end` 依 **posted（上架）日期**過濾，**不是**活動日期。送未來窗口會回空。我們不送這兩個參數，改由 client 端過濾。
- 兩者每頁 30 筆。
- 座標以**字串**形式回傳（`"25.1024"`）。
- 欄位含上游的拼字錯誤，照原樣保留：`distric`、`co_rganizer`。
- 描述是 HTML，有時嵌入整段 `<style>` —— 在通用 tag 移除前要先把 `<style>`/`<script>` **連同內容**整段移除，再解 entity。

### Schema / payload

`activities.json` 頂層：

```jsonc
{
  "version": 2,
  "generatedAt": "ISO-8601",
  "windowDays": 240,
  "count": 339,
  "source": "TDX + travel.taipei",          // legacy 字串 — 保留
  "sources": ["...", "...", "..."],           // 人類可讀清單
  "sourcesFreshness": { "tdx": "ISO", ... },  // 上次成功 fetch 時間
  "activities": [ /* Activity 紀錄 */ ]
}
```

> **不要移除頂層的 `source` 字串。** App Store 上的 v1.0.5 build 早於 Optional-`source` 修正，缺這個字串就無法 decode payload。在 v1.0.5 還有人用之前都要保留。

每筆 activity 紀錄帶 `source` = `"tdx"` / `"travel.taipei"` / `"travel.taipei.event"`，外加選填的 `detailUrl`。

## 備註

此 repo 只含衍生的公開資料，不含 DriveGO 原始碼或任何憑證。ETL 用的 API secrets 存在 GitHub Actions Secrets，永不出現在 repo 內。
