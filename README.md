# DriveGO-config

Public configuration and data files consumed by the DriveGO iOS app.

## Contents

- `data/activities.json` — Taiwan tourism activities (next 180 days), refreshed daily.
- `scripts/` — ETL scripts that produce the JSON files.
- `.github/workflows/` — GitHub Actions schedules that run the ETL.

## Data sources

- [TDX Tourism Activity](https://tdx.transportdata.tw/) — open data from Taiwan's Ministry of Transportation and Communications, provided under the Government Open Data License.

## Notes

This repo contains derived public data only. It does not contain DriveGO source code or any credentials. API secrets used by the ETL live in GitHub Actions Secrets and never appear in the repo.
