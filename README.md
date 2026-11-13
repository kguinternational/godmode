# GodMode — Public Haven Platform & Sovereign Grant Compliance Suite

**100% Open-Source North Carolina Spatial Situational Intelligence, Fiscal Audit & OpenBot 1.0 Orchestration Platform**  
Maintained by **KGU International** in partnership with **The JC Coffey Foundation (501(c)(3))**.

---

## Overview

`godmode` is a zero-dependency, self-hosted, 100% open-source Python & Leaflet platform engineered to run natively inside **OpenBot** on local open-weight models (`ollama/gemma4:12b`) with **zero proprietary cloud API dependencies**.

It directly powers two mission-critical public resilience and statutory administration pillars across North Carolina (**Cumberland, Moore, Harnett, Hoke, Buncombe, Yancey, and Mitchell Counties**):

1. **NC GIS & Public Data Stream Integration**:
   - **NC DOT DriveNC Live Traffic & Evacuation Cameras**: Real-time ingestion across **1,004+ statewide cameras** (`drivenc.gov`), parsing WGS84 coordinates, prioritizing designated high-risk counties and evacuation arteries (`I-95`, `I-40`, `I-26`, `US-1`), and caching snapshots in local SQLite for grid-down continuity.
   - **USGS NWIS Real-Time River Sensors**: Live stage height (`00065`, ft) and discharge (`00060`, cfs) monitoring across Cape Fear, Little River (Fort Liberty corridor), Drowning Creek, French Broad, Cane River, and Toe River basins, with automated Action/Flood Stage alerts.
   - **NOAA / NWS Active North Carolina Alerts**: Real-time ingestion from `api.weather.gov/alerts/active?area=NC`.
   - **7-County Emergency Repeater & PACE Radio Directory**: Verified VHF/UHF ARES/SKYWARN repeaters, GMRS emergency bridges, 915 MHz LoRa Meshtastic solar nodes, and NOAA Weather Radio (`162.475`, `162.400`, `162.550 MHz`).

2. **Fiscal Compliance, State Audit Preparation, Registration Maintenance & Quarterly Reporting**:
   - **2 CFR Part 200 & N.C.G.S. § 143C-6-23 SHA-256 Hash-Chained Fiscal Ledger**: Tamper-evident expenditure, disbursement, and non-federal match tracking that strictly separates `[OBSERVED]` verified transactions from `[MODELED]` budget allocations.
   - **Entity Registration & Licensing Tracker**: Monitors IRS 501(c)(3) / Form 990, NC Secretary of State Charitable Solicitation (**Form SL-4001**), **SAM.gov UEI & CAGE Code**, **NC eVP / NCGMS**, and **FEMA GO AOR** renewal windows.
   - **Automated Quarterly Progress Report (QPR) & SF-425 Compiler**: Synthesizes audit-ready quarterly milestone reports via local `ollama/gemma4:12b` with deterministic synthesis fallback.

3. **OpenBot 1.0 Orchestration (100% Open-Source GrokBot 1.0 Parity)**:
   - Supervised 24/7 by **Aria (`ExecutiveChiefOfStaff`)** with self-healing routines, per-bot token/compute usage leaderboard, Team *"Review Learnings"* approval queue, admin-governed shared connectors, Tailscale zero-trust mesh reachability, universal Share Sheet + Scam-Check intake, RFC822 email drafts with attachments, meeting transcript summarization, and permanent Safe Harbor human gates (`payment`, `outbound_message`, `deletion`, `credential_use`).

---

## Quick Start (Zero External Dependencies)

Requires only **Python 3.9+** (standard library) and optionally local **Ollama** (`ollama pull gemma4:12b`):

```bash
git clone https://github.com/kguinternational/godmode.git
cd godmode
python3 -m godmode.server
```

Open **`http://127.0.0.1:8096`** in any browser.

---

## REST API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/health` | Health check, active open-source model, and SHA-256 audit chain status |
| `GET` | `/api/streams/summary` | Latest NC DOT cameras, USGS river gauges, NWS alerts, and 7-county repeaters |
| `POST` | `/api/streams/refresh` | Trigger immediate live ingestion from NC DOT DriveNC, USGS NWIS, and NWS |
| `GET` | `/api/compliance/overview` | 2 CFR 200 / N.C.G.S. § 143C-6-23 ledger, registration gates, and QPR archive |
| `POST` | `/api/compliance/ledger` | Append SHA-256 chained `[OBSERVED]` or `[MODELED]` fiscal entry |
| `POST` | `/api/compliance/registrations/update` | Update EIN, SAM.gov UEI, Form SL-4001, NC eVP, or FEMA GO status |
| `POST` | `/api/compliance/qpr/generate` | Generate audit-ready Quarterly Progress Report (`2026-Q4`, etc.) |
| `GET` | `/api/aria/status` | Aria Chief of Staff oversight status, OpenBot queues, and local Ollama health |
| `GET` | `/api/openbot1/dashboard` | OpenBot 1.0 usage dashboard, Tailscale mesh, Review Learnings queue & drafts |

---

## License

Released under the **MIT License** — 100% free and open-source for North Carolina households, volunteer fire departments, and emergency management partners.
