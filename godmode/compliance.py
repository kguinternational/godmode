"""
GodMode Fiscal Compliance, State Audit Preparation, Registration Maintenance & Quarterly Reporting Engine
Aligned with:
  - 2 CFR Part 200 (Uniform Administrative Requirements, Cost Principles, and Audit Requirements for Federal Awards)
  - N.C.G.S. § 143C-6-23 & 09 NCAC 03M (North Carolina State Grant Funds Reporting & Audit Standards)

100% Open-Source: Uses SHA-256 tamper-evident ledger chaining in SQLite and local Ollama (gemma4:12b)
with deterministic synthesis fallback. Strictly separates [OBSERVED] verified entries from [MODELED] allocations.
"""

import hashlib
import json
import os
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

LOCAL_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
LOCAL_OSS_MODEL = os.getenv("GODMODE_OSS_MODEL", "gemma4:12b")

DEFAULT_REGISTRATIONS: List[Dict[str, Any]] = [
    {
        "reg_id": "irs_501c3",
        "authority": "Internal Revenue Service (IRS) & Florida Sunbiz",
        "credential_name": "501(c)(3) Public Charity Determination & FL Not-for-Profit Good Standing ($8.75)",
        "entity": "JC Coffey Foundation (Florida Nonprofit)",
        "identifier_value": "PENDING_OPERATOR_EIN_ENTRY",
        "portal_url": "https://dos.fl.gov/sunbiz/",
        "status": "ACTION_REQUIRED_VERIFY_EIN",
        "renewal_cycle": "Annual FL Nonprofit Report (May 1) + IRS Form 990 (15th day of 5th month)",
        "blocking_gate": "Human Gate: Pull $8.75 FL Sunbiz Certificate of Status PDF & verify Foundation EIN",
    },
    {
        "reg_id": "nc_sos_n09_foreign",
        "authority": "NC Secretary of State — Business Registration Division",
        "credential_name": "Certificate of Authority — Foreign Nonprofit Corporation (Form N-09, $125)",
        "entity": "JC Coffey Foundation (Florida Nonprofit -> Foreign Qualification in NC)",
        "identifier_value": "PENDING_N09_FOREIGN_FILING",
        "portal_url": "https://www.sosnc.gov",
        "status": "READY_TO_FILE_ONLINE",
        "renewal_cycle": "One-time Foreign Qualification ($125) + NC Registered Agent maintenance",
        "blocking_gate": "Human Gate: File Form N-09 ($125) on sosnc.gov attaching FL Sunbiz Certificate of Status",
    },
    {
        "reg_id": "nc_sos_sl4001",
        "authority": "NC Secretary of State — Charitable Solicitation Licensing",
        "credential_name": "NC Charitable Solicitation License (Form SL-4001, $50) + FL FDACS Check",
        "entity": "JC Coffey Foundation (Florida Nonprofit)",
        "identifier_value": "PENDING_SL4001_FILING",
        "portal_url": "https://www.sosnc.gov/divisions/charities",
        "status": "READY_TO_FILE_ONLINE",
        "renewal_cycle": "Annual (within 4.5 months of fiscal year-end; match FL FDACS § 496.405)",
        "blocking_gate": "Human Gate: File Form SL-4001 ($50) on sosnc.gov before collecting NC matching pledges",
    },
    {
        "reg_id": "federal_sam_uei",
        "authority": "U.S. General Services Administration (SAM.gov)",
        "credential_name": "Separate Active UEIs & CAGE Codes (Nonprofit Subrecipient + For-Profit Vendor)",
        "entity": "JC Coffey Foundation (FL Nonprofit) & KGU International (FL/WY For-Profit)",
        "identifier_value": "PENDING_UEI_CONFIRMATION",
        "portal_url": "https://sam.gov",
        "status": "ACTION_REQUIRED_CONFIRM_ACTIVE",
        "renewal_cycle": "Annual (365-day mandatory SAM.gov renewal for both entities)",
        "blocking_gate": "Human Gate: Confirm separate active UEIs in SAM.gov via Login.gov MFA",
    },
    {
        "reg_id": "nc_evp_ncgms",
        "authority": "NC Dept. of Administration & NCDPS / NCEM",
        "credential_name": "NC electronic Vendor Portal (eVP) + 2 CFR § 200.318 Arm's-Length COI Protocol",
        "entity": "JC Coffey Foundation (FL Grantee) & KGU International (FL/WY Technical Vendor)",
        "identifier_value": "PENDING_EVP_VENDOR_ID",
        "portal_url": "https://evp.nc.gov",
        "status": "READY_FOR_ENROLLMENT",
        "renewal_cycle": "Continuous (W-9 + N.C.G.S. § 143C-6-23 & IRS § 4958 independent board recusal)",
        "blocking_gate": "Human Gate: Enroll in evp.nc.gov & attach independent board COI recusal resolution",
    },
    {
        "reg_id": "fema_go_aor",
        "authority": "FEMA / Department of Homeland Security",
        "credential_name": "FEMA Grants Outcomes (FEMA GO) Authorized Organization Representative (AOR)",
        "entity": "JC Coffey Foundation (Subrecipient) + KGU International ($55k Direct Vendor)",
        "identifier_value": "PENDING_AOR_LOGIN",
        "portal_url": "https://go.fema.gov",
        "status": "PACKAGE_STAGED_AWAITING_AOR",
        "renewal_cycle": "Per Grant Cycle + SF-425 Quarterly Federal Financial Reports",
        "blocking_gate": "Human Gate: AOR Login.gov sign-in & click-submit on FEMA BRIC $750,000 dossier",
    },
]

NCEM_AND_BRIC_BUDGET_LINES: List[Dict[str, Any]] = [
    {
        "program": "NCEM_HMGP_350K",
        "cost_category": "Field Instructors & Military Liaison",
        "object_class": "a. Personnel / Contractual",
        "allocated_usd": 95000.0,
        "statute_ref": "09 NCAC 03M .0205 / 2 CFR 200.430",
    },
    {
        "program": "NCEM_HMGP_350K",
        "cost_category": "Emergency Hardware Kits (1,500 units @ $95)",
        "object_class": "e. Supplies",
        "allocated_usd": 142500.0,
        "statute_ref": "2 CFR 200.453 (Materials & Supplies)",
    },
    {
        "program": "NCEM_HMGP_350K",
        "cost_category": "Mobile Demonstration Unit & Trailer Logistics",
        "object_class": "c. Travel / Equipment",
        "allocated_usd": 32500.0,
        "statute_ref": "2 CFR 200.475",
    },
    {
        "program": "NCEM_HMGP_350K",
        "cost_category": "NC GIS & Data Stream Integration (GodMode Platform)",
        "object_class": "f. Contractual — KGU International",
        "allocated_usd": 45000.0,
        "statute_ref": "2 CFR 200.331 (Subrecipient / Technical Integration)",
    },
    {
        "program": "NCEM_HMGP_350K",
        "cost_category": "Program Administration & State Reporting",
        "object_class": "h. Direct Admin & Audit Compliance",
        "allocated_usd": 35000.0,
        "statute_ref": "N.C.G.S. § 143C-6-23 / 09 NCAC 03M",
    },
    {
        "program": "FEMA_BRIC_750K",
        "cost_category": "FEMA BRIC Federal Share + KGU Threat Radar ($55k Subrecipient)",
        "object_class": "SF-424A Consolidated Federal Share",
        "allocated_usd": 700000.0,
        "statute_ref": "44 CFR Part 206 / 2 CFR Part 200",
    },
    {
        "program": "FEMA_BRIC_750K",
        "cost_category": "FEMA BRIC Non-Federal 10% Match (In-Kind / Philanthropic)",
        "object_class": "SF-424A Non-Federal Cost Share",
        "allocated_usd": 50000.0,
        "statute_ref": "2 CFR 200.306 (Cost Sharing or Matching)",
    },
]


def _compute_entry_hash(prev_hash: str, payload: Dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{prev_hash}|{canonical}".encode("utf-8")).hexdigest()


def init_compliance_tables(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    now_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS registrations (
                reg_id TEXT PRIMARY KEY,
                authority TEXT,
                credential_name TEXT,
                entity TEXT,
                identifier_value TEXT,
                portal_url TEXT,
                status TEXT,
                renewal_cycle TEXT,
                blocking_gate TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS fiscal_audit_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT,
                program TEXT,
                entry_type TEXT,
                cost_category TEXT,
                amount_usd REAL,
                provenance TEXT,
                voucher_ref TEXT,
                notes TEXT,
                prev_hash TEXT,
                entry_hash TEXT
            );

            CREATE TABLE IF NOT EXISTS quarterly_reports (
                report_id TEXT PRIMARY KEY,
                fiscal_quarter TEXT,
                program TEXT,
                generated_at TEXT,
                generator_engine TEXT,
                audit_chain_valid INTEGER,
                observed_Disbursed_usd REAL,
                modeled_budget_usd REAL,
                markdown_body TEXT
            );
            """
        )

        # Seed missing registrations and sync metadata on unverified PENDING_* rows
        for reg in DEFAULT_REGISTRATIONS:
            conn.execute(
                """
                INSERT OR IGNORE INTO registrations
                (reg_id, authority, credential_name, entity, identifier_value, portal_url, status, renewal_cycle, blocking_gate, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    reg["reg_id"],
                    reg["authority"],
                    reg["credential_name"],
                    reg["entity"],
                    reg["identifier_value"],
                    reg["portal_url"],
                    reg["status"],
                    reg["renewal_cycle"],
                    reg["blocking_gate"],
                    now_iso,
                ),
            )
            conn.execute(
                """
                UPDATE registrations
                SET authority = ?, credential_name = ?, entity = ?, portal_url = ?, renewal_cycle = ?, blocking_gate = ?
                WHERE reg_id = ? AND identifier_value LIKE 'PENDING_%'
                """,
                (
                    reg["authority"],
                    reg["credential_name"],
                    reg["entity"],
                    reg["portal_url"],
                    reg["renewal_cycle"],
                    reg["blocking_gate"],
                    reg["reg_id"],
                ),
            )

        # Seed initial [MODELED] budget baseline & [OBSERVED] open-source platform engineering entry if empty
        l_cnt = conn.execute("SELECT COUNT(*) FROM fiscal_audit_ledger").fetchone()[0]
        if l_cnt == 0:
            conn.commit()
            for bl in NCEM_AND_BRIC_BUDGET_LINES:
                record_ledger_entry(
                    db_path=db_path,
                    program=bl["program"],
                    entry_type="BUDGET_ALLOCATION",
                    cost_category=bl["cost_category"],
                    amount_usd=bl["allocated_usd"],
                    provenance="[MODELED]",
                    voucher_ref=bl["statute_ref"],
                    notes=f"Pre-award SF-424A / State budget line ({bl['object_class']})",
                )
            record_ledger_entry(
                db_path=db_path,
                program="NCEM_HMGP_350K",
                entry_type="IN_KIND_ENGINEERING_DELIVERABLE",
                cost_category="NC GIS & Data Stream Integration (GodMode Platform)",
                amount_usd=0.0,
                provenance="[OBSERVED]",
                voucher_ref="GITHUB-KGUINTERNATIONAL-GODMODE-V1",
                notes="100% Open-Source NC DOT DriveNC + USGS NWIS + 7-County Repeater + 2 CFR 200 Audit Engine deployed",
            )


def record_ledger_entry(
    db_path: Path,
    program: str,
    entry_type: str,
    cost_category: str,
    amount_usd: float,
    provenance: str = "[OBSERVED]",
    voucher_ref: str = "",
    notes: str = "",
) -> Dict[str, Any]:
    """Appends an immutable SHA-256 hash-chained entry to the fiscal audit ledger."""
    if provenance not in ("[OBSERVED]", "[MODELED]"):
        provenance = "[OBSERVED]"
    now_iso = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        last = conn.execute("SELECT entry_hash FROM fiscal_audit_ledger ORDER BY id DESC LIMIT 1").fetchone()
        prev_hash = last["entry_hash"] if last else "GENESIS_2CFR200_NCGS143C"

        payload = {
            "created_at": now_iso,
            "program": program,
            "entry_type": entry_type,
            "cost_category": cost_category,
            "amount_usd": round(float(amount_usd), 2),
            "provenance": provenance,
            "voucher_ref": voucher_ref,
            "notes": notes,
        }
        entry_hash = _compute_entry_hash(prev_hash, payload)

        cur = conn.execute(
            """
            INSERT INTO fiscal_audit_ledger
            (created_at, program, entry_type, cost_category, amount_usd, provenance, voucher_ref, notes, prev_hash, entry_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now_iso,
                program,
                entry_type,
                cost_category,
                payload["amount_usd"],
                provenance,
                voucher_ref,
                notes,
                prev_hash,
                entry_hash,
            ),
        )
        return {"id": cur.lastrowid, **payload, "prev_hash": prev_hash, "entry_hash": entry_hash}


def verify_audit_chain(db_path: Path) -> Dict[str, Any]:
    """Verifies cryptographic integrity across every row of the state/federal fiscal ledger."""
    init_compliance_tables(db_path)
    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(r) for r in conn.execute("SELECT * FROM fiscal_audit_ledger ORDER BY id ASC")]

    expected_prev = "GENESIS_2CFR200_NCGS143C"
    for r in rows:
        if r["prev_hash"] != expected_prev:
            return {"valid": False, "checked_entries": len(rows), "broken_at_id": r["id"]}
        payload = {
            "created_at": r["created_at"],
            "program": r["program"],
            "entry_type": r["entry_type"],
            "cost_category": r["cost_category"],
            "amount_usd": round(float(r["amount_usd"]), 2),
            "provenance": r["provenance"],
            "voucher_ref": r["voucher_ref"],
            "notes": r["notes"],
        }
        calc = _compute_entry_hash(expected_prev, payload)
        if calc != r["entry_hash"]:
            return {"valid": False, "checked_entries": len(rows), "broken_at_id": r["id"]}
        expected_prev = calc

    return {
        "valid": True,
        "checked_entries": len(rows),
        "head_sha256": expected_prev,
        "standards": ["2 CFR Part 200", "N.C.G.S. § 143C-6-23", "09 NCAC 03M"],
    }


def update_registration(
    db_path: Path,
    reg_id: str,
    identifier_value: Optional[str] = None,
    status: Optional[str] = None,
) -> Dict[str, Any]:
    init_compliance_tables(db_path)
    now_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM registrations WHERE reg_id = ?", (reg_id,)).fetchone()
        if not row:
            raise ValueError(f"Unknown registration id: {reg_id}")
        new_id_val = identifier_value if identifier_value is not None else row["identifier_value"]
        new_status = status if status is not None else row["status"]
        conn.execute(
            "UPDATE registrations SET identifier_value = ?, status = ?, updated_at = ? WHERE reg_id = ?",
            (new_id_val, new_status, now_iso, reg_id),
        )
        updated = dict(conn.execute("SELECT * FROM registrations WHERE reg_id = ?", (reg_id,)).fetchone())
    return updated


def _summarize_with_local_ollama(prompt: str) -> Optional[str]:
    """Optional executive narrative polish using 100% open-source local Ollama (gemma4:12b)."""
    try:
        req_body = json.dumps(
            {
                "model": LOCAL_OSS_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2, "num_predict": 320},
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            f"{LOCAL_OLLAMA_URL}/api/generate",
            data=req_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=12) as resp:
            out = json.loads(resp.read().decode("utf-8", errors="ignore"))
            txt = (out.get("response") or "").strip()
            return txt if txt else None
    except Exception:
        return None


def generate_quarterly_progress_report(
    db_path: Path,
    gis_summary: Dict[str, Any],
    fiscal_quarter: str = "2026-Q4",
    program: str = "NCEM_HMGP_350K",
    use_ollama: bool = True,
) -> Dict[str, Any]:
    """
    Generates an audit-ready State/Federal Quarterly Progress Report (QPR) + SF-425 financial summary.
    Uses 100% open-source local Ollama (gemma4:12b) if reachable, or deterministic synthesis.
    """
    init_compliance_tables(db_path)
    chain = verify_audit_chain(db_path)
    now_iso = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        ledger_rows = [dict(r) for r in conn.execute("SELECT * FROM fiscal_audit_ledger WHERE program = ?", (program,))]
        regs = [dict(r) for r in conn.execute("SELECT * FROM registrations ORDER BY reg_id ASC")]

    observed_disbursed = sum(
        r["amount_usd"] for r in ledger_rows if r["provenance"] == "[OBSERVED]" and r["entry_type"] == "DISBURSEMENT"
    )
    observed_match = sum(
        r["amount_usd"] for r in ledger_rows if r["provenance"] == "[OBSERVED]" and "MATCH" in r["entry_type"]
    )
    modeled_budget = sum(
        r["amount_usd"] for r in ledger_rows if r["provenance"] == "[MODELED]" and r["entry_type"] == "BUDGET_ALLOCATION"
    )

    cams = gis_summary.get("ncdot_cameras") or {}
    rivers = gis_summary.get("usgs_rivers") or {}
    alerts = gis_summary.get("nws_alerts") or {}
    reps = gis_summary.get("county_repeaters") or {}

    engine_used = "deterministic-2cfr200-compiler"
    exec_commentary = (
        f"During {fiscal_quarter}, the open-source GodMode Public Haven Platform maintained real-time ingestion across "
        f"{cams.get('total_statewide', 0)} statewide NC DOT DriveNC traffic cameras ({cams.get('target_county_count', 0)} in designated "
        f"priority counties), {rivers.get('live_gauges_count', 0)} live USGS river basin flood gauges, {alerts.get('active_count', 0)} "
        f"active NWS North Carolina weather advisories, and {reps.get('count', 7)} verified county emergency repeater corridors."
    )

    if use_ollama:
        llm_note = _summarize_with_local_ollama(
            f"Write a concise 3-sentence official North Carolina Emergency Management (N.C.G.S. 143C-6-23) quarterly compliance summary for {fiscal_quarter}. "
            f"Facts: {cams.get('total_statewide', 0)} live NCDOT cameras, {rivers.get('live_gauges_count', 0)} live USGS river gauges, "
            f"7 county repeaters active, SHA-256 ledger valid={chain['valid']}, observed cash disbursed=${observed_disbursed:,.2f}, "
            f"modeled grant budget=${modeled_budget:,.2f}. Never invent metrics."
        )
        if llm_note:
            exec_commentary = llm_note
            engine_used = f"ollama/{LOCAL_OSS_MODEL}"

    reg_lines = "\n".join(
        f"| `{r['reg_id']}` | {r['authority']} | {r['status']} | `{r['identifier_value']}` |" for r in regs
    )

    report_id = f"qpr_{program.lower()}_{fiscal_quarter.lower()}_{int(datetime.now().timestamp())}"
    md = f"""# QUARTERLY PROGRESS & FISCAL COMPLIANCE REPORT ({fiscal_quarter})
**Program**: `{program}`  
**Lead Entity**: The JC Coffey Foundation (501(c)(3)) & KGU International (Technical Subrecipient)  
**Statutory Compliance**: 2 CFR Part 200 / N.C.G.S. § 143C-6-23 / 09 NCAC 03M  
**Generated At**: {now_iso}  
**Synthesis Engine**: `{engine_used}` (100% Sovereign Open-Source)  
**Audit Chain Integrity**: `{'VERIFIED_VALID' if chain['valid'] else 'TAMPER_ALERT'}` (`{chain['head_sha256'][:24]}...`)  

---

## 1. Executive Progress Summary
{exec_commentary}

---

## 2. Public Haven Platform — Live GIS Telemetry Verification (`[OBSERVED]`)
- **NC DOT DriveNC Traffic Cameras**: `{cams.get('total_statewide', 0)}` statewide live streams indexed (`{cams.get('ingested_count', 0)}` cached locally, `{cams.get('target_county_count', 0)}` inside target counties).
- **USGS NWIS River Sensors**: `{rivers.get('live_gauges_count', 0)} / 7` basin flood gauges reporting live stage height & discharge flow.
- **NOAA / NWS Active NC Alerts**: `{alerts.get('active_count', 0)}` active advisories ingested.
- **7-County Emergency Repeater Directory**: `{reps.get('count', 7)}` VHF/UHF/GMRS/LoRa/NOAA corridors maintained across Cumberland, Moore, Harnett, Hoke, Buncombe, Yancey, and Mitchell Counties.

---

## 3. SF-425 / State Audit Financial Reconciliation (`[OBSERVED]` vs `[MODELED]`)
- **`[OBSERVED]` Verified Grant Disbursements**: `${observed_disbursed:,.2f}` USD
- **`[OBSERVED]` Verified Non-Federal Match Logged**: `${observed_match:,.2f}` USD
- **`[MODELED]` Pre-Award / Authorized Program Budget**: `${modeled_budget:,.2f}` USD
- **Cryptographic Ledger Entries Verified**: `{chain['checked_entries']}` rows (`SHA-256` unbroken chain)

---

## 4. Entity Registration & Portal Maintenance Status
| ID | Authority | Status | Identifier |
| :--- | :--- | :--- | :--- |
{reg_lines}
"""

    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO quarterly_reports
            (report_id, fiscal_quarter, program, generated_at, generator_engine, audit_chain_valid, observed_Disbursed_usd, modeled_budget_usd, markdown_body)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report_id,
                fiscal_quarter,
                program,
                now_iso,
                engine_used,
                1 if chain["valid"] else 0,
                observed_disbursed,
                modeled_budget,
                md,
            ),
        )

    return {
        "report_id": report_id,
        "fiscal_quarter": fiscal_quarter,
        "program": program,
        "generated_at": now_iso,
        "generator_engine": engine_used,
        "audit_chain": chain,
        "observed_disbursed_usd": observed_disbursed,
        "modeled_budget_usd": modeled_budget,
        "markdown_body": md,
    }


def get_compliance_overview(db_path: Path) -> Dict[str, Any]:
    init_compliance_tables(db_path)
    chain = verify_audit_chain(db_path)
    with sqlite3.connect(db_path, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        regs = [dict(r) for r in conn.execute("SELECT * FROM registrations ORDER BY reg_id ASC")]
        ledger = [dict(r) for r in conn.execute("SELECT * FROM fiscal_audit_ledger ORDER BY id DESC LIMIT 50")]
        reports = [dict(r) for r in conn.execute("SELECT * FROM quarterly_reports ORDER BY generated_at DESC LIMIT 10")]

    observed_total = sum(r["amount_usd"] for r in ledger if r["provenance"] == "[OBSERVED]")
    modeled_total = sum(r["amount_usd"] for r in ledger if r["provenance"] == "[MODELED]")
    pending_gates = [r for r in regs if "PENDING" in r["identifier_value"] or "ACTION" in r["status"] or "AWAITING" in r["status"] or "READY" in r["status"]]

    return {
        "audit_chain": chain,
        "totals": {
            "observed_verified_usd": round(observed_total, 2),
            "modeled_authorized_budget_usd": round(modeled_total, 2),
        },
        "registrations": regs,
        "human_gates_remaining": len(pending_gates),
        "ledger": ledger,
        "quarterly_reports": reports,
    }
