#!/usr/bin/env python3
"""
GodMode — Public Haven Platform & 2 CFR 200 / N.C.G.S. § 143C-6-23 Compliance Server
100% Open-Source Python Standard-Library Multi-Threaded HTTP Server (Zero External Pip Dependencies).
Runs natively inside OpenBot under Aria (Executive Chief of Staff) on local Ollama (gemma4:12b).
"""

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from godmode.compliance import (
    generate_quarterly_progress_report,
    get_compliance_overview,
    init_compliance_tables,
    record_ledger_entry,
    update_registration,
    verify_audit_chain,
)
from godmode.grokbot_v1 import (
    get_grokbot_v1_dashboard,
    ingest_share_sheet_item,
    init_grokbot_v1_tables,
    stage_email_draft_with_attachments,
    submit_or_review_learning,
    summarize_meeting_notes,
)
from godmode.ingestion import get_cached_streams_summary, init_gis_tables, run_full_gis_cycle
from godmode.openbot_runner import AriaGodModeOverseer

DATA_DIR = Path(os.getenv("GODMODE_DATA_DIR", str(BASE_DIR / "data")))
DB_PATH = DATA_DIR / "godmode.db"
DRAFTS_DIR = DATA_DIR / "email_drafts"
STATIC_DIR = Path(__file__).resolve().parent / "static"

OVERSEER = AriaGodModeOverseer(db_path=DB_PATH, gis_interval_sec=900)


class GodModeHTTPHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _send_json(self, data: Dict[str, Any], status: int = 200) -> None:
        raw = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(raw)

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8", errors="ignore")
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def do_OPTIONS(self) -> None:
        self._send_json({"ok": True})

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path in ("/", "/index.html"):
            idx = STATIC_DIR / "index.html"
            if idx.exists():
                body = idx.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self._send_json({"app": "GodMode Public Haven Platform", "ui": "missing"}, 200)
            return

        if path == "/api/health":
            chain = verify_audit_chain(DB_PATH)
            self._send_json(
                {
                    "status": "ok",
                    "platform": "GodMode — Public Haven Platform (KGU International)",
                    "license": "MIT (100% Open Source)",
                    "overseer": OVERSEER.OVERSEER_NAME,
                    "model": OVERSEER.OVERSEER_MODEL,
                    "audit_chain_valid": chain["valid"],
                }
            )
            return

        if path == "/api/streams/summary":
            self._send_json(get_cached_streams_summary(DB_PATH))
            return

        if path == "/api/compliance/overview":
            self._send_json(get_compliance_overview(DB_PATH))
            return

        if path == "/api/aria/status":
            self._send_json(OVERSEER.get_status())
            return

        if path == "/api/openbot1/dashboard":
            self._send_json(get_grokbot_v1_dashboard(DB_PATH))
            return

        if path in ("/llms.txt", "/.well-known/agent-safe-harbor.json"):
            self._send_json(
                {
                    "protocol": "AgentSafeHarbor/1.0",
                    "maintainer": "KGU International & The JC Coffey Foundation",
                    "human_gated_actions": ["payment", "outbound_message", "deletion", "credential_use"],
                    "open_source": True,
                }
            )
            return

        self._send_json({"error": "Not Found", "path": path}, 404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        body = self._read_json()

        try:
            if path == "/api/streams/refresh":
                self._send_json(run_full_gis_cycle(DB_PATH))
                return

            if path == "/api/compliance/ledger":
                res = record_ledger_entry(
                    db_path=DB_PATH,
                    program=str(body.get("program") or "NCEM_HMGP_350K"),
                    entry_type=str(body.get("entry_type") or "DISBURSEMENT"),
                    cost_category=str(body.get("cost_category") or "NC GIS & Data Stream Integration"),
                    amount_usd=float(body.get("amount_usd") or 0.0),
                    provenance=str(body.get("provenance") or "[OBSERVED]"),
                    voucher_ref=str(body.get("voucher_ref") or ""),
                    notes=str(body.get("notes") or ""),
                )
                self._send_json(res, 201)
                return

            if path == "/api/compliance/registrations/update":
                res = update_registration(
                    db_path=DB_PATH,
                    reg_id=str(body.get("reg_id") or ""),
                    identifier_value=body.get("identifier_value"),
                    status=body.get("status"),
                )
                self._send_json(res)
                return

            if path == "/api/compliance/qpr/generate":
                gis = get_cached_streams_summary(DB_PATH)
                res = generate_quarterly_progress_report(
                    db_path=DB_PATH,
                    gis_summary=gis,
                    fiscal_quarter=str(body.get("fiscal_quarter") or "2026-Q4"),
                    program=str(body.get("program") or "NCEM_HMGP_350K"),
                    use_ollama=bool(body.get("use_ollama", True)),
                )
                self._send_json(res, 201)
                return

            if path == "/api/openbot1/share":
                res = ingest_share_sheet_item(
                    db_path=DB_PATH,
                    title=str(body.get("title") or "Shared Note"),
                    payload_text=str(body.get("payload_text") or ""),
                    content_type=str(body.get("content_type") or "text/plain"),
                    source_device=str(body.get("source_device") or "Share Sheet"),
                )
                self._send_json(res, 201)
                return

            if path == "/api/openbot1/email-draft":
                res = stage_email_draft_with_attachments(
                    db_path=DB_PATH,
                    drafts_dir=DRAFTS_DIR,
                    recipient=str(body.get("recipient") or ""),
                    subject=str(body.get("subject") or "Grant & Compliance Packet"),
                    body_text=str(body.get("body_text") or ""),
                    attachment_paths=list(body.get("attachment_paths") or []),
                )
                self._send_json(res, 201)
                return

            if path == "/api/openbot1/meeting-summary":
                res = summarize_meeting_notes(
                    db_path=DB_PATH,
                    title=str(body.get("title") or "Stakeholder Call"),
                    transcript_text=str(body.get("transcript_text") or ""),
                    platform=str(body.get("platform") or "Zoom / Meet / Teams"),
                    meeting_url=str(body.get("meeting_url") or ""),
                )
                self._send_json(res, 201)
                return

            if path == "/api/openbot1/learnings":
                res = submit_or_review_learning(
                    db_path=DB_PATH,
                    action=str(body.get("action") or "submit"),
                    learning_id=body.get("learning_id"),
                    submitted_by=str(body.get("submitted_by") or "Teammate"),
                    target_bot=str(body.get("target_bot") or "Aria"),
                    proposed_rule=str(body.get("proposed_rule") or ""),
                )
                self._send_json(res)
                return

            if path == "/api/aria/drain-inbox":
                self._send_json({"processed": OVERSEER.drain_openbot_inbox()})
                return

            self._send_json({"error": "Not Found", "path": path}, 404)
        except Exception as exc:
            self._send_json({"error": str(exc)}, 400)


def run_server() -> None:
    host = os.getenv("GODMODE_HOST", "0.0.0.0")
    port = int(os.getenv("GODMODE_PORT", "8096"))
    init_gis_tables(DB_PATH)
    init_compliance_tables(DB_PATH)
    init_grokbot_v1_tables(DB_PATH)
    OVERSEER.start()
    print(f"[GodMode] Public Haven Platform & Compliance Suite online at http://{host}:{port}")
    server = ThreadingHTTPServer((host, port), GodModeHTTPHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        OVERSEER.stop()
        server.server_close()


if __name__ == "__main__":
    run_server()
