#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_CONVERSATION_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_conversation_intelligence.csv"
DEFAULT_OPEN_LOOPS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "open_loops.csv"
DEFAULT_DEAL_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_deal_sheets.csv"
DEFAULT_EVENT_OPS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_event_ops_registry.csv"
DEFAULT_MENU_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_menu_profiles.csv"
DEFAULT_PRICING_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_pricing_scope_profiles.csv"
DEFAULT_SCHEDULE_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_schedule_commitments.csv"
DEFAULT_SELLER_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_seller_performance_signals.csv"
DEFAULT_SOURCE_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_source_attribution_profiles.csv"
DEFAULT_PROMISE_TRACKER_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "promise_tracker.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "miscommunication_intelligence"


CATEGORY_LABELS = {
    "unanswered_buyer_ask": "Unanswered Buyer Ask",
    "promise_followthrough_gap": "Promise Follow-Through Gap",
    "venue_alignment_gap": "Venue Alignment Gap",
    "uncertain_event_date": "Uncertain Event Date",
    "commercial_alignment_risk": "Commercial Alignment Risk",
    "menu_ops_detail_gap": "Menu / Ops Detail Gap",
    "decision_chain_stall": "Decision-Chain Stall",
    "won_carryover_watch": "Won Carryover Watch",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a likely-miscommunication audit layer from the normalized Comeketo CRM library.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--conversation-csv", type=Path, default=DEFAULT_CONVERSATION_CSV)
    parser.add_argument("--open-loops-csv", type=Path, default=DEFAULT_OPEN_LOOPS_CSV)
    parser.add_argument("--deal-csv", type=Path, default=DEFAULT_DEAL_CSV)
    parser.add_argument("--event-ops-csv", type=Path, default=DEFAULT_EVENT_OPS_CSV)
    parser.add_argument("--menu-csv", type=Path, default=DEFAULT_MENU_CSV)
    parser.add_argument("--pricing-csv", type=Path, default=DEFAULT_PRICING_CSV)
    parser.add_argument("--schedule-csv", type=Path, default=DEFAULT_SCHEDULE_CSV)
    parser.add_argument("--seller-csv", type=Path, default=DEFAULT_SELLER_CSV)
    parser.add_argument("--source-csv", type=Path, default=DEFAULT_SOURCE_CSV)
    parser.add_argument("--promise-tracker-csv", type=Path, default=DEFAULT_PROMISE_TRACKER_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def safe_int(value: Optional[str]) -> int:
    try:
        return int(value or 0)
    except ValueError:
        return 0


def compact_text(value: Optional[str], limit: int = 320) -> str:
    text = " ".join((value or "").split())
    return text[:limit]


def slugify(value: Optional[str], fallback: str = "unknown") -> str:
    text = (value or "").strip().lower()
    if not text:
        return fallback
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:100] or fallback


def pretty_label(value: str) -> str:
    return value.replace("_", " ")


def lead_dir_from_sources(*paths: str) -> Optional[Path]:
    filenames = {
        "lead_memory_brief.md",
        "lead_deal_sheet.md",
        "lead_deal_sheet.json",
        "lead_action_plan.md",
        "lead_event_ops_sheet.md",
        "lead_menu_profile.md",
        "lead_pricing_scope_sheet.md",
        "lead_schedule_commitment_sheet.md",
        "lead_seller_signal_sheet.md",
        "lead_source_attribution_sheet.md",
    }
    for raw in paths:
        if not raw:
            continue
        path = Path(raw)
        if path.name in filenames:
            return path.parent
    return None


def boolish(value: Optional[str]) -> bool:
    return (value or "").strip().lower() == "true"


def build_finding(
    lead_row: Dict[str, Any],
    category: str,
    severity_score: int,
    summary: str,
    evidence_text: str,
    recommended_fix: str,
    source_path: str,
) -> Dict[str, Any]:
    return {
        "lead_id": lead_row["lead_id"],
        "lead_name": lead_row["lead_name"],
        "lead_owner_name": lead_row["lead_owner_name"],
        "stage_label": lead_row["stage_label"],
        "stage_type": lead_row["stage_type"],
        "category": category,
        "category_label": CATEGORY_LABELS[category],
        "severity_score": severity_score,
        "summary": compact_text(summary, 240),
        "evidence_text": compact_text(evidence_text, 420),
        "recommended_fix": compact_text(recommended_fix, 280),
        "waiting_on_us": lead_row["waiting_on_us"],
        "priority_score": lead_row["priority_score"],
        "source_path": source_path,
        "lead_miscommunication_audit_path": lead_row["lead_miscommunication_audit_path"],
    }


def board_markdown(title: str, rows: Sequence[Dict[str, Any]]) -> str:
    lines = [f"# {title}", "", f"- Total rows: `{len(rows)}`", "", "## Top Findings"]
    for row in rows[:120]:
        lines.append(
            f"- `{row.get('severity_score') or 0}` | {row.get('lead_name') or ''} | "
            f"{row.get('lead_owner_name') or ''} | {row.get('stage_label') or ''} | {row.get('summary') or ''}"
        )
    if not rows:
        lines.append("- None.")
    lines.append("")
    return "\n".join(lines)


def overview_markdown(category_rows: Sequence[Dict[str, Any]]) -> str:
    lines = ["# Miscommunication Intelligence", "", "## Categories"]
    for row in category_rows:
        lines.append(
            f"- {row.get('category_label') or ''}: leads {row.get('lead_count') or 0} | "
            f"active {row.get('active_lead_count') or 0} | won {row.get('won_lead_count') or 0} | "
            f"high severity {row.get('high_severity_count') or 0} | {row.get('summary') or ''}"
        )
    lines.append("")
    return "\n".join(lines)


def lead_audit_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Lead Miscommunication Audit: {payload.get('lead_name') or ''}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Pipeline / Stage: {payload.get('pipeline_name') or ''} / {payload.get('stage_label') or ''}",
        f"- Primary Source: {payload.get('primary_source_channel') or ''}",
        f"- Waiting On Us: `{payload.get('waiting_on_us') or ''}`",
        f"- Audit Flag Count: `{payload.get('audit_flag_count') or 0}`",
        f"- Highest Severity: `{payload.get('highest_severity_score') or 0}`",
        f"- Top Category: {payload.get('top_category_label') or ''}",
        "",
        "## Why It Looks Fragile",
        f"- {payload.get('audit_summary') or ''}",
        "",
        "## Findings",
    ]
    findings = payload.get("findings") or []
    if findings:
        for item in findings:
            lines.extend(
                [
                    f"### {item.get('category_label') or ''}",
                    f"- Severity: `{item.get('severity_score') or 0}`",
                    f"- Summary: {item.get('summary') or ''}",
                    f"- Evidence: {item.get('evidence_text') or ''}",
                    f"- Recommended Fix: {item.get('recommended_fix') or ''}",
                    f"- Source: {item.get('source_path') or ''}",
                    "",
                ]
            )
    else:
        lines.append("- No strong miscommunication signal triggered in this pass.")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    output_dir = args.output_dir
    by_category_dir = output_dir / "by_category"
    ensure_dir(output_dir)
    ensure_dir(by_category_dir)

    conversation_rows = {row["lead_id"]: row for row in load_csv_rows(args.conversation_csv)}
    open_loop_rows = load_csv_rows(args.open_loops_csv)
    deal_rows = {row["lead_id"]: row for row in load_csv_rows(args.deal_csv)}
    event_ops_rows = {row["lead_id"]: row for row in load_csv_rows(args.event_ops_csv)}
    menu_rows = {row["lead_id"]: row for row in load_csv_rows(args.menu_csv)}
    pricing_rows = {row["lead_id"]: row for row in load_csv_rows(args.pricing_csv)}
    schedule_rows = {row["lead_id"]: row for row in load_csv_rows(args.schedule_csv)}
    seller_rows = {row["lead_id"]: row for row in load_csv_rows(args.seller_csv)}
    source_rows = {row["lead_id"]: row for row in load_csv_rows(args.source_csv)}
    promise_rows = load_csv_rows(args.promise_tracker_csv)

    open_loops_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    buyer_questions_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in open_loop_rows:
        lead_id = row.get("lead_id") or ""
        if not lead_id:
            continue
        open_loops_by_lead[lead_id].append(row)
        if row.get("loop_type") == "buyer_question":
            buyer_questions_by_lead[lead_id].append(row)

    promises_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in promise_rows:
        lead_id = row.get("lead_id") or ""
        if lead_id:
            promises_by_lead[lead_id].append(row)

    lead_ids = (
        set(conversation_rows.keys())
        | set(deal_rows.keys())
        | set(event_ops_rows.keys())
        | set(menu_rows.keys())
        | set(pricing_rows.keys())
        | set(schedule_rows.keys())
        | set(seller_rows.keys())
        | set(source_rows.keys())
    )

    finding_rows: List[Dict[str, Any]] = []
    lead_signal_rows: List[Dict[str, Any]] = []

    for lead_id in sorted(lead_ids):
        convo = conversation_rows.get(lead_id, {})
        deal = deal_rows.get(lead_id, {})
        ops = event_ops_rows.get(lead_id, {})
        menu = menu_rows.get(lead_id, {})
        pricing = pricing_rows.get(lead_id, {})
        schedule = schedule_rows.get(lead_id, {})
        seller = seller_rows.get(lead_id, {})
        source = source_rows.get(lead_id, {})

        lead_name = (
            deal.get("lead_name")
            or convo.get("lead_name")
            or seller.get("lead_name")
            or source.get("lead_name")
            or ""
        )
        lead_owner_name = (
            deal.get("lead_owner_name")
            or seller.get("lead_owner_name")
            or source.get("lead_owner_name")
            or convo.get("lead_owner_name")
            or "Unassigned"
        )
        pipeline_name = (
            deal.get("pipeline_name")
            or seller.get("pipeline_name")
            or source.get("pipeline_name")
            or ""
        )
        stage_label = (
            deal.get("stage_label")
            or seller.get("stage_label")
            or source.get("stage_label")
            or convo.get("stage_label")
            or ""
        )
        stage_type = (
            deal.get("stage_type")
            or seller.get("stage_type")
            or ""
        )

        lead_dir = lead_dir_from_sources(
            deal.get("deal_sheet_path") or "",
            source.get("lead_source_attribution_sheet_path") or "",
            seller.get("lead_seller_signal_sheet_path") or "",
            schedule.get("lead_schedule_commitment_sheet_path") or "",
        )
        if lead_dir is None:
            continue

        lead_stub = {
            "lead_id": lead_id,
            "lead_name": lead_name,
            "lead_owner_name": lead_owner_name,
            "pipeline_name": pipeline_name,
            "stage_label": stage_label,
            "stage_type": stage_type,
            "waiting_on_us": "True" if boolish(seller.get("waiting_on_us") or deal.get("waiting_on_us")) else "False",
            "priority_score": max(
                safe_int(seller.get("priority_score")),
                safe_int(deal.get("follow_up_priority_score")),
                safe_int(deal.get("readiness_score")),
            ),
            "lead_miscommunication_audit_path": str(lead_dir / "lead_miscommunication_audit.md"),
        }

        readiness_score = safe_int(deal.get("readiness_score"))
        due_today = safe_int(schedule.get("due_today_count"))
        due_48h = safe_int(schedule.get("due_48h_count"))
        open_loop_count = safe_int(convo.get("open_loop_count"))
        blocker_count = safe_int(convo.get("blocker_count"))
        waiting_on_us = boolish(lead_stub["waiting_on_us"])
        pending_promise_count = max(
            safe_int(seller.get("pending_commitment_count")),
            safe_int(schedule.get("pending_promise_count")),
            len(promises_by_lead.get(lead_id, [])),
        )
        quote_friction_count = max(
            safe_int(seller.get("quote_friction_count")),
            safe_int(pricing.get("quote_revision_count")) + safe_int(pricing.get("budget_pressure_count")) + safe_int(pricing.get("package_compare_count")),
        )
        budget_pressure_count = max(safe_int(seller.get("budget_pressure_count")), safe_int(pricing.get("budget_pressure_count")))
        scope_reduction_count = safe_int(pricing.get("scope_reduction_count"))
        menu_signal_score = safe_int(menu.get("menu_signal_score"))
        menu_question_count = safe_int(menu.get("menu_question_count"))
        venue_gap = boolish(ops.get("venue_gap_flag")) or (ops.get("venue_status") in {"unknown", "area_known_only", "venue_pending", "venue_conflict", "venue_type_captured"})
        uncertain_date = schedule.get("schedule_state") in {"uncertain_fallback_date", "stale_uncertain_date", "past_event_date"}
        decision_chain = (deal.get("decision_state") or "") == "partner_or_family_review"

        lead_findings: List[Dict[str, Any]] = []

        buyer_questions = buyer_questions_by_lead.get(lead_id, [])
        if stage_type == "active" and waiting_on_us and buyer_questions:
            top_question = buyer_questions[0]
            severity = max(readiness_score, lead_stub["priority_score"]) + len(buyer_questions) * 6 + due_today * 4 + due_48h * 2
            lead_findings.append(
                build_finding(
                    lead_stub,
                    "unanswered_buyer_ask",
                    severity,
                    "The buyer asked a direct question and the lead still looks like it is waiting on the team.",
                    top_question.get("text") or convo.get("latest_buyer_ask") or seller.get("primary_stall_reason") or "",
                    seller.get("seller_next_move") or "Reply directly to the buyer question and close with one concrete next step.",
                    top_question.get("source_path") or "",
                )
            )

        promise_items = promises_by_lead.get(lead_id, [])
        if stage_type == "active" and pending_promise_count > 0:
            top_promise = promise_items[0] if promise_items else {}
            severity = max(readiness_score, lead_stub["priority_score"]) + pending_promise_count * 10 + due_today * 4 + due_48h * 2
            lead_findings.append(
                build_finding(
                    lead_stub,
                    "promise_followthrough_gap",
                    severity,
                    "A promised follow-up still looks open, which is an easy place for trust to leak.",
                    top_promise.get("source_text") or seller.get("primary_stall_reason") or schedule.get("next_due_summary") or "",
                    top_promise.get("task_text") or seller.get("seller_next_move") or "Close the oldest promise before starting a new thread.",
                    top_promise.get("source_path") or "",
                )
            )

        if stage_type == "active" and venue_gap and (readiness_score >= 90 or due_today or due_48h):
            severity = max(readiness_score, lead_stub["priority_score"]) + due_today * 4 + due_48h * 2
            lead_findings.append(
                build_finding(
                    lead_stub,
                    "venue_alignment_gap",
                    severity,
                    "The lead is far enough along that venue ambiguity can easily create crossed wires in quoting or planning.",
                    ops.get("ops_watch_reason") or ops.get("venue_summary") or deal.get("venue_evidence") or "",
                    "Confirm the venue choice, the catering rules, and any service-window constraints before revising the plan again.",
                    ops.get("lead_event_ops_sheet_path") or deal.get("deal_sheet_path") or "",
                )
            )

        if stage_type == "active" and uncertain_date and (readiness_score >= 80 or due_today or due_48h):
            severity = max(readiness_score, lead_stub["priority_score"]) + due_today * 4 + due_48h * 2
            lead_findings.append(
                build_finding(
                    lead_stub,
                    "uncertain_event_date",
                    severity,
                    "The event date still looks like a fallback or stale CRM value, so timing assumptions can drift without anyone noticing.",
                    schedule.get("event_watch_reason") or schedule.get("next_due_summary") or "",
                    "Reconfirm the real event date before using it for urgency, staffing, or quote timing decisions.",
                    schedule.get("lead_schedule_commitment_sheet_path") or "",
                )
            )

        if stage_type == "active" and (quote_friction_count > 0 or budget_pressure_count > 0 or scope_reduction_count > 0) and (readiness_score >= 90 or waiting_on_us):
            severity = max(readiness_score, lead_stub["priority_score"]) + quote_friction_count * 8 + budget_pressure_count * 8 + scope_reduction_count * 10
            lead_findings.append(
                build_finding(
                    lead_stub,
                    "commercial_alignment_risk",
                    severity,
                    "Budget, quote, or scope signals suggest the team and buyer may not be fully aligned on what is being sold.",
                    pricing.get("top_budget_pressure")
                    or pricing.get("top_scope_change")
                    or pricing.get("top_pricing_question")
                    or pricing.get("pricing_scope_summary")
                    or "",
                    pricing.get("pricing_action") or seller.get("seller_next_move") or "Send a tighter side-by-side option set with the tradeoffs spelled out.",
                    pricing.get("lead_pricing_scope_sheet_path") or "",
                )
            )

        if stage_type == "active" and (
            menu.get("dietary_flags") not in {"", "unknown"}
            or menu.get("venue_food_flags") not in {"", "unknown"}
            or menu_question_count >= 4
        ) and (venue_gap or due_today or due_48h or readiness_score >= 90):
            severity = max(readiness_score, lead_stub["priority_score"]) + menu_signal_score + due_today * 3 + due_48h * 2
            lead_findings.append(
                build_finding(
                    lead_stub,
                    "menu_ops_detail_gap",
                    severity,
                    "There are enough food, service, or venue-rule details here that the next handoff could easily miss something material.",
                    menu.get("menu_profile_summary") or menu.get("venue_food_evidence") or menu.get("dietary_evidence") or "",
                    "Restate the menu constraints, service format, and venue limitations explicitly in the next quote or handoff note.",
                    menu.get("lead_menu_profile_path") or "",
                )
            )

        if stage_type == "active" and decision_chain and (waiting_on_us or open_loop_count > 0 or blocker_count >= 3):
            severity = max(readiness_score, lead_stub["priority_score"]) + blocker_count * 5 + open_loop_count * 8 + (15 if waiting_on_us else 0)
            lead_findings.append(
                build_finding(
                    lead_stub,
                    "decision_chain_stall",
                    severity,
                    "More than one decision-maker is in the loop, and the thread already shows signs of stall or signal loss.",
                    deal.get("decision_evidence") or convo.get("latest_blocker") or convo.get("latest_buyer_ask") or "",
                    "Summarize the recommendation for every decision-maker, answer the open questions, and set a review deadline.",
                    deal.get("deal_sheet_path") or convo.get("conversation_intelligence_path") or "",
                )
            )

        if stage_type == "won" and (blocker_count > 0 or open_loop_count > 0 or pending_promise_count > 0):
            severity = max(readiness_score, lead_stub["priority_score"]) + blocker_count * 5 + open_loop_count * 8 + pending_promise_count * 8
            lead_findings.append(
                build_finding(
                    lead_stub,
                    "won_carryover_watch",
                    severity,
                    "The deal is already marked won, but the conversation still carries unresolved or changed-detail signals worth double-checking.",
                    convo.get("latest_blocker") or convo.get("latest_buyer_ask") or seller.get("primary_stall_reason") or "",
                    "Do one last detail-confirmation pass so changed assumptions do not sneak into execution.",
                    deal.get("deal_sheet_path") or convo.get("conversation_intelligence_path") or "",
                )
            )

        lead_findings = sorted(lead_findings, key=lambda row: (-safe_int(str(row.get("severity_score") or 0)), row.get("category") or ""))
        finding_rows.extend(lead_findings)

        highest_severity = safe_int(str(lead_findings[0].get("severity_score") or 0)) if lead_findings else 0
        top_category_label = lead_findings[0].get("category_label") if lead_findings else ""
        audit_summary = (
            compact_text(
                f"{lead_name}: {len(lead_findings)} likely miscommunication signals. "
                f"Top issue {top_category_label or 'none'}. Waiting on us {lead_stub['waiting_on_us']}. "
                f"Open loops {open_loop_count}. Pending promises {pending_promise_count}. "
                f"Venue gap {venue_gap}. Uncertain date {uncertain_date}. Quote friction {quote_friction_count}.",
                420,
            )
            if lead_findings
            else f"{lead_name}: no strong miscommunication signal triggered in this audit pass."
        )

        lead_signal_row = {
            "lead_id": lead_id,
            "lead_name": lead_name,
            "lead_owner_name": lead_owner_name,
            "pipeline_name": pipeline_name,
            "stage_label": stage_label,
            "stage_type": stage_type,
            "primary_source_channel": source.get("primary_source_channel") or "",
            "waiting_on_us": lead_stub["waiting_on_us"],
            "open_loop_count": open_loop_count,
            "pending_promise_count": pending_promise_count,
            "quote_friction_count": quote_friction_count,
            "menu_signal_score": menu_signal_score,
            "venue_gap_flag": "True" if venue_gap else "False",
            "uncertain_event_date_flag": "True" if uncertain_date else "False",
            "decision_chain_flag": "True" if decision_chain else "False",
            "audit_flag_count": len(lead_findings),
            "highest_severity_score": highest_severity,
            "top_category_label": top_category_label,
            "audit_summary": audit_summary,
            "lead_miscommunication_audit_path": str(lead_dir / "lead_miscommunication_audit.md"),
            "lead_miscommunication_audit_json_path": str(lead_dir / "lead_miscommunication_audit.json"),
        }

        payload = dict(lead_signal_row)
        payload["findings"] = lead_findings
        write_json(lead_dir / "lead_miscommunication_audit.json", payload)
        (lead_dir / "lead_miscommunication_audit.md").write_text(lead_audit_markdown(payload), encoding="utf-8")
        lead_signal_rows.append(lead_signal_row)

    lead_signal_rows = sorted(
        lead_signal_rows,
        key=lambda row: (
            -safe_int(str(row.get("highest_severity_score") or 0)),
            -safe_int(str(row.get("audit_flag_count") or 0)),
            row.get("lead_name") or "",
        ),
    )
    finding_rows = sorted(
        finding_rows,
        key=lambda row: (
            -safe_int(str(row.get("severity_score") or 0)),
            row.get("lead_name") or "",
            row.get("category") or "",
        ),
    )

    category_summary_rows: List[Dict[str, Any]] = []
    finding_categories: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in finding_rows:
        finding_categories[row["category"]].append(row)

    for category, rows in sorted(finding_categories.items(), key=lambda item: (-len(item[1]), item[0])):
        category_dir = by_category_dir / slugify(category)
        ensure_dir(category_dir)
        (category_dir / "board.md").write_text(board_markdown(CATEGORY_LABELS[category], rows), encoding="utf-8")
        write_csv(category_dir / "findings.csv", rows)
        write_jsonl(category_dir / "findings.jsonl", rows)

        leads_in_category = {row["lead_id"] for row in rows}
        lead_rows = [row for row in lead_signal_rows if row["lead_id"] in leads_in_category]
        active_count = sum(1 for row in lead_rows if row.get("stage_type") == "active")
        won_count = sum(1 for row in lead_rows if row.get("stage_type") == "won")
        owners = Counter((row.get("lead_owner_name") or "unknown").strip() or "unknown" for row in lead_rows)
        stages = Counter((row.get("stage_label") or "unknown").strip() or "unknown" for row in lead_rows)
        category_summary_rows.append(
            {
                "category": category,
                "category_label": CATEGORY_LABELS[category],
                "finding_count": len(rows),
                "lead_count": len(leads_in_category),
                "active_lead_count": active_count,
                "won_lead_count": won_count,
                "high_severity_count": sum(1 for row in rows if safe_int(str(row.get("severity_score") or 0)) >= 120),
                "top_owners": " | ".join(f"{name} ({count})" for name, count in owners.most_common(3)),
                "top_stages": " | ".join(f"{name} ({count})" for name, count in stages.most_common(3)),
                "summary": compact_text(rows[0].get("summary") or "", 240),
                "board_path": str(category_dir / "board.md"),
            }
        )

    category_summary_rows = sorted(
        category_summary_rows,
        key=lambda row: (
            -safe_int(str(row.get("lead_count") or 0)),
            -safe_int(str(row.get("finding_count") or 0)),
            row.get("category_label") or "",
        ),
    )

    write_csv(normalized_dir / "lead_miscommunication_signals.csv", lead_signal_rows)
    write_jsonl(normalized_dir / "lead_miscommunication_signals.jsonl", lead_signal_rows)
    write_csv(normalized_dir / "miscommunication_findings.csv", finding_rows)
    write_jsonl(normalized_dir / "miscommunication_findings.jsonl", finding_rows)
    write_csv(normalized_dir / "miscommunication_category_summary.csv", category_summary_rows)

    (output_dir / "README.md").write_text(
        "\n".join(
            [
                "# Miscommunication Intelligence",
                "",
                "This layer looks for likely misses, dropped details, or crossed signals: unanswered buyer asks, unfulfilled promises, venue/date ambiguity, commercial misalignment, and handoff details that look easy to lose.",
                "",
                "## Snapshot",
                f"- Lead audit sheets: `{len(lead_signal_rows)}`",
                f"- Total findings: `{len(finding_rows)}`",
                f"- Categories triggered: `{len(category_summary_rows)}`",
                "",
                "## Key Files",
                "- `overview.md`: one-page category scan",
                "- `by_category/.../board.md`: findings grouped by category",
                "- `../normalized/lead_miscommunication_signals.csv`: one-row-per-lead audit summary",
                "- `../normalized/miscommunication_findings.csv`: one-row-per-finding evidence log",
                "- `../normalized/miscommunication_category_summary.csv`: one-row-per-category summary",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (output_dir / "overview.md").write_text(overview_markdown(category_summary_rows), encoding="utf-8")

    print(
        json.dumps(
            {
                "lead_audits": len(lead_signal_rows),
                "findings": len(finding_rows),
                "categories": len(category_summary_rows),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
