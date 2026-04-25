#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_MISCOMMUNICATION_SIGNALS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_miscommunication_signals.csv"
DEFAULT_MISCOMMUNICATION_FINDINGS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "miscommunication_findings.csv"
DEFAULT_ACTION_ITEMS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "action_items.csv"
DEFAULT_OWNER_TASK_BOARD_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "owner_task_board.csv"
DEFAULT_CUSTOMER_WAITING_BOARD_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "customer_waiting_board.csv"
DEFAULT_PROMISE_TRACKER_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "promise_tracker.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "recovery_intelligence"


BOARD_GROUPS = {
    "unanswered_buyer_ask": "same_day_recovery",
    "promise_followthrough_gap": "same_day_recovery",
    "commercial_alignment_risk": "commercial_recovery",
    "decision_chain_stall": "decision_chain_recovery",
    "venue_alignment_gap": "venue_date_recovery",
    "uncertain_event_date": "venue_date_recovery",
    "menu_ops_detail_gap": "venue_date_recovery",
    "won_carryover_watch": "won_reconfirmation",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ranked recovery queues from the miscommunication audit layer.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--miscommunication-signals-csv", type=Path, default=DEFAULT_MISCOMMUNICATION_SIGNALS_CSV)
    parser.add_argument("--miscommunication-findings-csv", type=Path, default=DEFAULT_MISCOMMUNICATION_FINDINGS_CSV)
    parser.add_argument("--action-items-csv", type=Path, default=DEFAULT_ACTION_ITEMS_CSV)
    parser.add_argument("--owner-task-board-csv", type=Path, default=DEFAULT_OWNER_TASK_BOARD_CSV)
    parser.add_argument("--customer-waiting-board-csv", type=Path, default=DEFAULT_CUSTOMER_WAITING_BOARD_CSV)
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


def priority_band(score: int) -> str:
    if score >= 190:
        return "critical"
    if score >= 145:
        return "high"
    if score >= 105:
        return "medium"
    return "watch"


def due_window(category: str, severity_score: int, stage_type: str, waiting_on_us: bool) -> Tuple[str, str, str]:
    now_utc = datetime.now(timezone.utc)
    if category in {"unanswered_buyer_ask", "promise_followthrough_gap"}:
        due_days = 0 if waiting_on_us or severity_score >= 120 else 1
        due_bucket = "today" if due_days == 0 else "48h"
        reason = "buyer is waiting or a promise is still hanging open"
    elif category == "commercial_alignment_risk":
        due_days = 0 if waiting_on_us or severity_score >= 180 else 1
        due_bucket = "today" if due_days == 0 else "48h"
        reason = "commercial mismatch can drift quickly without a reset"
    elif category in {"venue_alignment_gap", "uncertain_event_date", "menu_ops_detail_gap"}:
        due_days = 1 if severity_score >= 140 else 3
        due_bucket = "48h" if due_days == 1 else "3d"
        reason = "operational details should be locked before the next quote or handoff"
    elif category == "decision_chain_stall":
        due_days = 3 if stage_type == "active" else 7
        due_bucket = "3d" if due_days == 3 else "7d"
        reason = "multi-person review needs a forced decision checkpoint"
    else:
        due_days = 3 if severity_score >= 140 else 7
        due_bucket = "3d" if due_days == 3 else "7d"
        reason = "won-stage details deserve a clean re-confirmation pass"

    due_at = now_utc + timedelta(days=due_days)
    due_date_utc = due_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return due_bucket, due_date_utc, reason


def action_owner(category: str) -> str:
    if category == "won_carryover_watch":
        return "ops_review"
    return "sales_owner"


def lead_dir_from_paths(*paths: str) -> Optional[Path]:
    filenames = {
        "lead_miscommunication_audit.md",
        "lead_miscommunication_audit.json",
        "lead_deal_sheet.md",
        "lead_action_plan.md",
    }
    for raw in paths:
        if not raw:
            continue
        path = Path(raw)
        if path.name in filenames:
            return path.parent
    return None


def board_markdown(title: str, rows: Sequence[Dict[str, Any]]) -> str:
    lines = [f"# {title}", "", f"- Total rows: `{len(rows)}`", "", "## Top Rows"]
    for row in rows[:120]:
        lines.append(
            f"- `{row.get('recovery_priority_score') or 0}` | {row.get('lead_name') or ''} | "
            f"{row.get('lead_owner_name') or ''} | {row.get('stage_label') or ''} | {row.get('primary_recovery_action') or ''}"
        )
    if not rows:
        lines.append("- None.")
    lines.append("")
    return "\n".join(lines)


def owner_overview_markdown(rows: Sequence[Dict[str, Any]]) -> str:
    lines = ["# Owner Recovery Overview", "", "## Owners"]
    for row in rows:
        lines.append(
            f"- {row.get('owner_name') or ''}: leads {row.get('lead_count') or 0} | "
            f"critical {row.get('critical_lead_count') or 0} | high {row.get('high_lead_count') or 0} | "
            f"today {row.get('today_lead_count') or 0} | waiting on us {row.get('waiting_on_us_count') or 0} | "
            f"{row.get('owner_focus') or ''}"
        )
    lines.append("")
    return "\n".join(lines)


def lead_recovery_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Lead Recovery Plan: {payload.get('lead_name') or ''}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Pipeline / Stage: {payload.get('pipeline_name') or ''} / {payload.get('stage_label') or ''}",
        f"- Primary Source: {payload.get('primary_source_channel') or ''}",
        f"- Recovery Priority Score: `{payload.get('recovery_priority_score') or 0}`",
        f"- Recovery Priority Band: {payload.get('recovery_priority_band') or ''}",
        f"- Deadline Bucket: {payload.get('recovery_due_bucket') or ''}",
        f"- Waiting On Us: `{payload.get('waiting_on_us') or ''}`",
        f"- Existing Sales Tasks: `{payload.get('existing_sales_task_count') or 0}`",
        f"- Existing Promise Tasks: `{payload.get('existing_promise_task_count') or 0}`",
        "",
        "## Primary Move",
        f"- {payload.get('primary_recovery_action') or ''}",
        f"- Why: {payload.get('recovery_reason_summary') or ''}",
        "",
        "## Top Findings",
    ]
    for item in payload.get("top_findings") or []:
        lines.extend(
            [
                f"- [{item.get('category_label') or ''}] severity `{item.get('severity_score') or 0}`",
                f"  Summary: {item.get('summary') or ''}",
                f"  Fix: {item.get('recommended_fix') or ''}",
            ]
        )
    if not payload.get("top_findings"):
        lines.append("- None.")

    lines.extend(["", "## Existing Tasks"])
    for item in payload.get("existing_task_lines") or []:
        lines.append(f"- {item}")
    if not payload.get("existing_task_lines"):
        lines.append("- None.")

    lines.extend(["", "## Supporting Moves"])
    for item in payload.get("supporting_moves") or []:
        lines.append(f"- {item}")
    if not payload.get("supporting_moves"):
        lines.append("- None.")

    lines.append("")
    return "\n".join(lines)


def owner_board_markdown(owner_name: str, summary_row: Dict[str, Any], lead_rows: Sequence[Dict[str, Any]]) -> str:
    lines = [
        f"# Recovery Board: {owner_name}",
        "",
        "## Snapshot",
        f"- Leads: `{summary_row.get('lead_count') or 0}`",
        f"- Critical: `{summary_row.get('critical_lead_count') or 0}`",
        f"- High: `{summary_row.get('high_lead_count') or 0}`",
        f"- Due Today: `{summary_row.get('today_lead_count') or 0}`",
        f"- Due 48h: `{summary_row.get('forty_eight_hour_lead_count') or 0}`",
        f"- Waiting On Us: `{summary_row.get('waiting_on_us_count') or 0}`",
        f"- Focus: {summary_row.get('owner_focus') or ''}",
        "",
        "## Lead Queue",
    ]
    for row in lead_rows[:40]:
        lines.append(
            f"- `{row.get('recovery_priority_score') or 0}` | {row.get('lead_name') or ''} | "
            f"{row.get('recovery_due_bucket') or ''} | {row.get('primary_recovery_action') or ''}"
        )
    if not lead_rows:
        lines.append("- None.")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    output_dir = args.output_dir
    by_owner_dir = output_dir / "by_owner"
    ensure_dir(output_dir)
    ensure_dir(by_owner_dir)

    lead_signal_rows = {row["lead_id"]: row for row in load_csv_rows(args.miscommunication_signals_csv)}
    finding_rows = load_csv_rows(args.miscommunication_findings_csv)
    action_items = load_csv_rows(args.action_items_csv)
    owner_task_rows = load_csv_rows(args.owner_task_board_csv)
    customer_waiting_rows = load_csv_rows(args.customer_waiting_board_csv)
    promise_rows = load_csv_rows(args.promise_tracker_csv)

    findings_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in finding_rows:
        lead_id = row.get("lead_id") or ""
        if lead_id:
            findings_by_lead[lead_id].append(row)

    action_items_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    owner_tasks_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    customer_tasks_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    promise_tasks_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    for row in action_items:
        lead_id = row.get("lead_id") or ""
        if lead_id:
            action_items_by_lead[lead_id].append(row)
    for row in owner_task_rows:
        lead_id = row.get("lead_id") or ""
        if lead_id:
            owner_tasks_by_lead[lead_id].append(row)
    for row in customer_waiting_rows:
        lead_id = row.get("lead_id") or ""
        if lead_id:
            customer_tasks_by_lead[lead_id].append(row)
    for row in promise_rows:
        lead_id = row.get("lead_id") or ""
        if lead_id:
            promise_tasks_by_lead[lead_id].append(row)

    recovery_action_rows: List[Dict[str, Any]] = []
    lead_recovery_rows: List[Dict[str, Any]] = []

    for lead_id, signal_row in sorted(lead_signal_rows.items()):
        findings = sorted(findings_by_lead.get(lead_id, []), key=lambda row: (-safe_int(row.get("severity_score")), row.get("category") or ""))
        lead_dir = lead_dir_from_paths(
            signal_row.get("lead_miscommunication_audit_path") or "",
            signal_row.get("lead_miscommunication_audit_json_path") or "",
        )
        if lead_dir is None:
            continue

        top_findings = findings[:5]
        existing_owner_tasks = owner_tasks_by_lead.get(lead_id, [])
        existing_customer_tasks = customer_tasks_by_lead.get(lead_id, [])
        existing_promise_tasks = promise_tasks_by_lead.get(lead_id, [])
        all_existing_tasks = action_items_by_lead.get(lead_id, [])

        for finding in findings:
            severity = safe_int(finding.get("severity_score"))
            waiting_on_us = (finding.get("waiting_on_us") or "").lower() == "true"
            due_bucket, due_date_utc, due_reason = due_window(
                category=finding.get("category") or "",
                severity_score=severity,
                stage_type=finding.get("stage_type") or "",
                waiting_on_us=waiting_on_us,
            )
            board_group = BOARD_GROUPS.get(finding.get("category") or "", "recovery")
            recovery_action_rows.append(
                {
                    "lead_id": finding.get("lead_id") or "",
                    "lead_name": finding.get("lead_name") or "",
                    "lead_owner_name": finding.get("lead_owner_name") or "",
                    "stage_label": finding.get("stage_label") or "",
                    "stage_type": finding.get("stage_type") or "",
                    "category": finding.get("category") or "",
                    "category_label": finding.get("category_label") or "",
                    "board_group": board_group,
                    "recovery_owner_type": action_owner(finding.get("category") or ""),
                    "severity_score": severity,
                    "recovery_due_bucket": due_bucket,
                    "recovery_due_date_utc": due_date_utc,
                    "recovery_due_reason": due_reason,
                    "recovery_action": finding.get("recommended_fix") or "",
                    "summary": finding.get("summary") or "",
                    "evidence_text": finding.get("evidence_text") or "",
                    "existing_open_task_count": len(all_existing_tasks),
                    "existing_sales_task_count": len(existing_owner_tasks),
                    "existing_customer_task_count": len(existing_customer_tasks),
                    "existing_promise_task_count": len(existing_promise_tasks),
                    "source_path": finding.get("source_path") or "",
                    "lead_recovery_plan_path": str(lead_dir / "lead_recovery_plan.md"),
                }
            )

        highest_severity = safe_int(signal_row.get("highest_severity_score"))
        recovery_priority_score = highest_severity + safe_int(signal_row.get("audit_flag_count")) * 6
        if (signal_row.get("waiting_on_us") or "").lower() == "true":
            recovery_priority_score += 18
        recovery_priority_score += len(existing_promise_tasks) * 4

        primary_finding = top_findings[0] if top_findings else {}
        primary_recovery_action = primary_finding.get("recommended_fix") or "Review the audit findings and close the top issue first."
        primary_category = primary_finding.get("category") or ""
        recovery_due_bucket, recovery_due_date_utc, recovery_due_reason = due_window(
            category=primary_category,
            severity_score=highest_severity,
            stage_type=signal_row.get("stage_type") or "",
            waiting_on_us=(signal_row.get("waiting_on_us") or "").lower() == "true",
        )

        top_finding_payloads = [
            {
                "category_label": item.get("category_label") or "",
                "severity_score": safe_int(item.get("severity_score")),
                "summary": item.get("summary") or "",
                "recommended_fix": item.get("recommended_fix") or "",
            }
            for item in top_findings
        ]
        existing_task_lines: List[str] = []
        for item in (existing_owner_tasks + existing_promise_tasks + existing_customer_tasks)[:6]:
            existing_task_lines.append(
                compact_text(
                    f"[{item.get('task_kind') or item.get('task_owner_type') or 'task'}] {item.get('task_text') or ''} "
                    f"| due {item.get('due_bucket') or ''}",
                    300,
                )
            )
        supporting_moves = [item.get("recommended_fix") or "" for item in top_findings[1:4] if item.get("recommended_fix")]

        lead_row = {
            "lead_id": lead_id,
            "lead_name": signal_row.get("lead_name") or "",
            "lead_owner_name": signal_row.get("lead_owner_name") or "",
            "pipeline_name": signal_row.get("pipeline_name") or "",
            "stage_label": signal_row.get("stage_label") or "",
            "stage_type": signal_row.get("stage_type") or "",
            "primary_source_channel": signal_row.get("primary_source_channel") or "",
            "waiting_on_us": signal_row.get("waiting_on_us") or "",
            "audit_flag_count": safe_int(signal_row.get("audit_flag_count")),
            "highest_severity_score": highest_severity,
            "top_category_label": signal_row.get("top_category_label") or "",
            "recovery_priority_score": recovery_priority_score,
            "recovery_priority_band": priority_band(recovery_priority_score),
            "recovery_due_bucket": recovery_due_bucket,
            "recovery_due_date_utc": recovery_due_date_utc,
            "recovery_due_reason": recovery_due_reason,
            "primary_recovery_action": compact_text(primary_recovery_action, 300),
            "recovery_reason_summary": compact_text(signal_row.get("audit_summary") or primary_finding.get("summary") or "", 360),
            "existing_open_task_count": len(all_existing_tasks),
            "existing_sales_task_count": len(existing_owner_tasks),
            "existing_customer_task_count": len(existing_customer_tasks),
            "existing_promise_task_count": len(existing_promise_tasks),
            "lead_recovery_plan_path": str(lead_dir / "lead_recovery_plan.md"),
            "lead_recovery_plan_json_path": str(lead_dir / "lead_recovery_plan.json"),
        }

        payload = dict(lead_row)
        payload["top_findings"] = top_finding_payloads
        payload["existing_task_lines"] = existing_task_lines
        payload["supporting_moves"] = supporting_moves
        write_json(lead_dir / "lead_recovery_plan.json", payload)
        (lead_dir / "lead_recovery_plan.md").write_text(lead_recovery_markdown(payload), encoding="utf-8")
        lead_recovery_rows.append(lead_row)

    lead_recovery_rows = sorted(
        lead_recovery_rows,
        key=lambda row: (
            -safe_int(str(row.get("recovery_priority_score") or 0)),
            -safe_int(str(row.get("audit_flag_count") or 0)),
            row.get("lead_name") or "",
        ),
    )
    queued_lead_rows = [row for row in lead_recovery_rows if safe_int(str(row.get("audit_flag_count") or 0)) > 0]
    recovery_action_rows = sorted(
        recovery_action_rows,
        key=lambda row: (
            -safe_int(str(row.get("severity_score") or 0)),
            row.get("lead_name") or "",
            row.get("category") or "",
        ),
    )

    owner_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in queued_lead_rows:
        owner_groups[(row.get("lead_owner_name") or "Unassigned").strip() or "Unassigned"].append(row)

    owner_summary_rows: List[Dict[str, Any]] = []
    for owner_name, rows in sorted(owner_groups.items(), key=lambda item: (-len(item[1]), item[0].lower())):
        critical_count = sum(1 for row in rows if row.get("recovery_priority_band") == "critical")
        high_count = sum(1 for row in rows if row.get("recovery_priority_band") == "high")
        today_count = sum(1 for row in rows if row.get("recovery_due_bucket") == "today")
        forty_eight_count = sum(1 for row in rows if row.get("recovery_due_bucket") == "48h")
        waiting_count = sum(1 for row in rows if (row.get("waiting_on_us") or "").lower() == "true")
        top_categories = Counter((row.get("top_category_label") or "Unknown").strip() or "Unknown" for row in rows)
        owner_focus = compact_text(
            f"Top issue mix: {' | '.join(f'{name} ({count})' for name, count in top_categories.most_common(3))}. "
            f"Critical {critical_count}, high {high_count}, due today {today_count}, waiting on us {waiting_count}.",
            320,
        )
        owner_dir = by_owner_dir / slugify(owner_name)
        ensure_dir(owner_dir)
        owner_row = {
            "owner_name": owner_name,
            "lead_count": len(rows),
            "critical_lead_count": critical_count,
            "high_lead_count": high_count,
            "today_lead_count": today_count,
            "forty_eight_hour_lead_count": forty_eight_count,
            "waiting_on_us_count": waiting_count,
            "average_recovery_priority_score": f"{(sum(safe_int(str(row.get('recovery_priority_score') or 0)) for row in rows) / len(rows)):.1f}",
            "top_categories": " | ".join(f"{name} ({count})" for name, count in top_categories.most_common(3)),
            "owner_focus": owner_focus,
            "board_path": str(owner_dir / "board.md"),
        }
        (owner_dir / "board.md").write_text(owner_board_markdown(owner_name, owner_row, rows), encoding="utf-8")
        write_csv(owner_dir / "lead_recovery_queue.csv", rows)
        owner_summary_rows.append(owner_row)

    owner_summary_rows = sorted(
        owner_summary_rows,
        key=lambda row: (
            -safe_int(str(row.get("critical_lead_count") or 0)),
            -safe_int(str(row.get("high_lead_count") or 0)),
            row.get("owner_name") or "",
        ),
    )

    same_day_rows = [row for row in queued_lead_rows if row.get("recovery_due_bucket") == "today"]
    commercial_rows = [row for row in queued_lead_rows if row.get("top_category_label") == "Commercial Alignment Risk"]
    decision_chain_rows = [row for row in queued_lead_rows if row.get("top_category_label") == "Decision-Chain Stall"]
    venue_date_rows = [
        row for row in queued_lead_rows
        if row.get("top_category_label") in {"Venue Alignment Gap", "Uncertain Event Date", "Menu / Ops Detail Gap"}
    ]
    won_reconfirmation_rows = [row for row in queued_lead_rows if row.get("top_category_label") == "Won Carryover Watch"]

    write_csv(normalized_dir / "lead_recovery_queue.csv", queued_lead_rows)
    write_jsonl(normalized_dir / "lead_recovery_queue.jsonl", queued_lead_rows)
    write_csv(normalized_dir / "recovery_actions.csv", recovery_action_rows)
    write_jsonl(normalized_dir / "recovery_actions.jsonl", recovery_action_rows)
    write_csv(normalized_dir / "owner_recovery_board.csv", owner_summary_rows)
    write_csv(normalized_dir / "same_day_recovery_queue.csv", same_day_rows)
    write_csv(normalized_dir / "commercial_recovery_board.csv", commercial_rows)
    write_csv(normalized_dir / "decision_chain_recovery_board.csv", decision_chain_rows)
    write_csv(normalized_dir / "venue_date_recovery_board.csv", venue_date_rows)
    write_csv(normalized_dir / "won_reconfirmation_board.csv", won_reconfirmation_rows)

    (output_dir / "README.md").write_text(
        "\n".join(
            [
                "# Recovery Intelligence",
                "",
                "This layer turns the miscommunication audit into ranked recovery work: which leads to rescue first, what the primary fix is, and which owner is carrying the most recovery pressure.",
                "",
                "## Snapshot",
                f"- Lead recovery plans: `{len(lead_recovery_rows)}`",
                f"- Flagged lead queue rows: `{len(queued_lead_rows)}`",
                f"- Recovery actions: `{len(recovery_action_rows)}`",
                f"- Owner recovery rows: `{len(owner_summary_rows)}`",
                "",
                "## Key Files",
                "- `owner_recovery_overview.md`: owner-by-owner rescue scan",
                "- `recovery_action_board.md`: ranked all-lead recovery board",
                "- `same_day_recovery_queue.md`: fixes that really should happen today",
                "- `commercial_recovery_board.md`: budget / quote / scope rescue board",
                "- `decision_chain_recovery_board.md`: multi-decision-maker rescue board",
                "- `venue_date_recovery_board.md`: venue / date / menu-handoff rescue board",
                "- `won_reconfirmation_board.md`: booked deals that still deserve a cleanup pass",
                "- `../normalized/lead_recovery_queue.csv`: one-row-per-lead recovery queue",
                "- `../normalized/recovery_actions.csv`: one-row-per-recovery-action log",
                "- `../normalized/owner_recovery_board.csv`: one-row-per-owner recovery summary",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (output_dir / "owner_recovery_overview.md").write_text(owner_overview_markdown(owner_summary_rows), encoding="utf-8")
    (output_dir / "recovery_action_board.md").write_text(board_markdown("Recovery Action Board", queued_lead_rows), encoding="utf-8")
    (output_dir / "same_day_recovery_queue.md").write_text(board_markdown("Same-Day Recovery Queue", same_day_rows), encoding="utf-8")
    (output_dir / "commercial_recovery_board.md").write_text(board_markdown("Commercial Recovery Board", commercial_rows), encoding="utf-8")
    (output_dir / "decision_chain_recovery_board.md").write_text(board_markdown("Decision-Chain Recovery Board", decision_chain_rows), encoding="utf-8")
    (output_dir / "venue_date_recovery_board.md").write_text(board_markdown("Venue / Date Recovery Board", venue_date_rows), encoding="utf-8")
    (output_dir / "won_reconfirmation_board.md").write_text(board_markdown("Won Reconfirmation Board", won_reconfirmation_rows), encoding="utf-8")

    print(
        json.dumps(
            {
                "lead_recovery_plans": len(lead_recovery_rows),
                "queued_leads": len(queued_lead_rows),
                "recovery_actions": len(recovery_action_rows),
                "owner_rows": len(owner_summary_rows),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
