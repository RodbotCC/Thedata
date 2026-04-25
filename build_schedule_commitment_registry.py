#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_ACTION_ITEMS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "action_items.csv"
DEFAULT_PROMISE_TRACKER_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "promise_tracker.csv"
DEFAULT_OPEN_LOOPS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "open_loops.csv"
DEFAULT_EVENT_OPS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_event_ops_registry.csv"
DEFAULT_FUTURE_EVENT_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "future_event_calendar.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "schedule_commitment_registry"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a schedule / commitment registry from normalized Comeketo action, promise, and event layers.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--action-items-csv", type=Path, default=DEFAULT_ACTION_ITEMS_CSV)
    parser.add_argument("--promise-tracker-csv", type=Path, default=DEFAULT_PROMISE_TRACKER_CSV)
    parser.add_argument("--open-loops-csv", type=Path, default=DEFAULT_OPEN_LOOPS_CSV)
    parser.add_argument("--event-ops-csv", type=Path, default=DEFAULT_EVENT_OPS_CSV)
    parser.add_argument("--future-event-csv", type=Path, default=DEFAULT_FUTURE_EVENT_CSV)
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


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def iso_z(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_text(value: Optional[str], limit: int = 280) -> str:
    text = " ".join((value or "").split())
    return text[:limit]


def pretty_label(value: str) -> str:
    return value.replace("_", " ")


def due_bucket_rank(bucket: str) -> int:
    order = {"today": 0, "48h": 1, "3d": 2, "7d": 3, "14d": 4}
    return order.get(bucket or "", 99)


def normalize_schedule_state(row: Dict[str, str]) -> str:
    source = row.get("event_datetime_source") or ""
    planning_horizon = row.get("planning_horizon") or ""
    days_until = row.get("days_until_event") or ""
    try:
        days_until_int = int(days_until)
    except ValueError:
        days_until_int = None

    if not row.get("event_datetime_utc"):
        return "no_event_date"
    if source == "close_date_won_fallback":
        if planning_horizon == "past" or (days_until_int is not None and days_until_int < 0):
            return "stale_uncertain_date"
        return "uncertain_fallback_date"
    if days_until_int is not None and days_until_int < 0:
        return "past_event_date"
    if days_until_int is not None and days_until_int <= 7:
        return "event_this_week"
    if days_until_int is not None and days_until_int <= 30:
        return "event_this_month"
    return "future_scheduled_event"


def schedule_summary_line(
    lead_name: str,
    due_today: int,
    due_48h: int,
    pending_promises: int,
    open_questions: int,
    schedule_state: str,
    event_label: str,
    next_due_label: str,
) -> str:
    return compact_text(
        f"{lead_name}: due today {due_today}; due within 48h {due_48h}; pending promises {pending_promises}; "
        f"open buyer questions {open_questions}; schedule state {pretty_label(schedule_state)}; "
        f"next deadline {next_due_label or 'none'}; event {event_label or 'unknown'}.",
        420,
    )


def lead_dir_from_source(*paths: str) -> Optional[Path]:
    for raw_path in paths:
        if not raw_path:
            continue
        path = Path(raw_path)
        if path.name in {
            "lead_deal_sheet.md",
            "lead_deal_sheet.json",
            "lead_event_ops_sheet.md",
            "lead_event_ops_sheet.json",
            "lead_memory_brief.md",
            "lead_action_plan.md",
            "lead_action_plan.json",
        }:
            return path.parent
    return None


def build_timeline_rows(
    lead: Dict[str, str],
    action_rows: Sequence[Dict[str, str]],
    promise_rows: Sequence[Dict[str, str]],
    event_row: Dict[str, str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for row in sorted(action_rows, key=lambda item: (item.get("due_date_utc") or "", due_bucket_rank(item.get("due_bucket") or ""))):
        rows.append(
            {
                "lead_id": row.get("lead_id") or "",
                "lead_name": row.get("lead_name") or "",
                "lead_owner_name": row.get("lead_owner_name") or "",
                "stage_label": row.get("stage_label") or "",
                "scheduled_for_utc": row.get("due_date_utc") or "",
                "scheduled_month": (row.get("due_date_utc") or "")[:7],
                "timeline_kind": row.get("task_kind") or "task",
                "timeline_bucket": row.get("due_bucket") or "",
                "timeline_status": row.get("status") or "",
                "task_owner_type": row.get("task_owner_type") or "",
                "task_owner_name": row.get("task_owner_name") or "",
                "event_datetime_utc": row.get("event_datetime_utc") or "",
                "event_reference": "deadline",
                "priority_score": row.get("readiness_score") or "",
                "summary": row.get("task_text") or "",
                "source_text": row.get("source_text") or "",
                "source_path": row.get("source_path") or "",
            }
        )

    promise_keys = {
        (
            row.get("lead_id") or "",
            row.get("task_category") or "",
            row.get("due_date_utc") or "",
            row.get("source_path") or "",
        )
        for row in promise_rows
    }
    for row in sorted(promise_rows, key=lambda item: item.get("due_date_utc") or ""):
        rows.append(
            {
                "lead_id": row.get("lead_id") or "",
                "lead_name": row.get("lead_name") or "",
                "lead_owner_name": row.get("lead_owner_name") or "",
                "stage_label": row.get("stage_label") or "",
                "scheduled_for_utc": row.get("due_date_utc") or "",
                "scheduled_month": (row.get("due_date_utc") or "")[:7],
                "timeline_kind": "promise_due",
                "timeline_bucket": row.get("due_bucket") or "",
                "timeline_status": row.get("status") or "",
                "task_owner_type": row.get("task_owner_type") or "",
                "task_owner_name": row.get("task_owner_name") or "",
                "event_datetime_utc": row.get("event_datetime_utc") or "",
                "event_reference": "promise",
                "priority_score": row.get("readiness_score") or "",
                "summary": row.get("task_text") or "",
                "source_text": row.get("source_text") or "",
                "source_path": row.get("source_path") or "",
            }
        )

    if event_row.get("event_datetime_utc"):
        schedule_state = normalize_schedule_state(event_row)
        event_label = compact_text(event_row.get("ops_watch_reason") or event_row.get("venue_summary") or "", 320)
        rows.append(
            {
                "lead_id": lead.get("lead_id") or "",
                "lead_name": lead.get("lead_name") or "",
                "lead_owner_name": lead.get("lead_owner_name") or "",
                "stage_label": lead.get("stage_label") or "",
                "scheduled_for_utc": event_row.get("event_datetime_utc") or "",
                "scheduled_month": (event_row.get("event_datetime_utc") or "")[:7],
                "timeline_kind": "event_date",
                "timeline_bucket": event_row.get("planning_horizon") or "",
                "timeline_status": schedule_state,
                "task_owner_type": "",
                "task_owner_name": lead.get("lead_owner_name") or "",
                "event_datetime_utc": event_row.get("event_datetime_utc") or "",
                "event_reference": event_row.get("event_datetime_source") or "",
                "priority_score": event_row.get("readiness_score") or "",
                "summary": compact_text(
                    f"Event date: {event_row.get('event_datetime_utc') or ''} | {event_row.get('ops_summary') or event_label}",
                    500,
                ),
                "source_text": event_label,
                "source_path": event_row.get("lead_event_ops_sheet_path") or event_row.get("deal_sheet_path") or "",
            }
        )

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            row.get("lead_id") or "",
            row.get("timeline_kind") or "",
            row.get("scheduled_for_utc") or "",
            row.get("summary") or "",
            row.get("source_path") or "",
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return sorted(deduped, key=lambda row: (row.get("scheduled_for_utc") or "", row.get("timeline_kind") or ""))


def build_profile_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Schedule / Commitment Sheet: {payload.get('lead_name') or ''}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Pipeline / Stage: {payload.get('pipeline_name') or ''} / {payload.get('stage_label') or ''}",
        f"- Schedule Pressure Score: `{payload.get('schedule_pressure_score') or ''}`",
        f"- Schedule State: {pretty_label(payload.get('schedule_state') or '')}",
        f"- Event Date (UTC): `{payload.get('event_datetime_utc') or ''}`",
        f"- Event Date Source: {payload.get('event_datetime_source') or ''}",
        f"- Planning Horizon: {payload.get('planning_horizon') or ''}",
        f"- Days Until Event: `{payload.get('days_until_event') or ''}`",
        f"- Event Watch Reason: {payload.get('event_watch_reason') or 'None.'}",
        "",
        "## Counts",
        f"- Open Tasks: `{payload.get('open_task_count', 0)}`",
        f"- Pending Promises: `{payload.get('pending_promise_count', 0)}`",
        f"- Open Buyer Questions: `{payload.get('open_question_count', 0)}`",
        f"- Due Today: `{payload.get('due_today_count', 0)}`",
        f"- Due Within 48h: `{payload.get('due_48h_count', 0)}`",
        f"- Due Within 7d: `{payload.get('due_7d_count', 0)}`",
        "",
        "## Next Deadline",
        f"- {payload.get('next_due_summary') or 'None.'}",
        "",
        "## Upcoming Deadlines",
    ]
    upcoming_deadlines = payload.get("upcoming_deadlines") or []
    if upcoming_deadlines:
        for row in upcoming_deadlines:
            lines.append(
                f"- `{row.get('due_date_utc') or ''}` | {row.get('task_owner_name') or ''} | "
                f"{row.get('task_category') or ''} | {row.get('task_text') or ''}"
            )
    else:
        lines.append("- None.")

    lines.extend(["", "## Pending Promises"])
    pending_promises = payload.get("pending_promises") or []
    if pending_promises:
        for row in pending_promises:
            lines.append(
                f"- `{row.get('due_date_utc') or ''}` | {row.get('task_category') or ''} | "
                f"{row.get('source_text') or row.get('task_text') or ''}"
            )
    else:
        lines.append("- None.")

    lines.extend(["", "## Open Buyer Questions"])
    open_questions = payload.get("open_buyer_questions") or []
    if open_questions:
        for row in open_questions:
            lines.append(
                f"- `{row.get('event_datetime_utc') or ''}` | {row.get('category') or ''} | {row.get('text') or ''}"
            )
    else:
        lines.append("- None.")

    lines.extend(["", "## Timeline"])
    timeline_rows = payload.get("timeline_rows_preview") or []
    if timeline_rows:
        for row in timeline_rows:
            lines.append(
                f"- `{row.get('scheduled_for_utc') or ''}` | {row.get('timeline_kind') or ''} | {row.get('summary') or ''}"
            )
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "## Summary",
            f"- {payload.get('schedule_summary') or ''}",
            "",
        ]
    )
    return "\n".join(lines)


def build_board_markdown(title: str, rows: Sequence[Dict[str, Any]], count_label: str, column: str) -> str:
    lines = [f"# {title}", "", f"- Total {count_label}: `{len(rows)}`", "", "## Top Rows"]
    for row in rows[:120]:
        primary_text = (
            row.get(column)
            or row.get("next_due_summary")
            or row.get("schedule_summary")
            or row.get("event_watch_reason")
            or "No explicit schedule item captured."
        )
        lines.append(
            f"- `{row.get('schedule_pressure_score') or row.get('priority_score') or ''}` | "
            f"{row.get('lead_name') or ''} | {row.get('lead_owner_name') or ''} | "
            f"{row.get('stage_label') or ''} | {primary_text}"
        )
    lines.append("")
    return "\n".join(lines)


def build_owner_rollup_markdown(rows: Sequence[Dict[str, Any]]) -> str:
    lines = ["# Owner Schedule Rollup", "", f"- Owners: `{len(rows)}`", "", "## Rollup"]
    for row in rows:
        lines.append(
            f"- {row.get('lead_owner_name') or ''}: due today {row.get('due_today_count') or 0}; "
            f"due 48h {row.get('due_48h_count') or 0}; pending promises {row.get('pending_promise_count') or 0}; "
            f"upcoming events 30d {row.get('events_30d_count') or 0}"
        )
    lines.append("")
    return "\n".join(lines)


def build_readme(
    profiles: Sequence[Dict[str, Any]],
    immediate_rows: Sequence[Dict[str, Any]],
    promise_rows: Sequence[Dict[str, Any]],
    upcoming_event_rows: Sequence[Dict[str, Any]],
    stale_rows: Sequence[Dict[str, Any]],
    owner_rows: Sequence[Dict[str, Any]],
    month_count: int,
) -> str:
    lines = [
        "# Schedule / Commitment Registry",
        "",
        "This layer compresses timing and follow-through into a single schedule view: open tasks, pending promises, buyer questions, event dates, and deadline pressure.",
        "",
        "## Snapshot",
        f"- Lead schedule sheets: `{len(profiles)}`",
        f"- Immediate deadline rows: `{len(immediate_rows)}`",
        f"- Promise-due rows: `{len(promise_rows)}`",
        f"- Upcoming-event rows: `{len(upcoming_event_rows)}`",
        f"- Uncertain-date rows: `{len(stale_rows)}`",
        f"- Owner rollup rows: `{len(owner_rows)}`",
        f"- Month folders: `{month_count}`",
        "",
        "## Key Files",
        "- `immediate_deadline_board.md`: leads with deadlines already due or due inside 48 hours",
        "- `promise_due_board.md`: pending salesperson promises that still need follow-through",
        "- `upcoming_event_commitment_board.md`: future event leads with attached deadline pressure",
        "- `uncertain_event_date_board.md`: leads whose event date should not be trusted as-is",
        "- `owner_schedule_rollup.md`: one-row-per-owner timing pressure view",
        "- `../normalized/lead_schedule_commitments.csv`: machine-friendly one-row-per-lead schedule profile",
        "- `../normalized/commitment_timeline.csv`: machine-friendly unified deadline/event timeline",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    output_dir = args.output_dir
    by_month_dir = output_dir / "by_month"
    ensure_dir(output_dir)
    ensure_dir(by_month_dir)

    action_rows = [row for row in load_csv_rows(args.action_items_csv) if (row.get("status") or "").lower() == "open"]
    promise_rows = [row for row in load_csv_rows(args.promise_tracker_csv) if (row.get("status") or "").lower() in {"open", "pending"}]
    open_loops = [row for row in load_csv_rows(args.open_loops_csv) if (row.get("loop_status") or "").lower() in {"open", "pending"}]
    event_rows = {row["lead_id"]: row for row in load_csv_rows(args.event_ops_csv)}
    future_event_rows = {row["lead_id"]: row for row in load_csv_rows(args.future_event_csv)}

    actions_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    promises_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    loops_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    for row in action_rows:
        actions_by_lead[row.get("lead_id") or ""].append(row)
    for row in promise_rows:
        promises_by_lead[row.get("lead_id") or ""].append(row)
    for row in open_loops:
        loops_by_lead[row.get("lead_id") or ""].append(row)

    lead_ids = set(event_rows.keys()) | set(actions_by_lead.keys()) | set(promises_by_lead.keys()) | set(loops_by_lead.keys())

    profile_rows: List[Dict[str, Any]] = []
    profile_payloads: List[Dict[str, Any]] = []
    timeline_rows: List[Dict[str, Any]] = []

    now_utc = datetime.now(timezone.utc)

    for lead_id in sorted(lead_ids):
        event_row = event_rows.get(lead_id, {})
        future_event_row = future_event_rows.get(lead_id, {})
        lead_actions = sorted(
            actions_by_lead.get(lead_id, []),
            key=lambda row: (row.get("due_date_utc") or "", due_bucket_rank(row.get("due_bucket") or "")),
        )
        lead_promises = sorted(
            promises_by_lead.get(lead_id, []),
            key=lambda row: (row.get("due_date_utc") or "", due_bucket_rank(row.get("due_bucket") or "")),
        )
        lead_loops = sorted(
            loops_by_lead.get(lead_id, []),
            key=lambda row: row.get("priority_score") or "",
            reverse=True,
        )

        lead_name = event_row.get("lead_name") or (lead_actions[0].get("lead_name") if lead_actions else "") or (lead_promises[0].get("lead_name") if lead_promises else "") or (lead_loops[0].get("lead_name") if lead_loops else "")
        lead_owner_name = event_row.get("lead_owner_name") or (lead_actions[0].get("lead_owner_name") if lead_actions else "") or (lead_promises[0].get("lead_owner_name") if lead_promises else "") or (lead_loops[0].get("lead_owner_name") if lead_loops else "")
        stage_label = event_row.get("stage_label") or (lead_actions[0].get("stage_label") if lead_actions else "") or (lead_promises[0].get("stage_label") if lead_promises else "") or (lead_loops[0].get("stage_label") if lead_loops else "")
        pipeline_name = event_row.get("pipeline_name") or ""

        lead_dir = lead_dir_from_source(
            event_row.get("deal_sheet_path") or "",
            event_row.get("lead_event_ops_sheet_path") or "",
            lead_actions[0].get("source_path") if lead_actions else "",
            lead_promises[0].get("source_path") if lead_promises else "",
        )
        if lead_dir is None:
            continue

        due_today_count = sum(1 for row in lead_actions if row.get("due_bucket") == "today")
        due_48h_count = sum(1 for row in lead_actions if row.get("due_bucket") in {"today", "48h"})
        due_7d_count = sum(1 for row in lead_actions if row.get("due_bucket") in {"today", "48h", "3d", "7d"})
        open_question_rows = [row for row in lead_loops if row.get("loop_type") == "buyer_question"]
        pending_promise_count = len(lead_promises)
        schedule_state = normalize_schedule_state(future_event_row or event_row)

        next_due_row = lead_actions[0] if lead_actions else None
        next_due_label = ""
        if next_due_row:
            next_due_label = compact_text(
                f"{next_due_row.get('due_date_utc') or ''} | {next_due_row.get('task_owner_name') or ''} | {next_due_row.get('task_text') or ''}",
                320,
            )

        event_dt = parse_iso((future_event_row or event_row).get("event_datetime_utc") or "")
        days_until_event = ""
        if event_dt:
            days_until_event = str((event_dt.date() - now_utc.date()).days)

        schedule_pressure_score = (
            due_today_count * 6
            + max(due_48h_count - due_today_count, 0) * 4
            + max(due_7d_count - due_48h_count, 0) * 2
            + pending_promise_count * 3
            + len(open_question_rows) * 2
            + (3 if schedule_state == "event_this_week" else 2 if schedule_state == "event_this_month" else 0)
        )

        timeline_preview = build_timeline_rows(
            {
                "lead_id": lead_id,
                "lead_name": lead_name,
                "lead_owner_name": lead_owner_name,
                "stage_label": stage_label,
            },
            lead_actions,
            lead_promises,
            future_event_row or event_row,
        )
        timeline_rows.extend(timeline_preview)

        profile_row: Dict[str, Any] = {
            "lead_id": lead_id,
            "lead_name": lead_name,
            "lead_owner_name": lead_owner_name,
            "pipeline_name": pipeline_name,
            "stage_label": stage_label,
            "schedule_pressure_score": schedule_pressure_score,
            "schedule_state": schedule_state,
            "event_datetime_utc": (future_event_row or event_row).get("event_datetime_utc") or "",
            "event_datetime_source": (future_event_row or event_row).get("event_datetime_source") or "",
            "planning_horizon": (future_event_row or event_row).get("planning_horizon") or "",
            "days_until_event": days_until_event or ((future_event_row or event_row).get("days_until_event") or ""),
            "event_watch_reason": (future_event_row or event_row).get("ops_watch_reason") or "",
            "open_task_count": len(lead_actions),
            "pending_promise_count": pending_promise_count,
            "open_question_count": len(open_question_rows),
            "due_today_count": due_today_count,
            "due_48h_count": due_48h_count,
            "due_7d_count": due_7d_count,
            "next_due_date_utc": next_due_row.get("due_date_utc") if next_due_row else "",
            "next_due_summary": next_due_label,
            "lead_schedule_commitment_sheet_path": str(lead_dir / "lead_schedule_commitment_sheet.md"),
            "lead_schedule_commitment_sheet_json_path": str(lead_dir / "lead_schedule_commitment_sheet.json"),
            "action_plan_path": (future_event_row or event_row).get("action_plan_path") or "",
            "event_ops_path": (future_event_row or event_row).get("lead_event_ops_sheet_path") or "",
        }

        payload = dict(profile_row)
        payload.update(
            {
                "upcoming_deadlines": lead_actions[:8],
                "pending_promises": lead_promises[:8],
                "open_buyer_questions": open_question_rows[:8],
                "timeline_rows_preview": timeline_preview[:10],
                "schedule_summary": schedule_summary_line(
                    lead_name=lead_name,
                    due_today=due_today_count,
                    due_48h=due_48h_count,
                    pending_promises=pending_promise_count,
                    open_questions=len(open_question_rows),
                    schedule_state=schedule_state,
                    event_label=(future_event_row or event_row).get("event_datetime_utc") or "",
                    next_due_label=next_due_label,
                ),
            }
        )

        write_json(lead_dir / "lead_schedule_commitment_sheet.json", payload)
        (lead_dir / "lead_schedule_commitment_sheet.md").write_text(build_profile_markdown(payload), encoding="utf-8")

        profile_rows.append(profile_row)
        profile_payloads.append(payload)

    profile_rows = sorted(profile_rows, key=lambda row: (-int(row.get("schedule_pressure_score") or 0), row.get("next_due_date_utc") or ""))

    timeline_rows = sorted(
        timeline_rows,
        key=lambda row: (row.get("scheduled_for_utc") or "", row.get("lead_name") or "", row.get("timeline_kind") or ""),
    )

    immediate_rows = [
        row
        for row in profile_rows
        if int(row.get("due_today_count") or 0) > 0 or int(row.get("due_48h_count") or 0) > 0
    ]
    promise_due_rows = [row for row in profile_rows if int(row.get("pending_promise_count") or 0) > 0]
    upcoming_event_rows = [
        row
        for row in profile_rows
        if row.get("schedule_state") in {"event_this_week", "event_this_month", "future_scheduled_event"}
        and row.get("event_datetime_utc")
    ]
    stale_rows = [
        row
        for row in profile_rows
        if row.get("schedule_state") in {"stale_uncertain_date", "uncertain_fallback_date"}
    ]

    owner_rollup_map: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "lead_owner_name": "",
            "lead_count": 0,
            "due_today_count": 0,
            "due_48h_count": 0,
            "pending_promise_count": 0,
            "events_30d_count": 0,
            "total_schedule_pressure_score": 0,
        }
    )
    for row in profile_rows:
        owner = row.get("lead_owner_name") or "Unknown Owner"
        bucket = owner_rollup_map[owner]
        bucket["lead_owner_name"] = owner
        bucket["lead_count"] += 1
        bucket["due_today_count"] += int(row.get("due_today_count") or 0)
        bucket["due_48h_count"] += int(row.get("due_48h_count") or 0)
        bucket["pending_promise_count"] += int(row.get("pending_promise_count") or 0)
        if row.get("schedule_state") in {"event_this_week", "event_this_month"}:
            bucket["events_30d_count"] += 1
        bucket["total_schedule_pressure_score"] += int(row.get("schedule_pressure_score") or 0)
    owner_rows = sorted(
        owner_rollup_map.values(),
        key=lambda row: (
            -int(row.get("due_today_count") or 0),
            -int(row.get("due_48h_count") or 0),
            -int(row.get("pending_promise_count") or 0),
            row.get("lead_owner_name") or "",
        ),
    )

    month_rows = [
        row
        for row in timeline_rows
        if row.get("scheduled_for_utc")
        and parse_iso(row.get("scheduled_for_utc") or "")
        and parse_iso(row.get("scheduled_for_utc") or "") >= now_utc
    ]
    by_month: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in month_rows:
        month = row.get("scheduled_month") or "unknown"
        by_month[month].append(row)

    for month, rows in sorted(by_month.items()):
        month_dir = by_month_dir / month
        ensure_dir(month_dir)
        lines = [f"# {month} Schedule", "", f"- Rows: `{len(rows)}`", "", "## Timeline"]
        for row in rows[:300]:
            lines.append(
                f"- `{row.get('scheduled_for_utc') or ''}` | {row.get('lead_name') or ''} | "
                f"{row.get('timeline_kind') or ''} | {row.get('summary') or ''}"
            )
        lines.append("")
        (month_dir / "calendar.md").write_text("\n".join(lines), encoding="utf-8")

    write_csv(normalized_dir / "lead_schedule_commitments.csv", profile_rows)
    write_jsonl(normalized_dir / "lead_schedule_commitments.jsonl", profile_rows)
    write_csv(normalized_dir / "commitment_timeline.csv", timeline_rows)
    write_jsonl(normalized_dir / "commitment_timeline.jsonl", timeline_rows)
    write_csv(normalized_dir / "immediate_deadline_board.csv", immediate_rows)
    write_jsonl(normalized_dir / "immediate_deadline_board.jsonl", immediate_rows)
    write_csv(normalized_dir / "promise_due_board.csv", promise_due_rows)
    write_jsonl(normalized_dir / "promise_due_board.jsonl", promise_due_rows)
    write_csv(normalized_dir / "upcoming_event_commitment_board.csv", upcoming_event_rows)
    write_jsonl(normalized_dir / "upcoming_event_commitment_board.jsonl", upcoming_event_rows)
    write_csv(normalized_dir / "uncertain_event_date_board.csv", stale_rows)
    write_jsonl(normalized_dir / "uncertain_event_date_board.jsonl", stale_rows)
    write_csv(normalized_dir / "owner_schedule_rollup.csv", owner_rows)

    (output_dir / "README.md").write_text(
        build_readme(profile_rows, immediate_rows, promise_due_rows, upcoming_event_rows, stale_rows, owner_rows, len(by_month)),
        encoding="utf-8",
    )
    (output_dir / "immediate_deadline_board.md").write_text(
        build_board_markdown("Immediate Deadline Board", immediate_rows, "immediate-deadline rows", "next_due_summary"),
        encoding="utf-8",
    )
    (output_dir / "promise_due_board.md").write_text(
        build_board_markdown("Promise Due Board", promise_due_rows, "promise-due rows", "next_due_summary"),
        encoding="utf-8",
    )
    (output_dir / "upcoming_event_commitment_board.md").write_text(
        build_board_markdown("Upcoming Event Commitment Board", upcoming_event_rows, "upcoming-event rows", "event_watch_reason"),
        encoding="utf-8",
    )
    (output_dir / "uncertain_event_date_board.md").write_text(
        build_board_markdown("Uncertain Event Date Board", stale_rows, "uncertain-date rows", "event_watch_reason"),
        encoding="utf-8",
    )
    (output_dir / "owner_schedule_rollup.md").write_text(build_owner_rollup_markdown(owner_rows), encoding="utf-8")

    print(
        json.dumps(
            {
                "profiles": len(profile_rows),
                "timeline_rows": len(timeline_rows),
                "immediate_rows": len(immediate_rows),
                "promise_due_rows": len(promise_due_rows),
                "upcoming_event_rows": len(upcoming_event_rows),
                "uncertain_rows": len(stale_rows),
                "owner_rows": len(owner_rows),
                "month_folders": len(by_month),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
