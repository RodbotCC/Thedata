#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_DEAL_SHEETS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_deal_sheets.csv"
DEFAULT_CONVERSATION_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_conversation_intelligence.csv"
DEFAULT_FOLLOW_UP_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "follow_up_queue.csv"
DEFAULT_COMMUNICATIONS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_communications.csv"
DEFAULT_PRICING_SCOPE_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_pricing_scope_profiles.csv"
DEFAULT_SCHEDULE_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_schedule_commitments.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "seller_performance_intelligence"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build seller-performance and conversation-quality intelligence from normalized Comeketo layers.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--deal-sheets-csv", type=Path, default=DEFAULT_DEAL_SHEETS_CSV)
    parser.add_argument("--conversation-csv", type=Path, default=DEFAULT_CONVERSATION_CSV)
    parser.add_argument("--follow-up-csv", type=Path, default=DEFAULT_FOLLOW_UP_CSV)
    parser.add_argument("--communications-csv", type=Path, default=DEFAULT_COMMUNICATIONS_CSV)
    parser.add_argument("--pricing-scope-csv", type=Path, default=DEFAULT_PRICING_SCOPE_CSV)
    parser.add_argument("--schedule-csv", type=Path, default=DEFAULT_SCHEDULE_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


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


def safe_int(value: Optional[str]) -> int:
    try:
        return int(value or 0)
    except ValueError:
        return 0


def safe_float(value: Optional[str]) -> float:
    try:
        return float(value or 0)
    except ValueError:
        return 0.0


def format_hours(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.1f}"


def format_pct(numerator: int, denominator: int) -> str:
    if not denominator:
        return ""
    return f"{(numerator / denominator) * 100:.1f}"


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


def normalize_direction(value: str) -> str:
    lowered = (value or "").strip().lower()
    if lowered in {"incoming", "inbound"}:
        return "inbound"
    if lowered in {"outgoing", "outbound"}:
        return "outbound"
    return "unknown"


def median_value(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(median(values))


def mean_value(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(sum(values) / len(values))


def lead_dir_from_sources(*paths: str) -> Optional[Path]:
    filenames = {
        "lead_deal_sheet.md",
        "lead_deal_sheet.json",
        "lead_memory_brief.md",
        "lead_conversation_intelligence.md",
        "lead_conversation_intelligence.json",
        "lead_action_plan.md",
        "lead_action_plan.json",
    }
    for raw in paths:
        if not raw:
            continue
        path = Path(raw)
        if path.name in filenames:
            return path.parent
    return None


def build_response_turns(
    lead_id: str,
    lead_name: str,
    lead_owner_name: str,
    stage_label: str,
    communication_rows: Sequence[Dict[str, str]],
    waiting_on_us: bool,
) -> List[Dict[str, Any]]:
    now_utc = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []
    events = []
    for row in communication_rows:
        if row.get("channel") not in {"email", "sms"}:
            continue
        dt = parse_iso(row.get("event_datetime_utc"))
        if not dt:
            continue
        direction = normalize_direction(row.get("direction") or "")
        if direction == "unknown":
            continue
        events.append({**row, "_dt": dt, "_direction": direction})
    events.sort(key=lambda row: row["_dt"])

    pending: Optional[Dict[str, Any]] = None
    for event in events:
        direction = event["_direction"]
        if direction == "inbound":
            if pending is None:
                pending = {
                    "first_inbound_utc": iso_z(event["_dt"]),
                    "last_inbound_utc": iso_z(event["_dt"]),
                    "first_inbound_dt": event["_dt"],
                    "last_inbound_dt": event["_dt"],
                    "inbound_event_ids": [event.get("event_id") or ""],
                    "inbound_message_count": 1,
                    "channels": {event.get("channel") or ""},
                    "contact_name": event.get("contact_name") or "",
                    "latest_inbound_preview": event.get("body_preview") or event.get("subject") or "",
                    "inbound_folder": event.get("folder") or "",
                }
            else:
                pending["last_inbound_utc"] = iso_z(event["_dt"])
                pending["last_inbound_dt"] = event["_dt"]
                pending["inbound_event_ids"].append(event.get("event_id") or "")
                pending["inbound_message_count"] += 1
                pending["channels"].add(event.get("channel") or "")
                pending["latest_inbound_preview"] = event.get("body_preview") or event.get("subject") or pending["latest_inbound_preview"]
        elif pending is not None:
            response_hours_from_first = round((event["_dt"] - pending["first_inbound_dt"]).total_seconds() / 3600, 2)
            response_hours_from_last = round((event["_dt"] - pending["last_inbound_dt"]).total_seconds() / 3600, 2)
            rows.append(
                {
                    "lead_id": lead_id,
                    "lead_name": lead_name,
                    "lead_owner_name": lead_owner_name,
                    "stage_label": stage_label,
                    "response_state": "responded",
                    "inbound_started_utc": pending["first_inbound_utc"],
                    "last_inbound_utc": pending["last_inbound_utc"],
                    "outbound_utc": iso_z(event["_dt"]),
                    "response_hours_from_first": response_hours_from_first,
                    "response_hours_from_last": response_hours_from_last,
                    "inbound_message_count": pending["inbound_message_count"],
                    "channels": " | ".join(sorted(channel for channel in pending["channels"] if channel)),
                    "contact_name": pending["contact_name"],
                    "inbound_preview": compact_text(pending["latest_inbound_preview"], 320),
                    "outbound_preview": compact_text(event.get("body_preview") or event.get("subject") or "", 320),
                    "outbound_salesperson_name": event.get("salesperson_name") or "",
                    "inbound_event_ids": " | ".join(event_id for event_id in pending["inbound_event_ids"] if event_id),
                    "outbound_event_id": event.get("event_id") or "",
                    "inbound_folder": pending["inbound_folder"],
                    "outbound_folder": event.get("folder") or "",
                    "waiting_on_us": "False",
                }
            )
            pending = None

    if pending is not None:
        age_hours = round((now_utc - pending["last_inbound_dt"]).total_seconds() / 3600, 2)
        rows.append(
            {
                "lead_id": lead_id,
                "lead_name": lead_name,
                "lead_owner_name": lead_owner_name,
                "stage_label": stage_label,
                "response_state": "unanswered",
                "inbound_started_utc": pending["first_inbound_utc"],
                "last_inbound_utc": pending["last_inbound_utc"],
                "outbound_utc": "",
                "response_hours_from_first": "",
                "response_hours_from_last": age_hours,
                "inbound_message_count": pending["inbound_message_count"],
                "channels": " | ".join(sorted(channel for channel in pending["channels"] if channel)),
                "contact_name": pending["contact_name"],
                "inbound_preview": compact_text(pending["latest_inbound_preview"], 320),
                "outbound_preview": "",
                "outbound_salesperson_name": "",
                "inbound_event_ids": " | ".join(event_id for event_id in pending["inbound_event_ids"] if event_id),
                "outbound_event_id": "",
                "inbound_folder": pending["inbound_folder"],
                "outbound_folder": "",
                "waiting_on_us": "True" if waiting_on_us else "False",
            }
        )

    return rows


def summarize_owner_markdown(
    owner_name: str,
    summary_row: Dict[str, Any],
    stalled_rows: Sequence[Dict[str, Any]],
    promise_rows: Sequence[Dict[str, Any]],
    quote_rows: Sequence[Dict[str, Any]],
) -> str:
    lines = [
        f"# Seller Performance: {owner_name}",
        "",
        "## Snapshot",
        f"- Leads: `{summary_row.get('lead_count') or 0}`",
        f"- Active Leads: `{summary_row.get('active_lead_count') or 0}`",
        f"- Won Leads: `{summary_row.get('won_lead_count') or 0}`",
        f"- Lost Leads: `{summary_row.get('lost_lead_count') or 0}`",
        f"- Active Waiting On Us: `{summary_row.get('active_waiting_on_us_count') or 0}`",
        f"- Active Stalled Leads: `{summary_row.get('active_stalled_lead_count') or 0}`",
        f"- Quote Friction Leads: `{summary_row.get('active_quote_friction_count') or 0}`",
        f"- Pending Promises: `{summary_row.get('pending_commitment_count') or 0}`",
        f"- Promise Resolution Rate: `{summary_row.get('promise_resolution_rate_pct') or ''}%`" if summary_row.get("promise_resolution_rate_pct") else "- Promise Resolution Rate: ``",
        f"- Median Message Response (hrs): `{summary_row.get('median_message_response_hours') or ''}`",
        f"- Quick Response Rate (<=24h): `{summary_row.get('quick_response_rate_pct') or ''}%`" if summary_row.get("quick_response_rate_pct") else "- Quick Response Rate (<=24h): ``",
        f"- Unanswered Message Turns: `{summary_row.get('unanswered_message_turn_count') or 0}`",
        "",
        "## What To Watch",
        f"- {summary_row.get('owner_watch_summary') or ''}",
        "",
        "## Stalled Leads",
    ]
    if stalled_rows:
        for row in stalled_rows[:15]:
            lines.append(
                f"- `{row.get('stall_score') or ''}` | {row.get('lead_name') or ''} | "
                f"{row.get('stage_label') or ''} | {row.get('primary_stall_reason') or ''}"
            )
    else:
        lines.append("- None.")

    lines.extend(["", "## Pending Promises"])
    if promise_rows:
        for row in promise_rows[:15]:
            lines.append(
                f"- `{row.get('pending_commitment_count') or 0}` pending | {row.get('lead_name') or ''} | "
                f"{row.get('latest_pending_commitment_text') or ''}"
            )
    else:
        lines.append("- None.")

    lines.extend(["", "## Quote Friction"])
    if quote_rows:
        for row in quote_rows[:15]:
            lines.append(
                f"- `{row.get('priority_score') or ''}` | {row.get('lead_name') or ''} | "
                f"{row.get('pricing_posture') or ''} | {row.get('seller_next_move') or ''}"
            )
    else:
        lines.append("- None.")

    lines.append("")
    return "\n".join(lines)


def build_profile_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Seller Signal Sheet: {payload.get('lead_name') or ''}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Pipeline / Stage: {payload.get('pipeline_name') or ''} / {payload.get('stage_label') or ''}",
        f"- Stall Score: `{payload.get('stall_score') or 0}`",
        f"- Stall State: {pretty_label(payload.get('stall_state') or '')}",
        f"- Conversation Quality State: {pretty_label(payload.get('conversation_quality_state') or '')}",
        f"- Waiting On Us: `{payload.get('waiting_on_us') or ''}`",
        f"- Latest Observed Activity (UTC): `{payload.get('latest_observed_activity_utc') or ''}`",
        f"- Latest Inbound (UTC): `{payload.get('latest_incoming_utc') or ''}`",
        f"- Latest Outbound (UTC): `{payload.get('latest_outgoing_utc') or ''}`",
        "",
        "## Response Speed",
        f"- Message Response Turns: `{payload.get('message_turn_count') or 0}`",
        f"- Responded Turns: `{payload.get('responded_message_turn_count') or 0}`",
        f"- Unanswered Turns: `{payload.get('unanswered_message_turn_count') or 0}`",
        f"- Median Response (hrs): `{payload.get('median_message_response_hours') or ''}`",
        f"- Average Response (hrs): `{payload.get('average_message_response_hours') or ''}`",
        f"- Quick Response Rate (<=24h): `{payload.get('quick_response_rate_pct') or ''}%`" if payload.get("quick_response_rate_pct") else "- Quick Response Rate (<=24h): ``",
        f"- Slow Response Rate (>72h): `{payload.get('slow_response_rate_pct') or ''}%`" if payload.get("slow_response_rate_pct") else "- Slow Response Rate (>72h): ``",
        "",
        "## Promise Follow-Through",
        f"- Total Commitments: `{payload.get('total_commitment_count') or 0}`",
        f"- Resolved Commitments: `{payload.get('resolved_commitment_count') or 0}`",
        f"- Pending Commitments: `{payload.get('pending_commitment_count') or 0}`",
        f"- Promise Resolution Rate: `{payload.get('promise_resolution_rate_pct') or ''}%`" if payload.get("promise_resolution_rate_pct") else "- Promise Resolution Rate: ``",
        "",
        "## Conversation Load",
        f"- Buyer Asks: `{payload.get('buyer_ask_count') or 0}`",
        f"- Blockers: `{payload.get('blocker_count') or 0}`",
        f"- Open Loops: `{payload.get('open_loop_count') or 0}`",
        f"- Dominant Topics: {payload.get('dominant_topics') or ''}",
        "",
        "## Commercial / Deadline Pressure",
        f"- Pricing Posture: {pretty_label(payload.get('pricing_posture') or '')}",
        f"- Quote Friction Count: `{payload.get('quote_friction_count') or 0}`",
        f"- Budget Pressure Count: `{payload.get('budget_pressure_count') or 0}`",
        f"- Schedule Pressure Score: `{payload.get('schedule_pressure_score') or 0}`",
        f"- Due Today: `{payload.get('due_today_count') or 0}`",
        f"- Due Within 48h: `{payload.get('due_48h_count') or 0}`",
        "",
        "## Why This Lead Is Stuck",
        f"- {payload.get('primary_stall_reason') or 'No obvious stall reason captured.'}",
        "",
        "## Recommended Move",
        f"- {payload.get('seller_next_move') or ''}",
        "",
        "## Pending Promises",
    ]
    pending_promises = payload.get("pending_commitment_lines") or []
    if pending_promises:
        for line in pending_promises:
            lines.append(f"- {line}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Recent Message Turns"])
    response_turn_lines = payload.get("recent_response_turn_lines") or []
    if response_turn_lines:
        for line in response_turn_lines:
            lines.append(f"- {line}")
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "## Summary",
            f"- {payload.get('seller_signal_summary') or ''}",
            "",
        ]
    )
    return "\n".join(lines)


def build_board_markdown(title: str, rows: Sequence[Dict[str, Any]], count_label: str, column: str) -> str:
    lines = [f"# {title}", "", f"- Total {count_label}: `{len(rows)}`", "", "## Top Rows"]
    for row in rows[:120]:
        primary_text = (
            row.get(column)
            or row.get("owner_watch_summary")
            or row.get("seller_next_move")
            or row.get("primary_stall_reason")
            or row.get("seller_signal_summary")
            or "No signal captured."
        )
        score = row.get("stall_score") or row.get("active_stalled_lead_count") or row.get("unanswered_message_turn_count") or ""
        lines.append(
            f"- `{score}` | {row.get('lead_name') or row.get('owner_name') or ''} | "
            f"{row.get('lead_owner_name') or row.get('owner_name') or ''} | "
            f"{row.get('stage_label') or ''} | {primary_text}"
        )
    lines.append("")
    return "\n".join(lines)


def build_owner_board_markdown(
    title: str,
    rows: Sequence[Dict[str, Any]],
    count_label: str,
    score_column: str,
    summary_column: str,
) -> str:
    lines = [f"# {title}", "", f"- Total {count_label}: `{len(rows)}`", "", "## Top Rows"]
    for row in rows[:120]:
        lines.append(
            f"- `{row.get(score_column) or ''}` | {row.get('owner_name') or ''} | "
            f"median {row.get('median_message_response_hours') or 'n/a'}h | "
            f"pending promises {row.get('pending_commitment_count') or 0} | "
            f"unanswered turns {row.get('unanswered_message_turn_count') or 0} | "
            f"{row.get(summary_column) or ''}"
        )
    lines.append("")
    return "\n".join(lines)


def build_owner_overview_markdown(rows: Sequence[Dict[str, Any]]) -> str:
    lines = ["# Owner Performance Overview", "", "## Owners"]
    for row in rows:
        lines.append(
            f"- {row.get('owner_name') or ''}: active {row.get('active_lead_count') or 0} | "
            f"waiting on us {row.get('active_waiting_on_us_count') or 0} | stalled {row.get('active_stalled_lead_count') or 0} | "
            f"pending promises {row.get('pending_commitment_count') or 0} | median response {row.get('median_message_response_hours') or ''}h"
        )
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

    deal_rows = load_csv_rows(args.deal_sheets_csv)
    conversation_rows = load_csv_rows(args.conversation_csv)
    follow_up_rows = {row["lead_id"]: row for row in load_csv_rows(args.follow_up_csv)}
    communication_rows = load_csv_rows(args.communications_csv)
    pricing_rows = {row["lead_id"]: row for row in load_csv_rows(args.pricing_scope_csv)}
    schedule_rows = {row["lead_id"]: row for row in load_csv_rows(args.schedule_csv)}

    deal_by_lead = {row["lead_id"]: row for row in deal_rows}
    conversation_by_lead = {row["lead_id"]: row for row in conversation_rows}
    communications_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in communication_rows:
        communications_by_lead[row.get("lead_id") or ""].append(row)

    commitment_stats_by_lead: Dict[str, Dict[str, Any]] = {}
    for row in conversation_rows:
        lead_id = row["lead_id"]
        payload = load_json(Path(row.get("conversation_intelligence_json_path") or ""))
        commitments = payload.get("sales_commitments", [])
        total_commitment_count = len(commitments)
        resolved_commitment_count = sum(1 for item in commitments if (item.get("status") or "").lower() == "resolved")
        pending_commitments = [item for item in commitments if (item.get("status") or "").lower() != "resolved"]
        pending_commitment_lines = []
        for item in pending_commitments[:5]:
            category = pretty_label(item.get("category") or "commitment")
            pending_commitment_lines.append(f"[{category}] {compact_text(item.get('text'), 320)}")
        commitment_stats_by_lead[lead_id] = {
            "total_commitment_count": total_commitment_count,
            "resolved_commitment_count": resolved_commitment_count,
            "pending_commitment_count": len(pending_commitments),
            "pending_commitment_lines": pending_commitment_lines,
            "latest_pending_commitment_text": compact_text(pending_commitments[0].get("text"), 320) if pending_commitments else "",
        }

    lead_rows_output: List[Dict[str, Any]] = []
    lead_payloads: List[Dict[str, Any]] = []
    response_turn_rows: List[Dict[str, Any]] = []

    lead_ids = set(deal_by_lead.keys()) | set(conversation_by_lead.keys()) | set(follow_up_rows.keys()) | set(pricing_rows.keys()) | set(schedule_rows.keys())

    for lead_id in sorted(lead_ids):
        deal_row = deal_by_lead.get(lead_id, {})
        convo_row = conversation_by_lead.get(lead_id, {})
        follow_up_row = follow_up_rows.get(lead_id, {})
        pricing_row = pricing_rows.get(lead_id, {})
        schedule_row = schedule_rows.get(lead_id, {})
        commitment_stats = commitment_stats_by_lead.get(lead_id, {})

        lead_name = deal_row.get("lead_name") or convo_row.get("lead_name") or follow_up_row.get("lead_name") or schedule_row.get("lead_name") or ""
        lead_owner_name = deal_row.get("lead_owner_name") or convo_row.get("lead_owner_name") or follow_up_row.get("lead_owner_name") or schedule_row.get("lead_owner_name") or "Unassigned"
        stage_label = deal_row.get("stage_label") or convo_row.get("stage_label") or follow_up_row.get("stage_label") or schedule_row.get("stage_label") or ""
        pipeline_name = deal_row.get("pipeline_name") or follow_up_row.get("pipeline_name") or schedule_row.get("pipeline_name") or ""
        stage_type = deal_row.get("stage_type") or ""
        waiting_on_us = (follow_up_row.get("waiting_on_us") or "").strip().lower() == "true"

        lead_dir = lead_dir_from_sources(
            deal_row.get("deal_sheet_path") or "",
            convo_row.get("conversation_intelligence_path") or "",
            follow_up_row.get("brief_path") or "",
            schedule_row.get("lead_schedule_commitment_sheet_path") or "",
        )
        if lead_dir is None:
            continue

        turns = build_response_turns(
            lead_id=lead_id,
            lead_name=lead_name,
            lead_owner_name=lead_owner_name,
            stage_label=stage_label,
            communication_rows=communications_by_lead.get(lead_id, []),
            waiting_on_us=waiting_on_us,
        )
        response_turn_rows.extend(turns)

        responded_turns = [row for row in turns if row.get("response_state") == "responded"]
        unanswered_turns = [row for row in turns if row.get("response_state") == "unanswered"]
        response_hours = [safe_float(str(row.get("response_hours_from_last") or "")) for row in responded_turns if row.get("response_hours_from_last") not in {"", None}]
        quick_response_count = sum(1 for row in responded_turns if safe_float(str(row.get("response_hours_from_last") or "")) <= 24)
        slow_response_count = sum(1 for row in responded_turns if safe_float(str(row.get("response_hours_from_last") or "")) > 72)

        median_response_hours = median_value(response_hours)
        average_response_hours = mean_value(response_hours)

        buyer_ask_count = safe_int(convo_row.get("buyer_ask_count"))
        blocker_count = safe_int(convo_row.get("blocker_count"))
        open_loop_count = safe_int(convo_row.get("open_loop_count"))
        total_commitment_count = commitment_stats.get("total_commitment_count", 0)
        resolved_commitment_count = commitment_stats.get("resolved_commitment_count", 0)
        pending_commitment_count = commitment_stats.get("pending_commitment_count", 0)
        quote_friction_count = safe_int(pricing_row.get("quote_revision_count")) + safe_int(pricing_row.get("budget_pressure_count")) + safe_int(pricing_row.get("package_compare_count"))
        budget_pressure_count = safe_int(pricing_row.get("budget_pressure_count"))
        days_since_observed = safe_int(follow_up_row.get("days_since_observed"))
        due_today_count = safe_int(schedule_row.get("due_today_count"))
        due_48h_count = safe_int(schedule_row.get("due_48h_count"))
        schedule_pressure_score = safe_int(schedule_row.get("schedule_pressure_score"))
        priority_score = max(safe_int(deal_row.get("follow_up_priority_score")), safe_int(deal_row.get("readiness_score")), safe_int(follow_up_row.get("priority_score")))

        stall_score = 0
        if stage_type == "active":
            stall_score += min(priority_score // 20, 6)
            if waiting_on_us:
                stall_score += 4
            if pending_commitment_count:
                stall_score += min(pending_commitment_count * 2, 6)
            if unanswered_turns:
                stall_score += 3
            if due_today_count:
                stall_score += 3
            elif due_48h_count:
                stall_score += 2
            if quote_friction_count:
                stall_score += 2
            if budget_pressure_count:
                stall_score += 2
            if open_loop_count:
                stall_score += 1
            if days_since_observed > 2:
                stall_score += min(days_since_observed - 2, 4)
        elif stage_type == "won":
            stall_score += pending_commitment_count * 2
            if due_today_count:
                stall_score += 3
            if schedule_row.get("schedule_state") in {"stale_uncertain_date", "past_event_date"}:
                stall_score += 2
        else:
            stall_score += pending_commitment_count
            if unanswered_turns:
                stall_score += 2

        if stage_type == "lost":
            stall_state = "closed_lost"
        elif waiting_on_us:
            stall_state = "reply_needed"
        elif pending_commitment_count:
            stall_state = "promise_followthrough_needed"
        elif stage_type == "won" and due_today_count:
            stall_state = "post_sale_deadline_pressure"
        elif quote_friction_count:
            stall_state = "quote_friction"
        elif unanswered_turns:
            stall_state = "message_response_gap"
        elif due_48h_count:
            stall_state = "deadline_pressure"
        elif stage_type == "active" and days_since_observed >= 5:
            stall_state = "stalled_no_recent_touch"
        else:
            stall_state = "healthy_momentum"

        if waiting_on_us:
            primary_stall_reason = compact_text(follow_up_row.get("latest_buyer_signal") or follow_up_row.get("recommended_action") or "", 360)
            seller_next_move = "Reply to the latest inbound and close with a concrete next step."
        elif pending_commitment_count:
            primary_stall_reason = commitment_stats.get("latest_pending_commitment_text") or "A promised follow-up is still open."
            seller_next_move = "Fulfill the oldest pending promise before adding a new touch."
        elif quote_friction_count:
            primary_stall_reason = compact_text(
                pricing_row.get("top_budget_pressure")
                or pricing_row.get("top_pricing_question")
                or pricing_row.get("top_scope_change")
                or pricing_row.get("pricing_scope_summary")
                or "",
                360,
            )
            if pricing_row.get("pricing_posture") in {"budget_pressure", "budget_constrained_scope_reduction"}:
                seller_next_move = "Send a tighter price-fit option set with explicit savings and scope tradeoffs."
            else:
                seller_next_move = pricing_row.get("pricing_action") or "Send the revised quote side-by-side and ask for a decision."
        elif due_today_count or due_48h_count:
            primary_stall_reason = compact_text(schedule_row.get("next_due_summary") or schedule_row.get("event_watch_reason") or "", 360)
            seller_next_move = "Clear the due-now deadlines before starting a new conversation thread."
        elif unanswered_turns:
            primary_stall_reason = compact_text(unanswered_turns[0].get("inbound_preview") or "", 360)
            seller_next_move = "Answer the open message streak and reset momentum."
        else:
            primary_stall_reason = compact_text(
                follow_up_row.get("recommended_action") or deal_row.get("operator_move") or convo_row.get("latest_buyer_ask") or "",
                360,
            )
            seller_next_move = compact_text(deal_row.get("operator_move") or follow_up_row.get("recommended_action") or "Keep the lead moving with the next concrete milestone.", 360)

        if stage_type == "lost":
            conversation_quality_state = "closed_out"
        elif waiting_on_us or unanswered_turns:
            conversation_quality_state = "response_risk"
        elif pending_commitment_count or (total_commitment_count and resolved_commitment_count < total_commitment_count):
            conversation_quality_state = "promise_risk"
        elif median_response_hours is not None and median_response_hours <= 12 and not quote_friction_count and not open_loop_count:
            conversation_quality_state = "tight_followthrough"
        elif quote_friction_count:
            conversation_quality_state = "commercial_friction"
        else:
            conversation_quality_state = "watch"

        recent_response_turn_lines = []
        for row in sorted(turns, key=lambda item: item.get("last_inbound_utc") or "", reverse=True)[:5]:
            if row.get("response_state") == "responded":
                recent_response_turn_lines.append(
                    f"`{row.get('last_inbound_utc') or ''}` -> `{row.get('outbound_utc') or ''}` | "
                    f"{row.get('channels') or ''} | {row.get('response_hours_from_last')}h | {row.get('inbound_preview') or ''}"
                )
            else:
                recent_response_turn_lines.append(
                    f"`{row.get('last_inbound_utc') or ''}` | unanswered | {row.get('channels') or ''} | {row.get('inbound_preview') or ''}"
                )

        promise_resolution_rate_pct = format_pct(resolved_commitment_count, total_commitment_count)
        quick_response_rate_pct = format_pct(quick_response_count, len(responded_turns))
        slow_response_rate_pct = format_pct(slow_response_count, len(responded_turns))

        lead_row_out: Dict[str, Any] = {
            "lead_id": lead_id,
            "lead_name": lead_name,
            "lead_owner_name": lead_owner_name,
            "pipeline_name": pipeline_name,
            "stage_label": stage_label,
            "stage_type": stage_type,
            "latest_observed_activity_utc": follow_up_row.get("latest_observed_activity_utc") or convo_row.get("latest_observed_activity_utc") or "",
            "latest_incoming_utc": follow_up_row.get("latest_incoming_utc") or "",
            "latest_outgoing_utc": follow_up_row.get("latest_outgoing_utc") or "",
            "waiting_on_us": "True" if waiting_on_us else "False",
            "days_since_observed": days_since_observed,
            "priority_score": priority_score,
            "message_turn_count": len(turns),
            "responded_message_turn_count": len(responded_turns),
            "unanswered_message_turn_count": len(unanswered_turns),
            "median_message_response_hours": format_hours(median_response_hours),
            "average_message_response_hours": format_hours(average_response_hours),
            "quick_response_rate_pct": quick_response_rate_pct,
            "slow_response_rate_pct": slow_response_rate_pct,
            "total_commitment_count": total_commitment_count,
            "resolved_commitment_count": resolved_commitment_count,
            "pending_commitment_count": pending_commitment_count,
            "promise_resolution_rate_pct": promise_resolution_rate_pct,
            "buyer_ask_count": buyer_ask_count,
            "blocker_count": blocker_count,
            "open_loop_count": open_loop_count,
            "dominant_topics": convo_row.get("dominant_topics") or "",
            "pricing_posture": pricing_row.get("pricing_posture") or "",
            "quote_friction_count": quote_friction_count,
            "budget_pressure_count": budget_pressure_count,
            "schedule_pressure_score": schedule_pressure_score,
            "due_today_count": due_today_count,
            "due_48h_count": due_48h_count,
            "stall_score": stall_score,
            "stall_state": stall_state,
            "conversation_quality_state": conversation_quality_state,
            "primary_stall_reason": primary_stall_reason,
            "seller_next_move": seller_next_move,
            "seller_signal_summary": compact_text(
                f"{lead_name}: stall {pretty_label(stall_state)}; waiting on us {waiting_on_us}; pending promises {pending_commitment_count}; "
                f"unanswered turns {len(unanswered_turns)}; median response {format_hours(median_response_hours) or 'n/a'}h; "
                f"quote friction {quote_friction_count}; due today {due_today_count}.",
                420,
            ),
            "lead_seller_signal_sheet_path": str(lead_dir / "lead_seller_signal_sheet.md"),
            "lead_seller_signal_sheet_json_path": str(lead_dir / "lead_seller_signal_sheet.json"),
        }

        payload = dict(lead_row_out)
        payload.update(
            {
                "pending_commitment_lines": commitment_stats.get("pending_commitment_lines", []),
                "recent_response_turn_lines": recent_response_turn_lines,
            }
        )

        write_json(lead_dir / "lead_seller_signal_sheet.json", payload)
        (lead_dir / "lead_seller_signal_sheet.md").write_text(build_profile_markdown(payload), encoding="utf-8")

        lead_rows_output.append(lead_row_out)
        lead_payloads.append(payload)

    lead_rows_output = sorted(
        lead_rows_output,
        key=lambda row: (
            -safe_int(str(row.get("stall_score") or 0)),
            -safe_int(str(row.get("priority_score") or 0)),
            row.get("lead_name") or "",
        ),
    )

    owner_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    turns_by_owner: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in lead_rows_output:
        owner_groups[row.get("lead_owner_name") or "Unassigned"].append(row)
    for row in response_turn_rows:
        turns_by_owner[row.get("lead_owner_name") or "Unassigned"].append(row)

    owner_summary_rows: List[Dict[str, Any]] = []
    for owner_name, rows in sorted(owner_groups.items(), key=lambda item: (-len(item[1]), item[0].lower())):
        owner_turns = turns_by_owner.get(owner_name, [])
        responded_owner_turns = [row for row in owner_turns if row.get("response_state") == "responded"]
        unanswered_owner_turns = [row for row in owner_turns if row.get("response_state") == "unanswered"]
        owner_response_hours = [safe_float(str(row.get("response_hours_from_last") or "")) for row in responded_owner_turns if row.get("response_hours_from_last") not in {"", None}]
        owner_median_response = median_value(owner_response_hours)
        owner_average_response = mean_value(owner_response_hours)
        owner_quick_count = sum(1 for row in responded_owner_turns if safe_float(str(row.get("response_hours_from_last") or "")) <= 24)
        owner_slow_count = sum(1 for row in responded_owner_turns if safe_float(str(row.get("response_hours_from_last") or "")) > 72)
        stage_counts = Counter(row.get("stage_type") or "" for row in rows)
        active_rows = [row for row in rows if row.get("stage_type") == "active"]
        active_stalled_rows = [row for row in active_rows if row.get("stall_state") != "healthy_momentum"]
        quote_friction_rows = [row for row in active_rows if safe_int(str(row.get("quote_friction_count") or 0)) > 0]
        summary_row = {
            "owner_name": owner_name,
            "lead_count": len(rows),
            "active_lead_count": stage_counts.get("active", 0),
            "won_lead_count": stage_counts.get("won", 0),
            "lost_lead_count": stage_counts.get("lost", 0),
            "active_waiting_on_us_count": sum(1 for row in active_rows if (row.get("waiting_on_us") or "").lower() == "true"),
            "active_stalled_lead_count": len(active_stalled_rows),
            "active_quote_friction_count": len(quote_friction_rows),
            "budget_pressure_lead_count": sum(1 for row in active_rows if safe_int(str(row.get("budget_pressure_count") or 0)) > 0),
            "message_turn_count": len(owner_turns),
            "responded_message_turn_count": len(responded_owner_turns),
            "unanswered_message_turn_count": len(unanswered_owner_turns),
            "median_message_response_hours": format_hours(owner_median_response),
            "average_message_response_hours": format_hours(owner_average_response),
            "quick_response_rate_pct": format_pct(owner_quick_count, len(responded_owner_turns)),
            "slow_response_rate_pct": format_pct(owner_slow_count, len(responded_owner_turns)),
            "total_commitment_count": sum(safe_int(str(row.get("total_commitment_count") or 0)) for row in rows),
            "resolved_commitment_count": sum(safe_int(str(row.get("resolved_commitment_count") or 0)) for row in rows),
            "pending_commitment_count": sum(safe_int(str(row.get("pending_commitment_count") or 0)) for row in rows),
            "pending_promise_lead_count": sum(1 for row in rows if safe_int(str(row.get("pending_commitment_count") or 0)) > 0),
            "due_today_count": sum(safe_int(str(row.get("due_today_count") or 0)) for row in rows),
            "due_48h_count": sum(safe_int(str(row.get("due_48h_count") or 0)) for row in rows),
            "latest_observed_activity_utc": max((row.get("latest_observed_activity_utc") or "" for row in rows), default=""),
            "promise_resolution_rate_pct": format_pct(
                sum(safe_int(str(row.get("resolved_commitment_count") or 0)) for row in rows),
                sum(safe_int(str(row.get("total_commitment_count") or 0)) for row in rows),
            ),
            "owner_watch_summary": compact_text(
                f"{owner_name}: active waiting on us {sum(1 for row in active_rows if (row.get('waiting_on_us') or '').lower() == 'true')}; "
                f"active stalled {len(active_stalled_rows)}; pending promises {sum(safe_int(str(row.get('pending_commitment_count') or 0)) for row in rows)}; "
                f"median message response {format_hours(owner_median_response) or 'n/a'}h; unanswered turns {len(unanswered_owner_turns)}.",
                420,
            ),
            "performance_path": str(by_owner_dir / slugify(owner_name) / "performance.md"),
        }
        owner_summary_rows.append(summary_row)

    owner_summary_rows = sorted(
        owner_summary_rows,
        key=lambda row: (
            -safe_int(str(row.get("active_stalled_lead_count") or 0)),
            -safe_int(str(row.get("pending_commitment_count") or 0)),
            -safe_int(str(row.get("active_waiting_on_us_count") or 0)),
            row.get("owner_name") or "",
        ),
    )

    response_speed_rows = sorted(
        owner_summary_rows,
        key=lambda row: (
            -safe_int(str(row.get("unanswered_message_turn_count") or 0)),
            -(safe_float(str(row.get("median_message_response_hours") or 0))),
            row.get("owner_name") or "",
        ),
    )
    promise_board_rows = sorted(
        owner_summary_rows,
        key=lambda row: (
            -safe_int(str(row.get("pending_commitment_count") or 0)),
            safe_float(str(row.get("promise_resolution_rate_pct") or 0)),
            row.get("owner_name") or "",
        ),
    )
    stalled_lead_rows = [row for row in lead_rows_output if row.get("stall_state") != "healthy_momentum" and row.get("stage_type") == "active"]
    quote_friction_rows = [row for row in lead_rows_output if safe_int(str(row.get("quote_friction_count") or 0)) > 0 and row.get("stage_type") == "active"]

    for summary_row in owner_summary_rows:
        owner_name = summary_row["owner_name"]
        owner_slug = slugify(owner_name)
        owner_dir = by_owner_dir / owner_slug
        ensure_dir(owner_dir)
        owner_leads = [row for row in lead_rows_output if row.get("lead_owner_name") == owner_name]
        owner_stalled = [row for row in stalled_lead_rows if row.get("lead_owner_name") == owner_name]
        owner_promises = [row for row in owner_leads if safe_int(str(row.get("pending_commitment_count") or 0)) > 0]
        owner_quote = [row for row in quote_friction_rows if row.get("lead_owner_name") == owner_name]
        (owner_dir / "performance.md").write_text(
            summarize_owner_markdown(owner_name, summary_row, owner_stalled, owner_promises, owner_quote),
            encoding="utf-8",
        )
        write_csv(owner_dir / "lead_signals.csv", owner_leads)
        write_jsonl(owner_dir / "lead_signals.jsonl", owner_leads)

    write_csv(normalized_dir / "message_response_turns.csv", response_turn_rows)
    write_jsonl(normalized_dir / "message_response_turns.jsonl", response_turn_rows)
    write_csv(normalized_dir / "lead_seller_performance_signals.csv", lead_rows_output)
    write_jsonl(normalized_dir / "lead_seller_performance_signals.jsonl", lead_rows_output)
    write_csv(normalized_dir / "owner_performance_summary.csv", owner_summary_rows)
    write_csv(normalized_dir / "response_speed_board.csv", response_speed_rows)
    write_csv(normalized_dir / "owner_promise_followthrough_board.csv", promise_board_rows)
    write_csv(normalized_dir / "stalled_lead_board.csv", stalled_lead_rows)
    write_csv(normalized_dir / "quote_friction_lead_board.csv", quote_friction_rows)

    (output_dir / "README.md").write_text(
        "\n".join(
            [
                "# Seller Performance Intelligence",
                "",
                "This layer compresses seller-side execution into a working view: message response speed, promise-keeping, stall signals, and quote friction by owner and by lead.",
                "",
                "## Snapshot",
                f"- Lead seller signal sheets: `{len(lead_rows_output)}`",
                f"- Response turns: `{len(response_turn_rows)}`",
                f"- Owner summaries: `{len(owner_summary_rows)}`",
                f"- Stalled active leads: `{len(stalled_lead_rows)}`",
                f"- Quote-friction active leads: `{len(quote_friction_rows)}`",
                "",
                "## Key Files",
                "- `owner_performance_overview.md`: one-row-per-owner seller execution view",
                "- `response_speed_board.md`: owner-level message response and unanswered-turn pressure",
                "- `promise_followthrough_board.md`: owner-level promise-keeping pressure",
                "- `stalled_lead_board.md`: active leads that are currently stuck",
                "- `quote_friction_board.md`: active leads where commercial friction is slowing movement",
                "- `../normalized/lead_seller_performance_signals.csv`: machine-friendly one-row-per-lead seller signal profile",
                "- `../normalized/message_response_turns.csv`: machine-friendly inbound-to-outbound response turns",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (output_dir / "owner_performance_overview.md").write_text(build_owner_overview_markdown(owner_summary_rows), encoding="utf-8")
    (output_dir / "response_speed_board.md").write_text(
        build_owner_board_markdown(
            "Response Speed Board",
            response_speed_rows,
            "owner response-speed rows",
            "unanswered_message_turn_count",
            "owner_watch_summary",
        ),
        encoding="utf-8",
    )
    (output_dir / "promise_followthrough_board.md").write_text(
        build_owner_board_markdown(
            "Promise Follow-Through Board",
            promise_board_rows,
            "owner promise-followthrough rows",
            "pending_commitment_count",
            "owner_watch_summary",
        ),
        encoding="utf-8",
    )
    (output_dir / "stalled_lead_board.md").write_text(
        build_board_markdown("Stalled Lead Board", stalled_lead_rows, "stalled active lead rows", "primary_stall_reason"),
        encoding="utf-8",
    )
    (output_dir / "quote_friction_board.md").write_text(
        build_board_markdown("Quote Friction Board", quote_friction_rows, "quote-friction active lead rows", "seller_next_move"),
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "lead_profiles": len(lead_rows_output),
                "response_turns": len(response_turn_rows),
                "owner_summaries": len(owner_summary_rows),
                "stalled_active_leads": len(stalled_lead_rows),
                "quote_friction_active_leads": len(quote_friction_rows),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
