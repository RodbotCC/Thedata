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
DEFAULT_FOLLOW_UP_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "follow_up_queue.csv"
DEFAULT_SELLER_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_seller_performance_signals.csv"
DEFAULT_PRICING_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_pricing_scope_profiles.csv"
DEFAULT_SCHEDULE_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_schedule_commitments.csv"
DEFAULT_COMMUNICATIONS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_communications.csv"
DEFAULT_OPPORTUNITIES_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /Comeketo Catering opportunities 2026-03-26 18-32.json")
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "source_channel_intelligence"

SOURCE_FIELD = "custom.cf_ge7qOebiWpyPvuv7xkzNaYpM8PsmOeNvXasXFOtPXRt"
CUSTOMER_TYPE_FIELD = "custom.cf_fs7mrfN5x0M20CyoltczyVg8t0Xul5GFvkC4FNUKvY6"
ASSIGNMENT_LANE_FIELD = "lead_custom.cf_xF8FLufgEx9bsijfRAfHhgIrPBQ5ajuohcazC7OtNmT"
INTERNAL_FLAGS_FIELD = "lead_custom.cf_9vVeQH1oYtJbtdHoL9VPwGhNpuCzVCgi95p7MCasszj"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build source-channel and attribution intelligence from the normalized Comeketo CRM library.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--deal-sheets-csv", type=Path, default=DEFAULT_DEAL_SHEETS_CSV)
    parser.add_argument("--follow-up-csv", type=Path, default=DEFAULT_FOLLOW_UP_CSV)
    parser.add_argument("--seller-csv", type=Path, default=DEFAULT_SELLER_CSV)
    parser.add_argument("--pricing-csv", type=Path, default=DEFAULT_PRICING_CSV)
    parser.add_argument("--schedule-csv", type=Path, default=DEFAULT_SCHEDULE_CSV)
    parser.add_argument("--communications-csv", type=Path, default=DEFAULT_COMMUNICATIONS_CSV)
    parser.add_argument("--opportunities-path", type=Path, default=DEFAULT_OPPORTUNITIES_PATH)
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
        return []
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


def compact_text(value: Optional[str], limit: int = 320) -> str:
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


def format_number(value: Optional[float]) -> str:
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


def list_to_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, list):
        return " | ".join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip()


def split_pipe_values(value: Optional[str]) -> List[str]:
    text = (value or "").strip()
    if not text:
        return []
    return [chunk.strip() for chunk in text.split("|") if chunk.strip()]


def join_top_counts(counter: Counter[str], limit: int = 3) -> str:
    items = [f"{name} ({count})" for name, count in counter.most_common(limit) if name and count > 0]
    return " | ".join(items)


def hours_label_from_text(value: Optional[str]) -> str:
    text = (value or "").strip()
    return f"{text}h" if text else "n/a"


def hours_label_from_value(value: Optional[float]) -> str:
    text = format_hours(value)
    return f"{text}h" if text else "n/a"


def lead_dir_from_sources(*paths: str) -> Optional[Path]:
    filenames = {
        "lead_deal_sheet.md",
        "lead_deal_sheet.json",
        "lead_memory_brief.md",
        "lead_action_plan.md",
        "lead_schedule_commitment_sheet.md",
        "lead_seller_signal_sheet.md",
    }
    for raw in paths:
        if not raw:
            continue
        path = Path(raw)
        if path.name in filenames:
            return path.parent
    return None


def source_family(value: str) -> str:
    lowered = (value or "").strip().lower()
    if not lowered or lowered == "unknown":
        return "unknown"
    if any(token in lowered for token in ("expo", "show", "free tasting")):
        return "expo_event"
    if any(token in lowered for token in ("facebook", "instagram", "comeketocatering.com", "crisp chat", "website", "web")):
        return "digital_inbound"
    if any(token in lowered for token in ("the knot", "zola", "wedding wire", "eventective", "bark")):
        return "marketplace"
    if "inbound call" in lowered or lowered == "call":
        return "phone_inbound"
    if any(token in lowered for token in ("referral", "previous event", "planner", "decorator", "venue", "partner")):
        return "relationship_partner"
    return "other"


def opportunity_sort_key(row: Dict[str, Any]) -> Tuple[int, float]:
    status_rank = {"active": 0, "won": 1, "lost": 2}
    dt = parse_iso(
        row.get("date_updated")
        or row.get("lead_date_updated")
        or row.get("date_created")
        or row.get("lead_date_created")
        or ""
    )
    timestamp = dt.timestamp() if dt else 0.0
    return status_rank.get((row.get("status_type") or "").lower(), 99), -timestamp


def choose_anchor_opportunity(opportunities: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not opportunities:
        return None
    return min(opportunities, key=opportunity_sort_key)


def recent_opportunities(opportunities: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def recent_key(row: Dict[str, Any]) -> float:
        dt = parse_iso(
            row.get("date_updated")
            or row.get("lead_date_updated")
            or row.get("date_created")
            or row.get("lead_date_created")
            or ""
        )
        return dt.timestamp() if dt else 0.0

    return sorted(opportunities, key=recent_key, reverse=True)


def best_opportunity_value(opportunities: Sequence[Dict[str, Any]], field: str) -> Dict[str, str]:
    anchor = choose_anchor_opportunity(opportunities)
    candidates: List[Dict[str, Any]] = []
    if anchor is not None:
        candidates.append(anchor)
    for row in recent_opportunities(opportunities):
        if row is not anchor:
            candidates.append(row)

    for row in candidates:
        value = list_to_text(row.get(field))
        if value:
            dt = parse_iso(
                row.get("date_updated")
                or row.get("lead_date_updated")
                or row.get("date_created")
                or row.get("lead_date_created")
                or ""
            )
            return {
                "value": value,
                "updated_utc": iso_z(dt),
                "context_source": "anchor_opportunity" if row is anchor else "recent_nonempty_opportunity",
                "opportunity_id": row.get("id") or "",
                "status_label": row.get("status_label") or row.get("status_type") or "",
            }
    return {"value": "", "updated_utc": "", "context_source": "", "opportunity_id": "", "status_label": ""}


def build_opportunity_context(opportunities: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    anchor = choose_anchor_opportunity(opportunities)
    source_info = best_opportunity_value(opportunities, SOURCE_FIELD)
    customer_info = best_opportunity_value(opportunities, CUSTOMER_TYPE_FIELD)
    lane_info = best_opportunity_value(opportunities, ASSIGNMENT_LANE_FIELD)
    flags_info = best_opportunity_value(opportunities, INTERNAL_FLAGS_FIELD)
    anchor_dt = parse_iso(
        (anchor or {}).get("date_updated")
        or (anchor or {}).get("lead_date_updated")
        or (anchor or {}).get("date_created")
        or (anchor or {}).get("lead_date_created")
        or ""
    )
    return {
        "opportunity_count": len(opportunities),
        "anchor_opportunity_id": (anchor or {}).get("id") or "",
        "anchor_status_label": (anchor or {}).get("status_label") or (anchor or {}).get("status_type") or "",
        "anchor_updated_utc": iso_z(anchor_dt),
        "source_channels": source_info["value"],
        "customer_type": customer_info["value"],
        "assignment_lane": lane_info["value"],
        "internal_flags": flags_info["value"],
        "source_updated_utc": source_info["updated_utc"],
        "source_context_source": source_info["context_source"],
        "customer_type_updated_utc": customer_info["updated_utc"],
        "assignment_lane_updated_utc": lane_info["updated_utc"],
        "internal_flags_updated_utc": flags_info["updated_utc"],
    }


def summarize_communications(rows: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    channel_counts: Counter[str] = Counter()
    direction_counts: Counter[str] = Counter()
    total_count = 0
    for row in rows:
        channel = (row.get("channel") or row.get("event_family") or "").strip().lower()
        direction = normalize_direction(row.get("direction") or "")
        if channel:
            channel_counts[channel] += 1
        if direction != "unknown":
            direction_counts[direction] += 1
        total_count += 1
    dominant_channel = channel_counts.most_common(1)[0][0] if channel_counts else ""
    return {
        "total_communication_count": total_count,
        "call_count": channel_counts.get("call", 0),
        "sms_count": channel_counts.get("sms", 0),
        "email_count": channel_counts.get("email", 0),
        "inbound_count": direction_counts.get("inbound", 0),
        "outbound_count": direction_counts.get("outbound", 0),
        "dominant_communication_channel": dominant_channel,
        "communication_mix": join_top_counts(channel_counts, limit=3),
    }


def determine_source_move(
    primary_source_channel: str,
    waiting_on_us: bool,
    quote_friction_count: int,
    due_today_count: int,
    due_48h_count: int,
    seller_next_move: str,
    pricing_action: str,
    schedule_summary: str,
    operator_move: str,
    recommended_action: str,
) -> str:
    family = source_family(primary_source_channel)
    if waiting_on_us and seller_next_move:
        return compact_text(seller_next_move, 360)
    if quote_friction_count and pricing_action:
        return compact_text(pricing_action, 360)
    if (due_today_count or due_48h_count) and schedule_summary:
        return compact_text(schedule_summary, 360)
    if family == "expo_event":
        return "Use the expo / in-person context, recap the conversation, and ask for the next concrete milestone."
    if family == "digital_inbound":
        return "Keep the digital inbound hot with a fast response, a tight option set, and one clear ask for the next step."
    if family == "marketplace":
        return "Differentiate quickly with a clean package recommendation, transparent pricing path, and a direct booking step."
    if family == "relationship_partner":
        return "Lean relational: mention the connection or partner context and tighten the handoff to a quote, tasting, or decision checkpoint."
    if family == "phone_inbound":
        return "Use the phone momentum and close with a specific callback, quote send time, or tasting hold."
    return compact_text(
        seller_next_move or recommended_action or operator_move or "Keep momentum with one concrete next step and a date attached.",
        360,
    )


def infer_source_health_state(stage_type: str, waiting_on_us: bool, quote_friction_count: int, budget_pressure_count: int, stall_state: str) -> str:
    if stage_type == "won":
        return "booked_or_execution"
    if stage_type == "lost":
        return "closed_lost"
    if waiting_on_us:
        return "reply_needed"
    if quote_friction_count and budget_pressure_count:
        return "price_fit_risk"
    if quote_friction_count:
        return "commercial_friction"
    if stall_state and stall_state != "healthy_momentum":
        return "stalled"
    return "progressing"


def build_profile_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Lead Source Attribution Sheet: {payload.get('lead_name') or ''}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Pipeline / Stage: {payload.get('pipeline_name') or ''} / {payload.get('stage_label') or ''}",
        f"- Primary Source Channel: {payload.get('primary_source_channel') or ''}",
        f"- Source Family: {pretty_label(payload.get('primary_source_family') or '')}",
        f"- All Source Channels: {payload.get('source_channels') or ''}",
        f"- Customer Type: {payload.get('customer_type') or ''}",
        f"- Assignment Lane: {payload.get('assignment_lane') or ''}",
        f"- Internal Flags: {payload.get('internal_flags') or ''}",
        f"- Attribution Origin: {pretty_label(payload.get('attribution_origin') or '')}",
        f"- Opportunity Records Seen: `{payload.get('opportunity_count') or 0}`",
        "",
        "## Communication Mix",
        f"- Total Communications: `{payload.get('total_communication_count') or 0}`",
        f"- Calls: `{payload.get('call_count') or 0}`",
        f"- SMS: `{payload.get('sms_count') or 0}`",
        f"- Emails: `{payload.get('email_count') or 0}`",
        f"- Inbound / Outbound: `{payload.get('inbound_count') or 0}` / `{payload.get('outbound_count') or 0}`",
        f"- Dominant Communication Channel: {payload.get('dominant_communication_channel') or ''}",
        f"- Message Response Turns: `{payload.get('message_turn_count') or 0}`",
        f"- Unanswered Turns: `{payload.get('unanswered_message_turn_count') or 0}`",
        f"- Median Response (hrs): `{payload.get('median_message_response_hours') or ''}`",
        "",
        "## Current Pressure",
        f"- Source Health State: {pretty_label(payload.get('source_health_state') or '')}",
        f"- Stall State: {pretty_label(payload.get('stall_state') or '')}",
        f"- Conversation Quality State: {pretty_label(payload.get('conversation_quality_state') or '')}",
        f"- Quote Friction Count: `{payload.get('quote_friction_count') or 0}`",
        f"- Budget Pressure Count: `{payload.get('budget_pressure_count') or 0}`",
        f"- Due Today: `{payload.get('due_today_count') or 0}`",
        f"- Due Within 48h: `{payload.get('due_48h_count') or 0}`",
        f"- Waiting On Us: `{payload.get('waiting_on_us') or ''}`",
        f"- Readiness Bucket: {payload.get('readiness_bucket') or ''}",
        "",
        "## Recommended Move",
        f"- {payload.get('source_specific_move') or ''}",
        "",
        "## Summary",
        f"- {payload.get('attribution_summary') or ''}",
        "",
    ]
    return "\n".join(lines)


def build_summary_markdown(title: str, rows: Sequence[Dict[str, Any]], name_field: str, summary_field: str) -> str:
    lines = [f"# {title}", "", "## Rows"]
    for row in rows:
        lines.append(
            f"- {row.get(name_field) or ''}: leads {row.get('lead_count') or 0} | "
            f"active {row.get('active_lead_count') or 0} | won {row.get('won_lead_count') or 0} | "
            f"stalled {row.get('active_stalled_lead_count') or 0} | quote friction {row.get('active_quote_friction_count') or 0} | "
            f"median response {hours_label_from_text(row.get('median_message_response_hours'))} | {row.get(summary_field) or ''}"
        )
    lines.append("")
    return "\n".join(lines)


def build_board_markdown(
    title: str,
    rows: Sequence[Dict[str, Any]],
    name_field: str,
    score_field: str,
    summary_field: str,
    extra_field: str,
) -> str:
    lines = [f"# {title}", "", f"- Total rows: `{len(rows)}`", "", "## Top Rows"]
    for row in rows[:120]:
        lines.append(
            f"- `{row.get(score_field) or 0}` | {row.get(name_field) or ''} | "
            f"active {row.get('active_lead_count') or 0} | won {row.get('won_lead_count') or 0} | "
            f"{row.get(extra_field) or ''} | {row.get(summary_field) or ''}"
        )
    lines.append("")
    return "\n".join(lines)


def build_source_detail_markdown(summary_row: Dict[str, Any], lead_rows: Sequence[Dict[str, Any]]) -> str:
    lines = [
        f"# Source Channel: {summary_row.get('source_channel') or ''}",
        "",
        "## Snapshot",
        f"- Source Family: {pretty_label(summary_row.get('source_family') or '')}",
        f"- Leads: `{summary_row.get('lead_count') or 0}`",
        f"- Active Leads: `{summary_row.get('active_lead_count') or 0}`",
        f"- Won Leads: `{summary_row.get('won_lead_count') or 0}`",
        f"- Lost Leads: `{summary_row.get('lost_lead_count') or 0}`",
        f"- Active Waiting On Us: `{summary_row.get('active_waiting_on_us_count') or 0}`",
        f"- Active Stalled Leads: `{summary_row.get('active_stalled_lead_count') or 0}`",
        f"- Active Quote Friction: `{summary_row.get('active_quote_friction_count') or 0}`",
        f"- Active Budget Pressure: `{summary_row.get('active_budget_pressure_count') or 0}`",
        f"- Median Response (hrs): `{summary_row.get('median_message_response_hours') or 'n/a'}`",
        f"- Communication Mix: {summary_row.get('dominant_communication_mix') or ''}",
        f"- Top Assignment Lanes: {summary_row.get('top_assignment_lanes') or ''}",
        f"- Top Owners: {summary_row.get('top_owners') or ''}",
        f"- Focus: {summary_row.get('recommended_focus') or ''}",
        "",
        "## Top Leads",
    ]
    for row in lead_rows[:20]:
        lines.append(
            f"- `{row.get('priority_score') or 0}` | {row.get('lead_name') or ''} | "
            f"{row.get('lead_owner_name') or ''} | {row.get('stage_label') or ''} | {row.get('source_specific_move') or ''}"
        )
    if not lead_rows:
        lines.append("- None.")
    lines.append("")
    return "\n".join(lines)


def build_lane_detail_markdown(summary_row: Dict[str, Any], lead_rows: Sequence[Dict[str, Any]]) -> str:
    lines = [
        f"# Assignment Lane: {summary_row.get('assignment_lane') or ''}",
        "",
        "## Snapshot",
        f"- Leads: `{summary_row.get('lead_count') or 0}`",
        f"- Active Leads: `{summary_row.get('active_lead_count') or 0}`",
        f"- Won Leads: `{summary_row.get('won_lead_count') or 0}`",
        f"- Lost Leads: `{summary_row.get('lost_lead_count') or 0}`",
        f"- Active Waiting On Us: `{summary_row.get('active_waiting_on_us_count') or 0}`",
        f"- Active Stalled Leads: `{summary_row.get('active_stalled_lead_count') or 0}`",
        f"- Active Quote Friction: `{summary_row.get('active_quote_friction_count') or 0}`",
        f"- Median Response (hrs): `{summary_row.get('median_message_response_hours') or 'n/a'}`",
        f"- Top Sources: {summary_row.get('top_sources') or ''}",
        f"- Top Customer Types: {summary_row.get('top_customer_types') or ''}",
        f"- Communication Mix: {summary_row.get('dominant_communication_mix') or ''}",
        f"- Focus: {summary_row.get('recommended_focus') or ''}",
        "",
        "## Top Leads",
    ]
    for row in lead_rows[:20]:
        lines.append(
            f"- `{row.get('priority_score') or 0}` | {row.get('lead_name') or ''} | "
            f"{row.get('primary_source_channel') or ''} | {row.get('stage_label') or ''} | {row.get('source_specific_move') or ''}"
        )
    if not lead_rows:
        lines.append("- None.")
    lines.append("")
    return "\n".join(lines)


def summarize_source_group(name: str, rows: Sequence[Dict[str, Any]], path: Path) -> Dict[str, Any]:
    stage_type_counts = Counter(row.get("stage_type") or "" for row in rows)
    active_rows = [row for row in rows if row.get("stage_type") == "active"]
    response_values = [
        safe_float(str(row.get("median_message_response_hours") or ""))
        for row in active_rows
        if row.get("median_message_response_hours") not in {"", None}
    ]
    channel_counts = Counter()
    assignment_counts = Counter()
    customer_counts = Counter()
    owner_counts = Counter()
    stage_counts = Counter()
    total_priority = 0
    total_commitments = 0
    resolved_commitments = 0
    for row in rows:
        channel_counts["call"] += safe_int(str(row.get("call_count") or 0))
        channel_counts["sms"] += safe_int(str(row.get("sms_count") or 0))
        channel_counts["email"] += safe_int(str(row.get("email_count") or 0))
        assignment_counts[(row.get("assignment_lane") or "unknown").strip() or "unknown"] += 1
        customer_counts[(row.get("customer_type") or "unknown").strip() or "unknown"] += 1
        owner_counts[(row.get("lead_owner_name") or "unknown").strip() or "unknown"] += 1
        stage_counts[(row.get("stage_label") or "unknown").strip() or "unknown"] += 1
        total_priority += safe_int(str(row.get("priority_score") or 0))
        total_commitments += safe_int(str(row.get("total_commitment_count") or 0))
        resolved_commitments += safe_int(str(row.get("resolved_commitment_count") or 0))

    active_waiting = sum(1 for row in active_rows if (row.get("waiting_on_us") or "").lower() == "true")
    active_stalled = sum(1 for row in active_rows if (row.get("stall_state") or "") != "healthy_momentum")
    active_quote = sum(1 for row in active_rows if safe_int(str(row.get("quote_friction_count") or 0)) > 0)
    active_budget = sum(1 for row in active_rows if safe_int(str(row.get("budget_pressure_count") or 0)) > 0)
    due_today_leads = sum(1 for row in rows if safe_int(str(row.get("due_today_count") or 0)) > 0)
    due_48h_leads = sum(1 for row in rows if safe_int(str(row.get("due_48h_count") or 0)) > 0)
    median_response = median_value(response_values)
    average_priority = (total_priority / len(rows)) if rows else None

    if active_waiting:
        focus = "Fast-response pressure is the main drag here; reply speed matters more than anything else."
    elif active_quote:
        focus = "Commercial clarity is the main lever; tighten quote options, scope framing, and decision asks."
    elif active_stalled:
        focus = "Momentum is cooling in this source; revive leads with one concrete next-step ask."
    elif stage_type_counts.get("won", 0):
        focus = "This source is already producing booked business; keep handoff quality and promise follow-through tight."
    else:
        focus = "Keep the lane moving with the next milestone and avoid drifting into passive follow-up."

    return {
        "source_channel": name,
        "source_family": source_family(name),
        "lead_count": len(rows),
        "active_lead_count": stage_type_counts.get("active", 0),
        "won_lead_count": stage_type_counts.get("won", 0),
        "lost_lead_count": stage_type_counts.get("lost", 0),
        "lead_only_count": stage_type_counts.get("lead_only", 0),
        "active_waiting_on_us_count": active_waiting,
        "active_stalled_lead_count": active_stalled,
        "active_quote_friction_count": active_quote,
        "active_budget_pressure_count": active_budget,
        "due_today_lead_count": due_today_leads,
        "due_48h_lead_count": due_48h_leads,
        "median_message_response_hours": format_hours(median_response),
        "average_priority_score": format_number(average_priority),
        "promise_resolution_rate_pct": format_pct(resolved_commitments, total_commitments),
        "total_communication_count": sum(safe_int(str(row.get("total_communication_count") or 0)) for row in rows),
        "call_count": sum(safe_int(str(row.get("call_count") or 0)) for row in rows),
        "sms_count": sum(safe_int(str(row.get("sms_count") or 0)) for row in rows),
        "email_count": sum(safe_int(str(row.get("email_count") or 0)) for row in rows),
        "inbound_count": sum(safe_int(str(row.get("inbound_count") or 0)) for row in rows),
        "outbound_count": sum(safe_int(str(row.get("outbound_count") or 0)) for row in rows),
        "dominant_communication_mix": join_top_counts(channel_counts, limit=3),
        "top_assignment_lanes": join_top_counts(assignment_counts, limit=3),
        "top_customer_types": join_top_counts(customer_counts, limit=3),
        "top_stage_labels": join_top_counts(stage_counts, limit=3),
        "top_owners": join_top_counts(owner_counts, limit=3),
        "recommended_focus": focus,
        "source_channel_summary": compact_text(
            f"{name}: active {stage_type_counts.get('active', 0)}, won {stage_type_counts.get('won', 0)}, "
            f"stalled active {active_stalled}, quote friction {active_quote}, waiting on us {active_waiting}, "
            f"dominant comms {join_top_counts(channel_counts, limit=3) or 'none'}, "
            f"median response {hours_label_from_value(median_response)}.",
            420,
        ),
        "source_path": str(path),
    }


def summarize_lane_group(name: str, rows: Sequence[Dict[str, Any]], path: Path) -> Dict[str, Any]:
    stage_type_counts = Counter(row.get("stage_type") or "" for row in rows)
    active_rows = [row for row in rows if row.get("stage_type") == "active"]
    response_values = [
        safe_float(str(row.get("median_message_response_hours") or ""))
        for row in active_rows
        if row.get("median_message_response_hours") not in {"", None}
    ]
    channel_counts = Counter()
    source_counts = Counter()
    customer_counts = Counter()
    total_priority = 0
    total_commitments = 0
    resolved_commitments = 0
    for row in rows:
        channel_counts["call"] += safe_int(str(row.get("call_count") or 0))
        channel_counts["sms"] += safe_int(str(row.get("sms_count") or 0))
        channel_counts["email"] += safe_int(str(row.get("email_count") or 0))
        source_counts[(row.get("primary_source_channel") or "unknown").strip() or "unknown"] += 1
        customer_counts[(row.get("customer_type") or "unknown").strip() or "unknown"] += 1
        total_priority += safe_int(str(row.get("priority_score") or 0))
        total_commitments += safe_int(str(row.get("total_commitment_count") or 0))
        resolved_commitments += safe_int(str(row.get("resolved_commitment_count") or 0))

    active_waiting = sum(1 for row in active_rows if (row.get("waiting_on_us") or "").lower() == "true")
    active_stalled = sum(1 for row in active_rows if (row.get("stall_state") or "") != "healthy_momentum")
    active_quote = sum(1 for row in active_rows if safe_int(str(row.get("quote_friction_count") or 0)) > 0)
    active_budget = sum(1 for row in active_rows if safe_int(str(row.get("budget_pressure_count") or 0)) > 0)
    due_today_leads = sum(1 for row in rows if safe_int(str(row.get("due_today_count") or 0)) > 0)
    due_48h_leads = sum(1 for row in rows if safe_int(str(row.get("due_48h_count") or 0)) > 0)
    median_response = median_value(response_values)
    average_priority = (total_priority / len(rows)) if rows else None

    if active_waiting:
        focus = "This lane has live reply pressure; outbound follow-through speed matters."
    elif active_quote:
        focus = "Commercial friction is where this lane can win or lose time."
    elif active_stalled:
        focus = "This lane needs sharper reactivation and milestone setting."
    else:
        focus = "Lane looks comparatively healthy; keep the handoffs and next-step asks tight."

    return {
        "assignment_lane": name,
        "lead_count": len(rows),
        "active_lead_count": stage_type_counts.get("active", 0),
        "won_lead_count": stage_type_counts.get("won", 0),
        "lost_lead_count": stage_type_counts.get("lost", 0),
        "lead_only_count": stage_type_counts.get("lead_only", 0),
        "active_waiting_on_us_count": active_waiting,
        "active_stalled_lead_count": active_stalled,
        "active_quote_friction_count": active_quote,
        "active_budget_pressure_count": active_budget,
        "due_today_lead_count": due_today_leads,
        "due_48h_lead_count": due_48h_leads,
        "median_message_response_hours": format_hours(median_response),
        "average_priority_score": format_number(average_priority),
        "promise_resolution_rate_pct": format_pct(resolved_commitments, total_commitments),
        "total_communication_count": sum(safe_int(str(row.get("total_communication_count") or 0)) for row in rows),
        "call_count": sum(safe_int(str(row.get("call_count") or 0)) for row in rows),
        "sms_count": sum(safe_int(str(row.get("sms_count") or 0)) for row in rows),
        "email_count": sum(safe_int(str(row.get("email_count") or 0)) for row in rows),
        "dominant_communication_mix": join_top_counts(channel_counts, limit=3),
        "top_sources": join_top_counts(source_counts, limit=4),
        "top_customer_types": join_top_counts(customer_counts, limit=3),
        "recommended_focus": focus,
        "assignment_lane_summary": compact_text(
            f"{name}: active {stage_type_counts.get('active', 0)}, won {stage_type_counts.get('won', 0)}, "
            f"stalled active {active_stalled}, quote friction {active_quote}, waiting on us {active_waiting}, "
            f"top sources {join_top_counts(source_counts, limit=3) or 'unknown'}, "
            f"median response {hours_label_from_value(median_response)}.",
            420,
        ),
        "assignment_lane_path": str(path),
    }


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    output_dir = args.output_dir
    by_source_dir = output_dir / "by_source"
    by_assignment_lane_dir = output_dir / "by_assignment_lane"
    ensure_dir(output_dir)
    ensure_dir(by_source_dir)
    ensure_dir(by_assignment_lane_dir)

    deal_rows = load_csv_rows(args.deal_sheets_csv)
    follow_up_rows = {row["lead_id"]: row for row in load_csv_rows(args.follow_up_csv)}
    seller_rows = {row["lead_id"]: row for row in load_csv_rows(args.seller_csv)}
    pricing_rows = {row["lead_id"]: row for row in load_csv_rows(args.pricing_csv)}
    schedule_rows = {row["lead_id"]: row for row in load_csv_rows(args.schedule_csv)}
    communication_rows = load_csv_rows(args.communications_csv)
    opportunities = load_json(args.opportunities_path)

    deal_by_lead = {row["lead_id"]: row for row in deal_rows}
    communications_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in communication_rows:
        lead_id = row.get("lead_id") or ""
        if lead_id:
            communications_by_lead[lead_id].append(row)

    opportunities_by_lead: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in opportunities:
        lead_id = row.get("lead_id") or ""
        if lead_id:
            opportunities_by_lead[lead_id].append(row)

    lead_ids = set(deal_by_lead.keys()) | set(seller_rows.keys()) | set(pricing_rows.keys()) | set(schedule_rows.keys()) | set(follow_up_rows.keys())

    lead_rows_output: List[Dict[str, Any]] = []
    lead_payloads: List[Dict[str, Any]] = []

    for lead_id in sorted(lead_ids):
        deal_row = deal_by_lead.get(lead_id, {})
        follow_up_row = follow_up_rows.get(lead_id, {})
        seller_row = seller_rows.get(lead_id, {})
        pricing_row = pricing_rows.get(lead_id, {})
        schedule_row = schedule_rows.get(lead_id, {})

        lead_dir = lead_dir_from_sources(
            deal_row.get("deal_sheet_path") or "",
            seller_row.get("lead_seller_signal_sheet_path") or "",
            follow_up_row.get("brief_path") or "",
            schedule_row.get("lead_schedule_commitment_sheet_path") or "",
        )
        if lead_dir is None:
            continue

        opportunity_context = build_opportunity_context(opportunities_by_lead.get(lead_id, []))

        follow_up_source = (follow_up_row.get("source_channels") or "").strip()
        source_channels = follow_up_source or opportunity_context.get("source_channels") or "unknown"
        customer_type = (follow_up_row.get("customer_type") or "").strip() or opportunity_context.get("customer_type") or "unknown"
        assignment_lane = (follow_up_row.get("assignment_lane") or "").strip() or opportunity_context.get("assignment_lane") or "unknown"
        internal_flags = (follow_up_row.get("internal_flags") or "").strip() or opportunity_context.get("internal_flags") or ""

        if follow_up_source:
            attribution_origin = "follow_up_queue"
            attribution_updated_utc = follow_up_row.get("latest_observed_activity_utc") or opportunity_context.get("anchor_updated_utc") or ""
        else:
            attribution_origin = opportunity_context.get("source_context_source") or "opportunity_export"
            attribution_updated_utc = opportunity_context.get("source_updated_utc") or opportunity_context.get("anchor_updated_utc") or ""

        source_values = split_pipe_values(source_channels) or ["unknown"]
        primary_source_channel = source_values[0]
        primary_source_family = source_family(primary_source_channel)

        communication_summary = summarize_communications(communications_by_lead.get(lead_id, []))
        waiting_on_us = (
            (seller_row.get("waiting_on_us") or follow_up_row.get("waiting_on_us") or "").strip().lower() == "true"
        )

        lead_name = (
            deal_row.get("lead_name")
            or seller_row.get("lead_name")
            or follow_up_row.get("lead_name")
            or schedule_row.get("lead_name")
            or ""
        )
        lead_owner_name = (
            deal_row.get("lead_owner_name")
            or seller_row.get("lead_owner_name")
            or follow_up_row.get("lead_owner_name")
            or schedule_row.get("lead_owner_name")
            or "Unassigned"
        )
        pipeline_name = (
            deal_row.get("pipeline_name")
            or seller_row.get("pipeline_name")
            or follow_up_row.get("pipeline_name")
            or schedule_row.get("pipeline_name")
            or ""
        )
        stage_label = (
            deal_row.get("stage_label")
            or seller_row.get("stage_label")
            or follow_up_row.get("stage_label")
            or schedule_row.get("stage_label")
            or ""
        )
        stage_type = (
            deal_row.get("stage_type")
            or seller_row.get("stage_type")
            or follow_up_row.get("stage_type")
            or ""
        )

        priority_score = max(
            safe_int(follow_up_row.get("priority_score")),
            safe_int(seller_row.get("priority_score")),
            safe_int(deal_row.get("follow_up_priority_score")),
            safe_int(deal_row.get("readiness_score")),
        )
        total_commitment_count = safe_int(seller_row.get("total_commitment_count"))
        resolved_commitment_count = safe_int(seller_row.get("resolved_commitment_count"))
        pending_commitment_count = safe_int(seller_row.get("pending_commitment_count"))
        quote_friction_count = safe_int(seller_row.get("quote_friction_count")) or (
            safe_int(pricing_row.get("quote_revision_count"))
            + safe_int(pricing_row.get("budget_pressure_count"))
            + safe_int(pricing_row.get("package_compare_count"))
        )
        budget_pressure_count = safe_int(seller_row.get("budget_pressure_count")) or safe_int(pricing_row.get("budget_pressure_count"))
        due_today_count = safe_int(seller_row.get("due_today_count")) or safe_int(schedule_row.get("due_today_count"))
        due_48h_count = safe_int(seller_row.get("due_48h_count")) or safe_int(schedule_row.get("due_48h_count"))

        source_specific_move = determine_source_move(
            primary_source_channel=primary_source_channel,
            waiting_on_us=waiting_on_us,
            quote_friction_count=quote_friction_count,
            due_today_count=due_today_count,
            due_48h_count=due_48h_count,
            seller_next_move=seller_row.get("seller_next_move") or "",
            pricing_action=pricing_row.get("pricing_action") or "",
            schedule_summary=schedule_row.get("next_due_summary") or schedule_row.get("event_watch_reason") or "",
            operator_move=deal_row.get("operator_move") or "",
            recommended_action=follow_up_row.get("recommended_action") or "",
        )
        source_health_state = infer_source_health_state(
            stage_type=stage_type,
            waiting_on_us=waiting_on_us,
            quote_friction_count=quote_friction_count,
            budget_pressure_count=budget_pressure_count,
            stall_state=seller_row.get("stall_state") or "",
        )

        row_out: Dict[str, Any] = {
            "lead_id": lead_id,
            "lead_name": lead_name,
            "lead_owner_name": lead_owner_name,
            "pipeline_name": pipeline_name,
            "stage_label": stage_label,
            "stage_type": stage_type,
            "source_channels": source_channels,
            "primary_source_channel": primary_source_channel,
            "primary_source_family": primary_source_family,
            "source_channel_count": len(source_values),
            "customer_type": customer_type,
            "assignment_lane": assignment_lane,
            "internal_flags": internal_flags,
            "attribution_origin": attribution_origin,
            "attribution_updated_utc": attribution_updated_utc,
            "opportunity_count": opportunity_context.get("opportunity_count") or 0,
            "anchor_opportunity_status": opportunity_context.get("anchor_status_label") or "",
            "latest_observed_activity_utc": (
                seller_row.get("latest_observed_activity_utc")
                or follow_up_row.get("latest_observed_activity_utc")
                or ""
            ),
            "waiting_on_us": "True" if waiting_on_us else "False",
            "days_since_observed": safe_int(follow_up_row.get("days_since_observed")),
            "priority_score": priority_score,
            "readiness_bucket": deal_row.get("readiness_bucket") or "",
            "queue_bucket": follow_up_row.get("queue_bucket") or "",
            "pricing_posture": pricing_row.get("pricing_posture") or seller_row.get("pricing_posture") or "",
            "source_health_state": source_health_state,
            "stall_state": seller_row.get("stall_state") or "",
            "conversation_quality_state": seller_row.get("conversation_quality_state") or "",
            "message_turn_count": safe_int(seller_row.get("message_turn_count")),
            "responded_message_turn_count": safe_int(seller_row.get("responded_message_turn_count")),
            "unanswered_message_turn_count": safe_int(seller_row.get("unanswered_message_turn_count")),
            "median_message_response_hours": seller_row.get("median_message_response_hours") or "",
            "total_commitment_count": total_commitment_count,
            "resolved_commitment_count": resolved_commitment_count,
            "pending_commitment_count": pending_commitment_count,
            "promise_resolution_rate_pct": seller_row.get("promise_resolution_rate_pct") or format_pct(resolved_commitment_count, total_commitment_count),
            "quote_friction_count": quote_friction_count,
            "budget_pressure_count": budget_pressure_count,
            "due_today_count": due_today_count,
            "due_48h_count": due_48h_count,
            "total_communication_count": communication_summary["total_communication_count"],
            "call_count": communication_summary["call_count"],
            "sms_count": communication_summary["sms_count"],
            "email_count": communication_summary["email_count"],
            "inbound_count": communication_summary["inbound_count"],
            "outbound_count": communication_summary["outbound_count"],
            "dominant_communication_channel": communication_summary["dominant_communication_channel"],
            "communication_mix": communication_summary["communication_mix"],
            "source_specific_move": source_specific_move,
            "attribution_summary": compact_text(
                f"{lead_name}: primary source {primary_source_channel}; lane {assignment_lane or 'unknown'}; "
                f"owner {lead_owner_name}; stage {stage_label or 'unknown'}; waiting on us {waiting_on_us}; "
                f"stalled {seller_row.get('stall_state') or 'unknown'}; quote friction {quote_friction_count}; "
                f"dominant comms {communication_summary['communication_mix'] or 'none'}.",
                420,
            ),
            "lead_source_attribution_sheet_path": str(lead_dir / "lead_source_attribution_sheet.md"),
            "lead_source_attribution_sheet_json_path": str(lead_dir / "lead_source_attribution_sheet.json"),
        }

        payload = dict(row_out)
        write_json(lead_dir / "lead_source_attribution_sheet.json", payload)
        (lead_dir / "lead_source_attribution_sheet.md").write_text(build_profile_markdown(payload), encoding="utf-8")

        lead_rows_output.append(row_out)
        lead_payloads.append(payload)

    lead_rows_output = sorted(
        lead_rows_output,
        key=lambda row: (
            -safe_int(str(row.get("priority_score") or 0)),
            -safe_int(str(row.get("quote_friction_count") or 0)),
            row.get("lead_name") or "",
        ),
    )

    source_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    lane_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    primary_source_owner_groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    source_stage_groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)

    for row in lead_rows_output:
        lane_groups[(row.get("assignment_lane") or "unknown").strip() or "unknown"].append(row)
        primary_source = (row.get("primary_source_channel") or "unknown").strip() or "unknown"
        owner_name = (row.get("lead_owner_name") or "unknown").strip() or "unknown"
        primary_source_owner_groups[(primary_source, owner_name)].append(row)
        if row.get("stage_type") == "active":
            source_stage_groups[(primary_source, row.get("stage_label") or "unknown")].append(row)
        for source_name in split_pipe_values(row.get("source_channels")) or [primary_source]:
            source_groups[source_name].append(row)

    source_summary_rows: List[Dict[str, Any]] = []
    for source_name, rows in sorted(source_groups.items(), key=lambda item: (-len(item[1]), item[0].lower())):
        source_dir = by_source_dir / slugify(source_name)
        ensure_dir(source_dir)
        sorted_rows = sorted(
            rows,
            key=lambda row: (
                -safe_int(str(row.get("priority_score") or 0)),
                -safe_int(str(row.get("quote_friction_count") or 0)),
                row.get("lead_name") or "",
            ),
        )
        summary_row = summarize_source_group(source_name, sorted_rows, source_dir / "source.md")
        (source_dir / "source.md").write_text(build_source_detail_markdown(summary_row, sorted_rows), encoding="utf-8")
        write_csv(source_dir / "leads.csv", sorted_rows)
        write_jsonl(source_dir / "leads.jsonl", sorted_rows)
        source_summary_rows.append(summary_row)

    lane_summary_rows: List[Dict[str, Any]] = []
    for lane_name, rows in sorted(lane_groups.items(), key=lambda item: (-len(item[1]), item[0].lower())):
        lane_dir = by_assignment_lane_dir / slugify(lane_name)
        ensure_dir(lane_dir)
        sorted_rows = sorted(
            rows,
            key=lambda row: (
                -safe_int(str(row.get("priority_score") or 0)),
                -safe_int(str(row.get("quote_friction_count") or 0)),
                row.get("lead_name") or "",
            ),
        )
        summary_row = summarize_lane_group(lane_name, sorted_rows, lane_dir / "assignment_lane.md")
        (lane_dir / "assignment_lane.md").write_text(build_lane_detail_markdown(summary_row, sorted_rows), encoding="utf-8")
        write_csv(lane_dir / "leads.csv", sorted_rows)
        write_jsonl(lane_dir / "leads.jsonl", sorted_rows)
        lane_summary_rows.append(summary_row)

    source_summary_rows = sorted(
        source_summary_rows,
        key=lambda row: (
            -safe_int(str(row.get("active_lead_count") or 0)),
            -safe_int(str(row.get("lead_count") or 0)),
            row.get("source_channel") or "",
        ),
    )
    lane_summary_rows = sorted(
        lane_summary_rows,
        key=lambda row: (
            -safe_int(str(row.get("active_lead_count") or 0)),
            -safe_int(str(row.get("lead_count") or 0)),
            row.get("assignment_lane") or "",
        ),
    )

    source_stall_rows = sorted(
        source_summary_rows,
        key=lambda row: (
            -safe_int(str(row.get("active_stalled_lead_count") or 0)),
            -safe_int(str(row.get("active_waiting_on_us_count") or 0)),
            row.get("source_channel") or "",
        ),
    )
    source_quote_rows = sorted(
        source_summary_rows,
        key=lambda row: (
            -safe_int(str(row.get("active_quote_friction_count") or 0)),
            -safe_int(str(row.get("active_budget_pressure_count") or 0)),
            row.get("source_channel") or "",
        ),
    )

    source_owner_matrix_rows: List[Dict[str, Any]] = []
    for (source_name, owner_name), rows in sorted(
        primary_source_owner_groups.items(),
        key=lambda item: (-len(item[1]), item[0][0].lower(), item[0][1].lower()),
    ):
        active_rows = [row for row in rows if row.get("stage_type") == "active"]
        response_values = [
            safe_float(str(row.get("median_message_response_hours") or ""))
            for row in active_rows
            if row.get("median_message_response_hours") not in {"", None}
        ]
        source_owner_matrix_rows.append(
            {
                "primary_source_channel": source_name,
                "lead_owner_name": owner_name,
                "lead_count": len(rows),
                "active_lead_count": len(active_rows),
                "won_lead_count": sum(1 for row in rows if row.get("stage_type") == "won"),
                "active_waiting_on_us_count": sum(1 for row in active_rows if (row.get("waiting_on_us") or "").lower() == "true"),
                "active_stalled_lead_count": sum(1 for row in active_rows if (row.get("stall_state") or "") != "healthy_momentum"),
                "active_quote_friction_count": sum(1 for row in active_rows if safe_int(str(row.get("quote_friction_count") or 0)) > 0),
                "median_message_response_hours": format_hours(median_value(response_values)),
                "average_priority_score": format_number(mean_value([safe_int(str(row.get("priority_score") or 0)) for row in rows])),
            }
        )

    source_stage_friction_rows: List[Dict[str, Any]] = []
    for (source_name, stage_label), rows in sorted(
        source_stage_groups.items(),
        key=lambda item: (
            -sum(1 for row in item[1] if (row.get("stall_state") or "") != "healthy_momentum"),
            -len(item[1]),
            item[0][0].lower(),
            item[0][1].lower(),
        ),
    ):
        response_values = [
            safe_float(str(row.get("median_message_response_hours") or ""))
            for row in rows
            if row.get("median_message_response_hours") not in {"", None}
        ]
        assignment_counts = Counter((row.get("assignment_lane") or "unknown").strip() or "unknown" for row in rows)
        source_stage_friction_rows.append(
            {
                "primary_source_channel": source_name,
                "source_family": source_family(source_name),
                "stage_label": stage_label,
                "lead_count": len(rows),
                "active_waiting_on_us_count": sum(1 for row in rows if (row.get("waiting_on_us") or "").lower() == "true"),
                "active_stalled_lead_count": sum(1 for row in rows if (row.get("stall_state") or "") != "healthy_momentum"),
                "active_quote_friction_count": sum(1 for row in rows if safe_int(str(row.get("quote_friction_count") or 0)) > 0),
                "active_budget_pressure_count": sum(1 for row in rows if safe_int(str(row.get("budget_pressure_count") or 0)) > 0),
                "median_message_response_hours": format_hours(median_value(response_values)),
                "top_assignment_lane": join_top_counts(assignment_counts, limit=2),
                "summary": compact_text(
                    f"{source_name} / {stage_label}: stalled {sum(1 for row in rows if (row.get('stall_state') or '') != 'healthy_momentum')}, "
                    f"quote friction {sum(1 for row in rows if safe_int(str(row.get('quote_friction_count') or 0)) > 0)}, "
                    f"waiting on us {sum(1 for row in rows if (row.get('waiting_on_us') or '').lower() == 'true')}.",
                    320,
                ),
            }
        )

    write_csv(normalized_dir / "lead_source_attribution_profiles.csv", lead_rows_output)
    write_jsonl(normalized_dir / "lead_source_attribution_profiles.jsonl", lead_payloads)
    write_csv(normalized_dir / "source_channel_summary.csv", source_summary_rows)
    write_csv(normalized_dir / "assignment_lane_summary.csv", lane_summary_rows)
    write_csv(normalized_dir / "source_stall_board.csv", source_stall_rows)
    write_csv(normalized_dir / "source_quote_friction_board.csv", source_quote_rows)
    write_csv(normalized_dir / "source_owner_matrix.csv", source_owner_matrix_rows)
    write_csv(normalized_dir / "source_stage_friction_board.csv", source_stage_friction_rows)

    (output_dir / "README.md").write_text(
        "\n".join(
            [
                "# Source Channel Intelligence",
                "",
                "This layer compresses source, assignment-lane, and communication-mix context into one attribution view so we can see which entry paths are producing momentum, friction, and booked business.",
                "",
                "## Snapshot",
                f"- Lead attribution sheets: `{len(lead_rows_output)}`",
                f"- Source channel summaries: `{len(source_summary_rows)}`",
                f"- Assignment lane summaries: `{len(lane_summary_rows)}`",
                f"- Source / stage friction rows: `{len(source_stage_friction_rows)}`",
                "",
                "## Key Files",
                "- `source_channel_overview.md`: source-by-source operating view",
                "- `assignment_lane_overview.md`: lane-by-lane operating view",
                "- `source_stall_board.md`: sources generating the most active stall pressure",
                "- `source_quote_friction_board.md`: sources generating the most commercial friction",
                "- `../normalized/lead_source_attribution_profiles.csv`: one-row-per-lead source and channel profile",
                "- `../normalized/source_channel_summary.csv`: one-row-per-source summary",
                "- `../normalized/assignment_lane_summary.csv`: one-row-per-assignment-lane summary",
                "- `../normalized/source_owner_matrix.csv`: primary source x owner matrix",
                "- `../normalized/source_stage_friction_board.csv`: primary source x active-stage friction matrix",
                "",
                "## Notes",
                "- Source summaries are multi-attributed when a lead carries multiple source labels.",
                "- The lead sheet itself keeps a primary source channel so owner/source matrices stay stable.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (output_dir / "source_channel_overview.md").write_text(
        build_summary_markdown("Source Channel Overview", source_summary_rows, "source_channel", "source_channel_summary"),
        encoding="utf-8",
    )
    (output_dir / "assignment_lane_overview.md").write_text(
        build_summary_markdown("Assignment Lane Overview", lane_summary_rows, "assignment_lane", "assignment_lane_summary"),
        encoding="utf-8",
    )
    (output_dir / "source_stall_board.md").write_text(
        build_board_markdown(
            "Source Stall Board",
            source_stall_rows,
            "source_channel",
            "active_stalled_lead_count",
            "source_channel_summary",
            "recommended_focus",
        ),
        encoding="utf-8",
    )
    (output_dir / "source_quote_friction_board.md").write_text(
        build_board_markdown(
            "Source Quote Friction Board",
            source_quote_rows,
            "source_channel",
            "active_quote_friction_count",
            "source_channel_summary",
            "recommended_focus",
        ),
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "lead_profiles": len(lead_rows_output),
                "source_summaries": len(source_summary_rows),
                "assignment_lane_summaries": len(lane_summary_rows),
                "source_stage_rows": len(source_stage_friction_rows),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
