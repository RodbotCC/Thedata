#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_LEAD_BRIEFS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_memory_briefs.csv"
DEFAULT_MASTER_TIMELINE_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_master_timeline.csv"
DEFAULT_OPPORTUNITIES_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /Comeketo Catering opportunities 2026-03-26 18-32.json")
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "operational_intelligence"

FIELD_ALIASES = {
    "custom.cf_FV2xBkviv7BAQZkkjUf8NUOc3fOpPTObMy5lVxZbyiP": "event_datetime_utc",
    "custom.cf_nQLULOLLmtUAh9OwcpJibPc5pQKIqpFOjdGSTwC9ePO": "guest_count_text",
    "custom.cf_goMfyKkS7pFUhmo0xvrl1JvQv1KLaQkfoVo0j93dvhe": "event_type",
    "custom.cf_3LZk8uGw0lIvPpNzOMvFn4WwiCXbI91X66Ujvt8UJPx": "venue_type",
    "custom.cf_ge7qOebiWpyPvuv7xkzNaYpM8PsmOeNvXasXFOtPXRt": "source_channels",
    "custom.cf_fs7mrfN5x0M20CyoltczyVg8t0Xul5GFvkC4FNUKvY6": "customer_type",
    "lead_custom.cf_imMCu3Pod85W2K5ZkVUjBD7m3E5iZxbSf3mueeNpibM": "budget_band",
    "lead_custom.cf_bMmcNeKx2ltaIMgNPLXg3cQCVcKguZe28ilBnOilnO5": "venue_name",
    "lead_custom.cf_l7gEKQsPZLqjEw35V4WB6ewUuc84dS3nohisc0BeCdy": "venue_address",
    "lead_custom.cf_xD3AKAnhwHeZy3OAUrZvbbFYiDPFwtFfTrSLAbDbmA2": "venue_city",
    "lead_custom.cf_pXTVEI1DdERiT91NKuWAndlV6WuS4n6ZG2334fBR4b8": "postal_code",
    "lead_custom.cf_9vVeQH1oYtJbtdHoL9VPwGhNpuCzVCgi95p7MCasszj": "internal_flags",
    "lead_custom.cf_xF8FLufgEx9bsijfRAfHhgIrPBQ5ajuohcazC7OtNmT": "assignment_lane",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build operational intelligence layers from the normalized Comeketo CRM library.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--lead-briefs-csv", type=Path, default=DEFAULT_LEAD_BRIEFS_CSV)
    parser.add_argument("--master-timeline-csv", type=Path, default=DEFAULT_MASTER_TIMELINE_CSV)
    parser.add_argument("--opportunities-path", type=Path, default=DEFAULT_OPPORTUNITIES_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> Any:
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


def clean_label(value: Optional[str], fallback: str) -> str:
    text = (value or "").strip()
    return text or fallback


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


def compact_text(value: Optional[str], limit: int = 220) -> str:
    text = " ".join((value or "").split())
    return text[:limit]


def list_to_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, list):
        return " | ".join(str(item) for item in value if item not in (None, ""))
    return str(value)


def choose_current_opportunity(opportunities: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not opportunities:
        return None
    status_rank = {"active": 0, "won": 1, "lost": 2}
    sorted_rows = sorted(
        opportunities,
        key=lambda row: (
            status_rank.get((row.get("status_type") or "").lower(), 99),
            row.get("date_updated") or row.get("date_created") or "",
        ),
    )
    return sorted_rows[0]


def extract_guest_range(value: str) -> Tuple[Optional[int], Optional[int]]:
    if not value:
        return None, None
    numbers = [int(match) for match in re.findall(r"\d+", value)]
    if not numbers:
        return None, None
    if len(numbers) >= 2 and any(token in value for token in ["-", "to", "–", "—"]):
        return min(numbers[0], numbers[1]), max(numbers[0], numbers[1])
    return numbers[0], numbers[0]


def infer_event_date(current_opportunity: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    if not current_opportunity:
        return "", ""
    custom_value = current_opportunity.get("custom.cf_FV2xBkviv7BAQZkkjUf8NUOc3fOpPTObMy5lVxZbyiP")
    dt = parse_iso(custom_value)
    if dt:
        return iso_z(dt), "opportunity_custom_event_datetime"
    fallback = current_opportunity.get("date_won")
    dt = parse_iso(fallback) if fallback else None
    if dt:
        return iso_z(dt), "close_date_won_fallback"
    return "", ""


def infer_latest_signal(timeline_rows: List[Dict[str, str]]) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, str]], Optional[Dict[str, str]], bool]:
    latest_incoming = next((row for row in timeline_rows if (row.get("direction") or "") == "incoming"), None)
    latest_outgoing = next((row for row in timeline_rows if (row.get("direction") or "") == "outgoing"), None)
    latest_call = next((row for row in timeline_rows if (row.get("event_family") or "") == "call"), None)

    incoming_dt = parse_iso((latest_incoming or {}).get("event_datetime_utc"))
    outgoing_dt = parse_iso((latest_outgoing or {}).get("event_datetime_utc"))
    waiting_on_us = bool(incoming_dt and (not outgoing_dt or incoming_dt > outgoing_dt))
    return latest_incoming, latest_outgoing, latest_call, waiting_on_us


def stage_base_priority(stage_type: str, stage_label: str) -> int:
    label = stage_label.lower()
    if stage_type == "active":
        if "quote" in label:
            return 80
        if "asking for quote" in label:
            return 82
        if "tasting" in label:
            return 76
        if "qualified" in label:
            return 68
        if "prospect" in label:
            return 58
        return 65
    if stage_type == "won":
        if "job complete" in label:
            return 25
        if "fully paid" in label:
            return 45
        return 55
    if stage_type == "lead_only":
        return 42
    return 0


def stage_stale_threshold(stage_type: str, stage_label: str) -> int:
    label = stage_label.lower()
    if stage_type == "active":
        if "quote" in label:
            return 2
        if "tasting" in label:
            return 3
        if "qualified" in label:
            return 4
        if "prospect" in label:
            return 5
        return 4
    if stage_type == "won":
        return 7
    return 5


def build_priority(
    facts: Dict[str, Any],
    latest_incoming: Optional[Dict[str, str]],
    latest_outgoing: Optional[Dict[str, str]],
    waiting_on_us: bool,
) -> Tuple[int, str, str, List[str]]:
    reasons: List[str] = []
    stage_type = facts["stage_type"]
    stage_label = facts["stage_label"]
    score = stage_base_priority(stage_type, stage_label)

    latest_observed_dt = parse_iso(facts.get("latest_observed_activity_utc"))
    now_utc = datetime.now(timezone.utc)
    days_since_observed = None
    if latest_observed_dt:
        days_since_observed = max(0, (now_utc - latest_observed_dt).days)
        facts["days_since_observed"] = days_since_observed
    else:
        facts["days_since_observed"] = ""

    if waiting_on_us:
        score += 24
        reasons.append("latest inbound is newer than latest outbound")

    if days_since_observed is not None:
        threshold = stage_stale_threshold(stage_type, stage_label)
        if days_since_observed > threshold:
            stale_boost = min(20, days_since_observed - threshold + 6)
            score += stale_boost
            reasons.append(f"no observed activity for {days_since_observed} days")

    event_dt = parse_iso(facts.get("event_datetime_utc"))
    if event_dt:
        days_until_event = (event_dt.date() - now_utc.date()).days
        facts["days_until_event"] = days_until_event
        if 0 <= days_until_event <= 14:
            score += 26
            reasons.append(f"event is in {days_until_event} days")
        elif 15 <= days_until_event <= 30:
            score += 18
            reasons.append(f"event is within 30 days")
        elif 31 <= days_until_event <= 60:
            score += 10
            reasons.append(f"event is within 60 days")
        elif (
            days_until_event < 0
            and facts.get("event_datetime_source") == "opportunity_custom_event_datetime"
            and days_until_event >= -7
            and stage_type != "lost"
        ):
            score += 12
            reasons.append("custom event date is within the last week")
    else:
        facts["days_until_event"] = ""

    latest_signal = facts.get("latest_buyer_signal") or ""
    if latest_signal:
        signal_lower = latest_signal.lower()
        if any(token in signal_lower for token in ["quote", "pricing", "price", "orçamento", "budget"]):
            score += 8
            reasons.append("buyer signal mentions pricing/quote")
        if any(token in signal_lower for token in ["menu", "entree", "sides", "appetizer", "tasting", "degustação"]):
            score += 6
            reasons.append("buyer signal asks for menu/tasting detail")

    bucket = "manual_review"
    action = "Review the lead dossier and decide the next touch."

    if stage_type == "won":
        if facts.get("days_until_event") not in ("", None) and int(facts["days_until_event"]) <= 45:
            bucket = "upcoming_booked_event"
            action = "Confirm logistics, payment state, and fulfillment details for the booked event."
        else:
            bucket = "won_pipeline_watch"
            action = "Keep the booked event visible and verify there are no open delivery questions."
    elif waiting_on_us and stage_type in {"active", "lead_only"}:
        bucket = "reply_to_inbound"
        action = "Reply to the latest inbound message and move the opportunity forward."
    elif "quote" in stage_label.lower() and stage_type == "active":
        bucket = "advance_quote"
        action = "Send or revise the quote and ask for a decision-driving next step."
    elif "tasting" in stage_label.lower() and stage_type == "active":
        bucket = "schedule_tasting"
        action = "Lock the tasting details or use the tasting conversation to move toward deposit."
    elif stage_type == "active":
        bucket = "advance_active_opportunity"
        action = "Push the active opportunity to its next milestone."
    elif stage_type == "lead_only":
        bucket = "nurture_lead"
        action = "Review the recent signal and decide whether to revive or qualify the lead."

    if stage_type == "lost" or "do not call" in stage_label.lower() or "archived" in stage_label.lower():
        score = 0
        reasons = ["suppressed: lost/do-not-contact stage"]
        bucket = "suppressed"
        action = "Do not place in active follow-up."

    facts["priority_score"] = score
    return score, bucket, action, reasons


def priority_band(score: int) -> str:
    if score >= 105:
        return "urgent"
    if score >= 85:
        return "high"
    if score >= 65:
        return "medium"
    if score > 0:
        return "low"
    return "suppressed"


def build_event_facts_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Event Facts: {payload['lead_name']}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Pipeline / Stage: {payload.get('pipeline_name') or ''} / {payload.get('stage_label') or ''}",
        f"- Stage Type: {payload.get('stage_type') or ''}",
        f"- Event Date (UTC): `{payload.get('event_datetime_utc') or ''}`",
        f"- Event Date Source: `{payload.get('event_datetime_source') or ''}`",
        f"- Event Type: {payload.get('event_type') or ''}",
        f"- Guest Count: {payload.get('guest_count_text') or ''}",
        f"- Venue Type: {payload.get('venue_type') or ''}",
        f"- Venue Name: {payload.get('venue_name') or ''}",
        f"- Venue Address: {payload.get('venue_address') or ''}",
        f"- Venue City: {payload.get('venue_city') or ''}",
        f"- Source Channels: {payload.get('source_channels') or ''}",
        f"- Budget Band: {payload.get('budget_band') or ''}",
        "",
        "## Signals",
        f"- Latest Observed Activity (UTC): `{payload.get('latest_observed_activity_utc') or ''}`",
        f"- Latest Buyer Signal: {payload.get('latest_buyer_signal') or ''}",
        f"- Suggested Next Move: {payload.get('suggested_next_move') or ''}",
        "",
    ]
    if payload.get("raw_inferred_field_map"):
        lines.extend(["## Raw Field Map"])
        for key, value in payload["raw_inferred_field_map"].items():
            lines.append(f"- `{key}` -> {value}")
        lines.append("")
    return "\n".join(lines)


def build_follow_up_markdown(queue_rows: List[Dict[str, Any]]) -> str:
    bucket_counts = Counter(row["queue_bucket"] for row in queue_rows)
    band_counts = Counter(row["priority_band"] for row in queue_rows)
    lines = [
        "# Follow-Up Queue",
        "",
        "## Snapshot",
        f"- Total actionable leads: `{len(queue_rows)}`",
        "",
        "## Priority Bands",
    ]
    for label, count in band_counts.items():
        lines.append(f"- {label}: {count}")
    lines.extend(["", "## Queue Buckets"])
    for label, count in bucket_counts.items():
        lines.append(f"- {label}: {count}")
    lines.extend(["", "## Top Queue"])
    for row in queue_rows[:75]:
        lines.append(
            f"- `{row.get('priority_score')}` | {row.get('priority_band')} | {row.get('lead_name')} | "
            f"{row.get('lead_owner_name')} | {row.get('stage_label')} | {row.get('recommended_action')}"
        )
        lines.append(f"  Why: {row.get('priority_reasons') or ''}")
    lines.append("")
    return "\n".join(lines)


def build_watchlist_markdown(rows: List[Dict[str, Any]]) -> str:
    lines = [
        "# Upcoming Event Watchlist",
        "",
        f"- Leads with event dates: `{len(rows)}`",
        "",
        "## Events",
    ]
    for row in rows[:100]:
        lines.append(
            f"- `{row.get('event_datetime_utc') or ''}` | {row.get('lead_name')} | "
            f"{row.get('stage_label')} | {row.get('guest_count_text') or ''} guests | "
            f"{row.get('event_type') or ''} | {row.get('venue_type') or row.get('venue_name') or ''}"
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

    brief_rows = load_csv_rows(args.lead_briefs_csv)
    timeline_rows = load_csv_rows(args.master_timeline_csv)
    raw_opportunities = load_json(args.opportunities_path)

    timeline_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in timeline_rows:
        timeline_by_lead[row["lead_id"]].append(row)

    opportunities_by_lead: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    lead_ids = {row["lead_id"] for row in brief_rows}
    for opportunity in raw_opportunities:
        lead_id = opportunity.get("lead_id")
        if lead_id in lead_ids:
            opportunities_by_lead[lead_id].append(opportunity)

    event_facts_rows: List[Dict[str, Any]] = []
    follow_up_rows: List[Dict[str, Any]] = []
    watchlist_rows: List[Dict[str, Any]] = []
    suppressed_rows: List[Dict[str, Any]] = []

    for brief in brief_rows:
        lead_id = brief["lead_id"]
        lead_name = brief["lead_name"]
        lead_dir = Path(brief["brief_path"]).parent
        current_opportunity = choose_current_opportunity(opportunities_by_lead.get(lead_id, []))
        timeline = timeline_by_lead.get(lead_id, [])

        latest_incoming, latest_outgoing, latest_call, waiting_on_us = infer_latest_signal(timeline)
        event_datetime_utc, event_datetime_source = infer_event_date(current_opportunity)

        stage_label = clean_label((current_opportunity or {}).get("status_label") or brief.get("current_opportunity_status_label"), brief.get("lead_status_label") or "Unstaged")
        stage_type = clean_label((current_opportunity or {}).get("status_type") or "", "lead_only" if not current_opportunity else "unknown")
        pipeline_name = clean_label((current_opportunity or {}).get("pipeline_name"), "Lead Only" if not current_opportunity else "Unknown Pipeline")

        guest_count_text = clean_label((current_opportunity or {}).get("custom.cf_nQLULOLLmtUAh9OwcpJibPc5pQKIqpFOjdGSTwC9ePO"), "")
        guest_min, guest_max = extract_guest_range(guest_count_text)

        event_facts = {
            "lead_id": lead_id,
            "lead_name": lead_name,
            "lead_owner_name": brief.get("lead_owner_name") or "",
            "pipeline_name": pipeline_name,
            "stage_label": stage_label,
            "stage_type": stage_type,
            "event_datetime_utc": event_datetime_utc,
            "event_datetime_source": event_datetime_source,
            "event_type": clean_label((current_opportunity or {}).get("custom.cf_goMfyKkS7pFUhmo0xvrl1JvQv1KLaQkfoVo0j93dvhe"), ""),
            "event_type_source": "opportunity_custom_event_type" if (current_opportunity or {}).get("custom.cf_goMfyKkS7pFUhmo0xvrl1JvQv1KLaQkfoVo0j93dvhe") else "",
            "guest_count_text": guest_count_text,
            "guest_count_min": guest_min if guest_min is not None else "",
            "guest_count_max": guest_max if guest_max is not None else "",
            "guest_count_source": "opportunity_custom_guest_count" if guest_count_text else "",
            "venue_type": list_to_text((current_opportunity or {}).get("custom.cf_3LZk8uGw0lIvPpNzOMvFn4WwiCXbI91X66Ujvt8UJPx")),
            "venue_name": clean_label((current_opportunity or {}).get("lead_custom.cf_bMmcNeKx2ltaIMgNPLXg3cQCVcKguZe28ilBnOilnO5"), ""),
            "venue_address": clean_label((current_opportunity or {}).get("lead_custom.cf_l7gEKQsPZLqjEw35V4WB6ewUuc84dS3nohisc0BeCdy"), ""),
            "venue_city": clean_label((current_opportunity or {}).get("lead_custom.cf_xD3AKAnhwHeZy3OAUrZvbbFYiDPFwtFfTrSLAbDbmA2"), ""),
            "postal_code": clean_label((current_opportunity or {}).get("lead_custom.cf_pXTVEI1DdERiT91NKuWAndlV6WuS4n6ZG2334fBR4b8"), ""),
            "source_channels": list_to_text((current_opportunity or {}).get("custom.cf_ge7qOebiWpyPvuv7xkzNaYpM8PsmOeNvXasXFOtPXRt")),
            "customer_type": clean_label((current_opportunity or {}).get("custom.cf_fs7mrfN5x0M20CyoltczyVg8t0Xul5GFvkC4FNUKvY6"), ""),
            "budget_band": clean_label((current_opportunity or {}).get("lead_custom.cf_imMCu3Pod85W2K5ZkVUjBD7m3E5iZxbSf3mueeNpibM"), ""),
            "assignment_lane": clean_label((current_opportunity or {}).get("lead_custom.cf_xF8FLufgEx9bsijfRAfHhgIrPBQ5ajuohcazC7OtNmT"), ""),
            "internal_flags": list_to_text((current_opportunity or {}).get("lead_custom.cf_9vVeQH1oYtJbtdHoL9VPwGhNpuCzVCgi95p7MCasszj")),
            "latest_observed_activity_utc": brief.get("latest_observed_activity_utc") or "",
            "latest_incoming_utc": (latest_incoming or {}).get("event_datetime_utc") or "",
            "latest_outgoing_utc": (latest_outgoing or {}).get("event_datetime_utc") or "",
            "latest_call_utc": (latest_call or {}).get("event_datetime_utc") or "",
            "waiting_on_us": waiting_on_us,
            "latest_buyer_signal": brief.get("latest_buyer_signal") or "",
            "suggested_next_move": brief.get("suggested_next_move") or "",
            "brief_path": brief.get("brief_path") or "",
            "raw_inferred_field_map": {key: alias for key, alias in FIELD_ALIASES.items() if (current_opportunity or {}).get(key) not in (None, "", [], {})},
        }

        write_json(lead_dir / "lead_event_facts.json", event_facts)
        (lead_dir / "lead_event_facts.md").write_text(build_event_facts_markdown(event_facts), encoding="utf-8")
        event_facts_rows.append(event_facts)

        score, bucket, action, reasons = build_priority(event_facts, latest_incoming, latest_outgoing, waiting_on_us)
        queue_row = {
            **event_facts,
            "priority_score": score,
            "priority_band": priority_band(score),
            "queue_bucket": bucket,
            "recommended_action": action,
            "priority_reasons": " | ".join(reasons),
            "follow_up_snapshot_path": str(lead_dir / "lead_event_facts.json"),
        }

        if queue_row["priority_band"] == "suppressed":
            suppressed_rows.append(queue_row)
        else:
            follow_up_rows.append(queue_row)

        event_dt = parse_iso(event_facts.get("event_datetime_utc"))
        if (
            event_dt
            and event_dt >= datetime.now(timezone.utc)
            and event_facts.get("stage_type") in {"active", "won", "lead_only"}
        ):
            watchlist_rows.append(queue_row)

    follow_up_rows.sort(
        key=lambda row: (
            int(row.get("priority_score") or 0),
            row.get("latest_observed_activity_utc") or "",
        ),
        reverse=True,
    )
    watchlist_rows.sort(key=lambda row: row.get("event_datetime_utc") or "")
    suppressed_rows.sort(key=lambda row: row.get("lead_name") or "")

    write_csv(normalized_dir / "lead_event_facts.csv", event_facts_rows)
    write_jsonl(normalized_dir / "lead_event_facts.jsonl", event_facts_rows)
    write_csv(normalized_dir / "follow_up_queue.csv", follow_up_rows)
    write_jsonl(normalized_dir / "follow_up_queue.jsonl", follow_up_rows)
    write_csv(normalized_dir / "upcoming_event_watchlist.csv", watchlist_rows)
    write_jsonl(normalized_dir / "upcoming_event_watchlist.jsonl", watchlist_rows)
    write_csv(normalized_dir / "suppressed_follow_up_leads.csv", suppressed_rows)

    owner_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in follow_up_rows:
        owner_groups[row["lead_owner_name"]].append(row)
    for owner_name, rows in owner_groups.items():
        owner_slug = re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", owner_name.lower())).strip("_") or "unassigned"
        owner_dir = by_owner_dir / owner_slug
        ensure_dir(owner_dir)
        write_csv(owner_dir / "follow_up_queue.csv", rows)
        write_jsonl(owner_dir / "follow_up_queue.jsonl", rows)

    (output_dir / "follow_up_queue.md").write_text(build_follow_up_markdown(follow_up_rows), encoding="utf-8")
    (output_dir / "upcoming_event_watchlist.md").write_text(build_watchlist_markdown(watchlist_rows), encoding="utf-8")

    readme_lines = [
        "# Operational Intelligence",
        "",
        "This layer turns the normalized CRM library into action-oriented datasets.",
        "",
        "## What is here",
        "- `follow_up_queue.md`: prioritized action list for active/won leads",
        "- `upcoming_event_watchlist.md`: event-date watchlist",
        "- `by_owner/`: owner-specific follow-up queues",
        "",
        "## Normalized Files",
        "- `../normalized/lead_event_facts.csv`",
        "- `../normalized/follow_up_queue.csv`",
        "- `../normalized/upcoming_event_watchlist.csv`",
        "- `../normalized/suppressed_follow_up_leads.csv`",
        "",
        "## Notes",
        "- Event facts rely heavily on inferred custom-field aliases from the opportunity export.",
        "- `date_won` is only used as a fallback event-date hint when the dedicated event-date custom field is absent.",
        "",
    ]
    (output_dir / "README.md").write_text("\n".join(readme_lines), encoding="utf-8")

    print(
        json.dumps(
            {
                "event_facts_rows": len(event_facts_rows),
                "follow_up_rows": len(follow_up_rows),
                "watchlist_rows": len(watchlist_rows),
                "suppressed_rows": len(suppressed_rows),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
