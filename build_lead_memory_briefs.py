#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_LEAD_INDEX_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_leads.csv"
DEFAULT_MASTER_TIMELINE_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_master_timeline.csv"
DEFAULT_OPPORTUNITIES_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_opportunities.csv"
DEFAULT_EMAIL_THREADS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_email_threads.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build compact lead memory briefs from the normalized Comeketo lead tree.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--lead-index-csv", type=Path, default=DEFAULT_LEAD_INDEX_CSV)
    parser.add_argument("--master-timeline-csv", type=Path, default=DEFAULT_MASTER_TIMELINE_CSV)
    parser.add_argument("--opportunities-csv", type=Path, default=DEFAULT_OPPORTUNITIES_CSV)
    parser.add_argument("--email-threads-csv", type=Path, default=DEFAULT_EMAIL_THREADS_CSV)
    return parser.parse_args()


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


def compact_text(value: Optional[str], limit: int = 180) -> str:
    text = " ".join((value or "").split())
    return text[:limit]


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def format_contact(contact: Dict[str, Any]) -> str:
    name = clean_label(contact.get("name") or contact.get("display_name"), "Unknown Contact")
    emails = ", ".join(email.get("email") or "" for email in (contact.get("emails") or []) if email.get("email"))
    phones = ", ".join(
        phone.get("phone_formatted") or phone.get("phone") or ""
        for phone in (contact.get("phones") or [])
        if phone.get("phone_formatted") or phone.get("phone")
    )
    parts = [name]
    if emails:
        parts.append(emails)
    if phones:
        parts.append(phones)
    return " | ".join(parts)


def dedupe_contacts(contacts: List[Dict[str, Any]]) -> List[str]:
    best_by_key: Dict[Tuple[str, str], str] = {}
    order: List[Tuple[str, str]] = []
    for contact in contacts:
        name = clean_label(contact.get("name") or contact.get("display_name"), "Unknown Contact").lower()
        primary_phone = ""
        phones = contact.get("phones") or []
        if phones:
            primary_phone = clean_label(phones[0].get("phone_formatted") or phones[0].get("phone"), "")
        key = (name, primary_phone)
        formatted = format_contact(contact)
        current = best_by_key.get(key)
        if current is None:
            best_by_key[key] = formatted
            order.append(key)
        elif len(formatted) > len(current):
            best_by_key[key] = formatted
    return [best_by_key[key] for key in order]


def choose_current_opportunity(opportunities: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    if not opportunities:
        return None

    status_rank = {"active": 0, "won": 1, "lost": 2}
    sorted_rows = sorted(
        opportunities,
        key=lambda row: (
            status_rank.get((row.get("status_type") or "").lower(), 99),
            row.get("date_updated_utc") or row.get("date_created_utc") or "",
        ),
        reverse=False,
    )
    return sorted_rows[0]


def choose_latest_row(rows: List[Dict[str, str]], *, family: Optional[str] = None, direction: Optional[str] = None) -> Optional[Dict[str, str]]:
    candidates = rows
    if family is not None:
        candidates = [row for row in candidates if (row.get("event_family") or "") == family]
    if direction is not None:
        candidates = [row for row in candidates if (row.get("direction") or "") == direction]
    return candidates[0] if candidates else None


def choose_next_future_row(rows: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    now_utc = datetime.now(timezone.utc)
    future_rows = [
        row
        for row in rows
        if parse_iso(row.get("event_datetime_utc")) and parse_iso(row.get("event_datetime_utc")) > now_utc
    ]
    if not future_rows:
        return None
    return sorted(future_rows, key=lambda row: row.get("event_datetime_utc") or "")[0]


def compute_engagement_state(
    lead_metadata: Dict[str, Any],
    current_opportunity: Optional[Dict[str, str]],
    latest_observed: Optional[Dict[str, str]],
) -> str:
    if current_opportunity:
        status_type = (current_opportunity.get("status_type") or "").lower()
        status_label = clean_label(current_opportunity.get("status_label"), "")
        if status_type == "won":
            return f"Won opportunity: {status_label}"
        if status_type == "lost":
            return f"Lost opportunity: {status_label}"
        if status_type == "active":
            return f"Active opportunity: {status_label}"

    lead_status = clean_label(lead_metadata.get("lead_status_label"), "")
    if lead_status:
        return f"Lead status: {lead_status}"

    if latest_observed:
        return f"Latest observed channel: {latest_observed.get('event_family') or 'unknown'}"

    return "Needs review"


def compute_suggested_next_move(
    lead_metadata: Dict[str, Any],
    timeline_rows: List[Dict[str, str]],
    current_opportunity: Optional[Dict[str, str]],
    latest_incoming: Optional[Dict[str, str]],
    latest_outgoing: Optional[Dict[str, str]],
    next_future_row: Optional[Dict[str, str]],
) -> str:
    if current_opportunity:
        status_type = (current_opportunity.get("status_type") or "").lower()
        status_label = clean_label(current_opportunity.get("status_label"), "")
        if status_type == "won":
            return f"Treat as booked business and manage fulfillment around {status_label}."
        if status_type == "lost":
            return f"Re-engage only with care; current opportunity is marked lost ({status_label})."

    if latest_incoming and latest_outgoing:
        incoming_dt = parse_iso(latest_incoming.get("event_datetime_utc"))
        outgoing_dt = parse_iso(latest_outgoing.get("event_datetime_utc"))
        if incoming_dt and outgoing_dt and incoming_dt > outgoing_dt:
            return f"Respond to the latest inbound {latest_incoming.get('event_family') or 'message'}."

    if current_opportunity and (current_opportunity.get("status_type") or "").lower() == "active":
        return f"Push the active opportunity forward from {clean_label(current_opportunity.get('status_label'), 'its current stage')}."

    if next_future_row:
        return f"Work backward from the next scheduled timeline entry on {next_future_row.get('event_datetime_utc') or ''}."

    if timeline_rows:
        latest = timeline_rows[0]
        return f"Review the latest {latest.get('event_family') or 'activity'} and decide whether a follow-up is needed."

    lead_status = clean_label(lead_metadata.get("lead_status_label"), "")
    if lead_status:
        return f"Start from the current lead status: {lead_status}."

    return "Needs manual review."


def build_memory_brief_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Memory Brief: {payload['lead_name']}",
        "",
        "## Snapshot",
        f"- Lead ID: `{payload['lead_id']}`",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Engagement State: {payload.get('engagement_state') or ''}",
        f"- Suggested Next Move: {payload.get('suggested_next_move') or ''}",
        f"- Latest Observed Activity (UTC): `{payload.get('latest_observed_activity_utc') or ''}`",
        f"- Next Future Timeline Entry (UTC): `{payload.get('next_future_timeline_entry_utc') or ''}`",
        "",
        "## Counts",
        f"- Recorded Calls: `{payload.get('recorded_call_count') or 0}`",
        f"- SMS: `{payload.get('sms_count') or 0}`",
        f"- Emails: `{payload.get('email_count') or 0}`",
        f"- Email Threads: `{payload.get('email_thread_count') or 0}`",
        f"- Opportunities: `{payload.get('opportunity_record_count') or 0}`",
        "",
    ]

    if payload.get("contacts"):
        lines.extend(["## Contacts"])
        for contact in payload["contacts"]:
            lines.append(f"- {contact}")
        lines.append("")

    if payload.get("current_opportunity_summary"):
        lines.extend(["## Current Commercial State", f"- {payload['current_opportunity_summary']}", ""])

    if payload.get("latest_buyer_signal"):
        lines.extend(["## Latest Buyer Signal", payload["latest_buyer_signal"], ""])

    if payload.get("upcoming_events"):
        lines.extend(["## Upcoming / Future Entries"])
        for event in payload["upcoming_events"]:
            lines.append(f"- {event}")
        lines.append("")

    if payload.get("recent_threads"):
        lines.extend(["## Recent Threads"])
        for thread in payload["recent_threads"]:
            lines.append(f"- {thread}")
        lines.append("")

    if payload.get("recent_events"):
        lines.extend(["## Recent Events"])
        for event in payload["recent_events"]:
            lines.append(f"- {event}")
        lines.append("")

    lines.extend(
        [
            "## File Pointers",
            f"- Master Timeline: `{payload.get('master_timeline_path') or ''}`",
            f"- Opportunity Index: `{payload.get('opportunities_index_path') or ''}`",
            f"- Email Thread Index: `{payload.get('email_threads_index_path') or ''}`",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"

    lead_rows = load_csv_rows(args.lead_index_csv)
    master_timeline_rows = load_csv_rows(args.master_timeline_csv)
    opportunity_rows = load_csv_rows(args.opportunities_csv)
    thread_rows = load_csv_rows(args.email_threads_csv)

    timeline_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in master_timeline_rows:
        timeline_by_lead[row["lead_id"]].append(row)

    opportunity_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in opportunity_rows:
        opportunity_by_lead[row["lead_id"]].append(row)

    threads_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in thread_rows:
        threads_by_lead[row["lead_id"]].append(row)

    global_brief_rows: List[Dict[str, Any]] = []

    for lead_row in lead_rows:
        lead_id = lead_row["lead_id"]
        lead_dir = Path(lead_row["lead_folder"])
        lead_metadata = load_json(lead_dir / "lead_metadata.json")
        contacts = load_json(lead_dir / "contacts.json") if (lead_dir / "contacts.json").exists() else []

        timeline_rows = timeline_by_lead.get(lead_id, [])
        opportunities = opportunity_by_lead.get(lead_id, [])
        threads = threads_by_lead.get(lead_id, [])
        observed_timeline_rows = [
            row
            for row in timeline_rows
            if parse_iso(row.get("event_datetime_utc")) and parse_iso(row.get("event_datetime_utc")) <= datetime.now(timezone.utc)
        ]
        future_timeline_rows = [
            row
            for row in timeline_rows
            if parse_iso(row.get("event_datetime_utc")) and parse_iso(row.get("event_datetime_utc")) > datetime.now(timezone.utc)
        ]

        latest_observed = next(
            (row for row in observed_timeline_rows),
            None,
        )
        latest_incoming = choose_latest_row(timeline_rows, direction="incoming")
        latest_outgoing = choose_latest_row(timeline_rows, direction="outgoing")
        latest_call = choose_latest_row(timeline_rows, family="call")
        next_future_row = choose_next_future_row(timeline_rows)
        current_opportunity = choose_current_opportunity(opportunities)
        counts = Counter(row.get("event_family") or "" for row in timeline_rows)

        recent_events: List[str] = []
        for row in observed_timeline_rows[:8]:
            description = compact_text(row.get("description"), 170) or "[no preview]"
            recent_events.append(
                f"`{row.get('event_datetime_utc') or ''}` | {row.get('event_type') or ''} | "
                f"{clean_label(row.get('actor_name'), 'Unknown')} | {description}"
            )

        upcoming_events: List[str] = []
        for row in future_timeline_rows[:3]:
            description = compact_text(row.get("description"), 170) or "[no preview]"
            upcoming_events.append(
                f"`{row.get('event_datetime_utc') or ''}` | {row.get('event_type') or ''} | "
                f"{clean_label(row.get('actor_name'), 'Unknown')} | {description}"
            )

        recent_threads: List[str] = []
        for thread in sorted(threads, key=lambda row: row.get("last_email_utc") or row.get("thread_datetime_utc") or "", reverse=True)[:3]:
            recent_threads.append(
                f"`{thread.get('last_email_utc') or thread.get('thread_datetime_utc') or ''}` | "
                f"{thread.get('subject') or 'Untitled thread'} | {thread.get('email_count') or 0} emails"
            )

        latest_buyer_signal = ""
        if latest_incoming:
            latest_buyer_signal = compact_text(latest_incoming.get("description"), 240)
        elif latest_call:
            latest_buyer_signal = compact_text(latest_call.get("description"), 240)

        current_opportunity_summary = ""
        if current_opportunity:
            current_opportunity_summary = (
                f"{clean_label(current_opportunity.get('status_label'), 'Unknown status')} "
                f"in {clean_label(current_opportunity.get('pipeline_name'), 'Unknown pipeline')} "
                f"for {clean_label(current_opportunity.get('contact_name'), lead_row['lead_name'])}. "
                f"Value: {clean_label(current_opportunity.get('value_formatted'), '[not set]')}."
            )

        engagement_state = compute_engagement_state(lead_metadata, current_opportunity, latest_observed)
        suggested_next_move = compute_suggested_next_move(
            lead_metadata,
            timeline_rows,
            current_opportunity,
            latest_incoming,
            latest_outgoing,
            next_future_row,
        )

        brief_payload = {
            "lead_id": lead_id,
            "lead_name": lead_row["lead_name"],
            "lead_owner_name": clean_label(lead_metadata.get("lead_owner_name"), ""),
            "lead_status_label": clean_label(lead_metadata.get("lead_status_label"), ""),
            "engagement_state": engagement_state,
            "suggested_next_move": suggested_next_move,
            "latest_observed_activity_utc": (latest_observed or {}).get("event_datetime_utc") or "",
            "latest_observed_event_type": (latest_observed or {}).get("event_type") or "",
            "latest_observed_description": compact_text((latest_observed or {}).get("description"), 240),
            "next_future_timeline_entry_utc": (next_future_row or {}).get("event_datetime_utc") or "",
            "next_future_event_type": (next_future_row or {}).get("event_type") or "",
            "recorded_call_count": counts.get("call", 0),
            "sms_count": counts.get("sms", 0),
            "email_count": counts.get("email", 0),
            "email_thread_count": counts.get("email_thread", 0),
            "opportunity_record_count": len(opportunities),
            "current_opportunity_status_label": clean_label((current_opportunity or {}).get("status_label"), ""),
            "current_opportunity_status_type": clean_label((current_opportunity or {}).get("status_type"), ""),
            "current_opportunity_value_formatted": clean_label((current_opportunity or {}).get("value_formatted"), ""),
            "current_opportunity_summary": current_opportunity_summary,
            "latest_buyer_signal": latest_buyer_signal,
            "latest_incoming_utc": (latest_incoming or {}).get("event_datetime_utc") or "",
            "latest_incoming_channel": (latest_incoming or {}).get("event_family") or "",
            "latest_outgoing_utc": (latest_outgoing or {}).get("event_datetime_utc") or "",
            "latest_call_utc": (latest_call or {}).get("event_datetime_utc") or "",
            "recent_events": recent_events,
            "upcoming_events": upcoming_events,
            "recent_threads": recent_threads,
            "contacts": dedupe_contacts(contacts[:8]),
            "brief_path": str(lead_dir / "lead_memory_brief.md"),
            "brief_json_path": str(lead_dir / "lead_memory_brief.json"),
            "master_timeline_path": str(lead_dir / "master_timeline.md"),
            "opportunities_index_path": str(lead_dir / "opportunities_index.csv"),
            "email_threads_index_path": str(lead_dir / "email_threads_index.csv"),
        }

        write_json(lead_dir / "lead_memory_brief.json", brief_payload)
        (lead_dir / "lead_memory_brief.md").write_text(build_memory_brief_markdown(brief_payload), encoding="utf-8")

        global_brief_rows.append(
            {
                "lead_id": brief_payload["lead_id"],
                "lead_name": brief_payload["lead_name"],
                "lead_owner_name": brief_payload["lead_owner_name"],
                "lead_status_label": brief_payload["lead_status_label"],
                "engagement_state": brief_payload["engagement_state"],
                "suggested_next_move": brief_payload["suggested_next_move"],
                "latest_observed_activity_utc": brief_payload["latest_observed_activity_utc"],
                "next_future_timeline_entry_utc": brief_payload["next_future_timeline_entry_utc"],
                "recorded_call_count": brief_payload["recorded_call_count"],
                "sms_count": brief_payload["sms_count"],
                "email_count": brief_payload["email_count"],
                "email_thread_count": brief_payload["email_thread_count"],
                "opportunity_record_count": brief_payload["opportunity_record_count"],
                "current_opportunity_status_label": brief_payload["current_opportunity_status_label"],
                "current_opportunity_status_type": brief_payload["current_opportunity_status_type"],
                "current_opportunity_value_formatted": brief_payload["current_opportunity_value_formatted"],
                "latest_buyer_signal": brief_payload["latest_buyer_signal"],
                "brief_path": brief_payload["brief_path"],
            }
        )

    global_brief_rows.sort(
        key=lambda row: (
            row.get("latest_observed_activity_utc") or "",
            row.get("lead_name") or "",
        ),
        reverse=True,
    )

    write_csv(normalized_dir / "lead_memory_briefs.csv", global_brief_rows)
    write_jsonl(normalized_dir / "lead_memory_briefs.jsonl", global_brief_rows)

    print(
        json.dumps(
            {
                "lead_briefs_built": len(global_brief_rows),
                "output_dir": str(phone_library_dir),
                "brief_index_csv": str(normalized_dir / "lead_memory_briefs.csv"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
