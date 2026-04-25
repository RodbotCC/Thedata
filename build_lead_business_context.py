#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_LEAD_INDEX_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_leads.csv"
DEFAULT_COMMUNICATIONS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_communications.csv"
DEFAULT_EMAIL_THREADS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_email_threads.csv"
DEFAULT_OPPORTUNITIES_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /Comeketo Catering opportunities 2026-03-26 18-32.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build opportunity cards and master timelines for lead dossiers.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--lead-index-csv", type=Path, default=DEFAULT_LEAD_INDEX_CSV)
    parser.add_argument("--communications-csv", type=Path, default=DEFAULT_COMMUNICATIONS_CSV)
    parser.add_argument("--email-threads-csv", type=Path, default=DEFAULT_EMAIL_THREADS_CSV)
    parser.add_argument("--opportunities-path", type=Path, default=DEFAULT_OPPORTUNITIES_PATH)
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
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


def slugify(value: Optional[str], fallback: str = "unknown") -> str:
    text = (value or "").strip().lower()
    if not text:
        return fallback
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:100] or fallback


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


def parse_dateish(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = parse_iso(value)
    if dt:
        return dt
    try:
        only_date = date.fromisoformat(value)
    except ValueError:
        return None
    return datetime(only_date.year, only_date.month, only_date.day, tzinfo=timezone.utc)


def iso_z(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_text(text: str, limit: int = 220) -> str:
    compact = " ".join(text.split())
    return compact[:limit]


def first_summary_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip(" -")
        if stripped:
            return stripped
    return ""


def build_opportunity_markdown(metadata: Dict[str, Any]) -> str:
    lines = [
        f"# {metadata['opportunity_title']}",
        "",
        "## Metadata",
        f"- Opportunity ID: `{metadata['opportunity_id']}`",
        f"- Lead: {metadata['lead_name']}",
        f"- Contact: {metadata['contact_name']}",
        f"- Owner: {metadata['salesperson_name']}",
        f"- Pipeline: {metadata.get('pipeline_name') or ''}",
        f"- Status: {metadata.get('status_label') or ''}",
        f"- Status Type: {metadata.get('status_type') or ''}",
        f"- Value: {metadata.get('value_formatted') or ''}",
        f"- Created (UTC): `{metadata.get('date_created_utc') or ''}`",
        f"- Updated (UTC): `{metadata.get('date_updated_utc') or ''}`",
        f"- Close `date_won` Field (UTC): `{metadata.get('close_date_won_utc') or ''}`",
        f"- Confidence: `{metadata.get('confidence') or ''}`",
        "",
    ]

    if metadata.get("note"):
        lines.extend(["## Note", metadata["note"], ""])

    if metadata.get("snapshot_summary"):
        lines.extend(["## Snapshot", metadata["snapshot_summary"], ""])

    return "\n".join(lines)


def build_master_timeline_markdown(
    lead_name: str,
    lead_metadata: Dict[str, Any],
    timeline_rows: List[Dict[str, Any]],
    opportunity_rows: List[Dict[str, Any]],
    thread_rows: List[Dict[str, Any]],
) -> str:
    counts = Counter(row.get("event_family") or "" for row in timeline_rows)
    now_utc = datetime.now(timezone.utc)
    latest_row = timeline_rows[0] if timeline_rows else {}
    latest_observed_row = next(
        (
            row
            for row in timeline_rows
            if parse_dateish(row.get("event_datetime_utc")) and parse_dateish(row.get("event_datetime_utc")) <= now_utc
        ),
        {},
    )
    next_future_row = next(
        (
            row
            for row in reversed(timeline_rows)
            if parse_dateish(row.get("event_datetime_utc")) and parse_dateish(row.get("event_datetime_utc")) > now_utc
        ),
        {},
    )
    current_opportunities = [
        row for row in sorted(opportunity_rows, key=lambda item: item.get("date_updated_utc") or item.get("date_created_utc") or "", reverse=True)
    ]

    lines = [
        f"# Master Timeline for {lead_name}",
        "",
        "## Snapshot",
        f"- Lead Owner: {lead_metadata.get('lead_owner_name') or ''}",
        f"- Lead Status: {lead_metadata.get('lead_status_label') or ''}",
        f"- Latest Observed Activity (UTC): `{latest_observed_row.get('event_datetime_utc') or latest_row.get('event_datetime_utc') or ''}`",
        f"- Recorded Calls: `{counts.get('call', 0)}`",
        f"- SMS Messages: `{counts.get('sms', 0)}`",
        f"- Email Messages: `{counts.get('email', 0)}`",
        f"- Email Thread Events: `{counts.get('email_thread', 0)}`",
        f"- Opportunity Events: `{counts.get('opportunity', 0)}`",
        "",
    ]

    if next_future_row:
        lines.insert(4, f"- Latest Scheduled / Future Entry (UTC): `{latest_row.get('event_datetime_utc') or ''}`")

    if current_opportunities:
        lines.extend(["## Current Opportunities"])
        for row in current_opportunities[:10]:
            lines.append(
                f"- {row.get('status_label') or ''} | {row.get('value_formatted') or ''} | "
                f"{row.get('contact_name') or ''} | updated `{row.get('date_updated_utc') or row.get('date_created_utc') or ''}`"
            )
        lines.append("")

    if thread_rows:
        multi_thread_count = sum(1 for row in thread_rows if int(row.get("email_count") or 0) > 1)
        lines.extend(
            [
                "## Email Thread Rollup",
                f"- Total Threads: `{len(thread_rows)}`",
                f"- Multi-email Threads: `{multi_thread_count}`",
                "",
            ]
        )

    lines.extend(["## Recent Events"])
    for row in timeline_rows[:80]:
        description = row.get("description") or ""
        lines.append(
            f"- `{row.get('event_datetime_utc') or ''}` | {row.get('event_type') or ''} | "
            f"{row.get('actor_name') or ''} | {description}"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    ensure_dir(normalized_dir)

    lead_rows = load_csv_rows(args.lead_index_csv)
    communications = load_csv_rows(args.communications_csv)
    email_threads = load_csv_rows(args.email_threads_csv)
    opportunities = load_json(args.opportunities_path)

    leads_by_id = {row["lead_id"]: row for row in lead_rows}
    communications_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in communications:
        lead_id = row.get("lead_id") or ""
        if lead_id in leads_by_id:
            communications_by_lead[lead_id].append(row)

    threads_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in email_threads:
        lead_id = row.get("lead_id") or ""
        if lead_id in leads_by_id:
            threads_by_lead[lead_id].append(row)

    opportunities_by_lead: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for opportunity in opportunities:
        lead_id = opportunity.get("lead_id")
        if lead_id in leads_by_id:
            opportunities_by_lead[lead_id].append(opportunity)

    global_opportunity_rows: List[Dict[str, Any]] = []
    global_timeline_rows: List[Dict[str, Any]] = []

    for lead_row in lead_rows:
        lead_id = lead_row["lead_id"]
        lead_dir = Path(lead_row["lead_folder"])
        lead_metadata_path = lead_dir / "lead_metadata.json"
        lead_metadata = load_json(lead_metadata_path) if lead_metadata_path.exists() else {}

        opportunity_card_root = lead_dir / "opportunity_cards"
        ensure_dir(opportunity_card_root)

        lead_opportunities = sorted(
            opportunities_by_lead.get(lead_id, []),
            key=lambda item: item.get("date_created") or "",
        )
        opportunity_rows: List[Dict[str, Any]] = []

        for opportunity in lead_opportunities:
            created_dt = parse_dateish(opportunity.get("date_created"))
            updated_dt = parse_dateish(opportunity.get("date_updated"))
            won_dt = parse_dateish(opportunity.get("date_won"))
            month_slug = (created_dt or updated_dt or won_dt).strftime("%Y-%m") if (created_dt or updated_dt or won_dt) else "unknown_month"
            timestamp_slug = (created_dt or updated_dt or won_dt).strftime("%Y-%m-%d_%H%M%SZ") if (created_dt or updated_dt or won_dt) else "unknown_date"
            contact_name = clean_label(opportunity.get("contact_name"), lead_row["lead_name"])
            status_label = clean_label(opportunity.get("status_label"), "Unknown Status")
            salesperson_name = clean_label(opportunity.get("user_name"), lead_metadata.get("lead_owner_name") or "Unknown Owner")
            opportunity_slug = (
                f"{timestamp_slug}__{slugify(contact_name)}__{slugify(status_label)}__{opportunity['id']}"
            )
            opportunity_dir = opportunity_card_root / month_slug / opportunity_slug
            ensure_dir(opportunity_dir)

            snapshot_summary = (
                f"{status_label} in {clean_label(opportunity.get('pipeline_name'), 'Unknown Pipeline')} "
                f"for {contact_name}. Value: {clean_label(opportunity.get('value_formatted'), '[not set]')}."
            )
            metadata = {
                "opportunity_id": opportunity["id"],
                "opportunity_title": f"{contact_name} opportunity",
                "lead_id": lead_id,
                "lead_name": lead_row["lead_name"],
                "contact_id": opportunity.get("contact_id"),
                "contact_name": contact_name,
                "salesperson_id": opportunity.get("user_id"),
                "salesperson_name": salesperson_name,
                "pipeline_id": opportunity.get("pipeline_id"),
                "pipeline_name": opportunity.get("pipeline_name"),
                "status_id": opportunity.get("status_id"),
                "status_label": opportunity.get("status_label"),
                "status_type": opportunity.get("status_type"),
                "value": opportunity.get("value"),
                "value_formatted": opportunity.get("value_formatted"),
                "value_currency": opportunity.get("value_currency"),
                "value_period": opportunity.get("value_period"),
                "confidence": opportunity.get("confidence"),
                "date_created_utc": iso_z(created_dt),
                "date_updated_utc": iso_z(updated_dt),
                "close_date_won_utc": iso_z(won_dt),
                "note": (opportunity.get("note") or "").strip(),
                "snapshot_summary": snapshot_summary,
                "source_opportunities_path": str(args.opportunities_path),
                "opportunity_folder": str(opportunity_dir),
            }

            write_json(opportunity_dir / "metadata.json", metadata)
            (opportunity_dir / "opportunity.md").write_text(build_opportunity_markdown(metadata), encoding="utf-8")

            opportunity_rows.append(metadata)
            global_opportunity_rows.append(metadata)

        opportunity_rows.sort(key=lambda item: item.get("date_updated_utc") or item.get("date_created_utc") or "", reverse=True)
        write_csv(lead_dir / "opportunities_index.csv", opportunity_rows)
        write_jsonl(lead_dir / "opportunities_index.jsonl", opportunity_rows)

        timeline_rows: List[Dict[str, Any]] = []

        lead_created_dt = parse_dateish(lead_metadata.get("date_created"))
        if lead_created_dt:
            timeline_rows.append(
                {
                    "lead_id": lead_id,
                    "lead_name": lead_row["lead_name"],
                    "event_datetime_utc": iso_z(lead_created_dt),
                    "event_family": "lead",
                    "event_type": "lead_created",
                    "event_id": lead_id,
                    "actor_name": lead_metadata.get("lead_owner_name") or "",
                    "description": f"Lead created with status {lead_metadata.get('lead_status_label') or '[unknown]'}",
                    "folder": str(lead_dir),
                }
            )

        for row in communications_by_lead.get(lead_id, []):
            channel = row.get("channel") or ""
            raw_preview = row.get("body_preview") or row.get("summary_text") or row.get("subject") or ""
            preview = first_summary_line(raw_preview) if channel == "call" else compact_text(raw_preview)
            timeline_rows.append(
                {
                    "lead_id": lead_id,
                    "lead_name": lead_row["lead_name"],
                    "event_datetime_utc": row.get("event_datetime_utc") or "",
                    "event_family": channel,
                    "event_type": f"{channel}_message" if channel in {"sms", "email"} else channel,
                    "event_id": row.get("event_id") or "",
                    "actor_name": row.get("salesperson_name") or "",
                    "description": preview,
                    "direction": row.get("direction") or "",
                    "status": row.get("status") or "",
                    "contact_name": row.get("contact_name") or "",
                    "folder": row.get("folder") or "",
                }
            )

        for row in threads_by_lead.get(lead_id, []):
            timeline_rows.append(
                {
                    "lead_id": lead_id,
                    "lead_name": lead_row["lead_name"],
                    "event_datetime_utc": row.get("last_email_utc") or row.get("thread_datetime_utc") or "",
                    "event_family": "email_thread",
                    "event_type": "email_thread",
                    "event_id": row.get("thread_id") or "",
                    "actor_name": row.get("salesperson_name") or "",
                    "description": (
                        f"{row.get('subject') or 'Untitled thread'} "
                        f"({row.get('email_count') or 0} emails)"
                    ),
                    "status": "",
                    "contact_name": row.get("contact_name") or "",
                    "folder": row.get("thread_folder") or "",
                }
            )

        for row in opportunity_rows:
            created_utc = row.get("date_created_utc") or ""
            updated_utc = row.get("date_updated_utc") or ""
            close_date_won_utc = row.get("close_date_won_utc") or ""
            base_desc = (
                f"{row.get('status_label') or '[status]'} | {row.get('value_formatted') or '[no value]'} | "
                f"{row.get('contact_name') or lead_row['lead_name']}"
            )

            if created_utc:
                timeline_rows.append(
                    {
                        "lead_id": lead_id,
                        "lead_name": lead_row["lead_name"],
                        "event_datetime_utc": created_utc,
                        "event_family": "opportunity",
                        "event_type": "opportunity_created",
                        "event_id": row["opportunity_id"],
                        "actor_name": row.get("salesperson_name") or "",
                        "description": f"Opportunity created: {base_desc}",
                        "status": row.get("status_type") or "",
                        "contact_name": row.get("contact_name") or "",
                        "folder": row.get("opportunity_folder") or "",
                    }
                )

            status_type = row.get("status_type") or ""
            if close_date_won_utc:
                timeline_rows.append(
                    {
                        "lead_id": lead_id,
                        "lead_name": lead_row["lead_name"],
                        "event_datetime_utc": close_date_won_utc,
                        "event_family": "opportunity",
                        "event_type": "opportunity_close_date",
                        "event_id": row["opportunity_id"],
                        "actor_name": row.get("salesperson_name") or "",
                        "description": f"Opportunity Close `date_won` field: {base_desc}",
                        "status": status_type,
                        "contact_name": row.get("contact_name") or "",
                        "folder": row.get("opportunity_folder") or "",
                    }
                )

            if updated_utc and updated_utc != created_utc:
                timeline_rows.append(
                    {
                        "lead_id": lead_id,
                        "lead_name": lead_row["lead_name"],
                        "event_datetime_utc": updated_utc,
                        "event_family": "opportunity",
                        "event_type": "opportunity_snapshot",
                        "event_id": row["opportunity_id"],
                        "actor_name": row.get("salesperson_name") or "",
                        "description": f"Opportunity snapshot: {base_desc}",
                        "status": status_type,
                        "contact_name": row.get("contact_name") or "",
                        "folder": row.get("opportunity_folder") or "",
                    }
                )

        timeline_rows.sort(
            key=lambda item: ((item.get("event_datetime_utc") or ""), (item.get("event_type") or ""), (item.get("event_id") or "")),
            reverse=True,
        )

        now_utc = datetime.now(timezone.utc)
        latest_observed_row = next(
            (
                row
                for row in timeline_rows
                if parse_dateish(row.get("event_datetime_utc")) and parse_dateish(row.get("event_datetime_utc")) <= now_utc
            ),
            {},
        )

        write_csv(lead_dir / "master_timeline.csv", timeline_rows)
        write_jsonl(lead_dir / "master_timeline.jsonl", timeline_rows)
        (lead_dir / "master_timeline.md").write_text(
            build_master_timeline_markdown(
                lead_row["lead_name"],
                lead_metadata,
                timeline_rows,
                opportunity_rows,
                threads_by_lead.get(lead_id, []),
            ),
            encoding="utf-8",
        )

        lead_metadata["opportunity_record_count"] = len(opportunity_rows)
        lead_metadata["email_thread_count"] = len(threads_by_lead.get(lead_id, []))
        lead_metadata["multi_email_thread_count"] = sum(
            1 for row in threads_by_lead.get(lead_id, []) if int(row.get("email_count") or 0) > 1
        )
        lead_metadata["master_timeline_event_count"] = len(timeline_rows)
        lead_metadata["latest_master_activity_utc"] = latest_observed_row.get("event_datetime_utc") or ""
        lead_metadata["latest_master_timeline_entry_utc"] = timeline_rows[0]["event_datetime_utc"] if timeline_rows else ""
        lead_metadata["source_communications_csv"] = str(args.communications_csv)
        lead_metadata["source_email_threads_csv"] = str(args.email_threads_csv)
        lead_metadata["opportunities_index_csv"] = str(lead_dir / "opportunities_index.csv")
        lead_metadata["master_timeline_csv"] = str(lead_dir / "master_timeline.csv")
        write_json(lead_metadata_path, lead_metadata)

        global_timeline_rows.extend(timeline_rows)

    global_opportunity_rows.sort(
        key=lambda item: ((item.get("date_updated_utc") or item.get("date_created_utc") or ""), (item.get("lead_id") or "")),
        reverse=True,
    )
    write_csv(normalized_dir / "live_phone_call_lead_opportunities.csv", global_opportunity_rows)
    write_jsonl(normalized_dir / "live_phone_call_lead_opportunities.jsonl", global_opportunity_rows)

    global_timeline_rows.sort(
        key=lambda item: ((item.get("event_datetime_utc") or ""), (item.get("lead_id") or ""), (item.get("event_type") or "")),
        reverse=True,
    )
    write_csv(normalized_dir / "live_phone_call_lead_master_timeline.csv", global_timeline_rows)
    write_jsonl(normalized_dir / "live_phone_call_lead_master_timeline.jsonl", global_timeline_rows)

    print(
        json.dumps(
            {
                "lead_dossiers_updated": len(lead_rows),
                "opportunity_cards_built": len(global_opportunity_rows),
                "master_timeline_events": len(global_timeline_rows),
                "lead_with_opportunities": sum(1 for lead_id in leads_by_id if opportunities_by_lead.get(lead_id)),
                "output_dir": str(phone_library_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
