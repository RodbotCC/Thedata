#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_LEAD_INDEX_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_leads.csv"
DEFAULT_CONTACTS_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /Comeketo Catering contacts 2026-03-26 18-32.json")
DEFAULT_SMS_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /close_conversation_export/run_20260326_184308/raw/sms.json")
DEFAULT_EMAILS_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /close_conversation_export/run_20260326_184308/raw/emails.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build SMS/email message trees and unified timelines for lead call dossiers.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--lead-index-csv", type=Path, default=DEFAULT_LEAD_INDEX_CSV)
    parser.add_argument("--contacts-path", type=Path, default=DEFAULT_CONTACTS_PATH)
    parser.add_argument("--sms-path", type=Path, default=DEFAULT_SMS_PATH)
    parser.add_argument("--emails-path", type=Path, default=DEFAULT_EMAILS_PATH)
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


def iso_z(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def body_preview(text: str, limit: int = 220) -> str:
    compact = " ".join(text.split())
    return compact[:limit]


def build_message_markdown(metadata: Dict[str, Any], body_text: str) -> str:
    lines = [
        f"# {metadata['channel_label']} Message",
        "",
        "## Metadata",
        f"- Message ID: `{metadata['message_id']}`",
        f"- Channel: {metadata['channel']}",
        f"- Date (UTC): `{metadata.get('message_datetime_utc') or ''}`",
        f"- Salesperson: {metadata.get('salesperson_name') or ''}",
        f"- Contact: {metadata.get('contact_name') or ''}",
        f"- Lead: {metadata.get('lead_name') or ''}",
        f"- Direction: {metadata.get('direction') or ''}",
        f"- Status: {metadata.get('status') or ''}",
    ]

    if metadata.get("subject"):
        lines.append(f"- Subject: {metadata['subject']}")
    if metadata.get("thread_id"):
        lines.append(f"- Thread ID: `{metadata['thread_id']}`")
    if metadata.get("remote_phone_formatted") or metadata.get("remote_phone"):
        lines.append(f"- Remote phone: `{metadata.get('remote_phone_formatted') or metadata.get('remote_phone')}`")
    if metadata.get("local_phone_formatted") or metadata.get("local_phone"):
        lines.append(f"- Local phone: `{metadata.get('local_phone_formatted') or metadata.get('local_phone')}`")
    if metadata.get("to_line"):
        lines.append(f"- To: {metadata['to_line']}")
    if metadata.get("from_line"):
        lines.append(f"- From: {metadata['from_line']}")

    lines.extend(["", "## Body", body_text or "", ""])
    return "\n".join(lines)


def extract_email_party_line(items: Any) -> str:
    if not items:
        return ""
    values: List[str] = []
    if isinstance(items, list):
        for item in items:
            if isinstance(item, str):
                values.append(item)
            elif isinstance(item, dict):
                email = item.get("email") or ""
                name = item.get("name") or ""
                values.append(f"{name} <{email}>".strip())
    return ", ".join(value for value in values if value)


def build_global_message_index(
    phone_library_dir: Path,
    lead_rows: List[Dict[str, str]],
    contacts_map: Dict[str, Dict[str, Any]],
    sms_items: List[Dict[str, Any]],
    email_items: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    lead_folders = {row["lead_id"]: Path(row["lead_folder"]) for row in lead_rows}
    lead_context = {row["lead_id"]: row for row in lead_rows}
    indexed: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def add_message(item: Dict[str, Any], channel: str) -> None:
        lead_id = item.get("lead_id")
        if lead_id not in lead_folders:
            return

        contact = contacts_map.get(item.get("contact_id"))
        lead_row = lead_context.get(lead_id, {})
        salesperson_name = clean_label(
            item.get("user_name")
            or item.get("updated_by_name")
            or item.get("created_by_name")
            or lead_row.get("lead_owner_name"),
            "Unknown Salesperson",
        )
        contact_name = clean_label(
            (contact or {}).get("name")
            or item.get("contact_name")
            or lead_row.get("lead_name")
            or item.get("remote_phone_formatted")
            or item.get("remote_phone"),
            "Unknown Contact",
        )
        lead_name = clean_label(
            (contact or {}).get("lead_display_name") or item.get("lead_name") or lead_row.get("lead_name") or contact_name,
            "Unknown Lead",
        )
        message_dt = parse_iso(item.get("date_created") or item.get("activity_at") or item.get("date_sent"))
        body_text = (item.get("text") or item.get("body_text") or "").strip()
        subject = (item.get("subject") or "").strip()
        channel_label = "SMS" if channel == "sms" else "Email"
        month_slug = message_dt.strftime("%Y-%m") if message_dt else "unknown_month"
        timestamp_slug = message_dt.strftime("%Y-%m-%d_%H%M%SZ") if message_dt else "unknown_date"
        message_dir = (
            lead_folders[lead_id]
            / "messages"
            / channel
            / month_slug
            / (
                f"{timestamp_slug}__{slugify(contact_name)}__{slugify(salesperson_name)}__"
                f"{slugify(item.get('direction') or 'unknown')}__{item['id']}"
            )
        )

        metadata = {
            "message_id": item["id"],
            "channel": channel,
            "channel_label": channel_label,
            "message_datetime_utc": iso_z(message_dt),
            "lead_id": lead_id,
            "lead_name": lead_name,
            "contact_id": item.get("contact_id"),
            "contact_name": contact_name,
            "salesperson_id": item.get("user_id"),
            "salesperson_name": salesperson_name,
            "direction": item.get("direction"),
            "status": item.get("status"),
            "subject": subject,
            "thread_id": item.get("thread_id"),
            "body_preview": body_preview(body_text),
            "remote_phone": item.get("remote_phone"),
            "remote_phone_formatted": item.get("remote_phone_formatted"),
            "local_phone": item.get("local_phone"),
            "local_phone_formatted": item.get("local_phone_formatted"),
            "to_line": extract_email_party_line(item.get("to")),
            "from_line": extract_email_party_line((item.get("envelope") or {}).get("from")) or item.get("sender") or "",
            "source_path": str(DEFAULT_SMS_PATH if channel == "sms" else DEFAULT_EMAILS_PATH),
            "message_folder": str(message_dir),
        }

        indexed[lead_id].append(
            {
                **metadata,
                "body_text": body_text,
            }
        )

    for sms in sms_items:
        add_message(sms, "sms")
    for email in email_items:
        add_message(email, "email")

    return indexed


def build_timeline_markdown(
    lead_name: str,
    timeline_rows: List[Dict[str, Any]],
    channel_counts: Dict[str, int],
) -> str:
    lines = [
        f"# Communication Timeline for {lead_name}",
        "",
        "## Counts",
        f"- Calls: {channel_counts.get('call', 0)}",
        f"- SMS: {channel_counts.get('sms', 0)}",
        f"- Emails: {channel_counts.get('email', 0)}",
        "",
        "## Recent Timeline",
    ]
    for row in timeline_rows[:50]:
        subject = row.get("subject") or ""
        preview = row.get("body_preview") or row.get("summary_text") or ""
        lines.append(
            f"- `{row.get('event_datetime_utc') or ''}` | {row.get('channel') or ''} | "
            f"{row.get('direction') or ''} | {row.get('contact_name') or ''} | "
            f"{subject or preview}"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    lead_rows = load_csv_rows(args.lead_index_csv)
    contacts = load_json(args.contacts_path)
    contacts_map = {contact["id"]: contact for contact in contacts}
    sms_items = load_json(args.sms_path)
    email_items = load_json(args.emails_path)

    message_index = build_global_message_index(phone_library_dir, lead_rows, contacts_map, sms_items, email_items)

    global_timeline_rows: List[Dict[str, Any]] = []

    for lead_row in lead_rows:
        lead_id = lead_row["lead_id"]
        lead_dir = Path(lead_row["lead_folder"])
        messages_dir = lead_dir / "messages"
        ensure_dir(messages_dir)

        lead_messages = sorted(
            message_index.get(lead_id, []),
            key=lambda row: row.get("message_datetime_utc") or "",
        )

        persisted_message_rows: List[Dict[str, Any]] = []
        for message in lead_messages:
            message_dir = Path(message["message_folder"])
            ensure_dir(message_dir)
            metadata = {key: value for key, value in message.items() if key != "body_text"}
            write_json(message_dir / "metadata.json", metadata)
            (message_dir / "body.txt").write_text(message["body_text"], encoding="utf-8")
            (message_dir / "message.md").write_text(
                build_message_markdown(metadata, message["body_text"]),
                encoding="utf-8",
            )
            persisted_message_rows.append(metadata)

        calls_index_path = lead_dir / "calls_index.csv"
        call_rows = load_csv_rows(calls_index_path) if calls_index_path.exists() else []

        timeline_rows: List[Dict[str, Any]] = []
        for call in call_rows:
            timeline_rows.append(
                {
                    "lead_id": lead_id,
                    "lead_name": lead_row["lead_name"],
                    "channel": "call",
                    "event_id": call["call_id"],
                    "event_datetime_utc": call.get("call_datetime_utc") or "",
                    "direction": call.get("direction") or "",
                    "status": call.get("status") or "",
                    "contact_name": call.get("contact_name") or "",
                    "salesperson_name": call.get("salesperson_name") or "",
                    "subject": "",
                    "body_preview": call.get("summary_text") or "",
                    "summary_text": call.get("summary_text") or "",
                    "folder": call.get("lead_call_folder") or call.get("salesperson_folder") or "",
                }
            )

        for message in persisted_message_rows:
            timeline_rows.append(
                {
                    "lead_id": lead_id,
                    "lead_name": lead_row["lead_name"],
                    "channel": message["channel"],
                    "event_id": message["message_id"],
                    "event_datetime_utc": message.get("message_datetime_utc") or "",
                    "direction": message.get("direction") or "",
                    "status": message.get("status") or "",
                    "contact_name": message.get("contact_name") or "",
                    "salesperson_name": message.get("salesperson_name") or "",
                    "subject": message.get("subject") or "",
                    "body_preview": message.get("body_preview") or "",
                    "summary_text": "",
                    "folder": message.get("message_folder") or "",
                }
            )

        timeline_rows.sort(key=lambda row: row.get("event_datetime_utc") or "", reverse=True)
        write_csv(lead_dir / "communications_index.csv", timeline_rows)
        write_jsonl(lead_dir / "communications_index.jsonl", timeline_rows)

        message_rows_json = [
            {
                **message,
                "source_path": str(args.sms_path if message["channel"] == "sms" else args.emails_path),
            }
            for message in persisted_message_rows
        ]
        write_json(lead_dir / "messages_summary.json", message_rows_json)

        channel_counts = Counter(row["channel"] for row in timeline_rows)
        (lead_dir / "communication_timeline.md").write_text(
            build_timeline_markdown(lead_row["lead_name"], timeline_rows, channel_counts),
            encoding="utf-8",
        )

        global_timeline_rows.extend(timeline_rows)

    global_timeline_rows.sort(
        key=lambda row: ((row.get("event_datetime_utc") or ""), (row.get("lead_id") or ""), (row.get("event_id") or "")),
        reverse=True,
    )
    normalized_dir = phone_library_dir / "normalized"
    write_csv(normalized_dir / "live_phone_call_lead_communications.csv", global_timeline_rows)
    write_jsonl(normalized_dir / "live_phone_call_lead_communications.jsonl", global_timeline_rows)

    print(
        json.dumps(
            {
                "lead_dossiers_updated": len(lead_rows),
                "sms_messages_attached": sum(1 for row in global_timeline_rows if row["channel"] == "sms"),
                "email_messages_attached": sum(1 for row in global_timeline_rows if row["channel"] == "email"),
                "call_events_retained": sum(1 for row in global_timeline_rows if row["channel"] == "call"),
                "output_dir": str(phone_library_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
