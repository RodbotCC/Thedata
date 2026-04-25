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
DEFAULT_LEAD_INDEX_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_leads.csv"
DEFAULT_CONTACTS_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /Comeketo Catering contacts 2026-03-26 18-32.json")
DEFAULT_EMAIL_THREADS_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /close_conversation_export/run_20260326_184308/raw/email_threads.json")
DEFAULT_COMMUNICATIONS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_communications.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build thread-level email folders for lead call dossiers.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--lead-index-csv", type=Path, default=DEFAULT_LEAD_INDEX_CSV)
    parser.add_argument("--contacts-path", type=Path, default=DEFAULT_CONTACTS_PATH)
    parser.add_argument("--email-threads-path", type=Path, default=DEFAULT_EMAIL_THREADS_PATH)
    parser.add_argument("--communications-csv", type=Path, default=DEFAULT_COMMUNICATIONS_CSV)
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


def thread_email_datetime(email: Dict[str, Any]) -> Optional[datetime]:
    return parse_iso(email.get("date_sent") or email.get("date_created") or email.get("activity_at") or email.get("date_updated"))


def email_preview(email: Dict[str, Any]) -> str:
    return body_preview((email.get("body_text") or email.get("body_preview") or "").strip())


def load_email_message_map(path: Path) -> Dict[str, Dict[str, str]]:
    rows = load_csv_rows(path)
    return {row["event_id"]: row for row in rows if row.get("channel") == "email" and row.get("event_id")}


def resolve_contact_context(
    thread: Dict[str, Any],
    emails: List[Dict[str, Any]],
    contacts_map: Dict[str, Dict[str, Any]],
    message_map: Dict[str, Dict[str, str]],
    lead_row: Dict[str, str],
) -> Tuple[str, str]:
    thread_contact_id = clean_label(thread.get("contact_id"), "")
    if thread_contact_id and thread_contact_id in contacts_map:
        return thread_contact_id, clean_label(contacts_map[thread_contact_id].get("name"), lead_row["lead_name"])

    for email in emails:
        contact_id = clean_label(email.get("contact_id"), "")
        if contact_id and contact_id in contacts_map:
            return contact_id, clean_label(contacts_map[contact_id].get("name"), lead_row["lead_name"])

    for email in emails:
        message_row = message_map.get(email.get("id") or "")
        if message_row and message_row.get("contact_name"):
            return clean_label(email.get("contact_id"), ""), message_row["contact_name"]

    return "", lead_row["lead_name"]


def resolve_salesperson_name(thread: Dict[str, Any], emails: List[Dict[str, Any]], lead_row: Dict[str, str]) -> str:
    candidates = [thread.get("user_name")]
    candidates.extend(email.get("user_name") for email in emails)
    candidates.extend(email.get("updated_by_name") for email in emails)
    candidates.append(lead_row.get("lead_owner_name"))
    for candidate in candidates:
        clean = clean_label(candidate, "")
        if clean:
            return clean
    return "Unknown Salesperson"


def thread_subject(thread: Dict[str, Any], emails: List[Dict[str, Any]]) -> str:
    candidates = [thread.get("latest_normalized_subject")]
    candidates.extend(email.get("subject") for email in emails)
    for candidate in candidates:
        clean = clean_label(candidate, "")
        if clean:
            return clean
    return "Untitled Email Thread"


def summarize_thread(emails: List[Dict[str, Any]]) -> Dict[str, Any]:
    incoming_count = sum(1 for email in emails if (email.get("direction") or "").lower() == "incoming")
    outgoing_count = sum(1 for email in emails if (email.get("direction") or "").lower() == "outgoing")
    first_email = emails[0] if emails else {}
    last_email = emails[-1] if emails else {}
    return {
        "incoming_email_count": incoming_count,
        "outgoing_email_count": outgoing_count,
        "first_email_preview": email_preview(first_email),
        "latest_email_preview": email_preview(last_email),
    }


def build_thread_markdown(metadata: Dict[str, Any], email_rows: List[Dict[str, Any]]) -> str:
    lines = [
        f"# {metadata['subject']}",
        "",
        "## Metadata",
        f"- Thread ID: `{metadata['thread_id']}`",
        f"- Lead: {metadata['lead_name']}",
        f"- Contact: {metadata['contact_name']}",
        f"- Salesperson: {metadata['salesperson_name']}",
        f"- Total Emails: `{metadata['email_count']}`",
        f"- Incoming Emails: `{metadata['incoming_email_count']}`",
        f"- Outgoing Emails: `{metadata['outgoing_email_count']}`",
        f"- First Email (UTC): `{metadata.get('first_email_utc') or ''}`",
        f"- Last Email (UTC): `{metadata.get('last_email_utc') or ''}`",
        f"- Linked Message Folders: `{metadata.get('linked_message_count') or 0}`",
    ]

    if metadata.get("participant_line"):
        lines.append(f"- Participants: {metadata['participant_line']}")

    lines.extend(["", "## Close Thread Summary"])
    if metadata.get("close_summary_text"):
        lines.append(metadata["close_summary_text"])
    else:
        lines.append("No Close AI thread summary was present in the export for this thread.")

    if metadata.get("computed_thread_summary"):
        lines.extend(["", "## Computed Overview", metadata["computed_thread_summary"]])

    lines.extend(["", "## Emails"])
    for row in email_rows:
        lines.append(
            f"- `{row.get('email_datetime_utc') or ''}` | {row.get('direction') or ''} | "
            f"{row.get('from_line') or row.get('sender') or ''} -> {row.get('to_line') or ''} | "
            f"{row.get('body_preview') or row.get('subject') or ''}"
        )
        if row.get("linked_message_folder"):
            lines.append(f"  Message Folder: `{row['linked_message_folder']}`")
    lines.append("")
    return "\n".join(lines)


def build_thread_timeline_markdown(lead_name: str, thread_rows: List[Dict[str, Any]]) -> str:
    total_emails = sum(int(row.get("email_count") or 0) for row in thread_rows)
    multi_email_threads = sum(1 for row in thread_rows if int(row.get("email_count") or 0) > 1)
    lines = [
        f"# Email Threads for {lead_name}",
        "",
        "## Counts",
        f"- Threads: {len(thread_rows)}",
        f"- Multi-email Threads: {multi_email_threads}",
        f"- Total Emails Inside Threads: {total_emails}",
        "",
        "## Recent Threads",
    ]
    for row in thread_rows[:50]:
        lines.append(
            f"- `{row.get('last_email_utc') or row.get('thread_datetime_utc') or ''}` | "
            f"{row.get('email_count') or 0} emails | {row.get('salesperson_name') or ''} | "
            f"{row.get('subject') or ''}"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    lead_rows = load_csv_rows(args.lead_index_csv)
    contacts = load_json(args.contacts_path)
    threads = load_json(args.email_threads_path)
    contacts_map = {contact["id"]: contact for contact in contacts}
    message_map = load_email_message_map(args.communications_csv)

    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    ensure_dir(normalized_dir)

    lead_by_id = {row["lead_id"]: row for row in lead_rows}
    threads_by_lead: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    total_thread_email_rows = 0

    for thread in threads:
        lead_id = thread.get("lead_id")
        if lead_id not in lead_by_id:
            continue

        lead_row = lead_by_id[lead_id]
        lead_dir = Path(lead_row["lead_folder"])
        thread_root = lead_dir / "threads" / "email"
        ensure_dir(thread_root)

        emails = sorted(thread.get("emails") or [], key=lambda item: iso_z(thread_email_datetime(item)))
        if not emails:
            continue

        subject = thread_subject(thread, emails)
        contact_id, contact_name = resolve_contact_context(thread, emails, contacts_map, message_map, lead_row)
        salesperson_name = resolve_salesperson_name(thread, emails, lead_row)

        first_dt = thread_email_datetime(emails[0])
        last_dt = thread_email_datetime(emails[-1])
        month_slug = (last_dt or first_dt).strftime("%Y-%m") if (last_dt or first_dt) else "unknown_month"
        timestamp_slug = (last_dt or first_dt).strftime("%Y-%m-%d_%H%M%SZ") if (last_dt or first_dt) else "unknown_date"
        thread_slug = (
            f"{timestamp_slug}__{slugify(contact_name)}__{slugify(subject)}__"
            f"{thread['id']}"
        )
        thread_dir = thread_root / month_slug / thread_slug
        ensure_dir(thread_dir)

        email_rows: List[Dict[str, Any]] = []
        for index, email in enumerate(emails, start=1):
            dt = thread_email_datetime(email)
            linked_message = message_map.get(email.get("id") or "")
            email_row = {
                "thread_id": thread["id"],
                "email_index": index,
                "email_id": email.get("id"),
                "email_datetime_utc": iso_z(dt),
                "direction": email.get("direction") or "",
                "status": email.get("status") or "",
                "subject": clean_label(email.get("subject"), subject),
                "body_preview": email_preview(email),
                "sender": email.get("sender") or "",
                "from_line": extract_email_party_line((email.get("envelope") or {}).get("from")) or email.get("sender") or "",
                "to_line": extract_email_party_line(email.get("to")),
                "cc_line": extract_email_party_line(email.get("cc")),
                "bcc_line": extract_email_party_line(email.get("bcc")),
                "contact_id": email.get("contact_id") or contact_id,
                "contact_name": clean_label((contacts_map.get(email.get("contact_id") or "") or {}).get("name"), contact_name),
                "salesperson_name": clean_label(email.get("user_name"), salesperson_name),
                "linked_message_folder": (linked_message or {}).get("folder") or "",
            }
            email_rows.append(email_row)

        total_thread_email_rows += len(email_rows)
        summary = summarize_thread(emails)
        participant_line = extract_email_party_line(thread.get("participants"))
        metadata = {
            "thread_id": thread["id"],
            "thread_datetime_utc": iso_z(parse_iso(thread.get("activity_at")) or last_dt or first_dt),
            "first_email_utc": iso_z(first_dt),
            "last_email_utc": iso_z(last_dt),
            "lead_id": lead_id,
            "lead_name": lead_row["lead_name"],
            "contact_id": contact_id,
            "contact_name": contact_name,
            "salesperson_name": salesperson_name,
            "subject": subject,
            "latest_normalized_subject": thread.get("latest_normalized_subject") or "",
            "email_count": len(email_rows),
            "incoming_email_count": summary["incoming_email_count"],
            "outgoing_email_count": summary["outgoing_email_count"],
            "participant_count": len(thread.get("participants") or []),
            "participant_line": participant_line,
            "close_summary_text": clean_label(thread.get("summary"), ""),
            "computed_thread_summary": (
                f"{len(email_rows)} emails in thread. "
                f"First preview: {summary['first_email_preview'] or '[no preview]'} "
                f"Latest preview: {summary['latest_email_preview'] or '[no preview]'}"
            ),
            "linked_message_count": sum(1 for row in email_rows if row.get("linked_message_folder")),
            "source_email_threads_path": str(args.email_threads_path),
            "source_communications_csv": str(args.communications_csv),
            "thread_folder": str(thread_dir),
        }

        write_json(thread_dir / "thread_metadata.json", metadata)
        write_csv(thread_dir / "emails_index.csv", email_rows)
        write_jsonl(thread_dir / "emails_index.jsonl", email_rows)
        (thread_dir / "thread.md").write_text(build_thread_markdown(metadata, email_rows), encoding="utf-8")

        threads_by_lead[lead_id].append(metadata)

    global_thread_rows: List[Dict[str, Any]] = []

    for lead_row in lead_rows:
        lead_id = lead_row["lead_id"]
        lead_dir = Path(lead_row["lead_folder"])
        thread_rows = sorted(
            threads_by_lead.get(lead_id, []),
            key=lambda row: row.get("last_email_utc") or row.get("thread_datetime_utc") or "",
            reverse=True,
        )

        write_csv(lead_dir / "email_threads_index.csv", thread_rows)
        write_jsonl(lead_dir / "email_threads_index.jsonl", thread_rows)
        (lead_dir / "email_thread_timeline.md").write_text(
            build_thread_timeline_markdown(lead_row["lead_name"], thread_rows),
            encoding="utf-8",
        )

        global_thread_rows.extend(thread_rows)

    global_thread_rows.sort(
        key=lambda row: ((row.get("last_email_utc") or row.get("thread_datetime_utc") or ""), (row.get("lead_id") or "")),
        reverse=True,
    )

    write_csv(normalized_dir / "live_phone_call_lead_email_threads.csv", global_thread_rows)
    write_jsonl(normalized_dir / "live_phone_call_lead_email_threads.jsonl", global_thread_rows)

    print(
        json.dumps(
            {
                "lead_dossiers_updated": len(lead_rows),
                "email_threads_attached": len(global_thread_rows),
                "thread_email_rows_indexed": total_thread_email_rows,
                "multi_email_threads": sum(1 for row in global_thread_rows if int(row.get("email_count") or 0) > 1),
                "output_dir": str(phone_library_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
