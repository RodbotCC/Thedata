#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_CALL_INDEX_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_calls.csv"
DEFAULT_LEADS_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /Comeketo Catering leads 2026-03-26 18-32.json")
DEFAULT_OPPORTUNITIES_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /Comeketo Catering opportunities 2026-03-26 18-32.json")
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "by_lead"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build lead-centric dossiers from the normalized phone call library.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--call-index-csv", type=Path, default=DEFAULT_CALL_INDEX_CSV)
    parser.add_argument("--leads-path", type=Path, default=DEFAULT_LEADS_PATH)
    parser.add_argument("--opportunities-path", type=Path, default=DEFAULT_OPPORTUNITIES_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
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


def iter_json_array(path: Path, chunk_size: int = 1024 * 1024) -> Iterator[Dict[str, Any]]:
    decoder = json.JSONDecoder()
    with path.open("r", encoding="utf-8") as handle:
        buffer = ""
        pos = 0
        eof = False

        def fill() -> bool:
            nonlocal buffer, pos, eof
            if eof:
                return False
            if pos:
                buffer = buffer[pos:]
                pos = 0
            chunk = handle.read(chunk_size)
            if chunk == "":
                eof = True
                return False
            buffer += chunk
            return True

        fill()

        while True:
            if pos >= len(buffer) and not fill():
                return
            if pos < len(buffer):
                char = buffer[pos]
                if char.isspace():
                    pos += 1
                    continue
                if char == "[":
                    pos += 1
                    break
                raise ValueError(f"Expected '[' at start of JSON array in {path}")

        while True:
            while True:
                if pos >= len(buffer) and not fill():
                    return
                char = buffer[pos]
                if char.isspace() or char == ",":
                    pos += 1
                    continue
                if char == "]":
                    return
                break

            while True:
                try:
                    value, end = decoder.raw_decode(buffer, pos)
                    pos = end
                    yield value
                    break
                except json.JSONDecodeError:
                    if not fill():
                        raise


def excerpt_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    keep_keys = [
        "id",
        "name",
        "display_name",
        "user_id",
        "user_name",
        "status_id",
        "status_label",
        "date_created",
        "date_updated",
        "primary_opportunity_created",
        "primary_opportunity_pipeline_name",
        "primary_opportunity_status_label",
        "primary_opportunity_value_formatted",
        "last_communication_date",
        "last_call_created",
        "last_incoming_call_date",
        "last_outgoing_call_date",
        "last_sent_sms_date",
        "last_outgoing_email_date",
        "first_outgoing_call_date",
        "first_outgoing_sms_date",
        "first_outgoing_email_date",
        "num_contacts",
        "num_calls",
        "num_inbound_calls",
        "num_outbound_calls",
        "num_opportunities",
        "num_won_opportunities",
        "num_active_opportunities",
        "num_lost_opportunities",
        "num_tasks",
        "num_notes",
        "num_emails",
        "num_sms",
        "num_meetings",
        "contacts",
    ]
    excerpt = {key: lead.get(key) for key in keep_keys if key in lead}
    return excerpt


def resolve_call_folder(raw_path: str, current_library_dir: Path) -> Path:
    candidate = Path(raw_path)
    if candidate.exists():
        return candidate

    old_root = "/Users/jakeaaron/ComeketoData /phone_call_transcript_library"
    new_root = str(current_library_dir)
    remapped = Path(str(candidate).replace(old_root, new_root, 1))
    return remapped


def summarize_opportunities(opportunities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    trimmed: List[Dict[str, Any]] = []
    for opportunity in opportunities:
        trimmed.append(
            {
                "id": opportunity.get("id"),
                "lead_id": opportunity.get("lead_id"),
                "lead_name": opportunity.get("lead_name"),
                "contact_id": opportunity.get("contact_id"),
                "contact_name": opportunity.get("contact_name"),
                "pipeline_name": opportunity.get("pipeline_name"),
                "status_label": opportunity.get("status_label"),
                "status_type": opportunity.get("status_type"),
                "value_formatted": opportunity.get("value_formatted"),
                "date_created": opportunity.get("date_created"),
                "date_updated": opportunity.get("date_updated"),
                "date_won": opportunity.get("date_won"),
                "note": opportunity.get("note"),
            }
        )
    return sorted(trimmed, key=lambda item: item.get("date_created") or "", reverse=True)


def build_lead_summary_markdown(
    metadata: Dict[str, Any],
    contacts: List[Dict[str, Any]],
    opportunities: List[Dict[str, Any]],
    calls: List[Dict[str, Any]],
) -> str:
    lines: List[str] = [
        f"# {metadata['lead_name']}",
        "",
        "## Lead Overview",
        f"- Lead ID: `{metadata['lead_id']}`",
        f"- Owner: {metadata.get('lead_owner_name') or ''}",
        f"- Lead Status: {metadata.get('lead_status_label') or ''}",
        f"- Primary Opportunity Status: {metadata.get('primary_opportunity_status_label') or ''}",
        f"- Primary Opportunity Pipeline: {metadata.get('primary_opportunity_pipeline_name') or ''}",
        f"- Primary Opportunity Value: {metadata.get('primary_opportunity_value_formatted') or ''}",
        f"- First Communication: `{metadata.get('first_recorded_call_utc') or ''}`",
        f"- Last Communication: `{metadata.get('last_recorded_call_utc') or ''}`",
        f"- Recorded Calls In Library: `{metadata.get('recorded_call_count') or 0}`",
        f"- Salespeople Involved: {', '.join(metadata.get('salespeople', []))}",
        "",
    ]

    if contacts:
        lines.extend(["## Contacts"])
        for contact in contacts:
            phones = ", ".join(phone.get("phone_formatted") or phone.get("phone") or "" for phone in (contact.get("phones") or []))
            emails = ", ".join(email.get("email") or "" for email in (contact.get("emails") or []))
            lines.append(f"- {contact.get('name') or ''}: {emails} {phones}".strip())
        lines.append("")

    if opportunities:
        lines.extend(["## Opportunities"])
        for opportunity in opportunities[:15]:
            lines.append(
                f"- {opportunity.get('status_label') or ''} | {opportunity.get('value_formatted') or ''} | "
                f"{opportunity.get('contact_name') or ''} | created `{opportunity.get('date_created') or ''}`"
            )
        lines.append("")

    lines.extend(["## Recorded Calls"])
    for call in calls:
        summary_first_line = ""
        summary_text = call.get("summary_text") or ""
        if summary_text:
            summary_first_line = summary_text.splitlines()[0].strip()
        lines.append(
            f"- `{call.get('call_datetime_utc') or ''}` | {call.get('salesperson_name') or ''} | "
            f"{call.get('direction') or ''} | {call.get('contact_name') or ''} | {summary_first_line}"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    output_dir = args.output_dir
    ensure_dir(output_dir)
    normalized_dir = phone_library_dir / "normalized"
    ensure_dir(normalized_dir)

    call_rows = load_csv_rows(args.call_index_csv)
    lead_ids = {row["lead_id"] for row in call_rows if row.get("lead_id")}
    calls_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in call_rows:
        if row.get("lead_id"):
            calls_by_lead[row["lead_id"]].append(row)

    opportunities = load_json(args.opportunities_path)
    opportunities_by_lead: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for opportunity in opportunities:
        lead_id = opportunity.get("lead_id")
        if lead_id in lead_ids:
            opportunities_by_lead[lead_id].append(opportunity)

    lead_records: Dict[str, Dict[str, Any]] = {}
    for lead in iter_json_array(args.leads_path):
        lead_id = lead.get("id")
        if lead_id in lead_ids:
            lead_records[lead_id] = excerpt_lead(lead)
        if len(lead_records) == len(lead_ids):
            break

    lead_index_rows: List[Dict[str, Any]] = []

    for lead_id in sorted(lead_ids):
        lead_calls = sorted(calls_by_lead[lead_id], key=lambda row: row.get("call_datetime_utc") or "")
        lead_record = lead_records.get(lead_id, {})
        lead_name = clean_label(
            lead_record.get("name") or lead_record.get("display_name") or lead_calls[0].get("lead_name"),
            "Unknown Lead",
        )
        lead_owner_name = clean_label(
            lead_record.get("user_name") or lead_calls[0].get("salesperson_name"),
            "Unknown Owner",
        )
        lead_contacts = lead_record.get("contacts") or []
        summarized_opportunities = summarize_opportunities(opportunities_by_lead.get(lead_id, []))

        first_call = lead_calls[0]
        last_call = lead_calls[-1]
        salespeople = sorted({row["salesperson_name"] for row in lead_calls if row.get("salesperson_name")})
        lead_slug = f"{slugify(lead_name)}__{lead_id}"
        lead_dir = output_dir / lead_slug
        calls_dir = lead_dir / "calls"
        ensure_dir(calls_dir)

        copied_call_rows: List[Dict[str, Any]] = []
        for call in lead_calls:
            source_call_dir = resolve_call_folder(call["salesperson_folder"], phone_library_dir)
            call_folder_name = source_call_dir.name
            month_slug = (call.get("call_datetime_utc") or "unknown_date")[:7]
            target_call_dir = calls_dir / month_slug / call_folder_name
            ensure_dir(target_call_dir.parent)
            shutil.copytree(source_call_dir, target_call_dir, dirs_exist_ok=True)

            call_copy = dict(call)
            call_copy["lead_folder"] = str(lead_dir)
            call_copy["lead_call_folder"] = str(target_call_dir)
            copied_call_rows.append(call_copy)

        metadata = {
            "lead_id": lead_id,
            "lead_name": lead_name,
            "lead_owner_name": lead_owner_name,
            "lead_status_label": lead_record.get("status_label") or "",
            "primary_opportunity_status_label": lead_record.get("primary_opportunity_status_label") or "",
            "primary_opportunity_pipeline_name": lead_record.get("primary_opportunity_pipeline_name") or "",
            "primary_opportunity_value_formatted": lead_record.get("primary_opportunity_value_formatted") or "",
            "recorded_call_count": len(lead_calls),
            "first_recorded_call_utc": first_call.get("call_datetime_utc") or "",
            "last_recorded_call_utc": last_call.get("call_datetime_utc") or "",
            "salespeople": salespeople,
            "opportunity_count": len(summarized_opportunities),
            "won_opportunity_count": sum(1 for opp in summarized_opportunities if opp.get("status_type") == "won"),
            "active_opportunity_count": sum(1 for opp in summarized_opportunities if opp.get("status_type") == "active"),
            "lost_opportunity_count": sum(1 for opp in summarized_opportunities if opp.get("status_type") == "lost"),
            "source_leads_path": str(args.leads_path),
            "source_opportunities_path": str(args.opportunities_path),
            "source_call_index_csv": str(args.call_index_csv),
            "lead_export_excerpt_present": bool(lead_record),
        }
        for key, value in lead_record.items():
            if key not in metadata and key != "contacts":
                metadata[key] = value

        write_json(lead_dir / "lead_metadata.json", metadata)
        write_json(lead_dir / "lead_export_excerpt.json", lead_record)
        write_json(lead_dir / "contacts.json", lead_contacts)
        write_json(lead_dir / "opportunities.json", summarized_opportunities)
        write_csv(lead_dir / "calls_index.csv", copied_call_rows)
        (lead_dir / "lead_summary.md").write_text(
            build_lead_summary_markdown(metadata, lead_contacts, summarized_opportunities, copied_call_rows),
            encoding="utf-8",
        )

        lead_index_rows.append(
            {
                "lead_id": lead_id,
                "lead_name": lead_name,
                "lead_owner_name": lead_owner_name,
                "lead_status_label": metadata.get("lead_status_label") or "",
                "recorded_call_count": len(lead_calls),
                "first_recorded_call_utc": metadata["first_recorded_call_utc"],
                "last_recorded_call_utc": metadata["last_recorded_call_utc"],
                "salespeople": " | ".join(salespeople),
                "opportunity_count": len(summarized_opportunities),
                "won_opportunity_count": metadata["won_opportunity_count"],
                "active_opportunity_count": metadata["active_opportunity_count"],
                "lost_opportunity_count": metadata["lost_opportunity_count"],
                "lead_folder": str(lead_dir),
            }
        )

    write_json(normalized_dir / "live_phone_call_leads.json", lead_index_rows)
    write_jsonl(normalized_dir / "live_phone_call_leads.jsonl", lead_index_rows)
    write_csv(normalized_dir / "live_phone_call_leads.csv", lead_index_rows)

    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "lead_dossiers_built": len(lead_index_rows),
                "recorded_calls_covered": len(call_rows),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
