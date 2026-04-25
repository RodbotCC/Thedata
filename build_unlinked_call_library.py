#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_CALLS_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /close_conversation_export/run_20260326_184308/raw/calls.json")
DEFAULT_OUTPUT_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library/unlinked_calls")

GENERIC_CONTACT_LABELS = {"unknown", "contact", "customer", "caller", "unknown speaker"}
LIKELY_INTERNAL_NAMES = {
    "andre raw",
    "sales team event consultant team",
    "rhonna ricafort",
    "nicole cross",
    "eduarda fedrizzi",
    "eduarda",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a labeled library of recorded calls that do not have a lead_id in Close.")
    parser.add_argument("--calls-path", type=Path, default=DEFAULT_CALLS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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


def format_seconds(value: Optional[float]) -> str:
    if value is None:
        return ""
    total = float(value)
    minutes = int(total // 60)
    seconds = total - (minutes * 60)
    return f"{minutes:02d}:{seconds:04.1f}"


def normalize_summary_text(summary_text: Optional[str]) -> str:
    if not summary_text:
        return ""
    lines = [line.rstrip() for line in summary_text.strip().splitlines()]
    return "\n".join(line for line in lines if line.strip())


def dominant_contact_speaker(utterances: Iterable[Dict[str, Any]]) -> Optional[str]:
    counter: Counter[str] = Counter()
    for utterance in utterances:
        if utterance.get("speaker_side") != "contact":
            continue
        label = (utterance.get("speaker_label") or "").strip()
        text = (utterance.get("text") or "").strip()
        if label and label.lower() not in GENERIC_CONTACT_LABELS and text:
            counter[label] += len(text)
    if not counter:
        return None
    return counter.most_common(1)[0][0]


def build_normalized_utterances(
    call: Dict[str, Any],
    resolved_salesperson: str,
    resolved_contact: str,
) -> List[Dict[str, Any]]:
    transcript = call.get("recording_transcript") or {}
    utterances = transcript.get("utterances") or []
    normalized: List[Dict[str, Any]] = []

    for index, utterance in enumerate(sorted(utterances, key=lambda item: item.get("start") or 0), start=1):
        side = utterance.get("speaker_side")
        raw_label = (utterance.get("speaker_label") or "").strip()
        if side == "close-user":
            role = "salesperson"
            speaker_name = resolved_salesperson
        elif side == "contact":
            role = "contact"
            speaker_name = resolved_contact
        else:
            role = "unknown"
            speaker_name = raw_label or "Unknown Speaker"

        normalized.append(
            {
                "utterance_index": index,
                "speaker_role": role,
                "speaker_name": speaker_name,
                "speaker_label_raw": raw_label,
                "speaker_side": side,
                "start_seconds": utterance.get("start"),
                "end_seconds": utterance.get("end"),
                "text": (utterance.get("text") or "").strip(),
            }
        )

    return normalized


def build_transcript_text(utterances: Iterable[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for utterance in utterances:
        timestamp = format_seconds(utterance.get("start_seconds"))
        speaker = utterance.get("speaker_name") or "Unknown Speaker"
        role = utterance.get("speaker_role") or "unknown"
        text = utterance.get("text") or ""
        lines.append(f"[{timestamp}] {speaker} ({role}): {text}")
    return "\n".join(lines)


def first_summary_line(summary_text: str) -> str:
    for line in summary_text.splitlines():
        clean = line.strip(" -")
        if clean:
            return clean
    return ""


def looks_internal_name(candidate: str) -> bool:
    text = candidate.strip().lower()
    return text in LIKELY_INTERNAL_NAMES


def extract_name_from_summary(summary_text: str, salesperson_name: str) -> Tuple[Optional[str], str]:
    salesperson_pattern = re.escape(salesperson_name.strip())
    patterns = [
        (
            rf"- {salesperson_pattern} called ([A-Z][A-Za-z0-9&.'-]+(?: [A-Z][A-Za-z0-9&.'-]+){{0,4}})",
            "summary_target_after_salesperson_call",
        ),
        (
            rf"- {salesperson_pattern} initiated contact with ([A-Z][A-Za-z0-9&.'-]+(?: [A-Z][A-Za-z0-9&.'-]+){{0,4}})",
            "summary_target_after_salesperson_contact",
        ),
        (r"- Unknown caller \(([^)]+)\)", "summary_unknown_caller_parenthetical"),
        (r"- ([A-Z][A-Za-z.'-]+(?: [A-Z][A-Za-z.'-]+){0,3}) from ", "summary_name_from"),
        (r"- ([A-Z][A-Za-z.'-]+(?: [A-Z][A-Za-z.'-]+){0,3}) called", "summary_name_called"),
        (r"- ([A-Z][A-Za-z.'-]+(?: [A-Z][A-Za-z.'-]+){0,3}) reached", "summary_name_reached"),
        (r"- ([A-Z][A-Za-z.'-]+(?: [A-Z][A-Za-z.'-]+){0,3}) ligou", "summary_name_portuguese_ligou"),
        (r"- ([A-Z][A-Za-z.'-]+(?: [A-Z][A-Za-z.'-]+){0,3}) da ", "summary_name_portuguese_da"),
    ]

    for pattern, source in patterns:
        match = re.search(pattern, summary_text)
        if not match:
            continue
        candidate = clean_label(match.group(1), "")
        if not candidate or candidate.lower() == salesperson_name.strip().lower():
            continue
        return candidate, source

    return None, ""


def resolve_contact_label(call: Dict[str, Any], summary_text: str) -> Tuple[str, str]:
    raw_contact_name = clean_label(call.get("contact_name"), "")
    if raw_contact_name:
        return raw_contact_name, "call_contact_name"

    transcript = call.get("recording_transcript") or {}
    speaker_name = dominant_contact_speaker(transcript.get("utterances") or [])
    if speaker_name and not looks_internal_name(speaker_name):
        return speaker_name, "transcript_contact_speaker"

    salesperson_name = clean_label(call.get("user_name"), "Unknown Salesperson")
    extracted_name, extracted_source = extract_name_from_summary(summary_text, salesperson_name)
    if extracted_name:
        return extracted_name, extracted_source

    remote_phone = clean_label(call.get("remote_phone_formatted") or call.get("remote_phone"), "")
    if remote_phone:
        return remote_phone, "remote_phone"

    return "Unknown Contact", "unknown"


def classify_call(summary_text: str, salesperson_name: str) -> Tuple[str, str, str]:
    text = summary_text.lower()
    salesperson_lower = salesperson_name.lower()

    rules = [
        ("wrong_number", "Wrong Number", ["wrong number", "by mistake", "wrong business", "trying to contact colin"]),
        ("job_inquiry", "Job Inquiry", ["bartender positions", "job", "hiring", "position"]),
        (
            "carrier_or_account_support",
            "Carrier / Account Support",
            ["verizon", "iphone upgrade", "new wireless mobile phone", "account ending in", "new device"],
        ),
        (
            "vendor_or_sales",
            "Vendor / Sales Outreach",
            ["close crm", "merchant service center", "credit card processing", "solar inquiry", "ai voice agent"],
        ),
        ("gift_card_support", "Gift Card / Customer Support", ["gift card", "redemption options", "redemption"]),
        (
            "internal_or_event_ops",
            "Internal / Event Ops",
            ["final event timeline", "catering setup", "comeketo catering called to speak", "ligou para ajustar detalhes finais"],
        ),
        (
            "general_catering_inquiry",
            "General Catering Inquiry",
            ["uber eats", "delivery residencial", "food service", "catering", "pratos nordestinos"],
        ),
    ]

    for slug, label, keywords in rules:
        for keyword in keywords:
            if keyword in text:
                return slug, label, keyword

    if "comeketo catering" in text and salesperson_lower not in text:
        return "internal_or_event_ops", "Internal / Event Ops", "comeketo_catering"

    return "other_unlinked", "Other Unlinked Call", "fallback"


def build_markdown(metadata: Dict[str, Any], transcript_text: str) -> str:
    summary_text = metadata.get("summary_text") or ""
    note_text = metadata.get("call_note") or ""
    lines = [
        f"# {metadata['call_title']}",
        "",
        "## Metadata",
        f"- Call ID: `{metadata['call_id']}`",
        f"- Date (UTC): `{metadata.get('call_datetime_utc') or ''}`",
        f"- Salesperson: {metadata['salesperson_name']}",
        f"- Resolved Contact Label: {metadata['contact_name']}",
        f"- Contact Label Source: `{metadata.get('contact_label_source') or ''}`",
        f"- Category: {metadata.get('category_label') or ''}",
        f"- Category Reason: `{metadata.get('category_reason') or ''}`",
        f"- Direction: {metadata.get('direction') or ''}",
        f"- Status: {metadata.get('status') or ''}",
        f"- Duration (seconds): `{metadata.get('duration_seconds') or ''}`",
        f"- Remote phone: `{metadata.get('remote_phone_formatted') or metadata.get('remote_phone') or ''}`",
        f"- Local phone: `{metadata.get('local_phone_formatted') or metadata.get('local_phone') or ''}`",
        "",
    ]

    if summary_text:
        lines.extend(["## AI Summary", summary_text, ""])

    if note_text:
        lines.extend(["## Call Note", note_text, ""])

    lines.extend(["## Transcript", transcript_text, ""])
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    calls = load_json(args.calls_path)

    recorded_unlinked_calls = [
        call
        for call in calls
        if call.get("recording_transcript")
        and not call.get("lead_id")
        and ((call["recording_transcript"].get("utterances") or []) or call["recording_transcript"].get("summary_text"))
    ]

    output_dir = args.output_dir
    by_category_dir = output_dir / "by_category"
    by_salesperson_dir = output_dir / "by_salesperson"
    normalized_dir = output_dir / "normalized"
    ensure_dir(by_category_dir)
    ensure_dir(by_salesperson_dir)
    ensure_dir(normalized_dir)

    call_index_rows: List[Dict[str, Any]] = []
    all_utterance_rows: List[Dict[str, Any]] = []
    category_counter: Counter[str] = Counter()

    for call in sorted(recorded_unlinked_calls, key=lambda item: item.get("date_created") or item.get("activity_at") or ""):
        transcript = call.get("recording_transcript") or {}
        summary_text = normalize_summary_text(transcript.get("summary_text"))
        call_dt = parse_iso(call.get("date_created") or call.get("activity_at"))
        salesperson_name = clean_label(call.get("user_name"), "Unknown Salesperson")
        contact_name, contact_label_source = resolve_contact_label(call, summary_text)
        category_slug, category_label, category_reason = classify_call(summary_text, salesperson_name)
        category_counter[category_label] += 1

        normalized_utterances = build_normalized_utterances(call, salesperson_name, contact_name)
        transcript_text = build_transcript_text(normalized_utterances)

        timestamp_slug = call_dt.strftime("%Y-%m-%d_%H%M%SZ") if call_dt else "unknown_date"
        month_slug = call_dt.strftime("%Y-%m") if call_dt else "unknown_month"
        call_slug = (
            f"{timestamp_slug}__{slugify(contact_name)}__{slugify(salesperson_name)}__"
            f"{slugify(category_slug)}__{call['id']}"
        )

        category_call_dir = by_category_dir / category_slug / month_slug / call_slug
        salesperson_call_dir = by_salesperson_dir / slugify(salesperson_name) / month_slug / call_slug
        ensure_dir(category_call_dir)
        ensure_dir(salesperson_call_dir)

        metadata = {
            "call_id": call["id"],
            "call_title": f"{contact_name} with {salesperson_name}",
            "call_datetime_utc": iso_z(call_dt),
            "salesperson_name": salesperson_name,
            "salesperson_id": call.get("user_id"),
            "contact_name": contact_name,
            "contact_label_source": contact_label_source,
            "lead_name": "",
            "lead_id": "",
            "direction": call.get("direction"),
            "status": call.get("status"),
            "disposition": call.get("disposition"),
            "duration_seconds": call.get("duration"),
            "recording_duration_seconds": call.get("recording_duration"),
            "remote_phone": call.get("remote_phone"),
            "remote_phone_formatted": call.get("remote_phone_formatted"),
            "local_phone": call.get("local_phone"),
            "local_phone_formatted": call.get("local_phone_formatted"),
            "summary_text": summary_text,
            "summary_html": transcript.get("summary_html"),
            "summary_first_line": first_summary_line(summary_text),
            "speaker_count": len(transcript.get("speakers") or []),
            "utterance_count": len(normalized_utterances),
            "call_note": (call.get("note") or "").strip(),
            "call_method": call.get("call_method"),
            "source": call.get("source"),
            "recording_url": call.get("recording_url"),
            "category_slug": category_slug,
            "category_label": category_label,
            "category_reason": category_reason,
            "source_calls_path": str(args.calls_path),
            "category_folder": str(category_call_dir),
            "salesperson_folder": str(salesperson_call_dir),
        }

        for target_dir in (category_call_dir, salesperson_call_dir):
            write_json(target_dir / "metadata.json", metadata)
            write_json(target_dir / "utterances.json", normalized_utterances)
            write_jsonl(target_dir / "utterances.jsonl", normalized_utterances)
            (target_dir / "transcript.txt").write_text(transcript_text, encoding="utf-8")
            (target_dir / "call.md").write_text(build_markdown(metadata, transcript_text), encoding="utf-8")

        call_index_rows.append(
            {
                "call_id": call["id"],
                "call_datetime_utc": metadata["call_datetime_utc"],
                "salesperson_name": salesperson_name,
                "contact_name": contact_name,
                "contact_label_source": contact_label_source,
                "category_slug": category_slug,
                "category_label": category_label,
                "category_reason": category_reason,
                "direction": call.get("direction"),
                "status": call.get("status"),
                "duration_seconds": call.get("duration"),
                "utterance_count": len(normalized_utterances),
                "has_ai_summary": bool(summary_text),
                "summary_first_line": metadata["summary_first_line"],
                "summary_text": summary_text,
                "remote_phone_formatted": call.get("remote_phone_formatted") or "",
                "local_phone_formatted": call.get("local_phone_formatted") or "",
                "category_folder": str(category_call_dir),
                "salesperson_folder": str(salesperson_call_dir),
            }
        )

        for utterance in normalized_utterances:
            all_utterance_rows.append(
                {
                    "call_id": call["id"],
                    "call_datetime_utc": metadata["call_datetime_utc"],
                    "salesperson_name": salesperson_name,
                    "contact_name": contact_name,
                    "category_slug": category_slug,
                    "category_label": category_label,
                    **utterance,
                }
            )

    write_json(normalized_dir / "unlinked_live_calls.json", call_index_rows)
    write_jsonl(normalized_dir / "unlinked_live_calls.jsonl", call_index_rows)
    write_csv(normalized_dir / "unlinked_live_calls.csv", call_index_rows)
    write_jsonl(normalized_dir / "unlinked_live_call_utterances.jsonl", all_utterance_rows)

    readme_lines = [
        "# Unlinked Recorded Call Library",
        "",
        "This folder contains recorded Close calls that have transcript data but do not have a `lead_id` on the exported call object.",
        "",
        "## What is included",
        f"- Recorded calls without a lead link: {len(call_index_rows)}",
        "- AI summaries from Close when available",
        "- Best-effort contact labels extracted from transcript speakers, summaries, or phone numbers",
        "- Browseable trees by category and salesperson",
        "",
        "## Folder Layout",
        "- `by_category/`: call folders grouped by inferred call category and month",
        "- `by_salesperson/`: call folders grouped by salesperson and month",
        "- `normalized/unlinked_live_calls.csv`: one-row-per-call index",
        "- `normalized/unlinked_live_call_utterances.jsonl`: all utterances across all unlinked calls",
        "",
        "## Category Counts",
    ]
    for category_label, count in sorted(category_counter.items(), key=lambda item: (-item[1], item[0])):
        readme_lines.append(f"- {category_label}: {count}")
    readme_lines.extend(
        [
            "",
            "## Notes",
            "- These calls are intentionally kept outside the lead dossier tree until a real lead/contact linkage exists.",
            "- Contact labels are best-effort and include a `contact_label_source` field in each call's metadata.",
            "- This pass focuses on recorded live calls only, not voicemail transcripts.",
        ]
    )
    (output_dir / "README.md").write_text("\n".join(readme_lines) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "unlinked_recorded_calls": len(call_index_rows),
                "normalized_utterances": len(all_utterance_rows),
                "categories": dict(sorted(category_counter.items())),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
