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
DEFAULT_CONTACTS_PATH = Path("/Users/jakeaaron/Comeketo/ComeketoData /Comeketo Catering contacts 2026-03-26 18-32.json")
DEFAULT_OUTPUT_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a labeled phone call transcript library from Close exports.")
    parser.add_argument("--calls-path", type=Path, default=DEFAULT_CALLS_PATH)
    parser.add_argument("--contacts-path", type=Path, default=DEFAULT_CONTACTS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def slugify(value: Optional[str], fallback: str = "unknown") -> str:
    text = (value or "").strip().lower()
    if not text:
        return fallback
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:80] or fallback


def clean_label(value: Optional[str], fallback: str) -> str:
    text = (value or "").strip()
    return text or fallback


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def iso_z(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def format_seconds(value: Optional[float]) -> str:
    if value is None:
        return ""
    total = float(value)
    minutes = int(total // 60)
    seconds = total - (minutes * 60)
    return f"{minutes:02d}:{seconds:04.1f}"


def dominant_contact_speaker(utterances: Iterable[Dict[str, Any]]) -> Optional[str]:
    counter: Counter[str] = Counter()
    for utterance in utterances:
        if utterance.get("speaker_side") == "contact":
            label = (utterance.get("speaker_label") or "").strip()
            text = (utterance.get("text") or "").strip()
            if label and text:
                counter[label] += len(text)
    if not counter:
        return None
    return counter.most_common(1)[0][0]


def normalize_summary_text(summary_text: Optional[str]) -> str:
    if not summary_text:
        return ""
    lines = [line.rstrip() for line in summary_text.strip().splitlines()]
    return "\n".join(line for line in lines if line.strip())


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
        f"- Contact: {metadata['contact_name']}",
        f"- Lead: {metadata['lead_name']}",
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
    contacts = load_json(args.contacts_path)
    contact_map = {contact["id"]: contact for contact in contacts}

    recorded_calls = [
        call
        for call in calls
        if call.get("recording_transcript")
        and ((call["recording_transcript"].get("utterances") or []) or call["recording_transcript"].get("summary_text"))
    ]

    output_dir = args.output_dir
    by_salesperson_dir = output_dir / "by_salesperson"
    by_contact_dir = output_dir / "by_contact"
    normalized_dir = output_dir / "normalized"
    ensure_dir(by_salesperson_dir)
    ensure_dir(by_contact_dir)
    ensure_dir(normalized_dir)

    call_index_rows: List[Dict[str, Any]] = []
    all_utterance_rows: List[Dict[str, Any]] = []

    for call in sorted(recorded_calls, key=lambda item: item.get("date_created") or item.get("activity_at") or ""):
        call_dt = parse_iso(call.get("date_created") or call.get("activity_at"))
        contact_record = contact_map.get(call.get("contact_id"))
        transcript = call.get("recording_transcript") or {}
        summary_text = normalize_summary_text(transcript.get("summary_text"))

        salesperson_name = clean_label(call.get("user_name"), "Unknown Salesperson")
        contact_name = clean_label(
            call.get("contact_name")
            or (contact_record or {}).get("name")
            or dominant_contact_speaker(transcript.get("utterances") or [])
            or call.get("remote_phone_formatted")
            or call.get("remote_phone"),
            "Unknown Contact",
        )
        lead_name = clean_label(
            (contact_record or {}).get("lead_display_name") or contact_name,
            "Unknown Lead",
        )

        normalized_utterances = build_normalized_utterances(call, salesperson_name, contact_name)
        transcript_text = build_transcript_text(normalized_utterances)

        timestamp_slug = (call_dt.strftime("%Y-%m-%d_%H%M%SZ") if call_dt else "unknown_date")
        month_slug = (call_dt.strftime("%Y-%m") if call_dt else "unknown_month")
        call_slug = (
            f"{timestamp_slug}__{slugify(contact_name)}__{slugify(lead_name)}__"
            f"{slugify(salesperson_name)}__{slugify(call.get('direction') or 'unknown')}__{call['id']}"
        )

        salesperson_call_dir = by_salesperson_dir / slugify(salesperson_name) / month_slug / call_slug
        contact_call_dir = by_contact_dir / slugify(contact_name) / month_slug / call_slug
        ensure_dir(salesperson_call_dir)
        ensure_dir(contact_call_dir)

        metadata = {
            "call_id": call["id"],
            "call_title": f"{contact_name} with {salesperson_name}",
            "call_datetime_utc": iso_z(call_dt),
            "salesperson_name": salesperson_name,
            "salesperson_id": call.get("user_id"),
            "contact_name": contact_name,
            "contact_id": call.get("contact_id"),
            "lead_name": lead_name,
            "lead_id": call.get("lead_id"),
            "contact_primary_email": ((contact_record or {}).get("emails") or [{}])[0].get("email"),
            "contact_primary_phone": ((contact_record or {}).get("phones") or [{}])[0].get("phone_formatted")
            or ((contact_record or {}).get("phones") or [{}])[0].get("phone"),
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
            "speaker_count": len(transcript.get("speakers") or []),
            "utterance_count": len(normalized_utterances),
            "call_note": (call.get("note") or "").strip(),
            "call_method": call.get("call_method"),
            "source": call.get("source"),
            "recording_url": call.get("recording_url"),
            "source_calls_path": str(args.calls_path),
            "source_contacts_path": str(args.contacts_path),
            "salesperson_folder": str(salesperson_call_dir),
            "contact_folder": str(contact_call_dir),
        }

        for target_dir in (salesperson_call_dir, contact_call_dir):
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
                "lead_name": lead_name,
                "contact_id": call.get("contact_id"),
                "lead_id": call.get("lead_id"),
                "direction": call.get("direction"),
                "status": call.get("status"),
                "duration_seconds": call.get("duration"),
                "utterance_count": len(normalized_utterances),
                "has_ai_summary": bool(summary_text),
                "summary_text": summary_text,
                "salesperson_folder": str(salesperson_call_dir),
                "contact_folder": str(contact_call_dir),
            }
        )

        for utterance in normalized_utterances:
            all_utterance_rows.append(
                {
                    "call_id": call["id"],
                    "call_datetime_utc": metadata["call_datetime_utc"],
                    "salesperson_name": salesperson_name,
                    "contact_name": contact_name,
                    "lead_name": lead_name,
                    **utterance,
                }
            )

    write_json(normalized_dir / "live_phone_calls.json", call_index_rows)
    write_jsonl(normalized_dir / "live_phone_calls.jsonl", call_index_rows)
    write_csv(normalized_dir / "live_phone_calls.csv", call_index_rows)
    write_jsonl(normalized_dir / "live_phone_call_utterances.jsonl", all_utterance_rows)

    readme_lines = [
        "# Comeketo Communication Library",
        "",
        "This folder is the normalized local file-tree memory built from the Close exports plus the raw lead/contact/opportunity exports.",
        "",
        "## What is included",
        f"- Recorded live phone calls with transcript data: {len(call_index_rows)}",
        "- AI summaries from Close when available",
        "- Speaker-labeled utterances normalized to salesperson/contact roles",
        "- Browsable call trees by salesperson and by contact",
        "- Downstream builders add `by_lead/`, message folders, email threads, opportunity cards, master timelines, lead memory briefs, owner/stage dashboards, and `unlinked_calls/`",
        "",
        "## Core Folder Layout",
        "- `by_salesperson/`: recorded call folders grouped by salesperson and month",
        "- `by_contact/`: recorded call folders grouped by contact and month",
        "- `normalized/live_phone_calls.csv`: one-row-per-call index",
        "- `normalized/live_phone_call_utterances.jsonl`: all utterances across all calls",
        "",
        "## Files inside each call folder",
        "- `call.md`: readable call summary plus transcript",
        "- `metadata.json`: machine-friendly metadata and source pointers",
        "- `utterances.json` and `utterances.jsonl`: normalized utterance records",
        "- `transcript.txt`: plain-text transcript",
        "",
        "## Notes",
        "- Lead labels come from the contact export when available.",
        "- This pass is focused on live recorded phone calls only, not voicemail transcripts.",
        "- A few calls may fall back to transcript speaker labels or phone numbers when contact names are missing on the Close call object.",
    ]
    (output_dir / "README.md").write_text("\n".join(readme_lines) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "recorded_live_calls": len(call_index_rows),
                "normalized_utterances": len(all_utterance_rows),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
