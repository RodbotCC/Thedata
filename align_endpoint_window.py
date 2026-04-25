#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from export_close_conversations import (
    CloseClient,
    build_message_row,
    build_transcript_rows,
    fetch_detail,
    fetch_list_items,
    isoformat_z,
    load_json,
    utc_now,
    write_csv,
    write_json,
    write_jsonl,
    EndpointConfig,
)


ENDPOINT_MAP = {
    "calls": EndpointConfig("calls", "/activity/call/", ("recording_transcript", "voicemail_transcript")),
    "meetings": EndpointConfig("meetings", "/activity/meeting/", ("transcripts",)),
    "email_threads": EndpointConfig("email_threads", "/activity/emailthread/"),
    "emails": EndpointConfig("emails", "/activity/email/"),
    "sms": EndpointConfig("sms", "/activity/sms/"),
    "whatsapp_messages": EndpointConfig("whatsapp_messages", "/activity/whatsapp_message/"),
}

ENDPOINT_NAMES = tuple(ENDPOINT_MAP.keys())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Align one endpoint's raw export to a fixed time window.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Existing export directory.")
    parser.add_argument("--endpoint", choices=sorted(ENDPOINT_MAP.keys()), required=True)
    parser.add_argument("--window-start", required=True, help="UTC ISO timestamp, for example 2026-02-24T18:43:08Z")
    parser.add_argument("--window-end", required=True, help="UTC ISO timestamp, for example 2026-03-26T18:43:08Z")
    parser.add_argument("--page-limit", type=int, default=100)
    return parser.parse_args()


def parse_iso_z(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def in_window(item: Dict[str, Any], start_dt: datetime, end_dt: datetime) -> bool:
    raw = item.get("date_created") or item.get("activity_at") or item.get("date_updated")
    if not raw:
        return False
    created = parse_iso_z(raw)
    return start_dt < created < end_dt


def sort_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: item.get("date_created") or item.get("activity_at") or item.get("date_updated") or "",
    )


def rebuild_outputs(output_dir: Path, window_start: datetime, window_end: datetime) -> None:
    raw_dir = output_dir / "raw"
    normalized_dir = output_dir / "normalized"
    transcript_rows: List[Dict[str, Any]] = []
    message_rows: List[Dict[str, Any]] = []
    endpoint_summaries: List[Dict[str, Any]] = []
    all_failures = load_json(output_dir / "failures.json")

    for endpoint_name in ENDPOINT_NAMES:
        endpoint_raw_path = raw_dir / f"{endpoint_name}.json"
        endpoint_items = load_json(endpoint_raw_path) if endpoint_raw_path.exists() else []
        endpoint_failures_path = raw_dir / f"{endpoint_name}_failures.json"
        endpoint_failures = load_json(endpoint_failures_path) if endpoint_failures_path.exists() else []

        endpoint_transcripts: List[Dict[str, Any]] = []
        endpoint_messages: List[Dict[str, Any]] = []
        for item in endpoint_items:
            endpoint_transcripts.extend(build_transcript_rows(endpoint_name, item))
            endpoint_messages.append(build_message_row(endpoint_name, item))

        transcript_rows.extend(endpoint_transcripts)
        message_rows.extend(endpoint_messages)
        endpoint_summaries.append(
            {
                "endpoint": endpoint_name,
                "item_count": len(endpoint_items),
                "transcript_count": len(endpoint_transcripts),
                "nonempty_message_count": sum(1 for row in endpoint_messages if row.get("body_or_text")),
                "failure_count": len(endpoint_failures),
            }
        )

    transcript_rows = sorted(
        transcript_rows,
        key=lambda row: (row.get("date_created") or "", row.get("endpoint") or "", row.get("id") or ""),
    )
    message_rows = sorted(
        message_rows,
        key=lambda row: (row.get("date_created") or "", row.get("endpoint") or "", row.get("id") or ""),
    )

    write_jsonl(normalized_dir / "transcripts.jsonl", transcript_rows)
    write_csv(normalized_dir / "transcripts.csv", transcript_rows)
    write_jsonl(normalized_dir / "messages.jsonl", message_rows)
    write_csv(normalized_dir / "messages.csv", message_rows)

    manifest_path = output_dir / "manifest.json"
    manifest = load_json(manifest_path)
    manifest["exported_at"] = isoformat_z(utc_now())
    manifest["window_start"] = isoformat_z(window_start)
    manifest["window_end"] = isoformat_z(window_end)
    manifest["endpoint_summaries"] = endpoint_summaries
    manifest["failure_count"] = len(all_failures)
    write_json(manifest_path, manifest)


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    raw_dir = output_dir / "raw"
    endpoint = ENDPOINT_MAP[args.endpoint]
    window_start = parse_iso_z(args.window_start)
    window_end = parse_iso_z(args.window_end)

    api_key = getpass.getpass("Close API key: ").strip()
    if not api_key:
        raise SystemExit("Missing Close API key.")

    client = CloseClient(api_key=api_key)
    target_list = fetch_list_items(client, endpoint, window_start, window_end, args.page_limit)
    target_ids = {item["id"] for item in target_list if item.get("id")}

    existing_items = load_json(raw_dir / f"{args.endpoint}.json")
    kept_items = [item for item in existing_items if item.get("id") in target_ids and in_window(item, window_start, window_end)]
    kept_ids = {item["id"] for item in kept_items}
    missing_summaries = [item for item in target_list if item.get("id") not in kept_ids]

    recovered: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for item in missing_summaries:
        try:
            recovered.append(fetch_detail(client, endpoint, item))
        except Exception as exc:
            failures.append({"endpoint": args.endpoint, "id": item.get("id"), "error": str(exc)})

    final_items = sort_items(kept_items + recovered)
    write_json(raw_dir / f"{args.endpoint}.json", final_items)
    write_json(raw_dir / f"{args.endpoint}_failures.json", failures)

    all_failures_path = output_dir / "failures.json"
    all_failures = load_json(all_failures_path)
    all_failures = [failure for failure in all_failures if failure.get("endpoint") != args.endpoint]
    all_failures.extend(failures)
    write_json(all_failures_path, all_failures)

    rebuild_outputs(output_dir, window_start, window_end)

    print(f"Aligned {args.endpoint} to {isoformat_z(window_start)} .. {isoformat_z(window_end)}")
    print(f"{args.endpoint} items: {len(final_items)}")
    print(f"Remaining failures: {len(all_failures)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
