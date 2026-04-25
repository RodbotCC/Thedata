#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
from pathlib import Path
from typing import Any, Dict, List

from export_close_conversations import (
    CloseClient,
    build_message_row,
    build_transcript_rows,
    isoformat_z,
    load_json,
    utc_now,
    write_csv,
    write_json,
    write_jsonl,
)


ENDPOINT_NAMES = (
    "calls",
    "meetings",
    "email_threads",
    "emails",
    "sms",
    "whatsapp_messages",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair a completed Close export with failed items.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Existing export directory.")
    parser.add_argument("--endpoint", required=True, help="Endpoint name, for example sms.")
    parser.add_argument("--item-id", required=True, help="Failed Close activity id to refetch.")
    return parser.parse_args()


def sort_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: item.get("date_created") or item.get("activity_at") or item.get("date_updated") or "",
    )


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    raw_dir = output_dir / "raw"
    normalized_dir = output_dir / "normalized"

    api_key = getpass.getpass("Close API key: ").strip()
    if not api_key:
        raise SystemExit("Missing Close API key.")

    client = CloseClient(api_key=api_key)
    detail = client.get(f"/activity/{args.endpoint}/{args.item_id}/")

    raw_path = raw_dir / f"{args.endpoint}.json"
    raw_items = load_json(raw_path)
    by_id = {item["id"]: item for item in raw_items}
    by_id[detail["id"]] = detail
    repaired_items = sort_items(list(by_id.values()))
    write_json(raw_path, repaired_items)

    failures_path = output_dir / "failures.json"
    all_failures = load_json(failures_path) if failures_path.exists() else []
    all_failures = [
        failure
        for failure in all_failures
        if not (failure.get("endpoint") == args.endpoint and failure.get("id") == args.item_id)
    ]
    write_json(failures_path, all_failures)

    endpoint_failures_path = raw_dir / f"{args.endpoint}_failures.json"
    if endpoint_failures_path.exists():
        endpoint_failures = load_json(endpoint_failures_path)
        endpoint_failures = [failure for failure in endpoint_failures if failure.get("id") != args.item_id]
        write_json(endpoint_failures_path, endpoint_failures)

    transcript_rows: List[Dict[str, Any]] = []
    message_rows: List[Dict[str, Any]] = []
    endpoint_summaries: List[Dict[str, Any]] = []

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
    manifest["endpoint_summaries"] = endpoint_summaries
    manifest["failure_count"] = len(all_failures)
    write_json(manifest_path, manifest)

    print(f"Repaired {args.endpoint}/{args.item_id}")
    print(f"Remaining failures: {len(all_failures)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
