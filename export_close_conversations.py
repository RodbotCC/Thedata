#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import getpass
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://api.close.com/api/v1"
WINDOW_DAYS = 1
WINDOW_OVERLAP_SECONDS = 1
DEFAULT_DAYS_BACK = 30
DEFAULT_PAGE_LIMIT = 100
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class EndpointConfig:
    name: str
    path: str
    detail_fields: Optional[Tuple[str, ...]] = None


ENDPOINTS: Tuple[EndpointConfig, ...] = (
    EndpointConfig("calls", "/activity/call/", ("recording_transcript", "voicemail_transcript")),
    EndpointConfig("meetings", "/activity/meeting/", ("transcripts",)),
    EndpointConfig("email_threads", "/activity/emailthread/"),
    EndpointConfig("emails", "/activity/email/"),
    EndpointConfig("sms", "/activity/sms/"),
    EndpointConfig("whatsapp_messages", "/activity/whatsapp_message/"),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sanitize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def choose_first(mapping: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "", []):
            return value
    return None


class CloseClient:
    def __init__(self, api_key: str, timeout: int = 60, max_retries: int = 5) -> None:
        token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
        self.auth_header = f"Basic {token}"
        self.timeout = timeout
        self.max_retries = max_retries

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._request_json("GET", path, params=params)

    def _request_json(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        query_items: List[Tuple[str, Any]] = []
        if params:
            for key, value in params.items():
                if value is None:
                    continue
                query_items.append((key, value))

        url = f"{BASE_URL}{path}"
        if query_items:
            url = f"{url}?{urlencode(query_items, doseq=True)}"

        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")

        request = Request(
            url,
            method=method,
            data=data,
            headers={
                "Authorization": self.auth_header,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "close-conversation-export/1.0",
            },
        )

        for attempt in range(self.max_retries):
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    body = response.read().decode("utf-8")
                    return json.loads(body) if body else {}
            except HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                if exc.code in RETRYABLE_STATUS_CODES and attempt < self.max_retries - 1:
                    delay = min(30, 2 ** attempt)
                    print(
                        f"[retry] {method} {path} failed with {exc.code}; sleeping {delay}s",
                        file=sys.stderr,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"HTTP {exc.code} for {url}: {body}") from exc
            except URLError as exc:
                if attempt < self.max_retries - 1:
                    delay = min(30, 2 ** attempt)
                    print(
                        f"[retry] {method} {path} failed with network error; sleeping {delay}s",
                        file=sys.stderr,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"Network error for {url}: {exc}") from exc


def iter_time_windows(start_dt: datetime, end_dt: datetime) -> Iterable[Tuple[datetime, datetime]]:
    cursor = start_dt
    overlap = timedelta(seconds=WINDOW_OVERLAP_SECONDS)
    while cursor < end_dt:
        window_end = min(cursor + timedelta(days=WINDOW_DAYS), end_dt)
        effective_start = cursor - overlap if cursor > start_dt else start_dt - overlap
        yield effective_start, window_end
        cursor = window_end


def fetch_list_items(
    client: CloseClient,
    config: EndpointConfig,
    start_dt: datetime,
    end_dt: datetime,
    page_limit: int,
) -> List[Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}

    for window_start, window_end in iter_time_windows(start_dt, end_dt):
        skip = 0
        while True:
            params = {
                "date_created__gt": isoformat_z(window_start),
                "date_created__lt": isoformat_z(window_end),
                "_limit": page_limit,
                "_skip": skip,
            }
            payload = client.get(config.path, params=params)
            items = payload.get("data", [])
            for item in items:
                item_id = item.get("id")
                if item_id:
                    by_id[item_id] = item

            has_more = bool(payload.get("has_more"))
            if not items or not has_more:
                break
            skip += len(items)

    return sorted(
        by_id.values(),
        key=lambda item: choose_first(item, ("date_created", "date_updated", "activity_at", "starts_at")) or "",
    )


def fetch_detail(
    client: CloseClient,
    config: EndpointConfig,
    item: Dict[str, Any],
) -> Dict[str, Any]:
    item_id = item["id"]
    if config.detail_fields:
        detail = client.get(f"{config.path}{item_id}/", params={"_fields": ",".join(config.detail_fields)})
        merged = dict(item)
        merged.update(detail)
        return merged
    return client.get(f"{config.path}{item_id}/")


def join_utterances(utterances: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for utterance in utterances or []:
        speaker = utterance.get("speaker_label") or utterance.get("speaker_side") or "unknown"
        text = sanitize_text(utterance.get("text")).strip()
        start = utterance.get("start")
        end = utterance.get("end")
        timing = ""
        if start is not None or end is not None:
            timing = f" [{start}-{end}]"
        if text:
            lines.append(f"{speaker}{timing}: {text}")
    return "\n".join(lines)


def build_transcript_rows(endpoint_name: str, item: Dict[str, Any]) -> List[Dict[str, Any]]:
    base = {
        "endpoint": endpoint_name,
        "id": item.get("id"),
        "lead_id": item.get("lead_id"),
        "contact_id": item.get("contact_id"),
        "user_id": item.get("user_id"),
        "date_created": choose_first(item, ("date_created", "activity_at", "starts_at")),
        "date_updated": item.get("date_updated"),
        "direction": item.get("direction"),
        "status": item.get("status"),
        "subject_or_title": choose_first(item, ("subject", "title")),
    }

    rows: List[Dict[str, Any]] = []

    if endpoint_name == "calls":
        for transcript_name in ("recording_transcript", "voicemail_transcript"):
            transcript = item.get(transcript_name) or {}
            if transcript:
                rows.append(
                    {
                        **base,
                        "transcript_kind": transcript_name,
                        "summary_text": sanitize_text(transcript.get("summary_text")),
                        "utterance_count": len(transcript.get("utterances") or []),
                        "transcript_text": join_utterances(transcript.get("utterances") or []),
                    }
                )

    if endpoint_name == "meetings":
        for index, transcript in enumerate(item.get("transcripts") or []):
            rows.append(
                {
                    **base,
                    "transcript_kind": f"transcript_{index + 1}",
                    "summary_text": sanitize_text(transcript.get("summary_text")),
                    "utterance_count": len(transcript.get("utterances") or []),
                    "transcript_text": join_utterances(transcript.get("utterances") or []),
                }
            )

    return rows


def build_message_row(endpoint_name: str, item: Dict[str, Any]) -> Dict[str, Any]:
    body_text = choose_first(
        item,
        (
            "body_text",
            "text",
            "note",
            "message_markdown",
            "message_html",
            "body_html",
        ),
    )
    return {
        "endpoint": endpoint_name,
        "id": item.get("id"),
        "thread_id": choose_first(item, ("thread_id", "emailthread_id", "response_to_id")),
        "lead_id": item.get("lead_id"),
        "contact_id": item.get("contact_id"),
        "user_id": item.get("user_id"),
        "date_created": choose_first(item, ("date_created", "activity_at", "starts_at")),
        "date_updated": item.get("date_updated"),
        "direction": item.get("direction"),
        "status": item.get("status"),
        "subject_or_title": choose_first(item, ("subject", "title")),
        "body_or_text": sanitize_text(body_text),
    }


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
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


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def export_endpoint(
    client: CloseClient,
    config: EndpointConfig,
    start_dt: datetime,
    end_dt: datetime,
    page_limit: int,
    raw_dir: Path,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    raw_path = raw_dir / f"{config.name}.json"
    failure_path = raw_dir / f"{config.name}_failures.json"

    if raw_path.exists():
        print(f"[resume] {config.name}: using existing raw file", file=sys.stderr)
        detailed_items = load_json(raw_path)
        transcript_rows: List[Dict[str, Any]] = []
        message_rows: List[Dict[str, Any]] = []
        for detail in detailed_items:
            transcript_rows.extend(build_transcript_rows(config.name, detail))
            message_rows.append(build_message_row(config.name, detail))
        failures = load_json(failure_path) if failure_path.exists() else []
        return detailed_items, transcript_rows, message_rows, failures

    print(f"[fetch] {config.name} list", file=sys.stderr)
    list_items = fetch_list_items(client, config, start_dt, end_dt, page_limit)
    print(f"[fetch] {config.name}: found {len(list_items)} items", file=sys.stderr)

    detailed_items: List[Dict[str, Any]] = []
    transcript_rows: List[Dict[str, Any]] = []
    message_rows: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for index, item in enumerate(list_items, start=1):
        try:
            detail = fetch_detail(client, config, item)
        except Exception as exc:
            failures.append(
                {
                    "endpoint": config.name,
                    "id": item.get("id"),
                    "error": str(exc),
                }
            )
            print(
                f"[warn] {config.name}: failed to fetch {item.get('id')}: {exc}",
                file=sys.stderr,
            )
            continue

        detailed_items.append(detail)
        transcript_rows.extend(build_transcript_rows(config.name, detail))
        message_rows.append(build_message_row(config.name, detail))

        if index % 25 == 0 or index == len(list_items):
            print(
                f"[fetch] {config.name}: detailed {index}/{len(list_items)}",
                file=sys.stderr,
            )

    write_json(raw_path, detailed_items)
    if failures:
        write_json(failure_path, failures)
    return detailed_items, transcript_rows, message_rows, failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Close conversation data and transcripts.")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS_BACK, help="How many days back to export.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to write export files into.",
    )
    parser.add_argument("--page-limit", type=int, default=DEFAULT_PAGE_LIMIT, help="List page size.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    api_key = getpass.getpass("Close API key: ").strip()
    if not api_key:
        print("Missing Close API key.", file=sys.stderr)
        return 1

    started_at = utc_now()
    start_dt = started_at - timedelta(days=args.days)

    output_dir = args.output_dir
    if output_dir is None:
        stamp = started_at.strftime("%Y%m%d_%H%M%S")
        output_dir = Path("/Users/jakeaaron/close_conversation_export") / f"run_{stamp}"

    raw_dir = output_dir / "raw"
    normalized_dir = output_dir / "normalized"
    ensure_dir(raw_dir)
    ensure_dir(normalized_dir)

    client = CloseClient(api_key=api_key)

    try:
        me_payload = client.get("/me/")
    except Exception as exc:  # pragma: no cover
        print(f"Authentication check failed: {exc}", file=sys.stderr)
        return 1

    all_transcript_rows: List[Dict[str, Any]] = []
    all_message_rows: List[Dict[str, Any]] = []
    endpoint_summaries: List[Dict[str, Any]] = []
    all_failures: List[Dict[str, Any]] = []

    print(
        f"[start] Exporting Close conversations from {isoformat_z(start_dt)} to {isoformat_z(started_at)}",
        file=sys.stderr,
    )

    for config in ENDPOINTS:
        detailed_items, transcript_rows, message_rows, failures = export_endpoint(
            client=client,
            config=config,
            start_dt=start_dt,
            end_dt=started_at,
            page_limit=args.page_limit,
            raw_dir=raw_dir,
        )
        all_transcript_rows.extend(transcript_rows)
        all_message_rows.extend(message_rows)
        all_failures.extend(failures)
        endpoint_summaries.append(
            {
                "endpoint": config.name,
                "item_count": len(detailed_items),
                "transcript_count": len(transcript_rows),
                "nonempty_message_count": sum(1 for row in message_rows if row.get("body_or_text")),
                "failure_count": len(failures),
            }
        )

    transcript_rows_sorted = sorted(
        all_transcript_rows,
        key=lambda row: (row.get("date_created") or "", row.get("endpoint") or "", row.get("id") or ""),
    )
    message_rows_sorted = sorted(
        all_message_rows,
        key=lambda row: (row.get("date_created") or "", row.get("endpoint") or "", row.get("id") or ""),
    )

    write_jsonl(normalized_dir / "transcripts.jsonl", transcript_rows_sorted)
    write_csv(normalized_dir / "transcripts.csv", transcript_rows_sorted)
    write_jsonl(normalized_dir / "messages.jsonl", message_rows_sorted)
    write_csv(normalized_dir / "messages.csv", message_rows_sorted)

    manifest = {
        "exported_at": isoformat_z(utc_now()),
        "window_start": isoformat_z(start_dt),
        "window_end": isoformat_z(started_at),
        "organization_user": me_payload,
        "endpoint_summaries": endpoint_summaries,
        "failure_count": len(all_failures),
        "output_dir": str(output_dir),
    }
    write_json(output_dir / "manifest.json", manifest)
    if all_failures:
        write_json(output_dir / "failures.json", all_failures)

    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
