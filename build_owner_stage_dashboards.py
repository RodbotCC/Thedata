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
DEFAULT_LEAD_BRIEFS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_memory_briefs.csv"
DEFAULT_OPPORTUNITIES_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_opportunities.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "dashboards"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build owner and stage dashboards from the Comeketo lead memory briefs.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--lead-briefs-csv", type=Path, default=DEFAULT_LEAD_BRIEFS_CSV)
    parser.add_argument("--opportunities-csv", type=Path, default=DEFAULT_OPPORTUNITIES_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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


def clean_label(value: Optional[str], fallback: str) -> str:
    text = (value or "").strip()
    return text or fallback


def slugify(value: Optional[str], fallback: str = "unknown") -> str:
    text = (value or "").strip().lower()
    if not text:
        return fallback
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:100] or fallback


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


def dashboard_stage_key(brief_row: Dict[str, str], current_opportunity: Optional[Dict[str, str]]) -> Dict[str, str]:
    if current_opportunity:
        return {
            "pipeline_name": clean_label(current_opportunity.get("pipeline_name"), "Unknown Pipeline"),
            "stage_label": clean_label(current_opportunity.get("status_label"), "Unknown Stage"),
            "stage_type": clean_label(current_opportunity.get("status_type"), "unknown"),
        }
    return {
        "pipeline_name": "Lead Only",
        "stage_label": clean_label(brief_row.get("lead_status_label"), "Unstaged Lead"),
        "stage_type": "lead_only",
    }


def summarize_owner_markdown(owner_name: str, owner_rows: List[Dict[str, Any]], stage_counts: Counter[str]) -> str:
    latest_activity = owner_rows[0].get("latest_observed_activity_utc") if owner_rows else ""
    counts = Counter(row.get("stage_type") or "" for row in owner_rows)
    lines = [
        f"# Owner Dashboard: {owner_name}",
        "",
        "## Snapshot",
        f"- Leads: `{len(owner_rows)}`",
        f"- Active Opportunity Leads: `{counts.get('active', 0)}`",
        f"- Won Opportunity Leads: `{counts.get('won', 0)}`",
        f"- Lost Opportunity Leads: `{counts.get('lost', 0)}`",
        f"- Lead-only / Unstaged: `{counts.get('lead_only', 0)}`",
        f"- Latest Observed Activity (UTC): `{latest_activity or ''}`",
        "",
        "## Stage Breakdown",
    ]
    for label, count in stage_counts.most_common(20):
        lines.append(f"- {label}: {count}")
    lines.extend(["", "## Leads"])
    for row in owner_rows[:50]:
        lines.append(
            f"- `{row.get('latest_observed_activity_utc') or ''}` | {row.get('lead_name') or ''} | "
            f"{row.get('pipeline_name') or ''} / {row.get('stage_label') or ''} | "
            f"{row.get('engagement_state') or ''} | {row.get('suggested_next_move') or ''}"
        )
    lines.append("")
    return "\n".join(lines)


def summarize_stage_markdown(stage_title: str, rows: List[Dict[str, Any]], owner_counts: Counter[str]) -> str:
    latest_activity = rows[0].get("latest_observed_activity_utc") if rows else ""
    lines = [
        f"# Stage Dashboard: {stage_title}",
        "",
        "## Snapshot",
        f"- Leads: `{len(rows)}`",
        f"- Owners Represented: `{len(owner_counts)}`",
        f"- Latest Observed Activity (UTC): `{latest_activity or ''}`",
        "",
        "## Owner Breakdown",
    ]
    for owner, count in owner_counts.most_common(20):
        lines.append(f"- {owner}: {count}")
    lines.extend(["", "## Leads"])
    for row in rows[:60]:
        lines.append(
            f"- `{row.get('latest_observed_activity_utc') or ''}` | {row.get('lead_name') or ''} | "
            f"{row.get('lead_owner_name') or ''} | {row.get('engagement_state') or ''} | "
            f"{row.get('suggested_next_move') or ''}"
        )
    lines.append("")
    return "\n".join(lines)


def summarize_owner_overview(owner_rows: List[Dict[str, Any]]) -> str:
    lines = [
        "# Owner Overview",
        "",
        "## Owners",
    ]
    for row in owner_rows:
        lines.append(
            f"- {row.get('owner_name') or ''}: {row.get('lead_count') or 0} leads | "
            f"active {row.get('active_lead_count') or 0} | won {row.get('won_lead_count') or 0} | "
            f"lost {row.get('lost_lead_count') or 0} | lead-only {row.get('lead_only_count') or 0}"
        )
    lines.append("")
    return "\n".join(lines)


def summarize_stage_overview(stage_rows: List[Dict[str, Any]]) -> str:
    lines = [
        "# Stage Overview",
        "",
        "## Current Stages",
    ]
    for row in stage_rows:
        lines.append(
            f"- {row.get('pipeline_name') or ''} / {row.get('stage_label') or ''}: "
            f"{row.get('lead_count') or 0} leads across {row.get('owner_count') or 0} owners"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    output_dir = args.output_dir
    by_owner_dir = output_dir / "by_owner"
    by_stage_dir = output_dir / "by_stage"
    ensure_dir(output_dir)
    ensure_dir(by_owner_dir)
    ensure_dir(by_stage_dir)

    brief_rows = load_csv_rows(args.lead_briefs_csv)
    opportunities = load_csv_rows(args.opportunities_csv)

    opportunities_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in opportunities:
        opportunities_by_lead[row["lead_id"]].append(row)

    enriched_rows: List[Dict[str, Any]] = []
    owner_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    stage_groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)

    for brief in brief_rows:
        current_opportunity = choose_current_opportunity(opportunities_by_lead.get(brief["lead_id"], []))
        stage = dashboard_stage_key(brief, current_opportunity)
        owner_name = clean_label(brief.get("lead_owner_name"), "Unassigned")
        enriched = {
            **brief,
            "owner_name": owner_name,
            "pipeline_name": stage["pipeline_name"],
            "stage_label": stage["stage_label"],
            "stage_type": stage["stage_type"],
        }
        enriched_rows.append(enriched)
        owner_groups[owner_name].append(enriched)
        stage_groups[(stage["pipeline_name"], stage["stage_type"], stage["stage_label"])].append(enriched)

    for rows in owner_groups.values():
        rows.sort(key=lambda row: row.get("latest_observed_activity_utc") or "", reverse=True)
    for rows in stage_groups.values():
        rows.sort(key=lambda row: row.get("latest_observed_activity_utc") or "", reverse=True)

    owner_summary_rows: List[Dict[str, Any]] = []
    stage_summary_rows: List[Dict[str, Any]] = []
    owner_stage_matrix_rows: List[Dict[str, Any]] = []

    for owner_name, rows in sorted(owner_groups.items(), key=lambda item: (-len(item[1]), item[0].lower())):
        owner_slug = slugify(owner_name)
        owner_dir = by_owner_dir / owner_slug
        ensure_dir(owner_dir)

        stage_counts = Counter(f"{row['pipeline_name']} / {row['stage_label']}" for row in rows)
        type_counts = Counter(row["stage_type"] for row in rows)
        latest_activity = rows[0].get("latest_observed_activity_utc") if rows else ""

        for stage_label, count in stage_counts.items():
            pipeline_name, stage_name = stage_label.split(" / ", 1)
            owner_stage_matrix_rows.append(
                {
                    "owner_name": owner_name,
                    "pipeline_name": pipeline_name,
                    "stage_label": stage_name,
                    "lead_count": count,
                }
            )

        owner_dashboard_path = owner_dir / "dashboard.md"
        write_csv(owner_dir / "lead_briefs.csv", rows)
        write_jsonl(owner_dir / "lead_briefs.jsonl", rows)
        (owner_dashboard_path).write_text(summarize_owner_markdown(owner_name, rows, stage_counts), encoding="utf-8")

        owner_summary_rows.append(
            {
                "owner_name": owner_name,
                "lead_count": len(rows),
                "active_lead_count": type_counts.get("active", 0),
                "won_lead_count": type_counts.get("won", 0),
                "lost_lead_count": type_counts.get("lost", 0),
                "lead_only_count": type_counts.get("lead_only", 0),
                "latest_observed_activity_utc": latest_activity,
                "dashboard_path": str(owner_dashboard_path),
            }
        )

    for (pipeline_name, stage_type, stage_label), rows in sorted(
        stage_groups.items(),
        key=lambda item: (-len(item[1]), item[0][0].lower(), item[0][2].lower()),
    ):
        pipeline_slug = slugify(pipeline_name)
        stage_slug = f"{stage_type}__{slugify(stage_label)}"
        stage_dir = by_stage_dir / pipeline_slug / stage_slug
        ensure_dir(stage_dir)

        owner_counts = Counter(row["owner_name"] for row in rows)
        latest_activity = rows[0].get("latest_observed_activity_utc") if rows else ""
        stage_title = f"{pipeline_name} / {stage_label} ({stage_type})"
        stage_dashboard_path = stage_dir / "dashboard.md"

        write_csv(stage_dir / "lead_briefs.csv", rows)
        write_jsonl(stage_dir / "lead_briefs.jsonl", rows)
        (stage_dashboard_path).write_text(summarize_stage_markdown(stage_title, rows, owner_counts), encoding="utf-8")

        stage_summary_rows.append(
            {
                "pipeline_name": pipeline_name,
                "stage_label": stage_label,
                "stage_type": stage_type,
                "lead_count": len(rows),
                "owner_count": len(owner_counts),
                "latest_observed_activity_utc": latest_activity,
                "dashboard_path": str(stage_dashboard_path),
            }
        )

    owner_summary_rows.sort(key=lambda row: row["lead_count"], reverse=True)
    stage_summary_rows.sort(key=lambda row: row["lead_count"], reverse=True)
    owner_stage_matrix_rows.sort(key=lambda row: (row["owner_name"], row["pipeline_name"], row["stage_label"]))

    write_csv(normalized_dir / "owner_dashboard_summary.csv", owner_summary_rows)
    write_jsonl(normalized_dir / "owner_dashboard_summary.jsonl", owner_summary_rows)
    write_csv(normalized_dir / "stage_dashboard_summary.csv", stage_summary_rows)
    write_jsonl(normalized_dir / "stage_dashboard_summary.jsonl", stage_summary_rows)
    write_csv(normalized_dir / "owner_stage_matrix.csv", owner_stage_matrix_rows)
    write_jsonl(normalized_dir / "owner_stage_matrix.jsonl", owner_stage_matrix_rows)

    (output_dir / "owner_overview.md").write_text(summarize_owner_overview(owner_summary_rows), encoding="utf-8")
    (output_dir / "stage_overview.md").write_text(summarize_stage_overview(stage_summary_rows), encoding="utf-8")

    readme_lines = [
        "# Dashboard Layer",
        "",
        "This folder adds owner and stage navigation on top of the lead memory briefs.",
        "",
        "## What is here",
        "- `owner_overview.md`: top-level owner summary",
        "- `stage_overview.md`: top-level stage summary",
        "- `by_owner/`: one folder per owner with a dashboard and lead brief indexes",
        "- `by_stage/`: one folder per current stage with a dashboard and lead brief indexes",
        "",
        "## Normalized Summary Files",
        "- `../normalized/owner_dashboard_summary.csv`",
        "- `../normalized/stage_dashboard_summary.csv`",
        "- `../normalized/owner_stage_matrix.csv`",
        "",
    ]
    (output_dir / "README.md").write_text("\n".join(readme_lines), encoding="utf-8")

    print(
        json.dumps(
            {
                "owner_dashboards": len(owner_summary_rows),
                "stage_dashboards": len(stage_summary_rows),
                "output_dir": str(output_dir),
                "owner_summary_csv": str(normalized_dir / "owner_dashboard_summary.csv"),
                "stage_summary_csv": str(normalized_dir / "stage_dashboard_summary.csv"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
