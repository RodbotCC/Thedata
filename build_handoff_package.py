#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_PROJECT_ROOT = Path("/Users/jakeaaron/Comeketo")
DEFAULT_DATA_ROOT = Path("/Users/jakeaaron/Comeketo/ComeketoData ")
DEFAULT_LIBRARY_DIR = DEFAULT_DATA_ROOT / "phone_call_transcript_library"
DEFAULT_BUILDERS_DIR = DEFAULT_DATA_ROOT / "close_conversation_export"
DEFAULT_OUTPUT_PARENT = DEFAULT_PROJECT_ROOT / "handoff_packages"

BUILDER_FILENAMES = [
    "build_phone_call_library.py",
    "build_lead_call_dossiers.py",
    "build_lead_message_library.py",
    "build_unlinked_call_library.py",
    "build_lead_email_thread_library.py",
    "build_lead_business_context.py",
    "build_lead_memory_briefs.py",
    "build_lead_deal_sheets.py",
    "build_action_intelligence.py",
    "build_event_ops_registry.py",
    "build_menu_intelligence.py",
    "build_pricing_scope_intelligence.py",
    "build_schedule_commitment_registry.py",
    "build_seller_performance_intelligence.py",
    "build_source_channel_intelligence.py",
    "build_miscommunication_intelligence.py",
    "build_recovery_intelligence.py",
    "build_owner_stage_dashboards.py",
    "build_operational_intelligence.py",
    "build_conversation_intelligence.py",
    "build_handoff_package.py",
]

SOURCE_EXPORT_FILENAMES = [
    "Comeketo Catering contacts 2026-03-26 18-32.json",
    "Comeketo Catering contacts 2026-03-26 18-32.csv",
    "Comeketo Catering leads 2026-03-26 18-32.json",
    "Comeketo Catering opportunities 2026-03-26 18-32.json",
    "Comeketo Catering opportunities 2026-03-26 18-32.csv",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a portable Comeketo handoff package.")
    parser.add_argument("--project-root", type=Path, default=DEFAULT_PROJECT_ROOT)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--library-dir", type=Path, default=DEFAULT_LIBRARY_DIR)
    parser.add_argument("--builders-dir", type=Path, default=DEFAULT_BUILDERS_DIR)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--tag", default=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ"))
    parser.add_argument("--skip-zip", action="store_true")
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def copy_selected_files(source_dir: Path, filenames: List[str], target_dir: Path) -> List[str]:
    copied: List[str] = []
    for filename in filenames:
        source = source_dir / filename
        if not source.exists():
            continue
        shutil.copy2(source, target_dir / filename)
        copied.append(filename)
    return copied


def build_start_here(
    package_name: str,
    library_dir: Path,
    dashboards_dir: Path,
    normalized_dir: Path,
    builders_dir: Path,
    counts: Dict[str, Any],
) -> str:
    lines = [
        f"# {package_name}",
        "",
        "This is the portable Comeketo handoff package built from the normalized Close/CRM exports.",
        "",
        "## Start Here",
        f"- Open `{library_dir / 'README.md'}` for the library map.",
        f"- Open `{dashboards_dir / 'owner_overview.md'}` to browse by salesperson/owner.",
        f"- Open `{dashboards_dir / 'stage_overview.md'}` to browse by stage/pipeline state.",
        f"- Open `{normalized_dir / 'lead_memory_briefs.csv'}` for the quickest one-row-per-lead scan.",
        f"- Open `{normalized_dir / 'follow_up_queue.csv'}` for the action queue.",
        f"- Open `{normalized_dir / 'open_loops.csv'}` for unresolved buyer asks and pending promises.",
        f"- Open `{normalized_dir / 'operator_action_board.csv'}` for operator-ready deal sheets and queues.",
        f"- Open `{normalized_dir / 'owner_task_board.csv'}` for salesperson-owned next steps.",
        f"- Open `{normalized_dir / 'promise_tracker.csv'}` for promised follow-through that still needs closure.",
        f"- Open `{normalized_dir / 'lead_event_ops_registry.csv'}` for the compact event-and-ops registry.",
        f"- Open `{normalized_dir / 'execution_watch_board.csv'}` for leads already approaching execution.",
        f"- Open `{normalized_dir / 'lead_menu_profiles.csv'}` for a compact menu / cuisine / venue-food scan.",
        f"- Open `{normalized_dir / 'menu_customization_board.csv'}` for leads with the most menu-specific questions.",
        f"- Open `{normalized_dir / 'venue_food_restriction_board.csv'}` for kitchen, venue, and food-service constraints.",
        f"- Open `{normalized_dir / 'lead_pricing_scope_profiles.csv'}` for a compact pricing / scope scan.",
        f"- Open `{normalized_dir / 'pricing_action_board.csv'}` for leads where quote or scope movement still needs action.",
        f"- Open `{normalized_dir / 'budget_pressure_board.csv'}` for budget, minimum, travel-fee, or other price-fit friction.",
        f"- Open `{normalized_dir / 'lead_schedule_commitments.csv'}` for a compact timing / follow-through scan.",
        f"- Open `{normalized_dir / 'immediate_deadline_board.csv'}` for deadlines due now or inside 48 hours.",
        f"- Open `{normalized_dir / 'promise_due_board.csv'}` for pending salesperson promises that still need follow-through.",
        f"- Open `{normalized_dir / 'lead_seller_performance_signals.csv'}` for a compact seller-execution scan.",
        f"- Open `{normalized_dir / 'response_speed_board.csv'}` for owner response-speed pressure.",
        f"- Open `{normalized_dir / 'stalled_lead_board.csv'}` for active leads currently stuck.",
        f"- Open `{normalized_dir / 'lead_source_attribution_profiles.csv'}` for a compact source, lane, and channel-mix scan.",
        f"- Open `{normalized_dir / 'source_channel_summary.csv'}` for which entry paths are producing movement, friction, and wins.",
        f"- Open `{normalized_dir / 'source_stall_board.csv'}` for the sources currently generating the most stall pressure.",
        f"- Open `{normalized_dir / 'lead_miscommunication_signals.csv'}` for the one-row-per-lead missed-detail audit.",
        f"- Open `{normalized_dir / 'miscommunication_findings.csv'}` for specific likely misses, broken loops, and crossed signals.",
        f"- Open `{normalized_dir / 'miscommunication_category_summary.csv'}` for the category-level scan of those issues.",
        f"- Open `{normalized_dir / 'lead_recovery_queue.csv'}` for the ranked one-row-per-lead rescue queue.",
        f"- Open `{normalized_dir / 'owner_recovery_board.csv'}` for which owners are carrying the most recovery pressure.",
        f"- Open `{normalized_dir / 'same_day_recovery_queue.csv'}` for what should get cleaned up today.",
        "",
        "## Package Counts",
        f"- Lead dossiers: {counts['lead_dossiers']}",
        f"- Lead memory briefs: {counts['lead_briefs']}",
        f"- Owner dashboards: {counts['owner_dashboards']}",
        f"- Stage dashboards: {counts['stage_dashboards']}",
        f"- Recorded live calls: {counts['recorded_calls']}",
        f"- Unlinked recorded calls: {counts['unlinked_calls']}",
        "",
        "## Included Sections",
        f"- `communication_library/`: full normalized phone/message/lead library",
        f"- `builders/`: reusable scripts that generated the library layers",
        f"- `source_export_manifest.md`: where the original local raw exports came from",
        "",
        "## Suggested Monday Move",
        "- Copy this whole package to the office machine.",
        "- Keep `communication_library/` intact so all relative paths remain simple.",
        "- If you want to rebuild later with fresh exports, use the scripts in `builders/` as the starting point.",
        "",
    ]
    return "\n".join(lines)


def build_source_manifest(data_root: Path, builders_dir: Path, library_dir: Path, source_exports: List[str], builder_files: List[str]) -> str:
    lines = [
        "# Source Export Manifest",
        "",
        "This package does not duplicate the original large CRM/raw export files.",
        "It records where they lived on Jake's machine when this handoff package was built.",
        "",
        "## Original Raw Export Files",
    ]
    for filename in source_exports:
        lines.append(f"- `{data_root / filename}`")
    lines.extend(
        [
            "",
            "## Builder Script Source Folder",
            f"- `{builders_dir}`",
            "",
            "## Library Source Folder",
            f"- `{library_dir}`",
            "",
            "## Included Builder Scripts",
        ]
    )
    for filename in builder_files:
        lines.append(f"- `{filename}`")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    ensure_dir(args.output_parent)

    package_name = f"Comeketo_Handoff_{args.tag}"
    package_dir = args.output_parent / package_name
    if package_dir.exists():
        shutil.rmtree(package_dir)

    communication_target = package_dir / "communication_library"
    builders_target = package_dir / "builders"
    ensure_dir(package_dir)
    ensure_dir(builders_target)

    shutil.copytree(args.library_dir, communication_target, dirs_exist_ok=True)
    copied_builders = copy_selected_files(args.builders_dir, BUILDER_FILENAMES, builders_target)

    normalized_dir = communication_target / "normalized"
    dashboards_dir = communication_target / "dashboards"

    lead_briefs = load_csv_rows(normalized_dir / "lead_memory_briefs.csv")
    owner_dashboards = load_csv_rows(normalized_dir / "owner_dashboard_summary.csv")
    stage_dashboards = load_csv_rows(normalized_dir / "stage_dashboard_summary.csv")
    live_calls = load_csv_rows(normalized_dir / "live_phone_calls.csv")
    unlinked_calls = load_csv_rows(communication_target / "unlinked_calls" / "normalized" / "unlinked_live_calls.csv")

    counts = {
        "lead_dossiers": sum(1 for path in (communication_target / "by_lead").iterdir() if path.is_dir()),
        "lead_briefs": len(lead_briefs),
        "owner_dashboards": len(owner_dashboards),
        "stage_dashboards": len(stage_dashboards),
        "recorded_calls": len(live_calls),
        "unlinked_calls": len(unlinked_calls),
    }

    (package_dir / "START_HERE.md").write_text(
        build_start_here(package_name, communication_target, dashboards_dir, normalized_dir, builders_target, counts),
        encoding="utf-8",
    )
    (package_dir / "source_export_manifest.md").write_text(
        build_source_manifest(args.data_root, args.builders_dir, args.library_dir, SOURCE_EXPORT_FILENAMES, copied_builders),
        encoding="utf-8",
    )

    manifest = {
        "package_name": package_name,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "project_root": str(args.project_root),
        "data_root": str(args.data_root),
        "source_library_dir": str(args.library_dir),
        "package_dir": str(package_dir),
        "copied_builders": copied_builders,
        "counts": counts,
        "key_entrypoints": {
            "library_readme": str(communication_target / "README.md"),
            "owner_overview": str(dashboards_dir / "owner_overview.md"),
            "stage_overview": str(dashboards_dir / "stage_overview.md"),
            "lead_memory_briefs_csv": str(normalized_dir / "lead_memory_briefs.csv"),
            "owner_task_board_csv": str(normalized_dir / "owner_task_board.csv"),
            "promise_tracker_csv": str(normalized_dir / "promise_tracker.csv"),
            "lead_event_ops_registry_csv": str(normalized_dir / "lead_event_ops_registry.csv"),
            "execution_watch_board_csv": str(normalized_dir / "execution_watch_board.csv"),
            "lead_menu_profiles_csv": str(normalized_dir / "lead_menu_profiles.csv"),
            "menu_customization_board_csv": str(normalized_dir / "menu_customization_board.csv"),
            "venue_food_restriction_board_csv": str(normalized_dir / "venue_food_restriction_board.csv"),
            "lead_pricing_scope_profiles_csv": str(normalized_dir / "lead_pricing_scope_profiles.csv"),
            "pricing_action_board_csv": str(normalized_dir / "pricing_action_board.csv"),
            "budget_pressure_board_csv": str(normalized_dir / "budget_pressure_board.csv"),
            "lead_schedule_commitments_csv": str(normalized_dir / "lead_schedule_commitments.csv"),
            "immediate_deadline_board_csv": str(normalized_dir / "immediate_deadline_board.csv"),
            "promise_due_board_csv": str(normalized_dir / "promise_due_board.csv"),
            "lead_seller_performance_signals_csv": str(normalized_dir / "lead_seller_performance_signals.csv"),
            "response_speed_board_csv": str(normalized_dir / "response_speed_board.csv"),
            "stalled_lead_board_csv": str(normalized_dir / "stalled_lead_board.csv"),
            "lead_source_attribution_profiles_csv": str(normalized_dir / "lead_source_attribution_profiles.csv"),
            "source_channel_summary_csv": str(normalized_dir / "source_channel_summary.csv"),
            "source_stall_board_csv": str(normalized_dir / "source_stall_board.csv"),
            "lead_miscommunication_signals_csv": str(normalized_dir / "lead_miscommunication_signals.csv"),
            "miscommunication_findings_csv": str(normalized_dir / "miscommunication_findings.csv"),
            "miscommunication_category_summary_csv": str(normalized_dir / "miscommunication_category_summary.csv"),
            "lead_recovery_queue_csv": str(normalized_dir / "lead_recovery_queue.csv"),
            "owner_recovery_board_csv": str(normalized_dir / "owner_recovery_board.csv"),
            "same_day_recovery_queue_csv": str(normalized_dir / "same_day_recovery_queue.csv"),
        },
    }
    write_json(package_dir / "manifest.json", manifest)

    zip_path = ""
    if not args.skip_zip:
        archive_base = str(args.output_parent / package_name)
        zip_path = shutil.make_archive(archive_base, "zip", root_dir=args.output_parent, base_dir=package_name)
        manifest["zip_path"] = zip_path
        write_json(package_dir / "manifest.json", manifest)

    print(
        json.dumps(
            {
                "package_dir": str(package_dir),
                "zip_path": zip_path,
                "counts": counts,
                "copied_builders": copied_builders,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
