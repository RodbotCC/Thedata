#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_LEAD_DEAL_SHEETS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_deal_sheets.csv"
DEFAULT_OPEN_LOOPS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "open_loops.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "action_intelligence"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build executable task / promise layers from normalized deal sheets.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--lead-deal-sheets-csv", type=Path, default=DEFAULT_LEAD_DEAL_SHEETS_CSV)
    parser.add_argument("--open-loops-csv", type=Path, default=DEFAULT_OPEN_LOOPS_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
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


def compact_text(value: Optional[str], limit: int = 260) -> str:
    text = " ".join((value or "").split())
    return text[:limit]


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return text or "unknown"


def task_dedupe_key(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    normalized_text = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (row.get("task_text") or "").lower())).strip()
    return (
        row.get("lead_id") or "",
        row.get("task_owner_type") or "",
        row.get("task_kind") or "",
        normalized_text,
    )


def due_window(readiness_score: int, event_datetime_utc: str) -> Tuple[str, str, str]:
    now = datetime.now(timezone.utc)
    event_dt = parse_iso(event_datetime_utc)
    days_until_event = (event_dt.date() - now.date()).days if event_dt else None

    if days_until_event is not None and days_until_event <= 7:
        due_days = 0
        label = "today"
        reason = "event is within 7 days"
    elif readiness_score >= 105:
        due_days = 0
        label = "today"
        reason = "very high readiness / urgency score"
    elif days_until_event is not None and days_until_event <= 14:
        due_days = 1
        label = "48h"
        reason = "event is within 14 days"
    elif readiness_score >= 90:
        due_days = 1
        label = "48h"
        reason = "high readiness / urgency score"
    elif days_until_event is not None and days_until_event <= 30:
        due_days = 3
        label = "3d"
        reason = "event is within 30 days"
    elif readiness_score >= 75:
        due_days = 3
        label = "3d"
        reason = "mid-high readiness / urgency score"
    elif readiness_score >= 60:
        due_days = 7
        label = "7d"
        reason = "active but not immediate"
    else:
        due_days = 14
        label = "14d"
        reason = "watch / slower-moving lead"

    due_at = now + timedelta(days=due_days)
    return label, iso_z(due_at), reason


def make_task(
    lead: Dict[str, str],
    task_owner_type: str,
    task_owner_name: str,
    task_kind: str,
    task_category: str,
    task_text: str,
    source_text: str,
    source_path: str,
    status: str,
    priority_score: int,
    due_bucket: str,
    due_date_utc: str,
    due_reason: str,
) -> Dict[str, Any]:
    return {
        "lead_id": lead.get("lead_id") or "",
        "lead_name": lead.get("lead_name") or "",
        "lead_owner_name": lead.get("lead_owner_name") or "",
        "stage_label": lead.get("stage_label") or "",
        "readiness_bucket": lead.get("readiness_bucket") or "",
        "readiness_score": priority_score,
        "task_owner_type": task_owner_type,
        "task_owner_name": task_owner_name,
        "task_kind": task_kind,
        "task_category": task_category,
        "task_text": compact_text(task_text, 500),
        "source_text": compact_text(source_text, 500),
        "source_path": source_path,
        "status": status,
        "due_bucket": due_bucket,
        "due_date_utc": due_date_utc,
        "due_reason": due_reason,
        "event_datetime_utc": lead.get("event_datetime_utc") or "",
    }


def buyer_question_task(loop: Dict[str, str]) -> str:
    category = loop.get("category") or ""
    mapping = {
        "quote_revision": "Revise the quote and answer the requested changes.",
        "pricing": "Answer the pricing question and anchor the next-step decision.",
        "menu_selection": "Send menu options / customization choices and ask for selections.",
        "tasting": "Send tasting details or offer a non-tasting path forward.",
        "bar_service": "Clarify bar options and pricing.",
        "logistics": "Answer the logistics / staffing / venue question clearly.",
        "availability": "Reply with availability and the next workable slot.",
        "guest_count_scope": "Confirm whether the guest count / scope works and whether pricing changes.",
    }
    return mapping.get(category, "Reply to the buyer question and move the lead forward.")


def promise_task(loop: Dict[str, str]) -> str:
    category = loop.get("category") or ""
    mapping = {
        "send_quote": "Fulfill the promised quote follow-up.",
        "send_menu": "Fulfill the promised menu / options follow-up.",
        "send_tasting_details": "Fulfill the promised tasting-details follow-up.",
        "confirm_logistics": "Fulfill the promised logistics clarification.",
        "follow_up": "Close the follow-up promise instead of leaving it hanging.",
    }
    return mapping.get(category, "Fulfill the promise captured in the conversation.")


def build_bucket_tasks(lead: Dict[str, str], now_utc: datetime) -> List[Dict[str, Any]]:
    tasks: List[Dict[str, Any]] = []
    bucket = lead.get("readiness_bucket") or ""
    owner_name = lead.get("lead_owner_name") or "Sales Owner"
    customer_name = lead.get("lead_name") or "Customer"
    priority_score = int(lead.get("readiness_score") or 0)
    due_bucket, due_date_utc, due_reason = due_window(priority_score, lead.get("event_datetime_utc") or "")

    def add(owner_type: str, owner: str, kind: str, category: str, task_text: str, source_text: str = "", status: str = "open") -> None:
        tasks.append(
            make_task(
                lead=lead,
                task_owner_type=owner_type,
                task_owner_name=owner,
                task_kind=kind,
                task_category=category,
                task_text=task_text,
                source_text=source_text or task_text,
                source_path=lead.get("deal_sheet_path") or "",
                status=status,
                priority_score=priority_score,
                due_bucket=due_bucket,
                due_date_utc=due_date_utc,
                due_reason=due_reason,
            )
        )

    if bucket == "suppressed":
        return tasks
    if bucket == "contracting_or_deposit":
        add("sales_owner", owner_name, "operator_task", "contracting_or_deposit", "Confirm contract/deposit timing with the client and close the booking step.")
        add("customer", customer_name, "customer_task", "deposit_or_contract", "Sign the agreement and submit the deposit to secure the date.")
    elif bucket == "venue_pending":
        add("sales_owner", owner_name, "operator_task", "venue_pending", "Verify venue status and catering rules before revising the proposal any further.")
        add("customer", customer_name, "customer_task", "venue_selection", "Confirm the venue choice and whether outside catering is allowed.")
    elif bucket == "venue_conflict":
        add("sales_owner", owner_name, "operator_task", "venue_conflict", "Clarify whether the venue allows outside catering and whether Comeketo can still stay in play.")
        add("customer", customer_name, "customer_task", "venue_conflict", "Decide whether to stay with the venue package or continue with Comeketo.")
    elif bucket == "budget_risk":
        add("sales_owner", owner_name, "operator_task", "budget_risk", "Rescope the package and send a lower-cost path that still works for the event.")
        add("customer", customer_name, "customer_task", "budget_clarity", "Confirm the real budget ceiling and which service elements matter most.")
    elif bucket == "tasting_pending":
        add("sales_owner", owner_name, "operator_task", "tasting_pending", "Get a tasting decision or offer a non-tasting path to a quote/deposit.")
        add("customer", customer_name, "customer_task", "tasting_confirmation", "Confirm whether you are attending the tasting and which date works.")
    elif bucket == "quote_outstanding":
        add("sales_owner", owner_name, "operator_task", "quote_outstanding", "Send or revise the quote and ask for a concrete yes/no next step.")
    elif bucket == "decision_pending":
        add("sales_owner", owner_name, "operator_task", "decision_pending", "Follow up with a decision deadline instead of an open-ended check-in.")
        add("customer", customer_name, "customer_task", "decision_review", "Review the proposal with the actual decision-maker and reply with a decision deadline.")
    elif bucket == "booked_fulfillment_watch":
        add("ops_internal", "Internal Ops", "ops_handoff", "booked_fulfillment_watch", "Confirm headcount, logistics, staffing, and final payment timing for the booked event.")
        add("customer", customer_name, "customer_task", "final_logistics", "Confirm final logistics, venue access, and headcount as the event approaches.")
    elif bucket == "won_pipeline_watch":
        add("ops_internal", "Internal Ops", "ops_handoff", "won_pipeline_watch", "Verify there are no unresolved fulfillment or payment gaps.")
    else:
        add("sales_owner", owner_name, "operator_task", "active_pipeline", lead.get("operator_move") or "Continue the lead with the next sensible milestone.")

    risk_flags = lead.get("risk_flags") or ""
    if "partner_or_family_review" in risk_flags or "vendor_comparison" in risk_flags or "legal_review" in risk_flags or "external_party_review" in risk_flags:
        add("customer", customer_name, "customer_task", "decision_review", "Complete the outstanding stakeholder review and respond with a decision window.")

    return tasks


def build_loop_tasks(lead: Dict[str, str], loops: Sequence[Dict[str, str]]) -> List[Dict[str, Any]]:
    tasks: List[Dict[str, Any]] = []
    owner_name = lead.get("lead_owner_name") or "Sales Owner"
    priority_base = int(lead.get("readiness_score") or 0)

    for loop in loops:
        due_bucket, due_date_utc, due_reason = due_window(priority_base + 5, lead.get("event_datetime_utc") or "")
        if loop.get("loop_type") == "buyer_question":
            tasks.append(
                make_task(
                    lead=lead,
                    task_owner_type="sales_owner",
                    task_owner_name=owner_name,
                    task_kind="buyer_question_reply",
                    task_category=loop.get("category") or "",
                    task_text=buyer_question_task(loop),
                    source_text=loop.get("text") or "",
                    source_path=loop.get("source_path") or "",
                    status="open",
                    priority_score=priority_base + 5,
                    due_bucket=due_bucket,
                    due_date_utc=due_date_utc,
                    due_reason=due_reason,
                )
            )
        elif loop.get("loop_type") == "sales_commitment":
            tasks.append(
                make_task(
                    lead=lead,
                    task_owner_type="sales_owner",
                    task_owner_name=owner_name,
                    task_kind="promise_followthrough",
                    task_category=loop.get("category") or "",
                    task_text=promise_task(loop),
                    source_text=loop.get("text") or "",
                    source_path=loop.get("source_path") or "",
                    status="pending",
                    priority_score=priority_base + 4,
                    due_bucket=due_bucket,
                    due_date_utc=due_date_utc,
                    due_reason=due_reason,
                )
            )
    return tasks


def dedupe_tasks(tasks: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for task in sorted(tasks, key=lambda row: (int(row.get("readiness_score") or 0), row.get("due_date_utc") or ""), reverse=True):
        key = task_dedupe_key(task)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(task)
    return deduped


def build_lead_markdown(lead: Dict[str, str], sales_tasks: Sequence[Dict[str, Any]], customer_tasks: Sequence[Dict[str, Any]], ops_tasks: Sequence[Dict[str, Any]], promise_tasks: Sequence[Dict[str, Any]]) -> str:
    lines = [
        f"# Action Plan: {lead.get('lead_name') or ''}",
        "",
        "## Snapshot",
        f"- Owner: {lead.get('lead_owner_name') or ''}",
        f"- Stage: {lead.get('stage_label') or ''}",
        f"- Readiness Bucket: {lead.get('readiness_bucket') or ''}",
        f"- Readiness Score: `{lead.get('readiness_score') or ''}`",
        f"- Event Date (UTC): `{lead.get('event_datetime_utc') or ''}`",
        f"- Top Open Loop: {lead.get('top_open_loop_text') or ''}",
        "",
        "## Sales-Owned Tasks",
    ]
    if sales_tasks:
        for task in sales_tasks[:8]:
            lines.append(f"- `{task.get('due_bucket')}` | {task.get('task_kind')} | {task.get('task_text')}")
            if task.get("source_text"):
                lines.append(f"  Evidence: {task.get('source_text')}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Customer-Waiting Tasks"])
    if customer_tasks:
        for task in customer_tasks[:8]:
            lines.append(f"- `{task.get('due_bucket')}` | {task.get('task_text')}")
            if task.get("source_text"):
                lines.append(f"  Evidence: {task.get('source_text')}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Internal Handoffs"])
    if ops_tasks:
        for task in ops_tasks[:8]:
            lines.append(f"- `{task.get('due_bucket')}` | {task.get('task_text')}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Promise Tracker"])
    if promise_tasks:
        for task in promise_tasks[:8]:
            lines.append(f"- `{task.get('due_bucket')}` | {task.get('task_text')}")
            if task.get("source_text"):
                lines.append(f"  Promise: {task.get('source_text')}")
    else:
        lines.append("- None.")

    lines.append("")
    return "\n".join(lines)


def build_board_markdown(title: str, rows: Sequence[Dict[str, Any]], count_label: str) -> str:
    lines = [f"# {title}", "", f"- Total {count_label}: `{len(rows)}`", "", "## Top Tasks"]
    for row in rows[:120]:
        lines.append(
            f"- `{row.get('readiness_score')}` | {row.get('lead_name')} | {row.get('task_owner_name')} | "
            f"{row.get('due_bucket')} | {row.get('task_text')}"
        )
    lines.append("")
    return "\n".join(lines)


def build_readme(all_tasks: Sequence[Dict[str, Any]], sales_rows: Sequence[Dict[str, Any]], customer_rows: Sequence[Dict[str, Any]], ops_rows: Sequence[Dict[str, Any]], promise_rows: Sequence[Dict[str, Any]]) -> str:
    due_counts = Counter(row.get("due_bucket") or "" for row in all_tasks)
    lines = [
        "# Action Intelligence",
        "",
        "This layer turns the deal and conversation layers into executable tasks: sales-owned actions, customer-waiting items, internal handoffs, and promise follow-through.",
        "",
        "## Snapshot",
        f"- Total action items: `{len(all_tasks)}`",
        f"- Sales-owned tasks: `{len(sales_rows)}`",
        f"- Customer-waiting tasks: `{len(customer_rows)}`",
        f"- Internal handoffs: `{len(ops_rows)}`",
        f"- Promise tracker items: `{len(promise_rows)}`",
        "",
        "## Due Windows",
    ]
    for label, count in due_counts.items():
        lines.append(f"- {label}: {count}")
    lines.extend(
        [
            "",
            "## Key Files",
            "- `owner_task_board.md`: all sales-owned tasks across active leads",
            "- `customer_waiting_board.md`: tasks currently waiting on the buyer",
            "- `ops_handoff_board.md`: booked-event and internal execution handoffs",
            "- `promise_tracker.md`: pending salesperson promises pulled from the conversation layer",
            "- `../normalized/action_items.csv`: machine-friendly all-task index",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    output_dir = args.output_dir
    by_owner_dir = output_dir / "by_owner"
    ensure_dir(output_dir)
    ensure_dir(by_owner_dir)

    leads = load_csv_rows(args.lead_deal_sheets_csv)
    loops_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in load_csv_rows(args.open_loops_csv):
        loops_by_lead[row["lead_id"]].append(row)

    all_tasks: List[Dict[str, Any]] = []

    for lead in leads:
        lead_id = lead["lead_id"]
        lead_dir = Path(lead["deal_sheet_path"]).parent
        loop_tasks = build_loop_tasks(lead, loops_by_lead.get(lead_id, []))
        base_tasks = build_bucket_tasks(lead, datetime.now(timezone.utc))
        lead_tasks = dedupe_tasks(base_tasks + loop_tasks)

        sales_tasks = [row for row in lead_tasks if row.get("task_owner_type") == "sales_owner"]
        customer_tasks = [row for row in lead_tasks if row.get("task_owner_type") == "customer"]
        ops_tasks = [row for row in lead_tasks if row.get("task_owner_type") == "ops_internal"]
        promise_tasks = [row for row in lead_tasks if row.get("task_kind") == "promise_followthrough"]

        payload = {
            "lead_id": lead_id,
            "lead_name": lead.get("lead_name") or "",
            "lead_owner_name": lead.get("lead_owner_name") or "",
            "stage_label": lead.get("stage_label") or "",
            "readiness_bucket": lead.get("readiness_bucket") or "",
            "readiness_score": lead.get("readiness_score") or "",
            "event_datetime_utc": lead.get("event_datetime_utc") or "",
            "sales_tasks": sales_tasks,
            "customer_tasks": customer_tasks,
            "ops_tasks": ops_tasks,
            "promise_tasks": promise_tasks,
            "task_count": len(lead_tasks),
        }

        write_json(lead_dir / "lead_action_plan.json", payload)
        (lead_dir / "lead_action_plan.md").write_text(
            build_lead_markdown(lead, sales_tasks, customer_tasks, ops_tasks, promise_tasks),
            encoding="utf-8",
        )
        all_tasks.extend(lead_tasks)

    all_tasks = dedupe_tasks(all_tasks)
    sales_rows = [row for row in all_tasks if row.get("task_owner_type") == "sales_owner"]
    customer_rows = [row for row in all_tasks if row.get("task_owner_type") == "customer"]
    ops_rows = [row for row in all_tasks if row.get("task_owner_type") == "ops_internal"]
    promise_rows = [row for row in all_tasks if row.get("task_kind") == "promise_followthrough"]

    write_csv(normalized_dir / "action_items.csv", all_tasks)
    write_jsonl(normalized_dir / "action_items.jsonl", all_tasks)
    write_csv(normalized_dir / "owner_task_board.csv", sales_rows)
    write_jsonl(normalized_dir / "owner_task_board.jsonl", sales_rows)
    write_csv(normalized_dir / "customer_waiting_board.csv", customer_rows)
    write_jsonl(normalized_dir / "customer_waiting_board.jsonl", customer_rows)
    write_csv(normalized_dir / "ops_handoff_board.csv", ops_rows)
    write_jsonl(normalized_dir / "ops_handoff_board.jsonl", ops_rows)
    write_csv(normalized_dir / "promise_tracker.csv", promise_rows)
    write_jsonl(normalized_dir / "promise_tracker.jsonl", promise_rows)

    (output_dir / "README.md").write_text(
        build_readme(all_tasks, sales_rows, customer_rows, ops_rows, promise_rows),
        encoding="utf-8",
    )
    (output_dir / "owner_task_board.md").write_text(
        build_board_markdown("Owner Task Board", sales_rows, "sales-owned tasks"),
        encoding="utf-8",
    )
    (output_dir / "customer_waiting_board.md").write_text(
        build_board_markdown("Customer Waiting Board", customer_rows, "customer-waiting tasks"),
        encoding="utf-8",
    )
    (output_dir / "ops_handoff_board.md").write_text(
        build_board_markdown("Ops Handoff Board", ops_rows, "internal handoff tasks"),
        encoding="utf-8",
    )
    (output_dir / "promise_tracker.md").write_text(
        build_board_markdown("Promise Tracker", promise_rows, "promise follow-through tasks"),
        encoding="utf-8",
    )

    by_owner_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in sales_rows:
        by_owner_rows[slugify(row.get("task_owner_name") or "unknown")].append(row)

    for owner_slug, rows in by_owner_rows.items():
        owner_dir = by_owner_dir / owner_slug
        ensure_dir(owner_dir)
        write_csv(owner_dir / "task_board.csv", rows)
        write_jsonl(owner_dir / "task_board.jsonl", rows)

    print(
        json.dumps(
            {
                "all_tasks": len(all_tasks),
                "sales_rows": len(sales_rows),
                "customer_rows": len(customer_rows),
                "ops_rows": len(ops_rows),
                "promise_rows": len(promise_rows),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
