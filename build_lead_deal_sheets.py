#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_LEAD_BRIEFS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_memory_briefs.csv"
DEFAULT_EVENT_FACTS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_event_facts.csv"
DEFAULT_FOLLOW_UP_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "follow_up_queue.csv"
DEFAULT_CONVERSATION_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_conversation_intelligence.csv"
DEFAULT_OPEN_LOOPS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "open_loops.csv"
DEFAULT_OPPORTUNITIES_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_opportunities.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "deal_intelligence"

NOISE_PATTERNS = (
    "at the tone, please record your message",
    "your call has been forwarded",
    "person you're trying to reach is not available",
    "when you've finished recording",
    "privacy policy",
    "facebook",
    "instagram",
    "website",
    "direct line",
    "catering main",
)

SERVICE_STYLE_RULES: List[Tuple[str, Sequence[str]]] = [
    ("drop_off_only", (r"\bdrop[- ]off only\b", r"\bsetup only\b")),
    ("family_style", (r"\bfamily[- ]style\b",)),
    ("churrasco", (r"\bchurrasco\b", r"\bgrill(?:ing)?\b", r"\bon-site grilling\b")),
    ("plated_service", (r"\bplated\b", r"\bseated dinner\b")),
    ("buffet_service", (r"\bbuffet\b",)),
]

BAR_RULES: List[Tuple[str, Sequence[str]]] = [
    ("no_bar_service", (r"\bdon'?t need (?:the )?bar\b", r"\bno bar\b", r"\bwithout the wine\b")),
    ("byob", (r"\bbyob\b",)),
    ("open_bar", (r"\bopen bar\b",)),
    ("cash_bar", (r"\bcash bar\b",)),
    ("mobile_bar", (r"\bmobile bar\b",)),
    ("beer_wine", (r"\bbeer and wine\b", r"\bbeer, wine\b")),
]

STAFFING_RULES: List[Tuple[str, Sequence[str]]] = [
    ("drop_off_only", (r"\bdrop[- ]off only\b", r"\bsetup only\b")),
    ("staffed_service", (r"\bstaff(?:ing)?\b", r"\bbuffet attendants?\b", r"\bfull-service\b", r"\bon-site staff\b")),
    ("on_site_grilling", (r"\bon-site grilling\b", r"\bbring and operate their own grill\b")),
]

PAYMENT_ACTIVE_PATTERNS = (
    r"\blegal can sign\b",
    r"\bsend (?:your )?contract\b",
    r"\bplease send .*contract\b",
    r"\bmake the deposit\b",
    r"\bsubmit deposit\b",
    r"\bpay all this\b",
    r"\bready to secure\b",
    r"\bsign (?:the )?agreement\b",
)

PAYMENT_DISCUSSION_PATTERNS = (
    r"\bdeposit\b",
    r"\bcontract\b",
    r"\bagreement\b",
    r"\bsecure your date\b",
    r"\bpayment\b",
)

BUDGET_HIGH_PATTERNS = (
    r"\bout of budget\b",
    r"\bnot in our budget\b",
    r"\bdoesn'?t fit\b.*\bbudget\b",
    r"\btoo expensive\b",
    r"\bprice range\b",
    r"\bbudget range\b",
)

BUDGET_ACTIVE_PATTERNS = (
    r"\bbudget\b",
    r"\bprice\b",
    r"\bcost\b",
    r"\bquote\b",
    r"\bpricing\b",
    r"\bhow much\b",
    r"\bprice difference\b",
)

FAMILY_DECISION_PATTERNS = (
    r"\bfianc(?:e|ee|é)\b",
    r"\bhusband\b",
    r"\bwife\b",
    r"\bmy son\b",
    r"\bmy daughter\b",
    r"\breview with\b",
    r"\btalk to my\b",
    r"\bcheck with\b",
)

LEGAL_DECISION_PATTERNS = (
    r"\blegal\b",
    r"\bowner of\b",
)

VENDOR_COMPARE_PATTERNS = (
    r"\bother packages\b",
    r"\banother catering\b",
    r"\bother caterer\b",
    r"\bweigh our options\b",
    r"\bchristalight\b",
)

VENUE_CONFLICT_PATTERNS = (
    r"\bvenue provides the food\b",
    r"\bvenue offers catering\b",
    r"\boutside catering options\b",
    r"\boutside catering\b",
)

VENUE_PENDING_PATTERNS = (
    r"\blooking for a venue\b",
    r"\blook for two venues\b",
    r"\bdecide between 2 venues\b",
    r"\bdecide between two venues\b",
    r"\bvenue not yet secured\b",
    r"\bstill confirming\b.*\bvenue\b",
    r"\bneed venue name\b",
    r"\bvenue pending\b",
    r"\bdecide, but we're just getting information\b",
)

PRIVATE_HOME_PATTERNS = (
    r"\bbackyard\b",
    r"\bhome wedding\b",
    r"\bat our home\b",
    r"\bhouse\b",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build compact commercial deal sheets from the normalized Comeketo library.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--lead-briefs-csv", type=Path, default=DEFAULT_LEAD_BRIEFS_CSV)
    parser.add_argument("--event-facts-csv", type=Path, default=DEFAULT_EVENT_FACTS_CSV)
    parser.add_argument("--follow-up-csv", type=Path, default=DEFAULT_FOLLOW_UP_CSV)
    parser.add_argument("--conversation-csv", type=Path, default=DEFAULT_CONVERSATION_CSV)
    parser.add_argument("--open-loops-csv", type=Path, default=DEFAULT_OPEN_LOOPS_CSV)
    parser.add_argument("--opportunities-csv", type=Path, default=DEFAULT_OPPORTUNITIES_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


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


def compact_text(value: Optional[str], limit: int = 240) -> str:
    text = " ".join((value or "").split())
    return text[:limit]


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return text or "unknown"


def dedupe_key(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def is_noise_text(text: str) -> bool:
    lowered = text.lower().strip()
    if not lowered:
        return True
    return any(pattern in lowered for pattern in NOISE_PATTERNS)


def choose_current_opportunity(rows: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    if not rows:
        return None
    status_rank = {"active": 0, "won": 1, "lost": 2}
    return sorted(
        rows,
        key=lambda row: (
            status_rank.get((row.get("status_type") or "").lower(), 99),
            row.get("date_updated_utc") or row.get("date_created_utc") or "",
        ),
        reverse=False,
    )[0]


def sort_items_by_time(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(items, key=lambda item: item.get("event_datetime_utc") or "", reverse=True)


def build_text_rows(
    conversation_payload: Dict[str, Any],
    event_facts: Dict[str, str],
    current_opp: Optional[Dict[str, str]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()
    sections = ("buyer_asks", "blockers", "preferences", "sales_commitments", "open_loops")
    for section in sections:
        for item in sort_items_by_time(conversation_payload.get(section, [])):
            text = compact_text(item.get("text"), limit=800)
            if not text or is_noise_text(text):
                continue
            key = (section, item.get("category") or "", dedupe_key(text))
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "section": section,
                    "category": item.get("category") or "",
                    "text": text,
                    "event_datetime_utc": item.get("event_datetime_utc") or "",
                    "source_path": item.get("source_path") or "",
                }
            )

    extra_texts = [
        ("latest_signal", "", event_facts.get("latest_buyer_signal") or "", event_facts.get("latest_observed_activity_utc") or "", ""),
        (
            "current_opportunity",
            "",
            current_opp.get("note") or current_opp.get("snapshot_summary") or "",
            current_opp.get("date_updated_utc") or current_opp.get("date_created_utc") or "",
            current_opp.get("opportunity_folder") or "",
        )
        if current_opp
        else ("", "", "", "", ""),
    ]

    for section, category, text, dt_value, source_path in extra_texts:
        if not section or not text or is_noise_text(text):
            continue
        key = (section, category, dedupe_key(text))
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "section": section,
                "category": category,
                "text": compact_text(text, limit=800),
                "event_datetime_utc": dt_value,
                "source_path": source_path,
            }
        )

    return sorted(rows, key=lambda row: row.get("event_datetime_utc") or "", reverse=True)


def collect_labels(text_rows: Sequence[Dict[str, Any]], rules: Sequence[Tuple[str, Sequence[str]]]) -> Tuple[List[str], Dict[str, str]]:
    labels: List[str] = []
    evidence: Dict[str, str] = {}
    for row in text_rows:
        text = row.get("text") or ""
        for label, patterns in rules:
            if label in evidence:
                continue
            if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
                evidence[label] = text
                labels.append(label)
    return labels, evidence


def first_matching_text(text_rows: Sequence[Dict[str, Any]], patterns: Sequence[str]) -> str:
    for row in text_rows:
        text = row.get("text") or ""
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
            return compact_text(text, limit=500)
    return ""


def detect_venue_status(event_facts: Dict[str, str], text_rows: Sequence[Dict[str, Any]]) -> Tuple[str, str]:
    evidence = first_matching_text(text_rows, VENUE_CONFLICT_PATTERNS)
    if evidence:
        return "venue_conflict", evidence

    venue_name = (event_facts.get("venue_name") or "").strip()
    venue_address = (event_facts.get("venue_address") or "").strip()
    venue_type = (event_facts.get("venue_type") or "").strip()
    venue_city = (event_facts.get("venue_city") or "").strip()
    if venue_name or venue_address:
        return "venue_named", compact_text(" | ".join(part for part in (venue_name, venue_address, venue_city) if part), 500)

    evidence = first_matching_text(text_rows, PRIVATE_HOME_PATTERNS)
    if evidence:
        return "private_home", evidence

    evidence = first_matching_text(text_rows, VENUE_PENDING_PATTERNS)
    if evidence:
        return "venue_pending", evidence

    if venue_type and "needs venue" not in venue_type.lower():
        return "venue_type_captured", venue_type
    if venue_city:
        return "area_known_only", venue_city
    return "unknown", ""


def detect_budget_state(text_rows: Sequence[Dict[str, Any]]) -> Tuple[str, str]:
    evidence = first_matching_text(text_rows, BUDGET_HIGH_PATTERNS)
    if evidence:
        return "budget_risk_high", evidence
    evidence = first_matching_text(text_rows, BUDGET_ACTIVE_PATTERNS)
    if evidence:
        return "pricing_under_review", evidence
    return "no_budget_signal", ""


def detect_payment_state(stage_label: str, text_rows: Sequence[Dict[str, Any]]) -> Tuple[str, str]:
    lowered_stage = stage_label.lower()
    if "deposit" in lowered_stage:
        return "deposit_stage", stage_label
    evidence = first_matching_text(text_rows, PAYMENT_ACTIVE_PATTERNS)
    if evidence:
        return "contract_or_deposit_active", evidence
    evidence = first_matching_text(text_rows, PAYMENT_DISCUSSION_PATTERNS)
    if evidence:
        return "deposit_or_contract_discussed", evidence
    return "no_payment_signal", ""


def detect_decision_state(text_rows: Sequence[Dict[str, Any]]) -> Tuple[str, str]:
    evidence = first_matching_text(text_rows, PAYMENT_ACTIVE_PATTERNS)
    if evidence and re.search(r"\blegal\b", evidence, flags=re.IGNORECASE):
        return "legal_review", evidence
    evidence = first_matching_text(text_rows, VENDOR_COMPARE_PATTERNS)
    if evidence:
        return "vendor_comparison", evidence
    evidence = first_matching_text(text_rows, FAMILY_DECISION_PATTERNS)
    if evidence:
        return "partner_or_family_review", evidence
    evidence = first_matching_text(text_rows, LEGAL_DECISION_PATTERNS)
    if evidence:
        return "external_party_review", evidence
    return "direct_or_unknown", ""


def readiness_bucket(
    stage_label: str,
    stage_type: str,
    queue_bucket: str,
    venue_status: str,
    budget_state: str,
    payment_state: str,
    decision_state: str,
    open_loop_count: int,
    days_until_event: Optional[int],
) -> str:
    lowered_stage = stage_label.lower()
    if stage_type == "lost" or any(token in lowered_stage for token in ("lost", "archived", "do not call")):
        return "suppressed"
    if payment_state in {"contract_or_deposit_active", "deposit_stage"}:
        return "contracting_or_deposit"
    if stage_type == "won" and days_until_event is not None and days_until_event <= 60:
        return "booked_fulfillment_watch"
    if venue_status == "venue_conflict":
        return "venue_conflict"
    if venue_status == "venue_pending":
        return "venue_pending"
    if budget_state == "budget_risk_high":
        return "budget_risk"
    if "tasting" in lowered_stage or queue_bucket == "schedule_tasting":
        return "tasting_pending"
    if queue_bucket in {"advance_quote", "reply_to_inbound"} or "quote" in lowered_stage or open_loop_count > 0:
        return "quote_outstanding"
    if decision_state in {"partner_or_family_review", "vendor_comparison", "legal_review", "external_party_review"}:
        return "decision_pending"
    if stage_type == "won":
        return "won_pipeline_watch"
    return "active_pipeline"


def recommended_move(bucket: str) -> str:
    mapping = {
        "suppressed": "Do not actively chase; only revisit with clear reason.",
        "contracting_or_deposit": "Push contract/deposit across the line and confirm signature timing.",
        "booked_fulfillment_watch": "Confirm logistics, headcount, and final payment timing for the booked event.",
        "venue_conflict": "Clarify venue catering rules and whether Comeketo can still serve the event.",
        "venue_pending": "Lock the venue decision or at least get catering-permission clarity before revising scope.",
        "budget_risk": "Rescope the package, trim service levels, or present a lower-cost path.",
        "tasting_pending": "Get the tasting date confirmed or convert the conversation into a decision-driving quote step.",
        "quote_outstanding": "Send or revise the quote and ask for a concrete next-step decision.",
        "decision_pending": "Get the real decision-maker into the loop and press for a decision deadline.",
        "won_pipeline_watch": "Keep the booked deal visible and confirm there are no hidden fulfillment gaps.",
        "active_pipeline": "Continue normal follow-up with the next milestone in mind.",
    }
    return mapping.get(bucket, "Review the dossier and choose the next commercial move.")


def compute_readiness_score(
    base_score: int,
    bucket: str,
    venue_status: str,
    budget_state: str,
    payment_state: str,
    open_loop_count: int,
) -> int:
    score = base_score
    if payment_state == "contract_or_deposit_active":
        score += 14
    elif payment_state == "deposit_stage":
        score += 10

    if bucket == "venue_pending":
        score -= 6
    if bucket == "venue_conflict":
        score -= 12
    if budget_state == "budget_risk_high":
        score -= 10
    if bucket == "suppressed":
        score = 0
    score += min(8, open_loop_count * 2)
    return max(0, min(130, score))


def risk_flags(
    venue_status: str,
    budget_state: str,
    payment_state: str,
    decision_state: str,
    waiting_on_us: bool,
    open_loop_count: int,
) -> str:
    flags: List[str] = []
    if venue_status in {"venue_pending", "venue_conflict"}:
        flags.append(venue_status)
    if budget_state == "budget_risk_high":
        flags.append("budget_risk")
    if payment_state == "no_payment_signal":
        flags.append("no_payment_signal")
    if decision_state in {"partner_or_family_review", "vendor_comparison", "legal_review", "external_party_review"}:
        flags.append(decision_state)
    if waiting_on_us:
        flags.append("waiting_on_us")
    if open_loop_count:
        flags.append(f"open_loops_{open_loop_count}")
    return " | ".join(flags)


def format_queue_markdown(title: str, rows: Sequence[Dict[str, Any]], count_label: str) -> str:
    lines = [f"# {title}", "", f"- Total {count_label}: `{len(rows)}`", "", "## Top Rows"]
    for row in rows[:100]:
        lines.append(
            f"- `{row.get('readiness_score')}` | {row.get('lead_name')} | {row.get('stage_label')} | "
            f"{row.get('readiness_bucket')} | {row.get('operator_move')}"
        )
    lines.append("")
    return "\n".join(lines)


def build_readme(
    all_rows: Sequence[Dict[str, Any]],
    deposit_rows: Sequence[Dict[str, Any]],
    venue_rows: Sequence[Dict[str, Any]],
    quote_rows: Sequence[Dict[str, Any]],
    decision_rows: Sequence[Dict[str, Any]],
) -> str:
    bucket_counts = Counter(row.get("readiness_bucket") or "" for row in all_rows)
    lines = [
        "# Deal Intelligence",
        "",
        "This layer compresses each lead into an operator-facing deal sheet: commercial state, service mix, venue/payment readiness, and the next move.",
        "",
        "## Snapshot",
        f"- Lead deal sheets: `{len(all_rows)}`",
        f"- Deposit / contract queue: `{len(deposit_rows)}`",
        f"- Venue pending queue: `{len(venue_rows)}`",
        f"- Quote risk queue: `{len(quote_rows)}`",
        f"- Decision pending queue: `{len(decision_rows)}`",
        "",
        "## Readiness Buckets",
    ]
    for label, count in bucket_counts.items():
        lines.append(f"- {label}: {count}")
    lines.extend(
        [
            "",
            "## Key Files",
            "- `operator_action_board.md`: top active leads ordered by commercial readiness",
            "- `deposit_ready_queue.md`: leads closest to signing / paying",
            "- `venue_pending_queue.md`: leads blocked on venue status or venue rules",
            "- `quote_risk_queue.md`: active quote-stage leads with budget or scope pressure",
            "- `decision_pending_queue.md`: leads waiting on spouse / family / legal / other parties",
            "- `../normalized/lead_deal_sheets.csv`: machine-friendly one-row-per-lead deal sheet index",
            "",
        ]
    )
    return "\n".join(lines)


def build_lead_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Deal Sheet: {payload['lead_name']}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Pipeline / Stage: {payload.get('pipeline_name') or ''} / {payload.get('stage_label') or ''}",
        f"- Stage Type: {payload.get('stage_type') or ''}",
        f"- Current Value: {payload.get('value_formatted') or ''}",
        f"- Confidence: {payload.get('confidence') or ''}",
        f"- Event Date (UTC): `{payload.get('event_datetime_utc') or ''}`",
        f"- Guest Count: {payload.get('guest_count_text') or ''}",
        f"- Event Type: {payload.get('event_type') or ''}",
        f"- Venue Status: {payload.get('venue_status') or ''}",
        f"- Readiness Bucket: {payload.get('readiness_bucket') or ''}",
        f"- Readiness Score: `{payload.get('readiness_score') or ''}`",
        "",
        "## Commercial Signals",
        f"- Service Style: {payload.get('service_style_signals') or ''}",
        f"- Bar Plan: {payload.get('bar_signals') or ''}",
        f"- Staffing Plan: {payload.get('staffing_signals') or ''}",
        f"- Budget State: {payload.get('budget_state') or ''}",
        f"- Payment State: {payload.get('payment_state') or ''}",
        f"- Decision State: {payload.get('decision_state') or ''}",
        "",
        "## Operator Move",
        f"- Next Move: {payload.get('operator_move') or ''}",
        f"- Risk Flags: {payload.get('risk_flags') or ''}",
        f"- Top Open Loop: {payload.get('top_open_loop_text') or ''}",
        "",
        "## Evidence",
        f"- Service Evidence: {payload.get('service_style_evidence') or ''}",
        f"- Bar Evidence: {payload.get('bar_evidence') or ''}",
        f"- Staffing Evidence: {payload.get('staffing_evidence') or ''}",
        f"- Venue Evidence: {payload.get('venue_evidence') or ''}",
        f"- Budget Evidence: {payload.get('budget_evidence') or ''}",
        f"- Payment Evidence: {payload.get('payment_evidence') or ''}",
        f"- Decision Evidence: {payload.get('decision_evidence') or ''}",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    output_dir = args.output_dir
    by_owner_dir = output_dir / "by_owner"
    ensure_dir(output_dir)
    ensure_dir(by_owner_dir)

    brief_rows = load_csv_rows(args.lead_briefs_csv)
    event_rows = {row["lead_id"]: row for row in load_csv_rows(args.event_facts_csv)}
    follow_up_rows = {row["lead_id"]: row for row in load_csv_rows(args.follow_up_csv)}
    convo_rows = {row["lead_id"]: row for row in load_csv_rows(args.conversation_csv)}
    open_loops_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in load_csv_rows(args.open_loops_csv):
        open_loops_by_lead[row["lead_id"]].append(row)

    opps_by_lead: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in load_csv_rows(args.opportunities_csv):
        opps_by_lead[row["lead_id"]].append(row)

    all_rows: List[Dict[str, Any]] = []

    for brief in brief_rows:
        lead_id = brief["lead_id"]
        lead_dir = Path(brief["brief_path"]).parent
        event_facts = event_rows.get(lead_id, {})
        follow_up = follow_up_rows.get(lead_id, {})
        convo_summary = convo_rows.get(lead_id, {})
        current_opp = choose_current_opportunity(opps_by_lead.get(lead_id, []))

        convo_json_path = lead_dir / "lead_conversation_intelligence.json"
        conversation_payload = load_json(convo_json_path) if convo_json_path.exists() else {}
        text_rows = build_text_rows(conversation_payload, event_facts, current_opp)

        signal_text_rows = [row for row in text_rows if row.get("section") != "current_opportunity"]

        service_labels, service_evidence_map = collect_labels(text_rows, SERVICE_STYLE_RULES)
        bar_labels, bar_evidence_map = collect_labels(text_rows, BAR_RULES)
        staffing_labels, staffing_evidence_map = collect_labels(text_rows, STAFFING_RULES)

        service_style_signals = " | ".join(service_labels[:3])
        bar_signals = " | ".join(bar_labels[:3])
        staffing_signals = " | ".join(staffing_labels[:3])

        venue_status, venue_evidence = detect_venue_status(event_facts, text_rows)
        budget_state, budget_evidence = detect_budget_state(signal_text_rows)
        payment_state, payment_evidence = detect_payment_state(
            event_facts.get("stage_label") or brief.get("current_opportunity_status_label") or "",
            signal_text_rows,
        )
        decision_state, decision_evidence = detect_decision_state(signal_text_rows)

        stage_label = event_facts.get("stage_label") or brief.get("current_opportunity_status_label") or brief.get("lead_status_label") or ""
        stage_type = event_facts.get("stage_type") or ((current_opp or {}).get("status_type") or "")
        queue_bucket = follow_up.get("queue_bucket") or ""
        open_loops = sorted(open_loops_by_lead.get(lead_id, []), key=lambda row: row.get("event_datetime_utc") or "", reverse=True)
        days_until_event = None
        if event_facts.get("days_until_event") not in ("", None):
            try:
                days_until_event = int(event_facts["days_until_event"])
            except ValueError:
                days_until_event = None

        bucket = readiness_bucket(
            stage_label=stage_label,
            stage_type=stage_type,
            queue_bucket=queue_bucket,
            venue_status=venue_status,
            budget_state=budget_state,
            payment_state=payment_state,
            decision_state=decision_state,
            open_loop_count=len(open_loops),
            days_until_event=days_until_event,
        )

        base_priority = int((follow_up.get("priority_score") or "0") or 0)
        readiness_score = compute_readiness_score(
            base_score=base_priority,
            bucket=bucket,
            venue_status=venue_status,
            budget_state=budget_state,
            payment_state=payment_state,
            open_loop_count=len(open_loops),
        )

        payload = {
            "lead_id": lead_id,
            "lead_name": brief["lead_name"],
            "lead_owner_name": brief.get("lead_owner_name") or "",
            "pipeline_name": event_facts.get("pipeline_name") or ((current_opp or {}).get("pipeline_name") or ""),
            "stage_label": stage_label,
            "stage_type": stage_type,
            "current_opportunity_title": (current_opp or {}).get("opportunity_title") or "",
            "value_formatted": (current_opp or {}).get("value_formatted") or "",
            "confidence": (current_opp or {}).get("confidence") or "",
            "event_datetime_utc": event_facts.get("event_datetime_utc") or "",
            "event_type": event_facts.get("event_type") or "",
            "guest_count_text": event_facts.get("guest_count_text") or "",
            "venue_name": event_facts.get("venue_name") or "",
            "venue_city": event_facts.get("venue_city") or "",
            "venue_status": venue_status,
            "venue_evidence": venue_evidence,
            "service_style_signals": service_style_signals,
            "service_style_evidence": next(iter(service_evidence_map.values()), ""),
            "bar_signals": bar_signals,
            "bar_evidence": next(iter(bar_evidence_map.values()), ""),
            "staffing_signals": staffing_signals,
            "staffing_evidence": next(iter(staffing_evidence_map.values()), ""),
            "budget_state": budget_state,
            "budget_evidence": budget_evidence,
            "payment_state": payment_state,
            "payment_evidence": payment_evidence,
            "decision_state": decision_state,
            "decision_evidence": decision_evidence,
            "follow_up_priority_score": follow_up.get("priority_score") or "",
            "follow_up_priority_band": follow_up.get("priority_band") or "",
            "follow_up_queue_bucket": queue_bucket,
            "waiting_on_us": event_facts.get("waiting_on_us") or "",
            "open_loop_count": len(open_loops),
            "top_open_loop_text": compact_text((open_loops[0] if open_loops else {}).get("text"), 500),
            "top_open_loop_category": (open_loops[0] if open_loops else {}).get("category") or "",
            "readiness_bucket": bucket,
            "readiness_score": readiness_score,
            "operator_move": recommended_move(bucket),
            "risk_flags": risk_flags(
                venue_status=venue_status,
                budget_state=budget_state,
                payment_state=payment_state,
                decision_state=decision_state,
                waiting_on_us=str(event_facts.get("waiting_on_us") or "").lower() == "true",
                open_loop_count=len(open_loops),
            ),
            "deal_sheet_path": str(lead_dir / "lead_deal_sheet.md"),
            "deal_sheet_json_path": str(lead_dir / "lead_deal_sheet.json"),
            "brief_path": brief.get("brief_path") or "",
            "conversation_path": convo_summary.get("conversation_intelligence_path") or str(lead_dir / "lead_conversation_intelligence.md"),
            "event_facts_path": str(lead_dir / "lead_event_facts.md"),
        }

        write_json(lead_dir / "lead_deal_sheet.json", payload)
        (lead_dir / "lead_deal_sheet.md").write_text(build_lead_markdown(payload), encoding="utf-8")
        all_rows.append(payload)

    all_rows.sort(key=lambda row: (row.get("readiness_score") or 0, row.get("follow_up_priority_score") or ""), reverse=True)

    operator_rows = [row for row in all_rows if row.get("readiness_bucket") != "suppressed"]
    deposit_rows = [row for row in operator_rows if row.get("payment_state") in {"contract_or_deposit_active", "deposit_stage"} or row.get("readiness_bucket") == "contracting_or_deposit"]
    venue_rows = [row for row in operator_rows if row.get("venue_status") in {"venue_pending", "venue_conflict"}]
    quote_rows = [row for row in operator_rows if row.get("readiness_bucket") in {"quote_outstanding", "budget_risk"}]
    decision_rows = [row for row in operator_rows if row.get("decision_state") in {"partner_or_family_review", "vendor_comparison", "legal_review", "external_party_review"}]
    booked_rows = [row for row in operator_rows if row.get("readiness_bucket") == "booked_fulfillment_watch"]

    write_csv(normalized_dir / "lead_deal_sheets.csv", all_rows)
    write_jsonl(normalized_dir / "lead_deal_sheets.jsonl", all_rows)
    write_csv(normalized_dir / "operator_action_board.csv", operator_rows)
    write_jsonl(normalized_dir / "operator_action_board.jsonl", operator_rows)
    write_csv(normalized_dir / "deposit_ready_queue.csv", deposit_rows)
    write_jsonl(normalized_dir / "deposit_ready_queue.jsonl", deposit_rows)
    write_csv(normalized_dir / "venue_pending_queue.csv", venue_rows)
    write_jsonl(normalized_dir / "venue_pending_queue.jsonl", venue_rows)
    write_csv(normalized_dir / "quote_risk_queue.csv", quote_rows)
    write_jsonl(normalized_dir / "quote_risk_queue.jsonl", quote_rows)
    write_csv(normalized_dir / "decision_pending_queue.csv", decision_rows)
    write_jsonl(normalized_dir / "decision_pending_queue.jsonl", decision_rows)
    write_csv(normalized_dir / "booked_fulfillment_watch.csv", booked_rows)
    write_jsonl(normalized_dir / "booked_fulfillment_watch.jsonl", booked_rows)

    (output_dir / "README.md").write_text(
        build_readme(all_rows, deposit_rows, venue_rows, quote_rows, decision_rows),
        encoding="utf-8",
    )
    (output_dir / "operator_action_board.md").write_text(
        format_queue_markdown("Operator Action Board", operator_rows, "active deal sheets"),
        encoding="utf-8",
    )
    (output_dir / "deposit_ready_queue.md").write_text(
        format_queue_markdown("Deposit Ready Queue", deposit_rows, "deposit-ready leads"),
        encoding="utf-8",
    )
    (output_dir / "venue_pending_queue.md").write_text(
        format_queue_markdown("Venue Pending Queue", venue_rows, "venue-pending leads"),
        encoding="utf-8",
    )
    (output_dir / "quote_risk_queue.md").write_text(
        format_queue_markdown("Quote Risk Queue", quote_rows, "quote-risk leads"),
        encoding="utf-8",
    )
    (output_dir / "decision_pending_queue.md").write_text(
        format_queue_markdown("Decision Pending Queue", decision_rows, "decision-pending leads"),
        encoding="utf-8",
    )

    by_owner_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in operator_rows:
        by_owner_rows[slugify(row.get("lead_owner_name") or "unknown")].append(row)

    for owner_slug, rows in by_owner_rows.items():
        owner_dir = by_owner_dir / owner_slug
        ensure_dir(owner_dir)
        write_csv(owner_dir / "operator_action_board.csv", rows)
        write_jsonl(owner_dir / "operator_action_board.jsonl", rows)

    print(
        json.dumps(
            {
                "lead_rows": len(all_rows),
                "operator_rows": len(operator_rows),
                "deposit_ready_rows": len(deposit_rows),
                "venue_pending_rows": len(venue_rows),
                "quote_risk_rows": len(quote_rows),
                "decision_pending_rows": len(decision_rows),
                "booked_watch_rows": len(booked_rows),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
