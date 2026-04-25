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
DEFAULT_DEAL_SHEETS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_deal_sheets.csv"
DEFAULT_EVENT_FACTS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_event_facts.csv"
DEFAULT_CONVERSATION_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_conversation_intelligence.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "event_ops_registry"

NOISE_PATTERNS = (
    "privacy policy",
    "facebook",
    "instagram",
    "website",
    "direct line",
    "catering main",
    "copyright",
    "the knot worldwide",
    "when you've finished recording",
    "your call has been forwarded",
    "person you're trying to reach is not available",
)

SERVICE_STYLE_RULES: List[Tuple[str, Sequence[str]]] = [
    ("drop_off_only", (r"\bdrop[- ]off only\b", r"\bsetup only\b")),
    ("family_style", (r"\bfamily[- ]style\b",)),
    ("churrasco", (r"\bchurrasco\b", r"\bgrill(?:ing)?\b", r"\bon-site grilling\b", r"\brodizio\b")),
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
    ("bartender", (r"\bbartender\b", r"\bbartending\b")),
]

STAFFING_RULES: List[Tuple[str, Sequence[str]]] = [
    ("drop_off_only", (r"\bdrop[- ]off only\b", r"\bsetup only\b")),
    ("staffed_service", (r"\bstaff(?:ing)?\b", r"\bservers?\b", r"\bbuffet attendants?\b", r"\bfull-service\b", r"\bon-site staff\b")),
    ("on_site_grilling", (r"\bon-site grilling\b", r"\bbring and operate their own grill\b", r"\bgrill on site\b")),
]

OPS_REQUIREMENT_RULES: List[Tuple[str, Sequence[str]]] = [
    ("certificate_of_insurance", (r"\bcertificate of insurance\b", r"\bcoi\b")),
    ("prep_space_or_kitchen_access", (r"\bprep space\b", r"\bprep room\b", r"\bkitchen\b")),
    ("load_in_out", (r"\bload[- ]in\b", r"\bload[- ]out\b", r"\bbuilding access\b", r"\barrival time\b", r"\bsetup\b", r"\btear[- ]down\b")),
    ("cleanup_or_waste", (r"\bwaste removal\b", r"\bclean[- ]?up\b", r"\bcleanup\b")),
    ("travel_fee_or_distance", (r"\btravel fee\b", r"\bdistance\b", r"\bhow far\b")),
    ("venue_rules_or_outside_catering", (r"\boutside catering\b", r"\bvenue allows\b", r"\bvenue rules\b", r"\bcatering rules\b")),
    ("bartender_or_bar_setup", (r"\bbartender\b", r"\bopen bar\b", r"\bcash bar\b", r"\bmobile bar\b", r"\bbeer and wine\b")),
    ("on_site_grilling", (r"\bchurrasco\b", r"\bgrill(?:ing)?\b", r"\brodizio\b")),
    ("drop_off_service", (r"\bdrop[- ]off\b", r"\bsetup only\b")),
    ("kids_menu_or_split_pricing", (r"\bkids menu\b", r"\bkids and adults\b", r"\bchildren\b", r"\bprice change .*kids\b")),
    ("venue_search_or_decision", (r"\blooking for a venue\b", r"\blook for two venues\b", r"\bcountry club\b", r"\bbarn\b", r"\bvenue picked out\b")),
]

EVENT_TYPE_RULES: List[Tuple[str, Sequence[str]]] = [
    ("wedding", (r"\bwedding\b", r"\bcasamento\b", r"\breception\b")),
    ("birthday", (r"\bbirthday\b", r"\b50th birthday\b", r"\bgraduation party\b")),
    ("graduation", (r"\bgraduation\b",)),
    ("baby_shower", (r"\bbaby shower\b",)),
    ("bridal_shower", (r"\bbridal shower\b",)),
    ("anniversary", (r"\banniversary\b",)),
    ("corporate", (r"\bbusiness\b", r"\bcorporate\b", r"\bbreakfast\b", r"\blunch\b")),
    ("quinceanera", (r"\bquincea(?:ñ|n)era\b",)),
    ("party", (r"\bparty\b",)),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a structured event-and-ops registry from normalized Comeketo lead layers.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--deal-sheets-csv", type=Path, default=DEFAULT_DEAL_SHEETS_CSV)
    parser.add_argument("--event-facts-csv", type=Path, default=DEFAULT_EVENT_FACTS_CSV)
    parser.add_argument("--conversation-csv", type=Path, default=DEFAULT_CONVERSATION_CSV)
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
    if not path.exists():
        return {}
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


def compact_text(value: Optional[str], limit: int = 260) -> str:
    text = " ".join((value or "").split())
    return text[:limit]


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return text or "unknown"


def dedupe_key(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def split_labels(value: Optional[str]) -> List[str]:
    labels: List[str] = []
    for part in (value or "").split("|"):
        label = part.strip()
        if label and label not in labels:
            labels.append(label)
    return labels


def is_noise_text(text: str) -> bool:
    lowered = text.lower().strip()
    if not lowered:
        return True
    return any(pattern in lowered for pattern in NOISE_PATTERNS)


def normalize_event_type(raw_value: str) -> str:
    text = (raw_value or "").strip()
    if not text:
        return "unknown"
    for label, patterns in EVENT_TYPE_RULES:
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
            return label
    lowered = text.lower()
    return re.sub(r"[^a-z0-9]+", "_", lowered).strip("_") or "unknown"


def parse_int(value: Optional[str]) -> int:
    try:
        return int(value or 0)
    except ValueError:
        return 0


def derive_guest_band(guest_min: int, guest_max: int, guest_text: str) -> str:
    maximum = guest_max or guest_min
    if not maximum:
        numbers = [int(match) for match in re.findall(r"\d+", guest_text or "")]
        if numbers:
            maximum = max(numbers)
            guest_min = min(numbers)
    if not maximum:
        return "unknown"
    if maximum >= 150:
        return "150+"
    if maximum >= 100:
        return "100-149"
    if maximum >= 75:
        return "75-99"
    if maximum >= 50:
        return "50-74"
    if maximum >= 25:
        return "25-49"
    return "1-24"


def choose_primary(labels: Sequence[str], fallback: str = "unknown") -> str:
    return labels[0] if labels else fallback


def merge_labels(existing: Sequence[str], detected: Sequence[str]) -> List[str]:
    merged: List[str] = []
    for label in list(existing) + list(detected):
        if label and label not in merged:
            merged.append(label)
    return merged


def sort_items_by_time(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(items, key=lambda item: item.get("event_datetime_utc") or "", reverse=True)


def build_text_rows(conversation_payload: Dict[str, Any], deal_row: Dict[str, str], event_row: Dict[str, str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()

    for section in ("buyer_asks", "blockers", "preferences", "sales_commitments", "open_loops"):
        for item in sort_items_by_time(conversation_payload.get(section, [])):
            text = compact_text(item.get("text"), 900)
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

    extras = [
        ("deal_service", "", deal_row.get("service_style_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("deal_bar", "", deal_row.get("bar_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("deal_staffing", "", deal_row.get("staffing_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("deal_venue", "", deal_row.get("venue_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("deal_budget", "", deal_row.get("budget_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("latest_signal", "", event_row.get("latest_buyer_signal") or "", event_row.get("latest_observed_activity_utc") or "", event_row.get("brief_path") or ""),
    ]

    for section, category, text, dt_value, source_path in extras:
        normalized = compact_text(text, 900)
        if not normalized or is_noise_text(normalized):
            continue
        key = (section, category, dedupe_key(normalized))
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "section": section,
                "category": category,
                "text": normalized,
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
                evidence[label] = compact_text(text, 500)
                labels.append(label)
    return labels, evidence


def first_matching_text(text_rows: Sequence[Dict[str, Any]], patterns: Sequence[str]) -> str:
    for row in text_rows:
        text = row.get("text") or ""
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
            return compact_text(text, 500)
    return ""


def infer_signals(existing_value: str, existing_evidence: str, text_rows: Sequence[Dict[str, Any]], rules: Sequence[Tuple[str, Sequence[str]]]) -> Tuple[List[str], str]:
    existing_labels = split_labels(existing_value)
    detected_labels, detected_evidence = collect_labels(text_rows, rules)
    labels = merge_labels(existing_labels, detected_labels)

    if existing_evidence:
        evidence = compact_text(existing_evidence, 500)
    else:
        evidence = ""
        for label in labels:
            if label in detected_evidence:
                evidence = detected_evidence[label]
                break
    return labels, evidence


def event_month_label(event_dt: Optional[datetime]) -> str:
    if not event_dt:
        return "unknown"
    return event_dt.strftime("%Y-%m")


def planning_horizon(event_dt: Optional[datetime], now_utc: datetime) -> Tuple[str, str]:
    if not event_dt:
        return "unknown", ""
    days = (event_dt.date() - now_utc.date()).days
    if days < 0:
        return "past", str(days)
    if days == 0:
        return "today", "0"
    if days <= 7:
        return "this_week", str(days)
    if days <= 14:
        return "two_weeks", str(days)
    if days <= 30:
        return "this_month", str(days)
    if days <= 60:
        return "sixty_days", str(days)
    if days <= 120:
        return "one_twenty_days", str(days)
    return "future", str(days)


def venue_summary(event_row: Dict[str, str], deal_row: Dict[str, str]) -> str:
    parts = [
        event_row.get("venue_name") or deal_row.get("venue_name") or "",
        event_row.get("venue_city") or deal_row.get("venue_city") or "",
        event_row.get("venue_address") or "",
    ]
    summary = " | ".join(part for part in parts if part)
    if summary:
        return compact_text(summary, 200)
    if deal_row.get("venue_evidence"):
        return compact_text(deal_row.get("venue_evidence"), 200)
    if event_row.get("venue_type"):
        return compact_text(event_row.get("venue_type"), 200)
    return ""


def venue_bucket(venue_status: str) -> str:
    mapping = {
        "venue_named": "secured_or_named",
        "private_home": "private_home",
        "area_known_only": "area_only",
        "venue_type_captured": "venue_type_only",
        "venue_pending": "searching_or_pending",
        "venue_conflict": "conflict_or_restriction",
        "unknown": "unknown",
    }
    return mapping.get(venue_status or "unknown", "unknown")


def ops_scope(stage_type: str, readiness_bucket: str, payment_state: str) -> str:
    if stage_type == "lost" or readiness_bucket == "suppressed":
        return "historical_or_suppressed"
    if stage_type == "won" or readiness_bucket in {"booked_fulfillment_watch", "won_pipeline_watch", "contracting_or_deposit"} or payment_state in {"deposit_stage", "contract_or_deposit_active"}:
        return "execution_watch"
    return "active_sales_watch"


def execution_watch_flag(scope: str) -> bool:
    return scope == "execution_watch"


def venue_gap_flag(stage_type: str, readiness_bucket: str, venue_status: str) -> bool:
    if stage_type == "lost" or readiness_bucket == "suppressed":
        return False
    return venue_status in {"unknown", "area_known_only", "venue_pending", "venue_conflict"}


def requirement_flags(text_rows: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    _, evidence = collect_labels(text_rows, OPS_REQUIREMENT_RULES)
    return evidence


def ops_complexity_score(
    event_type_norm: str,
    guest_max: int,
    service_labels: Sequence[str],
    bar_labels: Sequence[str],
    staffing_labels: Sequence[str],
    venue_status: str,
    requirement_map: Dict[str, str],
    scope: str,
    days_until_event: Optional[int],
) -> int:
    score = 0

    if event_type_norm == "wedding":
        score += 6
    elif event_type_norm in {"quinceanera", "corporate", "anniversary"}:
        score += 3

    if guest_max >= 150:
        score += 7
    elif guest_max >= 100:
        score += 5
    elif guest_max >= 75:
        score += 4
    elif guest_max >= 50:
        score += 3
    elif guest_max >= 25:
        score += 1

    if "plated_service" in service_labels:
        score += 4
    if "churrasco" in service_labels:
        score += 5
    if "family_style" in service_labels:
        score += 2
    if "buffet_service" in service_labels:
        score += 1

    if any(label for label in bar_labels if label not in {"no_bar_service", ""}):
        score += 3
    if "bartender" in bar_labels:
        score += 1

    if "staffed_service" in staffing_labels:
        score += 3
    if "on_site_grilling" in staffing_labels:
        score += 5

    if venue_status == "private_home":
        score += 3
    elif venue_status == "venue_pending":
        score += 4
    elif venue_status == "venue_conflict":
        score += 6
    elif venue_status == "area_known_only":
        score += 2

    score += min(4, len(requirement_map))

    if scope == "execution_watch" and days_until_event is not None and days_until_event <= 30:
        score += 4

    return max(0, min(30, score))


def ops_complexity_band(score: int) -> str:
    if score >= 18:
        return "very_high"
    if score >= 12:
        return "high"
    if score >= 7:
        return "medium"
    return "low"


def high_touch_flag(
    complexity_score: int,
    guest_max: int,
    service_labels: Sequence[str],
    bar_labels: Sequence[str],
    staffing_labels: Sequence[str],
    venue_status: str,
    requirement_map: Dict[str, str],
) -> bool:
    if complexity_score >= 12:
        return True
    if guest_max >= 100:
        return True
    if any(label in service_labels for label in ("plated_service", "churrasco", "family_style")):
        return True
    if any(label for label in bar_labels if label not in {"", "no_bar_service"}):
        return True
    if any(label in staffing_labels for label in ("staffed_service", "on_site_grilling")):
        return True
    if venue_status == "private_home":
        return True
    return any(flag in requirement_map for flag in ("certificate_of_insurance", "load_in_out", "prep_space_or_kitchen_access"))


def ops_watch_reason(
    stage_type: str,
    scope: str,
    venue_gap: bool,
    complexity_band_value: str,
    payment_state: str,
    days_until_event: Optional[int],
    requirement_map: Dict[str, str],
    event_datetime_source: str,
) -> str:
    if stage_type == "lost":
        return "Historical / lost lead; keep for memory, not active ops."
    if days_until_event is not None and days_until_event < 0:
        if event_datetime_source == "close_date_won_fallback":
            return "Lead has a stale fallback CRM event date; treat the date as uncertain until confirmed."
        return "Past-dated event still attached to an active/execution record."
    if scope == "execution_watch" and days_until_event is not None and days_until_event <= 14:
        return "Execution-watch lead inside 14 days."
    if scope == "execution_watch" and days_until_event is not None and days_until_event <= 45:
        return "Execution-watch lead inside 45 days."
    if venue_gap:
        return "Venue details are still incomplete or unstable."
    if any(flag in requirement_map for flag in ("certificate_of_insurance", "load_in_out", "prep_space_or_kitchen_access", "cleanup_or_waste")):
        return "Venue / logistics requirements are already surfacing."
    if complexity_band_value in {"high", "very_high"}:
        return "Service mix and event shape suggest a high-touch execution."
    if payment_state in {"deposit_stage", "contract_or_deposit_active"}:
        return "Commercially close enough to carry ops assumptions forward."
    return "Standard operational watch."


def summary_line(
    lead_name: str,
    event_type_norm: str,
    guest_band: str,
    event_dt: Optional[datetime],
    venue_summary_value: str,
    service_labels: Sequence[str],
    bar_labels: Sequence[str],
    staffing_labels: Sequence[str],
    complexity_band_value: str,
    watch_reason: str,
) -> str:
    date_text = event_dt.strftime("%Y-%m-%d") if event_dt else "date unknown"
    service_text = " / ".join(service_labels) if service_labels else "service unknown"
    bar_text = " / ".join(bar_labels) if bar_labels else "unknown"
    staffing_text = " / ".join(staffing_labels) if staffing_labels else "unknown"
    venue_text = venue_summary_value or "venue unknown"
    return compact_text(
        f"{lead_name}: {event_type_norm} | {guest_band} guests | {date_text} | venue {venue_text} | "
        f"service {service_text} | bar {bar_text} | staffing {staffing_text} | complexity {complexity_band_value}. "
        f"{watch_reason}",
        420,
    )


def build_lead_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Event / Ops Sheet: {payload.get('lead_name') or ''}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Pipeline / Stage: {payload.get('pipeline_name') or ''} / {payload.get('stage_label') or ''}",
        f"- Ops Scope: {payload.get('ops_scope') or ''}",
        f"- Event Date (UTC): `{payload.get('event_datetime_utc') or ''}`",
        f"- Event Date Source: {payload.get('event_datetime_source') or ''}",
        f"- Planning Horizon: {payload.get('planning_horizon') or ''}",
        f"- Event Type: {payload.get('event_type_normalized') or ''}",
        f"- Guest Count: {payload.get('guest_count_text') or ''}",
        f"- Guest Band: {payload.get('guest_count_band') or ''}",
        f"- Venue: {payload.get('venue_summary') or ''}",
        f"- Venue Status: {payload.get('venue_status') or ''}",
        f"- Complexity: {payload.get('ops_complexity_band') or ''} (`{payload.get('ops_complexity_score') or ''}`)",
        "",
        "## Service Plan",
        f"- Service Style: {payload.get('service_style_signals') or ''}",
        f"- Bar Plan: {payload.get('bar_signals') or ''}",
        f"- Staffing Plan: {payload.get('staffing_signals') or ''}",
        f"- Payment State: {payload.get('payment_state') or ''}",
        f"- Decision State: {payload.get('decision_state') or ''}",
        "",
        "## Operational Watch",
        f"- Watch Reason: {payload.get('ops_watch_reason') or ''}",
        f"- Venue Gap Flag: {payload.get('venue_gap_flag')}",
        f"- Execution Watch Flag: {payload.get('execution_watch_flag')}",
        f"- High-Touch Flag: {payload.get('high_touch_flag')}",
        f"- Summary: {payload.get('ops_summary') or ''}",
        "",
        "## Requirement Flags",
    ]

    requirement_flags_value = split_labels(payload.get("ops_requirement_flags"))
    if requirement_flags_value:
        for label in requirement_flags_value:
            evidence = payload.get("ops_requirement_evidence_map", {}).get(label, "")
            lines.append(f"- {label}: {evidence}")
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "## Evidence",
            f"- Service Evidence: {payload.get('service_style_evidence') or ''}",
            f"- Bar Evidence: {payload.get('bar_evidence') or ''}",
            f"- Staffing Evidence: {payload.get('staffing_evidence') or ''}",
            f"- Venue Evidence: {payload.get('venue_evidence') or ''}",
            "",
        ]
    )
    return "\n".join(lines)


def sort_event_key(row: Dict[str, Any]) -> Tuple[int, datetime, int]:
    dt = parse_iso(row.get("event_datetime_utc"))
    fallback_dt = datetime.max.replace(tzinfo=timezone.utc)
    return (
        0 if dt else 1,
        dt or fallback_dt,
        -int(row.get("ops_complexity_score") or 0),
    )


def sort_execution_key(row: Dict[str, Any]) -> Tuple[int, datetime, int]:
    dt = parse_iso(row.get("event_datetime_utc"))
    fallback_dt = datetime.max.replace(tzinfo=timezone.utc)
    horizon = row.get("planning_horizon") or ""
    source = row.get("event_datetime_source") or ""

    if horizon in {"today", "this_week", "two_weeks", "this_month", "sixty_days", "one_twenty_days", "future"}:
        bucket = 0
    elif horizon == "unknown":
        bucket = 2
    elif source == "close_date_won_fallback":
        bucket = 3
    else:
        bucket = 4

    return (
        bucket,
        dt or fallback_dt,
        -int(row.get("ops_complexity_score") or 0),
    )


def sort_watch_key(row: Dict[str, Any]) -> Tuple[int, int, datetime]:
    dt = parse_iso(row.get("event_datetime_utc"))
    fallback_dt = datetime.max.replace(tzinfo=timezone.utc)
    stage_rank = {"won": 0, "active": 1, "lead_only": 2, "lost": 3}
    return (
        stage_rank.get((row.get("stage_type") or "").lower(), 9),
        0 if dt else 1,
        dt or fallback_dt,
    )


def build_board_markdown(title: str, rows: Sequence[Dict[str, Any]], count_label: str) -> str:
    lines = [f"# {title}", "", f"- Total {count_label}: `{len(rows)}`", "", "## Top Rows"]
    for row in rows[:120]:
        lines.append(
            f"- `{row.get('event_datetime_utc') or 'unknown'}` | {row.get('lead_name')} | {row.get('lead_owner_name')} | "
            f"{row.get('ops_complexity_band')} | {row.get('venue_status')} | {row.get('ops_watch_reason')}"
        )
    lines.append("")
    return "\n".join(lines)


def build_calendar_markdown(rows: Sequence[Dict[str, Any]]) -> str:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row.get("event_month") or "unknown"].append(row)

    lines = [
        "# Future Event Calendar",
        "",
        f"- Future active / execution events with known dates: `{len(rows)}`",
        "",
    ]

    for month in sorted(grouped.keys()):
        month_rows = sorted(grouped[month], key=sort_event_key)
        lines.append(f"## {month}")
        for row in month_rows[:80]:
            lines.append(
                f"- `{row.get('event_datetime_utc')}` | {row.get('lead_name')} | {row.get('stage_label')} | "
                f"{row.get('guest_count_band')} | {row.get('service_style_primary')} | {row.get('venue_status')}"
            )
        lines.append("")
    return "\n".join(lines)


def build_service_mix_markdown(rows: Sequence[Dict[str, Any]]) -> str:
    service_counts = Counter(row.get("service_style_primary") or "unknown" for row in rows)
    bar_counts = Counter(row.get("bar_primary") or "unknown" for row in rows)
    staffing_counts = Counter(row.get("staffing_primary") or "unknown" for row in rows)
    lines = [
        "# Service Mix Rollup",
        "",
        f"- Active / execution rows included: `{len(rows)}`",
        "",
        "## Service Style",
    ]
    for label, count in service_counts.most_common():
        lines.append(f"- {label}: {count}")
    lines.extend(["", "## Bar"])
    for label, count in bar_counts.most_common():
        lines.append(f"- {label}: {count}")
    lines.extend(["", "## Staffing"])
    for label, count in staffing_counts.most_common():
        lines.append(f"- {label}: {count}")
    lines.append("")
    return "\n".join(lines)


def build_readme(
    all_rows: Sequence[Dict[str, Any]],
    execution_rows: Sequence[Dict[str, Any]],
    venue_gap_rows: Sequence[Dict[str, Any]],
    high_touch_rows: Sequence[Dict[str, Any]],
    future_rows: Sequence[Dict[str, Any]],
    month_rows: Sequence[Dict[str, Any]],
) -> str:
    horizon_counts = Counter(row.get("planning_horizon") or "" for row in future_rows)
    complexity_counts = Counter(row.get("ops_complexity_band") or "" for row in all_rows)
    lines = [
        "# Event Ops Registry",
        "",
        "This layer compresses each lead into an event-and-execution record: event shape, venue state, service mix, requirement flags, and which leads already deserve operational attention.",
        "",
        "## Snapshot",
        f"- Lead event / ops sheets: `{len(all_rows)}`",
        f"- Execution-watch rows: `{len(execution_rows)}`",
        f"- Venue-gap rows: `{len(venue_gap_rows)}`",
        f"- High-touch rows: `{len(high_touch_rows)}`",
        f"- Future dated active / execution rows: `{len(future_rows)}`",
        f"- Event-month folders: `{len(month_rows)}`",
        "",
        "## Complexity Bands",
    ]
    for label, count in complexity_counts.items():
        lines.append(f"- {label}: {count}")
    lines.extend(["", "## Planning Horizon"])
    for label, count in horizon_counts.items():
        lines.append(f"- {label}: {count}")
    lines.extend(
        [
            "",
            "## Key Files",
            "- `execution_watch_board.md`: execution-adjacent leads, including won and deposit/contract-stage work",
            "- `venue_gap_board.md`: active or execution-watch leads with incomplete venue clarity",
            "- `high_touch_ops_board.md`: leads that look operationally complex even before fulfillment",
            "- `future_event_calendar.md`: future active / execution events grouped by month",
            "- `service_mix_rollup.md`: quick distribution of service, bar, and staffing patterns",
            "- `../normalized/lead_event_ops_registry.csv`: machine-friendly one-row-per-lead registry",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    output_dir = args.output_dir
    by_event_month_dir = output_dir / "by_event_month"
    ensure_dir(output_dir)
    ensure_dir(by_event_month_dir)

    deal_rows = load_csv_rows(args.deal_sheets_csv)
    event_rows = {row["lead_id"]: row for row in load_csv_rows(args.event_facts_csv)}
    convo_index = {row["lead_id"]: row for row in load_csv_rows(args.conversation_csv)}

    now_utc = datetime.now(timezone.utc)
    registry_rows: List[Dict[str, Any]] = []

    for deal_row in deal_rows:
        lead_id = deal_row["lead_id"]
        lead_dir = Path(deal_row["deal_sheet_path"]).parent
        event_row = event_rows.get(lead_id, {})
        convo_row = convo_index.get(lead_id, {})
        convo_json_path = Path(convo_row.get("conversation_intelligence_json_path") or lead_dir / "lead_conversation_intelligence.json")
        conversation_payload = load_json(convo_json_path)
        text_rows = build_text_rows(conversation_payload, deal_row, event_row)

        service_labels, service_evidence = infer_signals(
            deal_row.get("service_style_signals") or "",
            deal_row.get("service_style_evidence") or "",
            text_rows,
            SERVICE_STYLE_RULES,
        )
        bar_labels, bar_evidence = infer_signals(
            deal_row.get("bar_signals") or "",
            deal_row.get("bar_evidence") or "",
            text_rows,
            BAR_RULES,
        )
        staffing_labels, staffing_evidence = infer_signals(
            deal_row.get("staffing_signals") or "",
            deal_row.get("staffing_evidence") or "",
            text_rows,
            STAFFING_RULES,
        )

        requirement_map = requirement_flags(text_rows)
        event_dt = parse_iso(deal_row.get("event_datetime_utc") or event_row.get("event_datetime_utc"))
        horizon, days_until_event_text = planning_horizon(event_dt, now_utc)
        days_until_event = int(days_until_event_text) if days_until_event_text and re.fullmatch(r"-?\d+", days_until_event_text) else None

        guest_min = parse_int(event_row.get("guest_count_min"))
        guest_max = parse_int(event_row.get("guest_count_max"))
        guest_band = derive_guest_band(guest_min, guest_max, deal_row.get("guest_count_text") or event_row.get("guest_count_text") or "")
        event_type_norm = normalize_event_type(deal_row.get("event_type") or event_row.get("event_type") or "")
        venue_status = (deal_row.get("venue_status") or "unknown").strip() or "unknown"
        scope = ops_scope(deal_row.get("stage_type") or "", deal_row.get("readiness_bucket") or "", deal_row.get("payment_state") or "")
        venue_gap = venue_gap_flag(deal_row.get("stage_type") or "", deal_row.get("readiness_bucket") or "", venue_status)
        complexity_score = ops_complexity_score(
            event_type_norm=event_type_norm,
            guest_max=guest_max or guest_min,
            service_labels=service_labels,
            bar_labels=bar_labels,
            staffing_labels=staffing_labels,
            venue_status=venue_status,
            requirement_map=requirement_map,
            scope=scope,
            days_until_event=days_until_event,
        )
        complexity_band_value = ops_complexity_band(complexity_score)
        high_touch = high_touch_flag(
            complexity_score=complexity_score,
            guest_max=guest_max or guest_min,
            service_labels=service_labels,
            bar_labels=bar_labels,
            staffing_labels=staffing_labels,
            venue_status=venue_status,
            requirement_map=requirement_map,
        )
        watch_reason = ops_watch_reason(
            stage_type=deal_row.get("stage_type") or "",
            scope=scope,
            venue_gap=venue_gap,
            complexity_band_value=complexity_band_value,
            payment_state=deal_row.get("payment_state") or "",
            days_until_event=days_until_event,
            requirement_map=requirement_map,
            event_datetime_source=event_row.get("event_datetime_source") or "",
        )

        venue_summary_value = venue_summary(event_row, deal_row)
        action_plan_path = lead_dir / "lead_action_plan.md"
        payload: Dict[str, Any] = {
            "lead_id": lead_id,
            "lead_name": deal_row.get("lead_name") or "",
            "lead_owner_name": deal_row.get("lead_owner_name") or "",
            "pipeline_name": deal_row.get("pipeline_name") or "",
            "stage_label": deal_row.get("stage_label") or "",
            "stage_type": deal_row.get("stage_type") or "",
            "event_datetime_utc": deal_row.get("event_datetime_utc") or event_row.get("event_datetime_utc") or "",
            "event_datetime_source": event_row.get("event_datetime_source") or "",
            "event_month": event_month_label(event_dt),
            "days_until_event": days_until_event_text,
            "planning_horizon": horizon,
            "event_type_normalized": event_type_norm,
            "event_type_raw": deal_row.get("event_type") or event_row.get("event_type") or "",
            "guest_count_text": deal_row.get("guest_count_text") or event_row.get("guest_count_text") or "",
            "guest_count_min": guest_min,
            "guest_count_max": guest_max,
            "guest_count_band": guest_band,
            "venue_status": venue_status,
            "venue_bucket": venue_bucket(venue_status),
            "venue_name": event_row.get("venue_name") or deal_row.get("venue_name") or "",
            "venue_city": event_row.get("venue_city") or deal_row.get("venue_city") or "",
            "venue_summary": venue_summary_value,
            "service_style_primary": choose_primary(service_labels),
            "service_style_signals": " | ".join(service_labels),
            "service_style_evidence": service_evidence,
            "bar_primary": choose_primary(bar_labels),
            "bar_signals": " | ".join(bar_labels),
            "bar_evidence": bar_evidence,
            "staffing_primary": choose_primary(staffing_labels),
            "staffing_signals": " | ".join(staffing_labels),
            "staffing_evidence": staffing_evidence,
            "ops_requirement_flags": " | ".join(sorted(requirement_map.keys())),
            "ops_requirement_evidence_map": requirement_map,
            "ops_complexity_score": complexity_score,
            "ops_complexity_band": complexity_band_value,
            "ops_scope": scope,
            "execution_watch_flag": execution_watch_flag(scope),
            "venue_gap_flag": venue_gap,
            "high_touch_flag": high_touch,
            "payment_state": deal_row.get("payment_state") or "",
            "decision_state": deal_row.get("decision_state") or "",
            "readiness_bucket": deal_row.get("readiness_bucket") or "",
            "readiness_score": deal_row.get("readiness_score") or "",
            "ops_watch_reason": watch_reason,
            "ops_summary": summary_line(
                lead_name=deal_row.get("lead_name") or "",
                event_type_norm=event_type_norm,
                guest_band=guest_band,
                event_dt=event_dt,
                venue_summary_value=venue_summary_value,
                service_labels=service_labels,
                bar_labels=bar_labels,
                staffing_labels=staffing_labels,
                complexity_band_value=complexity_band_value,
                watch_reason=watch_reason,
            ),
            "deal_sheet_path": deal_row.get("deal_sheet_path") or "",
            "deal_sheet_json_path": deal_row.get("deal_sheet_json_path") or "",
            "event_facts_path": deal_row.get("event_facts_path") or "",
            "conversation_path": deal_row.get("conversation_path") or convo_row.get("conversation_intelligence_path") or "",
            "conversation_json_path": str(convo_json_path),
            "action_plan_path": str(action_plan_path) if action_plan_path.exists() else "",
            "lead_event_ops_sheet_path": str(lead_dir / "lead_event_ops_sheet.md"),
            "lead_event_ops_sheet_json_path": str(lead_dir / "lead_event_ops_sheet.json"),
        }

        write_json(lead_dir / "lead_event_ops_sheet.json", payload)
        (lead_dir / "lead_event_ops_sheet.md").write_text(build_lead_markdown(payload), encoding="utf-8")
        registry_rows.append(payload)

    registry_rows = sorted(registry_rows, key=sort_watch_key)
    execution_rows = sorted([row for row in registry_rows if row.get("execution_watch_flag")], key=sort_execution_key)
    venue_gap_rows = sorted([row for row in registry_rows if row.get("venue_gap_flag")], key=sort_event_key)
    high_touch_rows = sorted([row for row in registry_rows if row.get("high_touch_flag") and row.get("stage_type") != "lost"], key=lambda row: (-int(row.get("ops_complexity_score") or 0),) + sort_event_key(row))
    future_rows = sorted(
        [
            row
            for row in registry_rows
            if row.get("stage_type") != "lost"
            and parse_iso(row.get("event_datetime_utc"))
            and parse_iso(row.get("event_datetime_utc")) >= now_utc
        ],
        key=sort_event_key,
    )

    month_rollups: List[Dict[str, Any]] = []
    future_by_month: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in future_rows:
        future_by_month[row.get("event_month") or "unknown"].append(row)

    for month, rows in sorted(future_by_month.items()):
        month_dir = by_event_month_dir / month
        ensure_dir(month_dir)
        sorted_rows = sorted(rows, key=sort_event_key)
        write_csv(month_dir / "events.csv", sorted_rows)
        write_jsonl(month_dir / "events.jsonl", sorted_rows)
        (month_dir / "events.md").write_text(build_board_markdown(f"Events: {month}", sorted_rows, "rows"), encoding="utf-8")
        month_rollups.append(
            {
                "event_month": month,
                "row_count": len(sorted_rows),
                "execution_watch_count": sum(1 for row in sorted_rows if row.get("execution_watch_flag")),
                "venue_gap_count": sum(1 for row in sorted_rows if row.get("venue_gap_flag")),
                "high_touch_count": sum(1 for row in sorted_rows if row.get("high_touch_flag")),
            }
        )

    service_mix_rows: List[Dict[str, Any]] = []
    active_or_execution_rows = [row for row in registry_rows if row.get("stage_type") != "lost"]
    for dimension in ("service_style_primary", "bar_primary", "staffing_primary", "venue_status", "ops_complexity_band"):
        counts = Counter(row.get(dimension) or "unknown" for row in active_or_execution_rows)
        for label, count in counts.items():
            service_mix_rows.append({"dimension": dimension, "label": label, "count": count})
    service_mix_rows = sorted(service_mix_rows, key=lambda row: (row["dimension"], -row["count"], row["label"]))

    write_csv(normalized_dir / "lead_event_ops_registry.csv", registry_rows)
    write_jsonl(normalized_dir / "lead_event_ops_registry.jsonl", registry_rows)
    write_csv(normalized_dir / "execution_watch_board.csv", execution_rows)
    write_jsonl(normalized_dir / "execution_watch_board.jsonl", execution_rows)
    write_csv(normalized_dir / "venue_gap_board.csv", venue_gap_rows)
    write_jsonl(normalized_dir / "venue_gap_board.jsonl", venue_gap_rows)
    write_csv(normalized_dir / "high_touch_ops_board.csv", high_touch_rows)
    write_jsonl(normalized_dir / "high_touch_ops_board.jsonl", high_touch_rows)
    write_csv(normalized_dir / "future_event_calendar.csv", future_rows)
    write_jsonl(normalized_dir / "future_event_calendar.jsonl", future_rows)
    write_csv(normalized_dir / "event_month_rollup.csv", month_rollups)
    write_csv(normalized_dir / "service_mix_rollup.csv", service_mix_rows)

    (output_dir / "README.md").write_text(
        build_readme(registry_rows, execution_rows, venue_gap_rows, high_touch_rows, future_rows, month_rollups),
        encoding="utf-8",
    )
    (output_dir / "execution_watch_board.md").write_text(
        build_board_markdown("Execution Watch Board", execution_rows, "execution-watch rows"),
        encoding="utf-8",
    )
    (output_dir / "venue_gap_board.md").write_text(
        build_board_markdown("Venue Gap Board", venue_gap_rows, "venue-gap rows"),
        encoding="utf-8",
    )
    (output_dir / "high_touch_ops_board.md").write_text(
        build_board_markdown("High-Touch Ops Board", high_touch_rows, "high-touch rows"),
        encoding="utf-8",
    )
    (output_dir / "future_event_calendar.md").write_text(build_calendar_markdown(future_rows), encoding="utf-8")
    (output_dir / "service_mix_rollup.md").write_text(build_service_mix_markdown(active_or_execution_rows), encoding="utf-8")

    print(
        json.dumps(
            {
                "registry_rows": len(registry_rows),
                "execution_rows": len(execution_rows),
                "venue_gap_rows": len(venue_gap_rows),
                "high_touch_rows": len(high_touch_rows),
                "future_rows": len(future_rows),
                "event_months": len(month_rollups),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
