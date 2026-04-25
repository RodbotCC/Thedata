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
DEFAULT_CONVERSATION_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_conversation_intelligence.csv"
DEFAULT_EVENT_OPS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_event_ops_registry.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "menu_intelligence"

NOISE_PATTERNS = (
    "privacy policy",
    "facebook",
    "instagram",
    "website",
    "direct line",
    "catering main",
    "copyright",
    "the knot worldwide",
    "your call has been forwarded",
    "person you're trying to reach is not available",
)

CUISINE_RULES: List[Tuple[str, Sequence[str]]] = [
    ("brazilian", (r"\bbrazilian\b", r"\bpao de queijo\b", r"\bcaipirinha\b")),
    ("churrasco_grill", (r"\bchurrasco\b", r"\brodizio\b", r"\bgrill(?:ing)?\b", r"\bgrilled\b")),
    ("breakfast_brunch", (r"\bbreakfast\b", r"\bbkfst\b", r"\bbrunch\b", r"\bbreakfast sandwiches?\b")),
    ("seafood", (r"\bseafood\b", r"\btilapia\b", r"\bsalmon\b", r"\bshrimp\b", r"\bfish\b")),
    ("mediterranean", (r"\bcaprese\b", r"\bspanakopita\b", r"\bmediterranean\b")),
]

MENU_TOPIC_RULES: List[Tuple[str, Sequence[str]]] = [
    ("menu_choices", (r"\bchoice of entrees\b", r"\bchoice of sides\b", r"\bwhat meats?\b", r"\bwhat sides?\b", r"\bmenu\b", r"\boptions?\b")),
    ("appetizers_and_boards", (r"\bappetizers?\b", r"\bboards?\b", r"\bplatters?\b", r"\bgrazing\b", r"\bcharcuterie\b", r"\bcheese board\b", r"\bfruit platter\b", r"\bcaprese\b", r"\bskewer\b", r"\bmini cups?\b", r"\bspanakopita\b")),
    ("entrees_and_sides", (r"\bentrees?\b", r"\bsides?\b", r"\bproteins?\b", r"\bmeats?\b", r"\bchicken\b", r"\bsteak\b", r"\brice\b", r"\bbeans?\b", r"\bpotatoes?\b", r"\bsalad\b", r"\bvegetables?\b")),
    ("breakfast_brunch_items", (r"\bbreakfast\b", r"\bbkfst\b", r"\bbrunch\b", r"\bsandwiches?\b", r"\bcoffee\b", r"\btea\b", r"\bjuices?\b", r"\bpastr(?:y|ies)\b")),
    ("dessert_sweets", (r"\bdesserts?\b", r"\bcake\b", r"\bbrigadeiro\b", r"\bsweet table\b", r"\bfruit display\b")),
    ("kids_menu_or_split_pricing", (r"\bkids? menu\b", r"\bkids and adults\b", r"\bchildren\b", r"\bchild(?:ren)?\b", r"\badults?\b")),
    ("bar_program", (r"\bopen bar\b", r"\bcash bar\b", r"\bmobile bar\b", r"\bbartender\b", r"\bbeer and wine\b", r"\bcaipirinha\b", r"\bcocktails?\b", r"\bbar service\b")),
    ("tasting_path", (r"\btasting\b", r"\binvitation\b", r"\bregistration\b", r"\breserve your spot\b")),
]

DIETARY_RULES: List[Tuple[str, Sequence[str]]] = [
    ("vegetarian", (r"\bvegetarian\b",)),
    ("vegan", (r"\bvegan\b",)),
    ("gluten_free", (r"\bgluten[- ]?free\b", r"\bgluten\b")),
    ("dairy_free", (r"\bdairy[- ]?free\b", r"\bdairy\b")),
    ("nut_allergy", (r"\bnut allergy\b", r"\bnut[- ]?free\b", r"\bpeanut\b")),
    ("shellfish_or_seafood_restriction", (r"\bshellfish\b", r"\bseafood allergy\b")),
    ("general_allergy_flag", (r"\ballerg(?:y|ies|ic)\b",)),
]

VENUE_FOOD_RULES: List[Tuple[str, Sequence[str]]] = [
    ("outside_catering_rules", (r"\boutside catering\b", r"\bvenue allows\b", r"\bcatering rules\b")),
    ("no_commercial_kitchen", (r"\bcommercial kitchen\b", r"\bno kitchen\b", r"\bprep space\b", r"\bprep room\b")),
    ("staffing_or_service_window", (r"\bhow many staff\b", r"\bstaff stay\b", r"\bdo you stay\b", r"\bhow many hours\b", r"\bserve for how many hours\b", r"\bsetup and then\b", r"\bdinner clean[- ]?up\b")),
    ("warming_or_hot_holding", (r"\bwarming dishes\b", r"\bchafing dishes\b", r"\btemperature of the food\b", r"\bhot holding\b")),
    ("cleanup_or_waste", (r"\bclean[- ]?up\b", r"\bcleanup\b", r"\bwaste removal\b")),
    ("venue_visit_or_walkthrough", (r"\bwalkthrough\b", r"\bvenue visit\b", r"\bassess logistics\b", r"\bowner visit\b")),
    ("drop_off_or_setup_only", (r"\bdrop[- ]off\b", r"\bsetup only\b")),
]

ITEM_RULES: List[Tuple[str, Sequence[str]]] = [
    ("pao_de_queijo", (r"\bpao de queijo\b",)),
    ("quinoa_chickpea_salad", (r"\bquinoa\b.*\bchickpea\b", r"\bchickpea\b.*\bquinoa\b")),
    ("fruit_platter", (r"\bfruit platter\b",)),
    ("caprese_skewers", (r"\bcaprese\b", r"\bcaprese skewers?\b")),
    ("breakfast_sandwiches", (r"\bbreakfast sandwiches?\b", r"\bbkfst sandwiches?\b")),
    ("spanakopita", (r"\bspanakopita\b",)),
    ("cheese_and_crackers", (r"\bcheese (?:and|&)\s*cracker\b", r"\bcheese board\b", r"\bcheese platter\b")),
    ("grazing_table", (r"\bgrazing table\b", r"\bgrazing\b")),
    ("charcuterie_board", (r"\bcharcuterie\b",)),
    ("mashed_potatoes", (r"\bmashed potatoes?\b",)),
    ("baked_potatoes", (r"\bbaked potatoes?\b",)),
    ("chicken", (r"\bchicken\b",)),
    ("steak", (r"\bsteak\b",)),
    ("tilapia", (r"\btilapia\b",)),
    ("salad", (r"\bsalad\b",)),
    ("rice", (r"\brice\b",)),
    ("green_beans", (r"\bgreen beans\b",)),
    ("zucchini_or_squash", (r"\bzucchini\b", r"\bsquash\b")),
    ("coffee_tea_juice", (r"\bcoffee\b", r"\btea\b", r"\bjuices?\b")),
    ("caipirinha", (r"\bcaipirinha\b",)),
]

BAR_RULES: List[Tuple[str, Sequence[str]]] = [
    ("no_bar_service", (r"\bdon'?t need (?:the )?bar\b", r"\bno bar\b", r"\bwithout the wine\b")),
    ("open_bar", (r"\bopen bar\b",)),
    ("cash_bar", (r"\bcash bar\b",)),
    ("mobile_bar", (r"\bmobile bar\b",)),
    ("beer_and_wine", (r"\bbeer and wine\b", r"\bbeer, wine\b")),
    ("bartender", (r"\bbartender\b", r"\bbartending\b")),
    ("caipirinha", (r"\bcaipirinha\b",)),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a structured menu / cuisine / dietary intelligence layer from normalized Comeketo lead data.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--deal-sheets-csv", type=Path, default=DEFAULT_DEAL_SHEETS_CSV)
    parser.add_argument("--conversation-csv", type=Path, default=DEFAULT_CONVERSATION_CSV)
    parser.add_argument("--event-ops-csv", type=Path, default=DEFAULT_EVENT_OPS_CSV)
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


def compact_text(value: Optional[str], limit: int = 280) -> str:
    text = " ".join((value or "").split())
    return text[:limit]


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return text or "unknown"


def split_labels(value: Optional[str]) -> List[str]:
    labels: List[str] = []
    for part in (value or "").split("|"):
        label = part.strip()
        if label and label not in labels:
            labels.append(label)
    return labels


def merge_labels(*groups: Sequence[str]) -> List[str]:
    merged: List[str] = []
    for group in groups:
        for label in group:
            if label and label not in merged:
                merged.append(label)
    return merged


def pretty_label(value: str) -> str:
    return value.replace("_", " ")


def dedupe_key(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def is_noise_text(text: str) -> bool:
    lowered = text.lower().strip()
    if not lowered:
        return True
    return any(pattern in lowered for pattern in NOISE_PATTERNS)


def sort_items_by_time(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(items, key=lambda item: item.get("event_datetime_utc") or "", reverse=True)


def build_text_rows(conversation_payload: Dict[str, Any], deal_row: Dict[str, str], ops_row: Dict[str, str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()

    for section in ("buyer_asks", "blockers", "preferences", "open_loops", "sales_commitments"):
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
        ("deal_venue", "", deal_row.get("venue_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("latest_preference", "", conversation_payload.get("latest_preference") or "", conversation_payload.get("latest_observed_activity_utc") or "", conversation_payload.get("conversation_intelligence_path") or ""),
        ("latest_buyer_ask", "", conversation_payload.get("latest_buyer_ask") or "", conversation_payload.get("latest_observed_activity_utc") or "", conversation_payload.get("conversation_intelligence_path") or ""),
        ("ops_summary", "", ops_row.get("ops_summary") or "", ops_row.get("event_datetime_utc") or "", ops_row.get("lead_event_ops_sheet_path") or ""),
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
                labels.append(label)
                evidence[label] = compact_text(text, 500)
    return labels, evidence


def merge_existing_and_detected(existing_value: str, text_rows: Sequence[Dict[str, Any]], rules: Sequence[Tuple[str, Sequence[str]]]) -> Tuple[List[str], Dict[str, str]]:
    existing = split_labels(existing_value)
    detected, evidence = collect_labels(text_rows, rules)
    merged = merge_labels(existing, detected)
    for label in existing:
        evidence.setdefault(label, "")
    return merged, evidence


def relevant_menu_question(row: Dict[str, Any]) -> bool:
    if row.get("section") not in {"buyer_asks", "open_loops"}:
        return False
    text = row.get("text") or ""
    if not text:
        return False
    patterns: List[str] = []
    for _label, rule_set in MENU_TOPIC_RULES + CUISINE_RULES + DIETARY_RULES + VENUE_FOOD_RULES + ITEM_RULES + BAR_RULES:
        patterns.extend(rule_set)
    categories = {"menu_selection", "bar_service", "guest_count_scope", "tasting"}
    return row.get("category") in categories or any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def select_top_questions(text_rows: Sequence[Dict[str, Any]], limit: int = 6) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    seen = set()
    for row in text_rows:
        if not relevant_menu_question(row):
            continue
        text = row.get("text") or ""
        key = dedupe_key(text)
        if key in seen:
            continue
        seen.add(key)
        selected.append(row)
        if len(selected) >= limit:
            break
    return selected


def count_relevant_questions(text_rows: Sequence[Dict[str, Any]]) -> int:
    seen = set()
    count = 0
    for row in text_rows:
        if not relevant_menu_question(row):
            continue
        text = row.get("text") or ""
        key = dedupe_key(text)
        if key in seen:
            continue
        seen.add(key)
        count += 1
    return count


def first_evidence(evidence_map: Dict[str, str], labels: Sequence[str]) -> str:
    for label in labels:
        if evidence_map.get(label):
            return evidence_map[label]
    return ""


def menu_signal_score(
    cuisine_labels: Sequence[str],
    menu_topics: Sequence[str],
    item_labels: Sequence[str],
    dietary_labels: Sequence[str],
    venue_food_labels: Sequence[str],
    bar_labels: Sequence[str],
    question_count: int,
) -> int:
    return (
        len(cuisine_labels) * 2
        + len(menu_topics) * 2
        + len(item_labels)
        + len(dietary_labels) * 3
        + len(venue_food_labels) * 2
        + len([label for label in bar_labels if label and label != "no_bar_service"])
        + question_count
    )


def summary_line(
    lead_name: str,
    cuisine_labels: Sequence[str],
    menu_topics: Sequence[str],
    dietary_labels: Sequence[str],
    venue_food_labels: Sequence[str],
    bar_labels: Sequence[str],
    question_count: int,
    venue_summary: str,
) -> str:
    cuisine_text = " / ".join(pretty_label(label) for label in cuisine_labels) if cuisine_labels else "no explicit cuisine"
    topic_text = " / ".join(pretty_label(label) for label in menu_topics[:3]) if menu_topics else "general menu discovery"
    dietary_text = " / ".join(pretty_label(label) for label in dietary_labels) if dietary_labels else "no dietary flags"
    venue_text = " / ".join(pretty_label(label) for label in venue_food_labels[:3]) if venue_food_labels else "no venue food constraints"
    bar_text = " / ".join(pretty_label(label) for label in bar_labels) if bar_labels else "no bar direction"
    return compact_text(
        f"{lead_name}: cuisine {cuisine_text}; menu {topic_text}; dietary {dietary_text}; "
        f"venue food {venue_text}; bar {bar_text}; menu questions {question_count}; venue {venue_summary or 'unknown'}.",
        420,
    )


def sort_key(row: Dict[str, Any]) -> Tuple[int, int, int, datetime]:
    dt = parse_iso(row.get("event_datetime_utc"))
    fallback_dt = datetime.max.replace(tzinfo=timezone.utc)
    return (
        -int(row.get("priority_score") or 0),
        -int(row.get("menu_signal_score") or 0),
        -int(row.get("menu_question_count") or 0),
        dt or fallback_dt,
    )


def build_profile_markdown(payload: Dict[str, Any], top_questions: Sequence[Dict[str, Any]]) -> str:
    lines = [
        f"# Menu Profile: {payload.get('lead_name') or ''}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Pipeline / Stage: {payload.get('pipeline_name') or ''} / {payload.get('stage_label') or ''}",
        f"- Priority Score: `{payload.get('priority_score') or ''}`",
        f"- Menu Signal Score: `{payload.get('menu_signal_score') or ''}`",
        f"- Event Date (UTC): `{payload.get('event_datetime_utc') or ''}`",
        f"- Venue: {payload.get('venue_summary') or ''}",
        "",
        "## Preferences",
        f"- Cuisine Signals: {payload.get('cuisine_signals') or ''}",
        f"- Menu Topics: {payload.get('menu_topic_flags') or ''}",
        f"- Specific Items: {payload.get('specific_item_flags') or ''}",
        f"- Service Format: {payload.get('service_format_signals') or ''}",
        f"- Bar Program: {payload.get('bar_program_flags') or ''}",
        "",
        "## Restrictions",
        f"- Dietary Flags: {payload.get('dietary_flags') or ''}",
        f"- Venue Food Constraints: {payload.get('venue_food_flags') or ''}",
        "",
        "## Open Menu Questions",
        f"- Menu Question Count: `{payload.get('menu_question_count') or ''}`",
    ]
    if top_questions:
        for row in top_questions:
            lines.append(f"- {row.get('text') or ''}")
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "## Evidence",
            f"- Cuisine Evidence: {payload.get('cuisine_evidence') or ''}",
            f"- Menu Evidence: {payload.get('menu_topic_evidence') or ''}",
            f"- Item Evidence: {payload.get('specific_item_evidence') or ''}",
            f"- Dietary Evidence: {payload.get('dietary_evidence') or ''}",
            f"- Venue Food Evidence: {payload.get('venue_food_evidence') or ''}",
            f"- Bar Evidence: {payload.get('bar_program_evidence') or ''}",
            "",
            "## Summary",
            f"- {payload.get('menu_profile_summary') or ''}",
            "",
        ]
    )
    return "\n".join(lines)


def build_board_markdown(title: str, rows: Sequence[Dict[str, Any]], count_label: str, column: str) -> str:
    lines = [f"# {title}", "", f"- Total {count_label}: `{len(rows)}`", "", "## Top Rows"]
    for row in rows[:120]:
        primary_text = (
            row.get(column)
            or row.get("top_menu_question")
            or row.get("menu_profile_summary")
            or row.get("menu_topic_flags")
            or row.get("specific_item_flags")
            or row.get("cuisine_signals")
            or row.get("dietary_flags")
            or row.get("venue_food_flags")
            or row.get("bar_program_flags")
            or "No explicit menu signal captured."
        )
        lines.append(
            f"- `{row.get('priority_score')}` | {row.get('lead_name')} | {row.get('lead_owner_name')} | "
            f"{row.get('stage_label')} | {primary_text}"
        )
    lines.append("")
    return "\n".join(lines)


def build_rollup_markdown(rows: Sequence[Dict[str, Any]]) -> str:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["dimension"]].append(row)

    lines = ["# Menu Signal Rollup", ""]
    for dimension in sorted(grouped.keys()):
        lines.append(f"## {dimension}")
        for row in grouped[dimension]:
            lines.append(f"- {pretty_label(row['label'])}: {row['count']}")
        lines.append("")
    return "\n".join(lines)


def build_readme(
    profiles: Sequence[Dict[str, Any]],
    customization_rows: Sequence[Dict[str, Any]],
    venue_rows: Sequence[Dict[str, Any]],
    dietary_rows: Sequence[Dict[str, Any]],
    bar_rows: Sequence[Dict[str, Any]],
    cuisine_rows: Sequence[Dict[str, Any]],
) -> str:
    lines = [
        "# Menu Intelligence",
        "",
        "This layer compresses buyer food preferences into a structured menu profile: cuisine style, item requests, dietary notes, venue food constraints, and the open menu questions still blocking movement.",
        "",
        "## Snapshot",
        f"- Lead menu profiles: `{len(profiles)}`",
        f"- Menu customization rows: `{len(customization_rows)}`",
        f"- Venue food restriction rows: `{len(venue_rows)}`",
        f"- Dietary watch rows: `{len(dietary_rows)}`",
        f"- Bar-program rows: `{len(bar_rows)}`",
        f"- Cuisine-preference rows: `{len(cuisine_rows)}`",
        "",
        "## Key Files",
        "- `menu_customization_board.md`: leads with dense menu questions or item requests",
        "- `venue_food_restriction_board.md`: leads where kitchen / venue / staffing rules affect the menu",
        "- `dietary_watch_board.md`: leads with dietary or allergy notes",
        "- `bar_program_board.md`: leads with bar-direction asks or constraints",
        "- `cuisine_preference_board.md`: leads with explicit cuisine-style direction",
        "- `menu_signal_rollup.md`: global rollup across cuisines, topics, items, dietary flags, and venue constraints",
        "- `../normalized/lead_menu_profiles.csv`: machine-friendly one-row-per-lead menu profile",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    output_dir = args.output_dir
    ensure_dir(output_dir)

    deal_rows = load_csv_rows(args.deal_sheets_csv)
    conversation_rows = {row["lead_id"]: row for row in load_csv_rows(args.conversation_csv)}
    event_ops_rows = {row["lead_id"]: row for row in load_csv_rows(args.event_ops_csv)}

    profile_rows: List[Dict[str, Any]] = []

    for deal_row in deal_rows:
        lead_id = deal_row["lead_id"]
        lead_dir = Path(deal_row["deal_sheet_path"]).parent
        convo_row = conversation_rows.get(lead_id, {})
        ops_row = event_ops_rows.get(lead_id, {})
        convo_json_path = Path(convo_row.get("conversation_intelligence_json_path") or lead_dir / "lead_conversation_intelligence.json")
        conversation_payload = load_json(convo_json_path)
        text_rows = build_text_rows(conversation_payload, deal_row, ops_row)

        cuisine_labels, cuisine_evidence_map = collect_labels(text_rows, CUISINE_RULES)
        menu_topics, menu_topic_evidence_map = collect_labels(text_rows, MENU_TOPIC_RULES)
        item_labels, item_evidence_map = collect_labels(text_rows, ITEM_RULES)
        dietary_labels, dietary_evidence_map = collect_labels(text_rows, DIETARY_RULES)
        venue_food_labels, venue_food_evidence_map = collect_labels(text_rows, VENUE_FOOD_RULES)
        bar_labels, bar_evidence_map = merge_existing_and_detected(deal_row.get("bar_signals") or "", text_rows, BAR_RULES)
        service_format_labels = split_labels(deal_row.get("service_style_signals") or "")

        top_questions = select_top_questions(text_rows)
        question_count = count_relevant_questions(text_rows)
        priority_score = max(int(deal_row.get("readiness_score") or 0), int(convo_row.get("follow_up_priority_score") or 0))
        signal_score = menu_signal_score(
            cuisine_labels=cuisine_labels,
            menu_topics=menu_topics,
            item_labels=item_labels,
            dietary_labels=dietary_labels,
            venue_food_labels=venue_food_labels,
            bar_labels=bar_labels,
            question_count=question_count,
        )

        payload: Dict[str, Any] = {
            "lead_id": lead_id,
            "lead_name": deal_row.get("lead_name") or "",
            "lead_owner_name": deal_row.get("lead_owner_name") or "",
            "pipeline_name": deal_row.get("pipeline_name") or "",
            "stage_label": deal_row.get("stage_label") or "",
            "stage_type": deal_row.get("stage_type") or "",
            "event_datetime_utc": ops_row.get("event_datetime_utc") or deal_row.get("event_datetime_utc") or "",
            "venue_summary": ops_row.get("venue_summary") or "",
            "venue_status": ops_row.get("venue_status") or deal_row.get("venue_status") or "",
            "priority_score": priority_score,
            "menu_signal_score": signal_score,
            "menu_question_count": question_count,
            "top_menu_question": top_questions[0]["text"] if top_questions else "",
            "cuisine_signals": " | ".join(cuisine_labels),
            "menu_topic_flags": " | ".join(menu_topics),
            "specific_item_flags": " | ".join(item_labels),
            "dietary_flags": " | ".join(dietary_labels),
            "venue_food_flags": " | ".join(venue_food_labels),
            "bar_program_flags": " | ".join(bar_labels),
            "service_format_signals": " | ".join(service_format_labels),
            "cuisine_evidence": first_evidence(cuisine_evidence_map, cuisine_labels),
            "menu_topic_evidence": first_evidence(menu_topic_evidence_map, menu_topics),
            "specific_item_evidence": first_evidence(item_evidence_map, item_labels),
            "dietary_evidence": first_evidence(dietary_evidence_map, dietary_labels),
            "venue_food_evidence": first_evidence(venue_food_evidence_map, venue_food_labels),
            "bar_program_evidence": first_evidence(bar_evidence_map, bar_labels) or deal_row.get("bar_evidence") or "",
            "menu_profile_summary": summary_line(
                lead_name=deal_row.get("lead_name") or "",
                cuisine_labels=cuisine_labels,
                menu_topics=menu_topics,
                dietary_labels=dietary_labels,
                venue_food_labels=venue_food_labels,
                bar_labels=bar_labels,
                question_count=question_count,
                venue_summary=ops_row.get("venue_summary") or "",
            ),
            "lead_menu_profile_path": str(lead_dir / "lead_menu_profile.md"),
            "lead_menu_profile_json_path": str(lead_dir / "lead_menu_profile.json"),
            "deal_sheet_path": deal_row.get("deal_sheet_path") or "",
            "conversation_path": convo_row.get("conversation_intelligence_path") or deal_row.get("conversation_path") or "",
            "conversation_json_path": str(convo_json_path),
            "event_ops_path": ops_row.get("lead_event_ops_sheet_path") or "",
        }

        write_json(lead_dir / "lead_menu_profile.json", payload)
        (lead_dir / "lead_menu_profile.md").write_text(build_profile_markdown(payload, top_questions), encoding="utf-8")
        profile_rows.append(payload)

    profile_rows = sorted(profile_rows, key=sort_key)
    active_rows = [row for row in profile_rows if row.get("stage_type") != "lost"]

    customization_rows = [row for row in active_rows if row.get("menu_topic_flags") or row.get("specific_item_flags") or int(row.get("menu_question_count") or 0) > 0]
    venue_rows = [row for row in active_rows if row.get("venue_food_flags")]
    dietary_rows = [row for row in active_rows if row.get("dietary_flags")]
    bar_rows = [row for row in active_rows if row.get("bar_program_flags")]
    cuisine_rows = [row for row in active_rows if row.get("cuisine_signals")]

    rollup_rows: List[Dict[str, Any]] = []
    for dimension in ("cuisine_signals", "menu_topic_flags", "specific_item_flags", "dietary_flags", "venue_food_flags", "bar_program_flags", "service_format_signals"):
        counter = Counter()
        for row in active_rows:
            for label in split_labels(row.get(dimension) or ""):
                counter[label] += 1
        for label, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
            rollup_rows.append({"dimension": dimension, "label": label, "count": count})

    write_csv(normalized_dir / "lead_menu_profiles.csv", profile_rows)
    write_jsonl(normalized_dir / "lead_menu_profiles.jsonl", profile_rows)
    write_csv(normalized_dir / "menu_customization_board.csv", customization_rows)
    write_jsonl(normalized_dir / "menu_customization_board.jsonl", customization_rows)
    write_csv(normalized_dir / "venue_food_restriction_board.csv", venue_rows)
    write_jsonl(normalized_dir / "venue_food_restriction_board.jsonl", venue_rows)
    write_csv(normalized_dir / "dietary_watch_board.csv", dietary_rows)
    write_jsonl(normalized_dir / "dietary_watch_board.jsonl", dietary_rows)
    write_csv(normalized_dir / "bar_program_board.csv", bar_rows)
    write_jsonl(normalized_dir / "bar_program_board.jsonl", bar_rows)
    write_csv(normalized_dir / "cuisine_preference_board.csv", cuisine_rows)
    write_jsonl(normalized_dir / "cuisine_preference_board.jsonl", cuisine_rows)
    write_csv(normalized_dir / "menu_signal_rollup.csv", rollup_rows)

    (output_dir / "README.md").write_text(
        build_readme(profile_rows, customization_rows, venue_rows, dietary_rows, bar_rows, cuisine_rows),
        encoding="utf-8",
    )
    (output_dir / "menu_customization_board.md").write_text(
        build_board_markdown("Menu Customization Board", customization_rows, "menu-customization rows", "top_menu_question"),
        encoding="utf-8",
    )
    (output_dir / "venue_food_restriction_board.md").write_text(
        build_board_markdown("Venue Food Restriction Board", venue_rows, "venue-restriction rows", "venue_food_flags"),
        encoding="utf-8",
    )
    (output_dir / "dietary_watch_board.md").write_text(
        build_board_markdown("Dietary Watch Board", dietary_rows, "dietary-watch rows", "dietary_flags"),
        encoding="utf-8",
    )
    (output_dir / "bar_program_board.md").write_text(
        build_board_markdown("Bar Program Board", bar_rows, "bar-program rows", "bar_program_flags"),
        encoding="utf-8",
    )
    (output_dir / "cuisine_preference_board.md").write_text(
        build_board_markdown("Cuisine Preference Board", cuisine_rows, "cuisine-preference rows", "cuisine_signals"),
        encoding="utf-8",
    )
    (output_dir / "menu_signal_rollup.md").write_text(build_rollup_markdown(rollup_rows), encoding="utf-8")

    print(
        json.dumps(
            {
                "profiles": len(profile_rows),
                "customization_rows": len(customization_rows),
                "venue_rows": len(venue_rows),
                "dietary_rows": len(dietary_rows),
                "bar_rows": len(bar_rows),
                "cuisine_rows": len(cuisine_rows),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
