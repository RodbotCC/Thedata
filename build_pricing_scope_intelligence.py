#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


DEFAULT_PHONE_LIBRARY_DIR = Path("/Users/jakeaaron/Comeketo/ComeketoData /phone_call_transcript_library")
DEFAULT_DEAL_SHEETS_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_deal_sheets.csv"
DEFAULT_CONVERSATION_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "lead_conversation_intelligence.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "pricing_scope_intelligence"

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
    "copyright",
    "the knot worldwide",
)

PRICE_QUESTION_RULES: List[Tuple[str, Sequence[str]]] = [
    ("per_person_pricing", (r"\bper person\b", r"\bprice per person\b", r"\bhow much.*per person\b")),
    ("price_difference", (r"\bprice difference\b", r"\bcost difference\b", r"\bhow much more\b", r"\bhow much less\b")),
    ("quote_request", (r"\bquote\b", r"\bpricing\b", r"\bprice list\b", r"\bcost\b", r"\bhow much\b", r"\bsoft clip quote\b")),
    ("minimum_or_small_event", (r"\bminimum\b", r"\btoo much food\b", r"\btoo small\b", r"\bonly \d+\s+(?:people|guests)\b")),
]

QUOTE_REVISION_RULES: List[Tuple[str, Sequence[str]]] = [
    ("revised_quote", (r"\brevised quote\b", r"\bupdate(?:d)? quote\b", r"\bnew quote\b", r"\badjust(?:ed)? quote\b", r"\brevise\b.*\bquote\b")),
    ("two_quote_options", (r"\btwo quotes?\b", r"\bgive me two quotes?\b", r"\bwith and without\b", r"\bquote .*drop[- ]off\b", r"\bquote .*staff\b")),
    ("side_by_side_package_pricing", (r"\bprice difference\b", r"\bwhat would it be if\b", r"\bhow much would that be\b", r"\bcompare\b.*\bpackage\b")),
]

BUDGET_PRESSURE_RULES: List[Tuple[str, Sequence[str]]] = [
    ("out_of_budget", (r"\bout of budget\b", r"\bnot in (?:our|my) budget\b", r"\bnot within (?:our|my) budget\b", r"\bdon'?t think .* within (?:our|my) budget\b")),
    ("too_expensive", (r"\btoo expensive\b", r"\bexpensive\b", r"\bhigh price\b")),
    ("budget_dropped", (r"\bfunding .* dropped\b", r"\bbudget .* dropped\b", r"\bsize vastly decreased\b")),
    ("price_fit_issue", (r"\bdoesn'?t fit\b.*\bbudget\b", r"\bdoesn'?t fit\b", r"\bdoesn'?t make sense\b.*\bcost\b", r"\bcost .* doesn'?t make sense\b")),
    ("minimum_or_travel_friction", (r"\bminimum\b", r"\btravel fee\b", r"\bdistance\b")),
]

SCOPE_EXPANSION_RULES: List[Tuple[str, Sequence[str]]] = [
    ("add_bar_program", (r"\badd (?:the )?bar\b", r"\bmobile bar\b", r"\bcash bar\b", r"\bopen bar\b", r"\bbartender\b")),
    ("add_menu_items", (r"\badd more food\b", r"\badd .*appetizer", r"\badd .*dessert", r"\bgrazing table\b", r"\bcharcuterie\b", r"\bfruit platter\b")),
    ("upgrade_or_premium", (r"\bupgrade\b", r"\bpremium\b", r"\b3rd\b", r"\bthird (?:meat|option|entree)\b", r"\bextra\b", r"\badd .* option\b")),
    ("service_add_on", (r"\bgrilling service\b", r"\bon[- ]site grill(?:ing)?\b", r"\btable side meat cutting\b", r"\bservers?\b", r"\bdinner clean[- ]?up\b")),
]

SCOPE_REDUCTION_RULES: List[Tuple[str, Sequence[str]]] = [
    ("drop_off_only", (r"\bdrop[- ]off only\b", r"\bsetup only\b", r"\bdrop off service\b")),
    ("remove_bar", (r"\bdon'?t need .*bar\b", r"\bwithout the wine\b", r"\bno bar\b", r"\bremove the bar\b")),
    ("reduce_guest_count_or_food", (r"\breduced the number\b", r"\bonly \d+\s+(?:people|guests)\b", r"\bjust \d+\s+(?:people|guests)\b", r"\bsmaller\b", r"\b12 people\b")),
    ("remove_service_or_scope", (r"\bdon'?t need .*staff\b", r"\bwithout staff\b", r"\bfood only\b", r"\bonly the food\b", r"\bjust drop it off\b")),
]

PACKAGE_COMPARE_RULES: List[Tuple[str, Sequence[str]]] = [
    ("package_comparison", (r"\bwhich package\b", r"\bfirst package\b", r"\bsecond package\b", r"\bpackage .* option\b", r"\bcompare\b.*\bpackage\b")),
    ("venue_or_other_vendor_package", (r"\btheir packages\b", r"\bvenue package\b", r"\bwhat they have for the package\b", r"\bchristalight\b")),
    ("with_without_comparison", (r"\bwith and without\b", r"\bwhat would it be if\b", r"\bprice difference\b")),
]

DEPOSIT_RULES: List[Tuple[str, Sequence[str]]] = [
    ("deposit_ready", (r"\bmake the deposit\b", r"\bsubmit the deposit\b", r"\b30% deposit\b", r"\b35% deposit\b")),
    ("contract_ready", (r"\bsign (?:the )?(?:contract|agreement)\b", r"\bsend .*contract\b", r"\blegal can sign\b")),
    ("date_secure_language", (r"\bsecure the date\b", r"\bready to secure\b", r"\block in the date\b")),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build pricing-and-scope intelligence from the normalized Comeketo library.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--deal-sheets-csv", type=Path, default=DEFAULT_DEAL_SHEETS_CSV)
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


def compact_text(value: Optional[str], limit: int = 320) -> str:
    text = " ".join((value or "").split())
    return text[:limit]


def dedupe_key(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def split_labels(value: Optional[str]) -> List[str]:
    labels: List[str] = []
    for part in (value or "").split("|"):
        label = part.strip()
        if label and label not in labels:
            labels.append(label)
    return labels


def pretty_label(value: str) -> str:
    return value.replace("_", " ")


def is_noise_text(text: str) -> bool:
    lowered = text.lower().strip()
    if not lowered:
        return True
    return any(pattern in lowered for pattern in NOISE_PATTERNS)


def normalize_matching_text(text: str) -> str:
    return (
        text.replace("’", "'")
        .replace("‘", "'")
        .replace("“", '"')
        .replace("”", '"')
        .replace("–", "-")
        .replace("—", "-")
    )


def sort_items_by_time(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(items, key=lambda item: item.get("event_datetime_utc") or "", reverse=True)


def build_text_rows(conversation_payload: Dict[str, Any], deal_row: Dict[str, str]) -> List[Dict[str, Any]]:
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
        ("deal_budget", "", deal_row.get("budget_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("deal_payment", "", deal_row.get("payment_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("deal_service", "", deal_row.get("service_style_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("deal_bar", "", deal_row.get("bar_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("deal_decision", "", deal_row.get("decision_evidence") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("deal_open_loop", "", deal_row.get("top_open_loop_text") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
        ("deal_operator_move", "", deal_row.get("operator_move") or "", deal_row.get("event_datetime_utc") or "", deal_row.get("deal_sheet_path") or ""),
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


def match_labels_for_text(text: str, rules: Sequence[Tuple[str, Sequence[str]]]) -> List[str]:
    labels: List[str] = []
    normalized_text = normalize_matching_text(text)
    for label, patterns in rules:
        if any(
            re.search(pattern, text, flags=re.IGNORECASE) or re.search(pattern, normalized_text, flags=re.IGNORECASE)
            for pattern in patterns
        ):
            labels.append(label)
    return labels


def collect_matches(
    text_rows: Sequence[Dict[str, Any]],
    rules: Sequence[Tuple[str, Sequence[str]]],
    allowed_sections: Optional[Set[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[str], Dict[str, str]]:
    labels: List[str] = []
    evidence: Dict[str, str] = {}
    matches: List[Dict[str, Any]] = []
    seen_rows: Dict[str, Dict[str, Any]] = {}

    for row in text_rows:
        if allowed_sections and row.get("section") not in allowed_sections:
            continue
        text = row.get("text") or ""
        matched_labels = match_labels_for_text(text, rules)
        if not matched_labels:
            continue

        row_key = dedupe_key(text)
        existing = seen_rows.get(row_key)
        if not existing:
            existing = dict(row)
            existing["matched_labels"] = []
            seen_rows[row_key] = existing
            matches.append(existing)

        for label in matched_labels:
            if label not in labels:
                labels.append(label)
            evidence.setdefault(label, compact_text(text, 500))
            if label not in existing["matched_labels"]:
                existing["matched_labels"].append(label)

    return matches, labels, evidence


def first_text(rows: Sequence[Dict[str, Any]]) -> str:
    return rows[0]["text"] if rows else ""


def first_evidence(evidence_map: Dict[str, str], labels: Sequence[str]) -> str:
    for label in labels:
        if evidence_map.get(label):
            return evidence_map[label]
    return ""


def build_signal_lines(rows: Sequence[Dict[str, Any]], limit: int = 5) -> List[str]:
    lines: List[str] = []
    seen = set()
    for row in rows:
        text = row.get("text") or ""
        key = dedupe_key(text)
        if key in seen:
            continue
        seen.add(key)
        label_text = ", ".join(pretty_label(label) for label in row.get("matched_labels") or [])
        lines.append(f"[{label_text}] {text}" if label_text else text)
        if len(lines) >= limit:
            break
    return lines


def pricing_signal_score(
    pricing_question_count: int,
    quote_revision_count: int,
    budget_pressure_count: int,
    scope_expansion_count: int,
    scope_reduction_count: int,
    package_compare_count: int,
    deposit_signal_count: int,
) -> int:
    return (
        pricing_question_count * 2
        + quote_revision_count * 3
        + budget_pressure_count * 4
        + scope_expansion_count * 2
        + scope_reduction_count * 2
        + package_compare_count * 2
        + deposit_signal_count
    )


def classify_pricing_posture(
    stage_type: str,
    budget_pressure_count: int,
    scope_reduction_count: int,
    quote_revision_count: int,
    pricing_question_count: int,
    deposit_signal_count: int,
    package_compare_count: int,
) -> str:
    if stage_type == "lost":
        return "closed_lost"
    if deposit_signal_count and not budget_pressure_count:
        return "price_accepted_contracting"
    if budget_pressure_count and scope_reduction_count:
        return "budget_constrained_scope_reduction"
    if budget_pressure_count:
        return "budget_pressure"
    if quote_revision_count or package_compare_count:
        return "revision_or_comparison_active"
    if pricing_question_count:
        return "pricing_discovery"
    return "low_pricing_signal"


def classify_scope_posture(scope_expansion_count: int, scope_reduction_count: int) -> str:
    if scope_expansion_count and scope_reduction_count:
        return "scope_in_flux"
    if scope_reduction_count:
        return "scope_reduction"
    if scope_expansion_count:
        return "expansion_opportunity"
    return "stable_scope"


def suggest_pricing_action(
    stage_type: str,
    budget_pressure_count: int,
    scope_expansion_count: int,
    scope_reduction_count: int,
    quote_revision_count: int,
    pricing_question_count: int,
    package_compare_count: int,
    deposit_signal_count: int,
    operator_move: str,
) -> str:
    if stage_type == "lost":
        return "Closed-lost; keep the pricing history for learning only."
    if deposit_signal_count and not budget_pressure_count:
        return "Lock the accepted scope, send the contract/deposit path, and close the booking step."
    if budget_pressure_count and scope_reduction_count:
        return "Send a stripped-down revision with explicit savings for each scope cut."
    if budget_pressure_count and scope_expansion_count:
        return "Send tiered options so the client can see what each add-on costs and what can be removed."
    if quote_revision_count or package_compare_count:
        return "Send a side-by-side revised quote with clear price deltas between the options."
    if scope_expansion_count:
        return "Price the add-ons separately so the buyer can say yes in pieces instead of all at once."
    if pricing_question_count:
        return "Answer the pricing questions directly and close with a concrete decision window."
    return operator_move or "Confirm whether pricing or scope needs to change before the next follow-up."


def summary_line(
    lead_name: str,
    pricing_posture: str,
    scope_posture: str,
    pricing_question_labels: Sequence[str],
    budget_pressure_labels: Sequence[str],
    scope_expansion_labels: Sequence[str],
    scope_reduction_labels: Sequence[str],
    package_compare_labels: Sequence[str],
    deposit_labels: Sequence[str],
) -> str:
    pricing_text = " / ".join(pretty_label(label) for label in pricing_question_labels[:3]) if pricing_question_labels else "light pricing discovery"
    budget_text = " / ".join(pretty_label(label) for label in budget_pressure_labels[:3]) if budget_pressure_labels else "no clear budget pressure"
    expansion_text = " / ".join(pretty_label(label) for label in scope_expansion_labels[:3]) if scope_expansion_labels else "no clear add-on push"
    reduction_text = " / ".join(pretty_label(label) for label in scope_reduction_labels[:3]) if scope_reduction_labels else "no clear scope reduction"
    compare_text = " / ".join(pretty_label(label) for label in package_compare_labels[:2]) if package_compare_labels else "no explicit package comparison"
    deposit_text = " / ".join(pretty_label(label) for label in deposit_labels[:2]) if deposit_labels else "no contract readiness signal"
    return compact_text(
        f"{lead_name}: pricing posture {pretty_label(pricing_posture)}; scope posture {pretty_label(scope_posture)}; "
        f"pricing asks {pricing_text}; budget {budget_text}; expansion {expansion_text}; reduction {reduction_text}; "
        f"comparison {compare_text}; contract {deposit_text}.",
        420,
    )


def sort_key(row: Dict[str, Any]) -> Tuple[int, int, int, datetime]:
    dt = parse_iso(row.get("event_datetime_utc"))
    fallback_dt = datetime.max.replace(tzinfo=timezone.utc)
    return (
        -int(row.get("priority_score") or 0),
        -int(row.get("pricing_signal_score") or 0),
        -(int(row.get("quote_revision_count") or 0) + int(row.get("budget_pressure_count") or 0) + int(row.get("scope_expansion_count") or 0) + int(row.get("scope_reduction_count") or 0)),
        dt or fallback_dt,
    )


def build_profile_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        f"# Pricing / Scope Sheet: {payload.get('lead_name') or ''}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Pipeline / Stage: {payload.get('pipeline_name') or ''} / {payload.get('stage_label') or ''}",
        f"- Opportunity: {payload.get('current_opportunity_title') or ''}",
        f"- Value: {payload.get('value_formatted') or ''}",
        f"- Priority Score: `{payload.get('priority_score') or ''}`",
        f"- Pricing Signal Score: `{payload.get('pricing_signal_score') or ''}`",
        f"- Pricing Posture: {pretty_label(payload.get('pricing_posture') or '')}",
        f"- Scope Posture: {pretty_label(payload.get('scope_posture') or '')}",
        f"- Event Date (UTC): `{payload.get('event_datetime_utc') or ''}`",
        "",
        "## Counts",
        f"- Pricing Questions: `{payload.get('pricing_question_count', 0)}`",
        f"- Quote Revision Signals: `{payload.get('quote_revision_count', 0)}`",
        f"- Budget Pressure Signals: `{payload.get('budget_pressure_count', 0)}`",
        f"- Scope Expansion Signals: `{payload.get('scope_expansion_count', 0)}`",
        f"- Scope Reduction Signals: `{payload.get('scope_reduction_count', 0)}`",
        f"- Package Comparison Signals: `{payload.get('package_compare_count', 0)}`",
        f"- Deposit / Contract Signals: `{payload.get('deposit_signal_count', 0)}`",
        "",
        "## Recommended Move",
        f"- {payload.get('pricing_action') or ''}",
        "",
        "## Top Signals",
        f"- Top Pricing Question: {payload.get('top_pricing_question') or 'None.'}",
        f"- Top Budget Pressure: {payload.get('top_budget_pressure') or 'None.'}",
        f"- Top Scope Change: {payload.get('top_scope_change') or 'None.'}",
        f"- Top Upsell Opening: {payload.get('top_upsell_opening') or 'None.'}",
        f"- Top Package Comparison: {payload.get('top_package_compare') or 'None.'}",
        f"- Top Contract Signal: {payload.get('top_contract_signal') or 'None.'}",
        "",
        "## Pricing Questions",
    ]
    pricing_lines = payload.get("pricing_question_lines") or []
    if pricing_lines:
        for line in pricing_lines:
            lines.append(f"- {line}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Budget Pressure"])
    budget_lines = payload.get("budget_pressure_lines") or []
    if budget_lines:
        for line in budget_lines:
            lines.append(f"- {line}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Scope Expansion / Upsell Openings"])
    expansion_lines = payload.get("scope_expansion_lines") or []
    if expansion_lines:
        for line in expansion_lines:
            lines.append(f"- {line}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Scope Reduction"])
    reduction_lines = payload.get("scope_reduction_lines") or []
    if reduction_lines:
        for line in reduction_lines:
            lines.append(f"- {line}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Package Comparison"])
    package_lines = payload.get("package_compare_lines") or []
    if package_lines:
        for line in package_lines:
            lines.append(f"- {line}")
    else:
        lines.append("- None.")

    lines.extend(["", "## Contract / Deposit Readiness"])
    deposit_lines = payload.get("deposit_signal_lines") or []
    if deposit_lines:
        for line in deposit_lines:
            lines.append(f"- {line}")
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "## Summary",
            f"- {payload.get('pricing_scope_summary') or ''}",
            "",
        ]
    )
    return "\n".join(lines)


def build_board_markdown(title: str, rows: Sequence[Dict[str, Any]], count_label: str, column: str) -> str:
    lines = [f"# {title}", "", f"- Total {count_label}: `{len(rows)}`", "", "## Top Rows"]
    for row in rows[:120]:
        primary_text = (
            row.get(column)
            or row.get("pricing_action")
            or row.get("pricing_scope_summary")
            or row.get("top_pricing_question")
            or row.get("top_budget_pressure")
            or row.get("top_scope_change")
            or "No explicit pricing / scope signal captured."
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

    lines = ["# Pricing Signal Rollup", ""]
    for dimension in sorted(grouped.keys()):
        lines.append(f"## {dimension}")
        for row in grouped[dimension]:
            lines.append(f"- {pretty_label(row['label'])}: {row['count']}")
        lines.append("")
    return "\n".join(lines)


def build_readme(
    profiles: Sequence[Dict[str, Any]],
    pricing_action_rows: Sequence[Dict[str, Any]],
    budget_rows: Sequence[Dict[str, Any]],
    quote_revision_rows: Sequence[Dict[str, Any]],
    scope_change_rows: Sequence[Dict[str, Any]],
    upsell_rows: Sequence[Dict[str, Any]],
    package_compare_rows: Sequence[Dict[str, Any]],
) -> str:
    lines = [
        "# Pricing / Scope Intelligence",
        "",
        "This layer compresses commercial friction and expansion signals into a pricing-and-scope working sheet: quote revisions, budget pressure, scope cuts, upsell openings, package comparisons, and contract readiness.",
        "",
        "## Snapshot",
        f"- Lead pricing / scope profiles: `{len(profiles)}`",
        f"- Pricing action rows: `{len(pricing_action_rows)}`",
        f"- Budget-pressure rows: `{len(budget_rows)}`",
        f"- Quote-revision rows: `{len(quote_revision_rows)}`",
        f"- Scope-change rows: `{len(scope_change_rows)}`",
        f"- Upsell-opening rows: `{len(upsell_rows)}`",
        f"- Package-comparison rows: `{len(package_compare_rows)}`",
        "",
        "## Key Files",
        "- `pricing_action_board.md`: the best operator scan for pricing or scope movement",
        "- `budget_pressure_board.md`: active leads where budget, minimums, travel fees, or other price-fit friction is blocking movement",
        "- `quote_revision_board.md`: leads asking for revised quotes, side-by-side options, or clearer deltas",
        "- `scope_change_board.md`: leads where the requested scope is still changing",
        "- `upsell_opening_board.md`: active leads where the buyer is signaling expansion or add-ons",
        "- `package_compare_board.md`: active leads comparing packages or side-by-side options",
        "- `pricing_signal_rollup.md`: global rollup across pricing, budget, scope, comparison, and deposit signals",
        "- `../normalized/lead_pricing_scope_profiles.csv`: machine-friendly one-row-per-lead pricing/scope profile",
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

    full_payloads: List[Dict[str, Any]] = []
    profile_rows: List[Dict[str, Any]] = []

    for deal_row in deal_rows:
        lead_id = deal_row["lead_id"]
        lead_dir = Path(deal_row["deal_sheet_path"]).parent
        convo_row = conversation_rows.get(lead_id, {})
        convo_json_path = Path(convo_row.get("conversation_intelligence_json_path") or lead_dir / "lead_conversation_intelligence.json")
        conversation_payload = load_json(convo_json_path)
        text_rows = build_text_rows(conversation_payload, deal_row)

        pricing_question_rows, pricing_question_labels, pricing_question_evidence_map = collect_matches(
            text_rows,
            PRICE_QUESTION_RULES,
            allowed_sections={"buyer_asks", "open_loops"},
        )
        quote_revision_rows, quote_revision_labels, quote_revision_evidence_map = collect_matches(
            text_rows,
            QUOTE_REVISION_RULES,
            allowed_sections={"buyer_asks", "preferences", "open_loops", "sales_commitments", "deal_budget", "deal_payment"},
        )
        budget_pressure_rows, budget_pressure_labels, budget_pressure_evidence_map = collect_matches(
            text_rows,
            BUDGET_PRESSURE_RULES,
            allowed_sections={"buyer_asks", "blockers", "preferences", "open_loops", "deal_budget"},
        )
        scope_expansion_rows, scope_expansion_labels, scope_expansion_evidence_map = collect_matches(
            text_rows,
            SCOPE_EXPANSION_RULES,
            allowed_sections={"buyer_asks", "preferences", "blockers", "open_loops", "deal_service", "deal_bar"},
        )
        scope_reduction_rows, scope_reduction_labels, scope_reduction_evidence_map = collect_matches(
            text_rows,
            SCOPE_REDUCTION_RULES,
            allowed_sections={"buyer_asks", "preferences", "blockers", "open_loops", "deal_service", "deal_bar"},
        )
        package_compare_rows, package_compare_labels, package_compare_evidence_map = collect_matches(
            text_rows,
            PACKAGE_COMPARE_RULES,
            allowed_sections={"buyer_asks", "preferences", "blockers", "open_loops", "deal_budget"},
        )
        deposit_signal_rows, deposit_labels, deposit_evidence_map = collect_matches(
            text_rows,
            DEPOSIT_RULES,
            allowed_sections={"buyer_asks", "sales_commitments", "open_loops", "deal_payment"},
        )

        priority_score = max(int(deal_row.get("readiness_score") or 0), int(deal_row.get("follow_up_priority_score") or 0))
        score = pricing_signal_score(
            pricing_question_count=len(pricing_question_rows),
            quote_revision_count=len(quote_revision_rows),
            budget_pressure_count=len(budget_pressure_rows),
            scope_expansion_count=len(scope_expansion_rows),
            scope_reduction_count=len(scope_reduction_rows),
            package_compare_count=len(package_compare_rows),
            deposit_signal_count=len(deposit_signal_rows),
        )
        pricing_posture = classify_pricing_posture(
            stage_type=deal_row.get("stage_type") or "",
            budget_pressure_count=len(budget_pressure_rows),
            scope_reduction_count=len(scope_reduction_rows),
            quote_revision_count=len(quote_revision_rows),
            pricing_question_count=len(pricing_question_rows),
            deposit_signal_count=len(deposit_signal_rows),
            package_compare_count=len(package_compare_rows),
        )
        scope_posture = classify_scope_posture(
            scope_expansion_count=len(scope_expansion_rows),
            scope_reduction_count=len(scope_reduction_rows),
        )
        pricing_action = suggest_pricing_action(
            stage_type=deal_row.get("stage_type") or "",
            budget_pressure_count=len(budget_pressure_rows),
            scope_expansion_count=len(scope_expansion_rows),
            scope_reduction_count=len(scope_reduction_rows),
            quote_revision_count=len(quote_revision_rows),
            pricing_question_count=len(pricing_question_rows),
            package_compare_count=len(package_compare_rows),
            deposit_signal_count=len(deposit_signal_rows),
            operator_move=deal_row.get("operator_move") or "",
        )

        pricing_question_lines = build_signal_lines(pricing_question_rows)
        budget_pressure_lines = build_signal_lines(budget_pressure_rows)
        scope_expansion_lines = build_signal_lines(scope_expansion_rows)
        scope_reduction_lines = build_signal_lines(scope_reduction_rows)
        package_compare_lines = build_signal_lines(package_compare_rows)
        deposit_signal_lines = build_signal_lines(deposit_signal_rows)

        profile_row: Dict[str, Any] = {
            "lead_id": lead_id,
            "lead_name": deal_row.get("lead_name") or "",
            "lead_owner_name": deal_row.get("lead_owner_name") or "",
            "pipeline_name": deal_row.get("pipeline_name") or "",
            "stage_label": deal_row.get("stage_label") or "",
            "stage_type": deal_row.get("stage_type") or "",
            "current_opportunity_title": deal_row.get("current_opportunity_title") or "",
            "value_formatted": deal_row.get("value_formatted") or "",
            "event_datetime_utc": deal_row.get("event_datetime_utc") or "",
            "priority_score": priority_score,
            "pricing_signal_score": score,
            "pricing_posture": pricing_posture,
            "scope_posture": scope_posture,
            "pricing_question_count": len(pricing_question_rows),
            "quote_revision_count": len(quote_revision_rows),
            "budget_pressure_count": len(budget_pressure_rows),
            "scope_expansion_count": len(scope_expansion_rows),
            "scope_reduction_count": len(scope_reduction_rows),
            "package_compare_count": len(package_compare_rows),
            "deposit_signal_count": len(deposit_signal_rows),
            "top_pricing_question": first_text(pricing_question_rows),
            "top_budget_pressure": first_text(budget_pressure_rows),
            "top_scope_change": first_text(scope_reduction_rows) or first_text(scope_expansion_rows),
            "top_upsell_opening": first_text(scope_expansion_rows),
            "top_package_compare": first_text(package_compare_rows),
            "top_contract_signal": first_text(deposit_signal_rows),
            "pricing_question_labels": " | ".join(pricing_question_labels),
            "quote_revision_labels": " | ".join(quote_revision_labels),
            "budget_pressure_labels": " | ".join(budget_pressure_labels),
            "scope_expansion_labels": " | ".join(scope_expansion_labels),
            "scope_reduction_labels": " | ".join(scope_reduction_labels),
            "package_compare_labels": " | ".join(package_compare_labels),
            "deposit_labels": " | ".join(deposit_labels),
            "pricing_question_evidence": first_evidence(pricing_question_evidence_map, pricing_question_labels),
            "quote_revision_evidence": first_evidence(quote_revision_evidence_map, quote_revision_labels),
            "budget_pressure_evidence": first_evidence(budget_pressure_evidence_map, budget_pressure_labels),
            "scope_expansion_evidence": first_evidence(scope_expansion_evidence_map, scope_expansion_labels),
            "scope_reduction_evidence": first_evidence(scope_reduction_evidence_map, scope_reduction_labels),
            "package_compare_evidence": first_evidence(package_compare_evidence_map, package_compare_labels),
            "deposit_evidence": first_evidence(deposit_evidence_map, deposit_labels),
            "pricing_action": pricing_action,
            "pricing_scope_summary": summary_line(
                lead_name=deal_row.get("lead_name") or "",
                pricing_posture=pricing_posture,
                scope_posture=scope_posture,
                pricing_question_labels=pricing_question_labels,
                budget_pressure_labels=budget_pressure_labels,
                scope_expansion_labels=scope_expansion_labels,
                scope_reduction_labels=scope_reduction_labels,
                package_compare_labels=package_compare_labels,
                deposit_labels=deposit_labels,
            ),
            "lead_pricing_scope_sheet_path": str(lead_dir / "lead_pricing_scope_sheet.md"),
            "lead_pricing_scope_sheet_json_path": str(lead_dir / "lead_pricing_scope_sheet.json"),
            "deal_sheet_path": deal_row.get("deal_sheet_path") or "",
            "conversation_path": convo_row.get("conversation_intelligence_path") or deal_row.get("conversation_path") or "",
            "conversation_json_path": str(convo_json_path),
        }

        payload = dict(profile_row)
        payload.update(
            {
                "pricing_question_lines": pricing_question_lines,
                "budget_pressure_lines": budget_pressure_lines,
                "scope_expansion_lines": scope_expansion_lines,
                "scope_reduction_lines": scope_reduction_lines,
                "package_compare_lines": package_compare_lines,
                "deposit_signal_lines": deposit_signal_lines,
                "pricing_questions": pricing_question_rows,
                "budget_pressure_signals": budget_pressure_rows,
                "scope_expansion_signals": scope_expansion_rows,
                "scope_reduction_signals": scope_reduction_rows,
                "package_compare_signals": package_compare_rows,
                "deposit_signals": deposit_signal_rows,
            }
        )

        write_json(lead_dir / "lead_pricing_scope_sheet.json", payload)
        (lead_dir / "lead_pricing_scope_sheet.md").write_text(build_profile_markdown(payload), encoding="utf-8")

        full_payloads.append(payload)
        profile_rows.append(profile_row)

    profile_rows = sorted(profile_rows, key=sort_key)
    active_rows = [row for row in profile_rows if row.get("stage_type") != "lost"]

    pricing_action_rows = [
        row
        for row in active_rows
        if any(
            int(row.get(field) or 0) > 0
            for field in (
                "pricing_question_count",
                "quote_revision_count",
                "budget_pressure_count",
                "scope_expansion_count",
                "scope_reduction_count",
                "package_compare_count",
                "deposit_signal_count",
            )
        )
    ]
    budget_rows = [row for row in active_rows if int(row.get("budget_pressure_count") or 0) > 0]
    quote_revision_rows = [
        row
        for row in active_rows
        if int(row.get("quote_revision_count") or 0) > 0 or int(row.get("pricing_question_count") or 0) > 0
    ]
    scope_change_rows = [
        row
        for row in active_rows
        if int(row.get("scope_expansion_count") or 0) > 0 or int(row.get("scope_reduction_count") or 0) > 0
    ]
    upsell_rows = [row for row in active_rows if int(row.get("scope_expansion_count") or 0) > 0]
    package_compare_rows = [row for row in active_rows if int(row.get("package_compare_count") or 0) > 0]

    rollup_rows: List[Dict[str, Any]] = []
    for dimension in (
        "pricing_question_labels",
        "quote_revision_labels",
        "budget_pressure_labels",
        "scope_expansion_labels",
        "scope_reduction_labels",
        "package_compare_labels",
        "deposit_labels",
    ):
        counter = Counter()
        for row in active_rows:
            for label in split_labels(row.get(dimension) or ""):
                counter[label] += 1
        for label, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
            rollup_rows.append({"dimension": dimension, "label": label, "count": count})

    write_csv(normalized_dir / "lead_pricing_scope_profiles.csv", profile_rows)
    write_jsonl(normalized_dir / "lead_pricing_scope_profiles.jsonl", profile_rows)
    write_csv(normalized_dir / "pricing_action_board.csv", pricing_action_rows)
    write_jsonl(normalized_dir / "pricing_action_board.jsonl", pricing_action_rows)
    write_csv(normalized_dir / "budget_pressure_board.csv", budget_rows)
    write_jsonl(normalized_dir / "budget_pressure_board.jsonl", budget_rows)
    write_csv(normalized_dir / "quote_revision_board.csv", quote_revision_rows)
    write_jsonl(normalized_dir / "quote_revision_board.jsonl", quote_revision_rows)
    write_csv(normalized_dir / "scope_change_board.csv", scope_change_rows)
    write_jsonl(normalized_dir / "scope_change_board.jsonl", scope_change_rows)
    write_csv(normalized_dir / "upsell_opening_board.csv", upsell_rows)
    write_jsonl(normalized_dir / "upsell_opening_board.jsonl", upsell_rows)
    write_csv(normalized_dir / "package_compare_board.csv", package_compare_rows)
    write_jsonl(normalized_dir / "package_compare_board.jsonl", package_compare_rows)
    write_csv(normalized_dir / "pricing_signal_rollup.csv", rollup_rows)

    (output_dir / "README.md").write_text(
        build_readme(profile_rows, pricing_action_rows, budget_rows, quote_revision_rows, scope_change_rows, upsell_rows, package_compare_rows),
        encoding="utf-8",
    )
    (output_dir / "pricing_action_board.md").write_text(
        build_board_markdown("Pricing Action Board", pricing_action_rows, "pricing-action rows", "pricing_action"),
        encoding="utf-8",
    )
    (output_dir / "budget_pressure_board.md").write_text(
        build_board_markdown("Budget / Pricing Friction Board", budget_rows, "budget / pricing-friction rows", "top_budget_pressure"),
        encoding="utf-8",
    )
    (output_dir / "quote_revision_board.md").write_text(
        build_board_markdown("Quote Revision Board", quote_revision_rows, "quote-revision rows", "top_pricing_question"),
        encoding="utf-8",
    )
    (output_dir / "scope_change_board.md").write_text(
        build_board_markdown("Scope Change Board", scope_change_rows, "scope-change rows", "top_scope_change"),
        encoding="utf-8",
    )
    (output_dir / "upsell_opening_board.md").write_text(
        build_board_markdown("Upsell Opening Board", upsell_rows, "upsell-opening rows", "top_upsell_opening"),
        encoding="utf-8",
    )
    (output_dir / "package_compare_board.md").write_text(
        build_board_markdown("Package Compare Board", package_compare_rows, "package-compare rows", "top_package_compare"),
        encoding="utf-8",
    )
    (output_dir / "pricing_signal_rollup.md").write_text(build_rollup_markdown(rollup_rows), encoding="utf-8")

    print(
        json.dumps(
            {
                "profiles": len(profile_rows),
                "pricing_action_rows": len(pricing_action_rows),
                "budget_rows": len(budget_rows),
                "quote_revision_rows": len(quote_revision_rows),
                "scope_change_rows": len(scope_change_rows),
                "upsell_rows": len(upsell_rows),
                "package_compare_rows": len(package_compare_rows),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
