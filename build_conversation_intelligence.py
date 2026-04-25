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
DEFAULT_COMMUNICATIONS_JSONL = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "live_phone_call_lead_communications.jsonl"
DEFAULT_FOLLOW_UP_CSV = DEFAULT_PHONE_LIBRARY_DIR / "normalized" / "follow_up_queue.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_PHONE_LIBRARY_DIR / "conversation_intelligence"

ASK_PATTERNS: Dict[str, Sequence[str]] = {
    "quote_revision": (
        r"\b(can|could|would)\s+you\b.*\b(send|revise|update|adjust)\b.*\bquote\b",
        r"\bnew quote\b",
        r"\bupdated quote\b",
        r"\brevised quote\b",
        r"\bcan you send a quote\b",
    ),
    "pricing": (
        r"\bhow much\b",
        r"\bwhat is the (?:fee|cost|price)\b",
        r"\bprice\b",
        r"\bcost\b",
        r"\bdeposit\b",
        r"\bbudget\b",
        r"\bupgrade\b",
    ),
    "menu_selection": (
        r"\bchoice of entrees\b",
        r"\bchoice of sides\b",
        r"\bwhat (?:the )?choices are\b",
        r"\bwhat meats\b",
        r"\bwhat sides\b",
        r"\bmenu\b",
        r"\bappetizer\b",
        r"\bentrees?\b",
        r"\bsides?\b",
    ),
    "tasting": (
        r"\btasting\b",
        r"\binvitation\b",
        r"\bregistration\b",
        r"\breserve your spot\b",
    ),
    "bar_service": (
        r"\bcash bar\b",
        r"\bopen bar\b",
        r"\bmobile bar\b",
        r"\bdrinks?\b",
        r"\bbar service\b",
    ),
    "logistics": (
        r"\bvenue\b",
        r"\baddress\b",
        r"\bsetup\b",
        r"\bstaff\b",
        r"\bdrop[- ]off\b",
        r"\btravel fee\b",
        r"\bdistance\b",
    ),
    "availability": (
        r"\bwhen\b",
        r"\bwhat date\b",
        r"\bwhat time\b",
        r"\bavailable\b",
        r"\bavailability\b",
        r"\bdate\b",
    ),
    "guest_count_scope": (
        r"\bhow many\b",
        r"\bguests?\b",
        r"\bpeople\b",
        r"\btoo small\b",
        r"\bminimum\b",
    ),
}

BLOCKER_PATTERNS: Dict[str, Sequence[str]] = {
    "pricing_pressure": (
        r"\bprice\b",
        r"\bcost\b",
        r"\bbudget\b",
        r"\bdeposit\b",
        r"\bupgrade\b",
        r"\bafford\b",
        r"\btoo expensive\b",
    ),
    "guest_count_or_minimum": (
        r"\btoo small\b",
        r"\breduced the number\b",
        r"\bonly be about\b",
        r"\bonly about \d+\b",
        r"\bminimum\b",
    ),
    "schedule_or_availability": (
        r"\bcan(?:not|'t)\s+(?:find|make)\b",
        r"\bcan(?:not|'t) attend\b",
        r"\blimited vacation\b",
        r"\bnot sure\b.*\bdate\b",
        r"\bwhen is the tasting\b",
        r"\bavailability\b",
    ),
    "decision_dependency": (
        r"\breview with\b",
        r"\btalk(?:ing)? to\b",
        r"\bcheck with\b",
        r"\bmy son\b",
        r"\bmy daughter\b",
        r"\bfianc(?:e|ee|é)\b",
        r"\bpartner\b",
        r"\bhusband\b",
        r"\bwife\b",
    ),
    "scope_change": (
        r"\bthere have been a few changes\b",
        r"\breduced the number\b",
        r"\bdate is now\b",
        r"\bdon'?t need the bar service\b",
        r"\bdon'?t need .*staff\b",
        r"\bdrop[- ]off only\b",
        r"\bsetup only\b",
    ),
    "logistics_or_venue": (
        r"\bvenue address\b",
        r"\btravel fee\b",
        r"\bdistance\b",
        r"\bsetup\b",
        r"\bstaff\b",
        r"\bdrop[- ]off\b",
        r"\bvenue\b",
    ),
    "missing_information": (
        r"\bcan(?:not|'t) find the invitation\b",
        r"\bdid(?:n't| not) receive\b",
        r"\bwhat are the choices\b",
        r"\bchoice of entrees\b",
        r"\bchoice of sides\b",
    ),
}

PREFERENCE_PATTERNS: Dict[str, Sequence[str]] = {
    "drop_off_only": (
        r"\bdrop[- ]off only\b",
        r"\bsetup only\b",
        r"\bdon'?t need .*staff\b",
    ),
    "no_bar_service": (
        r"\bdon'?t need the bar service\b",
        r"\bno bar\b",
    ),
    "bar_interest": (
        r"\bcash bar\b",
        r"\bmobile bar\b",
        r"\bopen bar\b",
        r"\bbar service\b",
    ),
    "menu_customization": (
        r"\bchoice of entrees\b",
        r"\bchoice of sides\b",
        r"\bmenu\b",
        r"\bappetizer\b",
        r"\bpremium\b",
    ),
    "guest_count_change": (
        r"\breduced the number\b",
        r"\bonly be about\b",
        r"\bfor \d+ guests\b",
        r"\bfor \d+ people\b",
    ),
    "venue_or_service_setup": (
        r"\bvenue address\b",
        r"\btravel fee\b",
        r"\bsetup\b",
        r"\bbackyard\b",
        r"\bhome wedding\b",
        r"\bhome\b",
    ),
    "family_decision_process": (
        r"\breview with\b",
        r"\bmy son\b",
        r"\bmy daughter\b",
        r"\bfianc(?:e|ee|é)\b",
        r"\bpartner\b",
    ),
    "travel_or_tasting_constraint": (
        r"\bwest virginia\b",
        r"\blimited vacation\b",
        r"\bcan(?:not|'t) attend a tasting\b",
        r"\bon behalf of the couple\b",
    ),
}

COMMITMENT_PATTERNS: Dict[str, Sequence[str]] = {
    "send_quote": (
        r"\b(i am|i'm|i will|i'll|let me|we will|we'll)\b.*\b(send|create|prepare)\b.*\bquote\b",
        r"\btony will create\b.*\bquote\b",
        r"\bcommitted to\b.*\bquote\b",
    ),
    "send_menu": (
        r"\b(i am|i'm|i will|i'll|let me|we will|we'll)\b.*\b(send)\b.*\b(menu|menus|appetizer)\b",
        r"\bcommitted to sending\b.*\b(menu|menus|appetizer)\b",
    ),
    "send_tasting_details": (
        r"\b(i am|i'm|i will|i'll|let me|we will|we'll)\b.*\b(send|invite|share)\b.*\b(tasting|registration|invitation)\b",
    ),
    "confirm_logistics": (
        r"\b(i am|i'm|i will|i'll|let me|we will|we'll)\b.*\b(calculate|confirm|revise)\b.*\b(travel fee|venue|address|setup)\b",
    ),
    "follow_up": (
        r"\b(i am|i'm|i will|i'll|we will|we'll)\b.*\b(follow up|reach out|call you)\b",
    ),
}

FULFILLMENT_TOKENS: Dict[str, Sequence[str]] = {
    "send_quote": ("quote", "attached", "updated quote", "revised quote", "sample quotes"),
    "send_menu": ("menu", "menus", "appetizer", "options"),
    "send_tasting_details": ("tasting", "registration", "invitation", "reserve your spot"),
    "confirm_logistics": ("travel fee", "venue address", "setup only", "drop-off"),
    "follow_up": (),
}

SENTENCE_SPLIT_RE = re.compile(r"(?<=[?.!])\s+|\n+")
CALL_LINE_RE = re.compile(r"^\[(?P<ts>[^\]]+)\]\s+(?P<speaker>.+?)\s+\((?P<role>[^)]+)\):\s*(?P<text>.+)$")
NOISE_PATTERNS = (
    "privacy policy",
    "facebook",
    "instagram",
    "website",
    "direct line",
    "catering main",
    "copyright",
    "the knot worldwide",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build buyer asks / blockers / commitments layers from the normalized Comeketo library.")
    parser.add_argument("--phone-library-dir", type=Path, default=DEFAULT_PHONE_LIBRARY_DIR)
    parser.add_argument("--lead-briefs-csv", type=Path, default=DEFAULT_LEAD_BRIEFS_CSV)
    parser.add_argument("--communications-jsonl", type=Path, default=DEFAULT_COMMUNICATIONS_JSONL)
    parser.add_argument("--follow-up-csv", type=Path, default=DEFAULT_FOLLOW_UP_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_jsonl_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            rows.append(json.loads(stripped))
    return rows


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


def compact_text(value: Optional[str], limit: int = 220) -> str:
    text = " ".join((value or "").split())
    return text[:limit]


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return text or "unknown"


def dedupe_key(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def clean_markdown_text(value: str) -> str:
    text = re.sub(r"!\[[^\]]*\]\([^)]+\)", " ", value or "")
    text = re.sub(r"\[[^\]]+\]\([^)]+\)", " ", text)
    text = text.replace("**", " ").replace("__", " ")
    return " ".join(text.split())


def read_markdown_section(path: Path, heading: str) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8").splitlines()
    output: List[str] = []
    inside = False
    heading_line = f"## {heading}"
    for line in lines:
        if line.strip() == heading_line:
            inside = True
            continue
        if inside and line.startswith("## "):
            break
        if inside:
            output.append(line)
    return "\n".join(output).strip()


def strip_quoted_history(body: str) -> str:
    if not body:
        return ""
    kept: List[str] = []
    for line in body.splitlines():
        stripped = line.strip()
        lowered = stripped.lower()
        if stripped.startswith(">"):
            break
        if re.match(r"^On .+", stripped):
            break
        if re.match(r"^From:\s+.+", stripped):
            break
        if stripped == "-----Original Message-----":
            break
        if "forwarded message" in lowered and stripped.startswith("-"):
            break
        kept.append(line)
    return "\n".join(kept).strip()


def split_sentences(text: str) -> List[str]:
    cleaned = clean_markdown_text(text)
    if not cleaned:
        return []
    parts = [part.strip(" -") for part in SENTENCE_SPLIT_RE.split(cleaned) if part.strip()]
    sentences: List[str] = []
    buffer = ""
    for part in parts:
        if len(part) < 5:
            buffer = f"{buffer} {part}".strip()
            continue
        if buffer:
            part = f"{buffer} {part}".strip()
            buffer = ""
        sentences.append(part)
    if buffer:
        sentences.append(buffer)
    return sentences


def is_noise_text(text: str) -> bool:
    lowered = text.lower().strip()
    if not lowered:
        return True
    if sum(1 for token in ("http://", "https://", "www.") if token in lowered) >= 2:
        return True
    if any(pattern in lowered for pattern in NOISE_PATTERNS):
        return True
    if lowered.startswith("[http") or lowered.startswith("http"):
        return True
    if lowered.startswith("regards,"):
        return True
    if "@" in lowered and " " not in lowered:
        return True
    return False


def is_buyer_context_bullet(text: str) -> bool:
    lowered = text.lower()
    positive_markers = (
        "customer",
        "client",
        "lead",
        "organizing",
        "planned",
        "review",
        "unable",
        "budget",
        "requested",
        "request",
        "prefer",
        "wants",
        "looking for",
        "don't need",
        "doesn't need",
        "decision",
        "fiance",
        "fiancee",
        "son",
        "daughter",
        "venue",
        "guest",
        "still confirming",
        "not yet secured",
        "coming from",
        "vacation",
    )
    negative_starts = (
        "explained ",
        "outlined ",
        "offered ",
        "presented ",
        "comeketo will",
        "tony will",
        "consultant to send",
        "agreed to consult",
    )
    if any(lowered.startswith(prefix) for prefix in negative_starts):
        return False
    return any(marker in lowered for marker in positive_markers)


def extract_summary_sections(summary_text: str) -> Dict[str, List[str]]:
    sections: Dict[str, List[str]] = defaultdict(list)
    current_section = "AI Summary"
    for line in summary_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("### "):
            current_section = stripped[4:].strip()
            continue
        if stripped.startswith("- "):
            sections[current_section].append(clean_markdown_text(stripped[2:].strip()))
    return dict(sections)


def parse_call_transcript(transcript_text: str) -> List[Dict[str, str]]:
    utterances: List[Dict[str, str]] = []
    for line in transcript_text.splitlines():
        match = CALL_LINE_RE.match(line.strip())
        if not match:
            continue
        utterances.append(
            {
                "speaker": match.group("speaker").strip(),
                "role": match.group("role").strip().lower(),
                "text": clean_markdown_text(match.group("text").strip()),
            }
        )
    return utterances


def first_category(text: str, patterns: Dict[str, Sequence[str]]) -> Optional[str]:
    for category, regexes in patterns.items():
        if any(re.search(regex, text, flags=re.IGNORECASE) for regex in regexes):
            return category
    return None


def is_question_like(text: str) -> bool:
    lowered = text.lower()
    if "?" in text:
        return True
    return bool(re.search(r"\b(can|could|would|when|what|how|is there|do you|are you|will you)\b", lowered))


def make_item(
    lead_row: Dict[str, str],
    comm_row: Dict[str, Any],
    item_type: str,
    category: str,
    text: str,
    source_path: str,
    status: str = "",
    notes: str = "",
) -> Dict[str, Any]:
    return {
        "lead_id": comm_row.get("lead_id") or "",
        "lead_name": comm_row.get("lead_name") or "",
        "lead_owner_name": lead_row.get("lead_owner_name") or "",
        "stage_label": lead_row.get("current_opportunity_status_label") or lead_row.get("lead_status_label") or "",
        "event_datetime_utc": comm_row.get("event_datetime_utc") or "",
        "channel": comm_row.get("channel") or "",
        "event_id": comm_row.get("event_id") or "",
        "salesperson_name": comm_row.get("salesperson_name") or "",
        "contact_name": comm_row.get("contact_name") or "",
        "item_type": item_type,
        "category": category,
        "text": compact_text(text, limit=500),
        "status": status,
        "notes": notes,
        "source_path": source_path,
    }


def extract_message_items(
    lead_row: Dict[str, str],
    comm_row: Dict[str, Any],
    body_text: str,
    source_path: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    asks: List[Dict[str, Any]] = []
    blockers: List[Dict[str, Any]] = []
    preferences: List[Dict[str, Any]] = []
    commitments: List[Dict[str, Any]] = []

    current_body = strip_quoted_history(body_text)
    if not current_body:
        return asks, blockers, preferences, commitments

    sentences = split_sentences(current_body)
    direction = (comm_row.get("direction") or "").lower()
    is_buyer_message = direction in {"incoming", "inbound"}
    is_seller_message = direction in {"outgoing", "outbound"}

    for sentence in sentences:
        if len(sentence) < 10 or is_noise_text(sentence):
            continue
        if is_buyer_message:
            ask_category = first_category(sentence, ASK_PATTERNS)
            if ask_category and is_question_like(sentence):
                asks.append(make_item(lead_row, comm_row, "buyer_ask", ask_category, sentence, source_path))

            blocker_category = first_category(sentence, BLOCKER_PATTERNS)
            if blocker_category:
                blockers.append(make_item(lead_row, comm_row, "blocker", blocker_category, sentence, source_path))

            preference_category = first_category(sentence, PREFERENCE_PATTERNS)
            if preference_category:
                preferences.append(make_item(lead_row, comm_row, "preference", preference_category, sentence, source_path))

        if is_seller_message:
            commitment_category = first_category(sentence, COMMITMENT_PATTERNS)
            if commitment_category:
                commitments.append(make_item(lead_row, comm_row, "sales_commitment", commitment_category, sentence, source_path))

    return asks, blockers, preferences, commitments


def extract_call_items(
    lead_row: Dict[str, str],
    comm_row: Dict[str, Any],
    summary_text: str,
    transcript_text: str,
    source_path: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    asks: List[Dict[str, Any]] = []
    blockers: List[Dict[str, Any]] = []
    preferences: List[Dict[str, Any]] = []
    commitments: List[Dict[str, Any]] = []

    utterances = parse_call_transcript(transcript_text)
    for utterance in utterances:
        text = utterance["text"]
        if len(text) < 10 or is_noise_text(text):
            continue
        role = utterance["role"]
        if role == "contact":
            ask_category = first_category(text, ASK_PATTERNS)
            if ask_category and is_question_like(text):
                asks.append(make_item(lead_row, comm_row, "buyer_ask", ask_category, text, source_path))

            blocker_category = first_category(text, BLOCKER_PATTERNS)
            if blocker_category:
                blockers.append(make_item(lead_row, comm_row, "blocker", blocker_category, text, source_path))

            preference_category = first_category(text, PREFERENCE_PATTERNS)
            if preference_category:
                preferences.append(make_item(lead_row, comm_row, "preference", preference_category, text, source_path))

        if role == "salesperson":
            commitment_category = first_category(text, COMMITMENT_PATTERNS)
            if commitment_category:
                commitments.append(make_item(lead_row, comm_row, "sales_commitment", commitment_category, text, source_path))

    summary_sections = extract_summary_sections(summary_text)
    for section_name, bullets in summary_sections.items():
        lowered_section = section_name.lower()
        for bullet in bullets:
            if len(bullet) < 10 or is_noise_text(bullet):
                continue
            if (section_name == "AI Summary" or "event details" in lowered_section) and is_buyer_context_bullet(bullet):
                blocker_category = first_category(bullet, BLOCKER_PATTERNS)
                if blocker_category:
                    blockers.append(make_item(lead_row, comm_row, "blocker", blocker_category, bullet, source_path))

                preference_category = first_category(bullet, PREFERENCE_PATTERNS)
                if preference_category:
                    preferences.append(make_item(lead_row, comm_row, "preference", preference_category, bullet, source_path))

            if "next steps" in lowered_section or re.search(r"\b(committed to|will create|will email|will send)\b", bullet, re.IGNORECASE):
                commitment_category = first_category(bullet, COMMITMENT_PATTERNS)
                if commitment_category:
                    commitments.append(make_item(lead_row, comm_row, "sales_commitment", commitment_category, bullet, source_path))

    return asks, blockers, preferences, commitments


def dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sorted_items = sorted(items, key=lambda item: item.get("event_datetime_utc") or "", reverse=True)
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for item in sorted_items:
        key = (item.get("lead_id"), item.get("item_type"), item.get("category"), dedupe_key(item.get("text") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def summarize_categories(items: List[Dict[str, Any]], limit: int = 4) -> str:
    counter = Counter(item.get("category") or "" for item in items if item.get("category"))
    return " | ".join(label for label, _count in counter.most_common(limit))


def load_comm_texts(folder: Path, channel: str) -> Tuple[str, str]:
    if channel == "call":
        call_md = folder / "call.md"
        return read_markdown_section(call_md, "AI Summary"), read_markdown_section(call_md, "Transcript")
    message_md = folder / "message.md"
    return read_markdown_section(message_md, "Body"), ""


def build_seller_text_lookup(lead_comm_rows: List[Dict[str, Any]]) -> List[Tuple[datetime, str]]:
    seller_events: List[Tuple[datetime, str]] = []
    for row in lead_comm_rows:
        dt = parse_iso(row.get("event_datetime_utc"))
        if not dt:
            continue
        channel = row.get("channel") or ""
        folder = Path(row.get("folder") or "")
        summary_or_body, transcript = load_comm_texts(folder, channel)
        direction = (row.get("direction") or "").lower()
        seller_text = ""

        if channel in {"email", "sms"} and direction in {"outgoing", "outbound"}:
            seller_text = strip_quoted_history(summary_or_body)
        elif channel == "call":
            utterances = parse_call_transcript(transcript)
            seller_lines = [utt["text"] for utt in utterances if utt["role"] == "salesperson"]
            seller_text = " ".join(seller_lines)
            if summary_or_body:
                seller_text = f"{seller_text} {summary_or_body}".strip()

        if seller_text:
            seller_events.append((dt, clean_markdown_text(seller_text)))
    seller_events.sort(key=lambda item: item[0])
    return seller_events


def is_commitment_resolved(
    commitment: Dict[str, Any],
    seller_events: List[Tuple[datetime, str]],
) -> Tuple[bool, str]:
    committed_at = parse_iso(commitment.get("event_datetime_utc"))
    if not committed_at:
        return False, "no_timestamp"

    category = commitment.get("category") or ""
    tokens = tuple(token.lower() for token in FULFILLMENT_TOKENS.get(category, ()))

    for event_dt, seller_text in seller_events:
        if event_dt <= committed_at:
            continue
        lowered = seller_text.lower()
        if not tokens:
            return True, "later_seller_touch"
        if any(token in lowered for token in tokens):
            return True, "later_matching_seller_touch"
    return False, "no_later_matching_touch"


def build_open_loops(
    lead_row: Dict[str, str],
    asks: List[Dict[str, Any]],
    commitments: List[Dict[str, Any]],
    seller_events: List[Tuple[datetime, str]],
    follow_up_row: Optional[Dict[str, str]],
) -> List[Dict[str, Any]]:
    open_loops: List[Dict[str, Any]] = []
    latest_seller_touch = seller_events[-1][0] if seller_events else None
    stage_label = (lead_row.get("current_opportunity_status_label") or lead_row.get("lead_status_label") or "").lower()
    if any(token in stage_label for token in ("lost", "archived", "do not call")):
        return []

    for ask in asks:
        ask_dt = parse_iso(ask.get("event_datetime_utc"))
        if not ask_dt:
            continue
        if ask.get("channel") not in {"email", "sms"}:
            continue
        if latest_seller_touch and latest_seller_touch > ask_dt:
            continue
        open_loops.append(
            {
                **ask,
                "loop_type": "buyer_question",
                "loop_status": "open",
                "priority_score": (follow_up_row or {}).get("priority_score") or "",
                "priority_band": (follow_up_row or {}).get("priority_band") or "",
                "queue_bucket": (follow_up_row or {}).get("queue_bucket") or "",
            }
        )

    for commitment in commitments:
        resolved, resolution_note = is_commitment_resolved(commitment, seller_events)
        if resolved:
            continue
        open_loops.append(
            {
                **commitment,
                "loop_type": "sales_commitment",
                "loop_status": "pending",
                "priority_score": (follow_up_row or {}).get("priority_score") or "",
                "priority_band": (follow_up_row or {}).get("priority_band") or "",
                "queue_bucket": (follow_up_row or {}).get("queue_bucket") or "",
                "notes": resolution_note,
            }
        )
    return sorted(open_loops, key=lambda row: (row.get("priority_score") or "", row.get("event_datetime_utc") or ""), reverse=True)


def build_lead_markdown(
    payload: Dict[str, Any],
    asks: List[Dict[str, Any]],
    blockers: List[Dict[str, Any]],
    preferences: List[Dict[str, Any]],
    commitments: List[Dict[str, Any]],
    open_loops: List[Dict[str, Any]],
) -> str:
    lines = [
        f"# Conversation Intelligence: {payload['lead_name']}",
        "",
        "## Snapshot",
        f"- Owner: {payload.get('lead_owner_name') or ''}",
        f"- Stage: {payload.get('stage_label') or ''}",
        f"- Latest Observed Activity (UTC): `{payload.get('latest_observed_activity_utc') or ''}`",
        f"- Buyer Asks: `{payload.get('buyer_ask_count')}`",
        f"- Blockers / Concerns: `{payload.get('blocker_count')}`",
        f"- Preferences / Requirements: `{payload.get('preference_count')}`",
        f"- Sales Commitments: `{payload.get('sales_commitment_count')}`",
        f"- Open Loops: `{payload.get('open_loop_count')}`",
        f"- Dominant Topics: {payload.get('dominant_topics') or ''}",
        "",
        "## Latest Signal",
        f"- Latest Buyer Ask: {payload.get('latest_buyer_ask') or ''}",
        f"- Latest Blocker: {payload.get('latest_blocker') or ''}",
        f"- Latest Sales Commitment: {payload.get('latest_sales_commitment') or ''}",
        "",
        "## Open Loops",
    ]
    if open_loops:
        for item in open_loops[:8]:
            lines.append(
                f"- `{item.get('event_datetime_utc')}` | {item.get('loop_type')} | {item.get('category')} | {item.get('text')}"
            )
    else:
        lines.append("- None detected from the current normalized comms.")

    lines.extend(["", "## Buyer Asks"])
    if asks:
        for item in asks[:8]:
            lines.append(f"- `{item.get('event_datetime_utc')}` | {item.get('category')} | {item.get('text')}")
    else:
        lines.append("- None extracted.")

    lines.extend(["", "## Blockers / Concerns"])
    if blockers:
        for item in blockers[:8]:
            lines.append(f"- `{item.get('event_datetime_utc')}` | {item.get('category')} | {item.get('text')}")
    else:
        lines.append("- None extracted.")

    lines.extend(["", "## Preferences / Requirements"])
    if preferences:
        for item in preferences[:8]:
            lines.append(f"- `{item.get('event_datetime_utc')}` | {item.get('category')} | {item.get('text')}")
    else:
        lines.append("- None extracted.")

    lines.extend(["", "## Sales Commitments"])
    if commitments:
        for item in commitments[:8]:
            status_suffix = f" | {item.get('status')}" if item.get("status") else ""
            lines.append(f"- `{item.get('event_datetime_utc')}` | {item.get('category')}{status_suffix} | {item.get('text')}")
    else:
        lines.append("- None extracted.")

    lines.append("")
    return "\n".join(lines)


def build_readme(summary_rows: List[Dict[str, Any]], open_loops: List[Dict[str, Any]], blockers: List[Dict[str, Any]], commitments: List[Dict[str, Any]]) -> str:
    owner_counts = Counter(row.get("lead_owner_name") or "Unknown" for row in summary_rows)
    lines = [
        "# Conversation Intelligence",
        "",
        "This layer extracts buyer asks, blockers, preferences, and salesperson commitments from the normalized lead communication tree.",
        "",
        "## Snapshot",
        f"- Leads scanned: `{len(summary_rows)}`",
        f"- Open loops: `{len(open_loops)}`",
        f"- Blockers / concerns: `{len(blockers)}`",
        f"- Sales commitments: `{len(commitments)}`",
        "",
        "## Owner Coverage",
    ]
    for owner, count in owner_counts.items():
        lines.append(f"- {owner}: {count}")
    lines.extend(
        [
            "",
            "## Key Files",
            "- `open_loops.md`: top open buyer questions and pending promises",
            "- `blockers_overview.md`: blocker category counts and latest evidence",
            "- `commitments_overview.md`: commitment category counts and pending items",
            "- `../normalized/lead_conversation_intelligence.csv`: one-row-per-lead summary",
            "- `../normalized/open_loops.csv`: machine-friendly open loop index",
            "",
        ]
    )
    return "\n".join(lines)


def build_open_loops_markdown(rows: List[Dict[str, Any]]) -> str:
    loop_counts = Counter(row.get("loop_type") or "" for row in rows)
    lines = [
        "# Open Loops",
        "",
        "## Snapshot",
        f"- Total open loops: `{len(rows)}`",
        "",
        "## Loop Types",
    ]
    for label, count in loop_counts.items():
        lines.append(f"- {label}: {count}")
    lines.extend(["", "## Top Open Loops"])
    for row in rows[:100]:
        lines.append(
            f"- `{row.get('priority_score') or ''}` | {row.get('priority_band') or ''} | {row.get('lead_name')} | "
            f"{row.get('loop_type')} | {row.get('category')} | {row.get('text')}"
        )
    lines.append("")
    return "\n".join(lines)


def build_overview_markdown(title: str, rows: List[Dict[str, Any]], item_label: str) -> str:
    category_counts = Counter(row.get("category") or "" for row in rows)
    lines = [
        f"# {title}",
        "",
        f"- Total {item_label}: `{len(rows)}`",
        "",
        "## Categories",
    ]
    for label, count in category_counts.items():
        lines.append(f"- {label}: {count}")
    lines.extend(["", "## Latest Examples"])
    for row in rows[:100]:
        lines.append(f"- `{row.get('event_datetime_utc')}` | {row.get('lead_name')} | {row.get('category')} | {row.get('text')}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    phone_library_dir = args.phone_library_dir
    normalized_dir = phone_library_dir / "normalized"
    output_dir = args.output_dir
    by_owner_dir = output_dir / "by_owner"
    ensure_dir(output_dir)
    ensure_dir(by_owner_dir)

    lead_rows = load_csv_rows(args.lead_briefs_csv)
    lead_by_id = {row["lead_id"]: row for row in lead_rows}
    follow_up_by_lead = {row["lead_id"]: row for row in load_csv_rows(args.follow_up_csv)}
    comm_rows = load_jsonl_rows(args.communications_jsonl)

    comms_by_lead: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in comm_rows:
        if row.get("lead_id") in lead_by_id:
            comms_by_lead[row["lead_id"]].append(row)

    lead_summary_rows: List[Dict[str, Any]] = []
    all_asks: List[Dict[str, Any]] = []
    all_blockers: List[Dict[str, Any]] = []
    all_preferences: List[Dict[str, Any]] = []
    all_commitments: List[Dict[str, Any]] = []
    all_open_loops: List[Dict[str, Any]] = []

    for lead_row in lead_rows:
        lead_id = lead_row["lead_id"]
        lead_name = lead_row["lead_name"]
        lead_dir = Path(lead_row["brief_path"]).parent
        lead_comm_rows = sorted(comms_by_lead.get(lead_id, []), key=lambda row: row.get("event_datetime_utc") or "", reverse=True)

        asks: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        preferences: List[Dict[str, Any]] = []
        commitments: List[Dict[str, Any]] = []

        for comm_row in lead_comm_rows:
            folder = Path(comm_row.get("folder") or "")
            channel = comm_row.get("channel") or ""
            source_path = str((folder / ("call.md" if channel == "call" else "message.md"))) if folder else ""

            if channel in {"email", "sms"}:
                body_text, _unused = load_comm_texts(folder, channel)
                event_asks, event_blockers, event_preferences, event_commitments = extract_message_items(
                    lead_row, comm_row, body_text, source_path
                )
            elif channel == "call":
                summary_text, transcript_text = load_comm_texts(folder, channel)
                event_asks, event_blockers, event_preferences, event_commitments = extract_call_items(
                    lead_row, comm_row, summary_text, transcript_text, source_path
                )
            else:
                continue

            asks.extend(event_asks)
            blockers.extend(event_blockers)
            preferences.extend(event_preferences)
            commitments.extend(event_commitments)

        asks = dedupe_items(asks)
        blockers = dedupe_items(blockers)
        preferences = dedupe_items(preferences)
        commitments = dedupe_items(commitments)

        seller_events = build_seller_text_lookup(lead_comm_rows)
        for item in commitments:
            resolved, resolution_note = is_commitment_resolved(item, seller_events)
            item["status"] = "resolved" if resolved else "pending"
            item["notes"] = resolution_note

        open_loops = build_open_loops(lead_row, asks, commitments, seller_events, follow_up_by_lead.get(lead_id))

        dominant_topics = summarize_categories(asks + blockers + preferences + commitments)
        summary_row = {
            "lead_id": lead_id,
            "lead_name": lead_name,
            "lead_owner_name": lead_row.get("lead_owner_name") or "",
            "stage_label": lead_row.get("current_opportunity_status_label") or lead_row.get("lead_status_label") or "",
            "latest_observed_activity_utc": lead_row.get("latest_observed_activity_utc") or "",
            "buyer_ask_count": len(asks),
            "blocker_count": len(blockers),
            "preference_count": len(preferences),
            "sales_commitment_count": len(commitments),
            "open_loop_count": len(open_loops),
            "dominant_topics": dominant_topics,
            "latest_buyer_ask": asks[0]["text"] if asks else "",
            "latest_blocker": blockers[0]["text"] if blockers else "",
            "latest_preference": preferences[0]["text"] if preferences else "",
            "latest_sales_commitment": commitments[0]["text"] if commitments else "",
            "follow_up_priority_band": (follow_up_by_lead.get(lead_id) or {}).get("priority_band") or "",
            "follow_up_priority_score": (follow_up_by_lead.get(lead_id) or {}).get("priority_score") or "",
            "conversation_intelligence_path": str(lead_dir / "lead_conversation_intelligence.md"),
            "conversation_intelligence_json_path": str(lead_dir / "lead_conversation_intelligence.json"),
        }

        payload = {
            **summary_row,
            "buyer_asks": asks[:20],
            "blockers": blockers[:20],
            "preferences": preferences[:20],
            "sales_commitments": commitments[:20],
            "open_loops": open_loops[:20],
        }
        write_json(lead_dir / "lead_conversation_intelligence.json", payload)
        (lead_dir / "lead_conversation_intelligence.md").write_text(
            build_lead_markdown(payload, asks, blockers, preferences, commitments, open_loops),
            encoding="utf-8",
        )

        lead_summary_rows.append(summary_row)
        all_asks.extend(asks)
        all_blockers.extend(blockers)
        all_preferences.extend(preferences)
        all_commitments.extend(commitments)
        all_open_loops.extend(open_loops)

    lead_summary_rows.sort(key=lambda row: row.get("open_loop_count") or 0, reverse=True)
    all_asks = sorted(all_asks, key=lambda row: row.get("event_datetime_utc") or "", reverse=True)
    all_blockers = sorted(all_blockers, key=lambda row: row.get("event_datetime_utc") or "", reverse=True)
    all_preferences = sorted(all_preferences, key=lambda row: row.get("event_datetime_utc") or "", reverse=True)
    all_commitments = sorted(all_commitments, key=lambda row: row.get("event_datetime_utc") or "", reverse=True)
    all_open_loops = sorted(
        all_open_loops,
        key=lambda row: (int(row.get("priority_score") or 0), row.get("event_datetime_utc") or ""),
        reverse=True,
    )

    write_csv(normalized_dir / "lead_conversation_intelligence.csv", lead_summary_rows)
    write_jsonl(normalized_dir / "lead_conversation_intelligence.jsonl", lead_summary_rows)
    write_csv(normalized_dir / "customer_asks.csv", all_asks)
    write_jsonl(normalized_dir / "customer_asks.jsonl", all_asks)
    write_csv(normalized_dir / "objections_blockers.csv", all_blockers)
    write_jsonl(normalized_dir / "objections_blockers.jsonl", all_blockers)
    write_csv(normalized_dir / "buyer_preferences.csv", all_preferences)
    write_jsonl(normalized_dir / "buyer_preferences.jsonl", all_preferences)
    write_csv(normalized_dir / "sales_commitments.csv", all_commitments)
    write_jsonl(normalized_dir / "sales_commitments.jsonl", all_commitments)
    write_csv(normalized_dir / "open_loops.csv", all_open_loops)
    write_jsonl(normalized_dir / "open_loops.jsonl", all_open_loops)

    (output_dir / "README.md").write_text(
        build_readme(lead_summary_rows, all_open_loops, all_blockers, all_commitments),
        encoding="utf-8",
    )
    (output_dir / "open_loops.md").write_text(build_open_loops_markdown(all_open_loops), encoding="utf-8")
    (output_dir / "blockers_overview.md").write_text(
        build_overview_markdown("Blockers Overview", all_blockers, "blockers / concerns"),
        encoding="utf-8",
    )
    (output_dir / "commitments_overview.md").write_text(
        build_overview_markdown("Commitments Overview", all_commitments, "sales commitments"),
        encoding="utf-8",
    )

    owner_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in all_open_loops:
        owner_rows[slugify(row.get("lead_owner_name") or "Unknown")].append(row)

    for owner_slug, rows in owner_rows.items():
        owner_dir = by_owner_dir / owner_slug
        ensure_dir(owner_dir)
        write_csv(owner_dir / "open_loops.csv", rows)
        write_jsonl(owner_dir / "open_loops.jsonl", rows)

    print(
        json.dumps(
            {
                "lead_rows": len(lead_summary_rows),
                "buyer_asks": len(all_asks),
                "blockers": len(all_blockers),
                "preferences": len(all_preferences),
                "sales_commitments": len(all_commitments),
                "open_loops": len(all_open_loops),
                "output_dir": str(output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
