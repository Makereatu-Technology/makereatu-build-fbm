from __future__ import annotations

import json
import re
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional


@dataclass
class Chunk:
    chunk_id: str
    label: str
    pages: List[int]
    text: str
    tables: List[dict]


HEADING_PATTERNS = {
    "project_data": [
        r"\bbasic project data\b",
        r"\bproject data\b",
        r"\bdata sheet\b",
        r"\bproject information\b",
    ],
    "objectives_components": [
        r"\bobjectives?\b",
        r"\bproject development objectives?\b",
        r"\bcomponents?\b",
    ],
    "outputs": [
        r"\boutputs?\b",
        r"\bachievement of objectives?\b",
        r"\befficacy\b",
        r"\bintermediate outcome\b",
        r"\boutcomes\b",
    ],
    "schedule_costs": [
        r"\bproject cost\b",
        r"\bproject costs\b",
        r"\bfinancing\b",
        r"\bborrower contribution\b",
        r"\bdates\b",
        r"\bclosing date\b",
        r"\bschedule\b",
        r"\bdisbursement\b",
    ],
    "efficiency_economic": [
        r"\befficiency\b",
        r"\beconomic analysis\b",
        r"\beconomic internal rate of return\b",
        r"\beconomic rate of return\b",
        r"\berr\b",
        r"\beirr\b",
        r"\bnet present value\b",
    ],
    "safeguards": [
        r"\bsafeguard",
        r"\benvironment",
        r"\bresettlement\b",
        r"\bindigenous peoples\b",
        r"\benvironmental and social\b",
    ],
    "fiduciary_procurement": [
        r"\bfiduciary\b",
        r"\bprocurement\b",
        r"\bfinancial management\b",
        r"\baudit\b",
    ],
    "ratings": [
        r"\boverall assessment\b",
        r"\boverall outcome\b",
        r"\boverall rating\b",
        r"\bperformance rating\b",
        r"\brisk to development outcome\b",
        r"\bbank performance\b",
        r"\bborrower performance\b",
        r"\brelevance\b",
        r"\bsustainability\b",
    ],
    "lessons": [
        r"\blessons\b",
        r"\brecommendations?\b",
    ],
}


def _match_heading_label(text: str) -> Optional[str]:
    t = (text or "").lower()
    for label, patterns in HEADING_PATTERNS.items():
        for p in patterns:
            if re.search(p, t, flags=re.IGNORECASE):
                return label
    return None


def chunk_artifact_by_headings(artifact_path: str, max_chars_per_chunk: int = 12000) -> List[Chunk]:
    with open(artifact_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    pages = data.get("pages", [])
    tables = data.get("tables", [])

    tables_by_page: Dict[int, List[dict]] = {}
    for t in tables:
        p = t.get("page")
        tables_by_page.setdefault(p, []).append(t)

    chunks: List[Chunk] = []
    current_label = "unclassified"
    current_pages: List[int] = []
    current_text_parts: List[str] = []
    current_tables: List[dict] = []

    def flush():
        nonlocal current_pages, current_text_parts, current_tables, current_label, chunks
        if not current_pages:
            return
        text = "\n\n".join(current_text_parts).strip()
        chunk_id = f"p{current_pages[0]}-{current_pages[-1]}#{len(chunks)+1}"
        chunks.append(
            Chunk(
                chunk_id=chunk_id,
                label=current_label,
                pages=current_pages[:],
                text=text,
                tables=current_tables[:],
            )
        )
        current_pages = []
        current_text_parts = []
        current_tables = []

    for page in pages:
        pnum = int(page.get("page"))
        ptext = (page.get("text") or "").strip()
        page_head = "\n".join(ptext.splitlines()[:12])
        label = _match_heading_label(page_head) or _match_heading_label(ptext[:1500]) or current_label

        projected_len = len("\n\n".join(current_text_parts)) + len(ptext)

        if current_pages and (label != current_label or projected_len > max_chars_per_chunk):
            flush()

        if not current_pages:
            current_label = label

        current_pages.append(pnum)
        current_text_parts.append(f"--- Page {pnum} ---\n{ptext}")

        if pnum in tables_by_page:
            current_tables.extend(tables_by_page[pnum])

    flush()
    return chunks


def save_chunks_json(chunks: List[Chunk], out_path: str) -> str:
    payload = [asdict(c) for c in chunks]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return out_path