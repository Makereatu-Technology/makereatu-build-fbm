from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class DocClassification:
    bank: Optional[str]
    doc_type: str
    confidence: float
    signals: List[str]


def _load_artifact_text(artifact_path: str, max_pages: int = 8) -> str:
    with open(artifact_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    pages = data.get("pages", [])[:max_pages]
    text = "\n\n".join((p.get("text") or "") for p in pages)
    return text[:40000]


def _add_signal(signals: List[str], label: str, score: float, total: float) -> float:
    signals.append(label)
    return total + score


def _contains(text: str, pattern: str) -> bool:
    return re.search(pattern, text, flags=re.I) is not None


def classify_doc_type(artifact_path: str) -> DocClassification:
    text = _load_artifact_text(artifact_path, max_pages=8)
    low = text.lower()
    fname = os.path.basename(artifact_path).lower()

    signals: List[str] = []

    adb_score = 0.0
    wb_score = 0.0

    # -------------------------
    # Strong ADB signals
    # -------------------------
    if "asian development bank" in low:
        adb_score = _add_signal(signals, "adb:asian development bank", 4.0, adb_score)

    if "independent evaluation department" in low:
        adb_score = _add_signal(signals, "adb:independent evaluation department", 4.0, adb_score)

    if _contains(low, r"\breference number\s*:\s*(pcv|ppe)\s*[:\-]"):
        adb_score = _add_signal(signals, "adb:reference number pcv/ppe", 4.0, adb_score)

    if "validation report" in low:
        adb_score = _add_signal(signals, "adb:validation report", 3.0, adb_score)

    if "performance evaluation report" in low:
        adb_score = _add_signal(signals, "adb:performance evaluation report", 3.0, adb_score)

    if "project performance evaluation report" in low:
        adb_score = _add_signal(signals, "adb:project performance evaluation report", 3.0, adb_score)

    if "pcr validation" in low:
        adb_score = _add_signal(signals, "adb:pcr validation", 3.0, adb_score)

    if _contains(low, r"\bloan number\b"):
        adb_score = _add_signal(signals, "adb:loan number", 1.0, adb_score)

    if _contains(low, r"\bproject number\b"):
        adb_score = _add_signal(signals, "adb:project number", 1.0, adb_score)

    if "executing agency" in low:
        adb_score = _add_signal(signals, "adb:executing agency", 1.0, adb_score)

    # filename hints
    if "loan " in fname or "loan_" in fname or "loan-" in fname:
        adb_score = _add_signal(signals, "adb:filename loan", 0.8, adb_score)

    if "adb" in fname:
        adb_score = _add_signal(signals, "adb:filename adb", 1.2, adb_score)

    if "validation" in fname or "pper" in fname or "ppe" in fname:
        adb_score = _add_signal(signals, "adb:filename validation/pper", 1.0, adb_score)

    # -------------------------
    # Strong WB signals
    # -------------------------
    if "world bank" in low:
        wb_score = _add_signal(signals, "wb:world bank", 4.0, wb_score)

    if "independent evaluation group" in low:
        wb_score = _add_signal(signals, "wb:independent evaluation group", 4.0, wb_score)

    if _contains(low, r"\breport number\s*:\s*icrr"):
        wb_score = _add_signal(signals, "wb:report number icrr", 4.0, wb_score)

    if "implementation completion report" in low:
        wb_score = _add_signal(signals, "wb:implementation completion report", 3.0, wb_score)

    if "implementation completion and results report" in low:
        wb_score = _add_signal(signals, "wb:implementation completion and results report", 3.0, wb_score)

    if "icr review" in low:
        wb_score = _add_signal(signals, "wb:icr review", 3.0, wb_score)

    if "project development objective" in low or "project development objectives" in low:
        wb_score = _add_signal(signals, "wb:project development objective", 1.5, wb_score)

    if "bank performance" in low:
        wb_score = _add_signal(signals, "wb:bank performance", 1.0, wb_score)

    if "borrower performance" in low:
        wb_score = _add_signal(signals, "wb:borrower performance", 1.0, wb_score)

    if "public disclosure authorized" in low:
        wb_score = _add_signal(signals, "wb:public disclosure authorized", 1.5, wb_score)

    # filename hints
    if "ieg" in fname:
        wb_score = _add_signal(signals, "wb:filename ieg", 1.2, wb_score)

    if "icr" in fname or "icrr" in fname:
        wb_score = _add_signal(signals, "wb:filename icr/icrr", 1.2, wb_score)

    if "worldbank" in fname or "world_bank" in fname or "wb" in fname:
        wb_score = _add_signal(signals, "wb:filename worldbank/wb", 0.8, wb_score)

    # -------------------------
    # High-confidence subtype rules
    # -------------------------
    if (
        "independent evaluation group" in low
        and ("icr review" in low or _contains(low, r"\breport number\s*:\s*icrr"))
    ):
        return DocClassification(
            bank="WB",
            doc_type="WB_IEG_ICR_REVIEW",
            confidence=0.98,
            signals=signals,
        )

    if "implementation completion and results report" in low and "world bank" in low:
        return DocClassification(
            bank="WB",
            doc_type="WB_ICR",
            confidence=0.96,
            signals=signals,
        )

    if "implementation completion report" in low and "world bank" in low:
        return DocClassification(
            bank="WB",
            doc_type="WB_ICR",
            confidence=0.94,
            signals=signals,
        )

    if "validation report" in low and "asian development bank" in low:
        return DocClassification(
            bank="ADB",
            doc_type="ADB_VALIDATION",
            confidence=0.97,
            signals=signals,
        )

    if (
        ("performance evaluation report" in low or "project performance evaluation report" in low)
        and ("asian development bank" in low or "independent evaluation department" in low)
    ):
        return DocClassification(
            bank="ADB",
            doc_type="ADB_PPER",
            confidence=0.96,
            signals=signals,
        )

    if "project completion report" in low and "asian development bank" in low:
        return DocClassification(
            bank="ADB",
            doc_type="ADB_PCR",
            confidence=0.94,
            signals=signals,
        )

    # -------------------------
    # Generic bank decision
    # -------------------------
    diff = abs(adb_score - wb_score)
    top = max(adb_score, wb_score)

    if adb_score >= 4.0 and adb_score > wb_score:
        confidence = 0.75 if diff < 2 else 0.88
        return DocClassification(
            bank="ADB",
            doc_type="ADB_GENERIC",
            confidence=confidence,
            signals=signals,
        )

    if wb_score >= 4.0 and wb_score > adb_score:
        confidence = 0.75 if diff < 2 else 0.88
        return DocClassification(
            bank="WB",
            doc_type="WB_GENERIC",
            confidence=confidence,
            signals=signals,
        )

    # weak fallback using dominant signals
    if top >= 2.5:
        bank = "ADB" if adb_score > wb_score else "WB"
        return DocClassification(
            bank=bank,
            doc_type=f"{bank}_WEAK_GENERIC",
            confidence=0.60,
            signals=signals,
        )

    return DocClassification(
        bank=None,
        doc_type="UNKNOWN",
        confidence=0.30,
        signals=signals,
    )