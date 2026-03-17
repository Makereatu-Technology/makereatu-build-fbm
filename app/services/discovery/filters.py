from __future__ import annotations

import re
from typing import Iterable, List, Set

# Default keywords for rural roads discovery
KEYWORDS_DEFAULT: List[str] = [
    "rural road",
    "rural roads",
    "road rehabilitation",
    "road improvement",
    "road maintenance",
    "transport",
    "access road",
]

# SEA + Pacific heuristics (extend as you like)
TARGET_COUNTRIES: Set[str] = {
    # Southeast Asia
    "vietnam", "lao pdr", "laos", "cambodia", "thailand", "myanmar",
    "malaysia", "indonesia", "philippines", "timor-leste", "timor leste",
    "singapore", "brunei",
    # Pacific Islands
    "papua new guinea", "png", "solomon islands", "vanuatu", "fiji",
    "samoa", "tonga", "kiribati", "tuvalu", "nauru", "palau",
    "federated states of micronesia", "micronesia", "marshall islands",
    "cook islands", "niue",
}

_WS = re.compile(r"\s+")

def normalize(s: str) -> str:
    """Lowercase + collapse whitespace + strip punctuation-ish."""
    if s is None:
        return ""
    s = s.strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s\-\/]", " ", s)  # keep words, spaces, hyphen, slash
    s = _WS.sub(" ", s).strip()
    return s


def any_country_in_text(text: str, countries: Iterable[str] = TARGET_COUNTRIES) -> bool:
    blob = normalize(text)
    return any(normalize(c) in blob for c in countries)