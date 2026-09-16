"""Small deterministic aspect-sentiment component for the example pipeline."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass

ASPECT_ALIASES: dict[str, tuple[str, ...]] = {
    "battery": ("battery", "charge", "charging"),
    "camera": ("camera", "photo", "picture"),
    "delivery": ("delivery", "courier", "shipping", "arrived"),
    "packaging": ("packaging", "package", "box"),
    "performance": ("performance", "speed", "fast", "slow", "lag"),
    "price": ("price", "cost", "expensive", "cheap", "value"),
    "quality": ("quality", "build", "material", "durable", "broken"),
    "support": ("support", "service", "agent", "refund"),
    "usability": ("easy", "difficult", "interface", "setup", "use"),
}

POSITIVE_WORDS = {
    "amazing",
    "best",
    "clear",
    "durable",
    "easy",
    "excellent",
    "fast",
    "good",
    "great",
    "helpful",
    "love",
    "perfect",
    "quick",
    "reliable",
    "smooth",
}
NEGATIVE_WORDS = {
    "bad",
    "broken",
    "confusing",
    "difficult",
    "disappointing",
    "drains",
    "expensive",
    "hate",
    "lag",
    "late",
    "poor",
    "slow",
    "terrible",
    "unhelpful",
    "worse",
}


@dataclass(frozen=True)
class AspectResult:
    aspect: str
    sentiment_score: float
    sentiment_label: str
    evidence: str

    def as_dict(self) -> dict[str, str | float]:
        return asdict(self)


def _clauses(text: str) -> list[str]:
    return [
        item.strip()
        for item in re.split(r"[.!?;]|\bbut\b|\bhowever\b", text, flags=re.I)
        if item.strip()
    ]


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z']+", text.lower())


# An alias matches a whole token or a common inflection of it ("photo" matches
# "photos", "charge" matches "charged"). Matching is deliberately not by
# substring: "because" contains "use", "flag" contains "lag" and "costume"
# contains "cost", and each of those used to invent an aspect mention.
_INFLECTIONS = ("", "s", "es", "d", "ed", "ing", "r", "er", "ers", "ly")


def _mentions(tokens: set[str], alias: str) -> bool:
    return any(alias + suffix in tokens for suffix in _INFLECTIONS)


def score_aspects(text: str) -> list[AspectResult]:
    """Score sentiment near an aspect mention, keeping the supporting clause."""

    results: list[AspectResult] = []
    for clause in _clauses(text):
        tokens = _tokens(clause)
        token_set = set(tokens)
        positive = sum(token in POSITIVE_WORDS for token in tokens)
        negative = sum(token in NEGATIVE_WORDS for token in tokens)
        denominator = max(positive + negative, 1)
        score = round((positive - negative) / denominator, 4)
        label = "positive" if score > 0.1 else "negative" if score < -0.1 else "neutral"

        for aspect, aliases in ASPECT_ALIASES.items():
            if any(_mentions(token_set, alias) for alias in aliases):
                results.append(AspectResult(aspect, score, label, clause))

    best_by_aspect: dict[str, AspectResult] = {}
    for result in results:
        previous = best_by_aspect.get(result.aspect)
        if previous is None or abs(result.sentiment_score) > abs(previous.sentiment_score):
            best_by_aspect[result.aspect] = result
    return sorted(best_by_aspect.values(), key=lambda item: item.aspect)
