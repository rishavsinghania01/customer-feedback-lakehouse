from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from feedback_lakehouse.contracts import FeedbackEvent


def test_contract_normalises_timestamps_and_is_deterministic() -> None:
    event = FeedbackEvent(
        review_id="review-1",
        product_id="product-1",
        rating=4,
        review_text="The camera is excellent.",
        source="web",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert event.effective_updated_at == event.created_at
    assert event.fingerprint() == event.fingerprint()
    assert '"schema_version":1' in event.canonical_json()


@pytest.mark.parametrize("rating", [0, 6])
def test_contract_rejects_invalid_rating(rating: int) -> None:
    with pytest.raises(ValidationError):
        FeedbackEvent(
            review_id="review-1",
            product_id="product-1",
            rating=rating,
            review_text="A valid review body.",
            source="web",
            created_at="2026-01-01T00:00:00Z",
        )


def test_contract_rejects_unknown_fields_and_naive_timestamps() -> None:
    with pytest.raises(ValidationError):
        FeedbackEvent.model_validate(
            {
                "review_id": "review-1",
                "product_id": "product-1",
                "rating": 4,
                "review_text": "A valid review body.",
                "source": "web",
                "created_at": "2026-01-01T00:00:00",
                "unexpected": True,
            }
        )
