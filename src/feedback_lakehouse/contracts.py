"""Versioned data contracts shared by ingestion and processing jobs."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class FeedbackEvent(BaseModel):
    """The supported version-one feedback event."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    review_id: str = Field(min_length=1, max_length=100)
    product_id: str = Field(min_length=1, max_length=100)
    rating: int = Field(ge=1, le=5)
    review_text: str = Field(min_length=3, max_length=20_000)
    source: str = Field(min_length=1, max_length=50)
    created_at: datetime
    updated_at: datetime | None = None
    schema_version: int = Field(default=1, ge=1)
    customer_id_hash: str | None = Field(default=None, max_length=128)

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include a timezone")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_version_and_dates(self) -> FeedbackEvent:
        if self.schema_version != 1:
            raise ValueError(f"unsupported schema_version: {self.schema_version}")
        if self.updated_at is not None and self.updated_at < self.created_at:
            raise ValueError("updated_at cannot be before created_at")
        return self

    @property
    def effective_updated_at(self) -> datetime:
        return self.updated_at or self.created_at

    def canonical_json(self) -> str:
        payload = self.model_dump(mode="json", exclude_none=True)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def fingerprint(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
