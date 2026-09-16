from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from feedback_lakehouse.api import app


def test_api_validates_and_writes_local_inbox(tmp_path: Path, monkeypatch) -> None:
    inbox = tmp_path / "inbox.jsonl"
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setenv("LOCAL_INBOX", str(inbox))
    client = TestClient(app)
    payload = {
        "review_id": "review-1",
        "product_id": "product-1",
        "rating": 4,
        "review_text": "The camera is excellent.",
        "source": "web",
        "created_at": "2026-01-01T00:00:00Z",
        "schema_version": 1,
    }
    response = client.post("/v1/feedback", json=payload)
    assert response.status_code == 202
    assert response.json()["review_id"] == "review-1"
    assert json.loads(inbox.read_text(encoding="utf-8"))["rating"] == 4


def test_api_rejects_invalid_payload() -> None:
    client = TestClient(app)
    response = client.post("/v1/feedback", json={"rating": 9})
    assert response.status_code == 422


class FakeProducer:
    instances: list[FakeProducer] = []

    def __init__(self, config: dict[str, object]) -> None:
        self.config = config
        self.messages: list[tuple[str, str, str]] = []
        FakeProducer.instances.append(self)

    def produce(self, topic: str, key: str, value: str, on_delivery) -> None:
        self.messages.append((topic, key, value))
        on_delivery(None, None)

    def flush(self, timeout: float) -> int:
        return 0


def test_api_publishes_to_kafka_with_one_shared_idempotent_producer(monkeypatch) -> None:
    from feedback_lakehouse import api

    FakeProducer.instances.clear()
    api._producer.cache_clear()
    monkeypatch.setattr(api, "Producer", FakeProducer)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "feedback-events")
    client = TestClient(app)
    payload = {
        "review_id": "review-9",
        "product_id": "product-1",
        "rating": 3,
        "review_text": "The delivery was late.",
        "source": "web",
        "created_at": "2026-01-01T00:00:00Z",
    }

    first = client.post("/v1/feedback", json=payload)
    second = client.post("/v1/feedback", json=payload)

    assert first.status_code == second.status_code == 202
    assert len(FakeProducer.instances) == 1, "one producer per process, not per request"
    producer = FakeProducer.instances[0]
    assert producer.config["enable.idempotence"] is True
    assert producer.config["acks"] == "all"
    topics, keys, values = zip(*producer.messages, strict=True)
    assert topics == ("feedback-events", "feedback-events")
    assert keys == ("review-9", "review-9"), "the review id is the partition key"
    assert json.loads(values[0])["schema_version"] == 1
    assert first.json()["event_id"] == second.json()["event_id"]
    api._producer.cache_clear()


def test_api_returns_503_when_the_broker_rejects_the_message(monkeypatch) -> None:
    from feedback_lakehouse import api

    class FailingProducer(FakeProducer):
        def produce(self, topic: str, key: str, value: str, on_delivery) -> None:
            on_delivery("Local: Message timed out", None)

    api._producer.cache_clear()
    monkeypatch.setattr(api, "Producer", FailingProducer)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    client = TestClient(app)
    response = client.post(
        "/v1/feedback",
        json={
            "review_id": "review-10",
            "product_id": "product-1",
            "rating": 3,
            "review_text": "The delivery was late.",
            "source": "web",
            "created_at": "2026-01-01T00:00:00Z",
        },
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "event broker unavailable"
    api._producer.cache_clear()
