"""HTTP ingestion edge that publishes validated feedback to Kafka."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, status

from .contracts import FeedbackEvent

app = FastAPI(title="Customer Feedback Ingestion API", version="1.0.0")
_write_lock = threading.Lock()


def _publish(event: FeedbackEvent) -> str:
    event_id = event.fingerprint()
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    topic = os.getenv("KAFKA_TOPIC", "feedback-events")
    if bootstrap_servers:
        producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "feedback-ingestion-api",
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        delivery_error: list[str] = []

        def delivered(error: object, _message: object) -> None:
            if error:
                delivery_error.append(str(error))

        producer.produce(
            topic,
            key=event.review_id,
            value=event.canonical_json(),
            on_delivery=delivered,
        )
        producer.flush(10)
        if delivery_error:
            raise RuntimeError(delivery_error[0])
        return event_id

    inbox = Path(os.getenv("LOCAL_INBOX", "runtime/api_inbox.jsonl"))
    inbox.parent.mkdir(parents=True, exist_ok=True)
    with _write_lock, inbox.open("a", encoding="utf-8") as target:
        target.write(event.canonical_json() + "\n")
    return event_id


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy"}


@app.post("/v1/feedback", status_code=status.HTTP_202_ACCEPTED)
def ingest_feedback(event: FeedbackEvent) -> dict[str, str]:
    try:
        event_id = _publish(event)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="event broker unavailable") from exc
    return {"status": "accepted", "event_id": event_id, "review_id": event.review_id}


@app.get("/v1/contract")
def contract() -> dict[str, object]:
    return json.loads(json.dumps(FeedbackEvent.model_json_schema()))
