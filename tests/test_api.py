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
