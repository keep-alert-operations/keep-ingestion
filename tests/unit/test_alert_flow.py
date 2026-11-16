"""The steps of one submitted alert inside ingestion, as log records."""

import asyncio
import json
from uuid import uuid4

import pytest

from keep.ingestion.kafka_admission import KafkaAdmission
from keep.observability.testing import capture_records

TENANT = uuid4()
V1 = {
    "application": "billing",
    "object": "database",
    "node_name": "db-1",
    "message": "disk is full",
    "operator": "legacy",
    "time_created": "01-01-2026 00:00:00",
    "severity": "error",
}


class Publisher:
    """Stands in for the Kafka publisher: confirms, or fails a number of times first."""

    def __init__(self, failures=0, error=RuntimeError("kafka_publish_unconfirmed")):
        self.failures, self.error, self.published = failures, error, []

    async def publish(self, topic, key, value):
        if self.failures:
            self.failures -= 1
            raise self.error
        self.published.append(json.loads(value))
        return 3, 1200 + len(self.published)


@pytest.fixture
def records():
    return capture_records("ingestion", "ingestion-api")


def admit(publisher, body):
    admission = KafkaAdmission(publisher, environment="test", global_tenant_id=TENANT)
    return asyncio.run(admission.admit(json.dumps(body).encode(), "v1"))


def steps(records):
    return [(r["event"]["sequence"], r["event"]["action"]) for r in records]


def test_an_accepted_alert_leaves_received_validated_and_published_in_order(records):
    """Requirements: REV-74; observability 3."""
    publisher = Publisher()
    status, _ = admit(publisher, V1)
    assert status == 202
    received, validated, published = records()
    assert steps([received, validated, published]) == [
        (1, "alert_received"),
        (2, "alert_validated"),
        (3, "alert_published"),
    ]
    delivery = publisher.published[0]["event_id"]
    for record in (received, validated, published):
        assert record["keep"]["delivery_id"] == delivery
        assert len(record["keep"]["alert"]["key_digest"]) == 64
    for record in (received, validated, published):
        alert = record["keep"]["alert"]
        assert (alert["application"], alert["object"], alert["severity"]) == (
            "billing",
            "database",
            "error",
        )
        assert record["keep"]["tenant_id"] == str(TENANT)
        assert record["service"]["name"] == "ingestion"
        # The message text of the alert is on whole-alert records only.
        assert "message" not in alert
    assert published["keep"]["kafka"] == {
        "topic": "keep.test.alerts.received",
        "partition": 3,
        "offset": 1201,
    }
    assert published["keep"]["attempt"] == 1
    assert published["message"] == f"Kafka confirmed delivery {delivery} on attempt 1"


def test_a_rejected_alert_is_recorded_with_what_was_sent_and_what_was_wrong(records):
    """Requirements: REV-74; observability 3 and 4."""
    body = {"alerts": [{"payload": V1}, {"payload": {**V1, "severity": "loud", "surprise": 1}}]}
    status, result = admit(Publisher(), body)
    assert status == 207
    rejected = [r for r in records() if r["event"]["action"] == "alert_rejected"]
    (record,) = rejected
    assert record["keep"]["member"] == 1 and record["keep"]["count"] == 2
    assert record["error"]["code"] == result["results"][1]["error"]["code"]
    assert record["event"]["outcome"] == "failure"
    alert = record["keep"]["alert"]
    assert (alert["application"], alert["severity"], alert["message"]) == (
        "billing",
        "loud",
        "disk is full",
    )
    # A name the sender invented never becomes a log field.
    assert "surprise" not in alert
    assert "delivery_id" not in record["keep"]


def test_missing_and_unknown_fields_are_named_on_the_rejection(records):
    """Requirements: REV-74; observability 3. The record says what to fix."""
    sent = {name: value for name, value in V1.items() if name != "time_created"}
    status, result = admit(Publisher(), {**sent, "surprise": "x"})
    assert status == 422
    assert result["results"][0]["error"]["code"] == "invalid_fields"
    record = records()[-1]
    assert record["event"]["action"] == "alert_rejected"
    assert record["keep"]["fields"] == ["time_created", "surprise"]
    assert "surprise" not in record["keep"]["alert"]


def test_each_failed_attempt_and_the_final_confirmation_are_recorded(records):
    """Requirements: REV-74; observability 3."""
    status, _ = admit(Publisher(failures=2), V1)
    assert status == 202
    actions = [r["event"]["action"] for r in records()]
    assert actions == [
        "alert_received",
        "alert_validated",
        "alert_publish_failed",
        "alert_publish_failed",
        "alert_published",
    ]
    failed = records()[2]
    assert failed["log"]["level"] == "warning"
    assert (failed["keep"]["attempt"], failed["keep"]["reason"]) == (1, "unconfirmed")
    assert records()[-1]["keep"]["attempt"] == 3


def test_an_unconfirmed_alert_is_recorded_with_the_whole_alert(records):
    """Requirements: REV-74; observability 3."""
    status, _ = admit(Publisher(failures=3, error=TimeoutError()), V1)
    assert status == 503
    final = records()[-1]
    assert final["event"]["action"] == "alert_unconfirmed"
    assert final["event"]["outcome"] == "failure"
    assert final["keep"]["reason"] == "attempts"
    assert final["keep"]["alert"]["message"] == "disk is full"
    assert [r["keep"]["reason"] for r in records()[2:5]] == ["timeout"] * 3


def test_outcome_only_setting_keeps_one_record_for_an_accepted_alert(records, monkeypatch):
    """Requirements: REV-74; observability 3 and 10."""
    from keep.observability import logs

    monkeypatch.setattr(logs, "DETAIL", False)
    admit(Publisher(), V1)
    assert [r["event"]["action"] for r in records()] == ["alert_published"]
