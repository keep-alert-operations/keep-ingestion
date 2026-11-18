"""Grafana's standard webhook admitted as native v1 alerts into Global (REV-77)."""

import json
from types import SimpleNamespace

import httpx
import pytest

from keep.ingestion.main import create_app

GLOBAL = "00000000-0000-4000-8000-000000000001"


def member(**update):
    alert = {
        "status": "firing",
        "startsAt": "2026-11-18T09:30:05Z",
        "generatorURL": "https://grafana.example.org/alerting/grafana/abc/view",
        "labels": {
            "alertname": "Disk full",
            "application": "billing",
            "object": "disk",
            "node_name": "db-03",
            "operator": "nagios-prod",
            "severity": "major",
        },
        "annotations": {"message": "Disk is 95% full"},
    }
    return {**alert, **update}


def body(*alerts):
    return json.dumps(
        {"version": "1", "status": "firing", "truncatedAlerts": 0, "alerts": list(alerts)}
    ).encode()


class Publisher:
    def __init__(self):
        self.records = []

    async def publish(self, topic, key, value):
        self.records.append((topic, key, value))


@pytest.fixture
def ingest():
    settings = SimpleNamespace(
        environment="development",
        role="ingestion-api",
        clock_health_file=None,
        database_clock_health_file=None,
        kafka_admission_enabled=True,
        global_tenant_id=GLOBAL,
    )
    app = create_app(settings)
    app.state.settings = settings
    app.state.kafka_publisher = Publisher()
    return app


async def post(app, raw, **headers):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://ingest.test"
    ) as client:
        return await client.post(
            "/api/ingest/v1/grafana",
            content=raw,
            headers={"content-type": "application/json", **headers},
        )


def published(app):
    return [json.loads(value) for _, _, value in app.state.kafka_publisher.records]


@pytest.mark.asyncio
async def test_firing_member_is_published_as_global_v1_from_grafana(ingest):
    """Requirements: REV-77; operator operations 30; alert operations 10."""
    response = await post(ingest, body(member()))
    assert response.status_code == 202
    [record] = published(ingest)
    assert record["native_version"] == "v1"
    assert record["tenant_id"] == GLOBAL
    assert record["operator_id"] is None and record["credential_id"] is None
    assert record["adapter_version"] == "keep-grafana-native-v1"
    assert record["payload"]["object"] == "disk"
    assert record["payload"]["severity"] == "major"
    assert json.loads(record["raw_json"])["generatorURL"].startswith("https://grafana.")


@pytest.mark.asyncio
async def test_resolved_member_is_published_as_clear(ingest):
    """Requirements: REV-77; operator operations 30; alert operations 4."""
    response = await post(ingest, body(member(status="resolved")))
    assert response.status_code == 202
    assert published(ingest)[0]["payload"]["severity"] == "clear"


@pytest.mark.asyncio
async def test_missing_label_is_rejected_by_name_and_valid_members_still_publish(ingest):
    """Requirements: REV-77; operator operations 30; alert operations 11."""
    labels = {k: v for k, v in member()["labels"].items() if k != "object"}
    response = await post(ingest, body(member(), member(labels=labels)))
    assert response.status_code == 207
    results = response.json()["results"]
    assert results[0]["status"] == 202
    assert results[1]["status"] == 422
    assert results[1]["error"]["fields"] == [{"path": "labels.object"}]
    assert len(published(ingest)) == 1


@pytest.mark.asyncio
async def test_single_invalid_member_returns_422(ingest):
    """Requirements: REV-77; operator operations 30; alert operations 11."""
    response = await post(ingest, body(member(status="suspended")))
    assert response.status_code == 422
    assert not published(ingest)


@pytest.mark.asyncio
async def test_route_needs_no_operator_credential(ingest):
    """Requirements: REV-77; operator operations 30 (unauthenticated like native v1)."""
    response = await post(ingest, body(member()), authorization="Bearer ignored")
    assert response.status_code == 202
    assert published(ingest)[0]["operator_id"] is None
