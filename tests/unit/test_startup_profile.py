import json
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from appchi.config import Settings
from appchi.ingestion.main import create_app


@pytest.mark.parametrize("content", ['{broken', json.dumps({"visibility": {}}), json.dumps({"qualified": False})])
def test_invalid_supplied_profile_fails_before_kafka_start(content, tmp_path, monkeypatch):
    profile = tmp_path / "profile.json"
    profile.write_text(content)
    settings = Settings(role="ingestion-api", environment="staging", deployment_epoch="test",
                        kafka_bootstrap="unused:9092", kafka_security_protocol="SSL",
                        global_tenant_id=uuid4(), capacity_profile=profile)
    monkeypatch.setattr("appchi.ingestion.main.KafkaPublisher",
                        lambda *_: pytest.fail("Invalid profile reached Kafka startup"))
    with pytest.raises((ValueError, RuntimeError)):
        with TestClient(create_app(settings)):
            pytest.fail("Invalid profile started serving")
