"""Stateless Ingestion API; Kafka admission has no SQL/search/Redis lifecycle."""

import os
import json
import logging
from keep.observability import configure_logging, log_event, start_metrics_server
from keep.visibility.profile import capture_profile
from contextlib import asynccontextmanager
from fastapi import Request
from fastapi.responses import JSONResponse
from keep.config import Settings
from keep.http import create_http_app
from keep.ingestion.kafka_admission import UNPARSED, KafkaAdmission, KafkaPublisher
from keep.ingestion.schemas import request_schema, admission_responses
from keep.shared.errors import unavailable

log = logging.getLogger(__name__)


def create_app(settings=None):
    @asynccontextmanager
    async def lifespan(app):
        role = settings.role if settings else os.getenv("KEEP_ROLE", "ingestion-api")
        configure_logging("ingestion", role=role)
        start_metrics_server()
        app.state.settings = settings or Settings(role=role)
        if app.state.settings.role not in ("ingestion-api", "private-ingestion"):
            raise RuntimeError("Ingestion requires a public or private ingestion role")
        path = app.state.settings.capacity_profile
        app.state.profile = json.loads(path.read_text()) if path else None
        capture_profile(app.state.profile, deployment_epoch=app.state.settings.deployment_epoch)
        if (app.state.profile and app.state.settings.environment != "development"
                and not app.state.profile.get("qualified")):
            raise RuntimeError("A qualified capacity profile is required outside development")
        app.state.db = None
        app.state.kafka_publisher = KafkaPublisher(app.state.settings)
        log_event(log, "process_started", role=role)
        try:
            yield
        finally:
            log_event(log, "process_stopping", role=role, reason="shutdown")
            app.state.kafka_publisher.close()

    app = create_http_app(lifespan)

    def parsed_body(request):
        # The protocol middleware already parsed and validated this body. A domain
        # without that state makes admission parse it again rather than fail.
        return getattr(request.state, "json_body", UNPARSED)

    def admission(request):
        settings = request.app.state.settings
        if not settings.kafka_admission_enabled:
            raise unavailable("kafka_cutover_required")
        if not request.app.state.kafka_publisher or not settings.global_tenant_id:
            raise unavailable("kafka_admission_unconfigured")
        return KafkaAdmission(
            request.app.state.kafka_publisher,
            environment=settings.environment,
            global_tenant_id=settings.global_tenant_id,
        )

    @app.post(
        "/api/ingest/v1/alerts",
        tags=["ingestion"],
        status_code=202,
        responses=admission_responses(),
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {"application/json": {"schema": request_schema("v1")}},
            }
        },
    )
    async def ingest_v1(request: Request):
        status, result = await admission(request).admit(
            await request.body(),
            "v1",
            request.headers.get("idempotency-key"),
            received_at=request.state.received_at,
            parsed=parsed_body(request),
        )
        result["request_id"] = request.state.request_id
        return JSONResponse(result, status)

    @app.post(
        "/api/ingest/v1/grafana",
        tags=["ingestion"],
        status_code=202,
        responses=admission_responses(),
    )
    async def ingest_grafana_v1(request: Request):
        # Grafana's standard webhook mapped to native v1 for Global (REV-77).
        status, result = await admission(request).admit_grafana_v1(
            await request.body(),
            request.headers.get("idempotency-key"),
            received_at=request.state.received_at,
        )
        result["request_id"] = request.state.request_id
        return JSONResponse(result, status)

    @app.post(
        "/api/ingest/v2/alerts",
        tags=["ingestion"],
        status_code=202,
        responses=admission_responses(),
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {"application/json": {"schema": request_schema("v2")}},
            }
        },
    )
    async def ingest_v2(request: Request):
        from keep.ingestion.gateway import gateway_identity

        gateway = gateway_identity(request)
        status, result = await admission(request).admit(
            await request.body(),
            "v2",
            request.headers.get("idempotency-key"),
            gateway,
            received_at=request.state.received_at,
            parsed=parsed_body(request),
        )
        result["request_id"] = request.state.request_id
        return JSONResponse(result, status)

    @app.post(
        "/api/ingest/v2/grafana",
        tags=["ingestion"],
        status_code=202,
        responses=admission_responses(),
    )
    async def ingest_grafana(request: Request):
        from keep.ingestion.gateway import gateway_identity

        gateway = gateway_identity(request)
        status, result = await admission(request).admit_grafana(
            await request.body(),
            request.headers.get("idempotency-key"),
            gateway,
            received_at=request.state.received_at,
        )
        result["request_id"] = request.state.request_id
        return JSONResponse(result, status)

    return app


app = create_app()
