"""Kafka-first HTTP admission. No database access or application retry queue."""

import asyncio
from datetime import UTC, datetime
from time import monotonic, time
from uuid import UUID, uuid4

from appchi.ingestion.native import (
    RETRY_ID,
    canonical,
    format_native_time,
    identity,
    object_slices,
    parse_json,
    validate_native,
)
from appchi.shared.errors import AppError
from appchi.shared.transactions import wire


def validate_request(raw, version, request_key=None):
    if len(raw) > 4 * 1024 * 1024:
        raise AppError("body_too_large", "Ingestion body exceeds 4 MiB.", 413)
    parsed = parse_json(raw)
    if not isinstance(parsed, dict):
        raise AppError("invalid_envelope", "Expected an object.", 400)
    batch = "alerts" in parsed
    if batch and (set(parsed) != {"alerts"} or not isinstance(parsed["alerts"], list)):
        raise AppError("invalid_envelope", "Expected only an alerts array.", 400)
    if batch and not 1 <= len(parsed["alerts"]) <= 100:
        raise AppError("batch_size", "Batches require 1–100 members.", 413)
    if not batch and len(raw) > 65536:
        raise AppError("body_too_large", "Single objects are limited to 64 KiB.", 413)
    validate_legacy_key(request_key)
    slices = object_slices(raw)
    members = parsed["alerts"] if batch else [{"payload": parsed}]
    results = []
    for index, member in enumerate(members):
        try:
            if (
                not isinstance(member, dict)
                or set(member) - {"payload", "event_id"}
                or "payload" not in member
            ):
                raise AppError(
                    "invalid_wrapper", "Each member requires payload and optional event_id."
                )
            if "event_id" in member:
                validate_legacy_key(member["event_id"], required=True)
            original = slices.get(("alerts", index, "payload") if batch else (), b"")
            results.append((validate_native(member["payload"], version, original), original, None))
        except AppError as error:
            results.append(
                (None, b"", {"code": error.code, "message": error.message, "fields": error.fields})
            )
    return results


def validate_legacy_key(value, *, required=False):
    # Accepted migration syntax only. It provides no resubmission deduplication.
    if (required or value is not None) and (
        not isinstance(value, str) or not RETRY_ID.fullmatch(value)
    ):
        raise AppError("invalid_retry_id", "Invalid legacy sender identifier.", 422)


def received_key(tenant, application, key):
    return canonical([str(tenant), application, key])


class KafkaPublisher:
    def __init__(self, settings):
        from confluent_kafka import Producer
        from appchi.service_clients import kafka_options

        if not settings.kafka_bootstrap:
            raise ValueError("Kafka admission requires a broker binding")
        self.producer = Producer(
            {
                **kafka_options(settings),
                "enable.idempotence": True,
                "acks": "all",
                "max.in.flight.requests.per.connection": 5,
                "delivery.timeout.ms": 1000,
                "request.timeout.ms": 1000,
                "linger.ms": 0,
            }
        )

    async def publish(self, topic, key, value):
        outcomes = []
        self.producer.produce(
            topic, key=key, value=value, on_delivery=lambda error, message: outcomes.append(error)
        )
        deadline = monotonic() + 1.1
        while not outcomes and monotonic() < deadline:
            self.producer.poll(0)
            await asyncio.sleep(0.005)
        if not outcomes or outcomes[0] is not None:
            raise RuntimeError("kafka_publish_unconfirmed")

    def close(self):
        self.producer.flush(1.1)


class KafkaAdmission:
    def __init__(self, publisher, *, environment, global_tenant_id):
        self.publisher = publisher
        self.topic = f"appchi.{environment}.alerts.received"
        self.global_tenant_id = UUID(str(global_tenant_id))

    async def admit(self, raw, version, request_key=None, gateway=None, *, received_at=None):
        at = received_at or datetime.now(UTC)
        routing = self._routing(version, gateway)
        members = validate_request(raw, version, request_key)
        return await self._publish(members, version, routing, at, deadline=self._deadline(gateway))

    async def admit_grafana(self, raw, request_key, gateway, *, received_at=None):
        from appchi.operators.grafana import ADAPTER_VERSION, OperatorRouting, map_webhook

        at = received_at or datetime.now(UTC)
        routing = self._routing("v2", gateway)
        validate_legacy_key(request_key)
        names = gateway.routing_names
        mapped = map_webhook(
            raw,
            OperatorRouting(
                names[0], names[1] if len(names) == 2 else None, len(names) == 2, len(names) == 2
            ),
        )
        return await self._publish(
            [(m.payload, m.original_bytes, m.error) for m in mapped],
            "v2",
            routing,
            at,
            ADAPTER_VERSION,
            deadline=self._deadline(gateway),
        )

    @staticmethod
    def _deadline(gateway):
        authorized = getattr(gateway, "authorized_at", None)
        return (
            monotonic() + min(3.4, max(0, authorized + 10 - time()))
            if authorized is not None
            else monotonic() + 3.4
        )

    def _routing(self, version, gateway):
        if version == "v1":
            return self.global_tenant_id, None, None, None
        if (
            gateway is None
            or not gateway.verified
            or not gateway.tenant_id
            or not gateway.operator_id
            or not gateway.policy_generation
        ):
            raise AppError("invalid_credential", "Verified Kong policy context required.", 401)
        return (
            UUID(gateway.tenant_id),
            UUID(gateway.operator_id),
            gateway.credential_id,
            gateway.policy_generation,
        )

    async def _publish(self, members, version, routing, at, adapter=None, *, deadline=None):
        from appchi.event_models import KafkaReceivedEnvelope

        tenant, operator, credential, generation = routing

        async def member_result(index, member):
            payload, original, error = member
            if error:
                return {"index": index, "status": 422, "disposition": "rejected", "error": error}
            payload = dict(payload)
            defaulted = "time_created" not in payload
            if defaulted:
                payload["time_created"] = format_native_time(at.replace(microsecond=0))
            key, _, material = identity(payload, version, operator)
            delivery = str(uuid4())
            body = KafkaReceivedEnvelope.model_validate(
                {
                    "schema_version": 2,
                    "kind": "alerts.received",
                    "event_id": delivery,
                    "tenant_id": str(tenant),
                    "operator_id": str(operator) if operator else None,
                    "credential_id": credential,
                    "policy_generation": generation,
                    "native_version": version,
                    "received_at": wire(at),
                    "payload": payload,
                    "raw_json": original.decode("utf-8"),
                    "time_defaulted": defaulted,
                    "key_field": key,
                    "identity_material": material.decode("utf-8"),
                    "adapter_version": adapter,
                }
            )
            encoded = canonical(body.model_dump())
            partition_key = received_key(tenant, payload["application"], key)
            for _ in range(3):
                try:
                    remaining = min(1.1, deadline - monotonic())
                    if remaining <= 0:
                        break
                    await asyncio.wait_for(
                        self.publisher.publish(self.topic, partition_key, encoded), remaining
                    )
                    return {
                        "index": index,
                        "status": 202,
                        "disposition": "accepted",
                        "received_at": wire(at),
                    }
                except Exception:
                    # An uncertain acknowledgement can still leave a record in Kafka.
                    # Every attempt deliberately reuses the exact UUID/time/bytes.
                    pass
            return {
                "index": index,
                "status": 503,
                "disposition": "publish_failed",
                "error": {
                    "code": "kafka_publish_unconfirmed",
                    "message": "Kafka publication was not confirmed after three attempts.",
                    "retryable": True,
                },
            }

        results = await asyncio.gather(
            *(member_result(i, member) for i, member in enumerate(members))
        )
        statuses = {r["status"] for r in results}
        status = (
            202
            if statuses == {202}
            else 422
            if statuses == {422}
            else 503
            if 202 not in statuses and 503 in statuses
            else 207
        )
        return status, {"request_id": str(uuid4()), "results": results}
