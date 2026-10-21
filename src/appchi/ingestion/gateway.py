"""Gateway identity comes exclusively from a verified TLS connection, never headers alone."""

from dataclasses import dataclass
from hashlib import sha256
import base64
import json
import ssl
import math
import time
from uuid import UUID

from appchi.ingestion.http_protocol import BoundedHttpToolsProtocol
from appchi.shared.errors import AppError


@dataclass(frozen=True)
class GatewayIdentity:
    consumer_id: str
    credential_id: str
    verified: bool = True
    operator_id: str | None = None
    tenant_id: str | None = None
    policy_generation: str | None = None
    routing_names: tuple[str, ...] = ()
    authorized_at: float | None = None


def gateway_identity(request):
    if not request.scope.get("appchi.gateway_peer_verified"):
        raise AppError("invalid_credential", "Verified private gateway connection required.", 401)
    values = []
    for header in ("x-consumer-id", "x-credential-identifier"):
        found = request.headers.getlist(header)
        try:
            if len(found) != 1 or str(UUID(found[0])) != found[0]:
                raise ValueError()
        except ValueError:
            raise AppError("invalid_credential", "Invalid gateway identity.", 401) from None
        values.append(found[0])
    if request.headers.getlist("x-api-key") or request.headers.getlist("authorization"):
        raise AppError("invalid_credential", "Gateway credential stripping is required.", 401)
    context = []
    for header in ("x-appchi-operator-id", "x-appchi-tenant-id"):
        found = request.headers.getlist(header)
        try:
            if len(found) != 1 or str(UUID(found[0])) != found[0]:
                raise ValueError()
        except ValueError:
            raise AppError("invalid_credential", "Invalid gateway policy context.", 401) from None
        context.append(found[0])
    generations = request.headers.getlist("x-appchi-policy-generation")
    names = request.headers.getlist("x-appchi-routing-names")
    authorized = request.headers.getlist("x-appchi-authorized-at")
    try:
        if len(authorized) != 1:
            raise ValueError()
        authorized_at = float(authorized[0])
        if not math.isfinite(authorized_at) or not -2 <= time.time() - authorized_at < 10:
            raise ValueError()
        if (
            len(generations) != 1
            or not generations[0].isascii()
            or not generations[0].isdigit()
            or not 0 < int(generations[0]) <= 2**63 - 1
            or str(int(generations[0])) != generations[0]
            or len(names) != 1
        ):
            raise ValueError()
        routing_names = json.loads(base64.b64decode(names[0], validate=True))
        if (
            not isinstance(routing_names, list)
            or not 1 <= len(routing_names) <= 2
            or any(not isinstance(n, str) or not 1 <= len(n) <= 128 for n in routing_names)
        ):
            raise ValueError()
    except (ValueError, TypeError):
        raise AppError("invalid_credential", "Invalid gateway policy context.", 401) from None
    return GatewayIdentity(
        *values,
        operator_id=context[0],
        tenant_id=context[1],
        policy_generation=generations[0],
        routing_names=tuple(routing_names),
        authorized_at=authorized_at,
    )


def private_protocol(allowed_fingerprints):
    approved = frozenset(allowed_fingerprints)
    if not approved or any(
        len(value) != 64 or any(c not in "0123456789abcdef" for c in value) for value in approved
    ):
        raise ValueError("Configured gateway certificate SHA-256 fingerprints are required")

    class PrivateGatewayHttpTools(BoundedHttpToolsProtocol):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            original = self.app

            async def verified_app(scope, receive, send):
                ssl_object = self.transport.get_extra_info("ssl_object")
                peer = ssl_object.getpeercert(binary_form=True) if ssl_object else None
                verified = bool(
                    ssl_object
                    and ssl_object.context.verify_mode == ssl.CERT_REQUIRED
                    and peer
                    and sha256(peer).hexdigest() in approved
                )
                scope = {**scope, "appchi.gateway_peer_verified": verified}
                await original(scope, receive, send)

            self.app = verified_app

    return PrivateGatewayHttpTools
