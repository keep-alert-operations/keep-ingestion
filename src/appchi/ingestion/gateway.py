"""Gateway identity comes exclusively from a verified TLS connection, never headers alone."""

from dataclasses import dataclass
from hashlib import sha256
import ssl
from uuid import UUID
from uvicorn.protocols.http.h11_impl import H11Protocol

from appchi.shared.errors import AppError


@dataclass(frozen=True)
class GatewayIdentity:
    consumer_id: str
    credential_id: str
    verified: bool = True


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
    return GatewayIdentity(*values)


def private_protocol(allowed_fingerprints):
    approved = frozenset(allowed_fingerprints)
    if not approved or any(
        len(value) != 64 or any(c not in "0123456789abcdef" for c in value) for value in approved
    ):
        raise ValueError("Configured gateway certificate SHA-256 fingerprints are required")

    class PrivateGatewayH11(H11Protocol):
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

    return PrivateGatewayH11
