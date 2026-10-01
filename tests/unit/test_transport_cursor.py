from datetime import datetime, timedelta, timezone
from uuid import uuid4
import pytest
import base64
import time
from starlette.requests import Request
from appchi.ingestion.gateway import gateway_identity, private_protocol
from appchi.shared.cursors import Cursor
from appchi.shared.errors import AppError


def test_cursor_continuations_keep_original_absolute_expiry(cipher):
    now = datetime.now(timezone.utc)
    first = Cursor(cipher, {"scope": "a"}, now).encode("first")
    page = Cursor(cipher, {"scope": "a"}, now + timedelta(minutes=14))
    page.decode(first)
    continued = page.encode("second")
    with pytest.raises(AppError, match="cursor_expired"):
        Cursor(cipher, {"scope": "a"}, now + timedelta(minutes=16)).decode(continued)


def test_gateway_headers_alone_never_authenticate():
    ids = [str(uuid4()), str(uuid4())]
    scope = {
        "type": "http",
        "headers": [
            (b"x-consumer-id", ids[0].encode()),
            (b"x-credential-identifier", ids[1].encode()),
            (b"x-appchi-operator-id", ids[0].encode()),
            (b"x-appchi-tenant-id", str(uuid4()).encode()),
            (b"x-appchi-policy-generation", b"1"),
            (b"x-appchi-routing-names", base64.b64encode(b'["Operator"]')),
            (b"x-appchi-authorized-at", str(time.time()).encode()),
        ],
    }
    with pytest.raises(AppError):
        gateway_identity(Request(scope))
    result = gateway_identity(Request({**scope, "appchi.gateway_peer_verified": True}))
    assert result.consumer_id == ids[0]
    for extra in [
        (b"x-api-key", b"secret"),
        (b"authorization", b"Bearer secret"),
        scope["headers"][0],
    ]:
        with pytest.raises(AppError):
            gateway_identity(
                Request(
                    {
                        **scope,
                        "headers": scope["headers"] + [extra],
                        "appchi.gateway_peer_verified": True,
                    }
                )
            )
    with pytest.raises(ValueError):
        private_protocol([])
