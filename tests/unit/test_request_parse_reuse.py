"""Each admitted body is parsed once and its Kafka bytes stay exactly the original slices."""

import base64
import json
import time
from types import SimpleNamespace

import httpx
import pytest
from hypothesis import given, settings, strategies as st

import appchi.http as protocol
import appchi.ingestion.native as native
from appchi.ingestion import kafka_admission
from appchi.ingestion.kafka_admission import UNPARSED, _payload_slices, validate_request
from appchi.ingestion.main import create_app
from appchi.ingestion.native import canonical, object_slices, parse_json
from appchi.shared.errors import AppError

GLOBAL = "00000000-0000-4000-8000-000000000001"


def v1(application="app"):
    return {
        "application": application,
        "object": "cpu",
        "node_name": "n1",
        "message": 'firing é } ] " \\',
        "operator": "legacy",
        "time_created": "01-01-2026 00:00:00",
        "severity": "error",
    }


def v2(application="app"):
    return {
        "application": application,
        "component": "cpu",
        "message": "firing",
        "severity": "critical",
        "status": "firing",
        "site": "lab",
        "impact": "none",
        "runbook_url": "https://example.invalid/rb",
    }


def expected_slices(raw):
    parsed = parse_json(raw)
    slices = object_slices(raw)
    if "alerts" not in parsed:
        return [slices[()]]
    return [slices.get(("alerts", i, "payload"), b"") for i in range(len(parsed["alerts"]))]


def actual_slices(raw):
    parsed = parse_json(raw)
    if "alerts" in parsed:
        return _payload_slices(raw)
    # Capture the bytes handed to native validation whatever the payload's fields are.
    original = kafka_admission.validate_native
    kafka_admission.validate_native = lambda payload, version, raw: raw
    try:
        return [r[0] for r in validate_request(raw, "v1", parsed=parsed)]
    finally:
        kafka_admission.validate_native = original


SHAPES = [
    canonical(v1()),
    b"\r\n\t " + json.dumps(v1(), indent=3, ensure_ascii=False).encode() + b" \n",
    json.dumps(v1()).replace('"application"', '"\\u0061pplication"').encode(),
    json.dumps(
        {
            "alerts": [
                {"payload": v1("a")},
                {"payload": v1("b"), "event_id": "e-1"},
                {"event_id": "e-2", "payload": v1("c")},
                {"payload": "text"},
                {"payload": [1, {"x": "}"}]},
                "member",
                [{"payload": v1("hidden")}],
                {"payload": None},
                {"payload": dict(v1("d"), nested={"a": [1, {"b": "]"}]})},
                {"other": {"payload": v1("deep")}},
                {"payload": {}},
            ]
        },
        indent=2,
        ensure_ascii=False,
    ).encode(),
    b' { "\\u0061lerts" :\n[ {"event_id":"e1" , "pay\\u006coad" :\t'
    + canonical(v1("w1"))
    + b' } ,\r\n{ "payload":'
    + json.dumps(v1("w2"), indent=2).encode()
    + b"}\n] } ",
]


@pytest.mark.parametrize("raw", SHAPES)
def test_payload_bytes_equal_object_slices(raw):
    assert actual_slices(raw) == expected_slices(raw)


scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(-(10**20), 10**20),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(st.characters(blacklist_categories=("Cs",)), max_size=12),
)
values = st.recursive(
    scalars,
    lambda inner: st.one_of(
        st.lists(inner, max_size=4),
        st.dictionaries(st.text(max_size=6), inner, max_size=4),
    ),
    max_leaves=20,
)
members = st.one_of(
    values,
    st.dictionaries(st.sampled_from(["payload", "event_id", "pay", "x"]), values, max_size=3),
)


@settings(max_examples=300, deadline=None)
@given(
    st.lists(members, min_size=1, max_size=6),
    st.sampled_from([None, 0, 1, 4]),
    st.booleans(),
    st.sampled_from(["", " ", "\n\t ", "\r\n"]),
)
def test_payload_slices_match_object_slices_for_any_batch(alerts, indent, ascii_only, pad):
    raw = pad + json.dumps({"alerts": alerts}, indent=indent, ensure_ascii=ascii_only) + pad
    assert _payload_slices(raw.encode()) == expected_slices(raw.encode())


@settings(max_examples=200, deadline=None)
@given(
    st.dictionaries(st.text(max_size=6), values, max_size=5),
    st.sampled_from([None, 0, 2]),
    st.sampled_from(["", " ", "\n\t ", "\r\n"]),
)
def test_single_body_bytes_match_object_slices(value, indent, pad):
    value.pop("alerts", None)
    raw = (pad + json.dumps(value, indent=indent, ensure_ascii=False) + pad).encode()
    assert actual_slices(raw) == expected_slices(raw)


def test_admission_without_parsed_value_still_parses():
    calls = []

    def counted(raw):
        calls.append(raw)
        return parse_json(raw)

    raw = canonical(v1())
    original = kafka_admission.parse_json
    kafka_admission.parse_json = counted
    try:
        assert validate_request(raw, "v1")[0][1] == raw
        assert validate_request(raw, "v1", parsed=UNPARSED)[0][1] == raw
        assert validate_request(raw, "v1", parsed=parse_json(raw))[0][1] == raw
    finally:
        kafka_admission.parse_json = original
    assert len(calls) == 2


def test_parsed_value_does_not_bypass_size_limit():
    raw = b" " * (4 * 1024 * 1024 + 1)
    with pytest.raises(AppError) as error:
        validate_request(raw, "v1", parsed={})
    assert error.value.status == 413


class Publisher:
    def __init__(self):
        self.records = []

    async def publish(self, topic, key, value):
        self.records.append((topic, key, value))


@pytest.fixture
def ingest(monkeypatch):
    calls = []
    real = native.parse_json

    def counted(raw):
        calls.append(raw)
        return real(raw)

    # Every module that can parse a request body counts toward the same total.
    monkeypatch.setattr(protocol, "parse_json", counted)
    monkeypatch.setattr(kafka_admission, "parse_json", counted)
    settings_ = SimpleNamespace(
        environment="development",
        role="product-api",
        clock_health_file=None,
        database_clock_health_file=None,
        kafka_admission_enabled=True,
        global_tenant_id=GLOBAL,
    )
    app = create_app(settings_)
    app.state.settings = settings_
    publisher = Publisher()
    app.state.kafka_publisher = publisher

    async def asgi(scope, receive, send):
        if scope["type"] == "http" and scope["path"].startswith("/api/ingest/v2/"):
            scope = dict(scope, **{"appchi.gateway_peer_verified": True})
        await app(scope, receive, send)

    return SimpleNamespace(app=asgi, calls=calls, publisher=publisher)


def gateway_headers():
    return {
        "x-consumer-id": "00000000-0000-4000-8000-0000000000c1",
        "x-credential-identifier": "00000000-0000-4000-8000-0000000000c2",
        "x-appchi-operator-id": "00000000-0000-4000-8000-0000000000aa",
        "x-appchi-tenant-id": "00000000-0000-4000-8000-0000000000bb",
        "x-appchi-policy-generation": "7",
        "x-appchi-routing-names": base64.b64encode(b'["op"]').decode(),
        "x-appchi-authorized-at": str(time.time()),
    }


async def post(ingest, path, body, **headers):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingest.app), base_url="https://ingest.test"
    ) as client:
        return await client.post(
            path, content=body, headers={"content-type": "application/json", **headers}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["v1", "v2"])
@pytest.mark.parametrize(
    ("shape", "status", "records"),
    [
        ("single", 202, 1),
        ("pretty-single", 202, 1),
        ("batch", 207, 2),
        ("invalid-json", 400, 0),
        ("array", 400, 0),
        ("oversized-single", 413, 0),
    ],
)
async def test_one_parse_per_request(ingest, version, shape, status, records):
    make = v1 if version == "v1" else v2
    headers = {}
    if version == "v2":
        pytest.importorskip("appchi.ingestion.gateway")
        headers = gateway_headers()
    body = {
        "single": canonical(make()),
        "pretty-single": b"\n " + json.dumps(make(), indent=2).encode() + b"\n",
        "batch": json.dumps(
            {"alerts": [{"payload": make("a")}, {"payload": {"bad": 1}}, {"payload": make("b")}]},
            indent=1,
        ).encode(),
        "invalid-json": b'{"alerts": [',
        "array": b"[]",
        "oversized-single": json.dumps(dict(make(), message="x" * 70000)).encode(),
    }[shape]
    response = await post(ingest, f"/api/ingest/{version}/alerts", body, **headers)
    assert response.status_code == status
    assert len(ingest.calls) == 1
    assert len(ingest.publisher.records) == records
    if records:
        published = [
            json.loads(value)["raw_json"].encode() for _, _, value in ingest.publisher.records
        ]
        valid = [
            s for s, payload in zip(expected_slices(body), ("a", "bad", "b")) if payload != "bad"
        ]
        assert published == (valid if shape == "batch" else expected_slices(body))


@pytest.mark.asyncio
async def test_non_json_and_oversized_bodies_are_rejected_before_parsing(ingest):
    text = await post(ingest, "/api/ingest/v1/alerts", b"{}", **{"content-type": "text/plain"})
    assert text.status_code == 415
    large = await post(ingest, "/api/ingest/v1/alerts", b" " * (4 * 1024 * 1024 + 1))
    assert large.status_code == 413
    assert large.json()["error"]["code"] == "body_too_large"
    assert not ingest.calls
