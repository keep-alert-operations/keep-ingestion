"""Deterministic BoundedHttpToolsProtocol checks that feed data_received directly."""

import asyncio

import pytest
import uvicorn
from uvicorn.server import ServerState

from keep.ingestion.http_protocol import BoundedHttpToolsProtocol

INVALID = b"Invalid HTTP request received."


class Transport(asyncio.Transport):
    def __init__(self):
        super().__init__()
        self.written = []
        self.closed = False
        self.protocol = None

    def write(self, data):
        assert not self.closed, "write after close"
        self.written.append(bytes(data))

    def close(self):
        self.closed = True

    def is_closing(self):
        return self.closed

    def get_extra_info(self, name, default=None):
        return default

    def get_protocol(self):
        return self.protocol

    def set_protocol(self, protocol):
        self.protocol = protocol

    def pause_reading(self):
        pass

    def resume_reading(self):
        pass

    @property
    def output(self):
        return b"".join(self.written)


@pytest.fixture
def connection():
    loop = asyncio.new_event_loop()
    seen = []

    async def app(scope, receive, send):
        body = b""
        while True:
            message = await receive()
            if message["type"] != "http.request":
                break
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        seen.append((scope["path"], dict(scope["headers"]), len(body)))
        if scope["path"] == "/wait":
            await run.gate.wait()
        if scope["path"] == "/listen":  # like an SSE stream watching for disconnect
            run.events.append((await receive())["type"])
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-length", b"2")]})
        await send({"type": "http.response.body", "body": b"ok"})

    config = uvicorn.Config(app, http=BoundedHttpToolsProtocol, ws="none", lifespan="off",
                            log_level="critical")
    config.load()
    protocol = BoundedHttpToolsProtocol(config, ServerState(), {}, _loop=loop)
    transport = Transport()
    transport.protocol = protocol
    protocol.connection_made(transport)

    def run():
        for _ in range(20):
            loop.run_until_complete(asyncio.sleep(0))

    run.gate = asyncio.Event()  # holds the response to /wait until set
    run.events = []  # what /listen received after its body
    yield protocol, transport, seen, run
    for task in list(protocol.tasks):
        task.cancel()
    run()
    loop.close()


def _feed(protocol, *reads):
    for data in reads:
        protocol.data_received(data)


@pytest.mark.parametrize("bad", [
    b"GET /2 HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
    b"GET /2 HTTP/1.1\r\nHost: x\r\nX-Big: " + b"a" * 20 * 1024 + b"\r\n\r\n",
    b"GARBAGE\r\n\r\n",
], ids=["duplicate-host", "oversized-head", "parser-error"])
def test_invalid_pipelined_head_closes_without_writing_over_a_pending_response(
        connection, bad):
    protocol, transport, seen, run = connection
    _feed(protocol, b"POST /wait HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nok")
    run()
    assert [path for path, _, _ in seen] == ["/wait"]
    _feed(protocol, bad)
    run()
    assert transport.written == [] and transport.closed
    run.gate.set()
    run()
    assert transport.written == [] and [path for path, _, _ in seen] == ["/wait"]


WAIT = b"POST /wait HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nok"
NON_CHUNKED_CODING = b"POST /te HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\nhello"
BAD_CHUNK = b"POST /c HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\nzz\r\n"


@pytest.mark.parametrize("pending", [False, True], ids=["alone", "behind-wait"])
def test_non_chunked_final_coding_is_rejected_before_dispatch(connection, pending):
    protocol, transport, seen, run = connection
    _feed(protocol, *((WAIT,) if pending else ()), NON_CHUNKED_CODING)
    run()
    assert [path for path, _, _ in seen] == (["/wait"] if pending else [])
    assert transport.closed
    if pending:
        run.gate.set()
        run()
        assert transport.written == []
    else:
        assert transport.output.count(b"HTTP/1.1 ") == 1
        assert transport.output.startswith(b"HTTP/1.1 400 ") and transport.output.endswith(INVALID)


@pytest.mark.parametrize("fields", [
    b"Transfer-Encoding: gzip, chunked\r\n",
    b"Transfer-Encoding: chunked, chunked\r\n",
    b"Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n",
    b"Transfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n",
    b"Transfer-Encoding: chunked\t\r\n",  # llhttp itself rejects this only after dispatch
], ids=["gzip-chunked", "chunked-chunked", "two-fields", "two-fields-gzip-first",
        "trailing-tab"])
def test_transfer_encoding_must_be_exactly_chunked(connection, fields):
    protocol, transport, seen, run = connection
    _feed(protocol, b"POST /te HTTP/1.1\r\nHost: x\r\n" + fields + b"\r\n5\r\nhello\r\n0\r\n\r\n")
    run()
    assert seen == [] and transport.closed
    assert transport.output.count(b"HTTP/1.1 ") == 1
    assert transport.output.startswith(b"HTTP/1.1 400 ") and transport.output.endswith(INVALID)


def test_mixed_case_chunked_with_trailing_whitespace_is_accepted(connection):
    protocol, transport, seen, run = connection
    _feed(protocol, b"POST /te HTTP/1.1\r\nHost: x\r\nTransfer-Encoding:  ChUnKeD  \r\n\r\n"
                    b"5\r\nhello\r\n0\r\n\r\n")
    run()
    assert [(path, length) for path, _, length in seen] == [("/te", 5)]
    assert not transport.closed and transport.output.startswith(b"HTTP/1.1 200 ")


@pytest.mark.parametrize("pending", [False, True], ids=["alone", "behind-wait"])
def test_body_parse_error_gets_400_only_when_no_response_is_pending(connection, pending):
    protocol, transport, seen, run = connection
    _feed(protocol, *((WAIT,) if pending else ()), BAD_CHUNK)
    run()
    assert transport.closed
    if pending:
        run.gate.set()
        run()
        assert transport.written == []
    else:
        # The application saw a disconnect and wrote nothing; only the 400 went out.
        assert [(path, length) for path, _, length in seen] == [("/c", 0)]
        assert transport.output.count(b"HTTP/1.1 ") == 1
        assert transport.output.startswith(b"HTTP/1.1 400 ") and transport.output.endswith(INVALID)


def test_oversized_incomplete_head_behind_a_pending_response_closes_silently(connection):
    protocol, transport, seen, run = connection
    _feed(protocol, WAIT)
    run()
    _feed(protocol, b"GET /2 HTTP/1.1\r\nHost: x\r\nX-Big: " + b"a" * 200 * 1024)
    run()
    assert transport.closed and transport.written == []
    run.gate.set()
    run()
    assert transport.written == [] and [path for path, _, _ in seen] == ["/wait"]


def test_running_request_sees_disconnect_when_a_queued_one_is_rejected(connection):
    protocol, transport, seen, run = connection
    _feed(protocol, b"POST /listen HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nok")
    run()
    _feed(protocol, b"GET /queued HTTP/1.1\r\nHost: x\r\n\r\nGARBAGE\r\n\r\n")
    run()
    assert run.events == ["http.disconnect"]
    assert transport.closed and transport.written == [] and not protocol.tasks


def test_close_releases_an_application_blocked_on_write_flow_control(connection):
    protocol, transport, seen, run = connection
    _feed(protocol, WAIT)
    run()
    protocol.pause_writing()
    run.gate.set()
    run()
    assert protocol.tasks and transport.written == []  # parked in flow.drain()
    _feed(protocol, b"GARBAGE\r\n\r\n")
    run()
    assert transport.closed and transport.written == [] and not protocol.tasks


def test_oversized_incomplete_head_gets_exactly_one_400_and_close(connection):
    protocol, transport, seen, run = connection
    _feed(protocol, b"GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + b"a" * 200 * 1024)
    _feed(protocol, b"a" * 1024, b"\r\n\r\n")
    run()
    assert transport.closed
    assert transport.output.count(b"HTTP/1.1 400 ") == 1
    assert transport.output.endswith(INVALID)
    assert seen == []


def test_oversized_complete_head_in_one_read_gets_400(connection):
    protocol, transport, seen, run = connection
    _feed(protocol, b"GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + b"a" * 20 * 1024 + b"\r\n\r\n")
    run()
    assert transport.closed and transport.output.count(b"HTTP/1.1 400 ") == 1
    assert seen == []


@pytest.mark.parametrize("extra, accepted", [(0, True), (1, False)])
def test_complete_head_limit_is_exact(connection, extra, accepted):
    protocol, transport, seen, run = connection
    head = b"GET / HTTP/1.1\r\nHost: x\r\nX-Big: \r\n\r\n"
    head = head.replace(b"X-Big: ", b"X-Big: " + b"a" * (16 * 1024 + extra - len(head)))
    assert len(head) == 16 * 1024 + extra
    _feed(protocol, head)
    run()
    assert (not transport.closed and len(seen) == 1) is accepted
    assert transport.output.startswith(b"HTTP/1.1 200 " if accepted else b"HTTP/1.1 400 ")


def test_pipelined_heads_after_a_body_in_one_read_are_both_served(connection):
    protocol, transport, seen, run = connection
    big = b"a" * 15000
    first = b"POST /1 HTTP/1.1\r\nHost: x\r\nContent-Length: 12000\r\n\r\n" + b"b" * 12000
    second = b"GET /2 HTTP/1.1\r\nHost: x\r\nX-Big: " + big + b"\r\n\r\n"
    third = b"GET /3 HTTP/1.1\r\nHost: x\r\nX-Big: " + big + b"\r\n\r\n"
    data = first + second + third
    # The second head begins inside the first read, right after the first body.
    _feed(protocol, data[:16050], data[16050:21050], data[21050:])
    run()
    assert not transport.closed
    assert [path for path, _, _ in seen] == ["/1", "/2", "/3"]
    assert transport.output.count(b"HTTP/1.1 200 ") == 3


@pytest.mark.parametrize("split", [False, True])
def test_unsupported_upgrade_cannot_smuggle_a_request_in_its_body(connection, split):
    protocol, transport, seen, run = connection
    smuggled = (b"GET /smuggled HTTP/1.1\r\nHost: x\r\n"
                b"X-Consumer-Id: 11111111-1111-1111-1111-111111111111\r\n\r\n")
    body = b"\r\n" * 9000 + smuggled
    outer = (b"POST /outer HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: foo\r\n"
             b"Content-Length: %d\r\n\r\n" % len(body))
    _feed(protocol, *((outer, body) if split else (outer + body,)))
    run()
    assert seen == []
    assert transport.closed and transport.output.count(b"HTTP/1.1 ") == 1
    assert transport.output.startswith(b"HTTP/1.1 400 ")


@pytest.mark.parametrize("head", [
    b"POST / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: foo\r\n"
    b"Transfer-Encoding: chunked\r\n\r\n",
    b"CONNECT x:443 HTTP/1.1\r\nHost: x:443\r\n\r\n",
])
def test_chunked_upgrade_and_connect_are_rejected(connection, head):
    protocol, transport, seen, run = connection
    _feed(protocol, head + b"GET /smuggled HTTP/1.1\r\nHost: x\r\n\r\n")
    run()
    assert seen == [] and transport.closed
    assert transport.output.startswith(b"HTTP/1.1 400 ")


def test_bodiless_upgrade_is_served_and_the_rest_of_the_read_is_dropped(connection):
    protocol, transport, seen, run = connection
    _feed(protocol, b"GET /h2c HTTP/1.1\r\nHost: x\r\nConnection: Upgrade, HTTP2-Settings\r\n"
                    b"Upgrade: h2c\r\nHTTP2-Settings: AAMAAABkAARAAAAAAAIAAAAA\r\n"
                    b"Content-Length: 0\r\n\r\n" + b"\r\n" * 9000
                    + b"GET /dropped HTTP/1.1\r\nHost: x\r\n\r\n")
    run()
    assert [path for path, _, _ in seen] == ["/h2c"]
    assert transport.output.startswith(b"HTTP/1.1 200 ")


def test_oversized_trailer_section_closes_the_connection(connection):
    protocol, transport, seen, run = connection
    _feed(protocol, b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
                    b"5\r\nhello\r\n0\r\nX-Trailer: " + b"t" * 200 * 1024)
    _feed(protocol, b"\r\n\r\n")
    run()
    assert transport.closed
    assert b"HTTP/1.1 200 " not in transport.output  # the app saw a disconnect, not a body end


def test_small_trailers_are_ignored_and_the_body_is_served(connection):
    protocol, transport, seen, run = connection
    _feed(protocol, b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
                    b"5\r\nhello\r\n0\r\nX-Consumer-Id: spoofed\r\n\r\n")
    run()
    assert len(seen) == 1 and seen[0][2] == 5 and b"x-consumer-id" not in seen[0][1]
