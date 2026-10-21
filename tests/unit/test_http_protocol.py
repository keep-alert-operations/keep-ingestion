import json
import socket
import threading
import time

import pytest
import uvicorn
from uvicorn.protocols.http.httptools_impl import HttpToolsProtocol

from appchi.ingestion.gateway import private_protocol
from appchi.ingestion.http_protocol import BoundedHttpToolsProtocol


async def _echo(scope, receive, send):
    body = b""
    while True:
        message = await receive()
        if message["type"] != "http.request":
            break
        body += message.get("body", b"")
        if not message.get("more_body"):
            break
    payload = json.dumps(
        {
            "headers": [[k.decode("latin1"), v.decode("latin1")] for k, v in scope["headers"]],
            "body": len(body),
            "verified": scope.get("appchi.gateway_peer_verified"),
        }
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json"),
                        (b"content-length", str(len(payload)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": payload})


def _serve(protocol):
    config = uvicorn.Config(_echo, host="127.0.0.1", port=0, http=protocol, ws="none",
                            lifespan="off", log_level="critical", access_log=False)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not (server.started and server.servers):
        if time.monotonic() > deadline or not thread.is_alive():
            pytest.fail("Uvicorn did not start")
        time.sleep(0.02)
    return server, thread, server.servers[0].sockets[0].getsockname()[1]


@pytest.fixture(scope="module", params=["bounded", "gateway"])
def port(request):
    protocol = BoundedHttpToolsProtocol if request.param == "bounded" else private_protocol(["0" * 64])
    server, thread, bound = _serve(protocol)
    yield bound
    server.should_exit = True
    thread.join(10)


def _read_response(conn):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(65536)
        if not chunk:
            return data, None
        data += chunk
    head, _, rest = data.partition(b"\r\n\r\n")
    length = next(
        int(line.split(b":", 1)[1]) for line in head.split(b"\r\n")
        if line.lower().startswith(b"content-length:")
    )
    while len(rest) < length:
        chunk = conn.recv(65536)
        if not chunk:
            break
        rest += chunk
    status = int(head.split(b" ", 2)[1])
    return head, (status, rest[:length])


def _exchange(port, *parts, pause=0.0):
    with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
        for index, part in enumerate(parts):
            if index and pause:
                time.sleep(pause)
            conn.sendall(part)
        _, result = _read_response(conn)
        return result


def _json(result):
    status, body = result
    assert status == 200, body
    return json.loads(body)


def test_listeners_use_httptools(port):
    assert issubclass(BoundedHttpToolsProtocol, HttpToolsProtocol)
    assert issubclass(private_protocol(["0" * 64]), BoundedHttpToolsProtocol)
    data = _json(_exchange(port, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
    assert data["headers"] == [["host", "x"]]


def test_private_listener_marks_plaintext_peers_unverified(port):
    data = _json(_exchange(port, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
    assert data["verified"] in (None, False)


def test_header_values_lose_trailing_whitespace_and_names_are_lowercase(port):
    data = _json(_exchange(port, b"GET / HTTP/1.1\r\nHost: x\r\nX-Consumer-ID:  abc \t \r\n"
                                 b"X-A: 1\r\nx-a: 2\r\n\r\n"))
    assert data["headers"] == [["host", "x"], ["x-consumer-id", "abc"], ["x-a", "1"], ["x-a", "2"]]


def test_chunked_body_is_delivered_and_trailers_are_ignored(port):
    data = _json(_exchange(
        port,
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n",
        b"6\r\n world\r\n0\r\nX-Consumer-Id: spoofed\r\n\r\n",
        pause=0.05,
    ))
    assert data["body"] == 11
    assert ["x-consumer-id", "spoofed"] not in data["headers"]


@pytest.mark.parametrize("request_bytes", [
    b"GET / HTTP/1.1\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n",
    b"GARBAGE\r\n\r\n",
])
def test_invalid_heads_get_400(port, request_bytes):
    status, body = _exchange(port, request_bytes)
    assert (status, body) == (400, b"Invalid HTTP request received.")


def test_http10_without_host_is_accepted(port):
    with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
        conn.sendall(b"GET / HTTP/1.0\r\n\r\n")
        data = b""
        while chunk := conn.recv(65536):
            data += chunk
    assert data.startswith(b"HTTP/1.1 200 ")


def test_head_split_across_reads_within_limit_is_accepted(port):
    head = b"GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + b"a" * 15000 + b"\r\n\r\n"
    data = _json(_exchange(port, head[:500], head[500:], pause=0.1))
    assert len(data["headers"][1][1]) == 15000


def test_large_body_is_not_counted_as_head(port):
    body = b"z" * (256 * 1024)
    request = b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n" % len(body)
    data = _json(_exchange(port, request, body[:1024], body[1024:], pause=0.05))
    assert data["body"] == len(body)


def test_oversized_incomplete_head_is_rejected(port):
    with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
        conn.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nX-Big: ")
        sent = 0
        try:
            while sent < 4 * 1024 * 1024:
                conn.sendall(b"a" * 1024)
                sent += 1024
                time.sleep(0.002)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass
        try:
            _, result = _read_response(conn)
        except (ConnectionResetError, ConnectionAbortedError):
            result = None
    assert sent < 4 * 1024 * 1024
    assert result in (None, (400, b"Invalid HTTP request received."))


@pytest.mark.parametrize("size", [20 * 1024, 200 * 1024])
def test_oversized_head_in_one_write_is_rejected_before_it_completes(port, size):
    with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
        conn.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + b"a" * size)
        time.sleep(0.3)
        try:
            conn.sendall(b"\r\n\r\n")
            _, result = _read_response(conn)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            result = None
    assert result in (None, (400, b"Invalid HTTP request received."))


def test_complete_heads_in_one_large_write_are_accepted(port):
    head = b"GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + b"a" * 15000 + b"\r\n\r\n"
    data = _json(_exchange(port, head + head))
    assert len(data["headers"][1][1]) == 15000


def test_keep_alive_serves_sequential_requests(port):
    with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
        for path in (b"/1", b"/2"):
            conn.sendall(b"GET " + path + b" HTTP/1.1\r\nHost: x\r\n\r\n")
            _, result = _read_response(conn)
            assert result[0] == 200
