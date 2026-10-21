"""Uvicorn's httptools protocol with the request checks the h11 parser applied."""

from uvicorn.protocols.http.httptools_impl import HttpToolsProtocol

# h11's default maximum incomplete event size, which uvicorn's h11 protocol applied.
DEFAULT_MAX_INCOMPLETE_HEAD = 16 * 1024
INVALID_REQUEST = "Invalid HTTP request received."
_OWS = b" \t"
# Head bytes besides method, target and fields: " ", " HTTP/1.1", CRLF and the final CRLF.
_REQUEST_LINE_OVERHEAD = 14


class InvalidRequestHead(Exception):
    """Raised from a parser callback; httptools reports it as an HttpParserError (400)."""


class BoundedHttpToolsProtocol(HttpToolsProtocol):
    """HttpToolsProtocol that keeps the request rules of uvicorn's h11 protocol.

    httptools buffers an incomplete head without limit, keeps trailing whitespace in
    field values, appends chunked trailer fields to the request headers, accepts a
    missing or repeated Host, and after an unsupported Upgrade or CONNECT skips the
    declared body and parses the following bytes as a new request. This class:

    - rejects a head larger than ``h11_max_incomplete_event_size`` (16 KiB by
      default), and a chunk-size line or trailer section that grows past it;
    - strips trailing whitespace from field values and ignores trailer fields;
    - rejects an HTTP/1.1 request without exactly one Host;
    - rejects CONNECT, and an unsupported Upgrade that declares a body, and stops
      parsing the read after a bodiless unsupported Upgrade as uvicorn does.

    Violations get uvicorn's usual 400 reply, or a close when a response to an
    earlier request on the connection is still in progress. Reads are fed to the
    parser in slices no longer than the limit and checked after each slice.
    """

    def __init__(self, config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.max_incomplete_head = (
            config.h11_max_incomplete_event_size or DEFAULT_MAX_INCOMPLETE_HEAD
        )
        self.in_message = False
        self.head_complete = True
        self.head_known = 0  # head bytes seen through completed URL and field callbacks
        self.head_bytes = 0  # bound on the incomplete head fed so far
        self.head_began_in_slice = False
        self.body_progress = False
        self.stalled_bytes = 0  # body-phase bytes fed without body data (chunk lines, trailers)
        self.stop_reading = False

    # Parser callbacks
    def on_message_begin(self):
        super().on_message_begin()
        self.in_message = True
        self.head_complete = False
        self.head_known = 0
        self.head_bytes = 0
        self.head_began_in_slice = True
        self.stalled_bytes = 0

    def on_url(self, url):
        self.head_known += len(url)
        super().on_url(url)

    def on_header(self, name, value):
        if not self.head_complete:  # trailer fields never reach the application
            value = value.rstrip(_OWS)
            self.head_known += len(name) + len(value) + 4
            super().on_header(name, value)

    def on_headers_complete(self):
        self.head_complete = True
        method = self.parser.get_method()
        size = len(method) + len(self.url) + _REQUEST_LINE_OVERHEAD
        size += sum(len(name) + len(value) + 4 for name, value in self.headers)
        if size > self.max_incomplete_head:
            raise InvalidRequestHead("Request head exceeds the configured limit")
        hosts = sum(1 for name, _ in self.headers if name == b"host")
        if hosts > 1 or (hosts == 0 and self.parser.get_http_version() == "1.1"):
            raise InvalidRequestHead("HTTP/1.1 requires exactly one Host header")
        if self.parser.should_upgrade() and not self._should_upgrade():
            # llhttp skips the declared body of an upgrade and would parse it as a request.
            if method == b"CONNECT" or self._declares_body():
                raise InvalidRequestHead("Unsupported upgrade with a request body")
            self.stop_reading = True
        super().on_headers_complete()

    def on_body(self, body):
        self.body_progress = True
        super().on_body(body)

    def on_message_complete(self):
        self.in_message = False
        super().on_message_complete()

    def _declares_body(self):
        for name, value in self.headers:
            if name == b"transfer-encoding" or (name == b"content-length" and value != b"0"):
                return True
        return False

    def _reject(self):
        self.in_message = False
        self.logger.warning(INVALID_REQUEST)
        cycle = self.cycle
        if cycle is not None and not cycle.response_complete:
            # A 400 could interleave with a response the application may still send.
            cycle.disconnected = True
            cycle.message_event.set()
            self.transport.close()
        else:
            self.send_400_response(INVALID_REQUEST)

    def data_received(self, data):
        if self.transport.is_closing():  # e.g. TLS data still flushing after a 400
            return
        view = memoryview(data)
        step = self.max_incomplete_head
        for start in range(0, len(view), step):
            piece = view[start:start + step]
            self.body_progress = False
            super().data_received(piece)
            if self.transport.is_closing() or self.parser is None:
                return
            if self.stop_reading:  # uvicorn drops the rest of a read after an upgrade
                self.stop_reading = False
                self.head_began_in_slice = False
                return
            if self.head_began_in_slice:
                self.head_began_in_slice = False
                if not self.head_complete:
                    # Only bytes after on_message_begin count; the field still being
                    # read is covered by the next slice's full count.
                    self.head_bytes = self.head_known
                continue
            if not self.in_message:
                continue
            if not self.head_complete:
                self.head_bytes += len(piece)
                over = self.head_bytes > self.max_incomplete_head
            elif self.body_progress:
                self.stalled_bytes = 0
                over = False
            else:
                self.stalled_bytes += len(piece)
                over = self.stalled_bytes > self.max_incomplete_head
            if over:
                self._reject()
                return
