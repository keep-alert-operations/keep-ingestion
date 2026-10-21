"""Uvicorn's httptools protocol with the request-head checks the h11 parser applied."""

from uvicorn.protocols.http.httptools_impl import HttpToolsProtocol

# h11's default maximum incomplete event size, which uvicorn's h11 protocol applied.
DEFAULT_MAX_INCOMPLETE_HEAD = 16 * 1024
INVALID_REQUEST = "Invalid HTTP request received."
_OWS = b" \t"


class InvalidRequestHead(Exception):
    """Raised from a parser callback; httptools reports it as an HttpParserError (400)."""


class BoundedHttpToolsProtocol(HttpToolsProtocol):
    """HttpToolsProtocol that keeps the request-head behaviour of uvicorn's h11 protocol.

    httptools buffers an incomplete head without limit, keeps trailing whitespace in
    field values, appends chunked trailer fields to the request headers and accepts
    a missing or repeated Host. h11 rejected an incomplete head larger than
    ``h11_max_incomplete_event_size`` (16 KiB by default), stripped trailing
    whitespace, ignored trailers and rejected an HTTP/1.1 request without exactly one
    Host. This class restores those rules; violations get uvicorn's usual 400 reply.
    """

    def __init__(self, config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.max_incomplete_head = (
            config.h11_max_incomplete_event_size or DEFAULT_MAX_INCOMPLETE_HEAD
        )
        self.head_complete = True
        self.head_bytes = 0  # bytes read for the current head after the read that began it
        self.head_began_in_read = False

    def on_message_begin(self):
        super().on_message_begin()
        self.head_complete = False
        self.head_bytes = 0
        self.head_began_in_read = True

    def on_header(self, name, value):
        if not self.head_complete:  # trailer fields never reach the application
            super().on_header(name, value.rstrip(_OWS))

    def on_headers_complete(self):
        self.head_complete = True
        hosts = sum(1 for name, _ in self.headers if name == b"host")
        if hosts > 1 or (hosts == 0 and self.parser.get_http_version() == "1.1"):
            raise InvalidRequestHead("HTTP/1.1 requires exactly one Host header")
        super().on_headers_complete()

    def data_received(self, data):
        super().data_received(data)
        if self.head_complete or self.transport.is_closing():
            return
        if self.head_began_in_read:
            self.head_began_in_read = False
            return
        self.head_bytes += len(data)
        if self.head_bytes > self.max_incomplete_head:
            self.head_complete = True
            self.logger.warning(INVALID_REQUEST)
            self.send_400_response(INVALID_REQUEST)
