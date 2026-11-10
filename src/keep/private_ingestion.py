"""Private Kong listener. Public v1/product traffic uses the ordinary Uvicorn listener."""

import os
import ssl
import uvicorn
from keep.ingestion.gateway import private_protocol


def main():
    os.environ["KEEP_ROLE"] = "private-ingestion"
    required = (
        "KEEP_GATEWAY_CERT_FILE",
        "KEEP_GATEWAY_KEY_FILE",
        "KEEP_GATEWAY_CA_FILE",
        "KEEP_GATEWAY_PEER_SHA256",
    )
    values = {name: os.environ[name] for name in required}
    protocol = private_protocol(values["KEEP_GATEWAY_PEER_SHA256"].split(","))
    uvicorn.run(
        "keep.ingestion.main:app",
        host=os.getenv("KEEP_BIND_HOST", "127.0.0.1"),
        port=int(os.getenv("KEEP_BIND_PORT", "8443")),
        http=protocol,
        ws="none",
        ssl_certfile=values["KEEP_GATEWAY_CERT_FILE"],
        ssl_keyfile=values["KEEP_GATEWAY_KEY_FILE"],
        ssl_ca_certs=values["KEEP_GATEWAY_CA_FILE"],
        ssl_cert_reqs=ssl.CERT_REQUIRED,
        proxy_headers=False,
        access_log=False,
    )


if __name__ == "__main__":
    main()
