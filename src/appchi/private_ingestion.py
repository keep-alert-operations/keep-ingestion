"""Private Kong listener. Public v1/product traffic uses the ordinary Uvicorn listener."""

import os
import ssl
import uvicorn
from appchi.ingestion.gateway import private_protocol


def main():
    os.environ["APPCHI_ROLE"] = "private-ingestion"
    required = (
        "APPCHI_GATEWAY_CERT_FILE",
        "APPCHI_GATEWAY_KEY_FILE",
        "APPCHI_GATEWAY_CA_FILE",
        "APPCHI_GATEWAY_PEER_SHA256",
    )
    values = {name: os.environ[name] for name in required}
    protocol = private_protocol(values["APPCHI_GATEWAY_PEER_SHA256"].split(","))
    uvicorn.run(
        "appchi.main:app",
        host=os.getenv("APPCHI_BIND_HOST", "127.0.0.1"),
        port=int(os.getenv("APPCHI_BIND_PORT", "8443")),
        http=protocol,
        ws="none",
        ssl_certfile=values["APPCHI_GATEWAY_CERT_FILE"],
        ssl_keyfile=values["APPCHI_GATEWAY_KEY_FILE"],
        ssl_ca_certs=values["APPCHI_GATEWAY_CA_FILE"],
        ssl_cert_reqs=ssl.CERT_REQUIRED,
        proxy_headers=False,
        access_log=False,
    )


if __name__ == "__main__":
    main()
