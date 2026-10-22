import os
import uvicorn

from appchi.ingestion.http_protocol import BoundedHttpToolsProtocol


def main():
    os.environ["APPCHI_ROLE"] = "ingestion-api"
    uvicorn.run("appchi.ingestion.main:app", host=os.getenv("APPCHI_BIND_HOST", "127.0.0.1"),
                port=int(os.getenv("APPCHI_BIND_PORT", "8000")),
                http=BoundedHttpToolsProtocol, proxy_headers=False, access_log=False)


if __name__ == "__main__":
    main()
