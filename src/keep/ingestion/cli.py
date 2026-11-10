import os
import uvicorn

from keep.ingestion.http_protocol import BoundedHttpToolsProtocol


def main():
    os.environ["KEEP_ROLE"] = "ingestion-api"
    uvicorn.run("keep.ingestion.main:app", host=os.getenv("KEEP_BIND_HOST", "127.0.0.1"),
                port=int(os.getenv("KEEP_BIND_PORT", "8000")),
                http=BoundedHttpToolsProtocol, proxy_headers=False, access_log=False)


if __name__ == "__main__":
    main()
