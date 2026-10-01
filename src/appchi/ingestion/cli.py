import os
import uvicorn


def main():
    os.environ["APPCHI_ROLE"] = "ingestion-api"
    uvicorn.run("appchi.ingestion.main:app", host=os.getenv("APPCHI_BIND_HOST", "127.0.0.1"),
                port=int(os.getenv("APPCHI_BIND_PORT", "8000")),
                proxy_headers=False, access_log=False)


if __name__ == "__main__":
    main()
