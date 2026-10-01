# appchi-ingestion

Stateless native v1/v2 and Grafana admission to Kafka, with the Appchi Kong policy plugin.

See [system design](https://github.com/appchi-alert-operations/appchi-design), [workspace setup](https://github.com/appchi-alert-operations/appchi-delivery) and [source provenance](SOURCE.md).

## Development

Use Python 3.13 and the committed `uv.lock`. Configure Git authentication for the
private organization repositories before resolving internal dependencies.

```powershell
uv sync --locked --all-extras
uv run --locked ruff check src tests
uv run --locked pytest
uv build --wheel
```

Shared `appchi` modules use namespace packaging; each module has exactly one owning
distribution. Dependencies resolve immutable Git revisions in Python dependency metadata and `uv.lock`.
For coordinated local edits use the delivery workspace setup, which installs local
packages without changing release dependency pins.

## Execution

`uv run --locked appchi-ingestion` is the entrypoint. Supply the documented environment
bindings; package installation does not start services or migrate data.

Use `appchi-private-ingestion` for the separate verified mTLS v2 listener. The public listener accepts v1; route paths and trusted gateway checks are unchanged. No SQL/search/Redis runtime is required.
