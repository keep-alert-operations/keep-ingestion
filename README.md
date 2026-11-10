# keep-ingestion

Stateless native v1/v2 and Grafana admission to Kafka, with the Keep Kong policy plugin.

See [system design](https://github.com/keep-alert-operations/keep-design), [workspace setup](https://github.com/keep-alert-operations/keep-delivery) and [source provenance](SOURCE.md).

## Development

Use Python 3.13, Poetry 2.3.1 and the committed `poetry.lock`. Configure Git authentication for the
private organization repositories before resolving internal dependencies.

```powershell
poetry check --lock
poetry sync --all-extras
poetry run ruff check src tests
poetry run pytest
poetry build --format wheel
```

Shared `keep` modules use namespace packaging; each module has exactly one owning
distribution. Dependencies resolve immutable Git revisions in Python dependency metadata and `poetry.lock`.
For coordinated local edits use the delivery workspace setup, which installs local
packages without changing release dependency pins.

## Execution

`poetry run keep-ingestion` is the entrypoint. Supply the documented environment
bindings; package installation does not start services or migrate data.

Use `keep-private-ingestion` for the separate verified mTLS v2 listener. The public listener accepts v1; route paths and trusted gateway checks are unchanged. No SQL/search/Redis runtime is required.
