# Backend testing commands — Distributed File Storage

Follow the repository-wide test gate in [../AGENTS.md](../AGENTS.md).
This file supplies backend/deployment commands and environment details; it does
not restrict the shared policy to backend work. Read the relevant contracts and
milestone plan in `../docs/` before changing service behavior.

## Native Python checks

Run from `backend/` using its Python 3.12 `.venv`:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_storage_p1.py
.\.venv\Scripts\python.exe -m ruff check common metadata storage scripts tests
.\.venv\Scripts\python.exe -m ruff format --check common metadata storage scripts tests
```

Select the actual affected test file/name; P1 is an example, not a required suite
for every change. Narrow lint/format to affected paths when sufficient.
Native gRPC loopback timed out during initial setup. Docker Linux is the verified
integration environment; do not repeatedly debug that host issue unless relevant
to the requested work.

## Docker integration

Run Docker commands from the repository root. Read-only preflight:

```powershell
docker info
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
```

Check only required dependencies. Storage tests using a temporary directory and
in-process gRPC server do not need live Storage containers. PostgreSQL bootstrap
tests require a reachable test DB; the Compose tests service supplies
TEST_DATABASE_URL and uses isolated schemas. Missing TEST_DATABASE_URL and skipped
PostgreSQL tests are not passing database evidence.

Focused integration example:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_storage_p1.py
```

Use `--no-deps` so a test-only request does not implicitly start infrastructure.
PostgreSQL tests require postgres to be healthy before running. Rebuild the tests
image when included code/dependencies/configuration changed; do not treat a stale
image as evidence for new code.

Full backend suite and cluster smoke, when justified by the root test gate:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec metadata python scripts/smoke_base.py
```

Smoke requires healthy Metadata/PostgreSQL/Storage containers using the current
relevant build. If infrastructure is blocked, follow the root blocker policy and
report this recovery command from the repository root, noting any missing env/build:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait
```
