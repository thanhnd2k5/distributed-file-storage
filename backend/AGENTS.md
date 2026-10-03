# Backend test gate — Distributed File Storage

Apply these rules when selecting, running, debugging or reporting backend tests
in this repository. Explicit user instructions take precedence. Read the relevant
contracts and milestone plan in `../docs/` before changing service behavior.

## Choose the smallest sufficient tier

- **No execution:** documentation, comments or changes with no runtime behavior.
- **Targeted tests:** validation, pure logic or a focused helper/service change.
  Run only the affected pytest file or test name, plus directly affected regressions.
- **Focused integration:** gRPC, filesystem persistence, database, transaction,
  startup recovery or worker lifecycle changes. Run the affected integration tests
  with their actual required dependencies.
- **Full validation:** an explicit user request, merge/release readiness, or a
  cross-cutting change to backend structure, dependency locks or deployment/build.
  Include lint/format, the backend suite and relevant smoke checks.

Do not rerun passing tests unless their code, dependencies, configuration or
environment changed, or a new failure raises a concrete regression concern.
Advancing to another phase alone does not justify full validation.

## Commands and dependencies

Native Python commands run from `backend/` with its Python 3.12 `.venv`:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_storage_p1.py
.\.venv\Scripts\python.exe -m ruff check common metadata storage scripts tests
.\.venv\Scripts\python.exe -m ruff format --check common metadata storage scripts tests
```

Select the actual affected test file/name; the P1 path above is an example.
Native gRPC loopback timed out during initial setup, so Docker Linux is the
verified integration environment. Do not repeatedly debug that host issue unless
it is relevant to the requested work.

Docker commands run from the repository root. Read-only preflight:

```powershell
docker info
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
```

Check only dependencies required by the selected tests. Storage tests using a
temporary directory and in-process gRPC server do not require live Storage
containers. PostgreSQL bootstrap tests require a reachable test DB; the Compose
tests service supplies TEST_DATABASE_URL and uses isolated schemas.

Focused integration example, using an image built from the current relevant code:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_storage_p1.py
```

Use `--no-deps` to avoid implicitly starting infrastructure during a test-only
request. PostgreSQL tests require postgres to be healthy before this command.
Rebuild the tests image when included code/dependencies/configuration changed;
do not reuse a stale image as evidence for new code.

Full suite and cluster smoke, when that tier is justified:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec metadata python scripts/smoke_base.py
```

Smoke requires healthy Metadata/PostgreSQL/Storage containers using the current
relevant build. Do not claim PostgreSQL integration passed if its tests skipped
because TEST_DATABASE_URL was missing.

## Infrastructure blockers

If preflight or the intended test reports unavailable Docker/PostgreSQL/required
services, stop that dependent test path immediately and report the first actionable
failure. A shared-infrastructure flake is also a blocker; do not retry the aggregate
suite or silently change infrastructure to make it pass.

For a test-only task, do not start/restart services, run migrations, remove volumes
or reset data automatically. Report this recovery command from the repository root:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait
```

Explain which prerequisite failed and whether setup/build is also missing. Wait
for the user to confirm readiness, then rerun only the intended test command once.
Continue independent work that does not depend on the blocked infrastructure.

If the user has already authorized setup, deployment configuration or environment
repair, perform the necessary actions within that scope without asking again.
This exception does not authorize deleting volumes or resetting unrelated data.
Separate an environment failure from an assertion/code failure; fix code failures
within the task, then rerun the affected tests.

## Report the decision and evidence

Before execution, state the selected tier, exact command and why it is sufficient.
Afterward, state what passed, failed or skipped, the environment used, and what was
intentionally not run. Do not report smoke, replication, persistence or failover as
verified unless the corresponding check actually ran successfully.
