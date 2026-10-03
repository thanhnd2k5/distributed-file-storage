# Repository instructions

These instructions apply across the entire repository: frontend, backend,
deployment, scripts and documentation. Explicit user instructions take precedence.
Project scope, architecture and contracts are defined in `docs/`.

Read [backend/AGENTS.md](backend/AGENTS.md) for backend/deployment commands and
[frontend/AGENTS.md](frontend/AGENTS.md) for frontend conventions and checks.
The files below this root supplement this shared policy for their respective stacks.

## Test gate — choose the smallest sufficient tier

- **No execution:** documentation, copy, comments, static styling or changes with
  no runtime behavior. Run visual checks only when the task requires visual evidence.
- **Targeted tests/checks:** pure logic, validation, DTOs, mappers, priority rules
  or a focused component/helper/service change. Select the affected files or test
  names and directly affected regressions. Use checks available in that stack.
- **Focused integration/E2E:** HTTP/API wiring, gRPC, filesystem persistence,
  database, transactions, routing/state flows or worker lifecycle changes. Exercise
  only the affected boundary/flow with its actual required dependencies.
- **Full validation:** an explicit user request, merge/release readiness or a
  deliberately cross-cutting structure/dependency/build/infrastructure change.
  Validate the affected stacks and contracts; a backend-only change does not
  automatically require all frontend checks, or vice versa.

Do not rerun a passing check unless its code, dependencies, configuration or
environment changed, or a new failure raises a concrete regression concern.
Do not run full validation merely because a phase has progressed. Do not add a
test framework or invent a test command just to satisfy a tier label.

## Infrastructure preflight and blockers

Before infrastructure-dependent integration/E2E, perform a read-only preflight
for the selected check's prerequisites only. Use the stack-specific instructions
below; frontend unit/lint/build checks do not require Docker merely because the
backend uses it.

If Docker, PostgreSQL, an API service or another required dependency is unavailable:

1. Stop the dependent check immediately and report the exact failed prerequisite
   and appropriate existing recovery/setup command.
2. For a test-only task, do not start/restart services, run migrations, remove
   volumes or reset data automatically.
3. Wait for the user to confirm readiness, then rerun only the intended command
   once. Continue independent work that does not require the blocked dependency.

Treat shared-infrastructure flakes the same way: report the first actionable
failure and stop rather than retrying an aggregate suite. Separate environment
failures from assertion/code failures; fix code failures within the task and
rerun only the affected checks.

If the user has already authorized setup, deployment configuration or environment
repair, carry out necessary actions within that scope without asking again.
That authorization does not include deleting volumes or resetting unrelated data.

## Report the decision and evidence

Before execution, state the selected tier, exact command and why it is sufficient.
Afterward, state what passed, failed or skipped, the environment used, and what
was intentionally not run. A lint/build result is not runtime/E2E evidence.
Do not claim smoke, persistence, replication or failover passed unless the
corresponding check actually ran successfully. For no-execution changes, state
that checks were not run and why.
