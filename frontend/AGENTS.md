# Agent notes — Base B (Zustand + Query)

## Repository-wide test gate

Follow the shared test selection, infrastructure blocker and evidence rules in
[../AGENTS.md](../AGENTS.md). They apply to frontend work as well.

Run frontend commands from `frontend/`. For affected JavaScript/JSX files, use
`npm exec -- eslint <affected-files>` with actual paths. Use `npm run build` when
the change requires compilation/import/bundle evidence; `npm run lint` checks
the whole frontend only when that scope is justified. The current package has
no dedicated test/E2E script; do not invent one or describe lint/build as tests.
For runtime UI/API changes, verify the affected flow with available tooling.
API-dependent flows require the API to be ready; local lint/build does not require
Docker. No checks are needed for documentation-only changes.

Before changing code, read:

1. [docs/BASE_B.md](docs/BASE_B.md) — stack locks (auth, session, username; locale vi)
2. [docs/STRUCTURE.md](docs/STRUCTURE.md) — folders, pages, promote-to-shared, do/don’t
3. [../PROJECT_BOOTSTRAP.md](../PROJECT_BOOTSTRAP.md) — when forking a new project

Follow STRUCTURE checklists. Do not re-add Redux or Auth Context. Do not use TanStack Query as the auth initialize/login source of truth.

## Be proactive (do not stay silent)

When implementing a feature, **do not only dump everything under the page** and move on.

1. Prefer colocate for the first use.
2. While coding / before finishing, **scan** for UI, hooks, or utils that look reusable (generic name, no feature API/query coupling).
3. **Propose** to the user in the reply (short bullet list): what to extract, target path (`components/shared`, `src/hooks`, `hooks/queries`, `src/utils`), and why — then wait for OK **unless** they already said to extract freely.
4. If the user already approved promote / said “làm shared luôn”, extract in the same pass.
5. Never invent a shared abstraction that only one screen needs.

End-of-task habit: if anything was borderline reusable, mention it under a **Promote?** note even when you left it colocated.
