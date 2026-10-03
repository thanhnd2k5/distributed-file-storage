# Agent notes — Base B (Zustand + Query)

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
