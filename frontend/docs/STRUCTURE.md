# Structure & module conventions (Base B)

For humans and AI agents working in `react-starter-kit-zq`.  
Stack summary: [BASE_B.md](./BASE_B.md).

## Folder map

| Path | Role |
|------|------|
| `src/api/` | HTTP only (`apiAxios`). One folder per domain. `queryKeys.js` for TanStack keys. |
| `src/stores/` | Zustand client global state (`useAuthStore`, `useUiStore`, …). |
| `src/hooks/` | **Shared** hooks only (auth facade, debounce, dropzone, permissions, …). Not feature CRUD. |
| `src/hooks/queries/` | Shared/cross-route TanStack Query hooks. Feature-only queries may live under `pages/.../hooks/` if unused elsewhere. |
| `src/app/AuthBootstrap.jsx` | Calls `useAuthStore.initialize()` once. |
| `src/app/queryClient.js` | Shared `QueryClient` (`retry: 1`, `refetchOnWindowFocus: false`). |
| `src/app/pages/` | Route screens + **feature modules** (colocated components/hooks/schemas). See Pages below. |
| `src/app/layouts/`, `src/app/router/` | Shell + route tables. |
| `src/app/contexts/` | Theme / locale / sidebar / breakpoint only. **No auth Context.** |
| `src/middleware/` | Route guards (`AuthGuard`, `GhostGuard`). |
| `src/components/ui\|shared\|template` | Same split as Base A. |
| `src/utils/` | Pure helpers (`jwt`, `localStorage`, `permissions`). |

**No `src/states/`** — Redux removed. Do not re-add toolkit/saga unless product explicitly migrates back.

Imports: `baseUrl: "src"` → `import x from "stores/…"`.

## Auth (locked)

- Source of truth: `stores/useAuthStore` (`initialize`, `login`, `logout`).
- Bootstrap: `AuthBootstrap` only (idempotent init in store).
- Session: `authToken` via `utils/jwt`. Same as Base A.
- Login: **`username` + `password`** (switch to email per PROJECT_BOOTSTRAP if BE needs it).
- Default locale: **`vi`** (+ `en`).
- Pages/guards/profiles: **`useAuthActions`** (`doLogin` / `doLogout` / flags).
- `errorMessage`: `string | null`.
- Token orphan rules: clear `setSession(null)` on init/login/getMe failures.
- `useMeQuery` is a **sample refetch** — does not replace `initialize` / `login`.
- Do **not** reintroduce Auth Context or Redux auth.

## Where state goes

| Kind | Put it in |
|------|-----------|
| Session / current user / login flow | `stores/useAuthStore` |
| Client UI global (modals, palette, flags) | `stores/useUiStore` or new `stores/useXxxStore.js` |
| Server lists/details/cache (shared) | `hooks/queries/useXxxQuery.js` + keys in `api/queryKeys.js` |
| Page orchestration / forms | `pages/<domain>/<feature>/hooks/` |
| Theme / sidebar open / locale | existing `app/contexts/*` (do not move to Zustand unless asked) |

## Pages (feature modules)

Starter demos under `app/pages` are thin (Auth / home / settings). **Real features follow the CMS-style colocated module** — do not dump everything into `index.jsx` or into root `src/hooks/`.

### Template

```
src/app/pages/<domain>/<feature>/
├── index.jsx                 # route entry: wire controller → UI (keep thin)
├── detail.jsx                # optional second route
├── constants.js              # optional
├── components/               # feature UI only (list, modals, tabs)
├── hooks/                    # page-local: controller, actions, form, optional queries
│   ├── use<Feature>PageController.js
│   ├── use<Feature>Actions.js
│   └── use<Feature>Form.js
├── schemas/                  # yup/zod for this feature
└── utils/                    # optional pure helpers
```

Nested subfeatures (tabs) repeat the same contract:

```
…/<feature>/allocations/
├── AllocationsTab.jsx
├── components/
├── hooks/
└── schemas/
```

### Hook placement

| Hook type | Location |
|-----------|----------|
| List/filter/modal orchestration | `pages/.../hooks/useXxxPageController.js` |
| Mutations / screen actions | `pages/.../hooks/useXxxActions.js` |
| Form state + validation wiring | `pages/.../hooks/useXxxForm.js` |
| Server fetch used only here | `pages/.../hooks/useXxxQuery.js` **or** `src/hooks/queries/` if reused |
| Cross-app (auth, permissions, S3, debounce) | `src/hooks/` |
| HTTP | `src/api/<domain>/` — **never** page-level `services/` |

`pages/**/data/` is only for **mock/demo**. Live data → `api/` + Query.

### Page rules

- `index.jsx` stays thin: call controller hook, render components.
- Do not put feature CRUD hooks in `src/hooks/` unless 2+ unrelated domains reuse them.
- Shared UI primitives stay in `components/ui|shared`; feature chrome stays under the page folder.
- Register routes in `app/router/*`; nav entries in `app/navigation` when needed.

### Colocate first, then promote (shared extraction)

Default while building a feature: keep code under `pages/<domain>/<feature>/`.  
**Before finishing the PR / when copying the same snippet into a second screen**, pause and promote.

| Kind | Keep in page when… | Promote to… when… |
|------|--------------------|-------------------|
| UI | Tied to this feature’s copy/layout/API shape | Generic, domain-agnostic, usable in 2+ features → `components/shared/` (composites) or `components/ui/` (primitive) |
| Hook | Knows this feature’s routes, fields, API/query keys | Generic behavior (debounce, click-outside, table selection) → `src/hooks/`; shared server fetch → `src/hooks/queries/` |
| Util / format / parse | Uses feature enums or API DTO quirks | Pure + no feature imports → `src/utils/` |
| Schema | Field set is this form only | Shared validation helpers only → tiny pure helpers in `utils`; form schema stays with the page |
| Constant | Feature labels / column ids | App-wide paths/keys → `src/constants/` or `src/configs/` |

**Promote checklist (any of these → extract):**
1. Same component/hook/util appears (or is about to appear) in a **second** feature/domain.
2. Name makes sense **without** the feature name (`ConfirmDialog`, `useDebouncedValue`, `formatDate` — not `UserConfirmDialog` unless user-specific).
3. No imports from `pages/...` or feature-only API/store after move.
4. Props/args are generic (data in, callbacks out) — not hard-coded to one endpoint.

**Do not promote early:** one-off modals, page controllers, feature action hooks, schemas that mirror one API payload. Premature shared = fake abstractions and circular mess.

**After promote:** update imports; export from `hooks/index.js` only for widely used shared hooks; leave a thin page wrapper only if the feature still needs a tiny adapter.

**Agent behavior:** do not silently keep borderline-shared code forever in the page. After implementing (or mid-task if obvious), **propose** extract candidates to the user (`Promote?: path ← reason`). Only auto-extract when the user already asked for shared / approved. See also root `AGENTS.md`.

## How to add a feature module (checklist)

1. **API** — `src/api/<domain>/index.js` with `apiAxios`.
2. **Query keys** — extend `api/queryKeys.js` (`domainKeys.list`, `domainKeys.detail(id)`, …).
3. **Server state** — shared: `src/hooks/queries/…`; page-only: under `pages/.../hooks/`. Prefer Query for server data; do not duplicate into Zustand unless truly client-only.
4. **Client global (optional)** — `src/stores/use<Domain>Store.js` only for UI/session-like client state.
5. **Page module** — `src/app/pages/<domain>/<feature>/` with `components/` + `hooks/` + `schemas/` (template above).
6. **Routes** — `app/router/*` (+ navigation if needed).
7. **Permissions** — `usePermissions` / `utils/permissions` with `user` from `useAuthActions`.

## Flexibility (defaults, not dogma)

**Locked (no exceptions unless product explicitly changes the kit):**
- One auth source (Zustand `useAuthStore`)
- Session key `authToken` + `setSession` / clear-on-failure
- Login field `username` (not email)
- No Auth Context / no Redux reintroduction
- HTTP in `src/api/`; Query does not replace auth initialize/login

**Conventions (prefer these; bend when the feature is truly small or one-off):**
- Page template (`components/` + `hooks/` + `schemas/`) — skip folders for a tiny screen (e.g. static error, or a single form with under ~80 lines of logic in `index.jsx`).
- `useXxxPageController` — only when orchestration is non-trivial; a simple list can use one `useXxx` hook.
- Nested tab modules — only when a tab has its own forms/actions; a thin tab can be one component.
- Shared vs page hooks — if unsure, **colocate first**; promote to `src/hooks/` or `hooks/queries/` when a second domain needs it (see “Colocate first, then promote”).
- New Zustand store — only for real client-global UI/session state; don’t mirror server lists into stores.

Rule of thumb for agents: **match the template for CRUD/list features; keep auth locks absolute; don’t invent a third state system.**

## Do / don’t

| Do | Don’t |
|----|--------|
| One auth path (Zustand store) | AuthProvider Context or Redux |
| Query for server cache | Use Query as login/init source of truth |
| Colocate page hooks under `pages/.../hooks/` | Put feature CRUD in root `src/hooks/` |
| New domain under `api/` + queries | Stuff API calls in components without hooks |
| Keep `useAuthActions` shape for UI | Call `useAuthStore` getters scattered in every page (bootstrap/store internals OK) |
| Clear token on auth failures | Leave orphan `authToken` |
| Thin `index.jsx` + controller | Fat page files with inline CRUD |
