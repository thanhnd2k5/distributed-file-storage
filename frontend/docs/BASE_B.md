# Base B (react-starter-kit-zq)

Zustand auth + TanStack Query starter for CMS-style apps.

**Bootstrap checklist:** [../PROJECT_BOOTSTRAP.md](../PROJECT_BOOTSTRAP.md)  
**Module map & agent conventions:** [STRUCTURE.md](./STRUCTURE.md).

- **Locale:** mặc định `vi` (+ `en`). Không còn Arabic.
- **Auth:** Zustand `useAuthStore` only. No Redux, no AuthProvider Context.
- **Session:** `authToken` in localStorage via `utils/jwt` / `utils/localStorage`; `apiAxios` attaches the token.
- **Login field:** `username` + password (đổi sang `email` nếu BE yêu cầu — xem PROJECT_BOOTSTRAP).
- **Hook:** `useAuthActions` thin wrapper over the store (`doLogin` / `doLogout` + flags).
- **Server state sample:** TanStack Query (`queryClient`, `authKeys`, `useMeQuery`) — does not replace auth initialize/login.
- **Client UI sample:** `useUiStore` (e.g. command palette). Sidebar stays Context.
