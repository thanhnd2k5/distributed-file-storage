import { useCallback } from "react";

import { useAuthStore } from "stores/useAuthStore";

export function useAuthActions() {
  const user = useAuthStore((s) => s.user);
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);
  const isLoading = useAuthStore((s) => s.isLoading);
  const isInitialized = useAuthStore((s) => s.isInitialized);
  const errorMessage = useAuthStore((s) => s.errorMessage);
  const login = useAuthStore((s) => s.login);
  const logout = useAuthStore((s) => s.logout);

  const doLogin = useCallback(
    ({ username, password }) => {
      login({ username, password });
    },
    [login]
  );

  const doLogout = useCallback(() => {
    logout();
  }, [logout]);

  return {
    user,
    isAuthenticated,
    isLoading,
    isInitialized,
    errorMessage,
    doLogin,
    doLogout,
  };
}
