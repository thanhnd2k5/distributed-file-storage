import { create } from "zustand";

import { login as loginApi, getMe } from "api/auth";
import { authKeys } from "api/queryKeys";
import { queryClient } from "app/queryClient";
import { setSession, isTokenValid, getAuthToken } from "utils/jwt";

/** Module guards — not part of public store state (StrictMode / double-submit). */
let initializing = false;
let loginSeq = 0;

export const useAuthStore = create((set, get) => ({
  isAuthenticated: false,
  isInitialized: false,
  isLoading: false,
  user: null,
  errorMessage: null, // string | null

  initialize: async () => {
    if (get().isInitialized || initializing) return;
    initializing = true;

    try {
      const token = getAuthToken();
      if (token && isTokenValid(token)) {
        setSession(token);
        const user = await getMe();
        set({
          user,
          isAuthenticated: true,
          isInitialized: true,
          errorMessage: null,
        });
      } else {
        setSession(null);
        set({
          isInitialized: true,
          isAuthenticated: false,
          user: null,
          errorMessage: null,
        });
      }
    } catch {
      setSession(null);
      set({
        isInitialized: true,
        isAuthenticated: false,
        user: null,
        errorMessage: null,
      });
    } finally {
      initializing = false;
    }
  },

  login: async ({ username, password }) => {
    const seq = ++loginSeq;
    set({ isLoading: true, errorMessage: null });

    const isStale = () => seq !== loginSeq;

    try {
      const result = await loginApi({ username, password });
      if (isStale()) return;

      const access_token = result?.access_token;

      if (!access_token) {
        set({
          isLoading: false,
          errorMessage: "Invalid response: missing access_token",
        });
        return;
      }

      setSession(access_token);

      try {
        const user = await getMe();
        if (isStale()) return;

        queryClient.setQueryData(authKeys.me, user);
        set({
          isLoading: false,
          isAuthenticated: true,
          isInitialized: true,
          user,
          errorMessage: null,
        });
      } catch (error) {
        if (isStale()) return;
        setSession(null);
        queryClient.removeQueries({ queryKey: authKeys.all });
        set({
          isLoading: false,
          isAuthenticated: false,
          user: null,
          errorMessage: error.message || "Đăng nhập thất bại",
        });
      }
    } catch (error) {
      if (isStale()) return;
      setSession(null);
      queryClient.removeQueries({ queryKey: authKeys.all });
      set({
        isLoading: false,
        isAuthenticated: false,
        user: null,
        errorMessage: error.message || "Đăng nhập thất bại",
      });
    }
  },

  logout: () => {
    loginSeq += 1; // invalidate in-flight login
    setSession(null);
    queryClient.removeQueries({ queryKey: authKeys.all });
    set({
      isAuthenticated: false,
      user: null,
      errorMessage: null,
      isLoading: false,
    });
  },
}));
