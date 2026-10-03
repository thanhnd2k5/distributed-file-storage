import { useQuery } from "@tanstack/react-query";

import { getMe } from "api/auth";
import { authKeys } from "api/queryKeys";
import { useAuthStore } from "stores/useAuthStore";

/**
 * Sample refetch of current user via TanStack Query.
 * Does NOT replace useAuthStore.initialize() / login() — those remain
 * the source of truth for session bootstrap and credentials flow.
 */
export function useMeQuery() {
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);

  return useQuery({
    queryKey: authKeys.me,
    queryFn: getMe,
    enabled: isAuthenticated,
  });
}
