import { useCallback } from "react";

import { useAuthActions } from "./useAuthActions";
import {
  hasPermission as checkPermission,
  hasAnyPermission as checkAnyPermission,
} from "utils/permissions";

export function usePermissions() {
  const { user } = useAuthActions();

  const hasPermission = useCallback(
    (code) => checkPermission(user, code),
    [user]
  );

  const hasAnyPermission = useCallback(
    (codes) => checkAnyPermission(user, codes),
    [user]
  );

  return { hasPermission, hasAnyPermission };
}
