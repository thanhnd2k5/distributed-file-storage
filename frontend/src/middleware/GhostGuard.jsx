// Import Dependencies
import { Navigate, useOutlet } from "react-router";

// Local Imports
import { useAuthActions } from "hooks/useAuthActions";
import { HOME_PATH, REDIRECT_URL_KEY } from "constants/app.constant";

// ----------------------------------------------------------------------

export default function GhostGuard() {
  const outlet = useOutlet();
  const { isAuthenticated } = useAuthActions();

  const raw = new URLSearchParams(window.location.search).get(REDIRECT_URL_KEY);
  const redirectTo =
    raw && raw !== "null" && raw !== "undefined" && raw.startsWith("/")
      ? raw
      : null;

  if (isAuthenticated) {
    return <Navigate to={redirectTo ?? HOME_PATH} />;
  }

  return <>{outlet}</>;
}
