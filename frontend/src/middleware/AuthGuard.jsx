// Import Dependencies
import { Navigate, useLocation, useOutlet } from "react-router";

// Local Imports
import { useAuthActions } from "hooks/useAuthActions";
import { GHOST_ENTRY_PATH, REDIRECT_URL_KEY } from "../constants/app.constant";

// ----------------------------------------------------------------------

export default function AuthGuard() {
  const outlet = useOutlet();
  const { isAuthenticated } = useAuthActions();

  const location = useLocation();

  if (!isAuthenticated) {
    return (
      <Navigate
        to={`${GHOST_ENTRY_PATH}?${REDIRECT_URL_KEY}=${encodeURIComponent(location.pathname)}`}
        replace
      />
    );
  }

  return <>{outlet}</>;
}
