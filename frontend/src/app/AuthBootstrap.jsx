import { useEffect } from "react";
import PropTypes from "prop-types";

import { useAuthStore } from "stores/useAuthStore";

/**
 * Runs auth initialize() once on mount. Must wrap the router tree
 * that depends on isInitialized (e.g. Root splash).
 */
export function AuthBootstrap({ children }) {
  useEffect(() => {
    useAuthStore.getState().initialize();
  }, []);

  return children;
}

AuthBootstrap.propTypes = {
  children: PropTypes.node,
};
