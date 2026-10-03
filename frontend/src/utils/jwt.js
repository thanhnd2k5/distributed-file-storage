import { jwtDecode } from "jwt-decode";

import {
  getAuthToken,
  removeAuthToken,
  setAuthToken,
} from "./localStorage";

/**
 * Checks if the provided JWT token is valid (not expired).
 *
 * @param {string} authToken - The JWT token to validate.
 * @returns {boolean} - Returns `true` if the token is valid, otherwise `false`.
 */
const isTokenValid = (authToken) => {
  if (typeof authToken !== "string") {
    console.error("Invalid token format.");
    return false;
  }

  try {
    const decoded = jwtDecode(authToken);
    const currentTime = Date.now() / 1000;

    return decoded.exp > currentTime;
  } catch (err) {
    console.error("Failed to decode token:", err);
    return false;
  }
};

/**
 * Sets or clears the session token in localStorage only.
 * apiAxios reads `authToken` from localStorage on each request.
 *
 * @param {string} [authToken] - JWT to store, or null/undefined to clear.
 */
const setSession = (authToken) => {
  if (typeof authToken === "string" && authToken.trim() !== "") {
    setAuthToken(authToken);
  } else {
    removeAuthToken();
  }
};

export { isTokenValid, setSession, getAuthToken };
