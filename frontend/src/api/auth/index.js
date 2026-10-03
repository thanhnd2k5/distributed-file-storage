import { apiAxios } from "../rootApi";

/** API path for auth endpoints */
export const AUTH_API = {
  LOGIN: "/api/auth/login",
  ME: "/api/auth/me",
};

/**
 * Login API - Authenticate user with username and password
 * Response: { status, success, message, data: { access_token, expire_in, auth_type } }
 * @param {Object} credentials - Login credentials
 * @param {string} credentials.username - Username
 * @param {string} credentials.password - Password
 * @returns {Promise<{ access_token: string, expire_in: number, auth_type: string }>}
 */
export const login = (credentials) => {
  return apiAxios
    .post(AUTH_API.LOGIN, credentials)
    .then((res) => res.data?.data ?? res.data);
};

/**
 * Get current user profile (requires auth token in header)
 * Response: { status, success, message, data: { _id, username, full_name, ... } }
 * @returns {Promise<Object>} User object
 */
export const getMe = () => {
  return apiAxios
    .get(AUTH_API.ME)
    .then((res) => res.data?.data ?? res.data);
};
