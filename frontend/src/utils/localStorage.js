const AUTH_TOKEN_STORE_KEY = "authToken";

export const removeAuthToken = () => {
  return localStorage.removeItem(AUTH_TOKEN_STORE_KEY);
};

export const setAuthToken = (token) => {
  return localStorage.setItem(AUTH_TOKEN_STORE_KEY, token);
};

export const getAuthToken = () => {
  return localStorage.getItem(AUTH_TOKEN_STORE_KEY);
};

export const hasAuthToken = () => {
  return !!getAuthToken();
};

export const removeItem = (name) => {
  return localStorage.removeItem(name);
};
