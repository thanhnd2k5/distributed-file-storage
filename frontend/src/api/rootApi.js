/**
 * rootApi - Axios instance for direct API calls (components, hooks, sagas)
 * Use when: Calling API from React components, custom hooks, or Redux sagas
 * - Has baseURL, interceptors, dynamic auth token
 * - No Redux action dispatch
 */
import axios from "axios";

const baseUrl = import.meta.env.VITE_API_URL;

export const apiAxios = axios.create({
  withCredentials: true,
  baseURL: `${baseUrl}`,
});

// Set Authorization header dynamically on each request (uses "authToken" from setSession)
apiAxios.interceptors.request.use(
  (request) => {
    const token = window.localStorage.getItem("authToken");
    if (token) {
      request.headers.Authorization = `Bearer ${token}`;
    }
    return request;
  },
  (error) => {
    return Promise.reject(error);
  },
);

apiAxios.interceptors.response.use(
  function (response) {
    return response;
  },
  function (error) {
    if (error.response?.data) {
      const data = error.response.data;
      if (data.detail && typeof data.detail === "object") {
        const messages = Object.values(data.detail);
        if (messages.length > 0) {
          error.message = messages.join("\n");
        } else {
          error.message = data.message || error.message;
        }
      } else if (data.message) {
        error.message = data.message;
      } else if (typeof data === "string") {
        error.message = data;
      }
    }
    return Promise.reject(error);
  },
);
