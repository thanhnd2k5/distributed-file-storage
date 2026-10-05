import axios from "axios";
import { toStorageApiError } from "./errors";

function storageBaseUrl(value) {
  const url = new URL(value);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error(
      "VITE_STORAGE_API_URL phải là HTTP(S) URL, không chứa credentials, query hoặc fragment.",
    );
  }
  const path = url.pathname.replace(/\/+$/, "");
  url.pathname = path.endsWith("/api/v1") ? `${path}/` : `${path}/api/v1/`;
  return url.toString();
}

export const STORAGE_TIMEOUTS = {
  read: 15_000,
  operation: 180_000,
  transfer: 900_000,
};

export const storageAxios = axios.create({
  baseURL: storageBaseUrl(
    import.meta.env.VITE_STORAGE_API_URL || "http://localhost:8000/api/v1",
  ),
  withCredentials: false,
  timeout: STORAGE_TIMEOUTS.read,
});

storageAxios.interceptors.response.use(
  (response) => response,
  async (error) => Promise.reject(await toStorageApiError(error)),
);
