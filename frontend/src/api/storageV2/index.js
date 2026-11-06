import axios from "axios";

export class UploadApiError extends Error {
  constructor(
    message,
    { code, status = null, retryAfterMs = 0, outcomeUnknown = false } = {},
  ) {
    super(message);
    this.name = "UploadApiError";
    Object.assign(this, { code, status, retryAfterMs, outcomeUnknown });
  }
}

export function createUploadApi(
  base = new URL("/api/v2/", window.location.origin).href,
) {
  const url = new URL(base);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !url.pathname.replace(/\/$/, "").endsWith("/api/v2")
  ) {
    throw new Error("Địa chỉ API V2 không hợp lệ.");
  }
  url.pathname = url.pathname.replace(/\/$/, "") + "/";
  const http = axios.create({
    baseURL: url.href,
    timeout: 15000,
    withCredentials: false,
  });
  http.interceptors.request.use((config) => {
    config.headers.delete("Authorization");
    return config;
  });
  async function request(method, path, data, options = {}) {
    try {
      return (await http.request({ method, url: path, data, ...options })).data;
    } catch (error) {
      const status = error.response?.status ?? null;
      const envelope = error.response?.data?.error;
      const seconds = Number(error.response?.headers?.["retry-after"]);
      const retryAfterMs =
        Number.isSafeInteger(seconds) && seconds > 0 ? seconds * 1000 : 0;
      throw new UploadApiError(
        envelope?.message ||
          (error.code === "ERR_CANCELED"
            ? "Đã dừng chờ phản hồi. Kết quả trên server cần được đối soát."
            : "Không nhận được kết quả API V2; hãy kiểm tra trạng thái trước khi tiếp tục."),
        {
          code:
            envelope?.code ||
            (error.code === "ERR_CANCELED" ? "CANCELLED" : "NETWORK_ERROR"),
          status,
          retryAfterMs,
          outcomeUnknown:
            method !== "GET" && (status === null || status >= 500),
        },
      );
    }
  }
  return {
    baseURL: url.href,
    config: (signal) => request("GET", "config", undefined, { signal }),
    session: (id, signal) =>
      request("GET", `uploads/${id}`, undefined, { signal }),
    parts: (id, after, limit, signal) =>
      request("GET", `uploads/${id}/parts`, undefined, {
        params: { after_index: after, limit },
        signal,
      }),
    create: (payload, key, signal) =>
      request("POST", "uploads", payload, {
        headers: { "Idempotency-Key": key },
        signal,
        timeout: 20000,
      }),
    put: (id, index, blob, sha, signal, onUploadProgress) =>
      request("PUT", `uploads/${id}/parts/${index}`, blob, {
        headers: {
          "Content-Type": "application/octet-stream",
          "X-Part-SHA256": sha,
        },
        signal,
        onUploadProgress,
        timeout: 125000,
      }),
    complete: (id, signal) =>
      request("POST", `uploads/${id}/complete`, {}, { signal, timeout: 20000 }),
    abort: (id, signal) =>
      request("POST", `uploads/${id}/abort`, {}, { signal, timeout: 20000 }),
  };
}
