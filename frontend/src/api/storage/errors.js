export class StorageApiError extends Error {
  constructor(
    message,
    { status = null, code, details = {}, outcomeUnknown = false },
  ) {
    super(message);
    this.name = "StorageApiError";
    this.status = status;
    this.code = code;
    this.details = details;
    this.outcomeUnknown = outcomeUnknown;
  }
}

async function readErrorEnvelope(data) {
  try {
    if (typeof Blob !== "undefined" && data instanceof Blob)
      data = await data.text();
    if (typeof data === "string") data = JSON.parse(data);
    const envelope = data?.error;
    if (
      typeof envelope?.code === "string" &&
      typeof envelope?.message === "string" &&
      envelope.message.trim()
    ) {
      return envelope;
    }
  } catch {
    // Proxy HTML and incomplete response bodies have no application envelope.
  }
  return null;
}

export async function toStorageApiError(error) {
  if (error instanceof StorageApiError) return error;
  const status = error.response?.status ?? null;
  const envelope = await readErrorEnvelope(error.response?.data);
  const isMutation = ["post", "delete"].includes(
    error.config?.method?.toLowerCase(),
  );
  const timeout = ["ECONNABORTED", "ETIMEDOUT"].includes(error.code);
  const cancelled = error.code === "ERR_CANCELED";
  const outcomeUnknown = isMutation && status === null;
  if (envelope) {
    return new StorageApiError(envelope.message, {
      status,
      code: envelope.code,
      details:
        envelope.details &&
        typeof envelope.details === "object" &&
        !Array.isArray(envelope.details)
          ? envelope.details
          : {},
      outcomeUnknown,
    });
  }
  const code = timeout
    ? "REQUEST_TIMEOUT"
    : cancelled
      ? "REQUEST_CANCELLED"
      : status !== null
        ? "HTTP_ERROR"
        : "NETWORK_ERROR";
  let message = timeout
    ? "Hết thời gian chờ phản hồi từ Metadata."
    : cancelled
      ? "Đã ngừng chờ phản hồi."
      : status !== null
        ? `Metadata trả lỗi HTTP ${status}. Vui lòng thử làm mới trạng thái.`
        : "Không kết nối được Metadata. Hãy kiểm tra địa chỉ API và kết nối mạng.";
  if (outcomeUnknown)
    message +=
      " Kết quả thao tác chưa rõ; hãy làm mới trạng thái trước khi thử lại.";
  return new StorageApiError(message, { status, code, outcomeUnknown });
}
