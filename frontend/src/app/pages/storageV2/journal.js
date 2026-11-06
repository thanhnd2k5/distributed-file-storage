const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SHA = /^[0-9a-f]{64}$/;
const STATES = [
  "OPEN",
  "FINALIZING",
  "COMPLETED",
  "ABORTED",
  "EXPIRED",
  "FAILED",
];

export function validateJournal(value, baseURL) {
  if (
    value?.format !== 1 ||
    value.baseURL !== baseURL ||
    !UUID.test(value.key) ||
    (value.upload_id !== null && !UUID.test(value.upload_id)) ||
    !Number.isSafeInteger(value.request?.size_bytes) ||
    value.request.size_bytes < 0 ||
    !SHA.test(value.request.expected_sha256) ||
    typeof value.request.original_name !== "string" ||
    typeof value.request.content_type !== "string" ||
    typeof value.abort_requested !== "boolean" ||
    typeof value.create_attempted !== "boolean" ||
    !Number.isSafeInteger(value.next_retry_at) ||
    value.next_retry_at < 0 ||
    (value.server_state !== null && !STATES.includes(value.server_state))
  ) {
    throw new Error(
      "Bản ghi phiên không hợp lệ. Không ghi đè hoặc tự tạo phiên mới.",
    );
  }
  if (
    value.inflight !== null &&
    (!Number.isSafeInteger(value.inflight?.index) ||
      value.inflight.index < 0 ||
      !SHA.test(value.inflight.sha256))
  ) {
    throw new Error("Bản ghi part chưa đối soát không hợp lệ.");
  }
  return value;
}

export class UploadJournal {
  constructor(baseURL, storage) {
    this.baseURL = baseURL;
    this.storage = storage;
    this.key = `dfs:v2:upload:${baseURL}`;
  }
  read() {
    try {
      const raw = (this.storage || window.localStorage).getItem(this.key);
      if (raw === null) return null;
      if (raw.length > 16384) throw new Error("Bản ghi quá lớn.");
      return validateJournal(JSON.parse(raw), this.baseURL);
    } catch {
      throw new Error(
        "Không đọc được phiên đã lưu. Giữ nguyên bản ghi để có thể phục hồi.",
      );
    }
  }
  write(value) {
    validateJournal(value, this.baseURL);
    try {
      (this.storage || window.localStorage).setItem(
        this.key,
        JSON.stringify(value),
      );
    } catch {
      throw new Error(
        "Không lưu được phiên trên trình duyệt. Đã dừng trước mutation tiếp theo.",
      );
    }
  }
  clear() {
    (this.storage || window.localStorage).removeItem(this.key);
  }
}
