import { UploadApiError } from "../../../api/storageV2";
import { UploadJournal } from "./journal";
import { hashBlob } from "./utils/hashBlob";

const TERMINAL = ["COMPLETED", "ABORTED", "EXPIRED", "FAILED"];
const RETRYABLE = [
  "PART_BUSY",
  "OUTCOME_PENDING",
  "RESOURCE_LIMIT",
  "CAPACITY_UNAVAILABLE",
  "INSUFFICIENT_DOMAINS",
  "STORAGE_UNAVAILABLE",
  "METADATA_UNAVAILABLE",
];

function delay(ms, signal) {
  return new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const stop = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", stop);
      resolve();
    }, ms);
    signal.addEventListener("abort", stop, { once: true });
  });
}

export class UploadController {
  constructor(
    api,
    {
      journal = new UploadJournal(api.baseURL),
      hash = hashBlob,
      locks = navigator.locks,
      pollMs = 1000,
      maxPolls = 120,
      partAttempts = 3,
    } = {},
  ) {
    Object.assign(this, {
      api,
      journal,
      hash,
      locks,
      pollMs,
      maxPolls,
      partAttempts,
    });
    if (
      !Number.isFinite(pollMs) ||
      pollMs <= 0 ||
      pollMs > 30000 ||
      ![maxPolls, partAttempts].every(
        (n) => Number.isSafeInteger(n) && n > 0 && n <= 1000,
      )
    ) {
      throw new Error("Ngân sách poll/retry phải hữu hạn.");
    }
    this.listeners = new Set();
    this.view = {
      phase: "idle",
      busy: false,
      error: "",
      prepared: 0,
      total: 0,
      currentPart: null,
      sent: 0,
      session: null,
    };
    try {
      this.record = journal.read();
    } catch (error) {
      this.view.error = error.message;
      this.broken = true;
    }
    this.record ??= null;
    if (this.record) {
      this.view.phase =
        this.record.server_state === "COMPLETED"
          ? "completed"
          : this.record.server_state === "FAILED"
            ? "failed"
            : ["ABORTED", "EXPIRED"].includes(this.record.server_state)
              ? "aborted"
              : "paused";
    }
  }
  snapshot() {
    return { ...this.view, record: this.record };
  }
  subscribe(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  show(patch) {
    Object.assign(this.view, patch);
    for (const listener of this.listeners) listener(this.snapshot());
  }
  save(patch) {
    const next = { ...this.record, ...patch };
    this.journal.write(next); // Persist intent before mutation; never silently fall back to RAM.
    this.record = next;
    this.show({});
  }
  pause() {
    this.cancellation?.abort();
  }

  async operate(work) {
    if (this.view.busy) throw new Error("Một thao tác của phiên đang chạy.");
    if (this.broken) throw new Error(this.view.error);
    if (!this.locks)
      throw new Error(
        "Cần trình duyệt hỗ trợ Web Locks trên HTTPS hoặc localhost.",
      );
    this.cancellation = new AbortController();
    const signal = this.cancellation.signal;
    this.show({ busy: true, error: "" });
    const operation = this.locks.request(
      this.journal.key,
      { ifAvailable: true },
      async (lock) => {
        if (!lock) throw new Error("Phiên đang được xử lý trong tab khác.");
        this.record = this.journal.read();
        return work(signal);
      },
    );
    this.running = operation;
    try {
      return await operation;
    } catch (error) {
      if (
        this.record &&
        error instanceof UploadApiError &&
        error.retryAfterMs
      ) {
        try {
          this.save({ next_retry_at: Date.now() + error.retryAfterMs });
        } catch {
          /* Original journal still retains the operation identity. */
        }
      }
      this.show({
        phase: this.record?.server_state === "FAILED" ? "failed" : "paused",
        error: signal.aborted ? "" : error.message,
      });
      throw error;
    } finally {
      this.running = null;
      this.show({ busy: false, sent: 0, currentPart: null });
    }
  }

  async backoff(error, signal) {
    const wait = Math.max(this.pollMs, error?.retryAfterMs || 0);
    const due = Date.now() + wait;
    if (!Number.isSafeInteger(due))
      throw new Error("Retry-After không hợp lệ.");
    if (this.record) this.save({ next_retry_at: due });
    if (wait > 30000)
      throw new Error(
        "Server yêu cầu chờ lâu hơn. Phiên được giữ để tiếp tục sau Retry-After.",
      );
    await delay(wait, signal);
  }
  async respectRetry(signal) {
    const remaining = (this.record?.next_retry_at || 0) - Date.now();
    if (remaining > 30000)
      throw new Error("Chưa đến thời điểm Retry-After; hãy tiếp tục sau.");
    if (remaining > 0) await delay(remaining, signal);
  }
  async read(action, signal) {
    for (let attempt = 0; ; attempt++) {
      signal.throwIfAborted();
      try {
        return await action();
      } catch (error) {
        if (
          signal.aborted ||
          attempt >= 2 ||
          !(error instanceof UploadApiError) ||
          (error.status !== null &&
            ![429, 500, 502, 503, 504].includes(error.status))
        )
          throw error;
        await this.backoff(error, signal);
      }
    }
  }
  observe(session) {
    const request = this.record.request;
    if (
      (this.record.upload_id && session.upload_id !== this.record.upload_id) ||
      request.size_bytes !== session.size_bytes ||
      request.expected_sha256 !== session.expected_sha256 ||
      request.original_name !== session.original_name ||
      request.content_type !== session.content_type ||
      !["OPEN", "FINALIZING", ...TERMINAL].includes(session.state) ||
      !Number.isSafeInteger(session.chunk_size_bytes) ||
      session.chunk_size_bytes <= 0 ||
      session.part_count !==
        Math.ceil(session.size_bytes / session.chunk_size_bytes)
    ) {
      throw new Error(
        "Phiên server không khớp identity nguồn đã lưu. Đã chặn gửi tiếp.",
      );
    }
    this.save({
      upload_id: session.upload_id,
      server_state: session.state,
      cleanup_pending: session.cleanup_pending,
    });
    this.show({ session });
    return session;
  }
  async status(signal) {
    return this.observe(
      await this.read(
        () => this.api.session(this.record.upload_id, signal),
        signal,
      ),
    );
  }
  terminal(session) {
    if (session.state === "COMPLETED") {
      this.show({ phase: "completed" });
      return true;
    }
    if (["ABORTED", "EXPIRED", "FAILED"].includes(session.state)) {
      this.show({ phase: session.state === "FAILED" ? "failed" : "aborted" });
      if (session.state === "FAILED")
        throw new Error(
          `Xác minh thất bại: ${session.error_code || "FAILED"}. Chưa công bố file.`,
        );
      return true;
    }
    return false;
  }
  async ensureCreated(signal) {
    if (this.record.upload_id) return this.status(signal);
    this.show({ phase: "creating" });
    // An explicit resume may replay this exact durable intent after a lost
    // response. There is no automatic POST retry or replacement key.
    this.save({ create_attempted: true });
    return this.observe(
      await this.api.create(this.record.request, this.record.key, signal),
    );
  }
  async prepare(file, signal) {
    if (!file || !Number.isSafeInteger(file.size) || file.size < 0)
      throw new Error("Hãy chọn lại file nguồn.");
    this.show({ phase: "preparing", prepared: 0, total: file.size });
    const sha = await this.hash(file, {
      signal,
      onProgress: (prepared) => this.show({ prepared }),
    });
    signal.throwIfAborted();
    this.show({ prepared: file.size });
    if (this.record) {
      if (
        this.record.request.size_bytes !== file.size ||
        this.record.request.expected_sha256 !== sha
      ) {
        throw new Error(
          "File được chọn không khớp size/SHA-256 của phiên. Tên file giống nhau không đủ để resume.",
        );
      }
    } else {
      this.record = {
        format: 1,
        baseURL: this.api.baseURL,
        key: crypto.randomUUID(),
        request: {
          original_name: file.name,
          content_type: file.type || "application/octet-stream",
          size_bytes: file.size,
          expected_sha256: sha,
        },
        upload_id: null,
        create_attempted: false,
        abort_requested: false,
        next_retry_at: 0,
        server_state: null,
        cleanup_pending: false,
        inflight: null,
      };
      this.save({});
    }
  }
  async part(index, signal) {
    const page = await this.read(
      () => this.api.parts(this.record.upload_id, index - 1, 1, signal),
      signal,
    );
    return page.items.find((item) => item.index === index) || null;
  }
  async sendPart(file, session, index, observed, signal) {
    signal.throwIfAborted();
    const start = index * session.chunk_size_bytes;
    const blob = file.slice(
      start,
      Math.min(start + session.chunk_size_bytes, file.size),
    );
    const sha = await this.hash(blob, { signal });
    if (
      (this.record.inflight?.index === index &&
        this.record.inflight.sha256 !== sha) ||
      (observed && observed.checksum_sha256 !== sha)
    ) {
      throw new Error(
        "Nguồn hoặc hash part đã thay đổi. Không ghi đè identity của part cũ.",
      );
    }
    this.show({ phase: "uploading", currentPart: index, sent: 0 });
    for (let attempt = 0; attempt < this.partAttempts; attempt++) {
      await this.respectRetry(signal);
      if (observed?.state === "VERIFIED") {
        this.save({ inflight: null });
        return;
      }
      if (observed?.state === "CLAIMED") {
        await this.backoff(null, signal);
        observed = await this.part(index, signal);
        continue; // Never resend while a live/unknown claim still owns the part.
      }
      this.save({ inflight: { index, sha256: sha } });
      try {
        const result = await this.api.put(
          this.record.upload_id,
          index,
          blob,
          sha,
          signal,
          ({ loaded }) => this.show({ sent: Math.min(loaded, blob.size) }),
        );
        if (
          result.state !== "VERIFIED" ||
          result.index !== index ||
          result.size_bytes !== blob.size ||
          result.checksum_sha256 !== sha
        )
          throw new Error(
            "Phản hồi part chưa chứng minh confirmation. Hãy tra trạng thái.",
          );
        this.save({ inflight: null, next_retry_at: 0 });
        await this.status(signal);
        return;
      } catch (error) {
        if (signal.aborted) throw error;
        if (error instanceof UploadApiError && error.retryAfterMs) {
          this.save({ next_retry_at: Date.now() + error.retryAfterMs });
        }
        // A status observation is mandatory after every ambiguous PUT. This
        // also resolves a committed confirmation whose HTTP response was lost.
        observed = await this.part(index, signal);
        if (
          observed?.state === "VERIFIED" &&
          observed.checksum_sha256 === sha
        ) {
          this.save({ inflight: null });
          await this.status(signal);
          return;
        }
        if (
          !(error instanceof UploadApiError) ||
          (!error.outcomeUnknown && !RETRYABLE.includes(error.code))
        )
          throw error;
        await this.backoff(error, signal);
      }
    }
    throw new Error(
      "Part còn chờ đối soát. Phiên được giữ; hãy tra trạng thái rồi tiếp tục sau.",
    );
  }
  async uploadParts(file, session, signal) {
    let cursor = -1,
      index = 0;
    while (true) {
      const page = await this.read(
        () => this.api.parts(this.record.upload_id, cursor, 100, signal),
        signal,
      );
      for (const item of page.items) {
        if (
          !Number.isSafeInteger(item.index) ||
          item.index <= cursor ||
          item.index >= session.part_count
        ) {
          throw new Error("Trang part không hợp lệ.");
        }
        while (index < item.index)
          await this.sendPart(file, session, index++, null, signal);
        if (item.state !== "VERIFIED")
          await this.sendPart(file, session, item.index, item, signal);
        else if (this.record.inflight?.index === item.index)
          this.save({ inflight: null });
        index = item.index + 1;
        cursor = item.index;
      }
      if (!page.has_more) break;
      if (!page.items.length || page.next_after_index !== cursor)
        throw new Error("Cursor part không tiến triển.");
    }
    while (index < session.part_count)
      await this.sendPart(file, session, index++, null, signal);
  }
  async waitForTerminal(signal, cleanup = false) {
    this.show({ phase: cleanup ? "aborting" : "verifying" });
    for (let attempt = 0; attempt < this.maxPolls; attempt++) {
      const session = await this.status(signal);
      if (
        cleanup &&
        TERMINAL.includes(session.state) &&
        !session.cleanup_pending
      ) {
        this.show({
          phase:
            session.state === "COMPLETED"
              ? "completed"
              : session.state === "FAILED"
                ? "failed"
                : "aborted",
        });
        return session;
      }
      if (!cleanup && this.terminal(session)) return session;
      await this.backoff(null, signal);
    }
    throw new Error(
      "Server vẫn đang xử lý. Phiên được giữ; có thể tra trạng thái hoặc tiếp tục chờ sau.",
    );
  }
  async finish(signal) {
    let session = await this.status(signal);
    if (this.terminal(session)) return session;
    if (session.state === "OPEN") {
      this.show({ phase: "verifying" });
      try {
        session = this.observe(
          await this.api.complete(this.record.upload_id, signal),
        );
      } catch (error) {
        if (signal.aborted) throw error;
        session = await this.status(signal);
        if (session.state === "OPEN") throw error;
      }
    }
    return this.waitForTerminal(signal);
  }
  resume(file) {
    return this.operate(async (signal) => {
      await this.respectRetry(signal);
      if (this.record?.abort_requested) return this.abortFlow(signal);
      if (this.record?.upload_id) {
        const session = await this.status(signal);
        if (session.state === "FINALIZING") return this.waitForTerminal(signal);
        if (this.terminal(session)) return session;
      }
      await this.prepare(file, signal);
      const session = await this.ensureCreated(signal);
      if (session.state === "FINALIZING") return this.waitForTerminal(signal);
      if (this.terminal(session)) return session;
      await this.uploadParts(file, session, signal);
      return this.finish(signal);
    });
  }
  async abortFlow(signal) {
    if (!this.record) throw new Error("Chưa có phiên để hủy.");
    this.save({ abort_requested: true });
    this.show({ phase: "aborting" });
    if (!this.record.upload_id && !this.record.create_attempted) {
      this.save({ server_state: "ABORTED", cleanup_pending: false });
      this.show({ phase: "aborted" });
      return;
    }
    let session = await this.ensureCreated(signal);
    if (session.state === "COMPLETED") {
      this.show({ phase: "completed" });
      return session;
    }
    if (session.state === "OPEN" || session.state === "FINALIZING") {
      try {
        session = this.observe(
          await this.api.abort(this.record.upload_id, signal),
        );
      } catch (error) {
        if (signal.aborted) throw error;
        session = await this.status(signal);
        if (session.state === "OPEN" || session.state === "FINALIZING")
          throw error;
      }
    }
    return this.waitForTerminal(signal, true);
  }
  async abort() {
    this.pause();
    if (this.running) {
      try {
        await this.running;
      } catch {
        /* Keep the durable pending intent. */
      }
    }
    return this.operate((signal) =>
      this.respectRetry(signal).then(() => this.abortFlow(signal)),
    );
  }
  refresh() {
    return this.operate(async (signal) => {
      await this.respectRetry(signal);
      if (!this.record?.upload_id)
        throw new Error(
          "Chưa biết upload ID. Chọn đúng nguồn và tiếp tục để replay cùng key.",
        );
      const session = await this.status(signal);
      this.show({
        phase:
          session.state === "FINALIZING"
            ? "verifying"
            : session.state === "COMPLETED"
              ? "completed"
              : session.state === "FAILED"
                ? "failed"
                : ["ABORTED", "EXPIRED"].includes(session.state)
                  ? "aborted"
                  : "paused",
      });
      return session;
    });
  }
  newUpload() {
    return this.operate(() => {
      if (
        this.record &&
        (!TERMINAL.includes(this.record.server_state) ||
          this.record.cleanup_pending)
      ) {
        throw new Error(
          "Giữ phiên hiện tại tới khi hoàn tất hoặc cleanup kết thúc.",
        );
      }
      this.journal.clear();
      this.record = null;
      this.show({ phase: "idle", session: null, prepared: 0, total: 0 });
    });
  }
}
