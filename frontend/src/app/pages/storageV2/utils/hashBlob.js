export function hashBlob(blob, { signal, onProgress = () => {} } = {}) {
  return new Promise((resolve, reject) => {
    signal?.throwIfAborted();
    const worker = new Worker(new URL("../hash.worker.js", import.meta.url), {
      type: "module",
    });
    const finish = (error, hash) => {
      worker.terminate();
      signal?.removeEventListener("abort", cancel);
      if (error) reject(error);
      else resolve(hash);
    };
    const cancel = () =>
      finish(new DOMException("Đã dừng kiểm tra file.", "AbortError"));
    signal?.addEventListener("abort", cancel, { once: true });
    worker.onmessage = ({ data }) => {
      if (data.type === "progress") onProgress(data.loaded);
      else if (data.type === "done") finish(null, data.hash);
      else if (data.type === "error") finish(new Error(data.message));
    };
    worker.onerror = () =>
      finish(new Error("Không khởi động được worker kiểm tra hash."));
    worker.onmessageerror = () =>
      finish(new Error("Không truyền được nguồn tới worker."));
    worker.postMessage({ blob });
  });
}
