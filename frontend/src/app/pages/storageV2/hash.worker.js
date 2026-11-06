import { sha256 } from "@noble/hashes/sha2.js";
import { bytesToHex } from "@noble/hashes/utils.js";

// One job per worker. Terminating the worker cancels both hashing and further reads.
self.onmessage = async ({ data: { blob } }) => {
  try {
    const hash = sha256.create();
    let last = 0;
    for (let offset = 0; offset < blob.size; offset += 262144) {
      const block = new Uint8Array(
        await blob.slice(offset, offset + 262144).arrayBuffer(),
      );
      hash.update(block);
      const loaded = Math.min(offset + block.length, blob.size);
      if (performance.now() - last >= 100 || loaded === blob.size) {
        self.postMessage({ type: "progress", loaded });
        last = performance.now();
      }
    }
    self.postMessage({ type: "done", hash: bytesToHex(hash.digest()) });
  } catch {
    self.postMessage({
      type: "error",
      message: "Không đọc được nguồn. Hãy chọn lại file và kiểm tra lại hash.",
    });
  }
};
