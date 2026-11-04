import axios from "axios";
import { createUploadApi } from "../src/api/storageV2/index.js";
import { UploadController } from "../src/app/pages/storageV2/upload.js";
import { UploadJournal } from "../src/app/pages/storageV2/journal.js";
import { hashBlob } from "../src/app/pages/storageV2/utils/hashBlob.js";

const checks = [];
const state = document.querySelector("#state");
const report = document.querySelector("#report");
const api = createUploadApi();
const marker = "c4:test:reload";
const assert = (value, message) => {
  if (!value) throw new Error(message);
};

function memory() {
  const values = new Map();
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key),
  };
}
function controller(options = {}, transport = api, storage = memory()) {
  return new UploadController(transport, {
    journal: new UploadJournal(api.baseURL, storage),
    pollMs: 40,
    maxPolls: 100,
    ...options,
  });
}
function source(size, seed = 1, name = "c4-source.bin") {
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i++) bytes[i] = (i * 31 + seed) % 256;
  return new File([bytes], name, { type: "application/octet-stream" });
}
async function reference(blob) {
  // Reference inputs are small fixtures, not the application's file hash path.
  const bytes = new Uint8Array(
    await crypto.subtle.digest("SHA-256", await blob.arrayBuffer()),
  );
  return [...bytes].map((n) => n.toString(16).padStart(2, "0")).join("");
}
async function control(data) {
  const response = await fetch("/api/v2/_c4/control", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  assert(response.ok, "Fixture control failed");
}
async function evidence() {
  const response = await fetch("/api/v2/_c4/status");
  assert(response.ok, "Backend disk evidence failed");
  return response.json();
}
async function expectFailure(action, pattern) {
  try {
    await action();
  } catch (error) {
    if (pattern)
      assert(pattern.test(error.message), `Wrong error: ${error.message}`);
    return error;
  }
  throw new Error("Expected failure");
}
async function check(name, action) {
  state.textContent = `RUNNING: ${name}`;
  try {
    await action();
    checks.push({ name, passed: true });
  } catch (error) {
    checks.push({ name, passed: false, error: error.stack || error.message });
    throw error;
  } finally {
    report.textContent = JSON.stringify(checks, null, 2);
  }
}
async function upload(file, options = {}, transport = api) {
  const flow = controller(options, transport);
  await flow.resume(file);
  assert(
    flow.view.phase === "completed" && flow.record.server_state === "COMPLETED",
    "Premature success",
  );
  return flow;
}
async function main() {
  await control({ worker: true, fault: null });
  await check(
    "hash worker: empty, abc, block boundaries and backend-reference bytes",
    async () => {
      assert(
        (await hashBlob(new Blob([]))) ===
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "empty vector",
      );
      assert(
        (await hashBlob(new Blob(["abc"]))) ===
          "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "abc vector",
      );
      for (const size of [262143, 262144, 262145, 1048593]) {
        const file = source(size, 17);
        assert(
          (await hashBlob(file)) === (await reference(file)),
          `hash boundary ${size}`,
        );
      }
    },
  );
  await check("cancel preparation before create; worker stops", async () => {
    const before = (await evidence()).sessions;
    const flow = controller();
    const original = flow.hash;
    flow.hash = (file, options) =>
      original(file, {
        ...options,
        onProgress: (loaded) => {
          options.onProgress?.(loaded);
          flow.pause();
        },
      });
    await expectFailure(() => flow.resume(source(16 * 1024 ** 2)));
    assert(
      !flow.record && (await evidence()).sessions === before,
      "Reserved before pre-hash finished",
    );
  });
  await check("storage write failure blocks all mutations", async () => {
    const before = (await evidence()).sessions;
    const flow = controller({}, api, {
      getItem: () => null,
      setItem: () => {
        throw new Error("quota");
      },
    });
    await expectFailure(() => flow.resume(source(3)), /Không lưu/);
    assert(
      (await evidence()).sessions === before,
      "Mutation escaped failed journal",
    );
  });
  await check("corrupt journal is preserved and cannot create", async () => {
    const storage = memory();
    storage.setItem(new UploadJournal(api.baseURL).key, "{broken");
    const flow = controller({}, api, storage);
    const before = (await evidence()).sessions;
    await expectFailure(() => flow.resume(source(3)), /Không đọc/);
    assert(
      (await evidence()).sessions === before,
      "Corrupt journal overwritten",
    );
  });
  await check(
    "cross-controller Web Lock prevents concurrent create",
    async () => {
      let release, enter;
      const entered = new Promise((resolve) => {
        enter = resolve;
      });
      const pendingHash = new Promise((resolve) => {
        release = resolve;
      });
      const first = controller({
        hash: () => {
          enter();
          return pendingHash;
        },
      });
      const running = first.resume(source(3));
      await entered;
      await expectFailure(() => controller().resume(source(3)), /tab khác/);
      first.pause();
      release("a".repeat(64));
      await expectFailure(() => running);
      assert(!first.record, "Cancelled lock owner created a session");
    },
  );
  await check(
    "real HTTP empty/chunk-boundary/multi-block upload to AVAILABLE",
    async () => {
      for (const size of [0, 65536, 65537, 262161]) {
        const file = source(size, 33);
        let fullHashDone = false;
        const transport = {
          ...api,
          create: (...args) => {
            assert(fullHashDone, "POST before full pre-hash");
            return api.create(...args);
          },
        };
        const flow = await upload(
          file,
          {
            hash: async (blob, options) => {
              const hash = await hashBlob(blob, options);
              if (blob === file) fullHashDone = true;
              return hash;
            },
          },
          transport,
        );
        assert(
          flow.record.request.expected_sha256 === (await reference(file)),
          "source hash mismatch",
        );
      }
      const data = await evidence();
      assert(
        data.files.filter((f) => f.state === "AVAILABLE").length >= 4,
        "Disk verification absent",
      );
    },
  );
  await check(
    "lost create response: durable key replay, no duplicate session",
    async () => {
      const file = source(100, 44);
      const before = (await evidence()).sessions;
      const storage = memory(),
        keys = [];
      const transport = {
        ...api,
        create: (...args) => {
          keys.push(args[1]);
          return api.create(...args);
        },
      };
      await control({ fault: "after_create" });
      const first = controller({}, transport, storage);
      await expectFailure(() => first.resume(file));
      assert(
        !first.record.upload_id && first.record.create_attempted,
        "Lost create not retained",
      );
      assert(
        (await evidence()).sessions === before + 1,
        "Unexpected automatic create retry",
      );
      const next = controller({}, transport, storage);
      await next.resume(file);
      assert(
        next.view.phase === "completed" &&
          keys.length === 2 &&
          keys[0] === keys[1],
        "Wrong replay key",
      );
      assert(
        (await evidence()).sessions === before + 1,
        "Create replay duplicated session",
      );
    },
  );
  await check(
    "lost committed part response: GET observes VERIFIED, no resend",
    async () => {
      await control({ fault: "after_part", clear_audit: true });
      const flow = await upload(source(123, 45));
      const requests = (await evidence()).requests.filter((r) =>
        r.path.includes(flow.record.upload_id),
      );
      assert(
        requests.filter((r) => r.method === "PUT").length === 1,
        "Verified part resent",
      );
      assert(
        requests.some((r) => r.method === "GET" && r.path.endsWith("/parts")),
        "No outcome observation",
      );
    },
  );
  await check(
    "Retry-After before PUT replay plus status observation",
    async () => {
      await control({ fault: "before_part" });
      await control({ clear_audit: true });
      const started = performance.now();
      const flow = await upload(source(45, 46));
      assert(
        flow.view.phase === "completed" && performance.now() - started >= 1000,
        "Ignored Retry-After",
      );
      const puts = (await evidence()).requests.filter(
        (r) => r.method === "PUT",
      );
      assert(
        puts.length === 2 && puts[1].at - puts[0].at >= 0.99,
        "PUT replay ignored server retry time",
      );
    },
  );
  await check(
    "complete response lost after commit: poll, never second POST",
    async () => {
      await control({ fault: "after_complete", clear_audit: true });
      const flow = await upload(source(46, 47));
      const requests = (await evidence()).requests;
      assert(
        requests.filter(
          (r) =>
            r.path.includes(flow.record.upload_id) &&
            r.path.endsWith("/complete"),
        ).length === 1,
        "Blind complete replay",
      );
    },
  );
  await check(
    "202 finalize persists; replacement waits without file or another complete",
    async () => {
      await control({ worker: false });
      const storage = memory(),
        flow = controller({ maxPolls: 2 }, api, storage);
      await expectFailure(() => flow.resume(source(100, 48)), /vẫn đang xử lý/);
      assert(
        flow.record.server_state === "FINALIZING" &&
          flow.view.phase !== "completed",
        "202 treated as published",
      );
      await control({ worker: true, clear_audit: true });
      const next = controller({}, api, storage);
      await next.resume(null);
      assert(next.view.phase === "completed", "Failed fileless wait");
      assert(
        !(await evidence()).requests.some((r) =>
          ["POST", "PUT"].includes(r.method),
        ),
        "Mutated FINALIZING session",
      );
    },
  );
  await check(
    "abort after first confirmation, lost response, cleanup observed",
    async () => {
      const storage = memory(),
        file = source(65537, 49);
      let first;
      const transport = {
        ...api,
        put: async (...args) => {
          const result = await api.put(...args);
          first.pause();
          return result;
        },
      };
      first = controller({}, transport, storage);
      await expectFailure(() => first.resume(file));
      assert(first.record.upload_id, "No session after pause");
      await control({ fault: "after_abort", clear_audit: true });
      const next = controller({}, api, storage);
      await next.abort();
      assert(
        next.record.server_state === "ABORTED" && !next.record.cleanup_pending,
        "Cleanup not completed",
      );
      assert(
        (await evidence()).requests.filter((r) => r.path.endsWith("/abort"))
          .length === 1,
        "Blind abort replay",
      );
    },
  );
  await check(
    "abort resolves lost create with original key, without reselecting source",
    async () => {
      const storage = memory(),
        file = source(4, 50);
      await control({ fault: "after_create" });
      const first = controller({}, api, storage);
      await expectFailure(() => first.resume(file));
      const key = first.record.key;
      const next = controller({}, api, storage);
      await next.abort();
      assert(
        next.record.key === key && next.record.server_state === "ABORTED",
        "Lost-create abort lost identity",
      );
    },
  );
  await check(
    "source changes after pre-hash: full finalize mismatch, never success",
    async () => {
      const file = source(65537, 51);
      const flow = controller({
        hash: (blob, options) =>
          blob === file
            ? Promise.resolve("0".repeat(64))
            : hashBlob(blob, options),
      });
      await expectFailure(() => flow.resume(file), /Xác minh thất bại/);
      assert(
        flow.record.server_state === "FAILED" && flow.view.phase === "failed",
        "Published changed source",
      );
    },
  );
  await check(
    "prepare a persisted partial upload for real page reload",
    async () => {
      const file = source(4 * 65536 + 7, 71, "reload-source.bin");
      let first,
        count = 0;
      const transport = {
        ...api,
        put: async (...args) => {
          const result = await api.put(...args);
          if (++count === 3) first.pause();
          return result;
        },
      };
      first = new UploadController(transport, { pollMs: 40, maxPolls: 100 });
      assert(!first.record, "Unexpected user journal in fixture origin");
      await expectFailure(() => first.resume(file));
      const session = await api.session(first.record.upload_id);
      assert(
        session.confirmed_parts === 3 && session.state === "OPEN",
        "Partial upload fixture",
      );
      localStorage.setItem(
        marker,
        JSON.stringify({
          checks,
          upload_id: session.upload_id,
          size: file.size,
          seed: 71,
          key: first.record.key,
        }),
      );
    },
  );
  state.textContent =
    "RELOAD_READY — reload this page to verify persistence and source selection";
}

async function afterReload(saved) {
  checks.push(...saved.checks);
  const file = source(saved.size, saved.seed, "reload-source.bin");
  const transport = {
    ...api,
    parts: (id, after, limit, signal) =>
      api.parts(id, after, Math.min(limit, 2), signal),
  };
  const next = new UploadController(transport, { pollMs: 40, maxPolls: 100 });
  await check(
    "real reload restores upload/key, no bytes/file handle persisted",
    async () => {
      assert(
        next.record.upload_id === saved.upload_id &&
          next.record.key === saved.key,
        "Journal lost after reload",
      );
      assert(
        !JSON.stringify(next.record).includes("fileData"),
        "Persisted file bytes",
      );
    },
  );
  await check(
    "same name and size, different bytes rejected before mutation",
    async () => {
      await control({ clear_audit: true });
      await expectFailure(
        () => next.resume(source(saved.size, saved.seed + 1, file.name)),
        /không khớp/,
      );
      assert(
        !(await evidence()).requests.some((r) =>
          ["POST", "PUT"].includes(r.method),
        ),
        "Wrong source sent bytes",
      );
    },
  );
  await check(
    "correct reselect resumes missing parts only, real paged manifest",
    async () => {
      await control({ clear_audit: true });
      await next.resume(file);
      const requests = (await evidence()).requests.filter(
        (r) => r.method === "PUT",
      );
      assert(
        requests.length === 2 &&
          requests.every((r) => !/\/parts\/[012]$/.test(r.path)),
        "Verified part resent or missing part skipped",
      );
      assert(next.view.phase === "completed", "Resume not published");
      await next.newUpload();
    },
  );
  await check("adapter never forwards inherited Authorization", async () => {
    await control({ clear_audit: true });
    axios.defaults.headers.common.Authorization = "Bearer synthetic-c4-only";
    try {
      await createUploadApi().config();
    } finally {
      delete axios.defaults.headers.common.Authorization;
    }
    assert(
      (await evidence()).requests.every((r) => !r.authorization),
      "Authorization leaked",
    );
  });
  localStorage.removeItem(marker);
  await fetch("/api/v2/_c4/report", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ checks, userAgent: navigator.userAgent }),
  });
  state.textContent = `PASS ${checks.length} checks — backend validates both replica bytes and full SHA-256`;
}

document.querySelector("#run").addEventListener("click", async () => {
  document.querySelector("#run").disabled = true;
  try {
    const saved = localStorage.getItem(marker);
    if (saved) await afterReload(JSON.parse(saved));
    else await main();
  } catch (error) {
    state.textContent = `FAIL: ${error.message}`;
    console.error(error);
    await fetch("/api/v2/_c4/report", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ checks, error: error.stack }),
    });
  }
});
