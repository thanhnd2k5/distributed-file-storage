import { createUploadApi } from "../src/api/storageV2/index.js";
import { UploadController } from "../src/app/pages/storageV2/upload.js";
import { UploadJournal } from "../src/app/pages/storageV2/journal.js";
import { hashBlob } from "../src/app/pages/storageV2/utils/hashBlob.js";

const api = createUploadApi();
const state = document.querySelector("#state");
const live = document.querySelector("#live");
const report = document.querySelector("#report");
const marker = "dfs:c5:reload";
const results = JSON.parse(localStorage.getItem("dfs:c5:results") || "[]");
const assert = (ok, message) => { if (!ok) throw new Error(message); };
const now = () => performance.now();

async function control(action, values = {}) {
  const response = await fetch("/api/v2/_c5/control", {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({ action, ...values }),
  });
  assert(response.ok, `${action}: ${await response.clone().text()}`);
  return response.json();
}

async function publish() {
  localStorage.setItem("dfs:c5:results", JSON.stringify(results));
  report.textContent = JSON.stringify(results, null, 2);
  await fetch("/api/v2/_c5/report", {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({ results, userAgent: navigator.userAgent }),
  });
}

async function source(size, seed = 17) {
  const root = await navigator.storage.getDirectory();
  const directory = await root.getDirectoryHandle("dfs-c5-sources", { create: true });
  const handle = await directory.getFileHandle(`source-${seed}.bin`, { create: true });
  const current = await handle.getFile();
  if (current.size === size) return current;
  const quota = await navigator.storage.estimate();
  assert(quota.quota - quota.usage > size + 512 * 1024 ** 2, "Insufficient OPFS quota");
  const block = new Uint8Array(1024 ** 2);
  for (let i = 0; i < block.length; i++) block[i] = (i * 31 + seed) % 256;
  const writer = await handle.createWritable();
  try {
    for (let offset = 0; offset < size; offset += block.length) {
      await writer.write(block.subarray(0, Math.min(block.length, size - offset)));
      if (offset % (64 * 1024 ** 2) === 0) live.textContent = `Preparing disk source: ${offset}/${size}`;
    }
    await writer.close();
  } catch (error) {
    await writer.abort();
    throw error;
  }
  return handle.getFile();
}

async function removeSources() {
  const root = await navigator.storage.getDirectory();
  await root.removeEntry("dfs-c5-sources", { recursive: true });
}

function flow(options = {}, transport = api) {
  const controller = new UploadController(transport, {
    journal: new UploadJournal(api.baseURL), maxPolls: 1000, ...options,
  });
  let last = 0;
  controller.subscribe((view) => {
    if (now() - last > 500 || !view.busy) {
      last = now();
      live.textContent = JSON.stringify({ phase: view.phase, prepared: view.prepared,
        total: view.total, currentPart: view.currentPart, confirmed: view.session?.confirmed_parts,
        bytes: view.session?.confirmed_bytes, heap: performance.memory?.usedJSHeapSize }, null, 2);
    }
  });
  return controller;
}

async function fresh() {
  const controller = flow();
  if (controller.record) await controller.newUpload();
}

async function verify(controller, seed) {
  const session = controller.snapshot().session;
  assert(session?.state === "COMPLETED", "Not COMPLETED");
  return control("verify", { upload_id: session.upload_id, seed });
}

async function check(name, action) {
  state.textContent = `RUNNING: ${name}`;
  await control("phase", { name });
  const start = now();
  try {
    const detail = await action();
    results.push({ name, passed: true, seconds: (now() - start) / 1000, ...detail });
  } catch (error) {
    results.push({ name, passed: false, error: error.stack || error.message });
    throw error;
  } finally {
    await publish();
  }
}

async function small() {
  if (localStorage.getItem(marker)) {
    await check("reload-resume-correct-source", async () => {
      const file = await source(3 * 8388608 + 17);
      const wrong = await source(file.size, 18);
      const attempts = [];
      const transport = { ...api, put: (...args) => { attempts.push(args[1]); return api.put(...args); } };
      const controller = flow({}, transport);
      const old = controller.record.upload_id;
      let rejected = false;
      try { await controller.resume(wrong); } catch { rejected = true; }
      assert(rejected && attempts.length === 0, "Wrong source was sent");
      await controller.resume(file);
      assert(controller.record.upload_id === old, "Session changed after reload");
      assert(JSON.stringify(attempts) === "[2,3]", `Resent confirmed parts: ${attempts}`);
      localStorage.removeItem(marker);
      return { ...await verify(controller, 17), put_indices: attempts };
    });
    await fresh();
    await check("store-ack-transfer-kill-before-confirm", async () => {
      const file = await source(8388608);
      await control("hold-confirm");
      const calls = [];
      const transport = { ...api, put: (...args) => { calls.push(args[1]); return api.put(...args); } };
      const controller = flow({}, transport);
      const running = controller.resume(file).catch(() => {});
      const killed = await control("kill-transfer");
      await running;
      await control("wait-verified", { upload_id: controller.record.upload_id });
      const replacement = flow({}, transport);
      await replacement.resume(file);
      assert(calls.length === 1, "Re-sent STORED part after reconciliation");
      return { ...await verify(replacement, 17), killed, put_indices: calls };
    });
    await fresh();
    await check("worker-kill-before-publish", async () => {
      const file = await source(8388609);
      await control("hold-worker");
      const controller = flow();
      const running = controller.resume(file);
      const killed = await control("kill-worker");
      await running;
      return { ...await verify(controller, 17), killed };
    });
    await fresh();
    await check("cleanup-delete-ack-lost", async () => {
      const file = await source(3 * 8388608);
      let controller, count = 0;
      const transport = { ...api, put: async (...args) => {
        const result = await api.put(...args);
        if (++count === 1) controller.pause();
        return result;
      } };
      controller = flow({}, transport);
      await controller.resume(file).catch(() => {});
      await control("lose-delete-ack");
      const aborting = controller.abort();
      const pending = await control("observe-cleanup-pending");
      await aborting;
      await control("cleanup-status", { upload_id: controller.record.upload_id });
      return { pending, state: controller.record.server_state };
    });
    state.textContent = "SMALL PASS";
    await removeSources();
    return;
  }
  assert(results.length === 0, "Existing C5 results: use the continuation/large buttons");
  for (const size of [0, 8388607, 8388608, 8388609, 16777233]) {
    await fresh();
    await check(`boundary-${size}`, async () => {
      const controller = flow();
      await controller.resume(await source(size));
      return verify(controller, 17);
    });
  }
  for (const kind of ["create", "part", "complete"]) {
    await fresh();
    await check(`lost-${kind}-response`, async () => {
      const file = await source(8388609);
      await control("fault", { kind });
      let controller = flow();
      try { await controller.resume(file); } catch (error) {
        assert(kind === "create", error.message);
        const key = controller.record.key;
        controller = flow();
        await controller.resume(file);
        assert(controller.record.key === key, "Create key changed");
      }
      return verify(controller, 17);
    });
  }
  await fresh();
  await check("prepare-real-reload", async () => {
    const file = await source(3 * 8388608 + 17);
    let controller, count = 0;
    const transport = { ...api, put: async (...args) => {
      const response = await api.put(...args);
      if (++count === 2) controller.pause();
      return response;
    } };
    controller = flow({}, transport);
    await controller.resume(file).catch(() => {});
    const session = await api.session(controller.record.upload_id);
    assert(session.confirmed_parts === 2 && session.state === "OPEN", "Pause boundary wrong");
    localStorage.setItem(marker, "yes");
    return { upload_id: session.upload_id, confirmed_parts: 2 };
  });
  state.textContent = "RELOAD_READY: reload then click Run small and crash checks";
}

async function large(gib, slow = false) {
  await fresh();
  const name = slow ? "slow-replica-64MiB" : `large-${gib}GiB`;
  await check(name, async () => {
    const size = slow ? 64 * 1024 ** 2 : gib * 1024 ** 3;
    const file = await source(size);
    const referenceResponse = await fetch(`/api/v2/_c5/reference?size=${size}&seed=17`);
    assert(referenceResponse.ok, "Reference failed");
    const expected = (await referenceResponse.json()).sha256;
    await control("phase", { name: `${name}-upload` });
    const timings = { part_hash_seconds: 0, put_seconds: 0, put_count: 0, heap_peak: 0 };
    const started = now();
    const heap = setInterval(() => { timings.heap_peak = Math.max(timings.heap_peak,
      performance.memory?.usedJSHeapSize || 0); }, 100);
    let firstHash = true;
    const transport = { ...api, put: async (...args) => {
      const at = now();
      const response = await api.put(...args);
      timings.put_seconds += (now() - at) / 1000;
      timings.put_count++;
      return response;
    }, complete: async (...args) => {
      timings.complete_at_seconds = (now() - started) / 1000;
      await control("phase", { name: `${name}-finalize` });
      return api.complete(...args);
    } };
    const controller = flow({ hash: async (...args) => {
      const at = now();
      const result = await hashBlob(...args);
      const seconds = (now() - at) / 1000;
      if (firstHash) {
        firstHash = false;
        timings.pre_hash_seconds = seconds;
        assert(result === expected, "Pre-hash mismatch");
      } else timings.part_hash_seconds += seconds;
      return result;
    } }, transport);
    if (slow) await control("slow", { seconds: 0.02 });
    try {
      await controller.resume(file);
      timings.available_seconds = (now() - started) / 1000;
      timings.complete_to_available_seconds = timings.available_seconds - timings.complete_at_seconds;
      await control("phase", { name: `${name}-disk-verify` });
      const disk = await verify(controller, 17);
      assert(timings.put_count === Math.ceil(size / 8388608), "Part count mismatch");
      return { ...disk, ...timings, source: "OPFS File, incremental 256KiB reads", concurrency: 1 };
    } finally {
      clearInterval(heap);
      if (slow) await control("slow", { seconds: 0 });
      await removeSources();
    }
  });
  state.textContent = `${name} PASS`;
}

for (const [id, action] of [["small", small], ["large1", () => large(1)],
  ["large10", () => large(10)], ["slow", () => large(0, true)]]) {
  document.getElementById(id).onclick = async () => {
    const buttons = [...document.querySelectorAll("button")];
    buttons.forEach((button) => { button.disabled = true; });
    try { await action(); } catch (error) { state.textContent = `FAILED: ${error.message}`; }
    finally { buttons.forEach((button) => { button.disabled = false; }); }
  };
}
report.textContent = JSON.stringify(results, null, 2);
