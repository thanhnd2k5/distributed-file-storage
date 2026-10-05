import { formatBytes } from "../utils/snapshots";
import SnapshotStatus from "./SnapshotStatus";

export default function ClusterPanel({ query }) {
  const data = query.data;
  const counters = data
    ? [
        ["ACTIVE", data.nodes.active],
        ["SUSPECTED", data.nodes.suspected],
        ["DOWN", data.nodes.down],
        ["Disabled", data.nodes.disabled],
        ["File AVAILABLE", data.files_available],
        ["Chunk thiếu replica", data.under_replicated_chunks],
        ["Chunk chưa có replica khả dụng", data.unavailable_chunks],
        ["Chunk thiếu domain", data.domain_degraded_chunks],
        ["Chunk dư replica", data.over_replicated_chunks],
        ["Replica chờ cleanup", data.cleanup_pending_replicas],
      ]
    : [];
  return (
    <section
      aria-label="Tổng quan cluster"
      className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5"
    >
      <h2 className="text-lg font-semibold">Tổng quan cluster</h2>
      <SnapshotStatus query={query} />
      {data && (
        <>
          <dl className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
            {counters.map(([name, value]) => (
              <div
                className="dark:bg-dark-700 rounded-lg bg-gray-50 p-3"
                key={name}
              >
                <dt className="dark:text-dark-200 text-xs text-gray-600">
                  {name}
                </dt>
                <dd className="mt-2 text-xl font-semibold">{value}</dd>
              </div>
            ))}
          </dl>
          <p className="mt-4 text-sm">
            Failure domains đang active / cấu hình:{" "}
            {data.active_failure_domains} / {data.configured_failure_domains} ·
            RF mặc định {data.default_replication_factor} · Chunk mặc định{" "}
            {formatBytes(data.chunk_size_bytes)} · File tối đa{" "}
            {formatBytes(data.max_file_size_bytes)}
          </p>
          <p className="mt-2 text-sm">
            {data.operation_busy
              ? "Operation lock: đang bận"
              : "Operation lock: đang trống"}
            {query.isError && " (snapshot cũ)"}
          </p>
        </>
      )}
      <p className="dark:text-dark-200 mt-3 text-xs text-gray-500">
        Counters chunk chỉ tính file AVAILABLE; cleanup tính mọi file còn
        pending. Đây là quan sát theo metadata/health, không bảo đảm integrity
        hoặc lần download tiếp theo.
      </p>
    </section>
  );
}
