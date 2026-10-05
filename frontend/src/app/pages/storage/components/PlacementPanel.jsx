import { Table } from "components/ui/Table/Table";
import { formatBytes, formatTimestamp } from "../utils/snapshots";
import SnapshotStatus from "./SnapshotStatus";
import StatusBadge from "./StatusBadge";

export default function PlacementPanel({ query, file }) {
  return (
    <section
      aria-label="Chunk placement"
      className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5"
    >
      <h2 className="text-lg font-semibold">Chunk placement</h2>
      <SnapshotStatus query={query} />
      {query.data && (
        <>
          {query.data.items.length === 0 && (
            <p className="mt-4 text-sm">
              {file?.status === "AVAILABLE" && file.size_bytes === 0
                ? "File rỗng: không có chunk hoặc replica."
                : "Chưa có chunk được tạo."}
            </p>
          )}
          <div className="mt-4 space-y-4">
            {query.data.items.map((chunk) => (
              <article
                key={chunk.chunk_id}
                className="dark:border-dark-600 rounded-lg border border-gray-200 p-4"
              >
                <div className="flex flex-wrap items-center gap-3">
                  <h3 className="font-semibold">Chunk #{chunk.chunk_index}</h3>
                  <StatusBadge status={chunk.state} />
                  <span className="text-sm">
                    {formatBytes(chunk.size_bytes)} · Live replicas:{" "}
                    {chunk.live_replica_count} · Live domains:{" "}
                    {chunk.live_failure_domain_count}
                  </span>
                </div>
                <p className="mt-2 text-xs break-all">ID: {chunk.chunk_id}</p>
                <p className="mt-1 text-xs break-all">
                  SHA-256: {chunk.checksum_sha256}
                </p>
                {(chunk.domain_degraded || chunk.over_replicated) && (
                  <p className="mt-2 text-sm text-amber-700 dark:text-amber-400">
                    {[
                      chunk.domain_degraded && "Thiếu failure domain",
                      chunk.over_replicated && "Dư replica",
                    ]
                      .filter(Boolean)
                      .join(" · ")}
                  </p>
                )}
                <div className="mt-3 overflow-x-auto">
                  <Table className="w-full text-left text-sm">
                    <caption className="sr-only">
                      Replica history của chunk {chunk.chunk_index}
                    </caption>
                    <thead>
                      <tr>
                        {[
                          "Node / domain",
                          "Health node",
                          "Replica",
                          "Cleanup",
                          "Xác minh / lỗi gần nhất",
                        ].map((label) => (
                          <th
                            scope="col"
                            key={label}
                            className="px-3 py-2 font-semibold"
                          >
                            {label}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {chunk.replicas.map((replica) => (
                        <tr key={replica.node_id}>
                          <td className="px-3 py-2">
                            <p>{replica.node_id}</p>
                            <p className="dark:text-dark-200 text-xs text-gray-500">
                              {replica.failure_domain}
                            </p>
                          </td>
                          <td className="px-3 py-2">
                            <StatusBadge status={replica.node_status} />
                          </td>
                          <td className="px-3 py-2">
                            <StatusBadge status={replica.status} />
                          </td>
                          <td className="px-3 py-2">
                            {replica.cleanup_pending
                              ? "Đang chờ"
                              : "Không pending"}
                          </td>
                          <td className="px-3 py-2">
                            <time
                              dateTime={replica.last_verified_at ?? undefined}
                            >
                              {formatTimestamp(replica.last_verified_at)}
                            </time>
                            {replica.last_error && (
                              <p className="mt-1 max-w-sm text-xs break-words text-red-600 dark:text-red-400">
                                {replica.last_error}
                              </p>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </Table>
                  {chunk.replicas.length === 0 && (
                    <p className="py-3 text-sm">Chưa có replica mapping.</p>
                  )}
                </div>
              </article>
            ))}
          </div>
        </>
      )}
      <p className="dark:text-dark-200 mt-3 text-xs text-gray-500">
        History giữ cả mappings DOWN/PENDING/DELETED. Live counts chỉ tính
        VERIFIED trên node enabled ACTIVE và không cleanup_pending. Mọi node
        unreachable không đồng nghĩa chunk đã mất vĩnh viễn.
      </p>
    </section>
  );
}
