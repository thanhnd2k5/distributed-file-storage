import { formatBytes, formatTimestamp } from "../utils/snapshots";
import SnapshotStatus from "./SnapshotStatus";
import StatusBadge from "./StatusBadge";

export default function PlacementPanel({ query, file }) {
  return (
    <section aria-label="Chunk placement" className="mt-8">
      <div className="border-b border-gray-200 pb-3 dark:border-dark-600">
        <h2 className="text-sm font-medium text-gray-700 dark:text-dark-100">
          Chunk placement
        </h2>
      </div>
      <div className="mt-1">
        <SnapshotStatus query={query} />
      </div>
      {query.data && (
        <>
          {query.data.items.length === 0 && (
            <p className="mt-4 text-sm text-gray-600 dark:text-dark-200">
              {file?.status === "AVAILABLE" && file.size_bytes === 0
                ? "File rỗng: không có chunk hoặc replica."
                : "Chưa có chunk được tạo."}
            </p>
          )}
          <div className="mt-2 divide-y divide-gray-100 dark:divide-dark-700">
            {query.data.items.map((chunk) => (
              <article key={chunk.chunk_id} className="py-4">
                <div className="flex flex-wrap items-center gap-2">
                  <h3 className="text-sm font-medium text-gray-900 dark:text-dark-50">
                    Chunk #{chunk.chunk_index}
                  </h3>
                  <StatusBadge status={chunk.state} />
                  <span className="dark:text-dark-300 text-sm text-gray-500">
                    {formatBytes(chunk.size_bytes)} · Live replicas:{" "}
                    {chunk.live_replica_count} · Live domains:{" "}
                    {chunk.live_failure_domain_count}
                  </span>
                </div>
                <p className="dark:text-dark-400 mt-1 text-xs break-all text-gray-400">
                  {chunk.chunk_id}
                </p>
                <p className="dark:text-dark-400 mt-0.5 text-xs break-all text-gray-400">
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
                  <table className="w-full min-w-[32rem] text-left text-sm">
                    <caption className="sr-only">
                      Replica history của chunk {chunk.chunk_index}
                    </caption>
                    <thead>
                      <tr className="text-xs font-medium text-gray-500 dark:text-dark-300">
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
                            className="px-2 py-2 font-medium"
                          >
                            {label}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100 dark:divide-dark-700">
                      {chunk.replicas.map((replica) => (
                        <tr
                          key={replica.node_id}
                          className="transition-colors hover:bg-gray-50 dark:hover:bg-dark-800/80"
                        >
                          <td className="px-2 py-2.5">
                            <p className="text-gray-900 dark:text-dark-50">
                              {replica.node_id}
                            </p>
                            <p className="dark:text-dark-300 text-xs text-gray-500">
                              {replica.failure_domain}
                            </p>
                          </td>
                          <td className="px-2 py-2.5">
                            <StatusBadge status={replica.node_status} />
                          </td>
                          <td className="px-2 py-2.5">
                            <StatusBadge status={replica.status} />
                          </td>
                          <td className="px-2 py-2.5 text-gray-500 dark:text-dark-300">
                            {replica.cleanup_pending
                              ? "Đang chờ"
                              : "Không pending"}
                          </td>
                          <td className="px-2 py-2.5 text-gray-500 dark:text-dark-300">
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
                  </table>
                  {chunk.replicas.length === 0 && (
                    <p className="py-3 text-sm text-gray-500 dark:text-dark-300">
                      Chưa có replica mapping.
                    </p>
                  )}
                </div>
              </article>
            ))}
          </div>
        </>
      )}
      <p className="dark:text-dark-400 mt-3 text-xs text-gray-400">
        History giữ cả mappings DOWN/PENDING/DELETED. Live counts chỉ tính
        VERIFIED trên node enabled ACTIVE và không cleanup_pending. Mọi node
        unreachable không đồng nghĩa chunk đã mất vĩnh viễn.
      </p>
    </section>
  );
}
