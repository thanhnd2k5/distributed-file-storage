import { Table } from "components/ui/Table/Table";
import { formatBytes, formatTimestamp } from "../utils/snapshots";
import SnapshotStatus from "./SnapshotStatus";
import StatusBadge from "./StatusBadge";

export default function NodesPanel({ query }) {
  return (
    <section
      aria-label="Storage nodes"
      className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5"
    >
      <h2 className="text-lg font-semibold">Storage nodes</h2>
      <SnapshotStatus query={query} />
      {query.data && (
        <div className="mt-4 overflow-x-auto">
          <Table className="w-full text-left text-sm">
            <caption className="sr-only">
              Health và filesystem snapshot của từng node
            </caption>
            <thead>
              <tr>
                {[
                  "Node / domain",
                  "Trạng thái",
                  "Capacity / free / used",
                  "Health thành công gần nhất",
                ].map((label) => (
                  <th
                    key={label}
                    scope="col"
                    className="px-3 py-3 font-semibold"
                  >
                    {label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {query.data.items.map((node) => (
                <tr key={node.node_id}>
                  <td className="px-3 py-3">
                    <p className="font-medium">{node.node_id}</p>
                    <p>{node.failure_domain}</p>
                    <p className="dark:text-dark-200 text-xs text-gray-500">
                      {node.host}:{node.port}
                    </p>
                  </td>
                  <td className="px-3 py-3">
                    <StatusBadge
                      status={node.enabled ? node.status : "DISABLED"}
                    />
                    {!node.enabled && (
                      <p className="mt-1 text-xs">Detector: {node.status}</p>
                    )}
                  </td>
                  <td className="px-3 py-3">
                    <dl>
                      {[
                        ["Capacity", node.capacity_bytes],
                        ["Free", node.available_bytes],
                        ["Used", node.used_bytes],
                      ].map(([label, value]) => (
                        <div key={label} className="flex gap-2">
                          <dt>{label}:</dt>
                          <dd
                            title={value == null ? undefined : `${value} byte`}
                          >
                            {formatBytes(value)}
                          </dd>
                        </div>
                      ))}
                    </dl>
                  </td>
                  <td className="px-3 py-3">
                    <time dateTime={node.last_success_at ?? undefined}>
                      {formatTimestamp(node.last_success_at)}
                    </time>
                    {node.last_error && (
                      <p className="mt-1 max-w-sm text-xs break-words text-red-600 dark:text-red-400">
                        {node.last_error}
                      </p>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </Table>
          {query.data.items.length === 0 && (
            <p className="py-4">Chưa có node trong registry.</p>
          )}
        </div>
      )}
      <p className="dark:text-dark-200 mt-3 text-xs text-gray-500">
        Capacity/free là filesystem của từng node; các node có thể dùng chung ổ
        đĩa nên không cộng thành dung lượng cluster. Used là bytes committed
        trong DATA_DIR. ACTIVE chỉ là health snapshot.
      </p>
    </section>
  );
}
