import { Link } from "react-router";
import { Button } from "components/ui/Button";
import { Table } from "components/ui/Table/Table";
import { formatBytes, formatTimestamp } from "../utils/snapshots";
import SnapshotStatus from "./SnapshotStatus";
import StatusBadge from "./StatusBadge";

export default function FilesPanel({ query, params, setFilter, setOffset }) {
  const data = query.data;
  return (
    <section
      aria-label="Danh sách file"
      className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5"
    >
      <div className="flex flex-wrap items-center justify-between gap-4">
        <h2 className="text-lg font-semibold">Danh sách file</h2>
        <div className="flex flex-wrap items-center gap-4 text-sm">
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={params.include_inactive}
              onChange={(event) =>
                setFilter({ include_inactive: event.target.checked })
              }
            />
            Hiện file đang tải, lỗi và đang xóa
          </label>
          <label className="flex items-center gap-2">
            Số file mỗi trang
            <select
              className="dark:border-dark-500 rounded border border-gray-300 bg-transparent p-1"
              value={params.limit}
              onChange={(event) =>
                setFilter({ limit: Number(event.target.value) })
              }
            >
              {[10, 25, 50, 100].map((limit) => (
                <option key={limit} value={limit}>
                  {limit}
                </option>
              ))}
            </select>
          </label>
        </div>
      </div>
      <SnapshotStatus query={query} />
      {data && (
        <>
          {data.items.length === 0 ? (
            <p className="dark:text-dark-200 py-6 text-sm text-gray-600">
              {params.include_inactive
                ? "Không có file trong danh sách này."
                : "Chưa có file AVAILABLE."}
            </p>
          ) : (
            <div className="mt-4 overflow-x-auto">
              <Table hoverable className="w-full text-left text-sm">
                <caption className="sr-only">
                  Files theo thời gian tạo giảm dần
                </caption>
                <thead>
                  <tr>
                    {["File", "Kích thước", "Trạng thái", "Thời gian tạo"].map(
                      (label) => (
                        <th
                          scope="col"
                          key={label}
                          className="px-3 py-3 font-semibold"
                        >
                          {label}
                        </th>
                      ),
                    )}
                  </tr>
                </thead>
                <tbody>
                  {data.items.map((file) => (
                    <tr key={file.file_id}>
                      <td className="max-w-md px-3 py-3">
                        <Link
                          className="text-primary-600 dark:text-primary-400 break-all hover:underline"
                          to={`/files/${file.file_id}`}
                        >
                          {file.original_name}
                        </Link>
                        <p className="dark:text-dark-200 mt-1 text-xs break-all text-gray-500">
                          {file.file_id}
                        </p>
                      </td>
                      <td
                        className="px-3 py-3 whitespace-nowrap"
                        title={`${file.size_bytes} byte`}
                      >
                        {formatBytes(file.size_bytes)}
                      </td>
                      <td className="px-3 py-3">
                        <StatusBadge status={file.status} />
                      </td>
                      <td className="px-3 py-3 whitespace-nowrap">
                        <time
                          dateTime={file.created_at}
                          title={file.created_at}
                        >
                          {formatTimestamp(file.created_at)}
                        </time>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </Table>
            </div>
          )}
          <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-sm">
            <p>
              {data.total === 0
                ? "0 file"
                : `${data.offset + 1}–${data.offset + data.items.length} / ${data.total} file`}
            </p>
            <div className="flex gap-2">
              <Button
                variant="outlined"
                disabled={query.isFetching || params.offset === 0}
                onClick={() =>
                  setOffset(Math.max(0, params.offset - params.limit))
                }
              >
                Trang trước
              </Button>
              <Button
                variant="outlined"
                disabled={
                  query.isFetching || params.offset + params.limit >= data.total
                }
                onClick={() => setOffset(params.offset + params.limit)}
              >
                Trang sau
              </Button>
            </div>
          </div>
        </>
      )}
    </section>
  );
}
