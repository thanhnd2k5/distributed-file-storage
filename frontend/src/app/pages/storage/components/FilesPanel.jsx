import { Link } from "react-router";
import { useAutoAnimate } from "@formkit/auto-animate/react";
import {
  ChevronLeftIcon,
  ChevronRightIcon,
} from "@heroicons/react/24/outline";
import { Button } from "components/ui/Button";
import { formatBytes, formatTimestamp } from "../utils/snapshots";
import SnapshotStatus from "./SnapshotStatus";
import StatusBadge from "./StatusBadge";
import FileGlyph from "./FileGlyph";

export default function FilesPanel({ query, params, setFilter, setOffset }) {
  const data = query.data;
  const [listRef] = useAutoAnimate();

  return (
    <section aria-label="Danh sách file" className="mt-8">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-gray-200 pb-3 dark:border-dark-600">
        <h2 className="text-sm font-medium text-gray-700 dark:text-dark-100">
          File của bạn
        </h2>
        <div className="flex flex-wrap items-center gap-4 text-sm">
          <label className="dark:text-dark-300 flex items-center gap-2 text-gray-500">
            <input
              type="checkbox"
              className="rounded border-gray-300"
              checked={params.include_inactive}
              onChange={(event) =>
                setFilter({ include_inactive: event.target.checked })
              }
            />
            Hiện inactive
          </label>
          <label className="dark:text-dark-300 flex items-center gap-2 text-gray-500">
            Mỗi trang
            <select
              className="dark:border-dark-500 dark:bg-dark-800 rounded-md border border-gray-200 bg-transparent px-2 py-1"
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

      <div className="mt-1">
        <SnapshotStatus query={query} />
      </div>

      {data && (
        <>
          {data.items.length === 0 ? (
            <div className="px-2 py-20 text-center">
              <p className="text-base font-medium text-gray-800 dark:text-dark-50">
                {params.include_inactive
                  ? "Không có file trong bộ lọc này"
                  : "Thả file vào đây hoặc bấm Tải lên"}
              </p>
              <p className="dark:text-dark-300 mx-auto mt-2 max-w-sm text-sm text-gray-500">
                {params.include_inactive
                  ? "Bỏ lọc inactive để xem file sẵn sàng, hoặc tải lên file mới."
                  : "File sẽ xuất hiện trong danh sách sau khi Metadata xác nhận upload."}
              </p>
            </div>
          ) : (
            <div className="mt-2 overflow-x-auto">
              <table className="w-full min-w-[36rem] text-left text-sm">
                <caption className="sr-only">
                  Files theo thời gian tạo giảm dần
                </caption>
                <thead>
                  <tr className="text-xs font-medium text-gray-500 dark:text-dark-300">
                    <th scope="col" className="px-2 py-2 font-medium">
                      Tên
                    </th>
                    <th scope="col" className="px-2 py-2 font-medium">
                      Kích thước
                    </th>
                    <th scope="col" className="px-2 py-2 font-medium">
                      Trạng thái
                    </th>
                    <th scope="col" className="px-2 py-2 font-medium">
                      Tạo lúc
                    </th>
                  </tr>
                </thead>
                <tbody ref={listRef} className="divide-y divide-gray-100 dark:divide-dark-700">
                  {data.items.map((file) => (
                    <tr
                      key={file.file_id}
                      className="group transition-colors hover:bg-gray-50 dark:hover:bg-dark-800/80"
                    >
                      <td className="max-w-md px-2 py-2.5">
                        <Link
                          to={`/files/${file.file_id}`}
                          className="flex items-center gap-3"
                        >
                          <FileGlyph name={file.original_name} />
                          <span className="truncate font-medium text-gray-900 group-hover:text-primary-600 dark:text-dark-50 dark:group-hover:text-primary-400">
                            {file.original_name}
                          </span>
                        </Link>
                      </td>
                      <td
                        className="px-2 py-2.5 whitespace-nowrap text-gray-500 dark:text-dark-300"
                        title={`${file.size_bytes} byte`}
                      >
                        {formatBytes(file.size_bytes)}
                      </td>
                      <td className="px-2 py-2.5">
                        <StatusBadge status={file.status} />
                      </td>
                      <td className="px-2 py-2.5 whitespace-nowrap text-gray-500 dark:text-dark-300">
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
              </table>
            </div>
          )}

          <div className="mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-gray-100 pt-3 text-sm dark:border-dark-700">
            <p className="text-gray-500 dark:text-dark-300">
              {data.total === 0
                ? "0 file"
                : `${data.offset + 1}–${data.offset + data.items.length} / ${data.total}`}
            </p>
            <div className="flex gap-1">
              <Button
                variant="flat"
                className="!px-2"
                disabled={query.isFetching || params.offset === 0}
                aria-label="Trang trước"
                onClick={() =>
                  setOffset(Math.max(0, params.offset - params.limit))
                }
              >
                <ChevronLeftIcon className="size-5" />
              </Button>
              <Button
                variant="flat"
                className="!px-2"
                disabled={
                  query.isFetching ||
                  params.offset + params.limit >= data.total
                }
                aria-label="Trang sau"
                onClick={() => setOffset(params.offset + params.limit)}
              >
                <ChevronRightIcon className="size-5" />
              </Button>
            </div>
          </div>
        </>
      )}
    </section>
  );
}
