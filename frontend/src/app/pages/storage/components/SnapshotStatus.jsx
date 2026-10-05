import { formatTimestamp } from "../utils/snapshots";

export default function SnapshotStatus({ query }) {
  return (
    <div className="mt-3 text-sm">
      {query.isPending && query.fetchStatus !== "idle" && (
        <p role="status">Đang tải dữ liệu…</p>
      )}
      {query.error && (
        <p role="alert" className="text-red-600 dark:text-red-400">
          {query.error.message} ({query.error.code})
        </p>
      )}
      {query.dataUpdatedAt > 0 && (
        <p
          className={
            query.isError
              ? "mt-1 text-amber-700 dark:text-amber-400"
              : "dark:text-dark-200 mt-1 text-gray-500"
          }
        >
          {query.isError ? "Dữ liệu cũ · Lần đọc thành công: " : "Cập nhật: "}
          <time dateTime={new Date(query.dataUpdatedAt).toISOString()}>
            {formatTimestamp(query.dataUpdatedAt)}
          </time>
          {query.isFetching && " · Đang cập nhật…"}
        </p>
      )}
    </div>
  );
}
