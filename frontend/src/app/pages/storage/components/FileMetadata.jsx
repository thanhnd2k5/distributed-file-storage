import { formatBytes, formatTimestamp } from "../utils/snapshots";
import SnapshotStatus from "./SnapshotStatus";
import StatusBadge from "./StatusBadge";

export default function FileMetadata({ query }) {
  const file = query.data;
  const rows = file
    ? [
        ["File ID", file.file_id],
        ["Tên file", file.original_name],
        ["Content type", file.content_type],
        ["Kích thước", formatBytes(file.size_bytes)],
        ["Thời gian tạo", formatTimestamp(file.created_at)],
        ["Chunk size của file", formatBytes(file.chunk_size_bytes)],
        ["Số chunk", file.total_chunks],
        ["Replication factor của file", file.replication_factor],
        [
          "SHA-256 toàn file",
          file.checksum_sha256 ?? "Chưa có checksum commit",
        ],
        ["Mã lỗi", file.error_code ?? "Không có"],
        ["Chunk thiếu replica", file.under_replicated_chunks],
        ["Chunk chưa có replica khả dụng", file.unavailable_chunks],
        ["Chunk thiếu domain", file.domain_degraded_chunks],
        ["Chunk dư replica", file.over_replicated_chunks],
        ["Replica chờ cleanup", file.cleanup_pending_replicas],
      ]
    : [];

  return (
    <section aria-label="Metadata file" className="mt-8">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-gray-200 pb-3 dark:border-dark-600">
        <h2 className="text-sm font-medium text-gray-700 dark:text-dark-100">
          Metadata
        </h2>
        {file?.status && <StatusBadge status={file.status} />}
      </div>
      <div className="mt-1">
        <SnapshotStatus query={query} />
      </div>
      {file && (
        <>
          <dl className="mt-4 grid gap-x-6 gap-y-3 sm:grid-cols-2">
            {rows.map(([label, value]) => (
              <div key={label}>
                <dt className="dark:text-dark-300 text-xs text-gray-500">
                  {label}
                </dt>
                <dd className="mt-0.5 text-sm break-all text-gray-900 dark:text-dark-50">
                  {value}
                </dd>
              </div>
            ))}
          </dl>
          <p className="mt-4 text-sm text-gray-600 dark:text-dark-200">
            {file.known_readable
              ? "Có replica khả dụng theo snapshot."
              : "Chưa xác nhận file đọc được theo snapshot."}
          </p>
          {file.status === "UPLOADING" && (
            <p className="mt-1 text-sm text-gray-600 dark:text-dark-200">
              Counters chỉ tính các chunk đã tạo; file chưa commit.
            </p>
          )}
          {file.status === "DELETING" && (
            <p className="mt-1 text-sm text-gray-600 dark:text-dark-200">
              File đã xóa logic, đang dọn các bản sao còn lại.
            </p>
          )}
          <p className="dark:text-dark-400 mt-3 text-xs text-gray-400">
            known_readable và counters lấy từ metadata/health. Download vẫn cần
            xác minh bytes; snapshot false vẫn có thể đọc sau probing, snapshot
            true không bảo đảm request tiếp theo thành công.
          </p>
        </>
      )}
    </section>
  );
}
