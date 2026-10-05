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
    <section
      aria-label="Metadata file"
      className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5"
    >
      <h2 className="text-lg font-semibold">Metadata file</h2>
      <SnapshotStatus query={query} />
      {file && (
        <>
          <div className="mt-4">
            <StatusBadge status={file.status} />
          </div>
          <dl className="mt-4 grid gap-3 sm:grid-cols-2">
            {rows.map(([label, value]) => (
              <div key={label}>
                <dt className="dark:text-dark-200 text-xs text-gray-500">
                  {label}
                </dt>
                <dd className="mt-1 text-sm break-all">{value}</dd>
              </div>
            ))}
          </dl>
          <p className="mt-4 text-sm">
            {file.known_readable
              ? "Có replica khả dụng theo snapshot."
              : "Chưa xác nhận file đọc được theo snapshot."}
          </p>
          {file.status === "UPLOADING" && (
            <p className="mt-2 text-sm">
              Counters chỉ tính các chunk đã tạo; file chưa commit.
            </p>
          )}
          {file.status === "DELETING" && (
            <p className="mt-2 text-sm">
              File đã xóa logic, đang dọn các bản sao còn lại.
            </p>
          )}
          <p className="dark:text-dark-200 mt-2 text-xs text-gray-500">
            known_readable và counters lấy từ metadata/health. Download vẫn cần
            xác minh bytes; snapshot false vẫn có thể đọc sau probing, snapshot
            true không bảo đảm request tiếp theo thành công.
          </p>
        </>
      )}
    </section>
  );
}
