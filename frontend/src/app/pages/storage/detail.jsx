import { Link, useParams } from "react-router";
import { ArrowLeftIcon, ArrowPathIcon } from "@heroicons/react/24/outline";
import { Button } from "components/ui/Button";
import { useDocumentTitle } from "hooks/useDocumentTitle";
import { formatBytes, formatTimestamp } from "./utils/snapshots";
import { useFileDetailPage } from "./hooks/useFileDetailPage";
import FileGlyph from "./components/FileGlyph";
import StatusBadge from "./components/StatusBadge";
import FileMetadata from "./components/FileMetadata";
import PlacementPanel from "./components/PlacementPanel";
import DownloadPanel from "./components/DownloadPanel";
import DeletePanel from "./components/DeletePanel";
import RepairPanel from "./components/RepairPanel";

function EmptyState({ role, title, body }) {
  return (
    <div className="px-2 py-16 text-center" role={role}>
      <p className="text-base font-medium text-gray-800 dark:text-dark-50">
        {title}
      </p>
      <p className="dark:text-dark-300 mx-auto mt-2 max-w-sm text-sm text-gray-500">
        {body}
      </p>
      <Link
        to="/"
        className="text-primary-600 dark:text-primary-400 mt-4 inline-block text-sm font-medium hover:underline"
      >
        Về danh sách file
      </Link>
    </div>
  );
}

export default function StorageDetailPage() {
  const { fileId } = useParams();
  const page = useFileDetailPage(fileId);
  const file = page.valid ? page.file.data : null;
  const blocked = !page.valid || page.notFound || page.deleted;
  useDocumentTitle(
    file?.original_name
      ? `${file.original_name} — Distributed File Storage`
      : "Chi tiết file — Distributed File Storage",
  );

  let body;
  if (!page.valid) {
    body = (
      <EmptyState
        role="alert"
        title="File ID không hợp lệ"
        body="Hãy mở file từ danh sách để xem chi tiết và thao tác."
      />
    );
  } else if (page.deleted) {
    body = (
      <EmptyState
        title="File đã được xóa"
        body="Metadata đã xác nhận xóa. Quay về danh sách để xem các file còn lại."
      />
    );
  } else if (page.notFound) {
    body = (
      <EmptyState
        role="alert"
        title="Không tìm thấy file"
        body="File không tồn tại hoặc đã bị xóa. Quay về danh sách để xem trạng thái hiện tại."
      />
    );
  } else {
    body = (
      <>
        <section
          aria-label="Thao tác file"
          className="mt-6 flex flex-wrap items-center gap-2"
        >
          <DownloadPanel
            file={file}
            connection={page.connection}
            notFound={false}
          />
          <DeletePanel
            file={file}
            connection={page.connection}
            notFound={false}
            readError={page.file.isError}
          />
        </section>
        <FileMetadata query={page.file} />
        <PlacementPanel query={page.chunks} file={file} />
      </>
    );
  }

  return (
    <div className="dark:bg-dark-900/40 relative min-h-[70vh] rounded-2xl bg-white px-4 py-6 shadow-sm sm:px-6 dark:shadow-none">
      <Link
        to="/"
        className="dark:text-dark-300 inline-flex items-center gap-1 text-sm text-gray-500 transition-colors hover:text-gray-800 dark:hover:text-dark-50"
      >
        <ArrowLeftIcon className="size-4" />
        File của tôi
      </Link>

      <div className="mt-4 flex flex-wrap items-start justify-between gap-3">
        <div className="flex min-w-0 items-start gap-3">
          <FileGlyph name={file?.original_name || "file"} />
          <div className="min-w-0">
            <h1 className="truncate text-2xl font-semibold tracking-tight text-gray-900 dark:text-dark-50">
              {file?.original_name ||
                (!page.valid
                  ? "File ID không hợp lệ"
                  : page.deleted || page.notFound
                    ? "Không tìm thấy file"
                    : "Đang tải…")}
            </h1>
            <div className="mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-1 text-sm text-gray-500 dark:text-dark-300">
              {file?.status && <StatusBadge status={file.status} />}
              {file && (
                <>
                  <span title={`${file.size_bytes} byte`}>
                    {formatBytes(file.size_bytes)}
                  </span>
                  <span aria-hidden>·</span>
                  <time dateTime={file.created_at} title={file.created_at}>
                    {formatTimestamp(file.created_at)}
                  </time>
                </>
              )}
            </div>
            <p className="dark:text-dark-400 mt-1 text-xs break-all text-gray-400">
              {fileId}
            </p>
          </div>
        </div>
        <Button
          variant="outlined"
          className="gap-2"
          onClick={page.refresh}
          disabled={page.fetching}
        >
          <ArrowPathIcon
            className={`size-4 ${page.fetching ? "animate-spin" : ""}`}
          />
          Làm mới
        </Button>
      </div>

      {body}

      <RepairPanel
        key={fileId}
        connection={page.connection}
        fileId={fileId?.toLowerCase()}
        file={page.file.data}
        unavailable={blocked}
        readError={page.file.isError}
      />
    </div>
  );
}
