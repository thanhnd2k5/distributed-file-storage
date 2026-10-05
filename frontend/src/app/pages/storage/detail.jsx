import { Link, useParams } from "react-router";
import { Button } from "components/ui/Button";
import { useDocumentTitle } from "hooks/useDocumentTitle";
import { useFileDetailPage } from "./hooks/useFileDetailPage";
import MetadataStatus from "./components/MetadataStatus";
import FileMetadata from "./components/FileMetadata";
import PlacementPanel from "./components/PlacementPanel";
import DownloadPanel from "./components/DownloadPanel";
import DeletePanel from "./components/DeletePanel";
import TransferNotice from "./components/TransferNotice";
import RepairPanel from "./components/RepairPanel";

export default function StorageDetailPage() {
  const { fileId } = useParams();
  useDocumentTitle("Chi tiết file — Distributed File Storage");
  const page = useFileDetailPage(fileId);
  return (
    <>
      <Link
        to="/"
        className="text-primary-600 dark:text-primary-400 hover:underline"
      >
        Quay về trang file
      </Link>
      <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold">Chi tiết file</h1>
        <Button
          variant="outlined"
          onClick={page.refresh}
          disabled={page.fetching}
        >
          Làm mới dữ liệu
        </Button>
      </div>
      <p className="dark:text-dark-200 mt-2 text-sm break-all text-gray-600">
        ID: {fileId}
      </p>
      <MetadataStatus
        ready={page.connection.ready}
        cluster={page.connection.cluster}
      />
      <DownloadPanel
        file={page.valid ? page.file.data : null}
        connection={page.connection}
        notFound={page.notFound || page.deleted}
      />
      <DeletePanel
        file={page.valid ? page.file.data : null}
        connection={page.connection}
        notFound={page.notFound || page.deleted}
        readError={page.file.isError}
      />
      <TransferNotice />
      <RepairPanel
        key={fileId}
        connection={page.connection}
        fileId={fileId?.toLowerCase()}
        file={page.file.data}
        unavailable={!page.valid || page.notFound || page.deleted}
        readError={page.file.isError}
      />
      {!page.valid ? (
        <p role="alert" className="mt-6">
          File ID không hợp lệ. Hãy mở file từ danh sách.
        </p>
      ) : page.deleted ? (
        <p className="mt-6">
          Metadata đã xác nhận xóa file. Quay về trang file để xem danh sách
          hiện tại.
        </p>
      ) : page.notFound ? (
        <p role="alert" className="mt-6">
          Không tìm thấy file hoặc file đã được xóa. Quay về trang file để xem
          danh sách hiện tại.
        </p>
      ) : (
        <>
          <FileMetadata query={page.file} />
          <PlacementPanel query={page.chunks} file={page.file.data} />
        </>
      )}
    </>
  );
}
