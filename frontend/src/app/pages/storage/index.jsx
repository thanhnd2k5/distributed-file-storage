import { Button } from "components/ui/Button";
import { useDocumentTitle } from "hooks/useDocumentTitle";
import { useFilesPage } from "./hooks/useFilesPage";
import MetadataStatus from "./components/MetadataStatus";
import FilesPanel from "./components/FilesPanel";
import ClusterPanel from "./components/ClusterPanel";
import NodesPanel from "./components/NodesPanel";
import UploadPanel from "./components/UploadPanel";
import TransferNotice from "./components/TransferNotice";
import RepairPanel from "./components/RepairPanel";

export default function StoragePage() {
  useDocumentTitle("Lưu trữ file — Distributed File Storage");
  const page = useFilesPage();
  return (
    <>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold">Lưu trữ file</h1>
        <Button
          variant="outlined"
          onClick={page.refresh}
          disabled={page.fetching}
        >
          Làm mới dữ liệu
        </Button>
      </div>
      <p className="dark:text-dark-200 mt-2 text-gray-600">
        File và tình trạng cụm lưu trữ. Snapshot được cập nhật mỗi 4 giây khi
        trang đang hiển thị.
      </p>
      <MetadataStatus
        ready={page.connection.ready}
        cluster={page.connection.cluster}
      />
      <UploadPanel connection={page.connection} />
      <TransferNotice />
      <FilesPanel
        query={page.files}
        params={page.params}
        setFilter={page.setFilter}
        setOffset={page.setOffset}
      />
      <ClusterPanel query={page.connection.cluster} />
      <NodesPanel query={page.connection.nodes} />
      <RepairPanel connection={page.connection} />
    </>
  );
}
