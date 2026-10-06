import { Button } from "components/ui/Button";
import { useDocumentTitle } from "hooks/useDocumentTitle";
import { useClusterPage } from "./hooks/useClusterPage";
import MetadataStatus from "./components/MetadataStatus";
import ClusterPanel from "./components/ClusterPanel";
import NodesPanel from "./components/NodesPanel";
import RepairPanel from "./components/RepairPanel";

export default function StorageClusterPage() {
  useDocumentTitle("Cụm lưu trữ — Distributed File Storage");
  const page = useClusterPage();
  return (
    <>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold">Cụm lưu trữ</h1>
        <Button
          variant="outlined"
          onClick={page.refresh}
          disabled={page.fetching}
        >
          Làm mới dữ liệu
        </Button>
      </div>
      <p className="dark:text-dark-200 mt-2 text-gray-600">
        Tình trạng Metadata, node và repair thủ công toàn cụm. Snapshot được cập
        nhật mỗi 4 giây khi trang đang hiển thị.
      </p>
      <MetadataStatus
        ready={page.connection.ready}
        cluster={page.connection.cluster}
      />
      <ClusterPanel query={page.connection.cluster} />
      <NodesPanel query={page.connection.nodes} />
      <RepairPanel connection={page.connection} />
    </>
  );
}
