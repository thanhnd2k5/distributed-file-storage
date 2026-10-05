import { Badge } from "components/ui/Badge";
import SnapshotStatus from "./SnapshotStatus";

export default function MetadataStatus({ ready, cluster }) {
  const available = ready.data?.status === "READY" && !ready.isError;
  return (
    <section
      aria-label="Kết nối Metadata"
      className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5"
    >
      <div className="flex flex-wrap items-center gap-3">
        <h2 className="font-semibold">Metadata</h2>
        <Badge color={available ? "success" : "warning"} variant="soft">
          {ready.isPending
            ? "Đang kết nối"
            : available
              ? "Sẵn sàng"
              : "Chưa sẵn sàng"}
        </Badge>
        {cluster.data && !cluster.isError && (
          <span className="text-sm">
            {cluster.data.operation_busy
              ? "Đang có thao tác dữ liệu"
              : "Chưa có thao tác dữ liệu"}
          </span>
        )}
      </div>
      <SnapshotStatus query={ready} />
      {!available && !ready.isPending && (
        <p className="dark:text-dark-200 mt-2 text-sm text-gray-600">
          Các snapshot còn truy cập được vẫn được hiển thị bên dưới.
        </p>
      )}
    </section>
  );
}
