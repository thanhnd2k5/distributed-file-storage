import { Link } from "react-router";
import { Button } from "components/ui/Button";
import { useTransfer } from "../hooks/useTransfer";
import SnapshotStatus from "./SnapshotStatus";

export default function DeletionNotice() {
  const { state, deleteSnapshot, reconcile } = useTransfer();
  const count =
    deleteSnapshot.data?.status === "DELETING"
      ? deleteSnapshot.data.cleanup_pending_replicas
      : state.deleteResult?.cleanup_pending_replicas;
  return (
    <section
      aria-label="Trạng thái xóa file"
      className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5 text-sm"
    >
      <h2 className="text-lg font-semibold">Theo dõi xóa file</h2>
      <p className="mt-2 break-all">{state.name}</p>
      {state.phase === "deleting" && (
        <p role="status" className="mt-2">
          Đang gửi yêu cầu xóa. Chờ Metadata xác nhận…
        </p>
      )}
      {state.phase === "cleanup" && (
        <>
          <p role="status" className="mt-2">
            Đã xóa file, đang dọn các bản sao còn lại.
          </p>
          <p className="mt-2">
            Replica chờ cleanup: {count ?? "Chưa có snapshot"}.
          </p>
          <SnapshotStatus query={deleteSnapshot} />
          <p className="mt-2">
            Đang theo dõi bằng cách đọc trạng thái. Cleanup có thể cần nhiều
            lượt.
          </p>
        </>
      )}
      {state.phase === "deleted" && (
        <p role="status" className="mt-2">
          Metadata xác nhận đã xóa file và dọn xong các bản sao.
        </p>
      )}
      {state.phase === "gone" && (
        <p role="status" className="mt-2">
          File không còn hiển thị trong API đọc. Đã kết thúc theo dõi file;
          trạng thái này không xác nhận riêng từng bản sao trên đĩa.
        </p>
      )}
      {state.error && (
        <p role="alert" className="mt-2 text-red-600 dark:text-red-400">
          {state.error.message} ({state.error.code})
        </p>
      )}
      {state.needsReconcile && (
        <Button
          variant="outlined"
          className="mt-3"
          disabled={state.reconciling}
          onClick={reconcile}
        >
          {state.reconciling ? "Đang đọc lại file…" : "Đọc lại trạng thái xóa"}
        </Button>
      )}
      {state.reconcileError && (
        <p role="alert" className="mt-2 text-red-600">
          {state.reconcileError.message}
        </p>
      )}
      {state.reconciled && state.observedStatus && state.phase === "error" && (
        <p className="mt-2">
          Đã đọc lại file: {state.observedStatus}. Kiểm tra trạng thái trước khi
          xác nhận xóa lần nữa.
        </p>
      )}
      <Link
        className="text-primary-600 dark:text-primary-400 mt-3 inline-block break-all hover:underline"
        to={`/files/${state.fileId}`}
      >
        Xem trạng thái file
      </Link>
    </section>
  );
}
