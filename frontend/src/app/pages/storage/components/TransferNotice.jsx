import { Link } from "react-router";
import { Button } from "components/ui/Button";
import { useTransfer } from "../hooks/useTransfer";
import { isFileId } from "../utils/snapshots";
import DeletionNotice from "./DeletionNotice";

export default function TransferNotice() {
  const { state, reconcile } = useTransfer();
  if (state.phase === "idle" || state.kind === "repair") return null;
  if (state.kind === "delete") return <DeletionNotice />;
  const descriptions = {
    sending: "Đang gửi file",
    replicating: "Đang lưu các bản sao. Chờ Metadata xác nhận…",
    preparing: "Đang chuẩn bị file…",
    receiving: "Đang nhận file",
    success:
      state.kind === "upload"
        ? "Đã tải lên file (201)."
        : "Đã chuyển file cho trình duyệt lưu.",
  };
  const id = isFileId(state.error?.details?.file_id)
    ? state.error.details.file_id
    : state.fileId;
  return (
    <div className="mt-4 text-sm" aria-label="Trạng thái truyền file">
      <p className="break-all">{state.name}</p>
      {state.phase !== "error" && (
        <p role="status">{descriptions[state.phase]}</p>
      )}
      {state.progress !== null &&
        ["sending", "receiving"].includes(state.phase) && (
          <div className="mt-2">
            <progress
              aria-label={
                state.kind === "upload"
                  ? "Tiến độ gửi body"
                  : "Tiến độ nhận body"
              }
              max="100"
              value={state.progress}
              className="w-full"
            />
            <p>
              {state.progress}%{" "}
              {state.kind === "upload" ? "body đã gửi" : "body đã nhận"}
            </p>
          </div>
        )}
      {state.error && (
        <p role="alert" className="mt-2 text-red-600 dark:text-red-400">
          {state.error.message} ({state.error.code})
        </p>
      )}
      {id && (
        <Link
          className="text-primary-600 dark:text-primary-400 mt-2 inline-block hover:underline"
          to={`/files/${id}`}
        >
          Xem trạng thái file
        </Link>
      )}
      {state.needsReconcile && (
        <Button
          variant="outlined"
          className="mt-3"
          disabled={state.reconciling}
          onClick={reconcile}
        >
          {state.reconciling
            ? "Đang đọc lại danh sách…"
            : "Đọc lại danh sách trước khi thử lại"}
        </Button>
      )}
      {state.reconcileError && (
        <p role="alert" className="mt-2 text-red-600">
          {state.reconcileError.message}
        </p>
      )}
      {state.reconciled && (
        <p className="mt-2">
          Đã đọc lại danh sách. Kiểm tra file theo tên và thời gian tạo, kể cả
          file đang tải/lỗi, trước khi tự chọn tải lên lại. File trùng tên vẫn
          được phép.
        </p>
      )}
    </div>
  );
}
