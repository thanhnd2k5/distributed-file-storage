import { useRef, useState } from "react";
import {
  Dialog,
  DialogDescription,
  DialogPanel,
  DialogTitle,
} from "@headlessui/react";
import { Button } from "components/ui/Button";
import { useTransfer } from "../hooks/useTransfer";
import { canDeleteFile } from "../utils/deletions";

export default function DeletePanel({ file, connection, notFound, readError }) {
  const transfer = useTransfer();
  const [target, setTarget] = useState(null);
  const cancelRef = useRef(null);
  const available = !notFound && canDeleteFile(file);
  const enabled =
    available &&
    !readError &&
    connection.canMutate &&
    !transfer.busy &&
    !transfer.needsReconcile;
  return (
    <>
      {available && (
        <section
          aria-label="Xóa file"
          className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5"
        >
          <h2 className="text-lg font-semibold">Xóa file</h2>
          <Button
            color="error"
            variant="outlined"
            className="mt-4"
            disabled={!enabled}
            onClick={() =>
              setTarget({ fileId: file.file_id, name: file.original_name })
            }
          >
            Xóa file
          </Button>
          <p className="mt-2 text-sm">
            File sẽ bị ẩn khỏi danh sách và không thể tải xuống. Các bản sao có
            thể cần thêm thời gian để dọn.
          </p>
        </section>
      )}
      <Dialog
        open={Boolean(target)}
        onClose={() => setTarget(null)}
        initialFocus={cancelRef}
        className="relative z-50"
      >
        <div className="fixed inset-0 bg-black/50" aria-hidden="true" />
        <div className="fixed inset-0 flex items-center justify-center overflow-y-auto p-4">
          <DialogPanel className="dark:bg-dark-800 dark:text-dark-100 w-full max-w-md rounded-xl bg-white p-6 text-gray-800">
            <DialogTitle className="text-lg font-semibold">
              Xóa file?
            </DialogTitle>
            <DialogDescription className="mt-3 break-all">
              Xác nhận xóa “{target?.name}”. Thao tác không thể hoàn tác.
            </DialogDescription>
            <p className="mt-2 text-xs break-all">ID: {target?.fileId}</p>
            {!enabled && (
              <p role="alert" className="mt-3 text-sm">
                Trạng thái hoặc kết nối đã thay đổi. Đóng hộp thoại và đọc lại
                file.
              </p>
            )}
            <div className="mt-5 flex flex-wrap justify-end gap-3">
              <Button
                ref={cancelRef}
                variant="outlined"
                onClick={() => setTarget(null)}
              >
                Hủy
              </Button>
              <Button
                color="error"
                disabled={!enabled || file?.file_id !== target?.fileId}
                onClick={() => {
                  setTarget(null);
                  void transfer.remove(file, { canMutate: enabled });
                }}
              >
                Xác nhận xóa
              </Button>
            </div>
          </DialogPanel>
        </div>
      </Dialog>
    </>
  );
}
