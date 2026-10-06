import { useRef, useState } from "react";
import {
  Dialog,
  DialogDescription,
  DialogPanel,
  DialogTitle,
} from "@headlessui/react";
import { TrashIcon } from "@heroicons/react/24/outline";
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
        <Button
          color="error"
          variant="outlined"
          className="gap-2"
          disabled={!enabled}
          onClick={() =>
            setTarget({ fileId: file.file_id, name: file.original_name })
          }
        >
          <TrashIcon className="size-4" />
          Xóa
        </Button>
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
              Xác nhận xóa “{target?.name}”. Thao tác không thể hoàn tác. File sẽ
              bị ẩn khỏi danh sách và không thể tải xuống.
            </DialogDescription>
            <p className="dark:text-dark-300 mt-2 text-xs break-all text-gray-500">
              ID: {target?.fileId}
            </p>
            {!enabled && (
              <p role="alert" className="mt-3 text-sm text-amber-700 dark:text-amber-400">
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
