import {
  ArrowUpTrayIcon,
  XMarkIcon,
} from "@heroicons/react/24/outline";
import { Button } from "components/ui/Button";
import { useTransfer } from "../hooks/useTransfer";
import { formatBytes } from "../utils/snapshots";
import FileGlyph from "./FileGlyph";

export default function UploadPanel({
  connection,
  file,
  setFile,
  openPicker,
  blocked,
}) {
  const transfer = useTransfer();
  const maxSize = connection.cluster.data?.max_file_size_bytes;
  const oversized = file && Number.isFinite(maxSize) && file.size > maxSize;

  return (
    <section aria-label="Tải lên file" className="mt-6">
      <div className="flex flex-wrap items-center gap-2">
        <Button
          type="button"
          className="gap-2"
          disabled={blocked}
          onClick={(event) => {
            event.stopPropagation();
            openPicker();
          }}
        >
          <ArrowUpTrayIcon className="size-4" />
          Tải lên
        </Button>
        {!connection.canMutate && (
          <p className="dark:text-dark-300 text-sm text-gray-500">
            Đang chờ Metadata sẵn sàng…
          </p>
        )}
        <p className="dark:text-dark-400 text-xs text-gray-400">
          hoặc kéo thả file vào trang
          {Number.isFinite(maxSize) ? ` · tối đa ${formatBytes(maxSize)}` : ""}
        </p>
      </div>

      {file && (
        <div className="dark:border-dark-600 dark:bg-dark-800/80 mt-4 flex flex-wrap items-center gap-3 rounded-xl border border-gray-200 bg-gray-50 px-4 py-3">
          <FileGlyph name={file.name} />
          <div className="min-w-0 flex-1">
            <p className="truncate text-sm font-medium text-gray-900 dark:text-dark-50">
              {file.name}
            </p>
            <p className="dark:text-dark-300 text-xs text-gray-500">
              {formatBytes(file.size)}
            </p>
          </div>
          <Button
            type="button"
            disabled={
              oversized ||
              !connection.canMutate ||
              transfer.busy ||
              transfer.needsReconcile
            }
            onClick={(event) => {
              event.stopPropagation();
              void transfer.upload(file, {
                canMutate: connection.canMutate,
                maxSize,
              });
            }}
          >
            Bắt đầu tải lên
          </Button>
          {!transfer.busy && (
            <button
              type="button"
              className="dark:hover:bg-dark-700 rounded-lg p-2 text-gray-500 hover:bg-white"
              aria-label="Bỏ chọn file"
              onClick={(event) => {
                event.stopPropagation();
                setFile(null);
              }}
            >
              <XMarkIcon className="size-5" />
            </button>
          )}
        </div>
      )}

      {oversized && (
        <p role="alert" className="mt-2 text-sm text-red-600">
          File vượt giới hạn {formatBytes(maxSize)}. Chọn file nhỏ hơn.
        </p>
      )}
    </section>
  );
}
