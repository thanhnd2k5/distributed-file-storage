import { useCallback, useState } from "react";
import { useDropzone } from "react-dropzone";
import { ArrowPathIcon, CloudArrowUpIcon } from "@heroicons/react/24/outline";
import clsx from "clsx";
import { Button } from "components/ui/Button";
import { useDocumentTitle } from "hooks/useDocumentTitle";
import { useFilesPage } from "./hooks/useFilesPage";
import { useTransfer } from "./hooks/useTransfer";
import FilesPanel from "./components/FilesPanel";
import UploadPanel from "./components/UploadPanel";

export default function StoragePage() {
  useDocumentTitle("Lưu trữ file — Distributed File Storage");
  const page = useFilesPage();
  const transfer = useTransfer();
  const [file, setFile] = useState(null);
  const blocked =
    transfer.busy ||
    transfer.needsReconcile ||
    !page.connection.canMutate;

  const onDrop = useCallback(
    (accepted) => {
      if (blocked || !accepted?.[0]) return;
      setFile(accepted[0]);
    },
    [blocked],
  );

  const { getRootProps, getInputProps, isDragActive, open } = useDropzone({
    onDrop,
    multiple: false,
    noClick: true,
    noKeyboard: true,
    disabled: blocked,
  });

  return (
    <div
      className="dark:bg-dark-900/40 relative min-h-[70vh] rounded-2xl bg-white px-4 py-6 shadow-sm sm:px-6 dark:shadow-none"
      {...getRootProps()}
    >
      <input {...getInputProps()} />
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-gray-900 dark:text-dark-50">
            File của tôi
          </h1>
          <p className="mt-1 text-sm text-gray-500 dark:text-dark-300">
            Quản lý file trên cụm lưu trữ phân tán
          </p>
        </div>
        <Button
          variant="outlined"
          className="gap-2"
          onClick={(event) => {
            event.stopPropagation();
            page.refresh();
          }}
          disabled={page.fetching}
        >
          <ArrowPathIcon
            className={`size-4 ${page.fetching ? "animate-spin" : ""}`}
          />
          Làm mới
        </Button>
      </div>
      <UploadPanel
        connection={page.connection}
        file={file}
        setFile={setFile}
        openPicker={open}
        blocked={blocked}
      />
      <FilesPanel
        query={page.files}
        params={page.params}
        setFilter={page.setFilter}
        setOffset={page.setOffset}
      />
      <div
        className={clsx(
          "pointer-events-none absolute inset-0 z-20 flex items-center justify-center rounded-2xl border-2 border-dashed transition-opacity",
          isDragActive && !blocked
            ? "border-primary-500 bg-primary-50/95 opacity-100 dark:border-primary-400 dark:bg-dark-900/95"
            : "opacity-0",
        )}
        aria-hidden={!isDragActive}
      >
        <div className="flex flex-col items-center gap-2 text-primary-700 dark:text-primary-300">
          <CloudArrowUpIcon className="size-12" />
          <p className="text-base font-medium">Thả file để tải lên</p>
        </div>
      </div>
    </div>
  );
}
