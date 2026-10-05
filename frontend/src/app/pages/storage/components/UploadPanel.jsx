import { useState } from "react";
import { Button } from "components/ui/Button";
import { useTransfer } from "../hooks/useTransfer";
import { formatBytes } from "../utils/snapshots";

export default function UploadPanel({ connection }) {
  const [file, setFile] = useState(null);
  const transfer = useTransfer();
  const maxSize = connection.cluster.data?.max_file_size_bytes;
  const oversized = file && Number.isFinite(maxSize) && file.size > maxSize;
  return (
    <section
      aria-label="Tải lên file"
      className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5"
    >
      <h2 className="text-lg font-semibold">Tải lên file</h2>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          void transfer.upload(file, {
            canMutate: connection.canMutate,
            maxSize,
          });
        }}
      >
        <label className="mt-4 block text-sm">
          Chọn một file
          <input
            type="file"
            className="mt-2 block w-full min-w-0 text-sm"
            disabled={transfer.busy || transfer.needsReconcile}
            onChange={(event) => setFile(event.target.files?.[0] ?? null)}
          />
        </label>
        <p className="dark:text-dark-200 mt-2 text-sm text-gray-600">
          {Number.isFinite(maxSize)
            ? `Tối đa ${formatBytes(maxSize)} theo snapshot cluster. `
            : "Backend kiểm tra giới hạn file. "}
          Cho phép file rỗng.
        </p>
        {file && (
          <p className="mt-2 text-sm break-all">
            Đã chọn: {file.name} · {formatBytes(file.size)}
          </p>
        )}
        {oversized && (
          <p role="alert" className="mt-2 text-sm text-red-600">
            File vượt giới hạn {formatBytes(maxSize)}. Chọn file nhỏ hơn.
          </p>
        )}
        {!connection.canMutate && (
          <p className="mt-2 text-sm">
            Chờ Metadata sẵn sàng và operation lock trống để tải lên.
          </p>
        )}
        <Button
          type="submit"
          className="mt-4"
          disabled={
            !file ||
            oversized ||
            !connection.canMutate ||
            transfer.busy ||
            transfer.needsReconcile
          }
        >
          Tải lên
        </Button>
      </form>
    </section>
  );
}
