import { ArrowDownTrayIcon } from "@heroicons/react/24/outline";
import { Button } from "components/ui/Button";
import { useTransfer } from "../hooks/useTransfer";

export default function DownloadPanel({ file, connection, notFound }) {
  const transfer = useTransfer();
  const available = !notFound && file?.status === "AVAILABLE";
  const disabled =
    !available ||
    !connection.canMutate ||
    transfer.busy ||
    transfer.needsReconcile;

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Button
        className="gap-2"
        disabled={disabled}
        onClick={() =>
          void transfer.download(file, { canMutate: connection.canMutate })
        }
      >
        <ArrowDownTrayIcon className="size-4" />
        Tải xuống
      </Button>
      {!available && (
        <p className="dark:text-dark-300 text-sm text-gray-500">
          Chỉ tải xuống file AVAILABLE
        </p>
      )}
      {available && !connection.canMutate && (
        <p className="dark:text-dark-300 text-sm text-gray-500">
          Đang chờ Metadata sẵn sàng…
        </p>
      )}
    </div>
  );
}
