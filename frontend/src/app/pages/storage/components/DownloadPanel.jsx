import { Button } from "components/ui/Button";
import { useTransfer } from "../hooks/useTransfer";

export default function DownloadPanel({ file, connection, notFound }) {
  const transfer = useTransfer();
  const available = !notFound && file?.status === "AVAILABLE";
  return (
    <section
      aria-label="Tải xuống file"
      className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5"
    >
      <h2 className="text-lg font-semibold">Tải xuống file</h2>
      <Button
        className="mt-4"
        disabled={
          !available ||
          !connection.canMutate ||
          transfer.busy ||
          transfer.needsReconcile
        }
        onClick={() =>
          void transfer.download(file, { canMutate: connection.canMutate })
        }
      >
        Tải xuống
      </Button>
      {!available && (
        <p className="mt-2 text-sm">Chỉ tải xuống file AVAILABLE.</p>
      )}
      {available && !connection.canMutate && (
        <p className="mt-2 text-sm">
          Chờ Metadata sẵn sàng và operation lock trống để tải xuống.
        </p>
      )}
    </section>
  );
}
