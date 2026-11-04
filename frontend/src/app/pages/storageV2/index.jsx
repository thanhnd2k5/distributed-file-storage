import { Link } from "react-router";
import { useDocumentTitle } from "hooks/useDocumentTitle";
import { useV2Upload } from "./hooks/useV2Upload";
import UploadSession from "./components/UploadSession";

const button =
  "rounded-lg border border-gray-300 px-4 py-2 text-sm font-medium disabled:cursor-not-allowed disabled:opacity-50";

export default function UploadV2Page() {
  useDocumentTitle("Upload có thể tiếp tục — V2");
  const flow = useV2Upload();
  const { view, file } = flow;
  const record = view.record;
  const terminal = ["COMPLETED", "ABORTED", "EXPIRED", "FAILED"].includes(
    record?.server_state,
  );
  const needsFile =
    !record?.abort_requested &&
    (!record?.server_state || record.server_state === "OPEN");
  return (
    <main className="min-h-screen bg-slate-100 px-4 py-10 text-gray-900 sm:px-8">
      <div className="mx-auto max-w-3xl rounded-2xl bg-white p-6 shadow-sm sm:p-8">
        <Link to="/" className="text-sm text-blue-700 underline">
          Về kho lưu trữ
        </Link>
        <h1 className="mt-5 text-2xl font-semibold">
          Upload V2 · tiếp tục theo phiên
        </h1>
        <p className="mt-2 text-sm leading-6 text-gray-600">
          Chọn nguồn để kiểm tra SHA-256 trước khi tạo hoặc tiếp tục phiên. Sau
          reload, chọn lại đúng nội dung; tên và kích thước giống nhau chưa đủ.
          File chỉ hoàn tất sau khi server xác minh toàn bộ bytes.
        </p>
        <div className="mt-6">
          <label htmlFor="v2-source" className="mb-2 block text-sm font-medium">
            File nguồn {record ? "(chọn lại để resume)" : ""}
          </label>
          <input
            id="v2-source"
            type="file"
            disabled={view.busy}
            onChange={(event) => flow.setFile(event.target.files?.[0] || null)}
            className="block w-full rounded-lg border border-gray-300 p-3 text-sm"
          />
          {file && (
            <p className="mt-2 text-sm break-all text-gray-600">
              Đang chọn: {file.name}
            </p>
          )}
        </div>
        <div className="mt-5 flex flex-wrap gap-2">
          <button
            className={`${button} border-blue-700 bg-blue-700 text-white hover:bg-blue-800`}
            onClick={flow.resume}
            disabled={view.busy || terminal || (needsFile && !file)}
          >
            {record ? "Tiếp tục phiên" : "Kiểm tra và tải lên"}
          </button>
          <button className={button} onClick={flow.pause} disabled={!view.busy}>
            Dừng trên máy này
          </button>
          <button
            className={button}
            onClick={flow.refresh}
            disabled={view.busy || !record?.upload_id}
          >
            Tra trạng thái
          </button>
          <button
            className={`${button} text-red-700`}
            onClick={flow.abort}
            disabled={
              !record ||
              (terminal && !record.cleanup_pending) ||
              view.phase === "aborting"
            }
          >
            Hủy phiên trên server
          </button>
          <button
            className={button}
            onClick={flow.newUpload}
            disabled={view.busy || !terminal || record?.cleanup_pending}
          >
            Phiên mới
          </button>
        </div>
        <UploadSession view={view} />
        <p className="mt-5 text-xs leading-5 text-gray-500">
          Trình duyệt lưu upload ID và key tạo phiên, không lưu bytes file. Dừng
          chỉ ngừng xử lý tại đây; hủy phiên yêu cầu server cleanup. Màn hình
          này xử lý một phiên mỗi origin tại một thời điểm.
        </p>
      </div>
    </main>
  );
}
