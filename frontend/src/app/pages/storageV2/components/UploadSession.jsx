const phases = {
  idle: "Chọn file nguồn",
  preparing: "Chuẩn bị / kiểm tra file",
  creating: "Đang tạo hoặc đối soát phiên",
  uploading: "Đang gửi các part còn thiếu",
  verifying: "Đang chờ server xác minh toàn file",
  aborting: "Đang hủy và chờ cleanup",
  paused: "Đã dừng; có thể tiếp tục",
  completed: "Hoàn tất — file đã được công bố",
  aborted: "Phiên đã kết thúc",
  failed: "File chưa được công bố",
};

function size(bytes = 0) {
  return `${new Intl.NumberFormat("vi-VN").format(bytes)} byte`;
}

export default function UploadSession({ view }) {
  const { record, session } = view;
  return (
    <section
      aria-label="Trạng thái phiên"
      className="mt-6 space-y-4 rounded-xl border border-gray-200 bg-gray-50 p-5"
    >
      <h2 className="text-lg font-semibold" role="status">
        {phases[view.phase]}
      </h2>
      {view.phase === "preparing" && (
        <div>
          <label htmlFor="v2-hash-progress" className="block text-sm">
            Đã kiểm tra {size(view.prepared)} / {size(view.total)}
          </label>
          <progress
            id="v2-hash-progress"
            value={view.prepared}
            max={view.total || 1}
            className="mt-2 w-full"
          />
          <p className="text-sm text-gray-600">
            Đọc tăng dần trong worker. Phiên mới chỉ được tạo sau bước này.
          </p>
        </div>
      )}
      {record && (
        <dl className="grid gap-3 text-sm sm:grid-cols-2">
          <div>
            <dt className="text-gray-600">Nguồn của phiên</dt>
            <dd className="font-medium break-all">
              {record.request.original_name} · {size(record.request.size_bytes)}
            </dd>
          </div>
          <div>
            <dt className="text-gray-600">Upload ID</dt>
            <dd className="font-mono break-all">
              {record.upload_id ||
                "Chưa nhận được ID; giữ nguyên Idempotency-Key"}
            </dd>
          </div>
          <div className="sm:col-span-2">
            <dt className="text-gray-600">SHA-256 nguồn bất biến</dt>
            <dd className="font-mono break-all">
              {record.request.expected_sha256}
            </dd>
          </div>
          <div>
            <dt className="text-gray-600">Trạng thái server gần nhất</dt>
            <dd>{session?.state || record.server_state || "Chưa rõ"}</dd>
          </div>
          <div>
            <dt className="text-gray-600">Đã xác nhận trên server</dt>
            <dd>
              {session ? (
                <>
                  {size(session.confirmed_bytes)} · {session.confirmed_parts} /{" "}
                  {session.part_count} part
                </>
              ) : (
                "Chưa cập nhật sau reload; bấm Tra trạng thái."
              )}
            </dd>
          </div>
          {view.currentPart !== null && (
            <div className="sm:col-span-2">
              <dt className="text-gray-600">
                Part {view.currentPart}: bytes đã gửi từ trình duyệt
              </dt>
              <dd>{size(view.sent)} — đang chờ confirmation</dd>
            </div>
          )}
        </dl>
      )}
      {record?.cleanup_pending && (
        <p
          role="status"
          className="rounded-lg bg-amber-100 p-3 text-sm text-amber-950"
        >
          Cleanup còn pending. Phiên được giữ để tiếp tục tra trạng thái; bytes
          đã gửi không chứng minh dữ liệu đã được dọn.
        </p>
      )}
      {record?.next_retry_at > Date.now() && (
        <p className="text-sm text-gray-600">
          Tiếp tục sau{" "}
          {new Date(record.next_retry_at).toLocaleTimeString("vi-VN")} theo
          Retry-After.
        </p>
      )}
      {view.error && (
        <p
          role="alert"
          className="rounded-lg bg-red-50 p-3 text-sm text-red-800"
        >
          {view.error}
        </p>
      )}
    </section>
  );
}
