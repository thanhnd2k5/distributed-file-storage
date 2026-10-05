import { useId, useState } from "react";
import { Button } from "components/ui/Button";
import { useTransfer } from "../hooks/useTransfer";
import { isFileId } from "../utils/snapshots";
import { repairScope } from "../utils/repairs";

export default function RepairPanel({
  connection,
  fileId = null,
  file,
  unavailable = false,
  readError = false,
}) {
  const [nodeId, setNodeId] = useState("");
  const [maxChunks, setMaxChunks] = useState("8");
  const formId = useId();
  const [revision, setRevision] = useState(0);
  const { state, busy, needsReconcile, runRepair, reconcile } = useTransfer();
  const filters = {
    file_id: fileId,
    node_id: nodeId || null,
    max_chunks: Number(maxChunks),
  };
  const scope = `${repairScope(filters)}:${formId}:${revision}`;
  const matches = state.kind === "repair" && state.scope === scope;
  const result = matches && state.phase === "scanned" ? state.result : null;
  const repairState = state.kind === "repair" ? state : null;
  const valid =
    Number.isInteger(filters.max_chunks) &&
    filters.max_chunks >= 1 &&
    filters.max_chunks <= 8;
  const disabled =
    busy ||
    needsReconcile ||
    !connection.canMutate ||
    !valid ||
    (fileId !== null &&
      (!isFileId(fileId) ||
        unavailable ||
        readError ||
        file?.status !== "AVAILABLE"));
  const inputClass =
    "dark:border-dark-500 dark:bg-dark-900 mt-1 w-full rounded-lg border border-gray-300 px-3 py-2";
  return (
    <section
      aria-label="Manual repair"
      className="dark:border-dark-600 dark:bg-dark-800 mt-6 rounded-xl border border-gray-200 bg-white p-5"
    >
      <h2 className="text-lg font-semibold">Manual repair</h2>
      <p className="mt-2 text-sm">
        Mỗi lần bấm chạy một lượt. Kết quả chỉ phản ánh các chunk đã kiểm tra
        trong lượt đó.
      </p>
      <p className="mt-2 text-sm break-all">
        Phạm vi file: {fileId || "Toàn cluster"}
      </p>
      <div className="mt-3 grid gap-3 sm:grid-cols-2">
        <label>
          Node filter
          <select
            className={inputClass}
            value={nodeId}
            onChange={(event) => {
              setNodeId(event.target.value);
              setRevision((previous) => previous + 1);
            }}
            disabled={busy || needsReconcile || !connection.nodes.isSuccess}
          >
            <option value="">Tất cả node</option>
            {connection.nodes.data?.items.map((node) => (
              <option key={node.node_id} value={node.node_id}>
                {node.node_id}
                {!node.enabled ? " (DISABLED)" : ` (${node.status})`}
              </option>
            ))}
          </select>
        </label>
        <label>
          Max chunks mỗi lượt
          <input
            className={inputClass}
            type="number"
            min="1"
            max="8"
            step="1"
            value={maxChunks}
            onChange={(event) => {
              setMaxChunks(event.target.value);
              setRevision((previous) => previous + 1);
            }}
            disabled={busy || needsReconcile}
          />
        </label>
      </div>
      {!valid && (
        <p role="alert" className="mt-2 text-red-600">
          Max chunks phải là số nguyên từ 1 đến 8.
        </p>
      )}
      {connection.nodes.isError && (
        <p className="mt-2 text-sm">
          Chưa đọc được registry node; filter node tạm khóa.
        </p>
      )}
      {fileId && (unavailable || readError || file?.status !== "AVAILABLE") && (
        <p className="mt-2 text-sm">
          Repair chỉ dành cho file AVAILABLE đã đọc thành công.
        </p>
      )}
      <div className="mt-3 flex flex-wrap gap-3">
        <Button
          onClick={() =>
            runRepair(filters, { canMutate: connection.canMutate, scope })
          }
          disabled={disabled}
        >
          Bắt đầu scan mới
        </Button>
        {result?.remaining_chunks > 0 && (
          <Button
            variant="outlined"
            onClick={() =>
              runRepair(filters, {
                canMutate: connection.canMutate,
                continuation: true,
                scope,
              })
            }
            disabled={disabled}
          >
            Tiếp tục lượt sau
          </Button>
        )}
      </div>
      {matches && state.phase === "repairing" && (
        <p role="status" className="mt-3">
          Đang chạy một lượt repair…
        </p>
      )}
      {!matches && repairState?.phase === "repairing" && (
        <p role="status" className="mt-3">
          Lượt repair của phạm vi trước vẫn đang chờ phản hồi; chuyển trang
          không hủy công việc phía server.
        </p>
      )}
      {repairState?.error && (
        <div className="mt-3">
          {!matches && (
            <p>
              Kết quả lỗi thuộc phạm vi trước:{" "}
              {repairState.request.file_id || "Toàn cluster"}, node{" "}
              {repairState.request.node_id || "tất cả"}.
            </p>
          )}
          <p role="alert" className="text-red-600 dark:text-red-400">
            {repairState.error.message} ({repairState.error.code})
          </p>
          {needsReconcile && (
            <>
              <p className="mt-2">
                Kết quả chưa xác định. Không tiếp tục cursor hay tự gửi lại
                POST.
              </p>
              <Button
                className="mt-2"
                variant="outlined"
                disabled={busy}
                onClick={reconcile}
              >
                {state.reconciling
                  ? "Đang đọc lại repair…"
                  : "Đọc lại placement/cluster"}
              </Button>
            </>
          )}
          {repairState.reconcileError && (
            <p role="alert" className="mt-2 text-red-600">
              {repairState.reconcileError.message}
            </p>
          )}
          {repairState.reconciled && (
            <p className="mt-2">
              Đã đọc lại snapshot. Kiểm tra placement/cluster rồi tự bắt đầu
              scan mới; cursor cũ đã bỏ.
            </p>
          )}
        </div>
      )}
      {result && (
        <div aria-label="Kết quả repair" className="mt-4 text-sm">
          <p>
            Lượt {state.totals.pages}: checked_chunks {result.checked_chunks} ·
            repaired_replicas {result.repaired_replicas} · remaining_chunks{" "}
            {result.remaining_chunks}
          </p>
          <p className="mt-1">
            Tổng scan này: {state.totals.checked} chunk đã kiểm tra ·{" "}
            {state.totals.repaired} replica có Store ack.
          </p>
          <p role="status" className="mt-2">
            {result.remaining_chunks === 0
              ? "Đã quét hết phạm vi. Không đồng nghĩa mọi chunk đã khỏe."
              : result.checked_chunks === 0
                ? "Lượt chưa tiến thêm; vẫn còn chunk chưa quét. Có thể bấm tiếp tục lượt sau."
                : "Còn chunk chưa quét. Bấm tiếp tục để chạy một lượt nữa."}
          </p>
          <p className="mt-2 break-all">
            Cursor lượt sau:{" "}
            {result.next_after
              ? `${result.next_after.file_id} / chunk ${result.next_after.chunk_index}`
              : "null"}
          </p>
          <details className="mt-2">
            <summary>Summaries các lượt trong scan này</summary>
            <ul>
              {state.pages.map((page, index) => (
                <li key={index}>
                  Lượt {index + 1}: checked {page.result.checked_chunks},
                  repaired {page.result.repaired_replicas}, remaining{" "}
                  {page.result.remaining_chunks}
                </li>
              ))}
            </ul>
          </details>
          {result.results.some((item) =>
            ["ERROR", "UNAVAILABLE", "NO_DESTINATION"].includes(item.outcome),
          ) && (
            <p role="alert" className="mt-2 text-amber-700 dark:text-amber-400">
              Có chunk chưa repair thành công. Cursor đã đi qua chunk đó; tự bắt
              đầu scan mới để kiểm tra lại.
            </p>
          )}
          {result.results.length > 0 && (
            <div className="mt-3 overflow-x-auto">
              <table className="w-full text-left">
                <caption className="sr-only">
                  Outcomes của lượt repair vừa chạy
                </caption>
                <thead>
                  <tr>
                    {[
                      "File / chunk",
                      "Outcome",
                      "Live replicas",
                      "Domain degraded",
                      "Message",
                    ].map((label) => (
                      <th key={label} scope="col" className="px-2 py-2">
                        {label}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.results.map((item) => (
                    <tr key={item.chunk_id}>
                      <td className="px-2 py-2">
                        <p className="break-all">{item.file_id}</p>
                        <p>Chunk {item.chunk_index}</p>
                      </td>
                      <td className="px-2 py-2">{item.outcome}</td>
                      <td className="px-2 py-2">{item.live_replica_count}</td>
                      <td className="px-2 py-2">
                        {item.domain_degraded ? "Có" : "Không"}
                      </td>
                      <td className="px-2 py-2">{item.message || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
      <p className="dark:text-dark-200 mt-3 text-xs text-gray-500">
        Remaining là chunk chưa quét, không phải replica thiếu. Repaired chỉ đếm
        Store ack. HEALTHY vẫn có thể domain degraded;
        ERROR/UNAVAILABLE/NO_DESTINATION cần xem từng chunk. Đổi filter hoặc
        reload bắt đầu phạm vi mới, không có job nền.
      </p>
    </section>
  );
}
