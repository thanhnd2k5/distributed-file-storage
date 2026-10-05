import { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import {
  deleteFile,
  downloadFile,
  getFile,
  getChunks,
  getCluster,
  listFiles,
  repair,
  uploadFile,
} from "api/storage";
import { StorageApiError, toStorageApiError } from "api/storage/errors";
import { storageKeys } from "api/storage/queryKeys";
import { isFileId } from "../utils/snapshots";
import { canDeleteFile, deleteResultPhase } from "../utils/deletions";
import { repairScope, validRepairResult } from "../utils/repairs";
import { useSnapshotQuery } from "./useSnapshotQuery";
import {
  downloadFilename,
  saveBlob,
  transferProgress,
} from "../utils/transfers";

const initial = { phase: "idle", kind: null, progress: null, error: null };
const readHeader = (headers, key) => headers.get?.(key) ?? headers[key];

export function useStorageTransfers() {
  const [state, setState] = useState(initial);
  const busy = useRef(false);
  const mustReconcile = useRef(false);
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const invalidate = useCallback(() => {
    void Promise.all(
      [storageKeys.files, storageKeys.cluster, storageKeys.nodes].map(
        (queryKey) => queryClient.invalidateQueries({ queryKey }),
      ),
    ).catch(() => {});
  }, [queryClient]);
  const watchingDelete = state.kind === "delete" && state.phase === "cleanup";
  const deleteSnapshot = useSnapshotQuery({
    queryKey: storageKeys.detail(
      state.kind === "delete" ? state.fileId : "no-active-delete",
    ),
    queryFn: ({ signal }) => getFile(state.fileId, { signal }),
    enabled: watchingDelete && isFileId(state.fileId),
  });
  useEffect(() => {
    if (watchingDelete && deleteSnapshot.error?.status === 404) {
      setState((previous) =>
        previous.kind === "delete" && previous.phase === "cleanup"
          ? { ...previous, phase: "gone" }
          : previous,
      );
      invalidate();
    }
  }, [watchingDelete, deleteSnapshot.error?.status, invalidate]);

  async function upload(file, { canMutate, maxSize }) {
    if (busy.current || mustReconcile.current || !canMutate || !file) return;
    if (Number.isFinite(maxSize) && file.size > maxSize) return;
    busy.current = true;
    setState({ ...initial, kind: "upload", phase: "sending", name: file.name });
    try {
      const result = await uploadFile(file, {
        onUploadProgress: (event) => {
          const progress = transferProgress(event);
          setState((previous) =>
            ["sending", "replicating"].includes(previous.phase)
              ? {
                  ...previous,
                  progress,
                  phase:
                    progress === 100 || previous.phase === "replicating"
                      ? "replicating"
                      : "sending",
                }
              : previous,
          );
        },
      });
      if (
        result.httpStatus !== 201 ||
        result.status !== "AVAILABLE" ||
        !isFileId(result.file_id)
      ) {
        throw new StorageApiError(
          "Phản hồi upload chưa xác nhận file đã lưu.",
          {
            code: "UNEXPECTED_UPLOAD_RESPONSE",
            status: result.httpStatus,
            outcomeUnknown: true,
            details: { file_id: result.file_id },
          },
        );
      }
      setState({
        ...initial,
        kind: "upload",
        phase: "success",
        name: result.original_name,
        fileId: result.file_id,
      });
      invalidate();
      navigate(`/files/${result.file_id}`);
    } catch (cause) {
      const error = await toStorageApiError(cause);
      mustReconcile.current = error.outcomeUnknown;
      setState({
        ...initial,
        kind: "upload",
        phase: "error",
        name: file.name,
        error,
        needsReconcile: error.outcomeUnknown,
      });
    } finally {
      busy.current = false;
    }
  }

  async function reconcile() {
    if (busy.current || !mustReconcile.current) return;
    busy.current = true;
    setState((previous) => ({
      ...previous,
      reconciling: true,
      reconcileError: null,
    }));
    try {
      if (state.kind === "repair") {
        const params = { limit: 50, offset: 0, include_inactive: true };
        const reads = [
          { queryKey: storageKeys.cluster, queryFn: getCluster },
          ...(state.request.file_id
            ? [
                {
                  queryKey: storageKeys.detail(state.request.file_id),
                  queryFn: ({ signal }) =>
                    getFile(state.request.file_id, { signal }),
                },
                {
                  queryKey: storageKeys.chunks(state.request.file_id),
                  queryFn: ({ signal }) =>
                    getChunks(state.request.file_id, { signal }),
                },
              ]
            : [
                {
                  queryKey: storageKeys.list(params),
                  queryFn: ({ signal }) => listFiles(params, { signal }),
                },
              ]),
        ];
        await Promise.all(
          reads.map((read) =>
            queryClient.fetchQuery({ ...read, staleTime: 0, retry: false }),
          ),
        );
        mustReconcile.current = false;
        setState((previous) => ({
          ...previous,
          needsReconcile: false,
          reconciling: false,
          reconciled: true,
        }));
        return;
      }
      if (state.kind === "delete") {
        let file;
        try {
          file = await queryClient.fetchQuery({
            queryKey: storageKeys.detail(state.fileId),
            queryFn: ({ signal }) => getFile(state.fileId, { signal }),
            staleTime: 0,
            retry: false,
          });
        } catch (error) {
          if (error.status !== 404) throw error;
        }
        mustReconcile.current = false;
        setState((previous) => ({
          ...previous,
          phase: !file
            ? "gone"
            : file.status === "DELETING"
              ? "cleanup"
              : "error",
          error: file && file.status !== "DELETING" ? previous.error : null,
          needsReconcile: false,
          reconciling: false,
          reconciled: true,
          observedStatus: file?.status,
        }));
        invalidate();
        navigate(`/files/${state.fileId}`);
        return;
      }
      const params = { limit: 50, offset: 0, include_inactive: true };
      await queryClient.fetchQuery({
        queryKey: storageKeys.list(params),
        queryFn: ({ signal }) => listFiles(params, { signal }),
        staleTime: 0,
        retry: false,
      });
      invalidate();
      mustReconcile.current = false;
      setState((previous) => ({
        ...previous,
        needsReconcile: false,
        reconciled: true,
        reconciling: false,
      }));
      navigate("/");
    } catch (error) {
      setState((previous) => ({
        ...previous,
        reconciling: false,
        reconcileError: error,
      }));
    } finally {
      busy.current = false;
    }
  }

  async function runRepair(
    filters,
    { canMutate, continuation = false, scope = repairScope(filters) },
  ) {
    if (busy.current || mustReconcile.current || !canMutate) return;
    if (
      !Number.isInteger(filters.max_chunks) ||
      filters.max_chunks < 1 ||
      filters.max_chunks > 8
    )
      return;
    if (filters.file_id !== null && !isFileId(filters.file_id)) return;
    if (
      continuation &&
      (state.kind !== "repair" ||
        state.scope !== scope ||
        repairScope(state.request) !== repairScope(filters) ||
        state.phase !== "scanned" ||
        state.result.remaining_chunks === 0)
    )
      return;
    const request = {
      ...filters,
      after: continuation ? state.result.next_after : null,
    };
    const totals = continuation
      ? state.totals
      : { checked: 0, repaired: 0, pages: 0 };
    const pages = continuation ? state.pages : [];
    busy.current = true;
    setState({
      ...initial,
      kind: "repair",
      phase: "repairing",
      scope,
      request,
      totals,
      pages,
    });
    try {
      const result = await repair(request);
      if (!validRepairResult(result, request))
        throw new StorageApiError(
          "Phản hồi repair không đúng scope hoặc không đầy đủ. Hãy đọc lại dữ liệu.",
          { code: "UNEXPECTED_REPAIR_RESPONSE", outcomeUnknown: true },
        );
      setState({
        ...initial,
        kind: "repair",
        phase: "scanned",
        scope,
        request,
        result,
        pages: [...pages, { request, result }],
        totals: {
          checked: totals.checked + result.checked_chunks,
          repaired: totals.repaired + result.repaired_replicas,
          pages: totals.pages + 1,
        },
      });
    } catch (cause) {
      const error = await toStorageApiError(cause);
      const needsReconcile = error.outcomeUnknown || error.status >= 500;
      mustReconcile.current = needsReconcile;
      setState({
        ...initial,
        kind: "repair",
        phase: "error",
        scope,
        request,
        totals,
        pages,
        error,
        needsReconcile,
      });
    } finally {
      invalidate();
      busy.current = false;
    }
  }

  async function remove(file, { canMutate }) {
    if (
      busy.current ||
      mustReconcile.current ||
      !canMutate ||
      !canDeleteFile(file)
    )
      return;
    busy.current = true;
    setState({
      ...initial,
      kind: "delete",
      phase: "deleting",
      fileId: file.file_id,
      name: file.original_name,
    });
    try {
      const result = await deleteFile(file.file_id);
      const phase = deleteResultPhase(result, file.file_id);
      if (!phase)
        throw new StorageApiError(
          "Phản hồi xóa chưa xác nhận trạng thái. Hãy đọc lại file trước khi thử lại.",
          {
            code: "UNEXPECTED_DELETE_RESPONSE",
            status: result.httpStatus,
            outcomeUnknown: true,
          },
        );
      if (phase === "cleanup")
        queryClient.setQueryData(
          storageKeys.detail(file.file_id),
          (previous) =>
            previous
              ? {
                  ...previous,
                  status: "DELETING",
                  known_readable: false,
                  cleanup_pending_replicas: result.cleanup_pending_replicas,
                }
              : undefined,
        );
      setState({
        ...initial,
        kind: "delete",
        phase,
        name: file.original_name,
        fileId: file.file_id,
        deleteResult: result,
      });
      invalidate();
    } catch (cause) {
      const error = await toStorageApiError(cause);
      const needsReconcile = error.outcomeUnknown || error.status >= 500;
      mustReconcile.current = needsReconcile;
      setState({
        ...initial,
        kind: "delete",
        phase: "error",
        name: file.original_name,
        fileId: file.file_id,
        error,
        needsReconcile,
      });
      invalidate();
    } finally {
      busy.current = false;
    }
  }

  async function download(file, { canMutate }) {
    if (
      busy.current ||
      mustReconcile.current ||
      !canMutate ||
      file?.status !== "AVAILABLE"
    )
      return;
    busy.current = true;
    setState({
      ...initial,
      kind: "download",
      phase: "preparing",
      name: file.original_name,
      fileId: file.file_id,
    });
    try {
      const response = await downloadFile(file.file_id, {
        onDownloadProgress: (event) => {
          if (event.loaded > 0)
            setState((previous) =>
              ["preparing", "receiving"].includes(previous.phase)
                ? {
                    ...previous,
                    phase: "receiving",
                    progress: transferProgress(event),
                  }
                : previous,
            );
        },
      });
      const type =
        readHeader(response.headers, "content-type") ||
        response.data?.type ||
        "";
      if (
        response.status !== 200 ||
        !(response.data instanceof Blob) ||
        response.data.size !== file.size_bytes ||
        /(?:application\/(?:[\w.-]+\+)?json|text\/html)/i.test(type)
      ) {
        throw new StorageApiError(
          "Phản hồi download không phải file đầy đủ. Chưa lưu file.",
          {
            code: "INVALID_DOWNLOAD_RESPONSE",
            status: response.status,
          },
        );
      }
      const name = downloadFilename(
        readHeader(response.headers, "content-disposition"),
        file.original_name,
      );
      saveBlob(response.data, name);
      setState({
        ...initial,
        kind: "download",
        phase: "success",
        name,
        fileId: file.file_id,
      });
    } catch (cause) {
      const error = await toStorageApiError(cause);
      setState({
        ...initial,
        kind: "download",
        phase: "error",
        name: file.original_name,
        fileId: file.file_id,
        error,
      });
    } finally {
      busy.current = false;
    }
  }

  return {
    state,
    busy:
      [
        "sending",
        "replicating",
        "preparing",
        "receiving",
        "deleting",
        "repairing",
      ].includes(state.phase) || Boolean(state.reconciling),
    needsReconcile: Boolean(state.needsReconcile),
    upload,
    download,
    reconcile,
    remove,
    runRepair,
    deleteSnapshot,
    watchingDelete,
  };
}
