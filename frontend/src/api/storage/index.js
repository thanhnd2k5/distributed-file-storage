import { storageAxios, STORAGE_TIMEOUTS } from "./client";

const filePath = (fileId) => `files/${encodeURIComponent(fileId)}`;

export const getReadiness = ({ signal } = {}) =>
  storageAxios.get("health/ready", { signal }).then(({ data }) => data);
export const getCluster = ({ signal } = {}) =>
  storageAxios.get("cluster", { signal }).then(({ data }) => data);
export const getNodes = ({ signal } = {}) =>
  storageAxios.get("nodes", { signal }).then(({ data }) => data);
export const listFiles = (
  { limit = 50, offset = 0, include_inactive = false } = {},
  { signal } = {},
) =>
  storageAxios
    .get("files", { params: { limit, offset, include_inactive }, signal })
    .then(({ data }) => data);
export const getFile = (fileId, { signal } = {}) =>
  storageAxios.get(filePath(fileId), { signal }).then(({ data }) => data);
export const getChunks = (fileId, { signal } = {}) =>
  storageAxios
    .get(`${filePath(fileId)}/chunks`, { signal })
    .then(({ data }) => data);

export function uploadFile(file, { signal, onUploadProgress } = {}) {
  const body = new FormData();
  body.append("file", file);
  return storageAxios
    .post("files", body, {
      signal,
      onUploadProgress,
      timeout: STORAGE_TIMEOUTS.transfer,
    })
    .then(({ data, status }) => ({ ...data, httpStatus: status }));
}
export const downloadFile = (fileId, { signal, onDownloadProgress } = {}) =>
  storageAxios.get(`${filePath(fileId)}/download`, {
    signal,
    onDownloadProgress,
    responseType: "blob",
    timeout: STORAGE_TIMEOUTS.transfer,
  });
export const deleteFile = (fileId, { signal } = {}) =>
  storageAxios
    .delete(filePath(fileId), { signal, timeout: STORAGE_TIMEOUTS.operation })
    .then(({ data, status }) => ({ ...data, httpStatus: status }));
export const repair = (
  { file_id = null, node_id = null, max_chunks = 8, after = null } = {},
  { signal } = {},
) =>
  storageAxios
    .post(
      "admin/repair",
      { file_id, node_id, max_chunks, after },
      { signal, timeout: STORAGE_TIMEOUTS.operation },
    )
    .then(({ data }) => data);
