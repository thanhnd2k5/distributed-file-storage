import { getChunks, getFile } from "api/storage";
import { storageKeys } from "api/storage/queryKeys";
import { isFileId } from "../utils/snapshots";
import { useSnapshotQuery } from "./useSnapshotQuery";
import { useStorageConnection } from "./useStorageConnection";
import { useTransfer } from "./useTransfer";

export function useFileDetailPage(fileId) {
  const transfer = useTransfer();
  const watched = transfer.watchingDelete && transfer.state.fileId === fileId;
  const valid = isFileId(fileId);
  const file = useSnapshotQuery({
    queryKey: storageKeys.detail(fileId),
    queryFn: ({ signal }) => getFile(fileId, { signal }),
    enabled: valid,
    poll: !watched,
  });
  const chunks = useSnapshotQuery({
    queryKey: storageKeys.chunks(fileId),
    queryFn: ({ signal }) => getChunks(fileId, { signal }),
    enabled: valid && file.error?.status !== 404,
  });
  const connection = useStorageConnection();
  return {
    valid,
    file,
    chunks,
    connection,
    notFound: file.error?.status === 404 || chunks.error?.status === 404,
    deleted:
      transfer.state.kind === "delete" &&
      transfer.state.fileId === fileId &&
      transfer.state.phase === "deleted",
    refresh: () =>
      Promise.all([
        ...(valid ? [file.refetch(), chunks.refetch()] : []),
        connection.refresh(),
      ]),
    fetching:
      file.isFetching ||
      chunks.isFetching ||
      connection.ready.isFetching ||
      connection.cluster.isFetching,
  };
}
