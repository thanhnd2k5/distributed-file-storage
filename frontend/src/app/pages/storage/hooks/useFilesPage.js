import { useEffect, useState } from "react";
import { listFiles } from "api/storage";
import { storageKeys } from "api/storage/queryKeys";
import { lastPageOffset } from "../utils/snapshots";
import { useSnapshotQuery } from "./useSnapshotQuery";
import { useStorageConnection } from "./useStorageConnection";
import { useTransfer } from "./useTransfer";

export function useFilesPage() {
  const { state: transferState } = useTransfer();
  const [params, setParams] = useState({
    limit: 50,
    offset: 0,
    include_inactive: false,
  });
  const files = useSnapshotQuery({
    queryKey: storageKeys.list(params),
    queryFn: ({ signal }) => listFiles(params, { signal }),
  });
  const connection = useStorageConnection({ includeNodes: false });
  useEffect(() => {
    if (transferState.reconciled && transferState.kind === "upload")
      setParams((previous) => ({
        ...previous,
        include_inactive: true,
        offset: 0,
      }));
  }, [transferState.reconciled, transferState.kind]);
  useEffect(() => {
    if (
      files.isError ||
      !files.data ||
      files.data.offset !== params.offset ||
      files.data.limit !== params.limit
    )
      return;
    const offset = lastPageOffset(files.data.total, params.limit);
    if (params.offset > offset)
      setParams((previous) =>
        previous === params ? { ...previous, offset } : previous,
      );
  }, [files.data, files.isError, params]);
  return {
    files,
    connection,
    params,
    setFilter: (changes) =>
      setParams((previous) => ({ ...previous, ...changes, offset: 0 })),
    setOffset: (offset) => setParams((previous) => ({ ...previous, offset })),
    refresh: () => Promise.all([files.refetch(), connection.refresh()]),
    fetching:
      files.isFetching ||
      connection.ready.isFetching ||
      connection.cluster.isFetching,
  };
}
