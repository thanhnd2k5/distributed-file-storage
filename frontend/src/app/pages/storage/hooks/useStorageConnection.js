import { getCluster, getNodes, getReadiness } from "api/storage";
import { storageKeys } from "api/storage/queryKeys";
import { useSnapshotQuery } from "./useSnapshotQuery";

export function useStorageConnection({ includeNodes = true } = {}) {
  const ready = useSnapshotQuery({
    queryKey: storageKeys.ready,
    queryFn: getReadiness,
  });
  const cluster = useSnapshotQuery({
    queryKey: storageKeys.cluster,
    queryFn: getCluster,
  });
  const nodes = useSnapshotQuery({
    queryKey: storageKeys.nodes,
    queryFn: getNodes,
    enabled: includeNodes,
  });
  return {
    ready,
    cluster,
    nodes,
    canMutate:
      ready.isSuccess &&
      ready.data?.status === "READY" &&
      cluster.isSuccess &&
      !cluster.data.operation_busy,
    refresh: () =>
      Promise.all([
        ready.refetch(),
        cluster.refetch(),
        ...(includeNodes ? [nodes.refetch()] : []),
      ]),
  };
}
