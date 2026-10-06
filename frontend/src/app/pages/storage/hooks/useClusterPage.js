import { useStorageConnection } from "./useStorageConnection";

export function useClusterPage() {
  const connection = useStorageConnection();
  return {
    connection,
    refresh: () => connection.refresh(),
    fetching:
      connection.ready.isFetching ||
      connection.cluster.isFetching ||
      connection.nodes.isFetching,
  };
}
