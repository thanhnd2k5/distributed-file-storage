import { useSyncExternalStore } from "react";
import { useQuery } from "@tanstack/react-query";
import { SNAPSHOT_INTERVAL_MS, snapshotRetry } from "../utils/snapshots";

const subscribe = (callback) => {
  document.addEventListener("visibilitychange", callback);
  return () => document.removeEventListener("visibilitychange", callback);
};
const getVisible = () => document.visibilityState === "visible";

export function useSnapshotQuery({ enabled = true, poll = true, ...options }) {
  const visible = useSyncExternalStore(subscribe, getVisible);
  const canRead = (query) =>
    enabled && visible && query.state.error?.status !== 404;
  return useQuery({
    ...options,
    enabled: canRead,
    retry: snapshotRetry,
    refetchInterval: (query) =>
      poll && canRead(query) ? SNAPSHOT_INTERVAL_MS : false,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: canRead,
    refetchOnReconnect: canRead,
  });
}
