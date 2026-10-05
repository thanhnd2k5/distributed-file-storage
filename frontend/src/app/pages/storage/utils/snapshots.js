export const SNAPSHOT_INTERVAL_MS = 4_000;

export function isFileId(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
    value ?? "",
  );
}

export function snapshotRetry(failureCount, error) {
  return ![400, 404, 422].includes(error.status) && failureCount < 1;
}

export function lastPageOffset(total, limit) {
  return Math.max(0, Math.floor((total - 1) / limit) * limit);
}

export function formatBytes(value) {
  if (value === null || value === undefined) return "Chưa có";
  if (value < 1024) return `${value} byte`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let size = value / 1024;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index++;
  }
  return `${new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 2 }).format(size)} ${units[index]}`;
}

export function formatTimestamp(value) {
  if (!value) return "Chưa có";
  return new Intl.DateTimeFormat("vi-VN", {
    dateStyle: "short",
    timeStyle: "medium",
  }).format(new Date(value));
}
