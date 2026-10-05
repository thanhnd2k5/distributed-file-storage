export const canDeleteFile = (file) =>
  ["AVAILABLE", "FAILED"].includes(file?.status);

export function deleteResultPhase(result, fileId) {
  if (
    result.file_id !== fileId ||
    !Number.isSafeInteger(result.cleanup_pending_replicas) ||
    result.cleanup_pending_replicas < 0
  )
    return null;
  if (
    result.httpStatus === 200 &&
    result.status === "DELETED" &&
    result.cleanup_pending_replicas === 0
  )
    return "deleted";
  if (result.httpStatus === 202 && result.status === "DELETING")
    return "cleanup";
  return null;
}
