import { isFileId } from "./snapshots";

export const repairScope = ({ file_id, node_id, max_chunks }) =>
  JSON.stringify([file_id, node_id, max_chunks]);

const count = (value) => Number.isInteger(value) && value >= 0;
const outcomes = [
  "HEALTHY",
  "REPAIRED",
  "NO_DESTINATION",
  "UNAVAILABLE",
  "ERROR",
];

export function validRepairResult(result, request) {
  return Boolean(
    result &&
      count(result.checked_chunks) &&
      result.checked_chunks <= request.max_chunks &&
      count(result.repaired_replicas) &&
      count(result.remaining_chunks) &&
      (result.next_after === null ||
        (isFileId(result.next_after?.file_id) &&
          count(result.next_after.chunk_index) &&
          (!request.file_id ||
            result.next_after.file_id === request.file_id))) &&
      Array.isArray(result.results) &&
      result.results.length === result.checked_chunks &&
      result.results.every(
        (item) =>
          isFileId(item.file_id) &&
          isFileId(item.chunk_id) &&
          (!request.file_id || item.file_id === request.file_id) &&
          count(item.chunk_index) &&
          count(item.live_replica_count) &&
          typeof item.domain_degraded === "boolean" &&
          outcomes.includes(item.outcome) &&
          (item.message === null || typeof item.message === "string"),
      ),
  );
}
