import { Badge } from "components/ui/Badge";

const colors = {
  AVAILABLE: "success",
  ACTIVE: "success",
  VERIFIED: "success",
  UPLOADING: "info",
  PENDING: "info",
  DELETING: "warning",
  SUSPECTED: "warning",
  UNDER_REPLICATED: "warning",
  FAILED: "error",
  DOWN: "error",
  MISSING: "error",
  CORRUPTED: "error",
  UNAVAILABLE: "error",
  DELETED: "neutral",
  INACTIVE: "neutral",
  DISABLED: "neutral",
};
export default function StatusBadge({ status }) {
  return (
    <Badge color={colors[status] ?? "neutral"} variant="soft">
      {status}
    </Badge>
  );
}
