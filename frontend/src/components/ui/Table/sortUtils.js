/**
 * Toggle sort field/order for server-driven table headers.
 *
 * @param {string} currentField
 * @param {"asc"|"desc"} currentOrder
 * @param {string} nextField
 * @param {{ defaultOrder?: "asc"|"desc" }} [options]
 * @returns {{ field: string, order: "asc"|"desc" }}
 *
 * @example
 * const next = toggleSortState(sortField, sortOrder, "granted_until");
 * setSortField(next.field);
 * setSortOrder(next.order);
 */
export function toggleSortState(
  currentField,
  currentOrder,
  nextField,
  { defaultOrder = "asc" } = {},
) {
  if (currentField === nextField) {
    return {
      field: nextField,
      order: currentOrder === "asc" ? "desc" : "asc",
    };
  }

  return {
    field: nextField,
    order: defaultOrder === "desc" ? "desc" : "asc",
  };
}
