/**
 * Clickable table header with sort icon (controlled).
 *
 * @example
 * <SortableTh
 *   sorted={sortField === "granted_until" ? sortOrder : false}
 *   onSort={() => onSortChange("granted_until")}
 * >
 *   Hết hạn
 * </SortableTh>
 */
import PropTypes from "prop-types";
import { forwardRef } from "react";
import { TableSortIcon } from "components/shared/table/TableSortIcon";
import { Th } from "./TableTags";

function ariaSortValue(sorted) {
  if (sorted === "asc") return "ascending";
  if (sorted === "desc") return "descending";
  return "none";
}

export const SortableTh = forwardRef(function SortableTh(
  { sorted = false, onSort, className, children, ...rest },
  ref,
) {
  return (
    <Th
      ref={ref}
      className={className}
      {...rest}
      aria-sort={ariaSortValue(sorted)}
    >
      <button
        type="button"
        className="inline-flex cursor-pointer items-center gap-1.5 font-inherit"
        onClick={onSort}
      >
        {children}
        <TableSortIcon sorted={sorted} />
      </button>
    </Th>
  );
});

SortableTh.displayName = "SortableTh";

SortableTh.propTypes = {
  sorted: PropTypes.oneOf(["asc", "desc", false]),
  onSort: PropTypes.func,
  className: PropTypes.string,
  children: PropTypes.node,
};
