import { 
  Pagination, 
  PaginationPrevious, 
  PaginationItems, 
  PaginationNext,
  Select 
} from 'components/ui';
import clsx from 'clsx';

/**
 * Reusable Table Pagination Component
 * Combined Page Numbers + Per Page Selector
 */
const TablePagination = ({ 
  pagination = {}, 
  onPageChange, 
  onLimitChange, 
  className 
}) => {
  const { page = 1, total = 0, pages = 1, limit = 10 } = pagination;

  // Don't show anything if no items
  if (total === 0) return null;

  return (
    <div className={clsx(
      "bg-gray-50/50 dark:bg-gray-900/50 px-6 py-4 border-t border-gray-100 dark:border-gray-800 flex flex-col sm:flex-row items-center justify-between gap-4 mt-auto transition-all duration-200",
      className
    )}>
      {/* Left: Total and Limit Selector */}
      <div className="flex items-center gap-4 order-2 sm:order-1">
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap hidden sm:inline">
            Hiển thị:
          </span>
          <div className="w-20">
            <Select
              data={[
                { label: '10', value: 10 },
                { label: '20', value: 20 },
                { label: '50', value: 50 },
                { label: '100', value: 100 },
              ]}
              value={limit}
              onChange={(e) => onLimitChange?.(Number(e.target.value))}
              placement="top"
              className="!h-9 !py-1 text-xs"
              classNames={{
                select: "!py-1 !min-h-[36px]",
                options: "!max-h-48 !min-w-[100px]",
                option: "!pl-8 !py-2"
              }}
            />
          </div>
        </div>
        <span className="text-sm text-gray-500 dark:text-gray-400 hidden lg:inline border-l border-gray-200 dark:border-gray-700 pl-4 h-5 flex items-center">
          Tổng số: <span className="font-bold text-gray-900 dark:text-gray-100 ml-1">{total}</span>
        </span>
      </div>

      {/* Right: Page Selector */}
      <div className="flex items-center gap-4 order-1 sm:order-2">
        {pages > 1 ? (
          <Pagination 
            total={pages} 
            value={page} 
            onChange={onPageChange}
            className="flex items-center gap-1"
          >
            <PaginationPrevious />
            <PaginationItems />
            <PaginationNext />
          </Pagination>
        ) : (
          <span className="text-xs text-gray-400 dark:text-gray-500 font-medium">
            Trang 1 / 1
          </span>
        )}
      </div>
    </div>
  );
};

export default TablePagination;
