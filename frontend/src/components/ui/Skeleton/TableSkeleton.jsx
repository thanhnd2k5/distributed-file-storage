import { Skeleton } from './index';

/**
 * TableSkeleton component to render a placeholder for tables during loading.
 * @param {Object} props
 * @param {number} props.rowCount - Number of placeholder rows to render.
 * @param {Array} props.columns - Configuration for each column.
 *   - {string} type: 'checkbox' | 'text' | 'text-multi' | 'badge' | 'progress' | 'actions'
 *   - {string} align: 'left' | 'center' | 'right'
 *   - {string} width: CSS width class (e.g., 'w-32', 'w-full')
 *   - {number} count: For 'actions' type, number of action buttons.
 */
const TableSkeleton = ({ rowCount = 5, columns = [] }) => {
  const renderCellContent = (col) => {
    switch (col.type) {
      case 'checkbox':
        return <Skeleton className="h-4 w-4 rounded" />;
      
      case 'text-multi':
        return (
          <div className={`flex flex-col gap-1.5 ${col.align === 'center' ? 'items-center' : ''}`}>
            <Skeleton className={`h-4 ${col.width || 'w-32'} rounded`} />
            <Skeleton className={`h-3 ${col.width ? 'opacity-60 w-2/3' : 'w-20'} rounded`} />
          </div>
        );
      
      case 'badge':
        return (
          <div className={`flex ${col.align === 'center' ? 'justify-center' : ''}`}>
            <Skeleton className="h-6 w-24 rounded-full" />
          </div>
        );
      
      case 'progress':
        return (
          <div className={`flex flex-col gap-2 ${col.align === 'center' ? 'items-center' : ''}`}>
            <Skeleton className="h-2 w-32 rounded-full" />
            <Skeleton className="h-3 w-16 rounded opacity-70" />
          </div>
        );
      
      case 'actions':
        return (
          <div className={`flex gap-2 ${col.align === 'center' ? 'justify-center' : ''}`}>
            {Array.from({ length: col.count || 2 }).map((_, i) => (
              <Skeleton key={i} className="h-8 w-8 rounded-lg" />
            ))}
          </div>
        );
      
      case 'text':
      default:
        return <Skeleton className={`h-4 ${col.width || 'w-24'} rounded mx-auto`} />;
    }
  };

  return (
    <>
      {Array.from({ length: rowCount }).map((_, rowIndex) => (
        <tr key={rowIndex} className="border-b border-gray-100 dark:border-gray-800 last:border-0">
          {columns.map((col, colIndex) => (
            <td 
              key={colIndex} 
              className={`px-6 py-4 whitespace-nowrap ${col.align === 'center' ? 'text-center' : 'text-left'}`}
            >
              {renderCellContent(col)}
            </td>
          ))}
        </tr>
      ))}
    </>
  );
};

export { TableSkeleton };
export default TableSkeleton;
