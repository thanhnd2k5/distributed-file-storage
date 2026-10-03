import PropTypes from "prop-types";
import { useDropZone } from "hooks";

export function DropZone({ onFilePick, accept, icon, label, hint, className }) {
  const { isDragging, dropZoneProps, inputProps } = useDropZone(onFilePick);

  return (
    <div
      {...dropZoneProps}
      className={`
        border-2 border-dashed rounded-xl p-6 text-center cursor-pointer transition-all duration-200
        ${isDragging
          ? "border-blue-500 bg-blue-50 dark:bg-blue-900/20 scale-[1.01]"
          : "border-gray-300 dark:border-gray-600 hover:border-blue-400 hover:bg-gray-50 dark:hover:bg-gray-700/30"
        }
        ${className}
      `}
    >
      <input {...inputProps} accept={accept} />
      {icon && <div className="flex justify-center mb-3">{icon}</div>}
      <p className="text-sm font-bold text-gray-700 dark:text-gray-200">
        {label}
      </p>
      {hint && <p className="text-xs text-gray-400 mt-2 font-medium">{hint}</p>}
    </div>
  );
}

DropZone.propTypes = {
  onFilePick: PropTypes.func.isRequired,
  accept: PropTypes.string,
  icon: PropTypes.node,
  label: PropTypes.string.isRequired,
  hint: PropTypes.string,
  className: PropTypes.string,
};
