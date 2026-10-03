import { FiSettings } from "react-icons/fi";
import { Button } from "components/ui";

const HeaderSection = ({ onOpenSettings }) => {
  return (
    <div className="mb-2 flex flex-col justify-between gap-4 sm:flex-row sm:items-center">
      <div>
        <h1 className="bg-gradient-to-r from-gray-900 to-gray-600 bg-clip-text text-2xl font-bold text-transparent dark:from-white dark:to-gray-300">
          Tổng quan
        </h1>
        <p className="text-sm font-medium text-gray-500 dark:text-gray-400">
          Demo dashboard — thay bằng màn hình thật của dự án
        </p>
      </div>

      <Button
        isIcon
        variant="outlined"
        color="neutral"
        className="h-10 w-10 rounded-xl transition-transform hover:-translate-y-0.5"
        onClick={onOpenSettings}
      >
        <FiSettings className="h-5 w-5 text-gray-500 hover:text-primary-600" />
      </Button>
    </div>
  );
};

export default HeaderSection;
