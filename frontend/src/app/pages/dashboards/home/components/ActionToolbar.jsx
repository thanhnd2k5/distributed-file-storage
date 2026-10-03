import { FiSearch, FiDownload, FiPlus, FiCheckSquare } from "react-icons/fi";
import { Button, Input, Select, Card } from "components/ui";

const ActionToolbar = () => {
    return (
        <Card skin="shadow" className="flex flex-col xl:flex-row gap-4 justify-between items-start xl:items-center p-4 rounded-xl shadow-sm bg-white dark:bg-gray-800 border border-gray-100 dark:border-gray-700 mt-2">

            {/* Search and Filters */}
            <div className="flex flex-wrap items-center gap-3 w-full xl:w-auto">
                <div className="relative flex-grow sm:flex-grow-0 sm:min-w-[260px]">
                    <Input
                        prefix={<FiSearch className="h-4 w-4" />}
                        placeholder="Tìm link hoặc tên..."
                        classNames={{ input: "rounded-xl" }}
                    />
                </div>

                <div className="flex items-center gap-2">
                    <Select
                        data={["Tất cả trạng thái", "OK", "Lỗi", "Cảnh báo"]}
                        classNames={{ select: "rounded-xl min-w-[150px]" }}
                    />

                    <Select
                        data={["Tất cả nhóm", "Shopee", "Lazada", "Tiktok"]}
                        classNames={{ select: "rounded-xl min-w-[150px]" }}
                    />
                </div>
            </div>

            {/* Action Buttons */}
            <div className="flex flex-wrap items-center gap-3 w-full xl:w-auto justify-end">
                <Button variant="outlined" color="neutral" className="gap-2 rounded-xl">
                    <FiDownload className="text-emerald-500" />
                    Import
                </Button>
                <Button variant="outlined" color="neutral" className="gap-2 rounded-xl">
                    <FiCheckSquare className="text-gray-500" />
                    Excel
                </Button>
                <Button variant="filled" color="primary" className="gap-2 rounded-xl shadow-md hover:-translate-y-0.5 transition-transform" isGlow>
                    <FiPlus />
                    Thêm Link
                </Button>
                <Button variant="filled" color="info" className="gap-2 rounded-xl shadow-md hover:-translate-y-0.5 transition-transform" isGlow>
                    <FiCheckSquare />
                    Check All
                </Button>
            </div>

        </Card>
    );
};

export default ActionToolbar;
