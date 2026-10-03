import { FiEye, FiRefreshCw, FiEdit2, FiTrash2, FiExternalLink } from "react-icons/fi";
import { Badge, Card } from "components/ui";
import { MOCK_LINKS } from "../data/mockData";

const StatusBadge = ({ status }) => {
    const map = {
        OK: "success",
        ERROR: "error",
        WARNING: "warning"
    };

    return (
        <Badge color={map[status] || "neutral"} variant="soft" className="px-2.5 py-1 text-xs leading-5 font-semibold rounded-full border border-transparent">
            {status}
        </Badge>
    );
};

const LinkDataTable = () => {
    return (
        <Card skin="shadow" className="!p-0 rounded-2xl border border-gray-100 dark:border-gray-700 overflow-hidden flex flex-col bg-white dark:bg-gray-800">
            <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                    <thead className="bg-gray-50/50 dark:bg-gray-900/50 backdrop-blur-sm">
                        <tr>
                            <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                                <input type="checkbox" className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500" />
                            </th>
                            <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                                Tên / Label
                            </th>
                            <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                                URL
                            </th>
                            <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                                Nhóm
                            </th>
                            <th scope="col" className="px-6 py-4 text-center text-xs font-semibold text-gray-500 uppercase tracking-wider">
                                Trạng Thái
                            </th>
                            <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                                Lỗi
                            </th>
                            <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap">
                                Lần Check Cuối
                            </th>
                            <th scope="col" className="px-6 py-4 text-center text-xs font-semibold text-gray-500 uppercase tracking-wider">
                                Hành Động
                            </th>
                        </tr>
                    </thead>
                    <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-100 dark:divide-gray-800">
                        {MOCK_LINKS.map((link) => (
                            <tr key={link.id} className="hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors group">
                                <td className="px-6 py-4 whitespace-nowrap">
                                    <input type="checkbox" className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500" />
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap">
                                    <div className="text-sm font-medium text-gray-900 dark:text-gray-100">{link.name}</div>
                                </td>
                                <td className="px-6 py-4">
                                    <div className="flex items-center gap-2 group/link">
                                        <span className="text-sm text-indigo-600 dark:text-indigo-400 truncate max-w-[200px] lg:max-w-xs">{link.url}</span>
                                        <a href="#" className="opacity-0 group-hover/link:opacity-100 text-gray-400 hover:text-indigo-500 transition-all">
                                            <FiExternalLink className="w-4 h-4" />
                                        </a>
                                    </div>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                                    <span className="text-violet-600 dark:text-violet-400 font-medium">{link.group}</span>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-center">
                                    <StatusBadge status={link.status} />
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                                    {link.error ? <span className="text-rose-500">{link.error}</span> : "-"}
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                                    {link.lastChecked}
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-center text-sm font-medium">
                                    <div className="flex items-center justify-center gap-2 opacity-60 group-hover:opacity-100 transition-opacity">
                                        <button className="p-1.5 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 dark:hover:bg-indigo-900/30 rounded-lg transition-colors" title="View">
                                            <FiEye className="w-4 h-4" />
                                        </button>
                                        <button className="p-1.5 text-gray-400 hover:text-blue-600 hover:bg-blue-50 dark:hover:bg-blue-900/30 rounded-lg transition-colors" title="Refresh">
                                            <FiRefreshCw className="w-4 h-4" />
                                        </button>
                                        <button className="p-1.5 text-gray-400 hover:text-amber-600 hover:bg-amber-50 dark:hover:bg-amber-900/30 rounded-lg transition-colors" title="Edit">
                                            <FiEdit2 className="w-4 h-4" />
                                        </button>
                                        <button className="p-1.5 text-gray-400 hover:text-rose-600 hover:bg-rose-50 dark:hover:bg-rose-900/30 rounded-lg transition-colors" title="Delete">
                                            <FiTrash2 className="w-4 h-4" />
                                        </button>
                                    </div>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>

            {/* Pagination Footer */}
            <div className="bg-gray-50/50 dark:bg-gray-900/50 px-6 py-4 border-t border-gray-100 dark:border-gray-800 flex items-center justify-between sm:justify-end gap-4 rounded-b-2xl mt-auto">
                <select className="border-gray-200 dark:border-gray-700 rounded-lg text-sm bg-white dark:bg-gray-800 text-gray-700">
                    <option>Hiển thị: 20 link</option>
                    <option>Hiển thị: 50 link</option>
                    <option>Hiển thị: 100 link</option>
                </select>

                <nav className="relative z-0 inline-flex rounded-md shadow-sm -space-x-px" aria-label="Pagination">
                    <button className="relative inline-flex items-center px-3 py-2 rounded-l-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-medium text-gray-500 hover:bg-gray-50">
                        Trước
                    </button>
                    <span className="relative inline-flex items-center px-4 py-2 border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 text-sm font-medium text-gray-700 dark:text-gray-300">
                        1 / 1
                    </span>
                    <button className="relative inline-flex items-center px-3 py-2 rounded-r-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-medium text-gray-500 hover:bg-gray-50">
                        Sau
                    </button>
                </nav>
            </div>
        </Card>
    );
};

export default LinkDataTable;
