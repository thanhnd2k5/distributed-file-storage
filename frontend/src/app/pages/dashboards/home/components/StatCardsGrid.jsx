import { FiFolder, FiCheckCircle, FiXCircle, FiMonitor } from "react-icons/fi";
import { Card } from "components/ui";
import { MOCK_STATS } from "../data/mockData";

const StatCard = ({ title, value, icon: Icon, colorClass, bgClass, isLast }) => (
    <Card
        skin="shadow"
        className="relative p-6 overflow-hidden rounded-2xl hover:shadow-lg transition-all duration-300 group hover:-translate-y-1 !border-gray-100 dark:!border-gray-700"
    >
        {/* Background Decorative Blob */}
        <div className={`absolute -right-6 -top-6 w-24 h-24 rounded-full opacity-10 transition-transform duration-500 group-hover:scale-150 ${bgClass}`}></div>

        <div className="flex items-start justify-between relative z-10">
            <div>
                <p className="text-sm font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-1">
                    {title}
                </p>
                <h3 className={`text-3xl font-bold ${isLast ? 'text-gray-800 dark:text-gray-100' : 'text-gray-900 dark:text-white'}`}>
                    {value}
                </h3>
            </div>
            <div className={`p-3 rounded-xl ${bgClass} transition-colors duration-300`}>
                <Icon className={`w-6 h-6 ${colorClass}`} />
            </div>
        </div>
    </Card>
);

const StatCardsGrid = () => {
    const cards = [
        {
            title: "Tổng Link",
            value: MOCK_STATS.totalLinks.toLocaleString(),
            icon: FiFolder,
            colorClass: "text-amber-500",
            bgClass: "bg-amber-50 dark:bg-amber-500/10",
        },
        {
            title: "Hoạt Động",
            value: MOCK_STATS.active.toLocaleString(),
            icon: FiCheckCircle,
            colorClass: "text-emerald-500",
            bgClass: "bg-emerald-50 dark:bg-emerald-500/10",
        },
        {
            title: "Lỗi",
            value: MOCK_STATS.errors,
            icon: FiXCircle,
            colorClass: "text-rose-500",
            bgClass: "bg-rose-50 dark:bg-rose-500/10",
        },
        {
            title: "Check Gần Nhất",
            value: MOCK_STATS.lastCheck,
            icon: FiMonitor,
            colorClass: "text-indigo-500",
            bgClass: "bg-indigo-50 dark:bg-indigo-500/10",
            isLast: true
        },
    ];

    return (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 xl:gap-6">
            {cards.map((card, idx) => (
                <StatCard key={idx} {...card} />
            ))}
        </div>
    );
};

export default StatCardsGrid;
