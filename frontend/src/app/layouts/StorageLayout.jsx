import { NavLink, Outlet, useLocation } from "react-router";
import {
  CircleStackIcon,
  FolderIcon,
} from "@heroicons/react/24/outline";
import StorageTransferProvider from "app/pages/storage/components/StorageTransferProvider";
import TransferNotice from "app/pages/storage/components/TransferNotice";

const navClass = ({ isActive }) =>
  [
    "inline-flex items-center gap-1.5 rounded-full px-3.5 py-1.5 text-sm font-medium transition-colors",
    isActive
      ? "bg-gray-900 text-white dark:bg-dark-50 dark:text-dark-900"
      : "text-gray-600 hover:bg-gray-100 dark:text-dark-200 dark:hover:bg-dark-800",
  ].join(" ");

export default function StorageLayout() {
  const { pathname } = useLocation();
  const filesActive = pathname === "/" || pathname.startsWith("/files/");

  return (
    <div className="min-h-screen min-w-0 bg-[#f0f4f9] text-gray-800 dark:bg-dark-950 dark:text-dark-100">
      <header className="sticky top-0 z-10 border-b border-transparent bg-[#f0f4f9]/95 backdrop-blur dark:bg-dark-950/95">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-4 px-4 py-3 sm:px-6">
          <NavLink
            to="/"
            end
            className="text-[15px] font-semibold tracking-tight text-gray-900 dark:text-dark-50"
          >
            Distributed File Storage
          </NavLink>
          <nav
            aria-label="Điều hướng lưu trữ"
            className="flex flex-wrap items-center gap-1 rounded-full bg-white/80 p-1 shadow-sm dark:bg-dark-900"
          >
            <NavLink
              to="/"
              end
              className={() => navClass({ isActive: filesActive })}
              aria-current={filesActive ? "page" : undefined}
            >
              <FolderIcon className="size-4" />
              Files
            </NavLink>
            <NavLink to="/cluster" className={navClass}>
              <CircleStackIcon className="size-4" />
              Cụm
            </NavLink>
          </nav>
        </div>
      </header>
      <main
        id="storage-content"
        className="mx-auto max-w-6xl px-4 py-6 sm:px-6"
      >
        <StorageTransferProvider>
          <TransferNotice />
          <Outlet />
        </StorageTransferProvider>
      </main>
    </div>
  );
}
