import { Link, Outlet } from "react-router";
import StorageTransferProvider from "app/pages/storage/components/StorageTransferProvider";

export default function StorageLayout() {
  return (
    <div className="dark:bg-dark-900 dark:text-dark-100 min-h-screen min-w-0 bg-gray-50 text-gray-800">
      <header className="dark:border-dark-600 dark:bg-dark-800 border-b border-gray-200 bg-white">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-4 px-4 py-5 sm:px-6">
          <Link to="/" className="text-lg font-semibold">
            Distributed File Storage
          </Link>
          <span className="dark:text-dark-200 text-sm text-gray-500">
            Cụm lưu trữ
          </span>
        </div>
      </header>
      <main
        id="storage-content"
        className="mx-auto max-w-6xl px-4 py-8 sm:px-6"
      >
        <StorageTransferProvider>
          <Outlet />
        </StorageTransferProvider>
      </main>
    </div>
  );
}
