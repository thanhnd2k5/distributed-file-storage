import { Outlet } from "react-router";
import { useSidebarContext } from "app/contexts/sidebar/context";
import clsx from "clsx";

// Local Imports
import { Header } from "./Header";
import { Sidebar } from "./Sidebar";

// ----------------------------------------------------------------------

export default function Sideblock() {
  const { isExpanded, width, isResizing } = useSidebarContext();

  return (
    <div
      data-layout="sideblock"
      style={isExpanded ? { "--sidebar-panel-width": `${width}px` } : undefined}
      className={clsx(
        "min-h-screen bg-slate-50 dark:bg-dark-900 transition-all duration-300",
        !isExpanded && "is-sidebar-collapsed",
        isResizing && "is-resizing"
      )}
    >
      <Header />
      <main className="main-content transition-content grid grid-cols-1">
        <Outlet />
      </main>
      <Sidebar />
    </div>
  );
}
