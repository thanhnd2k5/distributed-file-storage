// Import Dependencies
import { Portal } from "@headlessui/react";
import { clsx } from "clsx";

// Local Imports
import { useBreakpointsContext } from "app/contexts/breakpoint/context";
import { useSidebarContext } from "app/contexts/sidebar/context";
import { useThemeContext } from "app/contexts/theme/context";

import { Header } from "./Header";
import { Menu } from "./Menu";
import { Profile } from "./Profile";

// ----------------------------------------------------------------------

export function Sidebar() {
  const { cardSkin } = useThemeContext();
  const { lgAndDown } = useBreakpointsContext();

  const {
    isExpanded: isSidebarExpanded,
    close: closeSidebar,
    startResizing,
    resetWidth,
  } = useSidebarContext();

  return (
    <div
      className={clsx(
        "sidebar-panel",
        cardSkin === "shadow"
          ? "shadow-soft dark:shadow-dark-900/60"
          : "dark:border-dark-600/80 ltr:border-r rtl:border-l border-gray-200",
      )}
    >
      <div
        className={clsx(
          "flex h-full grow flex-col bg-white",
          cardSkin === "shadow" ? "dark:bg-dark-750" : "dark:bg-dark-900",
        )}
      >
        <Header />
        <Menu />

        {/* Bottom Profile Section */}
        <div className="flex shrink-0 flex-col items-center py-4 border-t border-gray-150 dark:border-dark-600 transition-all duration-300">
          <Profile isExpanded={isSidebarExpanded} />
        </div>
      </div>

      {!lgAndDown && (
        <div
          onMouseDown={startResizing}
          onDoubleClick={resetWidth}
          className="absolute top-0 right-0 z-50 h-full w-1 cursor-col-resize hover:bg-primary-500/50 active:bg-primary-600 transition-colors"
        />
      )}

      {lgAndDown && isSidebarExpanded && (
        <Portal>
          <div
            onClick={closeSidebar}
            className="fixed inset-0 z-20 bg-gray-900/50 backdrop-blur-sm transition-opacity dark:bg-black/40"
          />
        </Portal>
      )}
    </div>
  );
}
