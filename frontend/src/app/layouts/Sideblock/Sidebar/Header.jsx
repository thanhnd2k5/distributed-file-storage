// Import Dependencies
import { Link } from "react-router";
import { clsx } from "clsx";
import {
  ChevronDoubleLeftIcon,
  ChevronDoubleRightIcon,
  XMarkIcon,
} from "@heroicons/react/24/outline";

// Local Imports
import Logo from "assets/appLogo.svg?react";
import LogoType from "assets/logotype.svg?react";
import { useSidebarContext } from "app/contexts/sidebar/context";
import { useBreakpointsContext } from "app/contexts/breakpoint/context";

// ----------------------------------------------------------------------

export function Header() {
  const { isExpanded, close, toggle } = useSidebarContext();
  const { lgAndDown } = useBreakpointsContext();

  return (
    <header
      className={clsx(
        "relative flex h-[70px] shrink-0 items-center border-b border-gray-150 dark:border-dark-600 transition-all duration-300",
        isExpanded ? "justify-between px-5" : "justify-center px-0",
      )}
    >
      {isExpanded ? (
        <>
          <Link
            to="/"
            className="flex items-center gap-3 overflow-hidden transition-all duration-300"
          >
            <Logo className="size-9 shrink-0 text-primary-600 dark:text-primary-400" />
            <LogoType className="h-4.5 w-auto text-gray-800 dark:text-dark-50 animate-in fade-in duration-300" />
          </Link>

          {/* Desktop Collapse Button inside Header */}
          {!lgAndDown && (
            <button
              type="button"
              onClick={toggle}
              aria-label="Thu gọn thanh bên"
              title="Thu gọn thanh bên"
              className="flex size-8 items-center justify-center rounded-lg text-gray-400 outline-hidden transition-all duration-200 hover:bg-gray-100 hover:text-gray-700 focus-visible:ring-2 focus-visible:ring-primary-500/50 dark:text-dark-300 dark:hover:bg-dark-700 dark:hover:text-dark-100 cursor-pointer"
            >
              <ChevronDoubleLeftIcon className="size-4.5 stroke-[2] rtl:rotate-180" />
            </button>
          )}

          {/* Mobile Drawer Close Button */}
          {lgAndDown && (
            <button
              type="button"
              onClick={close}
              aria-label="Đóng menu"
              className="flex size-8 items-center justify-center rounded-lg text-gray-500 outline-hidden hover:bg-gray-100 hover:text-gray-700 focus-visible:ring-2 focus-visible:ring-primary-500/50 dark:text-dark-300 dark:hover:bg-dark-700 dark:hover:text-dark-100 xl:hidden cursor-pointer"
            >
              <XMarkIcon className="size-5" />
            </button>
          )}
        </>
      ) : (
        /* Collapsed View (72px): Centered logo that transforms to expand button on hover */
        <button
          type="button"
          onClick={toggle}
          aria-label="Mở rộng thanh bên"
          title="Mở rộng thanh bên"
          className="group/expand relative flex size-11 items-center justify-center rounded-xl outline-hidden transition-all duration-200 hover:bg-gray-100 focus-visible:ring-2 focus-visible:ring-primary-500/50 dark:hover:bg-dark-700 cursor-pointer"
        >
          <Logo className="size-9 shrink-0 text-primary-600 transition-opacity duration-200 group-hover/expand:opacity-0 dark:text-primary-400" />
          <ChevronDoubleRightIcon className="absolute size-5 text-primary-600 opacity-0 transition-all duration-200 group-hover/expand:scale-110 group-hover/expand:opacity-100 stroke-[2] rtl:rotate-180 dark:text-primary-400" />
        </button>
      )}
    </header>
  );
}
