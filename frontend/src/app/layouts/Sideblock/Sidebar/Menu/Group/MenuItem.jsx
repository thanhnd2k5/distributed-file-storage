// Import Dependencies
import PropTypes from "prop-types";
import clsx from "clsx";
import { NavLink, useRouteLoaderData } from "react-router";
import { useTranslation } from "react-i18next";

// Local Imports
import { Badge } from "components/ui";
import { useBreakpointsContext } from "app/contexts/breakpoint/context";
import { useSidebarContext } from "app/contexts/sidebar/context";

// ----------------------------------------------------------------------

export function MenuItem({ data }) {
  const { Icon, path, id, transKey } = data;
  const { lgAndDown, lgAndUp } = useBreakpointsContext();
  const { isExpanded, close } = useSidebarContext();
  const { t } = useTranslation();

  const title = t(transKey) || data.title;
  const info = useRouteLoaderData("root")?.[id]?.info;

  const handleMenuItemClick = () => lgAndDown && close();

  // Collapsed View (Icon only - MainLayout Style)
  if (!isExpanded) {
    return (
      <div className="flex justify-center px-2 py-1">
        <NavLink
          to={path}
          onClick={handleMenuItemClick}
          {...{
            "data-tooltip": lgAndUp ? true : undefined,
            "data-tooltip-content": title,
            "data-tooltip-place": "right",
          }}
          className={({ isActive }) =>
            clsx(
              "relative flex size-10 shrink-0 items-center justify-center rounded-lg outline-hidden transition-all duration-200",
              isActive
                ? "bg-primary-600/10 text-primary-600 dark:bg-primary-400/15 dark:text-primary-400"
                : "text-gray-500 hover:bg-gray-100 hover:text-gray-900 dark:text-dark-200 dark:hover:bg-dark-300/20 dark:hover:text-dark-50",
            )
          }
        >
          {({ isActive }) => (
            <>
              {Icon && (
                <Icon
                  className={clsx(
                    "size-6 shrink-0 stroke-[1.5] transition-transform duration-200",
                    isActive ? "scale-110" : "group-hover:scale-110",
                  )}
                />
              )}
              {info && info.val && (
                <Badge
                  color={info.color}
                  className="absolute -top-1 -right-1 flex h-4 min-w-[1rem] items-center justify-center rounded-full px-1 text-[10px] font-bold ring-2 ring-white dark:ring-dark-900"
                >
                  {info.val}
                </Badge>
              )}
            </>
          )}
        </NavLink>
      </div>
    );
  }

  // Expanded View (Standard List Style)
  return (
    <div className="relative flex px-3">
      <NavLink
        to={path}
        onClick={handleMenuItemClick}
        className={({ isActive }) =>
          clsx(
            "group min-w-0 flex-1 rounded-md px-3 py-2 font-medium outline-hidden transition-colors ease-in-out",
            isActive
              ? "text-primary-600 dark:text-primary-400"
              : "text-gray-800 hover:bg-gray-100 hover:text-gray-950 focus:bg-gray-100 focus:text-gray-950 dark:text-dark-200 dark:hover:bg-dark-300/10 dark:hover:text-dark-50 dark:focus:bg-dark-300/10",
          )
        }
      >
        {({ isActive }) => (
          <>
            <div
              data-menu-active={isActive}
              className="flex min-w-0 items-center justify-between gap-2 text-xs-plus tracking-wide"
            >
              <div className="flex min-w-0 items-center gap-3">
                {Icon && (
                  <Icon
                    className={clsx(
                      "size-5 shrink-0 stroke-[1.5]",
                      !isActive && "opacity-80 group-hover:opacity-100",
                    )}
                  />
                )}
                <span className="truncate animate-in fade-in duration-300">{title}</span>
              </div>
              {info && info.val && (
                <Badge
                  color={info.color}
                  variant="soft"
                  className="h-4.5 min-w-[1rem] shrink-0 p-[5px] text-tiny-plus"
                >
                  {info.val}
                </Badge>
              )}
            </div>
            {isActive && (
              <div className="absolute bottom-1 top-1 w-1 bg-primary-600 dark:bg-primary-400 ltr:left-0 ltr:rounded-r-full rtl:right-0 rtl:rounded-l-lg" />
            )}
          </>
        )}
      </NavLink>
    </div>
  );
}

MenuItem.propTypes = {
  data: PropTypes.object,
};
