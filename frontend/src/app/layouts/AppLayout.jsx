// Import Dependencies
import { Outlet, useLocation } from "react-router";
import { useState } from "react";

// Local Imports
import { useSidebarContext } from "app/contexts/sidebar/context";
import { useThemeContext } from "app/contexts/theme/context";
import { useBreakpointsContext } from "app/contexts/breakpoint/context";
import { useIsomorphicEffect } from "hooks";

// ----------------------------------------------------------------------

const dataset = document?.body?.dataset;

export function AppLayout() {
  const { pathname } = useLocation();
  const { themeLayout } = useThemeContext();

  const activeLayout = pathname.startsWith("/settings")
    ? "main-layout"
    : themeLayout;

  const { close, open } = useSidebarContext();
  const { lgAndDown, xlAndUp } = useBreakpointsContext();
  const [isMounted, setIsMounted] = useState(false);

  useIsomorphicEffect(() => {
    if (xlAndUp) open();
    return () => {
      if (lgAndDown) close();
    };
  }, [close, lgAndDown, open, xlAndUp]);

  useIsomorphicEffect(() => {
    dataset.layout = activeLayout;

    // Fix flicker layout
    queueMicrotask(() => {
      dataset.layout = activeLayout;
    });

    return () => {
      if (dataset) {
        dataset.layout = themeLayout;
      }
    };
  }, [activeLayout, themeLayout]);

  useIsomorphicEffect(() => {
    setIsMounted(true);
  }, []);

  if (!isMounted) return null;

  return <Outlet />;
}
