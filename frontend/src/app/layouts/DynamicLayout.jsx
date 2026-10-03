// Import Dependencies
import { lazy, useMemo } from "react";

// Local Imports
import { useThemeContext } from "app/contexts/theme/context";
import { Loadable } from "components/shared/Loadable";
import { SplashScreen } from "components/template/SplashScreen";
import { useIsomorphicEffect } from "hooks";

// ----------------------------------------------------------------------

const themeLayouts = {
  "main-layout": lazy(() => import("./MainLayout")),
  sideblock: lazy(() => import("./Sideblock")),
};

export function DynamicLayout() {
  const { themeLayout } = useThemeContext();

  useIsomorphicEffect(() => {
    const dataset = document?.body?.dataset;
    if (dataset) {
      dataset.layout = themeLayout;
    }
  }, [themeLayout]);

  const CurrentLayout = useMemo(
    () => Loadable(themeLayouts[themeLayout], SplashScreen),
    [themeLayout],
  );

  return <CurrentLayout />;
}
