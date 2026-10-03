// Import Dependencies
import { isRouteErrorResponse, useRouteError, Link } from "react-router";
import { lazy, useEffect } from "react";

// Local Imports
import { Loadable } from "components/shared/Loadable";
import WindowCrash from "assets/illustrations/window-crash.svg?react";
import { Page } from "components/shared/Page";
import { Button } from "components/ui";
import { useThemeContext } from "app/contexts/theme/context";
import { useHover } from "hooks";

// ----------------------------------------------------------------------

const app = {
  401: lazy(() => import("./401")),
  404: lazy(() => import("./404")),
  429: lazy(() => import("./429")),
  500: lazy(() => import("./500")),
};

function RootErrorBoundary() {
  const error = useRouteError();
  const { primaryColorScheme: primary, isDark } = useThemeContext();
  const [btnRef, btnHovered] = useHover();

  useEffect(() => {
    if (error) {
      console.error("Root Error Boundary caught an error:", error);
    }
  }, [error]);

  if (isRouteErrorResponse(error)) {
    const Component = Loadable(app[error.status]);
    return <Component />;
  }

  return (
    <Page title="Something went wrong">
      <main className="min-h-100vh grid w-full grow grid-cols-1 place-items-center">
        <div className="w-full max-w-2xl p-6 text-center">
          <WindowCrash
            className="mx-auto w-full max-w-lg"
            style={{
              "--primary": isDark ? primary[500] : primary[600],
              "--primary-light": primary[300],
            }}
          />
          <p className="pt-8 text-xl font-semibold text-gray-800 dark:text-dark-50">
            Oops. Something went wrong.
          </p>
          <p className="pt-2 text-gray-500 dark:text-dark-200">
            An unexpected error occurred. Our team has been notified.
          </p>
          <div className="mt-8 flex justify-center gap-4">
            <Button
              component={Link}
              to="/"
              ref={btnRef}
              isGlow={btnHovered}
              color="primary"
              className="h-11 text-base px-8"
            >
              Back To Home
            </Button>
          </div>
        </div>
      </main>
    </Page>
  );
}

export default RootErrorBoundary;
