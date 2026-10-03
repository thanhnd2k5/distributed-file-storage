// Import Dependencies
import { QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "react-router";

// Local Imports
import { AuthBootstrap } from "app/AuthBootstrap";
import { queryClient } from "app/queryClient";
import { BreakpointProvider } from "app/contexts/breakpoint/Provider";
import { LocaleProvider } from "app/contexts/locale/Provider";
import { SidebarProvider } from "app/contexts/sidebar/Provider";
import { ThemeProvider } from "app/contexts/theme/Provider";
import router from "app/router/router";

// ----------------------------------------------------------------------

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthBootstrap>
        <ThemeProvider>
          <LocaleProvider>
            <BreakpointProvider>
              <SidebarProvider>
                <RouterProvider router={router} />
              </SidebarProvider>
            </BreakpointProvider>
          </LocaleProvider>
        </ThemeProvider>
      </AuthBootstrap>
    </QueryClientProvider>
  );
}

export default App;
