import { QueryClient } from "@tanstack/react-query";
import { storageKeys } from "api/storage/queryKeys";

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

queryClient.setMutationDefaults(storageKeys.all, { retry: false });
