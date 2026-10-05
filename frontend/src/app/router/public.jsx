import StorageLayout from "app/layouts/StorageLayout";

const publicRoutes = {
  id: "public",
  Component: StorageLayout,
  children: [
    {
      index: true,
      lazy: async () => ({
        Component: (await import("app/pages/storage")).default,
      }),
    },
    {
      path: "files/:fileId",
      lazy: async () => ({
        Component: (await import("app/pages/storage/detail")).default,
      }),
    },
  ],
};

export { publicRoutes };
