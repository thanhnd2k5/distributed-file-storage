export const storageKeys = {
  all: ["storage"],
  files: ["storage", "files"],
  list: ({ limit = 50, offset = 0, include_inactive = false } = {}) => [
    "storage",
    "files",
    "list",
    { limit, offset, include_inactive },
  ],
  file: (fileId) => ["storage", "files", fileId],
  detail: (fileId) => ["storage", "files", fileId, "detail"],
  chunks: (fileId) => ["storage", "files", fileId, "chunks"],
  nodes: ["storage", "nodes"],
  cluster: ["storage", "cluster"],
  ready: ["storage", "ready"],
  upload: ["storage", "upload"],
  delete: ["storage", "delete"],
  repair: ["storage", "repair"],
};
