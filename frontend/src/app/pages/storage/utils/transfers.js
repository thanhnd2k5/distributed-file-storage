export function transferProgress({ loaded, total }) {
  return Number.isFinite(total) && total > 0 && Number.isFinite(loaded)
    ? Math.min(100, Math.max(0, Math.floor((loaded / total) * 100)))
    : null;
}

function safeFilename(value) {
  const name = value
    .split(/[\\/]/)
    .at(-1)
    .replace(/[\p{Cc}<>:"|?*]/gu, "_")
    .trim()
    .replace(/[. ]+$/, "");
  if (!name || name === ".") return "download.bin";
  return /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(name)
    ? `_${name}`
    : name;
}

export function downloadFilename(disposition, fallback) {
  const extended = disposition?.match(
    /(?:^|;)\s*filename\*\s*=\s*(?:"([^"]*)"|([^;]*))/i,
  );
  const encoded = (extended?.[1] ?? extended?.[2])?.trim();
  const utf8 = encoded?.match(/^UTF-8'[^']*'(.*)$/i);
  if (utf8) {
    try {
      const decoded = decodeURIComponent(utf8[1]);
      if (decoded.trim()) return safeFilename(decoded);
    } catch {
      // A malformed filename* can still have a usable ordinary filename.
    }
  }
  const ordinary = disposition?.match(
    /(?:^|;)\s*filename\s*=\s*(?:"((?:\\.|[^"\\])*)"|([^;]*))/i,
  );
  const name = (
    ordinary?.[1]?.replace(/\\(.)/g, "$1") ?? ordinary?.[2]
  )?.trim();
  return safeFilename(name || fallback || "download.bin");
}

export function saveBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  try {
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    link.hidden = true;
    document.body.append(link);
    try {
      link.click();
    } finally {
      link.remove();
    }
  } finally {
    // Give the browser time to consume the object URL, even after navigation.
    window.setTimeout(() => URL.revokeObjectURL(url), 30_000);
  }
}
