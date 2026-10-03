import { useState, useEffect, useCallback } from "react";

export function useResizableSidebar(isExpanded, open, close, defaultWidth = 260, minWidth = 200, maxWidth = 450, collapseThreshold = 140) {
  const [width, setWidth] = useState(defaultWidth);
  const [isResizing, setIsResizing] = useState(false);

  // Handle mouse move to resize or trigger collapse
  const resize = useCallback((e) => {
    if (!isResizing) return;
    const newWidth = e.clientX;

    if (newWidth < collapseThreshold) {
      if (isExpanded) close();
    } else {
      if (!isExpanded) open();
      const clampedWidth = Math.max(minWidth, Math.min(newWidth, maxWidth));
      setWidth(clampedWidth);
    }
  }, [isResizing, isExpanded, minWidth, maxWidth, collapseThreshold, open, close]);

  const startResizing = useCallback((e) => {
    e.preventDefault();
    setIsResizing(true);
  }, []);

  const stopResizing = useCallback(() => {
    setIsResizing(false);
  }, []);

  const resetWidth = useCallback(() => {
    setWidth(defaultWidth);
    if (!isExpanded) open();
  }, [defaultWidth, isExpanded, open]);

  useEffect(() => {
    if (isResizing) {
      window.addEventListener("mousemove", resize);
      window.addEventListener("mouseup", stopResizing);
    }
    return () => {
      window.removeEventListener("mousemove", resize);
      window.removeEventListener("mouseup", stopResizing);
    };
  }, [isResizing, resize, stopResizing]);

  return { width, isResizing, startResizing, resetWidth };
}
