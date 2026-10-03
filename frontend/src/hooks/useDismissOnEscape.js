import { useEffect } from "react";

/**
 * Calls onDismiss when Escape is pressed while enabled.
 * @param {boolean} enabled
 * @param {() => void} onDismiss
 */
export function useDismissOnEscape(enabled, onDismiss) {
  useEffect(() => {
    if (!enabled) return;

    const onKeyDown = (event) => {
      if (event.key === "Escape") {
        onDismiss();
      }
    };

    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [enabled, onDismiss]);
}
