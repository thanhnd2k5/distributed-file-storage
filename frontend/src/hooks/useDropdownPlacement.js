import { useState, useLayoutEffect, useCallback } from "react";

/** Matches Tailwind `mt-1.5` / `mb-1.5` on the dropdown panel */
export const DROPDOWN_GAP_PX = 6;

/** Matches Tailwind `max-h-60` when no custom max-height is set */
export const DROPDOWN_FALLBACK_MAX_HEIGHT_PX = 240;

export const DROPDOWN_PLACEMENT_CLASS = {
  top: "bottom-full mb-1.5",
  bottom: "top-full mt-1.5",
};

export function getDropdownPlacementClassName(placement) {
  return placement === "top"
    ? DROPDOWN_PLACEMENT_CLASS.top
    : DROPDOWN_PLACEMENT_CLASS.bottom;
}

/**
 * Expected dropdown height for placement (respects max-height from CSS).
 */
export function getDropdownMeasuredHeight(
  dropdownEl,
  fallback = DROPDOWN_FALLBACK_MAX_HEIGHT_PX
) {
  if (!dropdownEl) return fallback;

  const { maxHeight } = getComputedStyle(dropdownEl);
  const parsed = parseFloat(maxHeight);
  const maxFromCss =
    maxHeight && maxHeight !== "none" && Number.isFinite(parsed)
      ? parsed
      : fallback;

  return Math.min(dropdownEl.scrollHeight, maxFromCss);
}

/**
 * Chooses "top" or "bottom" from viewport space around the trigger.
 */
export function resolveDropdownPlacement(
  triggerEl,
  dropdownEl,
  { gap = DROPDOWN_GAP_PX, fallbackMaxHeight = DROPDOWN_FALLBACK_MAX_HEIGHT_PX } = {}
) {
  const rect = triggerEl.getBoundingClientRect();
  const dropdownHeight = getDropdownMeasuredHeight(dropdownEl, fallbackMaxHeight);
  const spaceBelow = window.innerHeight - rect.bottom - gap;
  const spaceAbove = rect.top - gap;

  if (spaceBelow >= dropdownHeight) return "bottom";
  if (spaceAbove >= dropdownHeight) return "top";
  return spaceBelow >= spaceAbove ? "bottom" : "top";
}

/**
 * Resolves dropdown placement. Fixed values pass through; "auto" flips by viewport.
 *
 * @param {Object} params
 * @param {boolean} params.enabled - Recompute while the panel is open
 * @param {"auto"|"top"|"bottom"} [params.placement="auto"]
 * @param {React.RefObject<HTMLElement>} params.triggerRef
 * @param {React.RefObject<HTMLElement>} [params.dropdownRef]
 * @returns {"top"|"bottom"}
 */
export function useDropdownPlacement({
  enabled,
  placement = "auto",
  triggerRef,
  dropdownRef,
  gap = DROPDOWN_GAP_PX,
  fallbackMaxHeight = DROPDOWN_FALLBACK_MAX_HEIGHT_PX,
}) {
  const [autoPlacement, setAutoPlacement] = useState("bottom");

  const update = useCallback(() => {
    if (placement !== "auto" || !triggerRef?.current) return;

    const next = resolveDropdownPlacement(
      triggerRef.current,
      dropdownRef?.current,
      { gap, fallbackMaxHeight }
    );

    setAutoPlacement((prev) => (prev === next ? prev : next));
  }, [placement, triggerRef, dropdownRef, gap, fallbackMaxHeight]);

  useLayoutEffect(() => {
    if (!enabled || placement !== "auto") return;

    update();

    // Re-measure after paint when the panel node is fully laid out
    const rafId = requestAnimationFrame(update);

    const resizeObserver = new ResizeObserver(update);
    const observed = new Set();

    for (const el of [triggerRef?.current, dropdownRef?.current]) {
      if (el && !observed.has(el)) {
        observed.add(el);
        resizeObserver.observe(el);
      }
    }

    window.addEventListener("scroll", update, true);
    window.addEventListener("resize", update);

    return () => {
      cancelAnimationFrame(rafId);
      resizeObserver.disconnect();
      window.removeEventListener("scroll", update, true);
      window.removeEventListener("resize", update);
    };
  }, [enabled, placement, update, triggerRef, dropdownRef]);

  return placement === "auto" ? autoPlacement : placement;
}
