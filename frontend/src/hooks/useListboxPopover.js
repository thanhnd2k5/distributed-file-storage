import { useRef, useCallback } from "react";

import { useClickOutside } from "hooks/useClickOutside";
import { useDismissOnEscape } from "hooks/useDismissOnEscape";
import {
  useDropdownPlacement,
  getDropdownPlacementClassName,
} from "hooks/useDropdownPlacement";

/**
 * Shared overlay behavior for custom listbox panels (Select, MultiSelect, …).
 * Handles outside click, Escape, and auto top/bottom placement.
 */
export function useListboxPopover({
  isOpen,
  onClose,
  placement = "auto",
  triggerRef,
  dropdownRef,
}) {
  const wrapperRef = useRef(null);

  const handleClickOutside = useCallback(() => {
    if (isOpen) onClose();
  }, [isOpen, onClose]);

  useClickOutside(wrapperRef, handleClickOutside);
  useDismissOnEscape(isOpen, onClose);

  const effectivePlacement = useDropdownPlacement({
    enabled: isOpen,
    placement,
    triggerRef,
    dropdownRef,
  });

  return {
    wrapperRef,
    effectivePlacement,
    placementClassName: getDropdownPlacementClassName(effectivePlacement),
  };
}
