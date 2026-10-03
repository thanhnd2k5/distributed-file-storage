import { useCallback, useRef, useState } from "react";

/**
 * Exit confirmation for modals/dialogs (no route blocking).
 *
 * @example
 * const isDirty = formState.isDirty;
 * const { showConfirm, requestLeave, confirmLeave, cancelLeave } =
 *   useFormExitConfirm({ isDirty, enabled: isDirty && !isSubmitting });
 *
 * const handleClose = () => requestLeave(onClose);
 *
 * <Dialog onClose={handleClose} />
 * <FormExitConfirmModal show={showConfirm} onConfirm={confirmLeave} onCancel={cancelLeave} />
 */
export const useFormExitConfirm = ({ isDirty, enabled = true }) => {
  const shouldGuard = enabled && isDirty;
  const pendingActionRef = useRef(null);
  const [showConfirm, setShowConfirm] = useState(false);

  const requestLeave = useCallback(
    (action) => {
      if (!shouldGuard) {
        action();
        return;
      }
      pendingActionRef.current = action;
      setShowConfirm(true);
    },
    [shouldGuard],
  );

  const confirmLeave = useCallback(() => {
    setShowConfirm(false);
    const action = pendingActionRef.current;
    pendingActionRef.current = null;
    action?.();
  }, []);

  const cancelLeave = useCallback(() => {
    setShowConfirm(false);
    pendingActionRef.current = null;
  }, []);

  return { showConfirm, requestLeave, confirmLeave, cancelLeave };
};
