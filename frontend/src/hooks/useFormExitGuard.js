import { useCallback, useEffect, useRef, useState } from "react";
import { useBlocker } from "react-router";

export const useFormExitGuard = ({ isDirty, enabled = true }) => {
  const shouldGuard = enabled && isDirty;
  const pendingActionRef = useRef(null);
  const allowExitRef = useRef(false);
  const [showConfirm, setShowConfirm] = useState(false);

  const blocker = useBlocker(
    ({ currentLocation, nextLocation }) =>
      !allowExitRef.current &&
      shouldGuard &&
      currentLocation.pathname !== nextLocation.pathname,
  );

  useEffect(() => {
    if (blocker.state === "blocked") {
      setShowConfirm(true);
    }
  }, [blocker.state]);

  useEffect(() => {
    if (!shouldGuard) return undefined;

    const onBeforeUnload = (event) => {
      event.preventDefault();
      event.returnValue = "";
    };

    window.addEventListener("beforeunload", onBeforeUnload);
    return () => window.removeEventListener("beforeunload", onBeforeUnload);
  }, [shouldGuard]);

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
    allowExitRef.current = true;

    if (blocker.state === "blocked") {
      blocker.proceed();
      return;
    }

    const action = pendingActionRef.current;
    pendingActionRef.current = null;
    action?.();
  }, [blocker]);

  useEffect(() => {
    if (blocker.state === "unblocked" && allowExitRef.current) {
      allowExitRef.current = false;
    }
  }, [blocker.state]);

  const cancelLeave = useCallback(() => {
    setShowConfirm(false);
    pendingActionRef.current = null;
    if (blocker.state === "blocked") {
      blocker.reset();
    }
  }, [blocker]);

  return { showConfirm, requestLeave, confirmLeave, cancelLeave };
};
