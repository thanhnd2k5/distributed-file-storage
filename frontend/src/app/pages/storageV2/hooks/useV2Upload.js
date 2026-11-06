import { useEffect, useState } from "react";
import { createUploadApi } from "api/storageV2";
import { UploadController } from "../upload";

export function useV2Upload() {
  const [controller] = useState(() => new UploadController(createUploadApi()));
  const [view, setView] = useState(() => controller.snapshot());
  const [file, setFile] = useState(null);
  useEffect(() => {
    const unsubscribe = controller.subscribe(setView);
    return () => {
      unsubscribe();
      controller.pause();
    };
  }, [controller]);
  const act = (task) =>
    Promise.resolve()
      .then(task)
      .catch((error) => {
        // Errors before admission (another local action, corrupt receipt, missing
        // browser capability) also need visible feedback.
        if (!controller.view.error && error.name !== "AbortError") {
          controller.show({ error: error.message });
        }
      });
  return {
    view,
    file,
    setFile,
    resume: () => act(() => controller.resume(file)),
    pause: () => controller.pause(),
    abort: () => act(() => controller.abort()),
    refresh: () => act(() => controller.refresh()),
    newUpload: () =>
      act(async () => {
        await controller.newUpload();
        setFile(null);
      }),
  };
}
