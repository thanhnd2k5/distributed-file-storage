// Import Dependencies
import PropTypes from "prop-types";
import { useEffect, useState, Fragment } from "react";
import { Dialog, DialogPanel, Transition, TransitionChild } from "@headlessui/react";
import { FiZoomIn, FiX } from "react-icons/fi";
import clsx from "clsx";

// ----------------------------------------------------------------------

export function PreviewImg({ file, src, alt, zoomable = false, className, ...rest }) {
  const [previewUrl, setPreviewUrl] = useState(null);
  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    if (!file) {
      setPreviewUrl(null);
      return;
    }

    let url = "";
    try {
      url = URL.createObjectURL(file);
      setPreviewUrl(url);
    } catch (err) {
      console.error(err);
      setPreviewUrl(null);
    }

    return () => {
      if (url) {
        URL.revokeObjectURL(url);
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [file]);

  const imgUrl = previewUrl || src;

  if (zoomable) {
    return (
      <>
        <div
          onClick={() => setIsOpen(true)}
          className={clsx(
            "relative group/preview cursor-zoom-in overflow-hidden rounded-xl",
            className
          )}
        >
          <img
            src={imgUrl}
            alt={alt}
            className="w-full h-full object-contain transition-transform duration-350 group-hover/preview:scale-[1.02]"
            {...rest}
          />
          <div className="absolute inset-0 bg-black/0 group-hover/preview:bg-black/10 flex items-center justify-center opacity-0 group-hover/preview:opacity-100 transition-all duration-300 pointer-events-none">
            <div className="p-2 bg-white/95 dark:bg-dark-900/95 rounded-xl shadow-lg flex items-center gap-1.5 text-xs font-bold text-gray-700 dark:text-dark-200">
              <FiZoomIn className="size-4" />
              <span>Xem ảnh lớn</span>
            </div>
          </div>
        </div>

        {/* ─── Lightbox Modal ─── */}
        <Transition appear show={isOpen} as={Fragment}>
          <Dialog as="div" className="relative z-50" onClose={() => setIsOpen(false)}>
            <TransitionChild
              as={Fragment}
              enter="ease-out duration-300"
              enterFrom="opacity-0"
              enterTo="opacity-100"
              leave="ease-in duration-200"
              leaveFrom="opacity-100"
              leaveTo="opacity-0"
            >
              <div className="fixed inset-0 bg-black/80 backdrop-blur-sm" />
            </TransitionChild>

            <div className="fixed inset-0 overflow-y-auto w-full flex items-center justify-center p-4">
              <TransitionChild
                as={Fragment}
                enter="ease-out duration-300"
                enterFrom="opacity-0 scale-95"
                enterTo="opacity-100 scale-100"
                leave="ease-in duration-200"
                leaveFrom="opacity-100 scale-100"
                leaveTo="opacity-0 scale-95"
              >
                <DialogPanel className="max-w-4xl w-fit flex flex-col relative bg-transparent border-0 outline-none">
                  <div className="flex justify-end mb-2 w-full max-w-full">
                    <button
                      onClick={() => setIsOpen(false)}
                      className="p-2 bg-white/10 hover:bg-white/20 text-white rounded-full transition-all border border-white/20 shadow-lg"
                      title="Đóng"
                      aria-label="Đóng ảnh"
                    >
                      <FiX className="w-5 h-5" />
                    </button>
                  </div>
                  <div className="p-2 bg-white dark:bg-dark-900 rounded-3xl shadow-2xl border border-gray-100 dark:border-dark-800 overflow-hidden flex flex-col items-center max-w-full">
                    <img
                      src={imgUrl}
                      alt={alt}
                      className="max-h-[75vh] w-auto object-contain rounded-2xl max-w-full"
                    />
                    {alt && (
                      <div className="px-6 py-3.5 bg-gray-50/50 dark:bg-dark-850/50 w-full text-center border-t border-gray-100 dark:border-dark-850 mt-2 rounded-b-2xl">
                        <p className="text-sm font-semibold text-gray-800 dark:text-dark-100">
                          {alt}
                        </p>
                      </div>
                    )}
                  </div>
                </DialogPanel>
              </TransitionChild>
            </div>
          </Dialog>
        </Transition>
      </>
    );
  }

  return (
    <img
      src={imgUrl}
      alt={alt}
      className={className}
      {...rest}
    />
  );
}

PreviewImg.propTypes = {
  file: PropTypes.object,
  src: PropTypes.string,
  alt: PropTypes.string,
  zoomable: PropTypes.bool,
  className: PropTypes.string,
};
