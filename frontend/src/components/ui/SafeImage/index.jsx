import { Fragment, useState } from "react";
import { Dialog, DialogPanel, Transition, TransitionChild } from "@headlessui/react";
import { FiImage, FiX, FiMaximize2 } from "react-icons/fi";
import { Skeleton } from "../Skeleton";
import clsx from "clsx";

/**
 * SafeImage Component
 * Handles images gracefully with:
 * - Skeleton loading state
 * - Error/Deleted state fallback
 * - Empty state fallback
 * - Integrated Lightbox (Zoom)
 */
const SafeImage = ({
    src,
    alt = "Image",
    className,
    imgClassName,
    fallbackIcon: FallbackIcon = FiImage,
    fallbackText = "Không có ảnh minh chứng",
    errorText = "Ảnh đã bị xoá hoặc không tồn tại",
    zoomable = true,
    children,
    ...props
}) => {
    const [status, setStatus] = useState(src ? "loading" : "empty");
    const [showLightbox, setShowLightbox] = useState(false);

    const handleLoad = () => setStatus("loaded");
    const handleError = () => setStatus("error");

    const handleZoom = (e) => {
        if (!zoomable || status !== "loaded") return;
        e.stopPropagation();
        setShowLightbox(true);
    };

    // 1. Empty State
    if (status === "empty" || !src) {
        return (
            <div className={clsx("flex flex-col items-center justify-center bg-gray-50 dark:bg-gray-900/40 border-2 border-dashed border-gray-200 dark:border-gray-800 rounded-2xl p-8 text-gray-400 gap-3", className)}>
                <div className="p-4 bg-white dark:bg-gray-800 rounded-full shadow-sm">
                    <FallbackIcon className="w-8 h-8 opacity-50" />
                </div>
                <span className="text-sm font-medium italic">{fallbackText}</span>
            </div>
        );
    }

    // 2. Error/Deleted State
    if (status === "error") {
        return (
            <div className={clsx("flex flex-col items-center justify-center bg-rose-50/30 dark:bg-rose-900/10 border-2 border-dashed border-rose-200 dark:border-rose-900/30 rounded-2xl p-8 text-rose-500/70 gap-3", className)}>
                 <div className="p-4 bg-white dark:bg-gray-800 rounded-full shadow-sm">
                    <FallbackIcon className="w-8 h-8" />
                </div>
                <span className="text-sm font-medium italic">{errorText}</span>
            </div>
        );
    }

    return (
        <div className={clsx("relative group overflow-hidden", className)}>
            {/* Loading Skeleton */}
            {status === "loading" && (
                <Skeleton className="absolute inset-0 w-full h-full z-10" />
            )}

            {/* Main Image */}
            <img
                src={src}
                alt={alt}
                onLoad={handleLoad}
                onError={handleError}
                loading="lazy"
                onClick={handleZoom}
                className={clsx(
                    "transition-all duration-700 ease-in-out",
                    status === "loading" ? "opacity-0 scale-95" : "opacity-100 scale-100",
                    zoomable && "cursor-zoom-in",
                    imgClassName
                )}
                {...props}
            />

            {/* Success Overlays / Custom Children */}
            {status === "loaded" && (
                <>
                    {/* Zoom Icon Overlay */}
                    {zoomable && (
                        <div 
                            className="absolute top-3 right-3 p-2 bg-white/90 dark:bg-gray-800/90 rounded-xl shadow-lg opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer z-10"
                            onClick={handleZoom}
                        >
                            <FiMaximize2 className="w-4 h-4 text-indigo-600 dark:text-indigo-400" />
                        </div>
                    )}
                    
                    {/* Extra children (like action buttons) */}
                    {children}
                </>
            )}

            {/* Lightbox Modal */}
            <Transition appear show={showLightbox} as={Fragment}>
                <Dialog as="div" className="relative z-[100]" onClose={() => setShowLightbox(false)}>
                    <TransitionChild
                        as={Fragment}
                        enter="ease-out duration-300"
                        enterFrom="opacity-0"
                        enterTo="opacity-100"
                        leave="ease-in duration-200"
                        leaveFrom="opacity-100"
                        leaveTo="opacity-0"
                    >
                        <div className="fixed inset-0 bg-black/90 backdrop-blur-md" />
                    </TransitionChild>

                    <div className="fixed inset-0 overflow-y-auto">
                        <div className="flex min-h-full items-center justify-center p-4">
                            <TransitionChild
                                as={Fragment}
                                enter="ease-out duration-300"
                                enterFrom="opacity-0 scale-95"
                                enterTo="opacity-100 scale-100"
                                leave="ease-in duration-200"
                                leaveFrom="opacity-100 scale-100"
                                leaveTo="opacity-0 scale-95"
                            >
                                <DialogPanel className="relative w-fit h-fit flex items-center justify-center">
                                    <button
                                        onClick={() => setShowLightbox(false)}
                                        className="absolute -top-12 right-0 p-3 text-white hover:text-gray-300 transition-colors bg-white/10 rounded-full"
                                    >
                                        <FiX className="w-6 h-6" />
                                    </button>
                                    <img
                                        src={src}
                                        alt={alt}
                                        className="max-w-full max-h-[85vh] object-contain rounded-lg shadow-2xl"
                                    />
                                </DialogPanel>
                            </TransitionChild>
                        </div>
                    </div>
                </Dialog>
            </Transition>
        </div>
    );
};

export { SafeImage };
