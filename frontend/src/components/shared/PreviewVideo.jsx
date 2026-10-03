import PropTypes from "prop-types";
import { useEffect, useState } from "react";

/**
 * PreviewVideo Component
 * Handles local File objects and remote URLs for video preview.
 * 
 * @param {File} file - Local video file object
 * @param {string} src - Remote video URL
 * @param {string} className - Additional CSS classes
 */
export function PreviewVideo({ file, src, className, ...rest }) {
  const [previewUrl, setPreviewUrl] = useState(null);

  useEffect(() => {
    let url = null;
    if (file) {
      try {
        url = URL.createObjectURL(file);
        setPreviewUrl(url);
      } catch (err) {
        console.error("PreviewVideo Error:", err);
        setPreviewUrl(null);
      }
    } else {
      setPreviewUrl(null);
    }

    return () => {
      if (url) {
        URL.revokeObjectURL(url);
      }
    };
  }, [file]);

  const finalSrc = previewUrl || src;

  if (!finalSrc) return null;

  // Handle raw S3 keys that haven't been transformed to presigned URLs yet
  if (finalSrc && !finalSrc.startsWith("http") && !finalSrc.startsWith("blob:")) {
    return (
      <div className={`flex flex-col items-center justify-center bg-gray-100 dark:bg-gray-800/50 text-gray-500 rounded-xl border-2 border-dashed border-gray-200 dark:border-gray-700 p-6 ${className}`}>
        <div className="text-3xl mb-2">🎬</div>
        <p className="text-xs font-bold uppercase tracking-wider">Video đã được chọn</p>
        <p className="text-[10px] mt-1 text-gray-400">{`Vui lòng nhấn "Lưu" để có thể xem trước`}</p>
      </div>
    );
  }

  return (
    <video
      src={finalSrc}
      controls
      className={`rounded-xl bg-black shadow-lg ${className}`}
      {...rest}
    />
  );
}

PreviewVideo.propTypes = {
  file: PropTypes.object,
  src: PropTypes.string,
  className: PropTypes.string,
};
