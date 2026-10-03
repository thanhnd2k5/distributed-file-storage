import { useState } from "react";
import { toast } from "sonner";
import { getPresignUploadUrlApi, uploadToS3Api } from "api/media";
import { formatBytes } from "utils/formatBytes";
import { formatEta } from "utils/time";

/**
 * Generic Hook for Direct S3 Uploads with state-of-the-art Exponential Moving Average (EMA) progress statistics
 */
export const useS3Upload = () => {
  const [isUploading, setIsUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [speed, setSpeed] = useState("");
  const [uploadedSize, setUploadedSize] = useState("");
  const [totalSize, setTotalSize] = useState("");
  const [eta, setEta] = useState("");

  /**
   * Upload a file to S3 via presigned URL
   * @param {File} file - Raw file object
   * @param {string} folder - Target S3 folder path (e.g. 'alphabet_variants/videos')
   * @returns {Promise<string|null>} - The S3 Key or null on failure
   */
  const upload = async (file, folder) => {
    if (!file || !folder) return null;

    setIsUploading(true);
    setProgress(0);
    setSpeed("0 B/s");
    setUploadedSize("0 B");
    setTotalSize(file.size === 0 ? "0 B" : formatBytes(file.size, 1024, 1));
    setEta("");

    // Variables for Exponential Moving Average (EMA) Smoothing
    let lastLoaded = 0;
    let lastTime = Date.now();
    let smoothedSpeed = 0;
    const alpha = 0.85; // Smoothing factor (85% weight to history, 15% to instant change)

    try {
      // 1. Get presigned URL from backend
      const presignedData = await getPresignUploadUrlApi({
        filename: file.name,
        mimetype: file.type,
        folder,
      });

      // 2. Perform direct PUT to S3 with real-time stats tracking
      await uploadToS3Api(presignedData.uploadUrl, file, {
        onUploadProgress: (progressEvent) => {
          const loaded = progressEvent.loaded;
          const total = progressEvent.total || file.size;
          const percent = Math.round((loaded * 100) / total);

          setProgress(percent);
          setUploadedSize(loaded === 0 ? "0 B" : formatBytes(loaded, 1024, 1));
          setTotalSize(total === 0 ? "0 B" : formatBytes(total, 1024, 1));

          // Calculate Real-time Instantaneous Speed
          const now = Date.now();
          const deltaLoaded = loaded - lastLoaded;
          const deltaTime = (now - lastTime) / 1000; // seconds

          // Throttle updates to prevent dividing by extremely small intervals (e.g. < 50ms)
          if (deltaTime >= 0.05) {
            const instantSpeed = deltaLoaded / deltaTime; // Bytes/second in this step

            // Apply Exponential Moving Average (EMA) to smooth out networking spikes
            if (smoothedSpeed === 0) {
              smoothedSpeed = instantSpeed;
            } else {
              smoothedSpeed = (smoothedSpeed * alpha) + (instantSpeed * (1 - alpha));
            }

            // Save state for next progress tick
            lastLoaded = loaded;
            lastTime = now;

            // Update speed display
            setSpeed(smoothedSpeed === 0 ? "0 B/s" : `${formatBytes(smoothedSpeed, 1024, 1)}/s`);

            // Calculate Estimated Time Remaining (ETA) based on smoothed speed
            if (smoothedSpeed > 0) {
              const remainingBytes = total - loaded;
              const etaSeconds = remainingBytes / smoothedSpeed;
              setEta(formatEta(etaSeconds));
            } else {
              setEta("");
            }
          }
        },
      });

      // Return the S3 key on success
      return presignedData.key;
    } catch (error) {
      console.error("S3 Upload Error:", error);
      toast.error("Failed to upload file to cloud storage!");
      return null;
    } finally {
      setIsUploading(false);
    }
  };

  return {
    upload,
    isUploading,
    progress,
    speed,
    uploadedSize,
    totalSize,
    eta,
  };
};
