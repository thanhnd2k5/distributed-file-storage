import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";

dayjs.extend(relativeTime);

/**
 * Formats a date string or object to MM/DD/YYYY format.
 *
 * @param {string|Date} date - The date to format.
 * @returns {string} - Formatted date.
 */
export const formatDate = (date) => {
  return dayjs(date).format("MM/DD/YYYY");
};

/**
 * Returns a human-readable relative time string (e.g., "3 hours ago").
 *
 * @param {string|Date} date - The date.
 * @returns {string} - Relative time string.
 */
export const timeFromNow = (date) => {
  if (!date) return "";
  return dayjs(date).fromNow();
};

/**
 * Pads a number to two digits by adding a leading zero if necessary.
 *
 * @param {number} num - The number to pad.
 * @returns {string} - The padded number as a string.
 */
function padTo2Digits(num) {
  return num.toString().padStart(2, "0");
}

/**
 * Converts milliseconds into a formatted time string (HH:MM:SS).
 *
 * @param {number} milliseconds - The time in milliseconds to convert.
 * @returns {string} - The formatted time string.
 */
export function msToTime(milliseconds) {
  if (typeof milliseconds !== "number" || milliseconds < 0) {
    throw new Error("Input must be a non-negative number.");
  }

  let seconds = Math.floor(milliseconds / 1000);
  let minutes = Math.floor(seconds / 60);
  let hours = Math.floor(minutes / 60);

  // Get remaining seconds and minutes after dividing by 60
  seconds = seconds % 60;
  minutes = minutes % 60;

  // Format the time as HH:MM:SS
  return `${padTo2Digits(hours)}:${padTo2Digits(minutes)}:${padTo2Digits(seconds)}`;
}

/**
 * Formats a duration in seconds into a human-friendly ETA string.
 *
 * @param {number} etaSeconds - The estimated time remaining in seconds.
 * @returns {string} - The formatted human-readable ETA string.
 */
export function formatEta(etaSeconds) {
  if (typeof etaSeconds !== "number" || isNaN(etaSeconds) || etaSeconds < 0) {
    return "";
  }

  if (etaSeconds < 1) {
    return "Hoàn thành";
  }

  if (etaSeconds < 60) {
    return `Còn khoảng ${Math.round(etaSeconds)}s`;
  }

  const minutes = Math.floor(etaSeconds / 60);
  const seconds = Math.round(etaSeconds % 60);

  if (seconds === 0) {
    return `Còn khoảng ${minutes}m`;
  }

  return `Còn khoảng ${minutes}m ${seconds}s`;
}
