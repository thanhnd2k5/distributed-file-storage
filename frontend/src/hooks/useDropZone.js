import { useState, useRef } from "react";

/**
 * useDropZone Hook
 * Tách biệt logic xử lý drag & drop và click file input.
 */
export function useDropZone(onFilePick) {
  const [isDragging, setIsDragging] = useState(false);
  const inputRef = useRef(null);

  const handleDragOver = (e) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = () => {
    setIsDragging(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    const file = e.dataTransfer.files[0];
    if (file && onFilePick) {
      onFilePick(file);
    }
  };

  const handleClick = () => {
    inputRef.current?.click();
  };

  const handleInputChange = (e) => {
    const file = e.target.files[0];
    if (file && onFilePick) {
      onFilePick(file);
    }
  };

  return {
    isDragging,
    inputRef,
    dropZoneProps: {
      onDragOver: handleDragOver,
      onDragLeave: handleDragLeave,
      onDrop: handleDrop,
      onClick: handleClick,
    },
    inputProps: {
      ref: inputRef,
      type: "file",
      className: "hidden",
      onChange: handleInputChange,
    },
  };
}
