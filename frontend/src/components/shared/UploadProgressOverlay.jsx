import { VideoCameraIcon } from "@heroicons/react/24/outline";

export function UploadProgressOverlay({ isUploading, uploadProgress }) {
  if (!isUploading) return null;

  return (
    <div className="absolute inset-0 bg-gray-950/85 backdrop-blur-md flex flex-col items-center justify-center z-20 p-6 select-none animate-in fade-in duration-300">
      {/* Top spinner and status */}
      <div className="flex items-center gap-3 mb-4">
        <div className="relative flex items-center justify-center">
          <div className="w-10 h-10 border-4 border-blue-500/20 border-t-blue-500 rounded-full animate-spin"></div>
          <VideoCameraIcon className="w-4 h-4 text-blue-400 absolute animate-pulse" />
        </div>
        <div className="text-left">
          <p className="text-xs font-black text-white uppercase tracking-widest">Đang tải video lên S3</p>
          <p className="text-[10px] text-gray-400 mt-0.5">Vui lòng không đóng trình duyệt</p>
        </div>
      </div>

      {/* Percentage Display */}
      <div className="flex items-baseline justify-between w-4/5 mb-1.5">
        <span className="text-2xl font-black text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-indigo-300 tracking-tight">
          {uploadProgress?.percent || 0}%
        </span>
        <span className="text-[10px] font-bold text-gray-500 uppercase tracking-wider">
          {uploadProgress?.uploadedSize || "0 B"} / {uploadProgress?.totalSize || "0 B"}
        </span>
      </div>

      {/* Progress Track */}
      <div className="w-4/5 h-2.5 bg-gray-800/80 rounded-full overflow-hidden border border-gray-700/30 p-[2px] shadow-inner mb-4">
        <div
          className="h-full rounded-full bg-gradient-to-r from-blue-500 via-indigo-500 to-purple-500 transition-all duration-300 ease-out shadow-[0_0_12px_rgba(59,130,246,0.5)]"
          style={{ width: `${uploadProgress?.percent || 0}%` }}
        ></div>
      </div>

      {/* Speed & ETA Badges */}
      <div className="flex justify-between items-center w-4/5 gap-3">
        {/* Speed Badge */}
        <div className="flex items-center gap-1.5 px-3 py-1 bg-blue-950/40 border border-blue-800/30 rounded-lg">
          <span className="w-1.5 h-1.5 rounded-full bg-cyan-400 animate-ping"></span>
          <span className="text-[10px] font-bold text-cyan-300 font-mono tracking-wide">
            {uploadProgress?.speed || "0 B/s"}
          </span>
        </div>
        
        {/* ETA Badge */}
        {uploadProgress?.eta && (
          <div className="text-[10px] font-semibold text-gray-400 bg-gray-900/50 px-2.5 py-1 border border-gray-800 rounded-lg">
            {uploadProgress.eta}
          </div>
        )}
      </div>
    </div>
  );
}
