import {
  DocumentIcon,
  DocumentTextIcon,
  FilmIcon,
  MusicalNoteIcon,
  PhotoIcon,
  TableCellsIcon,
} from "@heroicons/react/24/solid";

const styles = {
  image: "bg-emerald-100 text-emerald-700 dark:bg-emerald-500/20 dark:text-emerald-300",
  video: "bg-violet-100 text-violet-700 dark:bg-violet-500/20 dark:text-violet-300",
  audio: "bg-fuchsia-100 text-fuchsia-700 dark:bg-fuchsia-500/20 dark:text-fuchsia-300",
  sheet: "bg-teal-100 text-teal-700 dark:bg-teal-500/20 dark:text-teal-300",
  text: "bg-sky-100 text-sky-700 dark:bg-sky-500/20 dark:text-sky-300",
  file: "bg-amber-100 text-amber-800 dark:bg-amber-500/20 dark:text-amber-200",
};

function kindFromName(name = "") {
  const ext = name.includes(".")
    ? name.slice(name.lastIndexOf(".") + 1).toLowerCase()
    : "";
  if (["png", "jpg", "jpeg", "gif", "webp", "svg", "bmp"].includes(ext))
    return "image";
  if (["mp4", "mov", "webm", "mkv", "avi"].includes(ext)) return "video";
  if (["mp3", "wav", "flac", "aac", "ogg"].includes(ext)) return "audio";
  if (["csv", "xls", "xlsx", "tsv"].includes(ext)) return "sheet";
  if (["txt", "md", "pdf", "doc", "docx", "json", "xml"].includes(ext))
    return "text";
  return "file";
}

const icons = {
  image: PhotoIcon,
  video: FilmIcon,
  audio: MusicalNoteIcon,
  sheet: TableCellsIcon,
  text: DocumentTextIcon,
  file: DocumentIcon,
};

export default function FileGlyph({ name, className = "" }) {
  const kind = kindFromName(name);
  const Icon = icons[kind];
  return (
    <span
      className={`inline-flex size-9 shrink-0 items-center justify-center rounded-lg ${styles[kind]} ${className}`}
      aria-hidden
    >
      <Icon className="size-5" />
    </span>
  );
}
