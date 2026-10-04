"""Upload input and bounded chunk processing; no HTTP, database or RPC side effects."""

import hashlib
import unicodedata
from collections.abc import Iterable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from os import SEEK_END
from typing import BinaryIO

from starlette.datastructures import UploadFile


class FileInputError(ValueError):
    """Business error for the file router to render using the existing envelope."""

    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code


def invalid_request(message: str) -> FileInputError:
    return FileInputError(400, "INVALID_REQUEST", message)


def temp_storage_unavailable() -> FileInputError:
    return FileInputError(503, "TEMP_STORAGE_UNAVAILABLE", "Không thể đọc file tạm của upload.")


def normalize_filename(filename: str | None) -> str:
    if filename is None:
        raise invalid_request("File phải có tên hợp lệ.")
    basename = filename.replace("\\", "/").rsplit("/", 1)[-1]
    basename = "".join(char for char in basename if unicodedata.category(char) != "Cc").strip()
    if not 1 <= len(basename) <= 255 or basename in {".", ".."}:
        raise invalid_request("Tên file phải có từ 1 đến 255 ký tự.")
    try:
        basename.encode("utf-8")
    except UnicodeEncodeError:
        raise invalid_request("Tên file phải là Unicode hợp lệ.") from None
    return basename


def normalize_content_type(content_type: str | None) -> str:
    value = (content_type or "").strip() or "application/octet-stream"
    if len(value) > 255 or any(unicodedata.category(char) in {"Cc", "Cs"} for char in value):
        raise invalid_request("Content type không hợp lệ.")
    return value


def measure_spool(stream: BinaryIO, max_file_size_bytes: int) -> int:
    """Measure actual bytes without reading them and leave the spool rewound."""
    if max_file_size_bytes < 0:
        raise ValueError("max_file_size_bytes must be nonnegative")
    try:
        stream.seek(0, SEEK_END)
        size = stream.tell()
        stream.seek(0)
    except (OSError, ValueError):
        raise temp_storage_unavailable() from None
    if size > max_file_size_bytes:
        raise FileInputError(413, "FILE_TOO_LARGE", "File vượt quá dung lượng cho phép.")
    return size


@dataclass(frozen=True)
class PreparedUpload:
    original_name: str
    content_type: str
    size_bytes: int
    stream: BinaryIO


@contextmanager
def prepare_upload(
    parts: Iterable[tuple[str, object]], max_file_size_bytes: int
) -> Iterator[PreparedUpload]:
    """Own parsed multipart spools, including rejected/duplicate file parts.

    Pass FormData.multi_items(), not a dict that discards duplicate fields.
    Use this synchronous scope in the transfer threadpool. HTTP parsing and
    disconnect detection belong to the router; this scope always closes spools.
    """
    fields = tuple(parts)
    with ExitStack() as resources:
        for _, value in fields:
            if isinstance(value, UploadFile):
                resources.callback(value.file.close)
        if len(fields) != 1 or fields[0][0] != "file" or not isinstance(fields[0][1], UploadFile):
            raise invalid_request("Upload phải có đúng một field file.")
        upload = fields[0][1]
        yield PreparedUpload(
            original_name=normalize_filename(upload.filename),
            content_type=normalize_content_type(upload.content_type),
            size_bytes=measure_spool(upload.file, max_file_size_bytes),
            stream=upload.file,
        )


@dataclass(frozen=True)
class UploadChunk:
    chunk_index: int
    data: bytes
    checksum_sha256: str

    @property
    def size_bytes(self) -> int:
        return len(self.data)


@dataclass(frozen=True)
class FileDigest:
    size_bytes: int
    total_chunks: int
    checksum_sha256: str


class ChunkReader(Iterator[UploadChunk]):
    """Consume a rewound spool once; summary exists only after verified EOF.

    The caller owns the spool (normally prepare_upload). Chunk size and expected
    size are snapshots for this file. At most one chunk is processed at a time;
    the iterator never collects the file or reads with an unbounded size.
    """

    def __init__(self, stream: BinaryIO, chunk_size_bytes: int, expected_size_bytes: int):
        if chunk_size_bytes <= 0 or expected_size_bytes < 0:
            raise ValueError("chunk_size_bytes must be positive and expected size nonnegative")
        self.summary: FileDigest | None = None
        self._iterator = self._read_chunks(stream, chunk_size_bytes, expected_size_bytes)

    def __iter__(self) -> "ChunkReader":
        return self

    def __next__(self) -> UploadChunk:
        return next(self._iterator)

    def _read_chunks(self, stream, chunk_size, expected_size):
        full_hash = hashlib.sha256()
        size = count = 0
        while True:
            # Short reads are not EOF. Fill a chunk before assigning its index.
            data = bytearray()
            try:
                while len(data) < chunk_size:
                    block = stream.read(chunk_size - len(data))
                    if not block:
                        break
                    data.extend(block)
            except (OSError, ValueError):
                raise temp_storage_unavailable() from None
            if not data:
                break
            size += len(data)
            if size > expected_size:
                raise invalid_request("Kích thước file thay đổi trong khi xử lý upload.")
            payload = bytes(data)
            full_hash.update(payload)
            yield UploadChunk(count, payload, hashlib.sha256(payload).hexdigest())
            count += 1
        if size != expected_size:
            raise invalid_request("Kích thước file thay đổi trong khi xử lý upload.")
        self.summary = FileDigest(size, count, full_hash.hexdigest())
