import hashlib
from io import BytesIO
from tempfile import SpooledTemporaryFile

import pytest
from starlette.datastructures import FormData, Headers, UploadFile

from metadata.file_input import (
    ChunkReader,
    FileInputError,
    measure_spool,
    normalize_content_type,
    normalize_filename,
    prepare_upload,
)

CHUNK_SIZE = 2 * 1024 * 1024
FILE_LIMIT = 64 * 1024 * 1024


class BoundedSpool(BytesIO):
    def __init__(self, data, *, short_read=None):
        super().__init__(data)
        self.read_sizes = []
        self.short_read = short_read

    def read(self, size=-1):
        assert 0 < size <= CHUNK_SIZE, "Upload attempted an unbounded/oversized read"
        self.read_sizes.append(size)
        return super().read(min(size, self.short_read) if self.short_read else size)


def upload(data=b"payload", *, filename="report.pdf", content_type=None, size=None, stream=None):
    return UploadFile(
        stream if stream is not None else BytesIO(data),
        filename=filename,
        size=size,
        headers=Headers({"content-type": content_type}) if content_type is not None else None,
    )


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("report.pdf", "report.pdf"),
        ("../../report.pdf", "report.pdf"),
        (r"C:\fakepath\report.pdf", "report.pdf"),
        (r"folder\nested/report.pdf", "report.pdf"),
        ("/tmp/báo cáo 😀.pdf", "báo cáo 😀.pdf"),
        ("a\x00b\r\nc\x7fd\x85e.txt", "abcde.txt"),
        ("  report.pdf  ", "report.pdf"),
        ("a" * 255, "a" * 255),
        ("報" * 255, "報" * 255),
        ("very-long-directory" * 30 + "/file.txt", "file.txt"),
    ],
)
def test_filename_normalizes_basename_controls_and_unicode(raw, expected):
    assert normalize_filename(raw) == expected


@pytest.mark.parametrize(
    "raw", [None, "", "   ", "\x00\r\n", "/", "folder/", "\\", ".", "..", "a" * 256, "\ud800.txt"]
)
def test_invalid_filename_has_safe_business_error(raw):
    with pytest.raises(FileInputError) as caught:
        normalize_filename(raw)
    assert (caught.value.status_code, caught.value.code) == (400, "INVALID_REQUEST")
    assert "\ud800" not in str(caught.value)


@pytest.mark.parametrize(
    "raw, expected",
    [
        (None, "application/octet-stream"),
        ("", "application/octet-stream"),
        ("  ", "application/octet-stream"),
        ("application/pdf", "application/pdf"),
        (" text/plain; charset=utf-8 ", "text/plain; charset=utf-8"),
        ("x" * 255, "x" * 255),
    ],
)
def test_content_type_hint_default_and_column_limit(raw, expected):
    assert normalize_content_type(raw) == expected


@pytest.mark.parametrize(
    "raw", ["x" * 256, "text/\x00plain", "text/\r\nplain", "text/\x85plain", "text/\ud800plain"]
)
def test_invalid_content_type_is_rejected(raw):
    with pytest.raises(FileInputError) as caught:
        normalize_content_type(raw)
    assert (caught.value.status_code, caught.value.code) == (400, "INVALID_REQUEST")


@pytest.mark.parametrize(
    "size", [0, 1, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1, 2 * CHUNK_SIZE, FILE_LIMIT]
)
def test_size_is_measured_without_reading_and_spool_is_rewound(size):
    spool = BoundedSpool(b"x" * size)
    spool.seek(size // 2)
    assert measure_spool(spool, FILE_LIMIT) == size
    assert spool.tell() == 0 and spool.read_sizes == []


def test_max_plus_one_is_rejected_without_reading():
    spool = BoundedSpool(b"x" * (FILE_LIMIT + 1))
    with pytest.raises(FileInputError) as caught:
        measure_spool(spool, FILE_LIMIT)
    assert (caught.value.status_code, caught.value.code) == (413, "FILE_TOO_LARGE")
    assert spool.tell() == 0 and spool.read_sizes == []


@pytest.mark.parametrize("declared_size", [None, 0, 999999999])
def test_prepare_uses_actual_spool_size_and_owns_it(declared_size):
    file = upload(size=declared_size, filename=r"C:\fakepath\report.pdf")
    file.file.seek(3)
    with prepare_upload(FormData([("file", file)]).multi_items(), 7) as prepared:
        assert prepared.original_name == "report.pdf"
        assert prepared.content_type == "application/octet-stream"
        assert prepared.size_bytes == 7 and prepared.stream.tell() == 0
        assert prepared.stream is file.file and not prepared.stream.closed
    assert file.file.closed


def test_prepare_rejects_lying_size_and_closes_oversized_spool():
    file = upload(size=1)
    with pytest.raises(FileInputError) as caught:
        with prepare_upload([("file", file)], 6):
            pytest.fail("Oversized file must fail before coordinator work")
    assert caught.value.code == "FILE_TOO_LARGE" and file.file.closed


@pytest.mark.parametrize(
    "case", ["missing", "text", "wrong_field", "duplicate", "extra_file", "rf", "chunk_size"]
)
def test_multipart_shape_rejects_extras_and_closes_all_uploads(case):
    files = [upload(), upload(filename="second.bin")]
    parts = {
        "missing": [],
        "text": [("file", "not-an-upload")],
        "wrong_field": [("other", files[0])],
        "duplicate": [("file", files[0]), ("file", files[1])],
        "extra_file": [("file", files[0]), ("other", files[1])],
        "rf": [("file", files[0]), ("replication_factor", "1")],
        "chunk_size": [("file", files[0]), ("chunk_size_bytes", "1")],
    }[case]
    try:
        with pytest.raises(FileInputError) as caught:
            with prepare_upload(FormData(parts).multi_items(), 100):
                pytest.fail("Invalid multipart reached coordinator")
        assert (caught.value.status_code, caught.value.code) == (400, "INVALID_REQUEST")
        for _, value in parts:
            if isinstance(value, UploadFile):
                assert value.file.closed
    finally:
        for file in files:
            file.file.close()


@pytest.mark.parametrize(
    "kwargs", [{"filename": None}, {"filename": "\x00"}, {"content_type": "x" * 256}]
)
def test_prepare_closes_spool_on_name_or_content_type_validation_failure(kwargs):
    file = upload(**kwargs)
    with pytest.raises(FileInputError):
        with prepare_upload([("file", file)], 100):
            pytest.fail("Invalid metadata reached coordinator")
    assert file.file.closed


def test_prepare_closes_spool_when_consumer_fails():
    file = upload()
    with pytest.raises(RuntimeError, match="cancelled"):
        with prepare_upload([("file", file)], 100):
            raise RuntimeError("cancelled")
    assert file.file.closed


def test_disk_backed_spool_is_measured_chunked_and_closed():
    spool = SpooledTemporaryFile(max_size=1, mode="w+b")
    spool.write(b"disk-backed")
    file = upload(stream=spool)
    with prepare_upload([("file", file)], 100) as prepared:
        reader = ChunkReader(prepared.stream, 4, prepared.size_bytes)
        assert b"".join(chunk.data for chunk in reader) == b"disk-backed"
        assert reader.summary.checksum_sha256 == hashlib.sha256(b"disk-backed").hexdigest()
    assert spool.closed


@pytest.mark.parametrize(
    "size", [0, 1, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1, 2 * CHUNK_SIZE, FILE_LIMIT]
)
def test_bounded_chunking_indexes_last_size_and_complete_hash(size):
    # A changing byte pattern catches ordering/data errors without random fixtures.
    source = (bytes(range(251)) * (size // 251 + 1))[:size]
    spool = BoundedSpool(source)
    reader = ChunkReader(spool, CHUNK_SIZE, size)
    assert reader.summary is None
    count = offset = 0
    for chunk in reader:
        assert chunk.chunk_index == count
        assert chunk.size_bytes == min(CHUNK_SIZE, size - offset)
        assert chunk.data == source[offset : offset + chunk.size_bytes]
        assert chunk.checksum_sha256 == hashlib.sha256(chunk.data).hexdigest()
        assert reader.summary is None
        count += 1
        offset += chunk.size_bytes
    assert count == (size + CHUNK_SIZE - 1) // CHUNK_SIZE
    assert offset == reader.summary.size_bytes == size
    assert reader.summary.total_chunks == count
    assert reader.summary.checksum_sha256 == hashlib.sha256(source).hexdigest()
    assert spool.read_sizes and max(spool.read_sizes) <= CHUNK_SIZE
    assert list(reader) == []  # no implicit rewind/reprocessing
    assert not spool.closed  # reader borrows the spool, prepare_upload owns it


def test_short_reads_fill_chunks_instead_of_creating_extra_chunks():
    source = bytes(range(55))
    spool = BoundedSpool(source, short_read=3)
    reader = ChunkReader(spool, 10, len(source))
    chunks = list(reader)
    assert [chunk.size_bytes for chunk in chunks] == [10, 10, 10, 10, 10, 5]
    assert [chunk.chunk_index for chunk in chunks] == list(range(6))
    assert b"".join(chunk.data for chunk in chunks) == source
    assert reader.summary.checksum_sha256 == hashlib.sha256(source).hexdigest()


def test_partial_consumption_does_not_publish_a_complete_file_digest():
    reader = ChunkReader(BytesIO(b"abcdef"), 2, 6)
    assert next(reader).data == b"ab"
    assert reader.summary is None


@pytest.mark.parametrize("expected_size", [0, 2, 8])
def test_changed_spool_size_cannot_publish_success_digest(expected_size):
    reader = ChunkReader(BytesIO(b"abcdef"), 2, expected_size)
    with pytest.raises(FileInputError) as caught:
        list(reader)
    assert caught.value.code == "INVALID_REQUEST"
    assert reader.summary is None


def test_spool_io_errors_are_sanitized_and_do_not_publish_digest():
    class BrokenSpool(BytesIO):
        def read(self, size=-1):
            raise OSError("private-spool-path")

        def seek(self, offset, whence=0):
            raise OSError("private-spool-path")

    with pytest.raises(FileInputError) as caught:
        measure_spool(BrokenSpool(), 100)
    assert (caught.value.status_code, caught.value.code) == (503, "TEMP_STORAGE_UNAVAILABLE")
    assert "private-spool-path" not in str(caught.value)
    reader = ChunkReader(BrokenSpool(), 2, 2)
    with pytest.raises(FileInputError) as caught:
        next(reader)
    assert caught.value.code == "TEMP_STORAGE_UNAVAILABLE" and reader.summary is None
    assert "private-spool-path" not in str(caught.value)


@pytest.mark.parametrize("chunk_size, expected_size", [(0, 1), (-1, 1), (2, -1)])
def test_reader_rejects_invalid_processing_limits(chunk_size, expected_size):
    with pytest.raises(ValueError):
        ChunkReader(BytesIO(), chunk_size, expected_size)


def test_zero_file_limit_supports_only_empty_file():
    assert measure_spool(BytesIO(), 0) == 0
    with pytest.raises(FileInputError, match="dung lượng"):
        measure_spool(BytesIO(b"x"), 0)
    with pytest.raises(ValueError):
        measure_spool(BytesIO(), -1)
