"""Pure V2 download selection; no leases, storage I/O or response streaming.

Call only after resolving an AVAILABLE immutable version. The HTTP adapter must
acquire its read lease before serving a successful GET, including an empty file,
and suppress every response body for HEAD. Error bodies use the V2 JSON envelope;
their length must not be taken from the selected binary representation.
"""

import re
import unicodedata
from dataclasses import dataclass, replace
from urllib.parse import quote

_TAG = re.compile(r'(W/)?("[\x21\x23-\x7e\x80-\xff]*")')
_RANGE = re.compile(r"([!#$%&'*+.^_`|~0-9A-Za-z-]+)=(.+)")
_BYTES = re.compile(r"([0-9]*)-([0-9]*)")


@dataclass(frozen=True)
class DownloadPlan:
    method: str
    size_bytes: int
    etag: str
    disposition: str
    status_code: int = 200
    start: int = 0
    stop: int = 0  # Exclusive; suitable for slicing a verified chunk.
    error_code: str | None = None

    @property
    def requires_lease(self):
        return self.method == "GET" and self.status_code in (200, 206)

    @property
    def headers(self):
        headers = {"ETag": self.etag, "Accept-Ranges": "bytes"}
        if self.status_code in (200, 206):
            headers.update(
                {
                    "Content-Length": str(self.stop - self.start),
                    "Content-Type": "application/octet-stream",
                    "Content-Disposition": self.disposition,
                }
            )
        if self.status_code == 206:
            headers["Content-Range"] = f"bytes {self.start}-{self.stop - 1}/{self.size_bytes}"
        elif self.status_code == 416:
            headers["Content-Range"] = f"bytes */{self.size_bytes}"
        return headers


def attachment(original_name):
    if (
        not isinstance(original_name, str)
        or not 1 <= len(original_name) <= 255
        or any(c in "/\\" or unicodedata.category(c) in ("Cc", "Cs") for c in original_name)
    ):
        raise ValueError("Invalid download basename")
    return "attachment; filename*=UTF-8''" + quote(original_name, safe="")


def _matches(value, etag, *, strong):
    value = value.strip(" \t")
    if value == "*":
        return True  # The caller has already resolved an existing representation.
    position, matched, count = 0, False, 0
    while position < len(value):
        # HTTP list syntax permits empty members; commas inside tags are opaque.
        while position < len(value) and value[position] in " \t,":
            position += 1
        if position == len(value):
            break
        tag = _TAG.match(value, position)
        if tag is None:
            raise ValueError("Invalid entity-tag list")
        weak, opaque = tag.groups()
        matched |= opaque == etag and (not strong or not weak)
        count += 1
        position = tag.end()
        while position < len(value) and value[position] in " \t":
            position += 1
        if position < len(value) and value[position] != ",":
            raise ValueError("Invalid entity-tag separator")
    if not count:
        raise ValueError("Empty entity-tag list")
    return matched


def _decimal(value):
    return value.lstrip("0") or "0"


def _less(left, right):
    # Never int() an unbounded Range numeral: RFC 9110 allows very long digits.
    return (len(left), left) < (len(right), right)


def _select_range(value, size):
    match = _RANGE.fullmatch(value.strip(" \t"))
    if match is None:
        raise ValueError("Invalid range specifier")
    unit, ranges = match.groups()
    if unit.lower() != "bytes":
        return None
    selected, count = None, 0
    for item in ranges.split(","):
        item = item.strip(" \t")
        if not item:
            continue
        part = _BYTES.fullmatch(item)
        if part is None or not any(part.groups()):
            raise ValueError("Invalid byte range")
        start, end = part.groups()
        start = _decimal(start) if start else None
        end = _decimal(end) if end else None
        if start is not None and end is not None and _less(end, start):
            raise ValueError("Range ends before its start")
        selected = start, end
        count += 1
    if not count:
        raise ValueError("Empty range set")
    if count > 1:
        return None  # Valid multi-range is deliberately served as full 200.
    start, end = selected
    length = str(size)
    if start is None:
        if end == "0":
            return 0, 0  # Unsatisfiable, including on an empty representation.
        if not size:
            return None  # Contract: positive suffix of empty file is full 200.
        return (size - int(end) if _less(end, length) else 0), size
    if not _less(start, length):
        return 0, 0
    stop = int(end) + 1 if end is not None and _less(end, length) else size
    return int(start), stop


def plan_download(
    *,
    method,
    size_bytes,
    checksum_sha256,
    original_name,
    range_header=None,
    if_range=None,
    if_match=None,
    if_none_match=None,
):
    """Select status, binary headers and [start, stop) under the V2 contract.

    The adapter combines repeated list field lines before calling. Header names
    are resolved by the HTTP framework. Date validators are deliberately absent.
    Invalid representation metadata is a caller error, not a client HTTP error.
    """
    if method not in ("GET", "HEAD"):
        raise ValueError("Download requires GET or HEAD")
    if type(size_bytes) is not int or not 0 <= size_bytes <= 2**63 - 1:
        raise ValueError("Invalid representation size")
    if not isinstance(checksum_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum_sha256):
        raise ValueError("Invalid representation SHA-256")
    plan = DownloadPlan(
        method,
        size_bytes,
        f'"sha256-{checksum_sha256}"',
        attachment(original_name),
        stop=size_bytes,
    )

    def failure(status, code=None):
        return replace(plan, status_code=status, start=0, stop=0, error_code=code)

    try:
        if if_match is not None and not _matches(if_match, plan.etag, strong=True):
            return failure(412, "PRECONDITION_FAILED")
        if if_none_match is not None and _matches(if_none_match, plan.etag, strong=False):
            return failure(304)
    except ValueError:
        return failure(400, "INVALID_REQUEST")

    # Preconditions precede Range parsing. HEAD ignores Range and If-Range, even
    # malformed ones. An unsupported/mismatched If-Range selects the full file.
    if method == "HEAD" or range_header is None:
        return plan
    if if_range is not None and if_range.strip(" \t") != plan.etag:
        return plan
    try:
        interval = _select_range(range_header, size_bytes)
    except ValueError:
        return failure(400, "INVALID_RANGE")
    if interval is None:
        return plan
    start, stop = interval
    if start == stop:
        return failure(416, "RANGE_NOT_SATISFIABLE")
    return replace(plan, status_code=206, start=start, stop=stop)
