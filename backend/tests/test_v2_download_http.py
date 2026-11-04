"""D1 contract cases; pure selection with actual byte slices, no services."""

from urllib.parse import unquote

import pytest

from transfer.v2.download_http import attachment, plan_download

SHA = "a" * 64
ETAG = f'"sha256-{SHA}"'
DATA = b"0123456789abcdef"


def select(**values):
    options = dict(
        method="GET",
        size_bytes=len(DATA),
        checksum_sha256=SHA,
        original_name="sample.bin",
    )
    options.update(values)
    return plan_download(**options)


@pytest.mark.parametrize(
    "header,expected",
    [
        ("bytes=0-0", DATA[:1]),
        ("bytes=0-7", DATA[:8]),
        ("bytes=7-8", DATA[7:9]),
        ("bytes=8-15", DATA[8:]),
        ("bytes=15-100", DATA[-1:]),
        ("bytes=8-", DATA[8:]),
        ("bytes=-3", DATA[-3:]),
        ("bytes=-16", DATA),
        ("bytes=-100", DATA),
        ("bytes=0-100", DATA),
        ("BYTES=0007-0008", DATA[7:9]),
        (" \tbytes=, 7-8, ,\t", DATA[7:9]),
    ],
)
def test_single_ranges_select_exact_bytes_and_headers(header, expected):
    plan = select(range_header=header)
    assert plan.status_code == 206
    assert DATA[plan.start : plan.stop] == expected
    assert plan.headers["Content-Range"] == f"bytes {plan.start}-{plan.stop - 1}/16"
    assert plan.headers["Content-Length"] == str(len(expected))
    assert plan.headers["Accept-Ranges"] == "bytes"
    assert plan.headers["ETag"] == ETAG
    assert plan.headers["Content-Type"] == "application/octet-stream"
    assert "Content-Encoding" not in plan.headers
    assert "Last-Modified" not in plan.headers
    assert plan.requires_lease


@pytest.mark.parametrize("header", [None, "items=0-1", "bytes=0-1, 4-7", "bytes=999-, -0"])
def test_full_and_ignored_ranges(header):
    plan = select(range_header=header)
    assert plan.status_code == 200
    assert (plan.start, plan.stop) == (0, len(DATA))
    assert plan.headers["Content-Length"] == "16"
    assert "Content-Range" not in plan.headers
    assert plan.requires_lease


@pytest.mark.parametrize("header", ["bytes=16-", "bytes=20-21", "bytes=-0", "bytes=-000"])
def test_unsatisfiable_ranges_report_representation_length(header):
    plan = select(range_header=header)
    assert (plan.status_code, plan.error_code) == (416, "RANGE_NOT_SATISFIABLE")
    assert plan.headers["Content-Range"] == "bytes */16"
    assert "Content-Length" not in plan.headers  # JSON error length belongs to the adapter.
    assert not plan.requires_lease


@pytest.mark.parametrize(
    "header",
    [
        "",
        "bytes",
        "bytes=",
        "bytes=, ,",
        "bytes=-",
        "bytes=1",
        "bytes=8-7",
        "bytes=+1-2",
        "bytes=1.0-2",
        "bytes=1--2",
        "bytes=1-2-3",
        "bytes=١-٢",
        "bytes=1 - 2",
        "bytes =1-2",
        "bytes=0-1, nope",
        "bytes=0-1, 8-7",
        "bytes=999-1",
        "bytes=0-1\n",
        "bytes=0-1\r\nX-Injected: yes",
    ],
)
def test_malformed_range_is_400_even_inside_multirange(header):
    plan = select(range_header=header)
    assert (plan.status_code, plan.error_code) == (400, "INVALID_RANGE")
    assert "Content-Range" not in plan.headers
    assert "Content-Length" not in plan.headers
    assert not plan.requires_lease


@pytest.mark.parametrize(
    "header,status",
    [
        (None, 200),
        ("bytes=-1", 200),
        ("bytes=-0", 416),
        ("bytes=0-", 416),
        ("bytes=0-0", 416),
        ("bytes=1-0", 400),
    ],
)
def test_empty_representation_contract(header, status):
    plan = select(size_bytes=0, range_header=header)
    assert plan.status_code == status
    assert (plan.start, plan.stop) == (0, 0)
    if status == 200:
        assert plan.headers["Content-Length"] == "0"
        assert plan.requires_lease  # GET still needs a version lease, even for empty files.
    else:
        assert not plan.requires_lease
    if status == 416:
        assert plan.headers["Content-Range"] == "bytes */0"


@pytest.mark.parametrize("header", [None, "bytes=0-1", "bytes=garbage", "bytes=999-"])
def test_head_ignores_range_and_if_range_without_acquiring_lease(header):
    plan = select(method="HEAD", range_header=header, if_range='W/"invalid')
    assert plan.status_code == 200
    assert plan.headers["Content-Length"] == "16"
    assert "Content-Range" not in plan.headers
    assert not plan.requires_lease


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize(
    "conditions,status",
    [
        ({"if_match": ETAG}, 200),
        ({"if_match": "*"}, 200),
        ({"if_match": f'"other", {ETAG}'}, 200),
        ({"if_match": f"W/{ETAG}"}, 412),
        ({"if_match": '"other"'}, 412),
        ({"if_none_match": ETAG}, 304),
        ({"if_none_match": f"W/{ETAG}"}, 304),
        ({"if_none_match": "*"}, 304),
        ({"if_none_match": f'"other,tag", {ETAG}'}, 304),
        ({"if_none_match": '"other"'}, 200),
        ({"if_match": '"other"', "if_none_match": "*"}, 412),
        ({"if_match": ETAG, "if_none_match": f"W/{ETAG}"}, 304),
    ],
)
def test_strong_if_match_weak_if_none_match_and_priority(method, conditions, status):
    plan = select(method=method, **conditions)
    assert plan.status_code == status
    assert plan.headers["ETag"] == ETAG
    assert plan.requires_lease == (status == 200 and method == "GET")
    if status in (304, 412):
        assert "Content-Length" not in plan.headers
        assert "Content-Range" not in plan.headers
        assert plan.error_code == ("PRECONDITION_FAILED" if status == 412 else None)


@pytest.mark.parametrize("condition", ["if_match", "if_none_match"])
@pytest.mark.parametrize(
    "value",
    [
        "",
        " , ",
        "unquoted",
        'w/"tag"',
        '"space tag"',
        '"unfinished',
        '"tag" junk',
        '*, "tag"',
        '"tag" "other"',
        '"tag\r\n"',
    ],
)
def test_invalid_entity_tag_list_is_request_error(condition, value):
    plan = select(**{condition: value}, range_header="bytes=0-1")
    assert (plan.status_code, plan.error_code) == (400, "INVALID_REQUEST")
    assert not plan.requires_lease


def test_entity_tags_are_opaque_and_entire_list_is_validated():
    # A quote is never backslash-escaped in an entity-tag. Commas may be literal.
    assert select(if_match=f'"literal\\", "comma,inside", {ETAG}').status_code == 200
    assert select(if_match=f", ,{ETAG}, ,").status_code == 200
    assert select(if_match=f"{ETAG}, invalid").status_code == 400
    assert select(if_match=f'"{SHA.upper()}"').status_code == 412


@pytest.mark.parametrize(
    "conditions,status",
    [
        ({"if_match": '"other"'}, 412),
        ({"if_none_match": "*"}, 304),
        ({"if_match": ETAG, "if_none_match": ETAG}, 304),
        ({"if_match": '"other"', "if_none_match": "invalid"}, 412),
        ({"if_range": f"W/{ETAG}"}, 200),
        ({"if_range": '"other"'}, 200),
        ({"if_range": "Wed, 21 Oct 2015 07:28:00 GMT"}, 200),
    ],
)
def test_preconditions_and_if_range_are_decided_before_parsing_range(conditions, status):
    assert select(range_header="malformed", **conditions).status_code == status


def test_if_range_requires_exact_strong_tag_and_does_not_act_without_range():
    assert select(range_header="bytes=8-", if_range=ETAG).status_code == 206
    assert select(range_header="bytes=8-", if_range=f" \t{ETAG} ").status_code == 206
    assert select(range_header="bad", if_range=ETAG).error_code == "INVALID_RANGE"
    assert select(if_range="anything").status_code == 200


def test_huge_range_numerals_have_no_overflow_or_python_digit_limit():
    huge = "9" * 5000
    assert select(range_header=f"bytes={huge}-").status_code == 416
    plan = select(range_header=f"bytes=1-{huge}")
    assert (plan.start, plan.stop, plan.status_code) == (1, len(DATA), 206)
    assert select(range_header=f"bytes=-{huge}").start == 0
    assert select(range_header=f"bytes={huge}-{huge[:-1]}").error_code == "INVALID_RANGE"
    assert select(range_header=f"bytes={'0' * 5000}1-2").start == 1


def test_large_representation_offsets_remain_exact():
    size = 2**63 - 1
    plan = select(size_bytes=size, range_header=f"bytes={size - 2}-")
    assert (plan.start, plan.stop) == (size - 2, size)
    assert plan.headers["Content-Length"] == "2"
    assert plan.headers["Content-Range"] == f"bytes {size - 2}-{size - 1}/{size}"


@pytest.mark.parametrize("name", ['báo cáo "Q1"; %.txt', "安全.bin", "report.bin"])
def test_attachment_is_ascii_header_with_exact_utf8_name(name):
    header = select(original_name=name).headers["Content-Disposition"]
    assert header.startswith("attachment; filename*=UTF-8''")
    assert unquote(header.split("''", 1)[1]) == name
    assert all(32 <= ord(char) < 127 for char in header)
    assert len(header.split(";")) == 2


@pytest.mark.parametrize("name", ["", "x" * 256, "../bad", "a\\b", "a\r\nb", "a\x00b", "a\x85b"])
def test_invalid_basename_is_never_put_into_a_header(name):
    with pytest.raises(ValueError, match="basename"):
        attachment(name)


@pytest.mark.parametrize(
    "values",
    [
        {"method": "POST"},
        {"size_bytes": True},
        {"size_bytes": -1},
        {"size_bytes": 2**63},
        {"size_bytes": 1.0},
        {"checksum_sha256": "A" * 64},
        {"checksum_sha256": SHA + "\n"},
    ],
)
def test_invalid_representation_metadata_is_a_caller_error(values):
    with pytest.raises(ValueError):
        select(**values)
