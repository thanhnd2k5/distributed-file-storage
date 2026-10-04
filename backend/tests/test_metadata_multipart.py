"""Upload ingress regressions without a database or Storage RPC dependency."""

import asyncio
from tempfile import SpooledTemporaryFile
from threading import Event
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from starlette.datastructures import Headers

from metadata.routes import files

PREFIX = b'--b\r\nContent-Disposition: form-data; name="file"; filename="x"\r\n\r\n'
SUFFIX = b"\r\n--b--\r\n"


def app(limit=4):
    application = FastAPI()
    application.state.settings = SimpleNamespace(max_file_size_bytes=limit)
    application.include_router(files.router)
    return application


@pytest.fixture
def no_coordinator(monkeypatch):
    def forbidden(*args):
        pytest.fail("Rejected or cancelled input reached the upload coordinator")

    monkeypatch.setattr(files, "upload_file", forbidden)


@pytest.mark.parametrize("charset", ["not-a-codec", "base64_codec", "hex_codec", "undefined"])
def test_invalid_multipart_charset_is_json_400_before_spool(monkeypatch, no_coordinator, charset):
    def forbidden(*args, **kwargs):
        pytest.fail("Invalid charset created a spool")

    monkeypatch.setattr("starlette.formparsers.SpooledTemporaryFile", forbidden)

    async def check():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app()), base_url="http://test"
        ) as client:
            result = await client.post(
                "/api/v1/files",
                content=PREFIX + b"x" + SUFFIX,
                headers={"content-type": f"multipart/form-data; boundary=b; charset={charset}"},
            )
            assert result.status_code == 400
            assert result.json()["error"]["code"] == "INVALID_REQUEST"

    asyncio.run(check())


@pytest.mark.parametrize("content_length", [None, "1"])
def test_oversized_stream_stops_before_eof_and_never_spools_over_limit(
    monkeypatch, no_coordinator, content_length
):
    spools, writes, consumed = [], [], []

    def create(*args, **kwargs):
        spool = SpooledTemporaryFile(*args, **kwargs)
        original = spool.write
        spools.append(spool)

        def write(data):
            writes.append(len(data))
            return original(data)

        spool.write = write
        return spool

    monkeypatch.setattr("starlette.formparsers.SpooledTemporaryFile", create)

    async def stream():
        yield PREFIX
        for block in (b"xxxx", b"x", b"tail that must not be read"):
            consumed.append(block)
            yield block
        yield SUFFIX

    async def check():
        headers = {"content-type": "multipart/form-data; boundary=b"}
        if content_length is not None:
            headers["content-length"] = content_length
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app()), base_url="http://test"
        ) as client:
            result = await client.post("/api/v1/files", content=stream(), headers=headers)
            assert result.status_code == 413
            assert result.json()["error"]["code"] == "FILE_TOO_LARGE"

    asyncio.run(check())
    assert consumed == [b"xxxx", b"x"]
    assert sum(writes) <= 4 and spools and all(spool.closed for spool in spools)


@pytest.mark.parametrize("charset", ["utf-8", "latin-1"])
@pytest.mark.parametrize("data", [b"", b"xxxx"])
def test_valid_text_charset_empty_and_exact_limit_preserve_file_bytes(charset, data):
    async def stream():
        # Fragment boundaries too, so counters cannot depend on HTTP chunks.
        body = PREFIX + data + SUFFIX
        for offset in range(0, len(body), 3):
            yield body[offset : offset + 3]

    async def check():
        form = await files.CompleteMultipartParser(
            Headers({"content-type": f"multipart/form-data; boundary=b; charset={charset}"}),
            stream(),
            file_size_limit=4,
        ).parse()
        upload = form["file"]
        assert isinstance(upload, files.OwnedUploadFile)
        assert upload.size == len(data) and await upload.read() == data
        await form.close()
        assert upload.file.closed

    asyncio.run(check())


@pytest.mark.parametrize("operation", ["write", "seek"])
def test_repeated_cancel_during_parser_io_drains_before_close(
    monkeypatch, no_coordinator, operation
):
    entered, release, finished = Event(), Event(), Event()
    spools, errors = [], []

    def create(*args, **kwargs):
        spool = SpooledTemporaryFile(*args, **kwargs)
        spool.rollover()
        original = getattr(spool, operation)
        spools.append(spool)

        def blocked(*args):
            entered.set()
            try:
                assert release.wait(4)
                return original(*args)
            except BaseException as exc:
                errors.append(type(exc).__name__)
                raise
            finally:
                finished.set()

        setattr(spool, operation, blocked)
        return spool

    monkeypatch.setattr("starlette.formparsers.SpooledTemporaryFile", create)

    async def check():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(limit=100)), base_url="http://test"
        ) as client:
            task = asyncio.create_task(
                client.post(
                    "/api/v1/files",
                    content=PREFIX + b"payload" + SUFFIX,
                    headers={"content-type": "multipart/form-data; boundary=b"},
                )
            )
            try:
                assert await asyncio.to_thread(entered.wait, 2)
                task.cancel()
                await asyncio.sleep(0.02)
                task.cancel()
                await asyncio.sleep(0.02)
                assert not task.done() and not spools[0].closed and not finished.is_set()
            finally:
                release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 3)
            assert finished.is_set() and spools[0].closed and errors == []

    asyncio.run(check())


def test_cancel_during_owned_close_waits_for_file_to_close():
    entered, release = Event(), Event()
    spool = SpooledTemporaryFile()
    original = spool.close

    def close():
        entered.set()
        assert release.wait(4)
        original()

    spool.close = close
    upload = files.OwnedUploadFile(spool)

    async def check():
        task = asyncio.create_task(upload.close())
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            await asyncio.sleep(0.02)
            assert not task.done() and not spool.closed
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
        assert spool.closed

    asyncio.run(check())
