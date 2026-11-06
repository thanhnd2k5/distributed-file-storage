"""One admitted chunk buffer, sequential replica attempts, verified bytes only."""

import asyncio
import hashlib
from datetime import datetime

import grpc
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc
from fastapi import Response
from starlette.requests import ClientDisconnect

from metadata.v2.http import error_response
from metadata.v2.repository import DomainError
from metadata.v2.storage_client import evidence, key_for, spec_for
from transfer.v2.download_http import plan_download


def clock():
    return asyncio.get_running_loop().time()


class Downloads:
    def __init__(self, api, limits):
        self.api, self.limits = api, limits
        self.active = set()
        self.stopping = False

    async def read_chunk(self, part, buffer):
        destinations = part["destinations"]
        if len(destinations) > self.limits.max_replicas:
            raise DomainError("METADATA_UNAVAILABLE")
        # No retry of the same destination and no concurrent replica reads.
        for d in destinations:
            if any(d[k] != part[k] for k in ("chunk_id", "size_bytes", "checksum_sha256")):
                raise DomainError("METADATA_UNAVAILABLE")
            try:
                await self.read_replica(d, buffer)
                return
            except (grpc.RpcError, DomainError):
                continue
        raise DomainError("CHUNK_UNAVAILABLE")

    async def read_replica(self, d, buffer):
        async with grpc.aio.insecure_channel(
            f"{d['host']}:{d['port']}",
            options=[("grpc.max_receive_message_length", 300000)],
        ) as channel:
            call = rpc.StorageServiceStub(channel).GetChunk(
                pb.GetChunkRequest(key=key_for(d), expected_chunk=spec_for(d)),
                timeout=self.limits.download_rpc_seconds,
            )
            total, digest, header, footer = 0, hashlib.sha256(), False, False
            try:
                async for frame in call:
                    kind = frame.WhichOneof("payload")
                    if kind == "header" and not header and not footer:
                        if evidence(frame.header, d).state != "STORED":
                            raise DomainError("INVALID_STORAGE_STREAM")
                        header = True
                    elif kind == "block" and header and not footer:
                        block = frame.block
                        if (
                            block.offset != total
                            or not 0 < len(block.data) <= 262144
                            or total + len(block.data) > d["size_bytes"]
                        ):
                            raise DomainError("INVALID_STORAGE_STREAM")
                        buffer[total : total + len(block.data)] = block.data
                        digest.update(block.data)
                        total += len(block.data)
                    elif kind == "footer" and header and not footer:
                        if (total, digest.hexdigest()) != (
                            d["size_bytes"],
                            d["checksum_sha256"],
                        ) or (
                            frame.footer.actual_size_bytes,
                            frame.footer.actual_checksum_sha256,
                        ) != (total, digest.hexdigest()):
                            raise DomainError("INVALID_STORAGE_STREAM")
                        footer = True
                    else:
                        raise DomainError("INVALID_STORAGE_STREAM")
                # Iteration must finish with OK, not just a convincing footer.
                if not header or not footer:
                    raise DomainError("INVALID_STORAGE_STREAM")
            finally:
                call.cancel()
                await channel.close()


class DownloadResponse(Response):
    def __init__(self, downloads, request, file):
        super().__init__()
        self.downloads, self.request, self.file = downloads, request, file
        self.started = False
        self.lease = None
        self.deadline = 0

    def check(self):
        if clock() >= self.deadline:
            raise DomainError("STORAGE_UNAVAILABLE")

    def update_lease(self, lease, started):
        if self.lease is not None and any(
            lease[k] != self.lease[k] for k in ("read_id", "lease_token", "file_id", "version_id")
        ):
            raise DomainError("METADATA_UNAVAILABLE")
        # Subtract the entire round trip. No comparison of wall clocks on hosts.
        expiry = min(
            datetime.fromisoformat(lease[k]) for k in ("expires_at", "absolute_expires_at")
        )
        ttl = (expiry - datetime.fromisoformat(lease["db_now"])).total_seconds()
        self.deadline = started + ttl - 0.1
        self.check()

    async def renew(self):
        limits, api = self.downloads.limits, self.downloads.api
        while True:
            # This also cancels a blocked renewal before the old lease expires.
            async with asyncio.timeout_at(self.deadline):
                await asyncio.sleep(limits.download_renew_seconds)
                started = clock()
                lease = await api.call(
                    "POST",
                    f"/internal/v2/downloads/{self.lease['read_id']}/renew",
                    dict(lease_token=self.lease["lease_token"]),
                    internal=True,
                    timeout=min(limits.control_seconds, self.deadline - clock()),
                )
                self.update_lease(lease, started)

    async def stream(self, plan, send):
        limits, api, lease = self.downloads.limits, self.downloads.api, self.lease
        chunk_size, size = lease["chunk_size_bytes"], lease["size_bytes"]
        if not 0 < chunk_size <= limits.download_chunk_bytes:
            raise DomainError("RESOURCE_LIMIT")
        if lease["part_count"] != (size + chunk_size - 1) // chunk_size:
            raise DomainError("METADATA_UNAVAILABLE")
        # Allocated only after a stream slot (and its byte/RPC reservation) exists.
        buffer = bytearray(chunk_size) if size else bytearray()

        async def start():
            self.check()
            self.started = True
            await send(
                {
                    "type": "http.response.start",
                    "status": plan.status_code,
                    "headers": [
                        (k.lower().encode(), v.encode("latin-1")) for k, v in plan.headers.items()
                    ],
                }
            )

        if plan.start == plan.stop:
            await start()
        else:
            first, last = plan.start // chunk_size, (plan.stop - 1) // chunk_size
            index = first
            while index <= last:
                self.check()
                # One item per page bounds both metadata memory and range lookup.
                page = await api.call(
                    "GET",
                    f"/internal/v2/downloads/{lease['read_id']}/manifest"
                    f"?after_index={index - 1}&limit=1",
                    internal=True,
                    headers={"X-Read-Lease-Token": lease["lease_token"]},
                )
                if page["version_id"] != lease["version_id"] or len(page["items"]) != 1:
                    raise DomainError("METADATA_UNAVAILABLE")
                part = page["items"][0]
                expected = min(chunk_size, size - index * chunk_size)
                if part["part_index"] != index or part["size_bytes"] != expected:
                    raise DomainError("METADATA_UNAVAILABLE")
                self.check()
                await self.downloads.read_chunk(part, buffer)
                self.check()
                if not self.started:
                    await start()
                low = max(0, plan.start - index * chunk_size)
                high = min(expected, plan.stop - index * chunk_size)
                # Backpressure completes every send before fetching the next chunk.
                for offset in range(low, high, limits.block_bytes):
                    self.check()
                    await send(
                        {
                            "type": "http.response.body",
                            "body": memoryview(buffer)[
                                offset : min(high, offset + limits.block_bytes)
                            ],
                            "more_body": True,
                        }
                    )
                index += 1
        self.check()
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def flow(self, scope, receive, send):
        api = self.downloads.api
        metadata = await api.call("GET", f"/api/v2/files/{self.file}")
        if metadata["state"] != "AVAILABLE":
            raise DomainError("FILE_NOT_READY")

        def tags(name):
            values = self.request.headers.getlist(name)
            return ",".join(values) if values else None

        plan = plan_download(
            method="GET",
            size_bytes=metadata["size_bytes"],
            checksum_sha256=metadata["checksum_sha256"],
            original_name=metadata["original_name"],
            if_match=tags("if-match"),
            if_none_match=tags("if-none-match"),
            range_header=tags("range"),
            if_range=tags("if-range"),
        )
        if not plan.requires_lease:
            response = (
                error_response(plan.error_code, plan.status_code)
                if plan.error_code
                else Response(status_code=plan.status_code)
            )
            response.headers.update(plan.headers)
            return await response(scope, receive, send)
        tasks = []
        try:
            started = clock()
            self.lease = await api.call(
                "POST",
                "/internal/v2/downloads",
                dict(file_id=self.file, version_id=metadata["version_id"]),
                internal=True,
            )
            self.update_lease(self.lease, started)
            if any(
                self.lease[k] != metadata[k]
                for k in ("file_id", "version_id", "size_bytes", "checksum_sha256", "original_name")
            ):
                raise DomainError("METADATA_UNAVAILABLE")
            body = asyncio.create_task(self.stream(plan, send))
            renew = asyncio.create_task(self.renew())
            tasks = [body, renew]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if renew in done:
                renew.result()
            return body.result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def __call__(self, scope, receive, send):
        downloads = self.downloads
        if downloads.stopping:
            return await error_response("STORAGE_UNAVAILABLE")(scope, receive, send)
        if len(downloads.active) >= downloads.limits.download_streams:
            return await error_response("RESOURCE_LIMIT")(scope, receive, send)
        current = asyncio.current_task()
        downloads.active.add(current)

        async def disconnected():
            while True:
                if (await receive())["type"] == "http.disconnect":
                    raise ClientDisconnect()

        tasks = []
        try:
            async with asyncio.timeout(downloads.limits.download_seconds):
                body = asyncio.create_task(self.flow(scope, receive, send))
                disconnect = asyncio.create_task(disconnected())
                tasks = [body, disconnect]
                try:
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    if disconnect in done:
                        disconnect.result()
                    body.result()
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        except ClientDisconnect:
            pass
        except (DomainError, TimeoutError, KeyError, ValueError, TypeError) as exc:
            if self.started:
                # Raising closes the transport; never JSON/padding after binary headers.
                raise RuntimeError("Download interrupted") from None
            code = (
                exc.code
                if isinstance(exc, DomainError)
                else (
                    "STORAGE_UNAVAILABLE"
                    if isinstance(exc, TimeoutError)
                    else "METADATA_UNAVAILABLE"
                )
            )
            if code in ("STALE_READ_LEASE", "READ_LEASE_NOT_FOUND"):
                code = "STORAGE_UNAVAILABLE"
            await error_response(code)(scope, receive, send)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # Uvicorn also reports disconnect after a completed response. Release
            # only after that watcher is drained, so it cannot cancel cleanup.
            if self.lease is not None:
                try:
                    async with asyncio.timeout(2):
                        await downloads.api.call(
                            "POST",
                            f"/internal/v2/downloads/{self.lease['read_id']}/release",
                            dict(lease_token=self.lease["lease_token"]),
                            internal=True,
                            timeout=2,
                        )
                except (Exception, asyncio.CancelledError):
                    pass  # DB expiry is the durable cleanup guard.
            downloads.active.discard(current)
