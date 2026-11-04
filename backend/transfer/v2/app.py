"""No file spool, automatic stream retry, or database access in Transfer."""

import asyncio
import hashlib
import re
from contextlib import asynccontextmanager
from dataclasses import asdict
from uuid import uuid4

import grpc
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc
from fastapi import FastAPI, Request, Response
from starlette.requests import ClientDisconnect

from metadata.v2.http import JsonClient, handlers, integer, part_result, uuid_value
from metadata.v2.repository import DomainError
from metadata.v2.runtime import Admission, Limits
from metadata.v2.storage_client import evidence, key_for, spec_for
from transfer.v2.download import DownloadResponse, Downloads
from transfer.v2.download_http import plan_download


def destination(d):
    result = dict(d)
    for k in ("namespace_id", "recovery_epoch", "incarnation", "allocation_id", "chunk_id"):
        result[k] = uuid_value(result[k])
    return result


async def together(coroutines):
    """Fail fast and drain siblings, including producers blocked by backpressure."""
    tasks = [asyncio.create_task(c) for c in coroutines]
    try:
        return await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class Transfer:
    def __init__(self, api: JsonClient, limits: Limits):
        self.api, self.limits = api, limits
        self.active = set()
        self.stopping = False
        self.downloads = Downloads(api, limits)

    async def ready(self):
        if self.stopping:
            raise DomainError("STORAGE_UNAVAILABLE")
        config = await self.api.call("GET", "/api/v2/config")
        await self.api.call("GET", "/health/ready", internal=True)
        if config["replication_factor"] > self.limits.max_replicas:
            raise ValueError("Replication policy exceeds Transfer buffer reservation")
        if config["chunk_size_bytes"] > self.limits.download_chunk_bytes:
            raise ValueError("Chunk policy exceeds Transfer download reservation")
        return True

    async def consume(self, request, expected, sha, queues, body_done):
        total, digest = 0, hashlib.sha256()
        async for incoming in request.stream():
            for start in range(0, len(incoming), self.limits.block_bytes):
                block = incoming[start : start + self.limits.block_bytes]
                total += len(block)
                if total > expected:
                    raise DomainError("PART_TOO_LARGE")
                digest.update(block)
                # The slowest replica bounds consumption of the HTTP body.
                for queue in queues:
                    await queue.put(block)
        body_done.set()
        if total != expected:
            raise DomainError("PART_SIZE_MISMATCH")
        if digest.hexdigest() != sha:
            raise DomainError("CHECKSUM_MISMATCH")
        # Clean RPC EOF is permitted only after HTTP EOF and actual hash match.
        for queue in queues:
            await queue.put(None)

    async def store(self, d, queue, deadline):
        options = [
            ("grpc.max_receive_message_length", 300000),
            ("grpc.max_send_message_length", 300000),
        ]
        async with grpc.aio.insecure_channel(
            f"{d['host']}:{d['port']}", options=options
        ) as channel:
            stub = rpc.StorageServiceStub(channel)
            # Outer HTTP deadline bounds all RPCs; individual RPCs never exceed it.
            prepared = await stub.PrepareAllocation(
                pb.PrepareAllocationRequest(
                    key=key_for(d), generation=d["generation"], chunk=spec_for(d)
                ),
                timeout=max(0.001, deadline - asyncio.get_running_loop().time()),
            )
            if evidence(prepared, d).state not in ("PREPARED", "STORED"):
                raise DomainError("INVALID_STORAGE_ACK")

            async def frames():
                yield pb.StoreChunkFrame(
                    header=pb.StoreChunkHeader(
                        key=key_for(d), generation=d["generation"], chunk=spec_for(d)
                    )
                )
                offset = 0
                while (block := await queue.get()) is not None:
                    yield pb.StoreChunkFrame(block=pb.DataBlock(offset=offset, data=block))
                    offset += len(block)

            ack = await stub.StoreChunk(
                frames(), timeout=max(0.001, deadline - asyncio.get_running_loop().time())
            )
            proof = evidence(ack.allocation, d)
            if proof.state != "STORED":
                raise DomainError("INVALID_STORAGE_ACK")
            return asdict(proof)

    async def put(self, request, upload, index):
        if self.stopping:
            raise DomainError("STORAGE_UNAVAILABLE")
        if len(self.active) >= self.limits.streams:
            raise DomainError("RESOURCE_LIMIT")
        current = asyncio.current_task()
        self.active.add(current)
        claim, path = None, f"/internal/v2/uploads/{upload}/parts/{index}"
        confirmed = False
        tasks = []
        try:
            async with asyncio.timeout(self.limits.request_seconds):
                deadline = asyncio.get_running_loop().time() + self.limits.request_seconds
                if (
                    request.headers.get("content-type", "").split(";")[0].strip().lower()
                    != "application/octet-stream"
                ):
                    raise DomainError("INVALID_REQUEST")
                sha = request.headers.get("x-part-sha256", "")
                if not re.fullmatch(r"[0-9a-f]{64}", sha):
                    raise DomainError("VALIDATION_ERROR")
                session = await self.api.call("GET", f"/api/v2/uploads/{upload}")
                if session["state"] == "ABORTED":
                    raise DomainError("UPLOAD_ABORTED")
                if session["state"] == "EXPIRED":
                    raise DomainError("UPLOAD_EXPIRED")
                if index >= session["part_count"]:
                    raise DomainError("VALIDATION_ERROR")
                if session["replication_factor"] > self.limits.max_replicas:
                    raise DomainError("RESOURCE_LIMIT")
                expected = min(
                    session["chunk_size_bytes"],
                    session["size_bytes"] - index * session["chunk_size_bytes"],
                )
                length = request.headers.get("content-length")
                if length is not None and integer(length) != expected:
                    raise DomainError("PART_SIZE_MISMATCH")
                claim = await self.api.call(
                    "POST",
                    path + "/claim",
                    dict(size_bytes=expected, checksum_sha256=sha, request_id=str(uuid4())),
                    internal=True,
                )
                verified = claim["state"] == "VERIFIED"
                if not verified and not 1 <= len(claim["destinations"]) <= self.limits.max_replicas:
                    raise DomainError("INVALID_STORAGE_ACK")
                done = asyncio.Event()

                async def renew():
                    while True:
                        await asyncio.sleep(self.limits.renew_seconds)
                        await self.api.call(
                            "POST",
                            path + "/renew",
                            dict(claim_token=claim["claim_token"], revision=claim["revision"]),
                            internal=True,
                        )

                async def disconnected():
                    await done.wait()  # Only receive after the body consumer reached EOF.
                    while True:
                        message = await request.receive()
                        if message["type"] == "http.disconnect":
                            raise ClientDisconnect()

                async def flow():
                    if verified:
                        await self.consume(request, expected, sha, [], done)
                        return part_result(claim)
                    queues = [
                        asyncio.Queue(self.limits.queue_blocks) for _ in claim["destinations"]
                    ]
                    results = await together(
                        [
                            self.consume(request, expected, sha, queues, done),
                            *(
                                self.store(destination(d), queue, deadline)
                                for d, queue in zip(claim["destinations"], queues)
                            ),
                        ]
                    )
                    return await self.api.call(
                        "POST",
                        path + "/confirm",
                        dict(
                            claim_token=claim["claim_token"],
                            revision=claim["revision"],
                            allocations=results[1:],
                        ),
                        internal=True,
                    )

                main = asyncio.create_task(flow())
                tasks = [main, asyncio.create_task(disconnected())]
                if not verified:
                    tasks.append(asyncio.create_task(renew()))
                finished, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                # Prefer a lost lease/disconnect over a coincident success.
                for task in finished:
                    if task != main:
                        task.result()
                result = main.result()
                confirmed = True
                return result
        except TimeoutError as exc:
            raise DomainError("STORAGE_UNAVAILABLE") from exc
        except (grpc.RpcError, ClientDisconnect) as exc:
            raise DomainError("STORAGE_UNAVAILABLE") from exc
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if claim is not None and claim["state"] != "VERIFIED" and not confirmed:
                # Idempotent on a successful confirmation too. Never infer that
                # cancelled Store or an HTTP timeout means bytes did not commit.
                try:
                    async with asyncio.timeout(min(2, self.limits.shutdown_seconds / 2)):
                        await self.api.call(
                            "POST",
                            path + "/uncertain",
                            dict(
                                claim_token=claim["claim_token"],
                                revision=claim["revision"],
                                allocation_ids=[d["allocation_id"] for d in claim["destinations"]],
                                reason_code="TRANSFER_ENDED",
                            ),
                            internal=True,
                            timeout=min(2, self.limits.shutdown_seconds / 2),
                        )
                except (Exception, asyncio.CancelledError):
                    pass  # Claim sweeper remains the durable recovery mechanism.
            self.active.discard(current)

    async def close(self):
        self.stopping = True
        self.downloads.stopping = True
        active = list(self.active | self.downloads.active)
        for task in active:
            if not task.cancelling():
                task.cancel()
        if active:
            _, pending = await asyncio.wait(active, timeout=self.limits.shutdown_seconds)
            if pending:
                raise RuntimeError("Transfer failed to drain")


def create_app(api, *, limits=None):
    limits = limits or Limits()
    transfer = Transfer(api, limits)

    @asynccontextmanager
    async def lifespan(app):
        await transfer.ready()
        yield
        await transfer.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.transfer = transfer
    handlers(app)
    app.add_middleware(Admission, limit=limits.requests, reserve=limits.request_reserve)

    @app.get("/health/ready")
    async def ready():
        await transfer.ready()
        return {"ready": True}

    @app.get("/health/live")
    async def live():
        return {"live": True}

    @app.put("/api/v2/uploads/{upload}/parts/{index}")
    async def put(upload: str, index: str, request: Request):
        return await transfer.put(request, str(uuid_value(upload)), integer(index))

    @app.head("/api/v2/files/{file}/download")
    async def head(file: str, request: Request):
        if transfer.stopping:
            raise DomainError("STORAGE_UNAVAILABLE")
        if request.query_params:
            raise ValueError("Unknown query")
        file = str(uuid_value(file))
        metadata = await api.call("GET", f"/api/v2/files/{file}")
        if metadata["state"] != "AVAILABLE":
            raise DomainError("FILE_NOT_READY")

        def tags(name):
            values = request.headers.getlist(name)
            return ",".join(values) if values else None

        plan = plan_download(
            method="HEAD",
            size_bytes=metadata["size_bytes"],
            checksum_sha256=metadata["checksum_sha256"],
            original_name=metadata["original_name"],
            if_match=tags("if-match"),
            if_none_match=tags("if-none-match"),
        )
        return Response(status_code=plan.status_code, headers=plan.headers)

    @app.get("/api/v2/files/{file}/download")
    async def get(file: str, request: Request):
        if transfer.stopping:
            raise DomainError("STORAGE_UNAVAILABLE")
        if request.query_params:
            raise ValueError("Unknown query")
        return DownloadResponse(transfer.downloads, request, str(uuid_value(file)))

    return app
