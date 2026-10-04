"""Drain owned transfer threads even when their awaiting coroutine is cancelled."""

import asyncio

import anyio
from fastapi.concurrency import run_in_threadpool


async def run_owned(function, *args):
    with anyio.CancelScope(shield=True):
        return await run_in_threadpool(function, *args)


async def finish_cancelled(task):
    with anyio.CancelScope(shield=True):
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if task.done() and not task.cancelled():
            task.exception()
