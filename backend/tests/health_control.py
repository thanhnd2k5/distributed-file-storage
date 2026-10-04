"""Freeze background work in snapshot tests while keeping the scheduler alive."""

from threading import Barrier


def pause_background(worker):
    worker._poll = lambda node: None
    # Occupy every health thread simultaneously: all earlier polls must have drained.
    barrier = Barrier(len(worker.nodes) + 1)
    futures = [worker._executor.submit(barrier.wait, 5) for _ in worker.nodes]
    barrier.wait(5)
    for future in futures:
        future.result(timeout=5)
    if worker._cleanup_executor is not None:
        worker._cleanup = lambda: None
        worker._cleanup_executor.submit(lambda: None).result(timeout=5)
    assert worker.running
