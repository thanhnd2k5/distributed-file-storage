"""E2 process boundary: fresh engine/owner, explicit crash and lock barriers."""

import multiprocessing
import os
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import create_engine

from metadata.v2.repository import DeleteEvidence, DomainError, Repository, execute
from storage.v2.ledger import Ledger, PreconditionError, Scope, Spec
from storage.v2.streaming import StreamingStore


@dataclass
class Call:
    method: str
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    fault: str | None = None


def storage_call(method, root, destination, data=b"abc"):
    d = destination
    scope = Scope(
        str(d["namespace_id"]), str(d["recovery_epoch"]), d["node_id"], str(d["incarnation"])
    )
    # A new process must reacquire the volume lock and run actual recovery.
    with_ledger = Ledger(Path(root), scope, provision=True)
    try:
        store = StreamingStore(with_ledger)
        aid, generation = str(d["allocation_id"]), d["generation"]
        spec = Spec(str(d["chunk_id"]), d["size_bytes"], d["checksum_sha256"])
        if method == "storage_store":
            store.prepare(scope, aid, generation, spec)
            return store.store(scope, aid, generation, spec, [(0, data)])[0]
        if method == "storage_inspect":
            return with_ledger.inspect(scope, aid, generation)
        if method != "storage_delete":
            raise ValueError("Unknown storage action")
        row = store.delete(scope, aid, generation)
        assert row["state"] == "DELETED" and row["cleanup_complete"]
        assert not (with_ledger.root / "chunks" / aid).exists()
        assert not list((with_ledger.root / "tmp").iterdir())
        return DeleteEvidence(
            d["namespace_id"],
            d["recovery_epoch"],
            d["node_id"],
            d["incarnation"],
            d["allocation_id"],
            generation,
            row["state"],
            row["cleanup_complete"],
        )
    finally:
        with_ledger.close()


def process(settings, call, pipe, start):
    engine = create_engine(
        settings.pop("url"), pool_size=1, max_overflow=0, connect_args={"connect_timeout": 5}
    )
    try:
        repo = Repository(engine, **settings)
        with repo.transaction() as db:
            backend = execute(db, "SELECT pg_backend_pid()").scalar_one()
        pipe.send(dict(pid=os.getpid(), backend=backend))
        assert start.wait(15), "Parent did not release process barrier"
        transaction = repo.transaction

        @contextmanager
        def crashing_transaction():
            with transaction() as db:
                yield db
                if call.fault == "before_commit":
                    os._exit(74)
            if call.fault == "after_commit":
                os._exit(73)

        repo.transaction = crashing_transaction
        try:
            if call.method.startswith("storage_"):
                value = storage_call(call.method, *call.args, **call.kwargs)
                if call.fault == "lost_storage_ack":
                    os._exit(75)
            else:
                value = getattr(repo, call.method)(*call.args, **call.kwargs)
            pipe.send(dict(value=value))
        except (DomainError, PreconditionError) as exc:
            pipe.send(dict(error=str(exc)))
    except BaseException:
        pipe.send(dict(unexpected=traceback.format_exc()))
        raise
    finally:
        pipe.close()
        engine.dispose()


def processes(repo, *calls, hold=None):
    """Optionally hold the contested DB row until BOTH backends really wait on it."""
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    children, pipes = [], []
    settings = dict(
        url=repo.engine.url.render_as_string(hide_password=False),
        namespace_id=repo.namespace_id,
        recovery_epoch=repo.epoch,
        schema=repo.schema,
        policy=repo.policy,
        max_job_backlog=repo.max_job_backlog,
    )
    try:
        for call in calls:
            parent, child = context.Pipe()
            runner = context.Process(target=process, args=(settings, call, child, start))
            children.append(runner)
            pipes.append(parent)
            runner.start()
            child.close()
        identities = []
        for pipe in pipes:
            assert pipe.poll(15), "Process startup timed out"
            identity = pipe.recv()
            assert "unexpected" not in identity, identity
            identities.append(identity)
        assert len({item["pid"] for item in identities}) == len(calls)
        assert len({item["backend"] for item in identities}) == len(calls)
        assert os.getpid() not in {item["pid"] for item in identities}
        if hold:
            with repo.transaction() as guard:
                execute(guard, hold)
                start.set()
                due = time.monotonic() + 2
                while True:
                    with repo.engine.connect() as observer:
                        blocked = execute(
                            observer,
                            "SELECT count(*) FROM pg_stat_activity WHERE pid=ANY(:pids) "
                            "AND wait_event_type='Lock'",
                            pids=[item["backend"] for item in identities],
                        ).scalar_one()
                    if blocked == len(calls):
                        break
                    assert time.monotonic() < due, "Contenders did not reach the held DB lock"
                    time.sleep(0.01)
        else:
            start.set()
        results = []
        exits = {"before_commit": 74, "after_commit": 73, "lost_storage_ack": 75}
        for runner, pipe, call in zip(children, pipes, calls):
            runner.join(20)
            assert not runner.is_alive(), "Process operation timed out"
            assert runner.exitcode == exits.get(call.fault, 0)
            if call.fault:
                results.append(dict(crash=call.fault))
            else:
                assert pipe.poll(1), "Process returned no result"
                result = pipe.recv()
                assert "unexpected" not in result, result
                results.append(result)
        return results
    finally:
        start.set()
        for runner in children:
            if runner.is_alive():
                runner.kill()
            runner.join(5)
            runner.close()
        for pipe in pipes:
            pipe.close()
