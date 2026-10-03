import errno
import hashlib

import grpc
import pytest
import storage_pb2

from storage.chunk_store import ChunkStore, StorageStartupError
from storage.validation import validate_chunk_id, validate_store

CHUNK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
LIMIT = 2 * 1024 * 1024
BAD_IDS = [
    "",
    "not-a-uuid",
    CHUNK_ID.upper(),
    CHUNK_ID.replace("-", ""),
    "{" + CHUNK_ID + "}",
    "../" + CHUNK_ID,
    CHUNK_ID + "/../other",
]


@pytest.mark.parametrize("chunk_id", BAD_IDS)
def test_reject_noncanonical_ids_and_paths(chunk_id, tmp_path):
    with pytest.raises(ValueError, match="canonical lowercase UUID"):
        validate_chunk_id(chunk_id)
    store = ChunkStore(tmp_path, LIMIT)
    with pytest.raises(ValueError):
        store.chunk_path(chunk_id)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("size", [1, LIMIT])
def test_store_validation_accepts_chunk_boundaries(size):
    data = b"x" * size
    checksum = hashlib.sha256(data).hexdigest()
    assert validate_store(CHUNK_ID, data, checksum, LIMIT) == checksum


@pytest.mark.parametrize("size", [0, LIMIT + 1])
def test_store_validation_rejects_empty_or_oversized_data(size):
    data = b"x" * size
    with pytest.raises(ValueError, match="CHUNK_SIZE_BYTES"):
        validate_store(CHUNK_ID, data, hashlib.sha256(data).hexdigest(), LIMIT)


@pytest.mark.parametrize("checksum", ["", "a" * 63, "A" * 64, "g" * 64, "a" * 64 + "\n"])
def test_store_validation_rejects_hash_format(checksum):
    with pytest.raises(ValueError, match="lowercase hex"):
        validate_store(CHUNK_ID, b"x", checksum, LIMIT)


def test_store_validation_compares_hash_to_actual_bytes():
    with pytest.raises(ValueError, match="does not match data"):
        validate_store(CHUNK_ID, b"x", hashlib.sha256(b"other").hexdigest(), LIMIT)


def test_startup_cleans_only_owned_tempfiles_and_counts_committed_chunks(tmp_path):
    committed = tmp_path / f"{CHUNK_ID}.chunk"
    committed.write_bytes(b"committed")
    stale = tmp_path / (".store-" + "1" * 32 + ".tmp")
    stale.write_bytes(b"partial")
    unrelated = [
        tmp_path / "notes.txt",
        tmp_path / ".store-not-ours.tmp",
        tmp_path / "not-a-uuid.chunk",
    ]
    for path in unrelated:
        path.write_bytes(b"untouched")
    nested = tmp_path / (".store-" + "2" * 32 + ".tmp")
    nested.mkdir()
    (nested / "keep.txt").write_bytes(b"keep")

    store = ChunkStore(tmp_path, LIMIT)
    assert store.used_bytes == len(b"committed")
    assert store.chunk_path(CHUNK_ID) == committed
    assert store.temporary_path().parent == tmp_path
    assert not stale.exists()
    assert committed.read_bytes() == b"committed"
    assert all(path.read_bytes() == b"untouched" for path in unrelated)
    assert (nested / "keep.txt").read_bytes() == b"keep"
    assert ChunkStore(tmp_path, LIMIT).used_bytes == len(b"committed")


def test_new_directory_and_temporary_name_follow_the_same_layout(tmp_path):
    store = ChunkStore(tmp_path / "new" / "chunks", LIMIT)
    temporary = store.temporary_path()
    assert store.used_bytes == 0
    assert temporary.parent == store.data_dir
    assert not temporary.exists()  # P1 reserves names; it does not implement writes.
    temporary.write_bytes(b"partial")
    ChunkStore(store.data_dir, LIMIT)
    assert not temporary.exists()


def test_startup_rejects_reduced_limit_without_modifying_data(tmp_path):
    committed = tmp_path / f"{CHUNK_ID}.chunk"
    data = b"x" * (256 * 1024 + 1)
    committed.write_bytes(data)
    stale = tmp_path / (".store-" + "1" * 32 + ".tmp")
    stale.write_bytes(b"partial")
    with pytest.raises(StorageStartupError, match="below stored chunk size"):
        ChunkStore(tmp_path, 256 * 1024)
    assert committed.read_bytes() == data
    assert stale.exists()


def test_startup_rejects_data_dir_that_is_a_file(tmp_path):
    target = tmp_path / "file"
    target.write_bytes(b"keep")
    with pytest.raises(StorageStartupError, match="Cannot initialize DATA_DIR"):
        ChunkStore(target, LIMIT)
    assert target.read_bytes() == b"keep"


def test_startup_requires_an_actual_writable_probe(tmp_path, monkeypatch):
    def denied(*args, **kwargs):
        raise PermissionError(errno.EACCES, "Permission denied")

    monkeypatch.setattr("storage.chunk_store.tempfile.TemporaryFile", denied)
    with pytest.raises(StorageStartupError, match="Permission denied"):
        ChunkStore(tmp_path, LIMIT)


def test_startup_rejects_nonregular_committed_path(tmp_path):
    (tmp_path / f"{CHUNK_ID}.chunk").mkdir()
    with pytest.raises(StorageStartupError, match="regular file"):
        ChunkStore(tmp_path, LIMIT)


def test_startup_does_not_follow_or_clean_temp_symlinks(tmp_path):
    target = tmp_path / "keep.txt"
    target.write_bytes(b"keep")
    link = tmp_path / (".store-" + "1" * 32 + ".tmp")
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("Symlink creation unavailable on this host")
    assert ChunkStore(tmp_path, LIMIT).used_bytes == 0
    assert link.is_symlink() and target.read_bytes() == b"keep"


@pytest.fixture
def rpc_node(storage_node_factory):
    stub, _, directory = storage_node_factory("p1-node")
    return stub, directory


@pytest.mark.parametrize("method", ["StoreChunk", "GetChunk", "DeleteChunk"])
@pytest.mark.parametrize("chunk_id", BAD_IDS)
def test_rpc_rejects_bad_ids_before_touching_disk(rpc_node, method, chunk_id):
    stub, directory = rpc_node
    if method == "StoreChunk":
        request = storage_pb2.StoreChunkRequest(
            chunk_id=chunk_id, data=b"x", checksum_sha256=hashlib.sha256(b"x").hexdigest()
        )
    else:
        request = getattr(storage_pb2, method + "Request")(chunk_id=chunk_id)
    with pytest.raises(grpc.RpcError) as error:
        getattr(stub, method)(request, timeout=2)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert list(directory.iterdir()) == []


@pytest.mark.parametrize(
    "data,checksum",
    [
        (b"", hashlib.sha256(b"").hexdigest()),
        (b"x" * (LIMIT + 1), hashlib.sha256(b"x" * (LIMIT + 1)).hexdigest()),
        (b"x", "A" * 64),
        (b"x", hashlib.sha256(b"other").hexdigest()),
    ],
)
def test_rpc_rejects_bad_store_payload_without_artifacts(rpc_node, data, checksum):
    stub, directory = rpc_node
    request = storage_pb2.StoreChunkRequest(chunk_id=CHUNK_ID, data=data, checksum_sha256=checksum)
    with pytest.raises(grpc.RpcError) as error:
        stub.StoreChunk(request, timeout=2)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert list(directory.iterdir()) == []


@pytest.mark.parametrize("size", [1, LIMIT])
def test_valid_store_accepts_chunk_boundaries(rpc_node, size):
    stub, directory = rpc_node
    data = b"x" * size
    request = storage_pb2.StoreChunkRequest(
        chunk_id=CHUNK_ID, data=data, checksum_sha256=hashlib.sha256(data).hexdigest()
    )
    response = stub.StoreChunk(request, timeout=2)
    assert response.size_bytes == size and not response.already_existed
    assert (directory / f"{CHUNK_ID}.chunk").read_bytes() == data
