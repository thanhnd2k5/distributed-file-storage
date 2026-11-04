# Mốc B — Storage và metadata nền V2

Trạng thái 23/10/2026: **B1–B4 hoàn tất trong phạm vi foundation**, bàn giao
[mốc C](V2_C_IMPLEMENTATION_PLAN.md). Nguồn: V2_API_CONTRACTS,
V2_METADATA_SCHEMA và V2_CORRECTNESS; không thay runtime/volume V1. Evidence
B4 và giới hạn nghiệm thu ở cuối tài liệu; các phần trước giữ lịch sử từng gate.

| Phase | Phạm vi | Gate |
|---|---|---|
| B1 | SQLite ledger, volume ownership, identity, durable prepare/inspect/delete, startup reconcile | Filesystem thật; retry/conflict/tombstone/identity; B01/B06/B07/B10 |
| B2 | Streaming Store/Get, per-allocation fencing/drain, inventory/health, gRPC server riêng | B02–B09; actual bytes/hash, cancel, bounded stream, process kill Linux |
| B3 | Migration PostgreSQL riêng, recovery gate, session/admission/capacity/attempt/job transactions | PostgreSQL thật; constraints, hai transaction cạnh tranh, không release capacity sớm |
| B4 | Maintenance epoch, quarantine/adoption, disk/headroom và bàn giao C | B10/B11/B-H01, fault injection, evidence từng command/topology |

Không đánh dấu B hoàn tất trước khi các gate trên có evidence. Compile/lint
không chứng minh crash durability; native Windows không thay gate fsync Linux.

Preflight 23/10: Docker Engine 29.8.2 truy cập được; Compose local không có
container đang chạy. PostgreSQL gate chưa sẵn sàng. Recovery/setup hiện có:
`docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait`.
Không chạy migration V1 để kiểm V2; test DB cần namespace riêng và chỉ tạo/dọn
fixture thuộc test. Đã chuẩn bị `deploy/compose.v2-metadata-tests.yml` với project
`dfs-v2-tests`, mạng internal và không publish host port. `docker compose -f
deploy/compose.v2-metadata-tests.yml config --quiet` passed.
Xem [hướng dẫn chạy và quản lý port](V2_LOCAL_TESTS.md).

Cập nhật sau xác nhận của người vận hành 23/10/2026: đã chạy
`docker compose -f deploy/compose.v2-metadata-tests.yml up -d --wait postgres`,
container `dfs-v2-tests-postgres-1` healthy. Truy vấn bằng `psql` trong container
trả `current_database=dfs_v2_test`, `current_user=v2_test`, `ready=1`.
Không publish host port; `5432/tcp` trong `compose ps` là port nội bộ container.
Ở bước setup ban đầu chỉ dựng DB fixture và kiểm kết nối, chưa chạy migration/tests metadata B3,
full suite hoặc cluster smoke. Blocker PostgreSQL setup đã được gỡ.

## Evidence Storage — 23/10/2026

Code mới trong `backend/storage/v2/` gồm ledger SQLite WAL/FULL, flock volume,
scope/incarnation gate, durable prepare/inspect/delete, inventory phân trang,
startup recovery/quarantine, streaming Store/Get và adapter đủ 7 RPC V2.
Server V2 là entry point riêng; Docker image generate cả hai bộ stubs, V1
vẫn dùng entry point/contracts cũ. Chưa deploy Storage V2 vào cluster.

Maintenance offline giữ volume lock, không mở service/RPC; đổi epoch theo
quyết định quản trị, adopt từng allocation sau verify hoặc ghi quyết định close.
Allocation chưa adopt giữ bytes và không phục vụ/cleanup dưới epoch mới.
Không tự chứng minh writer trên host khác đã cô lập. Hook local không thay
quy trình restore DB/cluster ở B3/B4–H.

Tier focused integration, Docker Engine 29.8.2, Linux container Python 3.12,
SQLite/filesystem thật, loopback gRPC trong container; temp dirs riêng từng test.
Các lệnh từ root repository:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml -f deploy/compose.v2-tests.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml -f deploy/compose.v2-tests.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_v2_ledger.py tests/test_v2_streaming.py
```

Đã qua 33 ca khác nhau; không cộng các lượt chạy 26/32 ca trước đó vào tổng.
Lượt cuối được ghi sau khi chạy lại thay đổi headroom/streaming bị ảnh hưởng.

| Gate | Evidence đã có | Giới hạn |
|---|---|---|
| B01/B07 | Prepare replay sau restart, immutable spec conflict; delete trước prepare giữ tombstone qua mọi generation | Local Storage, chưa có metadata RPC coordinator |
| B02/B05 | Interrupted stream dọn temp/retry; STORED retry đọc/kiểm body; Inspect actual size/hash; 7 RPC loopback thật | Chưa inject mất ACK trên mạng thật/hai Transfer |
| B03/B04/B06 | Parent SIGKILL child sau temp fsync, rename+directory fsync, STORED commit, delete fence; reopen kiểm bytes/ledger/temp | Có cả intent dựng trực tiếp và hooks trong chính StreamingStore; không suy ra chịu mất điện/host |
| B08/B09 | Writer gen1 chờ Event; Prepare gen2/Delete commit fence trước drain timeout; gen1 không publish, allocation khác tiến triển | Domain threads; chưa phải stress test nhiều client gRPC |
| B10/B-H01 local | Scope/incarnation mismatch bị chặn; mất ledger không tự tạo lại; maintenance/adoption/tombstone và unowned-byte quarantine | Registry quarantine/DB recovery gate còn B3 |
| B11 local | Inject ENOSPC fsync không ACK; fill tmpfs riêng 8 MiB đến ENOSPC, delete dùng physical ledger headroom, tombstone/restart đúng | Chưa có metadata charge/cleanup pending; tmpfs chỉ kiểm hết dung lượng, không kiểm power-loss durability |

Ruff check/format các file Python mới đã qua trên native Windows (9 files).
Không chạy PostgreSQL/migration/session/capacity/job tests, frontend, full V1
suite, cluster smoke, replication, resume, benchmark hoặc HA. Không có evidence
runtime tương ứng cho các phần này.

**Ghi nhận trước B4: B chưa hoàn tất.** B1 và Storage thuộc B2 đã có gate trên; B3 foundation có
evidence dưới đây. B4 mới có hook local và recovery gate DB, còn chốt nghiệm thu
liên kết/incarnation, disk failure và handoff C. Không suy ra restore toàn cluster
hoặc luồng Transfer đã hoàn thành từ các gate foundation này.

## Evidence B3 — 23/10/2026

Đã hiện thực `backend/metadata/v2/` độc lập với V1:

- Migration SQL version 1, checksum và advisory transaction lock; từ chối schema
  hiện có ngoài chuỗi V2, namespace/epoch khác hoặc checksum lệch. Không reset,
  downgrade hay tự chuyển epoch. Test dùng schema sở hữu riêng; CLI dùng DB V2
  và schema public. Đây là chuỗi migration V2 riêng, không thêm vào Alembic V1.
- Đủ 14 bảng theo baseline schema A và các CHECK/FK RESTRICT/index, kể cả FK
  incarnation deferrable, một incarnation ACTIVE/node và dedup job đang active.
- Transaction ngắn READ COMMITTED với lock/statement timeout; shared control row
  gate namespace/epoch/mode trước mọi repository operation. Không có RPC/file I/O
  trong transaction PostgreSQL; các instance không reset startup state của nhau.
- Create idempotent với canonical payload hash, admission/snapshot policy; claim
  part cố định spec, strict RF/domain placement, charge + allocation + attempt
  intent nguyên tử trước I/O. Retry/busy không tạo thêm allocation hay charge.
- Uncertain/lease sweeper giữ charge và enqueue reconcile; claim slot được trả
  một lần. Abort/expiry tăng revision, trả admission một lần và enqueue cleanup;
  không trả node capacity ở terminal transition.
- Cleanup authorization keyset tối đa 8 part/trang, giữ charge. Chỉ durable
  Delete ACK đúng namespace/epoch/node/incarnation/allocation/generation, DELETED
  và cleanup_complete mới release một lần; giữ lịch sử charged_bytes/attempt.
- Job claim SKIP LOCKED tối đa 8, DB-clock lease/renew/retry và fencing token.
  Cleanup publication kiểm job token/live/target trong cùng transaction sau các
  domain locks; stale job rollback cả budget/allocation/attempt mutations.
- Registry identity mới vào JOINING/node QUARANTINED; không chọn placement trước
  administrative activation/reconcile. Budget hạ dưới charge bị từ chối và node
  quarantine, charge cũ giữ nguyên. Health không cấp thêm capacity credit.

Tier focused integration, Docker Linux/Python 3.12/PostgreSQL 16, schema
`test_v2_<uuid>` riêng từng case, pool connections độc lập cho race. Preflight:
Docker Engine 29.8.2; `dfs-v2-tests-postgres-1` healthy và psql ready=1.

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_metadata.py
```

Gate mở rộng passed 33 ca sau khi DDL/FK và paging được chốt (4.01s). Sau bổ sung
job-publication fencing, rebuild image và chỉ chạy các ca cleanup bị ảnh hưởng:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_metadata.py -k "cleanup or storage_unknown"
```

Passed 4, deselected 30 (1.02s), trong đó một ca mới kiểm stale worker không
release được charge. Tổng 34 case khác nhau; không cộng lượt đầu 22 hoặc các
rerun vào tổng. Không có case skipped hoặc assertion failure trong các lượt này.
Các ca đối chiếu constraint/counter, concurrent idempotency/admission/part/budget,
expiry/sweeper, incarnation, budget shrink, job dedup/reclaim và recovery gate.
Một ca dùng SQLite/filesystem Storage thật cùng PostgreSQL: STORED mất ACK và
stream đứt đều giữ charge; durable Delete ACK mất/replay chỉ release đúng một lần,
đối chiếu ledger/bytes/temp và metadata. Đây là domain integration trực tiếp,
không phải HTTP/Transfer E2E hay network fault injection.

Đã chạy migration thật:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python scripts/migrate_v2_metadata.py
```

Passed `V2 migration applied`. Psql xác nhận database `dfs_v2_test`, schema public
15 bảng (14 domain + version), version=1, ACTIVE; namespace
`63f6cf3b-d9a4-40fc-8e67-c86a05520d4c`, epoch
`81efbf3d-2768-44be-9de6-924c16606115`, admission `upload_sessions=0/128`.
Repository readiness với cấu hình fixture đã kiểm riêng trên schema public.
Không tạo session/file thật trong schema public; không chạy migration V1.

Native Windows Ruff check và format --check passed 5 file Python mới:
`metadata/v2`, `scripts/migrate_v2_metadata.py`, `tests/test_v2_metadata.py`.
Không thay dependency. Không rerun Storage-only gate đã pass vì không đổi Storage;
không chạy full V1/backend/frontend suite, HTTP E2E, cluster smoke, benchmark,
HA, DB process restart hoặc restore backup. DB fixture vẫn là tmpfs, không dùng
làm bằng chứng persistence qua mất container/host.

B3 foundation đã có migration và gate; HTTP shape/adapters, claim renew/retry,
confirm, complete/finalize publication, download lease runtime và Worker lifecycle
còn thuộc C–D. Job consumer phải chỉ finish cleanup khi mọi page/outcome đã đối
soát; foundation giữ charge/history khi pending/DEAD. B4 tiếp tục kiểm liên kết
recovery/identity và fault matrix trước khi chốt toàn mốc B và bàn giao C.

## Review fixes — 23/10/2026

Đã sửa bốn findings được tái hiện trong lượt review:

1. Recovery hash được final sau rename chỉ publish STORED sau khi fsync file và
   thư mục chunks thành công. Nếu flush còn lỗi, constructor thất bại và ledger
   WRITING không được promote. Có regression đối chiếu SQLite trước từng flush,
   inject cả lỗi file/directory và kiểm có thể mở lại sau khi fault được gỡ.
2. Claim/session sweepers không giữ khóa nhiều session xuyên cả batch. Scan
   candidate vẫn bounded; mỗi owner được xử lý/recheck trong transaction riêng.
   Ca concurrent ép thứ tự A→B và B→A hoàn thành cả hai sweeper, kiểm state,
   claim/admission counters, dedup jobs và charge giữ nguyên; thêm gate limit=1.
3. Restore headroom luôn fallocate/fsync reservation kể cả file đã tồn tại;
   không dùng exists hoặc logical size làm bằng chứng cấp phát vật lý. Regression
   khởi động lại với file rỗng, partial và sparse full-size; kiểm blocks được
   cấp phát thật và Store thành công. Giữ headroom lock và fail-closed khi đầy disk.
4. Health counters dùng một stat cho mỗi entry, bỏ qua FileNotFoundError khi
   writer/checkpoint vừa xóa hoặc rename file. Giữ các lỗi filesystem khác là lỗi.
   Regression cho cả tempfile và ledger/WAL census, gồm race cũ giữa is_file/stat
   và entry biến mất trước stat. Counters vẫn chỉ là observation.

Tier focused integration. Preflight Docker Engine 29.8.2 passed, nhưng fixture
PostgreSQL đã dừng (`service "postgres" is not running`). Trong phạm vi setup
V2 đã được người vận hành cho phép, chạy lại lệnh hiện có:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml up -d --wait postgres
```

Postgres healthy, psql ready=1. Tmpfs đã mất public schema sau stop: ghi nhận
public_tables=0; không chạy lại public migration trong lượt sửa lỗi. Gate dùng
schema test sở hữu riêng và dọn sau mỗi case, không cần public schema.

Đã rebuild image với code mới rồi chạy đúng affected regressions:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_ledger.py tests/test_v2_streaming.py tests/test_v2_metadata.py -k "test_v2_ledger or test_v2_streaming or sweep or expiry or recovery_gate or storage_unknown"
```

Passed **52**, deselected **27**, không skipped/failure (5.18s), Docker Linux/
Python 3.12, SQLite/filesystem/gRPC và PostgreSQL 16 thật; ENOSPC dùng tmpfs 8 MiB
riêng. Bao gồm 12 case mới cho bốn fixes, không cộng lượt review tái hiện lỗi cũ
vào tổng passed. Ruff check và format --check 6 paths thay đổi passed native Windows:
`storage/v2/ledger.py`, `storage/v2/service.py`, `metadata/v2/repository.py`,
`tests/test_v2_ledger.py`, `tests/test_v2_streaming.py`, `tests/test_v2_metadata.py`.

Không rerun các metadata checks bị deselect, full V1/backend/frontend suite,
HTTP E2E, cluster smoke, benchmark, restore backup hoặc kiểm mất điện/host thực.
Không thay dependency, DDL/checksum hay runtime deployment V1. Evidence fsync
là thứ tự flush và hành vi fail-closed khi inject lỗi, không phải power-loss
certification. Tại thời điểm kết thúc lượt review, B4/handoff vẫn còn mở;
nghiệm thu bổ sung bên dưới chốt phần này.

## Evidence B4 và bàn giao C — 23/10/2026

Đối chiếu B10/B11/B-H01 cho thấy các hook đã có, nhưng evidence Storage và B3
chưa kiểm cùng ranh giới gRPC/ledger/PostgreSQL. Bổ sung
`backend/tests/test_v2_b4.py`, tái sử dụng fixture schema riêng của B3. Không đổi
production code, migration/checksum, dependency hoặc deployment V1/V2.

Tier **focused integration**. Preflight read-only passed: Docker Engine 29.8.2,
`dfs-v2-tests-postgres-1` healthy, psql trả `dfs_v2_test|v2_test|1`.
Môi trường: container Linux/Python 3.12, PostgreSQL 16, SQLite/filesystem thật,
gRPC loopback và subprocess CLI; ENOSPC dùng tmpfs 8 MiB riêng. Mỗi case tạo/dọn
schema `test_v2_<uuid>` và volume tạm sở hữu riêng. Không dùng public hoặc volume V1.

Các lệnh từ root repository đã chạy:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-tests.yml build tests

docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_b4.py
```

Passed **4**, không skipped/deselected/failure (4.65s). Sau khi mở rộng chính ca
recovery để kiểm cleanup worker đã có lease cũng bị gate chặn authorize/renew/
finish, rebuild cùng lệnh trên và chỉ chạy affected case:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_b4.py -k offline_epoch
```

Passed **1**, deselected **3**, không skipped/failure (2.60s). Tổng vẫn là
**4 case khác nhau**, không cộng lượt rerun. Ruff check và format --check
`tests/test_v2_b4.py` passed với Python `.venv` native Windows.

| Gate | Điểm inject / thao tác | Kết quả đối chiếu |
|---|---|---|
| B10 | Xóa ledger của volume chứa STORED; provision volume mới cùng node ID, incarnation mới | Volume cũ không tự tạo ledger/adopt bytes; incarnation mới JOINING và node bị cách ly khỏi placement; RPC scope cũ bị từ chối; sau administrative activation, durable Delete ACK của disk mới vẫn không release charge của incarnation cũ |
| B11, hai case | Fill tmpfs đến ENOSPC; Store mới; lỗi flush trước tombstone hoặc sau unlink | Store không ACK; Delete lỗi trả RESOURCE_EXHAUSTED, cleanup_complete=false; DB giữ DELETE_PENDING/charge/job RUNNING; retry dùng headroom thật khi disk vẫn đầy; ACK mất giữ charge, replay release đúng một lần; reopen giữ tombstone và phục hồi headroom |
| B-H01 foundation | Stream đã WRITING; DB RECOVERING; CLI begin khi process còn giữ volume; drain rồi begin/adopt/close/activate qua subprocess | DB chặn mutation và cleanup worker cũ; flock từ chối maintenance khi còn serving; sau drain/stop mới đổi epoch; RPC epoch cũ bị chặn; adoption giữ bytes đúng hash, tombstone không mở lại; allocation chưa adopt giữ bytes/epoch cũ, không inspect/delete dưới epoch mới; đổi Storage epoch không tự mở DB gate |

CLI đã thực thi trên volume test: `begin --epoch <new>`,
`adopt --epoch <new> --allocation <stored>`,
`close --epoch <new> --allocation <tombstone>` và `activate --epoch <new>`.
Thử adopt tombstone và begin khi serving đều phải thất bại. Việc cài mode
RECOVERING của DB trong case là fault setup của fixture, **không phải** công cụ
restore DB hoặc quy trình rebuild budget production.

Truy vấn read-only bổ sung sau test để đếm schema đã bị Docker API từ chối:
`permission denied ... npipe:////./pipe/dockerDesktopLinuxEngine`. Đã dừng
truy vấn này, không retry/start/restart dịch vụ. Không có số đếm schema sau lượt
test độc lập; fixture teardown thuộc các lượt pytest đã pass. Khi quyền Docker
Desktop sẵn sàng, kiểm lại bằng `docker info` trước truy vấn DB cần thiết.

**Chốt B:** B01–B11 và hook recovery B-H01 đã có evidence trong phạm vi
foundation B1–B4, gồm các gate Storage/B3 và review fixes trước đó. Không gọi
đây là một lượt full suite; không rerun các gate cũ vì code của chúng không đổi.
Không chạy frontend, full V1/backend suite, HTTP/Transfer E2E, live cluster smoke,
replication, benchmark, HA, restore backup hoặc power-loss/host-loss thực.
Restore toàn cluster, cô lập writer trên host khác, rebuild metadata/capacity và
RPO/RTO tiếp tục thuộc H. Không có runtime upload/resume/finalize V2 từ các test B.

**Bàn giao C:** bắt đầu từ transaction renew/confirm/retry/complete/publication
còn thiếu, rồi nối API/Transfer và Worker reconcile/cleanup/finalize. Giữ RPC và
filesystem I/O ngoài transaction DB, charge đến durable cleanup, và fencing
epoch/revision/job token. Kế hoạch và gate chi tiết: [mốc C](V2_C_IMPLEMENTATION_PLAN.md).
