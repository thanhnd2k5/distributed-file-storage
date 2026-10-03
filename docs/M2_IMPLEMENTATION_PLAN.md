# M2 — Metadata quan sát ba Storage Node

**Ngày lập:** 03/10/2026  
**Trạng thái:** M2 hoàn thành P1–P6 (03/10/2026); tiếp theo M3 upload/download RF=2.

**Đầu vào:** M0–M1 hoàn thành, ba Storage Node có HealthCheck thật và volume riêng.  
**Đầu ra:** Metadata tự cập nhật health/capacity, REST đọc snapshot đúng, phát hiện
node down/recovery và giữ metadata qua restart.

## 1. Mục tiêu và ranh giới

Thực hiện M2 trong [kế hoạch tổng](IMPLEMENTATION_PLAN.md), theo
[ARCHITECTURE.md, mục 2–4 và 8](ARCHITECTURE.md),
[API_CONTRACTS.md, mục 1 và 2.6](API_CONTRACTS.md) và
[bàn giao M1](M1_IMPLEMENTATION_PLAN.md).

M2 gồm:

- Hoàn thiện registry/startup cho detector, tận dụng schema bốn bảng đã có.
- Poll HealthCheck độc lập trên từng node; kiểm tra identity/domain/writable.
- Tính ACTIVE/SUSPECTED/DOWN bằng monotonic clock; lưu snapshot trong PostgreSQL.
- Nối worker vào lifespan của Metadata, quản lý channels và shutdown.
- GET `/api/v1/nodes`, GET `/api/v1/cluster`; giữ live/ready đúng contract.
- Kiểm tra ba node, down/recovery, Metadata restart và PostgreSQL persistence.

Upload/download REST, placement RF=2, fallback bytes, cleanup worker và repair
thuộc M3–M4. M2 không sửa wire contract, thêm push heartbeat, triển khai hai máy
hay làm UI. Health ACTIVE chỉ xác nhận reachable/writable/identity; không xác minh
integrity từng replica và không tự chuyển replica thành VERIFIED.

## 2. Điểm xuất phát thực tế

| Thành phần đã có | Việc M2 cần làm |
|---|---|
| `metadata/models.py`, migration `0001_initial.py` | Đã có files/chunks/storage_nodes/chunk_replicas; dùng lại, chỉ thêm migration khi có thiếu sót cụ thể |
| `metadata/bootstrap.py` | Đã upsert registry, disable node vắng config, reset DOWN, giữ mappings và recovery flags; kiểm tra/bổ sung baseline cho detector |
| `metadata/config.py` | Đã có interval 3 s, deadline 1 s, down-after 10 s và node config; dùng lại |
| `metadata/main.py` | Đã có lifespan/live/ready/operation_lock; thêm worker và routers |
| `metadata/db.py`, `metadata/errors.py` | Dùng session riêng theo thao tác và error envelope hiện có |
| Storage M1 | Bốn unary RPC, Health có executor riêng, cached used_bytes; dùng production server factory khi integration |
| `deploy/compose.local.yml` | Đã có PostgreSQL, Metadata một worker, ba node cùng dev_host, bốn named volumes; giữ topology |
| `tests/test_bootstrap.py`, `tests/test_base.py` | Đã kiểm tra registry/recovery và live khi DB down; mở rộng đúng phần bị ảnh hưởng |
| `scripts/smoke_base.py` | Chỉ kiểm tra RPC trực tiếp/DB/live/ready; chưa chứng minh Metadata detector hay API nodes/cluster |

Các file mới đã triển khai trong M2:

- `backend/metadata/health.py`: trạng thái, nhận kết quả Health và tính age.
- `backend/metadata/worker.py`: lịch poll, channels, executor, start/stop.
- `backend/metadata/cluster.py`: đọc node snapshots và tổng hợp cluster.
- `backend/metadata/schemas.py`, `backend/metadata/routes/cluster.py`: REST DTO/routes.
- `backend/tests/test_metadata_health.py`, `test_metadata_worker.py`,
  `test_metadata_cluster.py`, `test_metadata_lifecycle.py`.
- `backend/scripts/smoke_metadata.py`: quan sát REST/DB cho smoke M2.
- `backend/scripts/smoke_metadata.ps1`: điều phối kịch bản Compose down/recovery/restart.
- `backend/scripts/metadata_fixture.py`: seed/snapshot fixtures trong schema riêng cho lifecycle/persistence checks.

Không dựng repository framework hoặc chuyển toàn bộ backend sang async.
Nếu chia sẻ fixture PostgreSQL, chuyển fixture isolated-schema hiện có vào
`tests/conftest.py`; không để test modules import fixture của nhau. Fixture
Storage nhiều node phải cấp subdirectory riêng cho từng node: fixture M1 hiện
trả cùng tmp_path khi gọi nhiều lần, chưa đủ cho volume-isolation test M2.

## 3. Chia phase và thứ tự

| Phase | Nội dung | Phụ thuộc | Điều kiện chuyển bước |
|---|---|---|---|
| P1 | Registry và baseline sau startup | M1 | Reset DOWN; disabled giữ mapping; startup lỗi không chạy poll |
| P2 | Health state machine | P1 | Timing, identity, writable, metrics và recovery đúng bằng kiểm tra xác định |
| P3 | Poll worker và lifespan | P2 | RPC độc lập, DB transaction ngắn, stop sạch; lỗi một node không chặn các node khác |
| P4 | REST nodes/cluster | P1–P3 | Đúng schema/counters; GET chỉ đọc snapshot; DB lỗi trả 503 |
| P5 | Down/recovery và restart thật | P1–P4 | REST phản ánh lỗi process/recovery, metadata giữ qua restart, volumes độc lập |
| P6 | Rà DoD và bàn giao M3 | P5 | Đủ bằng chứng; docs đúng code; các gate thiếu vẫn ghi chưa hoàn thành |

Làm tuần tự; viết và kiểm tra hành vi trong phase sở hữu nó. Ước lượng gốc M2
là 2 buổi tập trung: buổi đầu P1–P3, buổi sau P4–P6. Đây là cách phân bổ dự kiến;
lỗi lifecycle/infra có thể dùng buffer của kế hoạch tổng, không hứa ngày hoàn tất.

## 4. P1 — Registry và baseline sau startup

### Đầu việc

- Rà models/migration/config với contract; không viết lại migration đã có.
- Giữ startup trong transaction ngắn, hoàn tất trước khi start worker; startup
  recovery thực hiện dưới operation_lock theo kiến trúc.
- Node có config: upsert host/port/domain, enabled=true, status=DOWN. Node vắng
  config: enabled=false, DOWN; không xóa row hay attempted replica mappings.
- Giữ quy tắc từ chối đổi failure_domain khi node đã có replica mapping và rollback
  cả transaction. Không âm thầm đổi topology để Health được coi là đúng identity.
- Khởi tạo trạng thái monotonic rỗng cho process mới. Timestamp/metrics đã lưu
  được giữ làm thông tin lịch sử; không dùng chúng để khôi phục ACTIVE sau restart.
  Node chưa từng có health success vẫn có last_success_at/metrics=null.
- Khi endpoint của node thay đổi, vô hiệu snapshot cũ của endpoint đó trước khi
  poll lại; tránh trình bày metrics cũ như quan sát của endpoint mới.
- Giữ startup recovery hiện có: UPLOADING → FAILED, pending cleanup được giữ;
  M2 không chạy cleanup hay sửa các replica của AVAILABLE từ health.

### Kiểm tra và gate

Focused PostgreSQL integration: registry lần đầu/lặp lại, remove/re-enable,
endpoint đổi, domain change bị từ chối, rollback và mappings/recovery flags
không mất. Có case restart khi DB lưu node ACTIVE nhưng process mới vẫn DOWN.
Tận dụng `test_bootstrap.py`, không tạo lại bộ test migration tương đương M0.

**Gate:** registry đúng; initialization thất bại giữ initialized=false/ready=503.
Worker sẽ được nối sau initialization thành công ở P3; P1 chưa tạo worker.

## 5. P2 — Health state machine và snapshot

### Quy tắc chuyển trạng thái

| Quan sát | Trạng thái/kết quả |
|---|---|
| Node disabled | DOWN; không gửi RPC |
| Chưa có success trong process hiện tại | DOWN cho đến response hợp lệ |
| Identity/domain khớp và writable=true | ACTIVE; cập nhật last_success monotonic, UTC timestamp, metrics; xóa last_error |
| RPC timeout/unreachable sau success, age < 10 s | SUSPECTED; giữ timestamp/metrics thành công gần nhất |
| RPC lỗi và age đạt ngưỡng down-after | DOWN; giữ snapshot lịch sử |
| node_id hoặc failure_domain sai | DOWN ngay, last_error=IDENTITY_MISMATCH; không nhận metrics của node sai |
| Identity đúng nhưng writable=false | DOWN ngay, ghi lý do filesystem không writable; không ghi nhận writable success |
| Response hợp lệ sau SUSPECTED/DOWN | ACTIVE ngay; không cần nhiều lượt success |

Identity/unwritable là lỗi chặn: giữ DOWN cho đến success hợp lệ, không để một
timeout kế tiếp nâng node thành SUSPECTED. Tên lý do unwritable có thể dùng
`STORAGE_NOT_WRITABLE`; last_error là chuỗi chẩn đoán có giới hạn, không lộ secrets.
Metrics cũ vẫn là lịch sử có last_success_at; không dùng làm bằng chứng node đang
eligible. Không cộng capacity/free của các node thành tổng dung lượng cluster.

### Đầu việc và kiểm tra

- Tách pure transition logic khỏi RPC/DB; inject monotonic clock và UTC clock
  để kiểm tra thời gian mà không sleep 10 giây.
- Dùng health_interval/deadline/down-after từ config; health không retry trong
  cùng lượt, không dùng chunk RPC retry policy.
- Kiểm tra age ngay dưới/đúng/trên threshold, failures lặp lại, recovery, disabled,
  identity/domain sai, unwritable và hard-DOWN không bị nâng bởi timeout.
- Kiểm tra UTC clock nhảy tới/lùi không đổi detector age; restart không tái dùng
  monotonic từ process trước. Success hợp lệ thay snapshot và clear error.

**Gate:** targeted tests của state machine qua, không cần Docker/PostgreSQL.
P2 chưa là bằng chứng worker/RPC thực tế.

## 6. P3 — Poll worker, DB và lifespan

### Đầu việc

- Một background loop của Metadata, start sau initialize thành công và stop
  theo lifespan; không tạo thread/task ở import. M4 sẽ nối cleanup vào loop này.
- Dùng synchronous gRPC channels/stubs theo từng node, options từ settings;
  blocking RPC/SQL chạy ngoài FastAPI event loop.
- Poll lần đầu ngay sau startup, sau đó theo interval mỗi node. Dùng executor
  hữu hạn đủ để ba node được gọi độc lập; không timeout tuần tự 1+1+1 giây.
  Không khởi động lượt thứ hai cho một node khi lượt trước còn chạy.
- Không giữ SQLAlchemy Session/transaction khi chờ RPC. Chụp config/call RPC,
  rồi persist kết quả trong transaction ngắn với Session riêng; node disabled
  không nhận kết quả cũ để thành ACTIVE. Không chia sẻ Session giữa threads.
- Health chỉ cập nhật storage_nodes; không lấy operation_lock, không chỉnh
  File/Chunk/ChunkReplica và không enqueue repair.
- DB lỗi: log có giới hạn, rollback và đóng Session; không áp dụng snapshot chưa
  commit như trạng thái authoritative. Loop không chết vĩnh viễn. Live vẫn phản
  hồi; ready/nodes/cluster phản ánh DB unavailable. Khi DB trở lại, lượt tiếp
  theo thành công phải cập nhật state nhất quán.
- Shutdown: ngừng lịch poll, chờ RPC đã chạy trong deadline, join worker/executor,
  đóng channels, cuối cùng dispose engine. Không có write DB sau dispose.
- Giữ ready phụ thuộc DB và initialization; một/all Storage DOWN không làm
  ready trả 503. Initialization thất bại vẫn cần sửa nguyên nhân rồi restart,
  không tự thêm bootstrap retry ngoài phạm vi hiện có.

### Kiểm tra và gate

- Integration qua server gRPC thật từ production factory và PostgreSQL schema
  riêng: success cập nhật DB; timeout/identity/unwritable đúng state.
- Một node bị treo: hai node tốt vẫn được poll đúng lịch; GET live vẫn phản hồi.
- Giữ operation_lock trong test: health vẫn tiến triển, không sửa replica rows.
- Kiểm tra lifespan start/stop/start ở các app instances riêng, startup fail
  không tạo worker, DB-write failure rồi recovery qua fault injection và DB thật.
- Có kiểm tra thread/channel được đóng và không ghi DB sau shutdown.

**Gate:** focused worker integration qua trong Docker Linux. Pure mocks không
thay thế gate gRPC/DB; tests dùng server tạm không cần ba Storage containers live.

## 7. P4 — GET nodes và cluster

### GET /api/v1/nodes

- Trả `{items: [...]}` với fields chính xác theo contract, bao gồm node disabled.
- Timestamp RFC 3339 UTC; metrics chưa có success là null; không giả thành 0.
- Đọc DB snapshot, không gọi HealthCheck trong request, không lấy operation_lock.
- Chọn node_id ASC để output/test/demo ổn định; không thêm pagination ngoài contract.

### GET /api/v1/cluster

- Trả chunk_size_bytes, default_replication_factor, max_file_size_bytes từ settings.
- active/suspected/down chỉ đếm enabled; disabled đếm riêng.
- configured_failure_domains đếm distinct domain của enabled nodes;
  active_failure_domains đếm distinct domain của enabled ACTIVE nodes.
- files_available chỉ đếm AVAILABLE. Bốn chunk counters chỉ xét chunks của
  AVAILABLE; lấy RF snapshot của từng file, không lấy default RF hiện tại.
- Live replica: VERIFIED + node enabled ACTIVE + cleanup_pending=false.
  UNDER_REPLICATED khi 1..RF-1; UNAVAILABLE khi 0; OVER_REPLICATED khi >RF.
- Domain degraded khi distinct live domains < min(RF của file,
  số configured enabled domains). Các counters có thể cùng tăng cho một chunk;
  ví dụ chunk unavailable cũng có thể domain degraded.
- cleanup_pending_replicas đếm pending rows trên mọi file chưa cleanup xong;
  operation_busy chỉ đọc snapshot lock, không acquire lock.
- Dùng aggregate/query theo tập dữ liệu, tránh một query cho từng chunk;
  dùng một DB read snapshot nhất quán cho summary. Không hard-code counters=0
  dù M2 chưa có endpoint tạo file; dữ liệu fixture/mappings cũ vẫn phải được đếm.
- DB/startup unavailable trả 503 METADATA_UNAVAILABLE theo error envelope.

### Kiểm tra và gate

Focused HTTP + PostgreSQL integration: exact keys/types/null/UTC, disabled,
counters từ fixture nhiều trạng thái file/replica/node/domain, RF khác nhau,
cleanup pending và lock busy. Bao gồm file AVAILABLE rỗng không thêm chunk count.
Giữ lock vẫn GET được; assert GET không phát RPC. DB fail trả đúng 503,
live vẫn 200; all nodes DOWN nhưng initialized/DB tốt thì ready vẫn 200.

**Gate:** response đúng API_CONTRACTS, không sửa contract để hợp code.

## 8. P5 — Kịch bản Compose và persistence thật

### Điều kiện trước khi chạy

Preflight read-only Docker Linux, PostgreSQL healthy, Metadata ready và ba
Storage healthy, env đã có. Build image chứa code M2; cập nhật Metadata bằng
`up -d --no-deps --wait metadata` khi triển khai đã được cho phép, giữ named volumes.
Storage không đổi code thì không rebuild/recreate chỉ để qua phase.

Kịch bản tác động service chỉ chạy trong môi trường local dev đã dành cho check.
Ghi nhận trạng thái ban đầu; script báo bước đang chạy, có finally bật lại node
đã tắt, và chỉ dọn fixtures/schema do chính script tạo. Không dùng down -v,
reset data hoặc ghi file fixture vào metadata đang dùng của ứng dụng.

### Kịch bản bắt buộc

1. **Baseline qua REST:** đợi trong thời hạn hữu hạn đến khi ba node ACTIVE;
   assert IDs/endpoints/domain, metrics không null; cluster active=3,
   configured/active domains=1 ở local dev_host. Ready và live đều 200.
2. **Node down:** stop storage-node-2; xác minh hai node khác vẫn ACTIVE.
   Kiểm tra worker bằng timeline của health success cuối và failures; chờ
   REST phản ánh DOWN với timeout hữu hạn. SUSPECTED phải có evidence worker
   integration P3; REST smoke ghi nhận nếu quan sát được, không đòi một trạng
   thái ngắn luôn bị bắt gặp. Down-after 10 s có sai số quan sát tới khoảng
   một poll interval + RPC deadline, không hứa DOWN đúng giây thứ 10.
3. **Recovery:** start node-2 bằng cùng volume; health hợp lệ đưa ACTIVE lại,
   last_error clear, cluster counts phục hồi; ready không mất chỉ vì node down.
4. **Metadata restart:** restart process thật với PostgreSQL còn chạy. Isolated
   schema fixture chứa files/chunks/replicas và node snapshots phải giữ IDs,
   hashes, mappings, AVAILABLE, DELETING và pending flags; UPLOADING được đổi
   FAILED đúng startup recovery. Node status reset DOWN trước poll mới, rồi
   ACTIVE sau success. Kiểm tra reset tức thời trong lifecycle test, tránh
   tranh timing với REST của service đã poll xong.
5. **PostgreSQL restart:** seed fixture trong schema UUID riêng trên cùng DB,
   restart PostgreSQL giữ named volume, đọc lại và so sánh toàn bộ fixture.
   Không dùng Base.metadata.create_all sau restart trước assertion để che mất
   schema/data. Chờ ready hồi phục và worker tiếp tục poll; không chỉ assert
   DB ping để gọi là persistence passed.
6. **Ba volumes độc lập:** inspect mounts của cả ba node khác named volume.
   Tận dụng evidence M1 nếu topology/build Storage không đổi; không lặp Store/Get
   persistence M1 chỉ để đánh dấu M2. Nếu volumes đổi, cần lại kiểm tra isolation.

`test_metadata_lifecycle.py` dùng schema riêng và child Metadata process thật
cho restart; không coi gọi initialize_metadata hai lần là process restart.
Smoke PowerShell điều phối Compose stop/start/restart và Python assertions,
bao gồm fixture schema qua DB restart; tất cả waits có timeout và lỗi rõ ràng.

**Gate:** các kịch bản thực đã có lệnh, thời điểm, expected/actual và cleanup;
không coi direct HealthCheck của smoke_base là detector/down/recovery evidence.
Chỉ chứng minh lỗi process local, chưa có replication/failover hay host failure.

## 9. P6 — Rà DoD và bàn giao M3

- Rà code với contract: interval/deadline/threshold, disabled, hard-DOWN,
  null/history metrics, counters, DB error và shutdown.
- Ghi evidence từng phase ngay sau chạy, gồm command, môi trường, passed/failed/
  skipped và dependency blockers. Không đánh dấu gate DB khi pytest skip.
- Cập nhật README/docs theo API/lệnh thật; đánh dấu M2 chỉ khi toàn bộ DoD đạt.
- M3 nhận registry/health snapshot đáng tin để lọc node enabled ACTIVE; free
  space vẫn chỉ là snapshot, không reservation. M3 tự xác minh Store/Get bytes
  và replica states; không diễn giải ACTIVE thành VERIFIED.
- Không tự chạy full suite khi tới P6. Rà evidence đã có và chạy phần thiếu hoặc
  bị ảnh hưởng bởi sửa code mới. Full validation chỉ khi user yêu cầu, cần
  merge/release readiness hoặc implementation thực sự đổi cấu trúc cross-cutting.

## 10. Test gate và commands theo stack

**Lượt lập kế hoạch ban đầu:** documentation-only, tier no execution; không chạy
tests/lint/build/smoke, không thay trạng thái service. Ở thời điểm lập kế hoạch,
tests/scripts M2 bên dưới chưa tồn tại; hiện đã triển khai và có evidence P1–P5.

Trước mỗi lần thực thi: báo tier, exact command và lý do đủ; sau đó ghi kết quả
và môi trường. Không rerun passing checks nếu code/deps/config/environment liên
quan không đổi. Lint không là bằng chứng runtime.

### P2 targeted và lint phần thay đổi — từ backend/

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_metadata_health.py
.\.venv\Scripts\python.exe -m ruff check metadata tests/test_metadata_health.py
.\.venv\Scripts\python.exe -m ruff format --check metadata tests/test_metadata_health.py
```

Ở phase khác, thay danh sách lint bằng các paths thực sự thay đổi, bao gồm
scripts/fixtures nếu sửa; không giữ nguyên danh sách ví dụ để bỏ sót file mới.

### Preflight integration — từ repo root

```powershell
docker info
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
```

P1/P3/P4/P5 tests trong container cần PostgreSQL healthy và TEST_DATABASE_URL
do tests service cấp. P3/lifecycle gRPC dùng server tạm trong tests container;
không cần live Storage. Compose smoke P5 cần Metadata/cả ba node/DB hiện hành.

Build tests khi code/dependency/config được copy vào image thay đổi, rồi chỉ
chạy đúng phase; mọi command container chạy ở /app:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
# P1: registry/database boundary
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_bootstrap.py
# P3: worker, real gRPC và PostgreSQL
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_worker.py
# P4: HTTP + DB snapshots/counters; giữ regression live/ready liên quan
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_cluster.py tests/test_base.py::test_metadata_db_down_still_live_but_not_ready
# P5: process lifecycle với schema riêng
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_lifecycle.py
```

Điều chỉnh exact command theo tên/path thật khi triển khai và báo deselected/skip.

### Deploy code M2 và smoke P5 — sau khi setup/check service được cho phép

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml build metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --no-deps --wait metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec metadata python scripts/smoke_metadata.py
powershell -ExecutionPolicy Bypass -File backend/scripts/smoke_metadata.ps1
```

Smoke Python mặc định read-only baseline; PowerShell script là kịch bản có tác
động stop/start/restart đã mô tả ở P5. Không tự chạy hai script trong lượt chỉ
lập kế hoạch hoặc test-only chưa được phép tác động service.

Nếu Docker/DB/API hoặc dependency được chọn chưa sẵn sàng: dừng check phụ thuộc,
báo prerequisite lỗi chính xác, không retry aggregate suite. Recovery command
hiện có từ repo root (cần deploy/.env và build đúng):

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait
```

Với test-only, báo lệnh và chờ user xác nhận readiness; nếu đã được cho phép
setup/repair thì thực hiện trong scope đó, giữ volumes. Khi ready, rerun đúng
command bị chặn một lần. Không chạy frontend, data replication/repair hoặc
demo hai máy để xác minh M2. Chỉ chạy alembic check nếu models/migration thay đổi
hoặc có nghi vấn schema drift cụ thể.

## 11. Definition of Done và evidence

- [x] P1: registry/reset/disabled giữ đúng mappings; bootstrap failure giữ initialized=false/ready=503 (03/10/2026).
- [x] P2: monotonic transitions, identity/domain, unwritable và recovery đúng (03/10/2026).
- [x] P3: polling độc lập, transaction ngắn, Sessions riêng, lifespan/shutdown sạch (03/10/2026).
- [x] P4: nodes/cluster đúng contract và counters thật; GET không gọi RPC (03/10/2026).
- [x] Live/ready/503 đúng khi Storage down hoặc DB unavailable (P3–P4 integration; Storage down trên Compose ở P5).
- [x] P5: ba node ACTIVE → node down → recovery được quan sát qua Metadata (03/10/2026).
- [x] P5: process Metadata restart giữ metadata và thực hiện startup recovery đúng (03/10/2026).
- [x] P5: PostgreSQL restart với named volume giữ fixture data/schema (03/10/2026).
- [x] Ba Storage volumes độc lập, topology local chỉ có một failure domain (P5 inspect; reuse M1 byte-isolation evidence).
- [x] P6: docs, commands và evidence cập nhật; bàn giao rõ ranh giới cho M3 (03/10/2026).
- [x] **M2 hoàn thành:** tất cả gate có bằng chứng thật, không còn blocker bắt buộc (03/10/2026).

Evidence sẽ bổ sung tại đây theo phase: ngày, commands thực tế, environment,
expected/actual, counts, skipped/deselected, cleanup và checks chủ ý không chạy.
Kết quả M1 chỉ là đầu vào; evidence M2 được ghi theo từng phase bên dưới.

### Bằng chứng P1 — 03/10/2026

Đã hoàn thiện `metadata/bootstrap.py`: đổi host/port hoặc domain của node chưa
có mapping sẽ xóa last_success_at/capacity/available/used snapshot của endpoint
cũ. Cùng endpoint/domain giữ lịch sử health, nhưng status luôn reset DOWN.
Remove/re-enable giữ node rows và replica mappings. Domain change khi có mapping
vẫn bị từ chối, rollback toàn transaction gồm update các node trước đó và insert
node mới. Models/migration/config không cần đổi.

`metadata/main.py` thực hiện initialize/recovery dưới operation_lock trong
threadpool. initialized chỉ true sau commit thành công; lỗi bootstrap giữ live
200 và ready 503, nhả lock; shutdown reset initialized=false. Chưa tạo detector,
monotonic state hay worker: các phần đó sẽ khởi tạo mới ở P2/P3, không lấy UTC
last_success_at trong DB để khôi phục ACTIVE.

Tier **focused integration** vì boundary được sửa là PostgreSQL transaction và
FastAPI lifespan. Preflight đọc Docker Linux/PostgreSQL healthy; sandbox ban đầu
chặn Docker named pipe, truy cập đã được cấp qua escalation. Không có dependency
blocker sau preflight. Dùng tests image đã rebuild chứa code/tests hiện tại,
Python 3.12 trong Docker Linux, PostgreSQL 16 đang chạy, TEST_DATABASE_URL do
Compose cấp. Tests tạo/dọn schema UUID riêng, không sửa app tables.

Commands thực tế:

```powershell
# Từ backend/: check cuối sau khi format file test
.\.venv\Scripts\python.exe -m ruff check metadata/bootstrap.py metadata/main.py tests/test_bootstrap.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/bootstrap.py metadata/main.py tests/test_bootstrap.py
# Từ repo root: preflight và integration
docker info --format '{{.OSType}}'
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps postgres
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_bootstrap.py tests/test_base.py::test_metadata_db_down_still_live_but_not_ready
```

Kết quả: **16 passed**, không skipped/deselected; Ruff check qua, format --check
3 files qua. Lần lint đầu phát hiện ba dòng test quá dài, đã sửa bằng formatter
trước gate cuối. Một warning Starlette/httpx đã biết, không ảnh hưởng assertions.

Cases bao gồm registry lần đầu/lặp lại, null metrics, reset ACTIVE/SUSPECTED/DOWN
giữ lịch sử, host/port/domain change, remove/re-enable giữ replica observations,
rollback cho PENDING/VERIFIED/DELETED mappings, recovery của các file states và
lifespan success/failure giữ lock/readiness đúng. Tests lifespan dùng DB/schema
thật với factory được trỏ vào schema riêng; chưa là process/container restart.

Không chạy full backend suite, Storage/gRPC tests, frontend, alembic check hoặc
cluster smoke vì các phần đó không đổi/ngoài P1. Không deploy/restart Metadata,
PostgreSQL hay Storage services; chỉ image tests đã cập nhật. P5 vẫn phải chứng
minh process/DB restart thật; P2–P6 và M2 chưa hoàn thành. Tiếp theo: P2 state machine.

### Bằng chứng P2 — 03/10/2026

Đã thêm `metadata/health.py` gồm immutable `HealthSnapshot` và `HealthDetector`.
Detector nhận NodeConfig, down-after cấu hình và hai clock có thể inject;
không gọi RPC, mở DB hoặc tạo thread. Các methods nhận snapshot trước đó và trả
candidate mới, không tự publish state: P3 sẽ commit DB trước khi nhận candidate.

Success đúng identity/domain và writable chuyển ACTIVE, thay metrics, cập nhật
monotonic/UTC, clear error/hard-down. Identity sai/unwritable chuyển DOWN ngay,
không nhận metrics lỗi; transient failure tiếp theo giữ lý do chặn và DOWN.
Failure thông thường là SUSPECTED khi age < down-after, DOWN khi age >= threshold;
chưa có success của process hiện tại luôn DOWN. RPC error chỉ lấy tên StatusCode,
không dùng raw exception/details. Disabled bỏ qua response, reset monotonic age;
re-enable cần success mới. Timestamp hiển thị chuẩn hóa UTC và không dùng làm age.

Tier **targeted tests**, chạy native Windows bằng `backend/.venv` Python 3.12.
Tests tạo protobuf response trong bộ nhớ và dùng fake clocks, không mở gRPC
loopback hay cần Docker/PostgreSQL. Commands từ backend/:

```powershell
.\.venv\Scripts\python.exe -m ruff format metadata/health.py tests/test_metadata_health.py
.\.venv\Scripts\python.exe -m pytest -q tests/test_metadata_health.py
.\.venv\Scripts\python.exe -m ruff check metadata/health.py tests/test_metadata_health.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/health.py tests/test_metadata_health.py
```

Kết quả: **32 passed**, không skipped/deselected; Ruff check qua, format --check
2 files qua. Một PytestCacheWarning do sandbox từ chối ghi pytest cache;
không có assertion failure, không cần retry các tests đã pass để xóa warning.

Cases gồm age 9.999/10/10.001 và monotonic=0, timeout/unreachable, failures lặp
lại, recovery/reset age, identity/domain sai, identity ưu tiên trước writable,
unwritable giữ DOWN, disabled/re-enable, UTC nhảy ±365 ngày, UTC timezone,
null so với metrics=0, ngưỡng tùy chỉnh và snapshot mới từ lịch sử sau restart.
Case restart ở đây chỉ kiểm tra state mới trong bộ nhớ, chưa là process restart.

Không rerun P1/DB/Storage/full suite/frontend vì code liên quan không đổi;
không build Docker image, deploy/restart services hoặc chạy smoke. Chưa có RPC
deadline/retry/scheduling hay persistence evidence P2: P3 sẽ nối settings
interval=3 s/deadline=1 s, RPC một lần mỗi lượt và commit snapshots vào DB.
P3 phải tạo snapshot mới từ các DB history fields, để process-local monotonic
và hard_down dùng defaults; không restore ACTIVE từ UTC timestamp cũ.
Tiếp theo P3 worker/lifespan; P3–P6 và M2 chưa hoàn thành.

### Bằng chứng P3 — 03/10/2026

Đã thêm `metadata/worker.py`, nối `MetadataWorker` vào lifespan sau startup
registry/recovery thành công. Không tạo threads/channels ở import hoặc create_app.
Worker có một scheduler và executor hữu hạn theo số configured nodes; poll ngay
lần đầu, rồi theo interval riêng, tối đa một future/node đang chạy. Channels/stubs
được tái dùng. RPC dùng timeout từ settings, wait_for_ready=false và options
grpc.enable_retries=0; không retry trong cùng lượt. Defaults vẫn 3 s/1 s/10 s.

Mỗi poll đọc registry/history bằng Session riêng, đóng transaction trước RPC,
rồi mở transaction khác để lock/recheck node và persist. Chỉ publish candidate
sau commit; DB lỗi rollback, giữ snapshot đã commit và thử ở interval kế tiếp.
Kết quả muộn của node đã disable không được ACTIVE; endpoint/domain đổi thì bỏ
kết quả của config cũ. Disabled nodes không được gọi RPC. DB history không khôi
phục monotonic/hard-down của process trước. Poll không giữ operation_lock hay
chỉnh replica/file/chunk rows.

SQL worker dùng SET LOCAL statement_timeout=5 s để giới hạn chờ query/row lock;
poll lỗi chỉ log node_id và exception class, tối đa một warning/node/30 s,
không log raw exception/credentials. Ready vẫn phản ánh DB/initialization,
không yêu cầu Storage ACTIVE. Nodes/cluster APIs chưa có, thuộc P4.

Shutdown ngừng scheduler, drain/cancel executor jobs, đóng channels rồi mới
dispose engine; blocking start/stop/dispose chạy ngoài event loop. Stop idempotent;
partial start failure cũng đóng các resources đã tạo và giữ ready=503.
Fixture PostgreSQL chuyển từ test_bootstrap.py vào conftest.py để dùng chung;
Storage fixtures M1 giữ nguyên. P3 có fixture server từ production factory với
DATA_DIR riêng cho từng node, hook lỗi chỉ nằm trong test service.

Tier **focused integration**, Docker Linux/Python 3.12 và PostgreSQL 16 healthy
qua preflight. Tests image rebuild chứa code P3; TEST_DATABASE_URL do Compose
cấp. Tests tạo/dọn schema UUID riêng, không ghi app tables; server gRPC là server
thật từ production factory trong tests container, không cần live Storage services.
Tests giảm interval/deadline/down-after để kiểm tra transitions nhanh; không
diễn giải timing rút ngắn đó thành evidence defaults trên cluster đang chạy.

Commands thực tế (lint từ backend/, Docker từ repo root):

```powershell
docker info --format '{{.OSType}}'
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps postgres
.\.venv\Scripts\python.exe -m ruff check metadata/worker.py metadata/main.py tests/conftest.py tests/test_bootstrap.py tests/test_metadata_worker.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/worker.py metadata/main.py tests/conftest.py tests/test_bootstrap.py tests/test_metadata_worker.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_worker.py tests/test_bootstrap.py tests/test_base.py::test_metadata_db_down_still_live_but_not_ready
```

Kết quả: **32 passed** (16 worker cases + 15 bootstrap + 1 DB-down live/ready),
không skipped/deselected; Ruff check và format --check 5 files qua. Một warning
Starlette/httpx đã biết. Import order và format được sửa trước các gate cuối;
không có assertion failure hoặc dependency blocker trong integration run.

Evidence gồm success metrics qua RPC/DB, zero connection checkout trong lượt RPC
được gate bằng Event, SUSPECTED → DOWN → recovery khi deadline thật, identity/
domain/unwritable bị từ chối, ba node poll độc lập khi một node chậm, không overlap
client calls, disabled/re-enable và registry đổi khi RPC còn đang chạy.
DB-write failure được inject sau SQL flush thật: rollback và không publish
candidate; bỏ fault thì lượt sau commit. Read-outage injection kiểm tra worker
vẫn sống, live=200/ready=503 rồi ready=200 khi fault bỏ, không restart DB thật.
Có checks operation_lock không chặn health, replica PENDING giữ nguyên, all
Storage DOWN vẫn READY, shutdown drain/closed channel/threads dừng, hai lifespan
instances dùng worker mới, dispose sau stop và cleanup khi start lỗi giữa chừng.

Không chạy lại P2 unit tests, Storage data suites, full backend/frontend,
alembic check hoặc Compose smoke: các phần đó không đổi/ngoài P3. Chỉ image tests
được rebuild; chưa build/deploy/restart Metadata, PostgreSQL hay Storage services.
Chưa có REST nodes/cluster, process/container restart, DB restart hoặc host
failure evidence ở P3; P4–P6 và M2 chưa hoàn thành. Tiếp theo P4 REST snapshots.

### Bằng chứng P4 — 03/10/2026

Đã thêm `metadata/schemas.py`, `metadata/cluster.py`, `metadata/routes/cluster.py`
và đăng ký router trong create_app. GET `/api/v1/nodes` trả NodeList, node_id ASC,
bao gồm disabled nodes; DTO giữ null metrics khi chưa có last_success_at, giữ 0
khi health đã success và serialize timestamp RFC 3339 UTC với Z. Không public
process-local monotonic/hard_down và không cộng capacity/free thành cluster capacity.

GET `/api/v1/cluster` lấy cấu hình từ settings, counts từ DB bằng một câu SELECT
với các CTE aggregates. Mọi aggregate dùng cùng MVCC snapshot của statement;
không query riêng từng chunk. Node counts ACTIVE/SUSPECTED/DOWN chỉ tính enabled;
domains configured tính enabled, active domains tính enabled ACTIVE. File/chunk
counts chỉ xét AVAILABLE, RF lấy snapshot từng file. Live replica chỉ gồm
VERIFIED + enabled ACTIVE + không cleanup_pending. Domain degraded so sánh
distinct live domains với min(file RF, configured enabled domains); counters
có thể overlap. Cleanup counter đếm mọi cleanup_pending row, không loại file lỗi
hoặc đang xóa. File AVAILABLE rỗng vẫn tăng files_available, không tăng chunk count.

Cả hai GET đọc DB, không gọi RPC hoặc acquire operation_lock; operation_busy
chỉ là quan sát lock tại thời điểm request. Startup chưa initialized hoặc DB lỗi
trả 503 METADATA_UNAVAILABLE theo envelope hiện có. SQLAlchemy failures chỉ log
exception class, không raw SQL/password; Session/transaction đóng khi lỗi và
request sau chạy lại bình thường khi DB có thể dùng.

Tier **focused integration**, Docker Linux/Python 3.12 với PostgreSQL 16 healthy
ở preflight, tests image đã rebuild. Tests dùng schema UUID riêng và HTTP qua
FastAPI TestClient thật. Fixture chạy lifespan, dừng poll worker rồi seed snapshot
DB để assertions REST xác định; polling đồng thời/RPC thuộc evidence P3, không
coi fixture này là runtime cluster smoke. Không dùng live Storage containers.

Commands thực tế (lint từ backend/, Docker từ repo root):

```powershell
docker info --format '{{.OSType}}'
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps postgres
.\.venv\Scripts\python.exe -m ruff check metadata/cluster.py metadata/schemas.py metadata/routes metadata/main.py tests/test_metadata_cluster.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/cluster.py metadata/schemas.py metadata/routes metadata/main.py tests/test_metadata_cluster.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_cluster.py tests/test_base.py::test_metadata_db_down_still_live_but_not_ready
```

Kết quả: **32 passed** (31 P4 cases + 1 DB-down live/ready), không skipped/deselected;
Ruff check và format --check 6 files qua, gồm routes/__init__.py. Một warning
Starlette/httpx đã biết; không có assertion failure hoặc prerequisite blocker.

Evidence: exact response keys/types, sorted disabled/history nodes, unknown null
và valid zero metrics, UTC timestamp, empty DB/AVAILABLE rỗng, enabled/disabled
counts, tất cả replica states và cleanup/node eligibility, RF=1/2/3 trong cùng
cluster, counters overlap và domains của enabled DOWN so với disabled nodes.
Inactive file states bị loại khỏi chunk counters nhưng pending cleanup vẫn đếm.
Giữ operation_lock vẫn GET=200 và busy=true; patch cả channel creation/HealthCheck
để fail nếu GET phát RPC. SQL fault injection cho mỗi endpoint xác nhận 503,
không leak details, live=200, ready=503, không giữ checkout và request sau hồi phục.

Query instrumentation xác nhận một SQL/GET ở dữ liệu 1 và 40 chunk. Một transaction
khác commit node disable và file mới sau SELECT execution nhưng trước đọc response:
summary đầu giữ toàn bộ counters cũ, request sau thấy toàn bộ counters mới.
Đây là bằng chứng DB snapshot nhất quán; operation_busy vẫn là snapshot riêng
của in-process lock, không tuyên bố atomic với transaction DB.

Không rerun passing P1/P2/P3 suites, Storage data tests, full backend/frontend,
alembic check hoặc Compose smoke vì các phần đó không đổi/ngoài P4. Models,
migration, config, worker và wire contract giữ nguyên. Chỉ rebuild tests image,
chưa deploy/restart Metadata/PostgreSQL/Storage services. P5 vẫn cần baseline
REST trên cluster thật, stop/start node, Metadata/DB restart và volume evidence;
P5–P6 và M2 chưa hoàn thành. Tiếp theo P5 lifecycle/Compose smoke.

### Bằng chứng P5 — 03/10/2026

**Tier:** focused integration/E2E cho process/HTTP/gRPC/PostgreSQL và service
lifecycle. Native Windows chỉ chạy lint/format và PowerShell syntax parser;
runtime checks chạy Docker Linux, Python 3.12, PostgreSQL 16. Preflight
`docker info --format '{{.OSType}}'` trả `linux`; `compose ps` xác nhận PostgreSQL,
Metadata và cả ba Storage containers healthy. Không có dependency blocker.

Đã thêm `tests/test_metadata_lifecycle.py`, `scripts/metadata_fixture.py`,
`scripts/smoke_metadata.py` và `scripts/smoke_metadata.ps1`. Python smoke mặc định
chỉ đọc REST; fixture commands dùng schema `m2_smoke_<UUID hex>` riêng. PowerShell
có preflight, waits hữu hạn, finally bật lại node nếu check thất bại và dọn schema
do lượt chạy tạo; không reset app data hoặc xóa volumes.

Commands thực tế từ repo root:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q --tb=short tests/test_metadata_lifecycle.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --no-deps --wait metadata
powershell -ExecutionPolicy Bypass -File backend/scripts/smoke_metadata.ps1

# Read-only check thêm sau DB restart: health timestamps phải mới hơn thời điểm này.
$after = [DateTime]::UtcNow.ToString('o')
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec -T metadata python scripts/smoke_metadata.py baseline --after $after
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
```

Lifecycle test **1 passed trong 3,29 s**, không skipped/deselected. Hai child
Metadata processes chạy Uvicorn thật, khác PID và dùng cùng schema PostgreSQL
riêng cùng một gRPC Storage server thật. Test-only startup barrier chặn worker
sau initialize, xác minh toàn bộ fixture columns chỉ có thay đổi recovery được
đặc tả: reset node DOWN/disabled, UPLOADING → FAILED/UPLOAD_INTERRUPTED và cleanup
flags cho replicas chưa DELETED. Sau release, REST ready/live và health ACTIVE
qua; lần restart thứ hai giữ IDs/checksums/mappings, AVAILABLE/DELETING và các
pending flags. Harness tạo UPLOADING trong process đầu trước khi dừng để lần
restart thứ hai thực sự kiểm tra interrupted upload. Shutdown marker chỉ xuất
hiện sau worker.stop hoàn tất; schema do database fixture dọn.

Lần chạy đầu thất bại ở assertion exit code `0`: Uvicorn bản hiện tại re-raise
SIGTERM sau graceful shutdown nên child trả `-15`. Đã xác minh hành vi trong
source Uvicorn local, thêm marker worker shutdown và cho phép `0`/`-SIGTERM`;
rebuild riêng tests image rồi rerun duy nhất test lỗi. Không sửa production
service để xử lý lỗi assertion này.

Compose smoke (khoảng **15:12–15:15 UTC**, 03/10/2026) passed:

- Baseline REST đúng node-1/2/3, storage-node-1/2/3:50051/50052/50053,
  domain dev_host; metrics non-null, used_bytes=0. Cluster active=3,
  configured/active domains=1, RF=2, chunk=2097152, max file=67108864;
  live/ready đều HTTP 200. Không seed file fixtures vào app schema.
- Stop node-2: REST quan sát ACTIVE ở age 3,14 s, SUSPECTED ở 4,18 s,
  DOWN ở **13,47 s** từ last success, error DEADLINE_EXCEEDED. Đây là cửa sổ
  quan sát phù hợp threshold 10 s + polling/deadline, không cam kết đúng giây 10.
  Hai node khác ACTIVE; live/ready vẫn HTTP 200 trong toàn bộ down observation.
- Start lại cùng container/volume: node-2 ACTIVE, error clear, cluster active=3.
  Restart Metadata thật: readiness và ba node ACTIVE hồi phục với timestamps mới.
- Seed schema `m2_smoke_4118e6fcf9034e18bd367884cf7851df`, restart PostgreSQL
  giữ named volume, đọc lại trực tiếp (không create_all trước assertion).
  SHA-256 toàn bộ columns/rows của **5 files, 5 chunks, 4 nodes, 6 replicas**
  giữ nguyên: `c60fa5b3dce8bfcd35fe3d28ae12a2b839494a38b89c53af93cbdd1026145678`.
  Bao gồm AVAILABLE/UPLOADING/DELETING/FAILED/DELETED, hashes, mappings, flags,
  errors và historical metrics. Finally đã drop đúng schema này.
- Sau restart DB, read-only check bổ sung yêu cầu cả ba last_success_at mới hơn
  cutoff lấy sau restart; passed ở khoảng 15:15:19 UTC. Script hiện lấy cutoff
  sau khi lệnh restart trả về để loại snapshot ghi ngay trước DB restart.
- Inspect xác nhận node-1-data, node-2-data, node-3-data là ba named volumes khác
  nhau; cả chúng và postgres-data giữ nguyên tên sau lifecycle. Topology Storage
  và source không đổi, reuse byte-isolation/persistence evidence M1. Chỉ Metadata
  được recreate sang build M2; Storage containers giữ build M1 đã kiểm tra.

Native checks từ backend/: `ruff check` và `ruff format --check` cho
`scripts/metadata_fixture.py scripts/smoke_metadata.py tests/test_metadata_lifecycle.py`
passed (3 files); PowerShell parser của smoke script không có syntax errors.
Trạng thái cuối: Metadata/PostgreSQL/cả ba Storage healthy; REST ba node ACTIVE,
app files_available=0, cleanup_pending=0. Không còn fixture schema của lượt smoke.

**Không chạy chủ ý:** full suite P1–P4/M1, frontend, alembic drift check (models/
migrations không đổi), lặp Store/Get volume isolation M1, replication/download
failover/repair hoặc demo hai host. Những luồng bytes/placement thuộc M3 trở đi;
P5 chỉ chứng minh health/process faults trong local dev_host. Còn P6 rà DoD và
bàn giao M3; chưa đánh dấu toàn bộ M2 hoàn thành.

### Bằng chứng P6 — 03/10/2026

**Kết luận:** M2 hoàn thành P1–P6; không phát hiện gap bắt buộc còn thiếu sau
đối chiếu source, contracts và evidence từng phase. Đây là rà DoD/bàn giao
milestone, chưa là merge/release validation.

Rà `bootstrap.py`, `health.py`, `worker.py`, `main.py`, `cluster.py`, DTO/routes,
settings, Compose và lifecycle scripts với `ARCHITECTURE.md` mục 2–4/8,
`API_CONTRACTS.md` mục 2.6 và HealthCheck. Ma trận gate:

| Gate | Source/hành vi đã rà | Evidence đã chạy |
|---|---|---|
| Registry/startup | Một transaction dưới operation_lock; reset DOWN, giữ rows/mappings/history cùng endpoint; xóa history khi endpoint đổi; domain có mapping không được đổi | P1 bootstrap/rollback/lifespan; P5 child process reset trước poll |
| Detector | Defaults interval 3 s/deadline 1 s/down-after 10 s; age monotonic, UTC chỉ hiển thị; identity/unwritable hard-DOWN đến valid success; disabled không khôi phục ACTIVE từ lịch sử | P2 threshold/wall-clock/identity/writable/zero/null; P3 RPC errors/recovery; P5 timeline defaults |
| Poll/persistence | Một future/node, independent scheduling; channel reuse, không retry health; Session/transaction riêng trước/sau RPC; recheck registry và publish sau commit | P3 slow node/no overlap/no checkout during RPC/late registry change/write rollback |
| Lifecycle/DB lỗi | Không tạo worker ở import; start sau initialization; stop/drain/close trước dispose; lỗi init/DB giữ live và trả ready 503; node DOWN không làm ready 503 | P1/P3 startup/partial start/shutdown/outage; P4 HTTP errors; P5 real process/DB restart |
| Nodes REST | Sorted IDs, disabled included, metrics chưa success là null, zero vẫn zero, timestamp UTC; GET không gọi RPC | P4 exact keys/null/UTC/disabled/busy lock; P5 live identity/endpoints/metrics |
| Cluster REST | Một SQL snapshot; live = VERIFIED + enabled ACTIVE + không cleanup_pending; chunk counters chỉ AVAILABLE, RF của file; domain target gồm enabled DOWN; cleanup đếm mọi pending row | P4 RF=1/2/3, inactive/empty files, eligibility, constant query count và concurrent commit |
| Local deployment/persistence | Một Uvicorn worker, ba node dev_host, ba volumes khác nhau; named DB volume giữ nguyên, fixture schemas riêng được dọn | P5 REST down/recovery và Metadata/PostgreSQL restart; reuse M1 byte-isolation evidence |

Kết quả test đã ghi ở các phase (không chạy lại trong P6):

| Phase | Passed | Môi trường và phạm vi |
|---|---:|---|
| P1 | 16 | Docker Linux, PostgreSQL 16; bootstrap + DB-down live/ready |
| P2 | 32 | Native Windows Python 3.12.14; pure detector với injected clocks |
| P3 | 32 | Docker Linux, PostgreSQL 16, gRPC thật; 16 worker + 15 bootstrap + 1 live/ready |
| P4 | 32 | Docker Linux, HTTP TestClient + PostgreSQL; 31 API/counter + 1 live/ready |
| P5 | 1 | Docker Linux, real Metadata child processes + PostgreSQL schema riêng + gRPC thật; Compose smoke riêng passed |

Các lượt cuối đều không skipped/deselected; không cộng counts thành số unique
tests hoặc gọi đây là một full-suite run vì bootstrap/live-ready được lặp như
regression có liên quan. Lint/format của các files thay đổi đã qua theo từng
phase; PowerShell syntax và Compose smoke có evidence riêng ở P5. Warnings
đã ghi ở P1–P4 và assertion SIGTERM đã sửa ở P5 không còn chặn DoD.

**Tier P6: no execution.** Chỉ sửa README/plan/evidence, không sửa runtime,
dependencies, models, migrations, contracts hoặc deployment. Không chạy
pytest/lint/build/smoke, không preflight hay restart services trong P6 vì không
có runtime check mới cần dependencies. Chỉ đọc source/test definitions/evidence
và rà diff văn bản. Trạng thái healthy/ba node ACTIVE được dẫn từ lượt cuối P5,
không tuyên bố đã đo lại trong P6. Không còn prerequisite blocker được ghi nhận
ở gate P5; không mở thêm full suite, frontend hoặc schema drift check.

### Bàn giao M3 — upload/download RF=2

M3 nhận bốn bảng metadata và Storage RPC M1, registry/health worker M2,
`operation_lock` trong `app.state`, session_factory/settings cùng GET nodes/cluster.
Placement đọc StorageNode bằng Session của thao tác, lọc enabled + ACTIVE và
available_bytes đủ cho chunk. ACTIVE đã qua identity/writable khi health success;
available_bytes vẫn là snapshot, không reservation hoặc cam kết Store sẽ thành công.

Các bất biến cần giữ khi nối data flow:

1. Một Metadata process/worker; upload và chuẩn bị download dùng operation_lock.
   Health và GET nodes/cluster tiếp tục chạy khi lock busy. Không chia sẻ Session
   giữa request và health thread; không giữ DB transaction trong lúc RPC.
2. Snapshot chunk_size/RF vào File mới; chọn hai node khác nhau, ưu tiên khác
   failure domain rồi fallback trong cùng domain theo contract. Local dev_host
   chỉ có một domain; đủ RF=2 chưa là chịu lỗi cả host.
3. Lưu UPLOADING, chunks và attempted PENDING mappings trước Store RPC. Chỉ
   VERIFIED/AVAILABLE sau ack hợp lệ và đủ RF cho mọi chunk. Health success
   không xác minh bytes và không được biến PENDING/MISSING thành VERIFIED.
4. Lỗi/cancel/timeout giữ attempted mappings, chuyển FAILED và cleanup_pending
   theo contract. Timeout là outcome chưa biết; không xóa mapping vì chưa có ack.
   Startup recovery đã có; cleanup worker/repair tiếp tục thuộc M4.
5. Download Get từng chunk, xác minh checksum/size, thử replica fallback theo
   contract, assemble tempfile và xác minh whole-file SHA-256 trước HTTP 200.
   Giữ các giới hạn retry/deadline, không phát response file chưa đầy đủ.

Điểm bắt đầu M3: validation/chunking và placement RF=2, tiếp đến upload
commit/failure, rồi download assembly/fallback và REST file list/detail theo
API_CONTRACTS. Gate M3 cần file nhiều chunk có hai replicas/chunk, checksum
download đúng và tắt một node vẫn đọc được; hiện **chưa có evidence** cho các
luồng đó. UI, repair/delete cleanup và triển khai hai host chưa hoàn thành.

### Review bổ sung và sửa findings M2 — 03/10/2026

Sau P6, review chuyên sâu đã tái hiện ba lỗi bằng probes native với mock DB/RPC,
không sửa app data: guard schema bị bỏ khi Python chạy `-O` có thể đi tới DROP
public CASCADE; health interval=inf được nhận và làm scheduler chết trong khi
ready vẫn 200; smoke coi node lịch sử disabled như node phải ACTIVE. Đã sửa:

- `scripts/smoke_metadata.py`: dùng require/SmokeCheckError tường minh cho mọi
  check, không còn assert; regex schema và fixture command được kiểm tra trước
  tạo DB engine. Digest mismatch vẫn fail dưới `-O`. Không dùng optimized mode
  để thử drop schema ứng dụng thật; regression destructive path dùng mock engine.
- `metadata/config.py`: mọi interval/deadline/time budget dùng FiniteFloat và
  gt=0, từ chối inf/-inf/nan/0/âm, kể cả inf đọc từ environment. Defaults không đổi.
- `metadata/worker.py`: running phản ánh scheduler thread/events; fatal scheduler
  exception dừng poll mới và log exception class, không raw diagnostics. OS wait
  được cap theo threading.TIMEOUT_MAX để giá trị hữu hạn lớn không OverflowError.
- `metadata/main.py`: ready kiểm tra initialized, worker.running và DB. Scheduler
  chết/dừng trả 503; live và GET snapshots vẫn đọc được khi DB dùng được. Storage
  DOWN hoặc RPC failure bình thường không làm mất readiness. Contract/README cập nhật.
- Smoke chỉ yêu cầu configured enabled nodes ACTIVE; giữ disabled rows trong
  output, đếm disabled riêng và không yêu cầu metrics/history của chúng. Quan sát
  down cho phép hai GET straddle một health commit, kể cả đổi active domain count.
  API test fixture giữ scheduler sống, no-op riêng poll work để seed snapshots xác định.

**Tier:** targeted checks cho settings/smoke và focused integration cho readiness/
worker/HTTP/process lifecycle. Native Windows Python 3.12.14; integration Docker
Linux/Python 3.12, PostgreSQL 16 healthy, gRPC server thật từ production factory,
schemas UUID riêng được dọn. Preflight đọc Docker Linux/Compose services healthy;
không có dependency blocker. Build tests/runtime images từ source sửa hiện tại.

Commands thực tế (native từ backend/, Docker từ repo root):

```powershell
.\.venv\Scripts\python.exe -m ruff check metadata/config.py metadata/worker.py metadata/main.py scripts/smoke_metadata.py tests/test_metadata_config.py tests/test_metadata_worker.py tests/test_metadata_cluster.py tests/test_smoke_metadata.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/config.py metadata/worker.py metadata/main.py scripts/smoke_metadata.py tests/test_metadata_config.py tests/test_metadata_worker.py tests/test_metadata_cluster.py tests/test_smoke_metadata.py
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider --tb=short tests/test_metadata_config.py tests/test_smoke_metadata.py tests/test_base.py::test_config_rejects_duplicate_nodes_and_impossible_rf tests/test_base.py::test_message_limit_must_fit_payload

docker info --format '{{.OSType}}'
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q --tb=short tests/test_metadata_worker.py tests/test_metadata_cluster.py tests/test_bootstrap.py::test_app_startup_holds_operation_lock_and_gates_readiness tests/test_base.py::test_metadata_db_down_still_live_but_not_ready tests/test_metadata_lifecycle.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --no-deps --wait metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec -T metadata python -O scripts/smoke_metadata.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
```

**Kết quả:** 54 native passed trong 9,14 s (33 config/scheduler wait, 19 smoke,
2 config regressions); 53 focused integration passed trong 21,94 s (18 worker,
31 API, 2 startup, 1 live/ready, 1 real lifecycle). Không skipped/deselected;
lint/format 8 files passed. Một warning Starlette/httpx đã biết trong mỗi pytest
lượt; không có assertion failure. Hai dòng test harness quá dài ở lượt lint đầu
đã sửa trước checks cuối. Tests optimized Python là child interpreter thật,
bao phủ unsafe schema trước engine creation, digest mismatch và HTTP validation;
DB/RPC ở nhóm smoke vẫn mock, không gọi đây là runtime persistence evidence.

Metadata đã recreate sang bản sửa; PostgreSQL và Storage giữ nguyên containers/
volumes, không stop/start chúng. Baseline REST read-only dưới `-O` passed khoảng
16:40:40 UTC: live/ready 200, node-1/2/3 ACTIVE, metrics có giá trị, cluster active=3,
configured/active domains=1, files_available=0 và cleanup_pending=0. Compose ps
xác nhận tất cả services healthy. Native disabled-history smoke regressions dùng
fixtures bao gồm disabled ACTIVE/SUSPECTED/DOWN và null metrics, không seed node
lịch sử vào app registry đang chạy.

Không chạy lại full suite, P2 detector unit tests, Storage data suites, frontend,
alembic drift hoặc Compose down/DB restart script: không có thay đổi ở các phần
đó. Real Metadata child lifecycle đã chạy lại vì main/worker đổi; evidence P5
DB restart/volumes vẫn được giữ. M2 tiếp tục hoàn thành; ba findings review đã
được sửa và có regression evidence, chưa triển khai data flows M3.
