# Mốc E — admission, capacity và fairness khi mở rộng instance

Ngày 29/10/2026; cập nhật E2/E3 ngày 30/10/2026. D1–D5 baseline và fixes review
trước E đã có evidence. C5/browser/file 1–10 GiB tiếp tục tạm gác theo phạm vi
đang làm; E không tự nghiệm thu C5.
Đọc cùng V2_DESIGN.md §4.2/§6/§7/§9, V2_METADATA_SCHEMA.md và V2_API_CONTRACTS.md.

## Các lát triển khai

| Phase | Phạm vi | Gate nhỏ nhất đủ |
|---|---|---|
| E1 — hoàn tất baseline | Admission hai nhóm cho incoming HTTP và outgoing control: new work/progress, reservation hai chiều trong tổng cap, không queue | 40 policy + 4 integration mới, 45 direct regressions và 1 mixed-load nhiều process pass; evidence bên dưới |
| E2 — hoàn tất baseline | Audit durable session/claim/node accounting; hai process tranh cùng cap, crash/lost ACK/restart; chốt chưa thêm grant download/job toàn cluster trong topology hữu hạn | 22 focused integration pass trên PostgreSQL/POSIX thật; đối chiếu counter với owner rows cùng snapshot; quyết định và giới hạn bên dưới |
| E3 — hoàn tất baseline | Luân phiên claim/dispatch theo kind; nhường lượt sau chunk/allocation/RPC; bounded continuations và shared read credit trước GetChunk | 16 policy + 11 integration mới + 50 direct regressions pass; paused lease/token/deadline và shutdown có gate bên dưới |
| E4 — hoàn tất baseline | Gate multi-process, 1/4/16 session, 16 stream đồng thời, overload/delete/cleanup và budget deployment | 3 workload session pass (0.258s/0.705s/1.980s) + 1 gate 16-stream pass + 1 mixed-load pass trên Docker Linux/PostgreSQL; evidence xác nhận 06/11/2026 |

Không coi admission trong RAM là counter bền vững toàn cluster. E1 chỉ bảo vệ
entry points/control calls của từng process; owner/capacity ledger PostgreSQL,
fair scheduling Worker và workload mở rộng có gate riêng ở E2–E4.

## Contract E1

- Mỗi admission budget có tổng cap `limit` và reservation `reserve` cho **mỗi**
  nhóm, với `0 <= 2*reserve <= limit`. Mỗi nhóm tối đa `limit-reserve`, tổng hai
  nhóm không quá `limit`. Shared capacity được dùng linh hoạt, phần tối thiểu của
  nhóm đối diện không bị mượn. Không có waiting queue và không đọc body trước grant.
- Incoming HTTP dùng `V2_REQUESTS=32`, `V2_REQUEST_RESERVE=8`; outgoing JsonClient
  dùng `V2_CONTROL_CALLS=32`, `V2_CONTROL_RESERVE=8`. Bình thường tối đa 24 slot một
  nhóm, còn ít nhất 8 cho nhóm kia. Khi cùng hoạt động có thể 24+8 hoặc 16+16,
  không phải 32 slot cho mỗi nhóm. Reserve=0 hỗ trợ cap nhỏ/legacy nhưng không có
  bảo đảm reservation; standalone constructors suy ra min(8,limit//4) khi không chỉ rõ.
- Nhóm progress: POST complete/abort upload; DELETE file; internal upload part
  renew/confirm/uncertain; internal read renew/release và GET manifest. Create
  upload, acquire read, claim part mới, binary upload/download và queries/polls
  khác là new work. Classifier dựa method + route shape, không dựa body hay header
  do caller tự chọn; không thay validation, token, recovery epoch hoặc quyền truy cập.
- Khi một nhóm chạm class cap hoặc tổng chạm cap: 429 RESOURCE_LIMIT, Retry-After
  và request ID; HEAD không body. Hai health-ready slots riêng, live bypass như cũ.
- Bên bị từ chối không I/O/acquire thêm lease/claim/bytes. Grant đã nhận được trả
  đúng một lần khi success/error/cancel; control cancellation đóng transport trước
  khi trả slot. Terminal command không được tự retry vì admission/timeout.
- Reservation đảm bảo khả năng nhận việc tối thiểu, không hứa mọi renew đều thành
  công khi nhóm progress cũng quá tải. Không hứa FIFO, latency SLO, chống caller
  ác ý hoặc deadline DB/Storage vượt các budget hiện hữu.

## Evidence E1 — 29/10/2026

Tier **targeted + focused integration/E2E**, do thay routing/admission và control
transport dùng chung. Không cần full backend/frontend. Preflight read-only:

```powershell
docker info --format '{{.ServerVersion}}'
docker compose -f deploy/compose.v2-metadata-tests.yml ps postgres
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -Atc 'SELECT current_database(), current_user, 1'
```

Docker 29.8.2, `dfs-v2-tests-postgres-1` healthy, query trả
`dfs_v2_test|v2_test|1`. Dùng fixture PostgreSQL 16 đã được người dùng cho phép;
không restart, xóa volume hoặc reset dữ liệu ngoài schema test riêng.
Policy chạy native Windows/Python 3.12 từ `backend/`:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_v2_e1.py
```

**40 passed / 0.38s**: method/route classification và lookalikes, reserve/total
cap cả hai chiều, concurrency 16 threads, slot accounting, invalid config,
cap nhỏ/reserve=0 và cấu hình hai reserve độc lập.

Phần integration chạy Docker Linux/Python 3.12. Rebuild code hiện tại từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_e1_integration.py
```

Image `dfs-v2-http-tests:dev`, inspect ID
`sha256:58ef848d9ce07d94f61784ba24fb0f50dbd868b09ced6f95a2500e43cb86dfd2`.
**4 passed / 1.60s**, không skip:

- Hai ca TCP thật giữ ba slot cùng nhóm ở cap 4/reserve 1, reject lần thứ tư
  trước network I/O, nhóm kia vẫn thành công. Cancel đóng socket và trả đúng
  slot để request cùng nhóm thay thế chạy được; cuối cùng active=0.
- Hai ca HTTP/PostgreSQL thật giữ ba acquire hoặc renew tại repository boundary,
  kiểm outgoing reject và incoming 429 độc lập qua raw HTTP. Acquire flood vẫn
  renew/release read lease được; renew flood vẫn acquire mới được. Epoch sai vẫn
  503 RECOVERY_REQUIRED; request ID canonical duy nhất, Retry-After=1. Mọi lease
  được release, file vẫn AVAILABLE. Barrier giữ trước transaction, không giả
  lập contention của DB pool hoặc row lock.

Hồi quy trực tiếp, cùng image và fixture:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_pre_e.py tests/test_v2_c3.py::test_upload_complete_publish_real_http_bytes tests/test_v2_c3.py::test_limits_and_control_admission tests/test_v2_c3.py::test_independent_readiness_and_recovery_gate tests/test_v2_c3.py::test_stream_admission_deadline_disconnect_and_renew_failure tests/test_v2_d3.py::test_internal_read_commands_commit_page_renew_and_release_via_json_client tests/test_v2_d4.py::test_blocked_send_stops_and_drains_on_lease_or_request_end tests/test_v2_d4.py::test_control_failures_before_headers_and_best_effort_release
```

**45 passed / 19.46s**, không skip. Kiểm duration float, nested tracing/HEAD,
control total deadline/TLS/cancel/body bound, real upload/complete, readiness,
stream disconnect/renew failure, internal read commands và download blocked-send/
control failure cleanup. Không cộng gate fixes trước E vào số ca E1 hiện tại.

Một ca mixed-load D5 được chọn vì runtime admission cùng thay đổi, từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-d5-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d5.py::test_mixed_load_slow_clients_replica_finalize_and_rss
```

**1 passed / 31.78s**, không skip. Nginx, hai API public/hai internal/hai Transfer,
hai Storage và một Worker process, cùng host/container cap 3 GiB; PostgreSQL
container riêng. Bốn GET cùng file 64 MiB/RF=2 đúng bytes/hash, hai reader mỗi
Transfer, bốn renew trong lúc client bị giữ; 16 GET thêm trả 429 và không thêm
read lease. Complete file đang chờ và upload/publish file mới vẫn chạy, đo cùng
nhau khoảng 0.889s trong fixture. Download/buffer/lease đều drain; sampled RSS
mixed phase Transfer khoảng 101.95/103.19 MiB, không vượt guard HWM 256 MiB/process.
Raw report/log được giữ ở
`backend/.runtime/v2-e1/test_mixed_load_slow_clients_replica_finalize_and_rss/`;
nguồn harness mặc định là `.runtime/v2-d5/`. Đây là regression của topology cũ,
không phải phép đo trực tiếp khi HTTP/control chạm cap 32 hoặc benchmark E4.

Tổng **90 ca khác nhau passed**, gồm 40 native và 50 Docker; không có failure
hoặc skip. Ruff check/format sáu file Python thay đổi pass. Không chạy full suite,
frontend, toàn bộ D5, browser C5, file 1–10 GiB, restart/failover/HA hoặc workload
1/4/16 phiên E4; các gate đó không được suy ra từ E1.

## Bàn giao E1 → E2 (29/10/2026)

E1 đã hoàn tất baseline trong phạm vi admission mỗi process. Tiếp theo E2:
audit counter active sessions/per-session claims/node charge qua nhiều owner,
lost ACK/restart và durable Delete ACK, chốt ngân sách toàn cluster cần bổ sung.
Chưa đổi schema, proto, Worker scheduling hoặc durable accounting trong E1;
E2–E4 và toàn mốc E chưa hoàn tất.

## Contract và audit E2 — 30/10/2026

E2 đã hoàn tất baseline audit/verification. Không tìm thấy lỗi accounting cần sửa
runtime trong phạm vi này; thêm `tests/test_v2_e2.py` và helper
`tests/v2_e2_process.py`, không thay schema/proto hoặc thuật toán admission E1.
Những nhận xét về E2 chưa hoàn tất ở evidence E1 phía trên là trạng thái bàn giao
ngày 29/10, không phải trạng thái hiện tại.

| Tài nguyên | Owner và bất biến bền vững | Khóa/cấp/trả |
|---|---|---|
| Session toàn namespace | `admission_counters.upload_sessions.held_count = count(session.admission_released_at IS NULL)`; các owner này là OPEN/FINALIZING | Counter trước file/session. Create + idempotency + increment cùng commit; replay đọc owner trước kiểm cap. Abort/expiry/delete/publish/fail exhausted đặt marker cùng decrement; crash không reset counter |
| Claim mỗi session | `active_claims = count(part.claim_held)` trong snapshot policy `max_parallel_parts` | File → session → part. Claim/retry grant slot cùng owner/token; confirm/uncertain/sweep/cleanup CAS marker và decrement cùng transaction. Hết lease trả slot nhưng không cấp writer mới cho part còn RECONCILING |
| Bytes mỗi node/incarnation | `node_budgets.charged_bytes = sum(allocation.charged_bytes WHERE state!='RELEASED')` | Khóa budget theo thứ tự ổn định trước reserve; allocation + attempt + charge commit trước I/O. Retry generation trên allocation cũ không charge thêm. Abort/expiry/confirm/UNKNOWN/node health không trả bytes |
| Release bytes | Allocation DELETE_PENDING, exact identity/generation và CLEANUP owner/token/lease còn hợp lệ | Chỉ ACK `DELETED + cleanup_complete=true` đã kiểm chứng mới đổi RELEASED và decrement; replay không trừ lần hai. Inspect NOT_FOUND, cleanup chưa durable, identity sai hoặc stale job đều không trả charge |

Đối chiếu trong gate dùng **một SQL statement/một MVCC snapshot** cho counter và
owner rows, kiểm cả equality lẫn `0 <= actual <= cap`, không chỉ tổng cuối test.
Không sửa counter để làm assertion pass. Đây là audit trong integration gate,
không bổ sung reconciliation chạy nền hoặc lệnh vận hành tự sửa dữ liệu.
Session terminal có thể còn `claim_held` chờ cleanup/sweeper; session admission
đã trả nhưng charge còn giữ là trạng thái hợp lệ, không phải rò quota.

### Quyết định grant download/job toàn cluster

**Chưa thêm durable grant download/job trong baseline E.** Chốt phạm vi deployment
là tập process hữu hạn, cấu hình cap cho từng process và tính tổng tài nguyên
khi tăng số instance. PostgreSQL tiếp tục là nguồn cap chung cho session và
bytes; buffer/RPC là tài nguyên cục bộ được cấp trước I/O. Không có yêu cầu giữ
một số stream/job cố định bất kể số instance trong baseline hiện tại.

- Download: `V2_DOWNLOAD_STREAMS` và buffer reservation là cap của từng Transfer.
  Tổng stream đang phục vụ không vượt tổng cap của các process đang phục vụ;
  thêm instance sẽ tăng envelope này. Read lease bền vững bảo vệ version/delete,
  **không phải download concurrency grant**. Acquire mất ACK hoặc Transfer chết
  có thể để lease ACTIVE tới expiry; không được suy số stream từ số row ACTIVE.
  TTL/absolute TTL và release token giữ lifecycle, không tự suy ra bytes credit.
- Job: row `jobs` có owner/expiry/fencing và SKIP LOCKED chống hai owner còn hợp
  lệ cùng commit một job; đây **không phải cap tổng job RUNNING toàn cluster**.
  Scheduler bị giới hạn `V2_WORKER_SLOTS`/`V2_WORKER_FINALIZE_SLOTS` mỗi Worker;
  mỗi internal API còn một lane đồng bộ `retry_for_consumer`, phải cộng riêng
  khi tính deployment envelope. Claim batch tối đa 8 không phải cluster cap.
- `V2_JOB_BACKLOG_LIMIT` là phanh nhận upload mới dựa trên snapshot backlog,
  không phải hard quota số job: session cũ vẫn có quyền enqueue cleanup/reconcile.
  `read_bytes_per_second` áp dụng cho từng `Work`, chưa phải shared rate limiter
  toàn Worker/cluster. Không gọi các giới hạn này là durable cluster grants.
- Envelope cấu hình: download concurrency `sum(download_streams)`; job execution
  `sum(worker.slots) + số internal retry lanes`; finalize `sum(finalize_slots)`.
  Buffer/RPC và tốc độ đọc phải cộng theo process/lane, kể cả lane retry. Không
  suy fairness hoặc SLO từ các tổng này. E3 kiểm scheduling/I/O; E4 phải ghi số
  instance, overrides và tài nguyên thực đo khi chạy workload mở rộng.

Vì không thêm grant, E2 không cần forward migration, owner table mới hoặc cơ chế
thu hồi grant mới. Nếu đổi sang autoscaling với **hard cap cố định toàn cluster**,
phải bổ sung grant có owner/expiry/fencing/reconciliation và forward migration,
kiểm grant/release/reclaim lost ACK trước khi nhận phạm vi đó. Không dùng counter
RAM E1, read leases hoặc số job RUNNING để giả làm grant này.

## Evidence E2 — 30/10/2026

Tier **focused integration**, chỉ ranh giới PostgreSQL/commit và POSIX
ledger/filesystem. Preflight read-only từ root:

```powershell
docker info --format '{{.ServerVersion}}'
docker compose -f deploy/compose.v2-metadata-tests.yml ps postgres
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -Atc 'SELECT current_database(), current_user, 1'
```

Docker **29.8.2**, `dfs-v2-tests-postgres-1` healthy; query trả
`dfs_v2_test|v2_test|1`. Dùng PostgreSQL 16 fixture hiện hữu; không start/restart
dịch vụ, migration vào schema dùng chung, xóa volume hoặc reset dữ liệu. Mỗi test
tạo/migrate/drop schema UUID riêng và dùng directory Storage tạm riêng.

Build source hiện tại và chạy từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_e2.py
```

Image `dfs-python-tests:dev`, inspect ID
`sha256:91f160c65123bccdd51653638d1cebe039d4ee8cf3494faa9f01304fc6aecb1d`.
**22 passed / 60.85s**, không failure/skip:

- Hai process tạo bằng `spawn`, mỗi process khởi tạo engine/repository riêng;
  assertion kiểm PID và `pg_backend_pid()` khác nhau. Với race counter/session/
  budget/confirm/release, parent giữ row lock và đợi **cả hai** backend có
  `wait_event_type='Lock'` rồi mới thả. Không chỉ chạy hai lệnh tuần tự hoặc dùng
  chung engine với hai wrapper để giả tranh chấp nhiều owner.
- Tranh slot create ở cap 1; cùng idempotency key ở cap đầy vẫn chỉ một owner;
  crash trước commit rollback cả owner/counter, crash sau commit trước response
  replay đúng owner. Abort với abort/delete/expiry chỉ trả admission một lần,
  chưa trả claim/bytes trước cleanup.
- Publish thành công, checksum mismatch và worker exhausted đều commit terminal
  rồi mất response; process khác không decrement admission lần hai. Bytes vẫn
  charge, failure tạo cleanup và session mới dùng lại được slot.
- Claim cùng part hoặc hai part trong một session cap 1; hai session tranh hai
  node cap 3 bytes/RF=2; không có part/attempt thừa khi bị từ chối. Claim crash
  trước/sau commit, hai sweeper trả đúng một slot, OUTCOME_PENDING vẫn chặn writer
  và node đầy vẫn từ chối allocation mới sau khi claim hết hạn.
- Confirm mất response rồi hai process replay; confirm đụng uncertain trả một
  slot. Retry generation cũ với part mới tranh cùng cap; allocation cũ không
  charge lại, chỉ allocation của part mới (nếu thắng) thêm charge.
- Cleanup release crash trước/sau commit rồi hai process replay cùng ACK chỉ
  trừ một lần. Hai job claimers chỉ một owner, expired job đổi token; late ACK
  của owner cũ rollback release, owner mới còn nghĩa vụ/capacity để xử lý.
- Hai Storage volume thật: process Store commit rồi kết thúc trước khi trả
  response cho caller, process mới mở ledger/recovery thấy STORED đúng hash;
  chưa confirm vẫn giữ charge. Sau abort, Delete durable cũng mất response;
  restart/Inspect DELETED không tự trả charge. Retry Delete cung cấp ACK thật;
  sai state/cleanup/incarnation/generation không release, hai metadata process
  nhận cùng ACK chỉ một lần trừ. Tombstone qua restart chặn late Store, và owner
  khác dùng lại được dung lượng sau release. Fault này nằm sau operation đã hoàn
  tất/đóng ledger, không phải kiểm mất điện trong fsync hoặc PostgreSQL restart.

Ruff native Windows/Python 3.12 từ `backend/`, **pass** cho đúng hai file mới:

```powershell
.\.venv\Scripts\python.exe -m ruff check tests/test_v2_e2.py tests/v2_e2_process.py
.\.venv\Scripts\python.exe -m ruff format --check tests/test_v2_e2.py tests/v2_e2_process.py
```

Các ca transaction dùng evidence nội bộ dựng đúng contract; ca Storage lost ACK
lấy DeleteEvidence từ kết quả StreamingStore thật. Gate này không dùng HTTP/gRPC,
không giả mất packet trên wire; E2 không thay adapters nên giữ evidence C2/D5
hiện hữu, không chạy lại các gate đã pass. Không chạy full backend/frontend,
smoke, browser C5, file 1–10 GiB, replication/failover/HA, benchmark 1/4/16 phiên
hoặc fairness E3. Không cộng evidence E1/C2/D5 vào 22 ca E2.

## Bàn giao E2 → E3 (30/10/2026)

E1 và E2 hoàn tất baseline trong phạm vi đã chốt. E3 tiếp theo: bảo đảm cleanup,
reconcile và finalize đều có quyền tiến triển khi backlog liên tục; audit cả
Worker scheduler, internal retry lane và I/O mỗi Work. E4 phải chốt deployment
envelope và đo overload/mixed load 1/4/16 phiên. **E3–E4 và toàn mốc E chưa hoàn
tất**; C5/browser/file lớn vẫn tạm gác.

## Contract E3 — Worker fairness và I/O

E3 sửa `metadata/v2/worker.py`, `repository.py::claim_jobs` và
`storage_client.py::read`; không đổi schema/proto. Trạng thái bàn giao E2 phía
trên là lịch sử; hiện E1–E3 đã hoàn tất baseline.

### Scheduling và giới hạn cục bộ

- Claim riêng từng kind CLEANUP/RECONCILE/FINALIZE, luân phiên điểm bắt đầu;
  mỗi tick tối đa một claim/kind. Trong mỗi kind vẫn dùng thứ tự run_after/job_id
  và SKIP LOCKED của PostgreSQL. Backlog già của một kind không che các kind khác.
- `V2_WORKER_JOBS_PER_KIND=2`, hợp lệ 1–8: tối đa `3 * jobs_per_kind` job resident,
  gồm đang chạy và continuation tạm dừng. Mặc định tối đa **6 lease/heartbeat**,
  cấu hình tối đa 24. Đây là queue continuation hữu hạn mới của E3, thay cho
  baseline C2 chỉ giữ job gắn với execution slot; không prefetch không giới hạn.
- `V2_WORKER_SLOTS=2` vẫn là tổng execution/RPC quantum đang chạy; mỗi kind
  cleanup/reconcile tối đa `slots-1`, finalize tối đa
  `V2_WORKER_FINALIZE_SLOTS=1` (cũng không quá slots-1). Khi chỉ một kind có việc,
  reservation có thể để trống slot để kind khác tới vẫn tiến triển.
- Dispatch round-robin giữa các kind đủ điều kiện và FIFO giữa resident jobs
  trong một kind. Finalize nhường sau **một chunk đã verified**; cleanup sau
  **một allocation đã Delete ACK + release commit**; reconcile/retry sau mỗi
  Inspect/Prepare RPC. Query page vẫn tối đa 8 part. Không giữ DB transaction,
  Storage RPC hoặc raw chunk buffer xuyên điểm yield.
- Hash SHA-256, cursor và một manifest page hữu hạn nằm trong RAM của generator
  finalize; không thay full hash bằng danh sách chunk hash. Continuation quay lại
  cùng lease/attempt, không requeue durable job và không tăng retry attempt theo
  số quantum. Process chết thì bỏ state RAM, reclaim đọc/hash lại từ byte zero.

Fairness bảo đảm quyền dispatch cho kind có việc và chia lượt trong resident set
hữu hạn khi các quantum/RPC/DB kết thúc trong budget. Không hứa chia đều thời
gian/bytes, latency SLO hoặc FIFO toàn cluster. Job ngoài resident cap chờ durable
queue; với jobs_per_kind=1 không có interleave hai file cùng kind. Không preempt
giữa một RPC/chunk; Storage timeout, Work deadline và cancellation vẫn áp dụng.

### Shared read credit trước I/O

- `V2_WORKER_READ_BYTES_PER_SECOND=67108864` giờ áp dụng cho **tổng Work của cùng
  một Worker**, không còn reset thành rate riêng cho mỗi finalize. Worker của
  internal retry lane vẫn riêng; download thuộc Transfer, không mượn budget này.
- Mỗi replica GetChunk phải trả trước toàn bộ expected chunk size, theo lượt
  reservation tối đa 262144 bytes, dùng shared FIFO deadlines và không tích credit
  khi idle. Kiểm stop/deadline/lease cancellation trong lúc chờ và ngay trước RPC;
  không mở GetChunk khi chưa nhận đủ credit.
- Failed replica, partial stream hoặc cancellation không hoàn credit; fallback
  sang replica khác trả expected size lần nữa. Cách tính bảo thủ này không cho
  nhiều job/cancel/retry lách rate. Hash/size/footer/OK EOF vẫn kiểm bytes thực.
- Đây là admission theo chunk, không phải network shaper mượt theo từng block:
  sau grant, RPC/Storage có thể burst trong một chunk. RPC đồng thời vẫn bị giới
  hạn execution/finalize slots; không thêm prefetch hoặc materialize cả file.
  Buffer transport/RSS thực và tranh I/O với download cần đo ở E4, không suy từ
  pacing gate nhỏ.

### Lease, restart và shutdown

Heartbeat bắt đầu ngay khi prime continuation sau claim, kể cả khi chưa có thread
I/O. `job_seconds` tính từ lúc đó và không reset qua yield; renew không kéo dài
absolute Work deadline. Mọi commit vẫn kiểm job owner/token/live DB lease như
trước. Claim có danh sách exclude tối đa 24 resident UUID để chính Worker không
reclaim một iterator cũ còn trong RAM nếu lease của nó hết hạn; instance khác vẫn
reclaim bình thường và fencing chặn old continuation publish/release.

Shutdown dừng claim/dispatch, cancel/drain RPC, đóng continuation đang pause để
dừng heartbeat, rồi đợi dedicated retry consumer. Các bước dùng chung shutdown
deadline. Future bị executor hủy trước khi chạy không được đưa vào `wait()` như
một quantum còn sống; có regression riêng cho CANCELLED chưa NOTIFIED. Intent
chưa xong vẫn RUNNING trong DB để process khác reclaim sau expiry, không tự trả
session/node charge vì shutdown.

E4 phải cộng cả **execution** (`sum(worker.slots) + internal retry lanes`) và
**resident leases/heartbeat/page state** (`sum(3 * jobs_per_kind) + retry lanes`).
Read credit cộng theo Worker instance; không có hard cap toàn cluster khi thêm
instance. E2 chưa thêm cluster grant vẫn là quyết định hiện hành.

## Evidence E3 — 30/10/2026

Tier **targeted + focused integration**, chọn đúng Worker, repository job claim
và HTTP retry consumer bị ảnh hưởng. Read-only preflight như E2: Docker **29.8.2**,
`dfs-v2-tests-postgres-1` healthy, query `dfs_v2_test|v2_test|1`. PostgreSQL 16 đang
chạy được giữ nguyên; schema UUID/POSIX volume tạm riêng cho từng test. Không
start/restart service, xóa volume hoặc reset dữ liệu dùng chung.

Native Windows/Python 3.12, từ `backend/`:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_v2_e3.py
.\.venv\Scripts\python.exe -m pytest -q tests/test_v2_e3.py::test_close_drains_a_cancelled_unnotified_quantum_without_waiting_for_timeout
```

**16 ca native riêng biệt pass**: lượt đầu 15 passed/0.11s; sau khi thêm regression
shutdown, chỉ chạy ca mới **1 passed/0.17s**. Config resident cap, block size,
shared credit qua 8 threads, idle không tích credit, cancel không refund và
stop/deadline trước reservation; không cộng hai lượt thành một full-file run.

Docker Linux/Python 3.12, source hiện tại rebuild từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_e3_integration.py
```

Image `dfs-python-tests:dev`, build manifest-list digest
`sha256:919d96a06aa4424264d75f139d6ad0e861d71ab3800926726729425bbc65c8c0`.
**11 passed / 10.47s**, không skip:

- Lần lượt flood CLEANUP, RECONCILE và FINALIZE bằng bốn job già, giữ I/O lane
  bằng barrier trong suốt flow của hai kind còn lại. Hai kind đó hoàn thành qua
  PostgreSQL/gRPC/filesystem thật trong khi cả bốn flood job vẫn còn pending;
  sau thả barrier, flood cũng hoàn thành. Counter/owner/charge đối chiếu cùng
  snapshot, execution/resident caps được kiểm mỗi tick.
- Cleanup và finalize file 12 chunk nhường cho file một chunk cùng kind; file
  nhỏ hoàn tất trước file lớn, RPC order chứng minh interleave. Attempt vẫn 1,
  paused lease renew được sau khi test rút TTL về 2s. Full SHA-256 đúng cho file
  lớn, cleanup trả đúng charge. Chunk fixture ở các ca này là 3 bytes, RF=2.
- Hai finalize concurrent (slots=3/finalize_slots=2), mỗi file hai chunk 32 KiB,
  tổng 128 KiB phải qua một shared budget 64 KiB/s; gate kiểm elapsed ít nhất
  2s, cả hai thực sự overlap và hash đều đúng. Đây là kiểm pacing, không phải
  throughput benchmark hoặc số đo storage/network tối đa.
- Instance khác reclaim paused finalize với token mới; old iterator không
  publish/đổi owner, replacement hoàn thành. Paused Work giữ original deadline;
  deadline trong lúc chờ read credit không mở channel/GetChunk, failure giữ charge
  và enqueue cleanup. Exclude resident không làm mất khả năng reclaim từ owner khác.
- Dedicated consumer đang chờ Inspect được close/cancel và drain dưới shutdown
  budget 2s; job còn RUNNING và charge giữ nguyên cho recovery.

Lượt đầu có **một code failure shutdown**: canceled queued future làm wait chờ
hết timeout. Đã sửa và thêm native regression; 10 integration pass sau sửa,
rồi bổ sung pre-GetChunk credit/gate và chạy 11 ca hiện tại pass. Không còn code
failure mở; không cộng các ca chạy lặp vào tổng evidence.

Direct regressions, cùng image/fixture, từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_c2.py tests/test_v2_metadata.py::test_job_claim_race_expiry_fencing_renew_and_retry tests/test_v2_e2.py::test_job_reclaim_fences_late_cleanup_ack_without_resetting_capacity tests/test_v2_c3.py::test_actual_bytes_fail_then_synchronous_retry_handoff tests/test_v2_c3.py::test_lost_retry_handoff_recovers_through_claim_sweeper
```

**50 passed / 32.94s**, không skip. Toàn bộ C2 Worker suite vì execute/lifecycle
đã đổi: hash/empty/fallback/malformed EOF, renew/stale lease/epoch/retry budget,
process crash finalize/cleanup/retry boundaries và SIGTERM với gRPC thật. Thêm
job claim race/reclaim accounting và HTTP synchronous handoff/lost response C3.
Không phải full backend hoặc toàn bộ C3/D5.

Ruff **check + format pass** cho năm file Python thay đổi, từ `backend/`:

```powershell
.\.venv\Scripts\python.exe -m ruff check metadata/v2/worker.py metadata/v2/repository.py metadata/v2/storage_client.py tests/test_v2_e3.py tests/test_v2_e3_integration.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/v2/worker.py metadata/v2/repository.py metadata/v2/storage_client.py tests/test_v2_e3.py tests/test_v2_e3_integration.py
```

Tổng **77 ca riêng biệt pass**: 16 native, 11 integration mới và 50 regressions.
Không chạy full suite, frontend, smoke, toàn D5, browser C5, file 1–10 GiB,
benchmark RSS/1/4/16 phiên hoặc HA nhiều host. Process-crash/SIGTERM là evidence
Worker C2 được chạy lại, không phải nghiệm thu cluster failover/HA.

## Bàn giao E4

**E1–E3 hoàn tất baseline; E4 và toàn mốc E chưa hoàn tất.** E4 tiếp tục với
topology/process counts và budget mới (resident/execution/read rate), gate
overload/mixed load 1/4/16 phiên, bytes/hash/counter/charge, latency/RSS và
bounded rejection. C5/browser/file 1–10 GiB tiếp tục tạm gác.

### Evidence E4 từng phần — 30/10/2026

Tier focused integration/E2E, Docker Linux/Python 3.12, PostgreSQL 16 fixture
healthy, Nginx round robin, hai API public, hai internal, hai Transfer, hai
Storage và một Worker cùng host; tests container cap 3 GiB. Mỗi test dùng schema
và directories Storage riêng. Ruff check/format `tests/test_v2_e4.py` pass.

```powershell
docker info --format '{{.ServerVersion}}'
docker compose -f deploy/compose.v2-metadata-tests.yml ps postgres
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -Atc 'SELECT current_database(), current_user, 1'
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-e4-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-e4-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_e4.py
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-e4-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d5.py::test_mixed_load_slow_clients_replica_finalize_and_rss
```

Preflight Docker 29.8.2, PostgreSQL healthy và query `dfs_v2_test|v2_test|1`.
**3 passed / 39.19s**: 1/4/16 active upload sessions, file 64 KiB, RF=2,
tối đa bốn transfer đồng thời; đối chiếu session/claim/node counter, hash download
và hash bytes tại cả hai Storage, cả hai Transfer được dùng ở mức 4/16. Cap của
schema test riêng đặt 16 cho ca 16 (fixture gốc là 8). GET 429 được kiểm
Retry-After/request ID và retry tối đa tám lần; lượt hiện tại tối đa hai lần thử.
Thời gian toàn workload lần lượt 0.299/0.548/1.713s; sampled RSS Transfer ở mức
16 là 88,800/87,996 KiB, Worker 95,196 KiB. Số đo nhỏ trên một host, không SLO.

**1 passed / 31.55s**: bốn reader 64 MiB cùng file/RF=2 giữ đủ bốn download
slots, 16 GET thừa trả 429 không cấp thêm lease; upload/finalize khác vẫn hoàn
thành sau khoảng 1.277s. Hash bốn reader đúng, leases/buffers drain; sampled
RSS Transfer mixed phase 104,640/103,808 KiB, dưới guard 256 MiB/process.
Raw reports/logs nằm trong `backend/.runtime/v2-e4/`, tách khỏi evidence D5.

Lượt đầu mức 16 bị 429 tại session thứ 9 do fixture cap=8; đã đổi cap trong
schema riêng. Lượt sau gặp 429 download vì ingress phân phối không đều so với
cap 2/process; harness hiện kiểm và retry GET hữu hạn. Không cộng các lượt lỗi
hoặc lặp vào số ca pass. Không restart service, reset dữ liệu hoặc xóa volume.

Phần session workload ở trên có 16 session active nhưng chỉ bốn transfer đồng
thời; gate 16 stream và delete/cleanup bổ sung ở dưới.

### E4: 16 byte stream và delete/cleanup — 30/10/2026

Preflight Docker 29.8.2 và PostgreSQL 16 fixture healthy; query trả
`dfs_v2_test|v2_test|1`. Không restart service hay reset dữ liệu. Dùng cùng
topology hai API public, hai internal, hai Transfer, hai Storage, một Worker,
Nginx và PostgreSQL riêng; tests container cap 3 GiB. Gate này cấu hình riêng
`V2_DOWNLOAD_STREAMS=8` và `V2_DOWNLOAD_BUFFERED_BYTES=71303168` cho **mỗi**
Transfer, thay vì hai stream mặc định của D5; chunk 8 MiB, RF=2. Worker mặc định
2 execution slots/6 resident leases/64 MiB/s, internal retry mỗi instance một
lane. Các cap này là per-process, không phải hard cap chung khi thêm instance.

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-e4-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-e4-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_e4_concurrent.py
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-e4-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d5.py::test_mixed_load_slow_clients_replica_finalize_and_rss
```

**1 passed / 15.99s**, không skip: 16 GET cùng file 16 MiB+17 bytes được giữ
đồng thời, mỗi Transfer giữ tám slot/buffer và PostgreSQL có 16 ACTIVE read
lease. Bốn GET tiếp theo trả 429/Retry-After, không tạo thêm lease. Delete qua
ingress tombstone file, cả hai API trả 404 và cleanup đợi reader; node charge
vẫn bằng hai replica. Thả reader: cả 16 bytes/hash đúng, hai Transfer đều phục vụ,
lease/buffer drain. Sau đó cleanup hoàn tất, allocations RELEASED, attempts CLOSED,
charge=0, cả hai Storage không còn chunks và ledger có durable DELETED ACK.

Gate có barrier giữ EOF trước khi response bắt đầu, không phải 16 client đọc
throttle độc lập hoặc 16 file khác nhau. Report ingress ghi request tới cả hai
API, hai internal và hai Transfer; Worker hoàn tất publish/cleanup; cả hai
Storage có tombstone, nhưng luồng đọc trong lượt này dùng Storage-0. Report
cũng ghi PID, ports, config, request log và sampled resource của mọi process.
Elapsed workload 2.218s, download TTFB khoảng 0.612–0.970s, toàn download
1.477–1.759s; sampled RSS Transfer 151,604/153,160 KiB, Worker 100,756 KiB.
Đây là phép đo một fixture nóng cache, không phải SLO hoặc HA nhiều host.

Lượt đầu assertion kiểm lease ngay sau khi slot đã nhận và thấy 3/16 lease;
đã sửa harness chờ đúng barrier 16 lease và chạy lại ca lỗi. Kiểm hồi quy
mixed-load cấu hình cũ 2 stream/Transfer **1 passed / 31.55s**; Ruff check/format
hai file test thay đổi pass. Reports hiện tại tại `backend/.runtime/v2-e4/`.

Các gate E4 đã pass ở những lượt ghi bên trên; lượt xác minh sau thay đổi fixture
chưa có kết quả pytest xác nhận, xem cập nhật ngày 01/11. Không chạy
full backend/frontend, smoke, browser C5, file 1–10 GiB, HA/failover nhiều host
hoặc gate release; không suy các kết quả đó từ E4. Hai workload 16 session và
16 stream là các ca riêng, không phải 16 upload và 16 download đồng thời.

### Xác minh hoàn tất E4 — 06/11/2026

Preflight: Docker 29.8.2 hoạt động, `dfs-v2-tests-postgres-1` healthy, query trả
`dfs_v2_test|v2_test|1`. Lệnh chạy chính thức từ root:

```powershell
docker-compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-e4-tests.yml build tests
docker-compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-e4-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_e4.py
```

Image `dfs-v2-http-tests:dev` (built từ target `v2-http-tests`).
**3 passed in 39.67s**, không skip:
- 1/4/16 active upload sessions, file 64 KiB, RF=2, tối đa bốn transfer đồng thời;
  đối chiếu session/claim/node counter, hash download và hash bytes tại cả hai
  Storage, cả hai Transfer được dùng ở mức 4/16.
- Toàn bộ 16 session download đều hash đúng, transfers cân bằng (mỗi Transfer 8 chunks đọc,
  max 2 concurrent downloads).
- Thời gian toàn workload lần lượt 0.258s (1 session), 0.705s (4 sessions), và
  1.980s (16 sessions).
- Kết hợp với gate 16 stream đồng thời và mixed-load đã pass trước đó, **toàn bộ
  gate E4 đã được xác minh thành công với kết quả pytest pass**.
- Mốc E (E1–E4) chính thức **hoàn tất baseline**. Đủ điều kiện bắt đầu runtime Mốc F.
