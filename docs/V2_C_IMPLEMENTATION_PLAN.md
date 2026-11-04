# Mốc C — Upload streaming, resume và finalize V2

Trạng thái 26/10/2026: **C1–C4 hoàn tất; C5 đang nghiệm thu**. Gate nhiều instance
với file nhỏ đã qua; còn client C4 trên topology này, file lớn/tài nguyên và phần
crash matrix còn thiếu. **C chưa hoàn tất**.
B1–B4 đã chốt foundation theo
[evidence B](V2_B_IMPLEMENTATION_PLAN.md). Tài liệu này là bàn giao theo
[thiết kế V2](V2_DESIGN.md), [API](V2_API_CONTRACTS.md),
[schema](V2_METADATA_SCHEMA.md) và [correctness](V2_CORRECTNESS.md), không mở rộng
contract hay ghi nhận runtime chưa chạy. Evidence C1–C5 và phần C5 còn lại ở cuối tài liệu.

## 1. Điểm bắt đầu

Đã có Storage V2 riêng với bảy RPC, SQLite ledger, streaming, fencing,
inventory/health và maintenance CLI offline; Metadata có migration riêng,
session/admission/claim/attempt/capacity ledger, job lease và cleanup publication
guard. B4 đã liên kết PostgreSQL/gRPC/filesystem cho identity, disk đầy và epoch.

C1 đã bổ sung claim renew, confirm/reconcile/retry, complete/immutable manifest
và finalize publication. C2 có Worker V2 riêng cho reconcile/cleanup/finalize,
bounded loop/renew/retry/shutdown. C3 đã nối API public/internal, Transfer streaming
và ingress HTTP thật; cấu hình ở [runtime C3](V2_C3_RUNTIME.md). C4 đã có
[client hash/resume tối thiểu](V2_C4_CLIENT.md); Web hoàn chỉnh thuộc G. Không coi foundation B là luồng
upload đang hoạt động. Runtime/volumes V1 tiếp tục tách riêng.

## 2. Thứ tự triển khai

| Phase | Phạm vi | Gate nhỏ nhất đủ chứng minh |
|---|---|---|
| C1 — hoàn tất domain | Transaction renew/confirm/retry, complete/manifest, finalize publication; evidence DTO/validation và lỗi domain theo contract | PostgreSQL thật, hai repository/connection; stale lease/revision/epoch/job token, ACK spec/RF/domain, complete/abort/confirm races, admission/charge không double release |
| C2 — hoàn tất | Worker reconcile/cleanup upload lỗi và finalize; bounded job loop, renew/retry/shutdown | PostgreSQL + gRPC/filesystem thật; lost ACK, lease expiry, generation fence/drain, cleanup paging, stale worker, process restart tại commit boundaries |
| C3 — hoàn tất | API public/internal và Transfer streaming nối các primitive C1–C2 | HTTP → API → Transfer → Storage thật; cancellation/deadline/actual size/hash; 200 chỉ sau confirm commit, 202 complete đến publish, readiness/lifecycle đúng |
| C4 — hoàn tất | Client tối thiểu pre-hash/create/resume/parts/complete/abort | Client chọn lại đúng nguồn, file cùng tên/size khác hash bị chặn, nguồn đổi dẫn tới finalize mismatch; resume chỉ gửi phần cần thiết |
| C5 | Nghiệm thu nhiều instance, crash và tài nguyên; bàn giao D | Hai API/hai Transfer luân phiên không sticky session; empty/boundary/1 GiB/10 GiB, full hash/RF, bounded RSS/queue, đo finalize riêng và lưu command/topology/config |

C5 là bước nghiệm thu tiếp theo. C2 đã đưa reconcile/cleanup vào trước khi nghiệm thu
upload, để mọi attempt có đường đối soát sau crash. C3 đã kiểm Worker
phục hồi và dọn upload lỗi. Tên module/entry point chọn theo ranh giới
trách nhiệm, không đưa route V2 vào runtime V1 một cách ngầm định.

## 3. Điều kiện giữ xuyên các phase

- Mọi mutation kiểm namespace/epoch/mode qua DB gate. Lock theo thứ tự đã chốt;
  transaction ngắn, không RPC/filesystem I/O khi giữ transaction PostgreSQL.
- Allocation/attempt/charge commit trước Prepare/Store; ACK phải đúng scope,
  identity, generation, actual size/hash. Confirm kiểm thêm session/claim lease,
  revision/token và RF/domain; replay không tạo allocation/charge mới.
- Lease dùng DB clock. Renew không hồi sinh claim/job hết hạn. Retry part chỉ
  sau khi resolve/fence và drain attempt cũ; không tự clear UNKNOWN để ghi lại.
- Terminal session không mở lại; abort/expiry chặn confirm/finalize bằng revision.
  Charge giữ tới durable Delete ACK có cleanup_complete, đúng job token còn hạn.
  Worker không finish cleanup job khi còn page/outcome chưa đối soát.
- Complete cố định immutable manifest và enqueue FINALIZE cùng commit. Publish
  chỉ khi actual full hash bằng expected và evidence replica/policy còn đủ; token
  hoặc revision cũ không thể đưa file AVAILABLE. Finalize có budget riêng.
- Streaming dùng block/buffer/queue hữu hạn, cancellation/drain/deadline và giới
  hạn đồng thời; không gom cả file vào RAM. Hash nguồn tăng dần, ràng buộc session
  trước upload/resume. Cleanup nghĩa vụ bền vững cả khi job DEAD.
- Registry/health observation không cấp lại credit hoặc tự activate disk mới.
  Epoch đổi qua maintenance đã cô lập writer, không tự cài epoch từ RPC.

## 4. Ma trận bàn giao và evidence cần bổ sung

Các gate C01–C03 đã có một phần ở B3 cho transaction create/claim/budget. Giữ
evidence đó, kiểm phần adapter/lifecycle mới tác động, không đếm lại thành test
mới. C04–C10/C12 cần outcome, confirm/finalize, crash/restart và race thật;
C11 cần số đo file lớn và tài nguyên. Mỗi ca phải đối chiếu DB, ledger và bytes,
không chỉ status HTTP/gRPC. Hai instance ở B3 là domain concurrency; C5 phải có
hai API/hai Transfer thật.

Trước từng gate chọn tier và ghi exact command. Integration preflight chỉ kiểm
dependency cần dùng; dùng test schema/volume sở hữu riêng và `--no-deps` cho
test-only. Không chạy aggregate suite để thay thế một flow còn thiếu, không dùng
lint/build làm evidence runtime. Các lệnh pytest C cụ thể sẽ bổ sung khi có test
và topology tương ứng. Lệnh C1/C2/C3 và gate runtime HTTP/Transfer ở phần evidence
bên dưới; không thay thế gate client/tài nguyên C4–C5.

## 5. Giới hạn hiện tại

Đã có gate HTTP upload/retry/finalize, browser client pre-hash/resume trên topology
riêng và gate file nhỏ với hai API/hai Transfer thật trong test container.
Chưa có benchmark hoặc C4 browser trên topology nhiều instance. Range/read-delete
lease thuộc D; mở rộng tải/fairness thuộc E;
repair hoàn chỉnh thuộc F; UI/metrics tổng hợp/multi-host thuộc G; HA/backup
restore và RPO/RTO thuộc H. Recovery gate/epoch và cleanup nền đã có ở B, C phải
giữ chúng trong toàn flow upload.

## 6. Evidence C1 — 23/10/2026

Hiện thực trong `backend/metadata/v2/repository.py`, không đổi migration SQL,
checksum schema, dependency hoặc runtime V1:

- `AllocationEvidence` bất biến cho identity/spec/state/actual bytes/hash từ
  Prepare/Store/Inspect đã kiểm chứng. Đây là DTO nội bộ; JSON unknown fields,
  envelope, status/header và body limits sẽ được nối/kiểm ở C3.
- `renew_part` chỉ renew claim OPEN còn hạn theo DB clock, clamp theo session
  expiry và cập nhật attempt leases. Không thay session expiry.
- `confirm_part` kiểm token/revision/live claim, exact ACK của mọi allocation,
  node/incarnation/domain và RF. ACK/attempt/VERIFIED/counter cùng commit;
  replay không ghi lại thời điểm ACK hoặc trừ claim slot lần hai. Claim trên
  VERIFIED trả record không cấp allocation mới; Transfer C3 vẫn phải đọc/kiểm
  actual body trước 200 khi PUT replay.
- `reconcile_part` phục hồi STORED mất ACK theo job lease, tăng part revision,
  không trả slot lần hai. `authorize_part_retry` commit generation/attempt intent
  và token mới trước RPC; `retry_part` chỉ cấp claim sau đủ Prepare drain ACK
  PREPARED hoặc STORED đúng generation. Attempt cũ chỉ CLOSED sau proof fence;
  không tạo allocation/charge mới. Không thể dùng lại proof của lượt retry trước.
- `complete_upload` kiểm part count/size/order, VERIFIED, không còn unknown
  allocation/attempt, đủ eligible RF/domain; dùng aggregate và INSERT SELECT
  trong DB, không materialize toàn manifest vào Python. OPEN→FINALIZING,
  file VERIFYING, immutable manifest và một job FINALIZE có revision/version
  snapshot cùng transaction. Replay FINALIZING/COMPLETED không sửa manifest.
- `publish_finalize` nhận actual full size/hash từ Worker ngoài transaction,
  kiểm lại manifest/eligibility/revision/job owner+token+lease+payload rồi commit
  AVAILABLE/COMPLETED/job DONE/admission release. Size/hash mismatch thành FAILED
  và enqueue CLEANUP cùng commit; charge vẫn giữ. Abort/stale Worker không hồi sinh file.
- Validator job dùng chung đã sửa race được tái hiện: biểu thức live tính trong
  SELECT trước row-lock wait có thể còn true sau khi lease hết hạn. Validator
  nay lấy lock trước, rồi đọc lại DB clock; cleanup capacity release cũng dùng guard này.

### Môi trường và lệnh đã chạy

Tier **focused integration**, Docker Engine 29.8.2, container Linux/Python 3.12,
PostgreSQL 16. Preflight `docker info`, Compose ps và psql ready đều pass;
`dfs-v2-tests-postgres-1` healthy, DB/user/ready=`dfs_v2_test|v2_test|1`.
Mỗi fixture dùng schema `test_v2_<uuid>` riêng; concurrent cases dùng connection
riêng và hai repository instance. Không migrate public, không start/restart DB,
không mount volume V1. Một ca dùng cả SQLite/filesystem Storage thật: STORED
mất ACK → lease sweeper → inspect/reconcile → full hash từ bytes thật → publish.
Các ca domain khác dùng typed ACK evidence, không coi đó là gRPC/HTTP E2E.

Từ root repository; mỗi lượt source đổi đã build lại:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests

docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c1.py

docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_metadata.py -k "cleanup or recovery_gate or claim_same or unknown_outcome or admission or expiry or sweeper"
```

Gate đầu passed **46** (8.49s), không skipped/deselected/failure. Direct B3
regressions passed **17**, deselected 19 (2.56s). Sau khi bổ sung guard allocation
unknown và thêm 11 case, build lại và chỉ chạy nhóm affected:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c1.py -k "retry or recovery or complete or finalize or lost_storage or abort or enforce_current"
```

Passed **35**, deselected 22 (7.25s). Một lượt lọc chạy trước khi build hoàn tất
đã dùng image cũ; lượt đó bị loại khỏi evidence/counts. Lượt 35 trên chạy sau khi
build hoàn tất thành công.

Regression job-lock expiry ban đầu tái hiện assertion failure: transaction retry
không từ chối job đã hết hạn sau wait. Sau sửa validator dùng chung, rebuild và
chạy đúng affected publication/cleanup:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c1.py -k "retry or reconcile or finalize or lost_storage or abort"
```

**30 passed, 28 deselected**, một ca cleanup mới lỗi bố trí synchronization vì
enqueue đã chờ job lock trước điểm đo. Sửa harness sang kiểm capacity release
sau cleanup authorization, không sửa production code thêm; rebuild rồi chạy:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c1.py -k retry_and_cleanup_recheck

docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_metadata.py -k cleanup
```

Passed **2**, deselected 57 (2.98s) và **3**, deselected 33 (0.95s). Kiểm cả
RECONCILE intent và CLEANUP release khi lease hết hạn trong lúc chờ job row lock;
domain mutations/attempt/counter đều rollback. Không còn assertion failure mở.

Tổng **59 ca C1 khác nhau + 17 regression B3 khác nhau** đã pass qua các lượt
phù hợp với code bị ảnh hưởng; không cộng rerun, không gọi đây là một lượt full
suite. Native Windows `.venv` Ruff check/format --check passed hai file Python.
Không thay dependency hoặc DDL/checksum. Thay đổi comment module sau kiểm thử
không có runtime behavior, không chạy lại các gate đã pass chỉ vì sửa comment/docs.

Không chạy full V1/backend/frontend suite, Storage-only gate không bị đổi, B4,
HTTP/Transfer E2E, Worker process-kill/restart, live cluster smoke, replication,
benchmark/RSS/file 1–10 GiB, multi-host, HA hoặc restore backup. Kết quả DB/domain
không chứng minh upload/resume/finalize end-to-end hoặc hai API/Transfer thật.

## 7. Bàn giao C2 tại thời điểm chốt C1 — lịch sử

C1 hoàn tất transaction primitives; **C vẫn chưa hoàn tất**. Bước tiếp theo là
Worker có bounded job loop/renew/retry/shutdown và I/O bên ngoài transaction:

1. RECONCILE inspect exact current identity/spec; đủ STORED thì reconcile_part.
   Nếu cần ghi lại, authorize generation intent trước Prepare; chỉ retry_part
   sau Prepare đã fence/drain mọi writer cũ. Owner/token phải được bàn giao cho
   lượt Transfer đang chờ; không tự giữ claim nền khi không có consumer. Thiếu
   ACK/không drain được thì giữ RECONCILING/charge và retry hữu hạn.
2. CLEANUP authorize theo trang ≤8 part, durable Delete ACK đúng scope/generation,
   release từng allocation đúng một lần; đối soát hết mọi page/outcome trước
   finish job. DEAD vẫn giữ charge/nghĩa vụ cleanup để vận hành.
3. FINALIZE đọc/hash toàn bytes theo thứ tự immutable manifest, kiểm actual
   chunk/full size/hash, không hash danh sách chunk hash. Renew job bằng DB clock;
   gọi publish_finalize với revision/version snapshot. Dừng publish khi lease mất.
4. Kiểm crash tại các commit boundary và shutdown/cancellation/drain có giới hạn;
   admission/buffer/finalize budget phải có trước readiness runtime. C2 chưa có
   CLI/server/worker loop hoặc gate process lifecycle ở thời điểm chốt C1.

## 8. Evidence C2 — 24/10/2026

Hiện thực Worker riêng trong `backend/metadata/v2/worker.py` và transport
`storage_client.py`; repository thêm bounded snapshots/pages, filtered job claim,
cleanup completion guard và retry-budget transaction. Không sửa migration/checksum,
protobuf, dependency hoặc runtime/volumes V1.

- Loop claim từng job khi có slot, không prefetch queue. Mặc định tối đa 2 job,
  1 finalize; giữ một slot cho reconcile/cleanup. Mỗi page tối đa 8 part;
  finalize đọc từng block tối đa 256 KiB, hash bytes theo manifest order, không
  materialize file. Read budget mặc định 64 MiB/s cho mỗi finalize, job budget
  3600s. Đây là giới hạn cấu hình, chưa phải số đo RSS/băng thông C5.
- Renew mỗi 15s bằng DB clock, lease 60s; kiểm expiry sau khi sở hữu job row lock.
  Mất renew hoặc shutdown hủy RPC đang chạy; RPC có deadline 5s. SIGINT/SIGTERM
  đóng claim loop, drain trong budget mặc định 30s; intent chưa kết thúc được
  process mới nhận lại sau lease expiry. `ready()` kiểm loop đang chạy và DB gate;
  process CLI không có HTTP readiness endpoint.
- RECONCILE Inspect exact scope/spec/generation/actual ACK. STORED đủ thì
  `reconcile_part`; không tự cấp claim khi thiếu consumer. C3 có thể gọi đồng bộ
  `retry_for_consumer(chunk_id, owner)` trên Worker riêng: commit generation intent,
  Prepare mọi destination để fence/drain, rồi trả claim/token cho caller. Slot
  riêng chặn hai consumer cùng dùng instance. Crash giữa intent và một phần Prepare
  được phục hồi bằng generation tiếp theo, không tin Inspect ở generation cũ.
- CLEANUP authorize → durable Delete ACK → release từng allocation, I/O ngoài
  transaction. Retry bắt đầu từ đầu và bỏ các allocation RELEASED; finish kiểm lại
  toàn session không còn allocation chưa release hoặc attempt chưa CLOSED. Mất
  ACK giữ nguyên charge; process chết sau release không trừ lần hai.
- FINALIZE kiểm header/key/spec/actual hash, block offset/size, footer và successful
  EOF. Replica lỗi không làm thay đổi digest prefix; có thể fallback. Publish chỉ
  sau full bytes hash, DB recheck revision/manifest/RF/job token. Full hash mismatch
  FAILED và enqueue cleanup; abort, epoch gate hoặc lease/token cũ chặn publish.
- Retry exponential 5–300s, mặc định tối đa 8 lần claim (kể cả reclaim sau crash).
  Hết budget reconcile/finalize: FAILED, release admission và enqueue CLEANUP cùng
  transaction; cleanup DEAD giữ charge/nghĩa vụ và `last_error` để vận hành xử lý.
  Không có consumer cũng tiêu thụ budget, không giữ upload RECONCILING vô hạn.

### Môi trường, lệnh và kết quả

Tier **focused integration**, Docker Engine 29.8.2, Linux/Python 3.12,
PostgreSQL 16. Preflight `docker info --format '{{.ServerVersion}}'`,
`docker compose -f deploy/compose.v2-metadata-tests.yml ps` và psql read-only trả
`dfs_v2_test|v2_test|1`; PostgreSQL healthy. Lần preflight trong sandbox bị chặn
quyền named pipe; chạy lại với quyền Docker đã xác nhận dịch vụ có sẵn. Không
start/restart DB, không migrate public hoặc mount volume V1.

Mỗi fixture dùng schema `test_v2_<uuid>`, hai Storage server gRPC loopback với
SQLite ledger/filesystem tạm riêng, RF=2/domain=2, chunk 3 byte để kiểm boundary.
Process crash dùng SIGKILL thật; lifecycle cuối chạy đúng entrypoint
`python -m metadata.v2.worker`, SIGTERM khi GetChunk đang chờ, kiểm RPC drain và
replacement Worker publish sau reclaim. Không suy ra mất host/power hoặc benchmark.

Từ root, mỗi lần source trong image đổi đều build và đợi thành công trước run:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests

docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c2.py
```

Gate đầu **28 passed** (44.41s), không skipped/deselected/failure. Bao gồm lost
Store/Delete ACK, lease sweeper, cleanup 10 part qua nhiều page, empty/ordered full
hash, mismatch, stale finalize/abort/epoch, bounded concurrency/shutdown/renew và
10 SIGKILL boundary cases (claim/read/publish, cleanup intent/ACK/release,
generation intent/drain/handoff). Sau khi bổ sung guard partial generation,
consumer slot, retry budget sau reclaim và malformed-wire cases, chỉ chạy affected:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c2.py -k "partial_generation or live_old_writer or malformed_wire or reconcile or retry or bounded_loop or finalize_hashes"

docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c1.py tests/test_v2_metadata.py -k "job or cleanup or retry or reconcile or finalize"
```

Affected C2 **27 passed, 15 deselected** (24.44s); direct C1/B3 regressions
**31 passed, 64 deselected** (14.36s), không skipped/failure. Bổ sung test entrypoint,
rebuild test layer rồi chỉ chạy ca mới:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c2.py -k worker_entrypoint_sigterm
```

**1 passed, 42 deselected** (3.24s). Rà guard cuối transaction RETRY/DEAD đã thêm
recheck lease sau tentative failure/enqueue cleanup và CAS bằng DB clock trước
đổi job state. Regression mới ép expiry trong transaction, xác minh rollback
FAILED/admission/cleanup nếu Worker mất lease. Rebuild rồi chỉ chạy nhánh affected:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c2.py -k "exhausted_job or retry_budget or background_reconcile or cleanup_pages or late_finalize"
```

**10 passed, 34 deselected** (7.91s), không skipped/failure. Tổng **44 ca C2 khác
nhau** qua các lượt, không cộng rerun thành full suite. Ruff check/format bốn
Python paths đã qua trên Windows/Python venv (chỉ check/format lại paths đổi).
Chỉ build/run
tests image; chưa deploy Worker lâu dài, chưa chạy HTTP/Transfer E2E, client,
full suite/frontend, 1/10 GiB, RSS/throughput hoặc cluster restore.

## 9. Bàn giao C3 tại thời điểm chốt C2 — lịch sử

C2 hoàn tất phạm vi Worker; **C vẫn chưa hoàn tất**. C3 cần:

1. API public/internal và Transfer V2 riêng, strict DTO/envelope/body limits,
   deadline/cancellation/drain; allocation/attempt commit trước Storage RPC.
2. Nối resume/retry với consumer UUID và synchronous handoff; consumer chết hoặc
   mất response phải để claim hết lease/reconcile, không claim nền rồi bỏ token.
   Không chạy `retry_for_consumer` trên instance đang giữ background loop.
3. Readiness/lifespan riêng cho API/Transfer/Worker, pool/admission/finalize/backlog
   budget trước readiness; CLI Worker hiện dùng WorkerConfig defaults, adapter
   deployment cần truyền cấu hình rõ ràng và expose health phù hợp.
4. Gate HTTP → API → Transfer → Storage → Worker thật: 200 part sau confirm commit,
   202 complete tới publish, actual size/hash và replay body, shutdown/resume/abort.
   Benchmark/two API/two Transfer/large-file gate vẫn thuộc C5.

## 10. Evidence C3 — 24/10/2026

Hiện thực API upload V2 trong `backend/metadata/v2/api.py`, strict wire/envelope
và bounded control transport trong `http.py`, role/lifespan/readiness trong
`runtime.py`; byte plane riêng ở `backend/transfer/v2/app.py`. Repository bổ sung
bounded status/part page và gate backlog trước nhận phiên mới, sau kiểm replay.
Không sửa DDL/checksum, protobuf hoặc Python dependencies. Image runtime thêm
package Transfer; Nginx chỉ thêm vào test stage `v2-http-tests` riêng.

- Public và internal là hai ASGI listener riêng. Public không có route internal,
  response không lộ claim token/allocation/endpoint. DTO từ chối unknown fields,
  duplicate keys, số không nguyên chính xác, ID/hash không canonical và control
  characters; JSON actual body tối đa 16 KiB/deadline 10s. Error envelope,
  Retry-After, Location và request ID/log lỗi 500 đã kiểm qua HTTP.
- Transfer không truy cập DB hay spool file; actual size/hash được đếm tăng dần,
  mỗi block ≤256 KiB, queue tối đa 2 block/replica. Admission trước đọc body,
  buffer reservation và control-call budget hữu hạn; replica chậm chặn producer.
  Deadline toàn request giới hạn RPC theo thời gian còn lại; mất renew/disconnect
  hoặc shutdown hủy/drain stream. Best-effort uncertain có timeout; lease sweeper
  vẫn là cơ chế phục hồi nếu caller chưa nhận được claim/token.
- Claim/attempt đã commit trước Prepare/Store. Chỉ clean EOF sau đủ size/hash;
  ACK exact identity/spec/actual hash và STORED; 200 sau confirm commit quan sát
  từ connection khác. VERIFIED replay vẫn kiểm toàn body part, không allocation
  mới. Sai body → RECONCILING → dedicated consumer Worker fence/drain bằng
  generation tiếp theo → handoff đúng owner; mất phản hồi handoff phải qua expiry
  và sweep, không lấy lại claim đang sống. STORED mất confirm/response phục hồi
  bằng inspect, không tăng generation nếu không cần ghi lại.
- Complete 202/Location → Worker đọc/hash bytes theo manifest → AVAILABLE và
  COMPLETED; replay complete 200. Empty file, chunk boundary và 2 MiB+4B qua
  nhiều block đã kiểm; hash nguồn sai → FAILED và cleanup. Abort 202 tới cleanup
  hết pending rồi 200; complete ABORTED/EXPIRED trả 410. ACK sai generation không
  được confirm. Backlog gồm cả DEAD chặn create mới nhưng không chặn replay.
- API/Transfer/Worker có readiness/lifespan độc lập, pool/admission/worker/finalize
  budgets trước readiness. Entry point `python -m metadata.v2.runtime` đã chạy
  subprocess cho cả bốn role api/internal/transfer/worker, SIGTERM thật. Uvicorn
  có thể re-raise SIGTERM sau drain; kiểm durable state và replacement resume,
  không chỉ exit code. Race tín hiệu trước claim response giữ CLAIMED hợp lệ;
  test ép DB lease expiry rồi sweep, không chờ 60s hoặc bỏ qua fencing.
- Nginx thật dùng `deploy/v2-ingress.conf`: HTTP/1.1, buffering off, không tự retry
  upstream mutation. Request chưa gửi EOF đã tới claim ở DB; sau gửi tiếp đủ bytes
  trả 200 rồi complete/publish thành công. Ingress không forward `/internal/v2`.

### Môi trường, lệnh và kết quả

Tier **focused integration**, Docker Engine 29.8.2, Linux/Python 3.12,
PostgreSQL 16 và Nginx 1.26.3 trong tests image. Read-only preflight `docker info`,
Compose ps và psql trả `dfs_v2_test|v2_test|1`, PostgreSQL healthy. Sandbox ban đầu
không có quyền named pipe; dùng quyền Docker được chấp thuận để xác nhận dịch vụ.
Không start/restart DB, không migrate public, không dùng Storage/volumes V1.
Mỗi fixture sở hữu schema `test_v2_<uuid>`, HTTP loopback ports và hai Storage
gRPC/SQLite/bytes tạm, RF=2/domain=2. Chunk fixture cơ bản 3B; ca nhiều block
snapshot chunk 1 MiB. Đây không phải benchmark RSS/throughput hoặc nhiều host.

Từ root, build và đợi thành công sau mỗi thay đổi code trong image:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c3.py

docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_metadata.py -k 'create_idempotency or admission_race'
```

Gate C3 **25 passed** (39.90s), không skipped/failure. Direct create/admission
regressions **2 passed, 34 deselected** (0.70s). Hai lượt đầu của C3 có assertion
SIGTERM quá chặt (chỉ nhận exit=0 hoặc bắt buộc uncertain đã commit); đã sửa harness
theo lifecycle Uvicorn và trường hợp mất claim response, rồi kiểm resume sau sweep.
Không còn failure mở.

Rà hợp đồng cuối bổ sung 410 terminal complete, request ID/log 500 và kiểm ACK
sai generation qua internal HTTP. Rebuild rồi chỉ chạy affected:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c3.py -k 'complete_publish or terminal_session or error_request_id or real_ingress'
```

**8 passed, 20 deselected** (11.55s), không skipped/failure. Tổng **28 ca C3 khác
nhau + 2 direct regressions** qua các lượt phù hợp với code bị ảnh hưởng; không
cộng rerun thành full suite. Native Windows `.venv` Ruff check/format --check
passed tám Python paths; sau sửa cuối chỉ check lại ba paths đã đổi.

Không chạy full V1/backend/frontend suite, client C4, hai API/hai Transfer,
benchmark/1–10 GiB/RSS, multi-host, download/Range hoặc restore/HA. Chỉ build và
chạy test topology tạm; chưa deploy các daemon V2 lâu dài. C vẫn chưa hoàn tất.

## 11. Bàn giao C4 tại thời điểm chốt C3 — lịch sử

1. Client tối thiểu đọc/hash nguồn tăng dần **trước create**, giữ identity nguồn
   `size_bytes`/`expected_sha256`; chọn lại file phải so full hash trước resume.
2. Persist upload ID và Idempotency-Key cho create; tra session/parts theo trang,
   chỉ gửi phần còn thiếu, hash từng part. Không đổi expected hash của part cũ,
   không retry mù POST/PUT khi outcome chưa rõ; tôn trọng Retry-After.
3. Complete 202 thì poll upload tới COMPLETED/FAILED; không coi 200 part hoặc
   progress bytes là file AVAILABLE. Có cancel/abort và trạng thái cleanup pending.
4. Kiểm đúng nguồn/sai file cùng tên-size/nguồn đổi, mất response/restart/resume và
   chỉ resend phần cần thiết qua HTTP hiện có. Không gộp C5 two-instance,
   large-file/RSS/finalize timing hoặc download D vào evidence C4.

## 12. Evidence C4 — 25/10/2026

Client tối thiểu ở `frontend/src/app/pages/storageV2`, route độc lập `/uploads-v2`,
HTTP adapter riêng `frontend/src/api/storageV2`. Không sửa runtime, migration,
protobuf hay API/domain/backend V1/V2; backend chỉ thêm fixture test sở hữu riêng.
Thêm exact dependency `@noble/hashes` **2.4.0** và integrity lock, không đổi các
dependency khác hoặc thêm test framework/npm test script.

- Hash tăng dần trong Web Worker, block ≤256 KiB, vector empty/abc và các boundary
  262143/262144/262145/1048593 byte khớp Web Crypto reference trên fixture nhỏ.
  Pre-hash hoàn thành trước POST create; cancel Worker trước create không cấp
  session. Web Crypto không nằm trên đường hash nguồn production.
- Journal localStorage lưu canonical payload/key trước mutation; upload ID,
  pending part/abort và Retry-After qua reload, không lưu File/bytes. Quota/corrupt
  journal chặn mutation. Hai controller dùng Web Locks thật không tạo phiên
  song song; đây không phải gate hai API/Transfer hoặc hai browser riêng.
- Lost create response sau commit → giữ key/payload → explicit resume replay
  cùng key, không thêm session. Lost part response sau confirm → GET thấy
  VERIFIED → không PUT lại. 503 trước PUT → GET trạng thái → chờ Retry-After
  ≥1s theo audit thời gian request trước retry.
- Complete response mất sau commit → GET/poll, không POST complete lần hai.
  Worker dừng giữ FINALIZING/202, client hết poll budget là paused; bật lại Worker,
  controller mới tiếp tục chờ không cần file và không mutation. Chỉ COMPLETED
  mới là thành công; source fault sau pre-hash làm full finalize FAILED.
- Pause sau confirmation giữ phiên; abort mất response vẫn GET/đợi cleanup tới
  ABORTED và hết pending, không POST abort mù lần hai. Abort của create đã gửi
  nhưng chưa nhận ID resolve bằng key cũ, không cần chọn lại nguồn.
- Reload browser thật sau ba part VERIFIED; journal/key/ID khôi phục. File cùng
  tên/size khác bytes bị chặn trước mọi POST/PUT. Chọn đúng nguồn, đọc manifest
  thực qua page size 2: chỉ PUT hai part còn thiếu, không resend ba part cũ.
  Adapter không forward Authorization dù Axios defaults có synthetic bearer.

### Gate, môi trường và kết quả

Tier **focused browser/API integration**. Windows frontend với Node **26.5.1**,
Vite **6.3.5**, browser IAB/Chromium; Docker Engine **29.8.2**, Linux/Python 3.12,
PostgreSQL 16, Nginx 1.26.3. Preflight Docker pass nhưng PostgreSQL test ban đầu
chưa chạy; đã dừng gate phụ thuộc và báo đúng blocker. Người dùng cho phép
khởi động fixture V2; `docker compose -f deploy/compose.v2-metadata-tests.yml up -d --wait postgres`
và psql ready trả `dfs_v2_test|v2_test|1`. Không xóa volume/reset dữ liệu khác.

Fixture `backend/tests/v2_c4_fixture.py` dùng schema riêng
`test_v2_59edb09749d1420c872d8189454f117a`, hai Storage gRPC/SQLite/bytes tạm,
RF=2/domain=2, chunk 65536 byte. Nginx cấu hình C3; fault hooks chỉ có trong tests.
Mạng test internal không publish port được trên môi trường này; overlay C4 thêm
mạng browser riêng chỉ cho tests service, bind ingress localhost 18080. PostgreSQL
vẫn chỉ trên mạng internal cũ. Vite phải chạy ngoài sandbox để browser có quyền
loopback; không đổi firewall/network isolation của DB.

Lệnh build/run fixture từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-browser-tests.yml run --rm --no-deps -p 127.0.0.1:18080:18080 --name dfs-v2-c4-fixture tests python tests/v2_c4_fixture.py
```

Frontend dùng Vite và harness versioned `/checks/v2-upload.html`, bấm Run,
chờ RELOAD_READY, reload thật rồi Run tiếp. **18 nhóm kiểm passed**, không
failure trong report cuối; một bước chuẩn bị partial session là setup của gate
reload, không cộng riêng vào 18. Lượt đầu bị Vite dependency optimization reload
ngắt khi vừa hash; không tính là evidence, chỉ lượt hoàn thành report được giữ.

Gate không mock Axios adapter/API transactions/Storage/Worker. Có fault injection
proxy 503 trước request/sau commit và hasher fault để kiểm cửa sổ source đổi;
đây không phải mất gói hoặc sửa file vật lý trên disk. Reference digest chỉ trên
fixture nhỏ; Python đọc lại bytes **cả hai replica**, kiểm size/chunk/full SHA và
domains cho **10 file AVAILABLE** (empty file không có allocation). Ba file FAILED
là hai abort và một full-hash mismatch; không là lỗi assertion. Tổng 13 session,
42 allocation records gồm cả nghĩa vụ cleanup/tombstone, không coi 42 là copies
đang phục vụ. Không cộng các giá trị này thành số test.

Evidence ignored: `.runtime/v2-c4/backend-report.json` và
`.runtime/v2-c4/browser-gate.png`; fixture source và harness được versioned.
Report và ảnh đã lưu trước khi dọn topology tạm.
Fixture cuối đã exit 0; kiểm PostgreSQL trả 0 schema còn lại cho hai fixture C4
đã tạo. Mạng browser riêng đã dọn sau khi hết attachment; dev server do phiên
này khởi động đã dừng, PostgreSQL V2 giữ nguyên. Không chạy lại gate sau sửa docs.

Tier **targeted lint/build** từ frontend:

```powershell
.\node_modules\.bin\eslint.cmd src/api/storageV2 src/app/pages/storageV2 src/app/router/router.jsx checks
npm run build
```

ESLint sạch. Build lần đầu pass **35.43s**, Worker bundle riêng **6.31 kB**.
Sau sửa phần hiển thị state/counters sau reload và màu nút, chỉ lint affected
paths và build lại: pass **50.65s**, không chạy lại các luồng client/API đã pass
chỉ vì thay mapper hiển thị. Ruff check/format --check fixture Python passed.
Các warning starter sẵn có (Table/PreviewImg, Accordion circular chunks, CSS
`:is()`, chunk >500 kB, Node module.register deprecation) vẫn còn; không sửa ngoài scope.

**Giới hạn UI automation:** route React đã render controls đúng label trước khi
chọn file. File chooser automation mất target và treo; navigation IAB sau đó
timeout dù Vite HTML/JS trả 200. Không claim native picker hoặc toàn bộ click-flow
React UI upload đã pass. Fixture `/checks/v2-upload-ui.html` bổ sung cách chọn
File tổng hợp qua input change event để kiểm thủ công; phiên này chưa chạy xong
gate bổ sung đó. 18 nhóm trên kiểm controller/hash/journal/API thật trong browser,
không thay thế bằng lint/build và không nâng thành UI E2E đầy đủ.

Không chạy full frontend/backend/V1 suites, cluster smoke, hai API/hai Transfer,
1/10 GiB, RSS/throughput/finalize benchmark, multi-host, download D hoặc restore/HA.
C4 hoàn tất phạm vi client tối thiểu; **mốc C vẫn chưa hoàn tất**.

## 13. Bàn giao C5

1. Topology ít nhất hai API public, internal và hai Transfer, cùng metadata/session;
   luân phiên instance/route không sticky session. Ghi command, build và policy.
2. Dùng client C4 pre-hash, kiểm empty/boundary/1 GiB/10 GiB; đối chiếu full SHA,
   size/part count và RF/domain từ bytes thật, không chỉ counters hay ACK.
3. Đo RSS, queue/buffer, concurrency và backpressure cả thư viện/socket; đo riêng
   pre-hash, gửi bytes, chờ job/finalize, full verification và thời gian AVAILABLE.
4. Restart/kill tại các boundary khi client/worker đang chạy, mất response/resume,
   abort/cleanup và charge không double release; lưu evidence theo đúng topology.
5. Rà DoD C với C1–C4 evidence trước bàn giao download/Range/read-delete lease D.
   Không dùng một lượt aggregate suite hoặc UI polish để thay gate tài nguyên.

## 14. Evidence C5 bước đầu — 26/10/2026

Thêm `backend/tests/test_v2_c5.py`, không đổi production runtime, dependency,
contract, migration hoặc deployment. Tier **focused integration**, Docker Engine
29.8.2, Linux/Python 3.12, PostgreSQL 16 và Nginx 1.26.3. Native Windows chỉ chạy
Ruff check/format file test mới. Preflight Docker/Compose báo PostgreSQL healthy;
psql ban đầu bị sandbox từ chối Docker pipe, kiểm read-only bằng quyền phù hợp
trả `dfs_v2_test|v2_test|1`. Không start/restart DB hoặc migrate schema public.

Mỗi ca có schema `test_v2_<uuid>` và hai gRPC/SQLite Storage với thư mục tạm
riêng, chunk 65536 byte, RF=2/domain=2, budget mỗi node 16 MiB. Runtime thật chạy
bằng `python -m metadata.v2.runtime`: hai API public, hai internal listener và
hai Transfer là sáu process độc lập; thêm một Worker process. Nginx dùng cấu hình
C3 với upstream round robin cho từng role, `proxy_next_upstream off`, không
sticky session. Listener internal chỉ ở loopback container; public chặn route
internal. Access log và header test xác nhận cả hai instance thực sự nhận request.

Lệnh từ root đã chạy:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests

docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps -v D:/distributed-file-storage/.runtime/v2-c5:/evidence -v D:/distributed-file-storage/backend/tests/test_v2_c5.py:/app/tests/test_v2_c5.py:ro -e V2_C5_REPORT_DIR=/evidence tests python -m pytest -q -x tests/test_v2_c5.py
```

Image build passed. File test được mount read-only để dùng harness vừa sửa trên
runtime/config đã build hiện tại. Lượt đầu **2 passed, 1 failed** (116.75s):
ca SIGKILL timeout vì harness dừng Worker trong lúc reconcile còn giữ job lease.
Runtime cố ý giữ job đang chạy khi shutdown; harness đã được sửa để đợi commit
RETRY trước shutdown và đợi backoff thật trước consumer handoff. Không sửa runtime
hoặc timestamp DB. Chỉ chạy lại ca bị ảnh hưởng:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps -v D:/distributed-file-storage/.runtime/v2-c5:/evidence -v D:/distributed-file-storage/backend/tests/test_v2_c5.py:/app/tests/test_v2_c5.py:ro -e V2_C5_REPORT_DIR=/evidence tests python -m pytest -q tests/test_v2_c5.py -k sigkill
```

Rerun **1 passed, 2 deselected** (92.03s), không skipped/failure. Tổng **3 ca
khác nhau đã qua** qua hai lượt focused; không gọi là một lượt full suite.

| Ca | Evidence |
|---|---|
| Round robin, empty và boundary | 0/65535/65536/65537/196625 byte; create/replay qua API khác giữ cùng ID; VERIFIED replay qua Transfer khác không thêm charge; parts paging limit=1 và resume chỉ gửi phần thiếu; complete → AVAILABLE |
| Restart sau confirmation | SIGKILL một API và một Transfer sau part 0 VERIFIED; API còn sống đọc được session; process mới dùng cùng DB/config, resume chỉ PUT parts 1–3; tám allocation, charge đúng RF × size, file AVAILABLE |
| SIGKILL giữa stream | Cả hai Storage WRITING trước kill; Transfer còn lại nhận PART_BUSY khi claim sống; Worker sweep sau lease thật, thời gian đợi đo được 59.90s; consumer khác Prepare fence/handoff generation 2, charge không tăng đôi; abort 202 → 200, cả hai ledger DELETED, chunks rỗng và charge=0 sau replay abort |

Sáu file AVAILABLE (gồm một file rỗng) được đối chiếu metadata/manifest, part
count, actual/full/chunk SHA và bytes đọc lại trên **cả hai** Storage/domain.
File rỗng không có part/allocation; RF=2 trong report là policy, không phải hai
copy vật lý của empty file. Ca bị abort không được tính là AVAILABLE.

JSON topology/ports/PIDs/request audit/disk evidence và service logs lưu trong
ignored `.runtime/v2-c5/`; report có thể tồn tại cả khi assertion lỗi, nên luôn
đọc cùng kết quả pytest. Fixture source được versioned. Đã kiểm read-only sau
teardown: **0** schema còn lại trong bốn schema sở hữu của hai lượt, test containers
đã được `--rm` dọn; PostgreSQL test vẫn healthy. Không dọn/reset dữ liệu V1.

Native `.venv` Ruff check và format --check `tests/test_v2_c5.py` passed. Chỉ có
formatting sau lượt integration cuối, không đổi behavior hay chạy lại checks đã
pass vì sửa docs. Không chạy full V1/backend/frontend, C1–C4 regressions, UI E2E,
1/10 GiB, RSS/throughput/queue/socket/backpressure, multi-host hoặc download D.

**C5 còn lại:** dùng client C4 pre-hash/resume trên topology này; gate 1 GiB rồi
10 GiB với hash/RF thật; đo riêng pre-hash, transfer, chờ job/finalize, full
verification và AVAILABLE; đo RSS/buffer/queue/concurrency/backpressure; bổ sung
lost-response và crash Worker/finalize trên nhiều instance theo matrix, rồi rà
DoD C trước bàn giao D. Restart gate trên thay process trước khi gửi request mới,
không chứng minh ingress failover trong downtime hoặc HA/multi-host.

## 15. Chuẩn bị gate browser/tệp lớn — 26/10/2026, chưa hoàn tất

Thêm harness `frontend/checks/v2-c5.html`/`v2-c5.js` và fixture
`backend/tests/v2_c5_fixture.py`, `v2_c5_service.py`, `v2_c5_storage.py`.
Các probe/fault chỉ nằm trong tests: queue high-water/blocked puts, RSS/RssAnon/HWM
100ms qua `/proc`, TCP socket memory qua `ss`, mất response sau commit, Transfer
giữ trước confirm, Worker giữ trước publish và mất durable Delete ACK. Tệp nguồn
dùng OPFS File ghi theo block 1 MiB, hash bằng chính Worker C4 đọc 256 KiB; không
tạo Blob 10 GiB trong RAM. Các nhánh lớn/crash/slow **chưa được xác nhận runtime**.

Fixture có hai process Storage riêng cùng hai API public, hai internal, hai
Transfer, một Worker và Nginx; chunk 8 MiB, RF=2/domain=2, budget 16 GiB/node.
`deploy/compose.v2-c5-tests.yml` đặt memory limit 3 GiB cho riêng tests service.
Helper Runtime/ingress C5 thêm tham số tùy chọn cho entrypoint test, bind/port và
control upstream; mặc định gate file nhỏ giữ nguyên. Không sửa production code,
contract, dependency, DB migration hoặc V1. Chưa chạy lại gate cũ vì chỉ thêm
nhánh harness tùy chọn, không dùng các gate cũ để suy ra harness mới đã đúng.

Tier **focused browser/API integration**. Preflight Docker 29.8.2 pass nhưng
PostgreSQL test đã dừng; dừng dependent gate và hỏi người dùng theo AGENTS.md.
Sau khi người dùng cho phép, chạy:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml up -d --wait postgres
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -Atc 'SELECT current_database(),current_user,1 AS ready'
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-browser-tests.yml -f deploy/compose.v2-c5-tests.yml run --rm --no-deps -p 127.0.0.1:18080:18080 --name dfs-v2-c5-fixture -v D:/distributed-file-storage/.runtime/v2-c5-browser:/evidence -e V2_C5_REPORT_DIR=/evidence tests python tests/v2_c5_fixture.py
```

DB healthy, psql `dfs_v2_test|v2_test|1`, image build passed. Không migrate public
hoặc reset dữ liệu khác. Compose run không hỗ trợ `--memory`; đã chuyển limit
sang overlay trên trước khi fixture chạy. Windows có khoảng 49 GiB trống trên C:
và 39 GiB trên D: lúc preflight; đây không phải cam kết capacity production.

Vite chạy native Windows bằng
`frontend/node_modules/.bin/vite.cmd --host 127.0.0.1 --port 5173 --strictPort`
(từ frontend dùng `.\node_modules\.bin\vite.cmd ...`). npm wrapper từ chối
flags nên dùng CLI có sẵn, không thay dependency hay cài thêm công cụ.
Mở `/checks/v2-c5.html` trong IAB/Chromium 156. **Một ca boundary-0 passed**:
client C4 hash empty trước create, complete → AVAILABLE, manifest/size/hash được
đối chiếu; empty không có allocation/bytes replica vật lý.

Sau ca đầu, browser báo `Failed to fetch` trước phase tiếp theo; không có request
phase mới trong ingress audit. API và Vite vẫn trả lời kiểm tra localhost ngoài
sandbox. Không coi đây là assertion/runtime storage failure hoặc tiếp tục chạy
aggregate gate. Thử chuyển sang Chrome riêng bằng Computer Use bị công cụ dừng
trước thao tác: không xác định URL hiện tại trên Windows đủ tin cậy để thực thi
policy. Đã dừng browser work; không có kết quả Chrome, crash hay file lớn.

Evidence partial trong ignored `.runtime/v2-c5-browser/`: `browser-report.json`,
`disk-evidence.json`, `resources.json`, `topology.json`, `transfer-probes.jsonl`,
`requests.jsonl`, worker events và service logs. Các số đo mới chỉ thuộc setup/
empty case, **không phải RSS/queue/backpressure benchmark 1–10 GiB**.
Fixture nhận control finish, exit 0 và dọn schema/thư mục tạm; Vite đã dừng.
PostgreSQL V2 giữ nguyên theo setup được cho phép.

Targeted native Ruff check/format bốn Python paths và ESLint `checks/v2-c5.js`
passed. Không chạy full suites, frontend build (chỉ thêm dev harness), các gate
file nhỏ đã pass, 1/10 GiB, slow replica, reload/crash mới hoặc browser UI E2E.
**C5 còn mở:** cần browser localhost ổn định để xác nhận harness trước khi chạy
các gate còn lại trong mục 14; không bàn giao D từ evidence partial này.

## 16. Tiếp tục C5 — 28/10/2026, browser vẫn bị chặn

Đã khôi phục ngữ cảnh và tiếp tục đúng gate **focused browser/API integration**
ở mục 15. Không sửa runtime/harness, không chạy lại Ruff/ESLint hoặc các gate đã
pass. Docker pipe cần quyền ngoài sandbox; read-only preflight với quyền phù hợp
trả Engine `29.8.2`, PostgreSQL fixture đang dừng. Dùng quyền khởi động fixture
người dùng đã cấp trong chat trước, chạy lại lệnh `up -d --wait postgres` ở mục
15; DB healthy, psql trả `dfs_v2_test|v2_test|1`.

Giữ image `dfs-v2-http-tests:dev` đã build ngày 26/10 do code/config không đổi;
không rebuild hoặc coi việc image tồn tại là kết quả kiểm thử. Chạy cùng lệnh
fixture ở mục 15 với thư mục evidence mới `.runtime/v2-c5-20261028/`, giới hạn
3 GiB, `--no-deps` và port loopback 18080. Fixture báo ready với schema riêng
`test_v2_f42da0c3f46046ad9289b26aecc5f47d`; Vite native Windows ready ở port 5173.

Browser trong app không mở được `http://127.0.0.1:5173/checks/v2-c5.html`:
`net::ERR_CONNECTION_TIMED_OUT`. Chuyển sang Chrome trên Windows bị Computer Use
dừng trước thao tác vì không xác định được URL hiện tại đủ tin cậy để thực thi
policy. Không lặp lại browser gate, không coi đây là lỗi assertion của Storage;
**không có ca browser/file lớn/crash mới chạy trong lượt này**.

Đã gửi control `finish` bằng HTTP ngoài sandbox: trả `ok=true`; fixture báo
`stopped=true`, exit 0 và chạy teardown schema/thư mục tạm. Vite đã dừng,
PostgreSQL test giữ nguyên. Đây là evidence setup/teardown, không phải evidence
upload, RSS, backpressure hoặc failover. Thay đổi tài liệu này thuộc tier
**no execution**; không chạy thêm checks cho docs. C5 vẫn còn các gate ở mục 14–15,
chưa bàn giao D.

**Điều chỉnh theo chỉ đạo người dùng (28/10/2026):** tạm gác gate browser C5,
chuyển sang triển khai phần download độc lập. C5 vẫn chưa hoàn tất; không coi
đây là nghiệm thu hoặc bàn giao hoàn chỉnh mốc C. Tiến độ mới theo
[kế hoạch D](V2_D_IMPLEMENTATION_PLAN.md), bắt đầu với logic HTTP D1 không cần browser.
