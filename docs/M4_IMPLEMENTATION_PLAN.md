# M4 — Failure, DELETE, cleanup bền vững và manual repair

**Ngày lập:** 04/10/2026  
**Trạng thái:** M4 hoàn thành P1–P7 ngày 04/10/2026; bàn giao M5 UI tối thiểu.  
**Đầu vào:** M0–M3 hoàn thành; xem [bàn giao M3](M3_IMPLEMENTATION_PLAN.md#bàn-giao-m4--failure-delete-cleanup-và-repair).  
**Đầu ra:** file đã xóa không hồi sinh, replica của upload lỗi được dọn khi node
reachable trở lại, repair khôi phục RF bằng bytes đã xác minh và giữ mapping qua lỗi/restart.

## 1. Phạm vi và nguyên tắc

Theo [kế hoạch tổng](IMPLEMENTATION_PLAN.md), [ARCHITECTURE.md, mục 8–9](ARCHITECTURE.md)
và [API_CONTRACTS.md, mục 2.5, 2.7 và 3](API_CONTRACTS.md):

- Thêm Metadata Delete RPC wrapper và `DELETE /api/v1/files/{file_id}`.
- Commit DELETING/cleanup flags trước RPC; DELETED chỉ sau xác nhận dọn hết.
- Background cleanup cho FAILED/DELETING, bounded và tiếp tục sau restart.
- `POST /api/v1/admin/repair`: probe known mappings, copy replica thiếu,
  reconcile node trở lại, filter/cursor/budget và summary đúng contract.
- Kiểm tra fault windows, concurrency, cancellation, shutdown và lifecycle thật.

Giữ một Metadata process, một operation lock và scheduler thuộc lifespan.
Không giữ transaction qua RPC; không chia sẻ Session giữa threads. Timeout là
outcome chưa biết: giữ attempted mapping để reconcile/cleanup. Health ACTIVE
không chứng minh chunk còn nguyên. RF lấy từ file, không từ default hiện tại.

Schema hiện có đủ states, cleanup flag, timestamps và tombstone; dự kiến không
thêm migration/proto/RPC. UI thuộc M5, hai host thật thuộc M6. Không thêm job
queue, background repair, inventory/orphan scan, auto prune dư RF hay GC tombstone.

## 2. Điểm xuất phát và các quyết định triển khai

Snapshot dưới đây ghi điểm xuất phát trước P1; implementation cuối và DoD
đã đối chiếu nằm ở mục 12, evidence theo từng phase ở cuối tài liệu.

| Thành phần hiện có | Dùng trong M4 |
|---|---|
| `storage_client.py`, `operations.py` | Thêm Delete theo cùng deadline/retry/call scope; cấm RPC trong transaction |
| `models.py`, `bootstrap.py` | Giữ rows; UPLOADING → FAILED đã có, FAILED/DELETING được enqueue lại; AVAILABLE PENDING giữ để probe |
| `worker.py`, `main.py` | Worker hiện chỉ health; nối cleanup vào scheduler, giữ health độc lập và sửa thứ tự drain |
| `config.py` | Cleanup interval 5 s; repair max 8 chunks/budget 30 s đã validate; không thêm env nếu chưa cần |
| `placement.py`, `replica_health.py`, `download.py` | Dùng lại selection/counters và quy tắc xác minh; không gọi download assembly để repair |
| `routes/files.py`, `schemas.py`, `errors.py` | Thêm DeleteResult/route và RepairRequest/Result/admin route, envelope nhất quán |
| `tests/conftest.py`, lifecycle tests M3 | PostgreSQL schema riêng, Storage server thật/temp dirs và child Metadata process |

Module dự kiến: `metadata/cleanup.py`, `metadata/delete.py`, `metadata/repair.py`,
`metadata/routes/admin.py`. Có thể điều chỉnh tên/tách file khi code; chỉ tách
helper dùng chung khi delete/cleanup/repair thật sự cần, tránh refactor M3 rộng.

Các lựa chọn nội bộ dưới đây phải được kiểm tra trong implementation:

1. **Cleanup bounded và công bằng:** tối đa 8 replica/lượt, từng RPC tối đa
   2 attempts × deadline 5 s + backoff 0.2 s. Chọn enabled/configured reachable
   (ACTIVE/SUSPECTED); DOWN/disabled giữ pending, không gọi RPC. Dùng thứ tự ổn
   định và cursor luân phiên trong process để 8 mappings lỗi đầu không chặn
   mappings sau. Cursor có thể reset khi restart vì pending thật nằm trong DB;
   không thêm bảng queue. Tổng lượt hữu hạn nhưng không hứa xong trong 5 s.
2. **DELETE nhanh và tiếp tục được:** dùng cùng cleanup engine cho một lượt
   tối đa 8 replica của file rồi trả 200/202. File lớn có thể trả 202 dù các
   node online; worker tiếp tục. DELETE DELETING lặp trả snapshot 202 theo
   contract, không khởi tạo lại tombstone/flags; DELETED trả 200.
3. **Cleanup ưu tiên trước repair:** dưới cùng operation scope, chạy một lượt
   cleanup bounded đang đến hạn/có việc reachable trước scan repair. Pending
   trên node DOWN/disabled không chặn mọi repair vô hạn. Không gọi lồng helper
   lấy lock lần nữa. Budget monotonic của request phải tính cả công việc trước
   scan. P5 chốt trường hợp hết budget trước chunk đầu: checked_chunks=0, giữ
   cursor đầu vào (có thể null), remaining_chunks vẫn đếm scope chưa quét;
   client dùng remaining_chunks=0 để xác nhận kết thúc scan.
4. **Domain:** destination ưu tiên domain khác khi RF còn thiếu. Đủ live RF
   nhưng cùng domain thì báo `domain_degraded`, không tự thêm replica chỉ để
   đổi domain. Replica cũ quay lại gây dư RF được giữ nguyên.
5. **Cancellation:** ngừng bắt đầu RPC/chunk mới, drain owning thread/RPC rồi
   mới thả lock/đóng client. DELETE đã commit vẫn DELETING; repair đã ack vẫn
   VERIFIED, attempted chưa xác nhận vẫn PENDING. Không rollback filesystem giả.

## 3. Chia phase và thứ tự

| Phase | Nội dung | Phụ thuộc | Gate chuyển bước |
|---|---|---|---|
| P1 | Delete RPC wrapper và operation boundary | M3 | Ack/idempotency/retry/cancel đúng, không tx qua RPC |
| P2 | Cleanup engine và DELETE/tombstone | P1 | Xóa logic trước RPC; 200/202 đúng; lỗi giữ pending/mapping |
| P3 | Background cleanup, fairness và startup/shutdown | P2 | FAILED/DELETING tiếp tục dọn; health không bị chặn; drain an toàn |
| P4 | Repair một chunk và reconcile | P1–P3 | Source hash đúng, PENDING trước Store, chỉ ack mới đếm repaired |
| P5 | Repair REST, scope/cursor/budget/summary | P4 | Scan không bỏ/lặp sai chunk, validation và concurrency đúng |
| P6 | Fault lifecycle và smoke Compose thật | P3/P5 | Offline → restart → recovery; repair RF; bytes/mapping không hồi sinh |
| P7 | Rà DoD/evidence và bàn giao M5 | P6 | Docs đúng hành vi, fixture được xử lý, giới hạn được ghi rõ |

Thực hiện tuần tự. P1–P3 tạo một mốc có thể review/deploy riêng trước repair;
P4–P5 tạo mốc manual repair; P6 chứng minh milestone. Ước lượng tổng ban đầu
3 buổi tập trung (M4 trong kế hoạch tổng), nhưng fault/lifecycle có thể cần
buffer; phase là gate kỹ thuật, không đồng nghĩa mỗi phase một buổi hay một PR.

## 4. P1 — Delete RPC wrapper

- Thêm `StorageClient.delete_chunk` và `DataOperation.delete_chunk`; cùng
  `_invoke`, cancellation, close/drain và configured-node guard của Store/Get.
- Ack phải đúng chunk_id. `existed=false` vẫn thành công; ack sai không xác
  nhận cleanup. NOT_FOUND tương thích được xem đã xóa; INTERNAL/I/O/timeout
  không được biến thành success. Chỉ retry UNAVAILABLE/DEADLINE_EXCEEDED.
- Không đổi Storage Delete M1 nếu không có finding cụ thể.

**Checks:** mở rộng tests storage client/operations đang có; fake RPC cho
deadline/retry/ack sai, production gRPC cho idempotent Delete và ack mất sau
xóa thật. Kiểm tra close/cancel không để RPC còn sống sau scope.

**Gate:** Delete có outcome xác minh được, số attempts hữu hạn, transaction
đã đóng trước RPC. Chưa coi REST/worker cleanup hoàn thành.

## 5. P2 — Cleanup engine và DELETE

1. Lấy nonblocking data lock, check readiness, đọc file trong transaction ngắn.
   ID chưa có → 404; UPLOADING → 409; DELETING/DELETED lặp theo contract.
2. AVAILABLE/FAILED → DELETING, ghi deleted_at và cleanup_pending trên mọi
   attempted replica chưa DELETED; commit trước Delete đầu tiên. Replica đã
   DELETED/không pending không cần gọi lại. Giữ chunk/node/replica rows.
3. Cleanup lấy snapshot rồi RPC ngoài transaction. Chỉ ack đúng/NOT_FOUND mới
   ghi replica DELETED, clear pending/error. Timeout, ack sai, I/O hoặc node
   không reachable giữ pending và lỗi thích hợp.
4. Finalize DELETING → DELETED khi pending=0 và không còn replica chưa xác nhận
   xóa; file rỗng hoặc FAILED chưa tạo chunk có thể hoàn tất ngay. FAILED được
   worker dọn vẫn FAILED, giữ nguyên nguyên nhân lỗi.
5. Trả DeleteResult `file_id/status/cleanup_pending_replicas`: DELETED/0 → 200,
   còn pending → 202. DB lỗi → 503 METADATA_UNAVAILABLE; không trả 200 giả sau
   Delete trên disk khi chưa persist được kết quả.

**Checks:** HTTP → PostgreSQL → Storage thật; quan sát tombstone/flags đã commit
từ Session khác ngay tại Delete RPC. Kiểm tra file rỗng, AVAILABLE, FAILED,
UPLOADING, repeat Delete, mọi attempted PENDING, offline/disabled, ack loss,
DB fail trước/sau RPC, cạnh tranh upload/download và cancellation sau commit.
List mặc định ẩn DELETING; inactive detail/chunks còn xem được; download bị chặn;
DELETED read trả 404. Kiểm tra bytes của đúng fixture đã mất, rows vẫn còn.

**Gate:** delete logic bền vững, pending không mất, response phản ánh cleanup thật.

## 6. P3 — Worker cleanup và recovery

- Nối cleanup vào scheduler hiện có theo interval 5 s, dùng executor/scope có
  ownership rõ; không thực hiện RPC dài trong thread điều phối health.
- Mỗi lần chỉ một cleanup task, nonblocking lock: bận thì bỏ lượt; không tích
  backlog theo từng tick. Chọn cả FAILED và DELETING, kể cả không còn pending
  nhưng cần finalize DELETING. Không dọn PENDING của AVAILABLE.
- Thực hiện fairness P2, không hot-loop khi node down/DB lỗi; log lỗi giới hạn
  tần suất, tiếp tục ở interval sau. Health/read snapshots vẫn hoạt động.
- Startup recovery dưới lock vẫn không gọi RPC; worker dọn sau khi client và
  app sẵn sàng. Re-enable node có mapping pending sẽ được dọn sau health recovery.
- Shutdown đóng admission/scheduling cleanup mới, drain cleanup/data operations
  và final DB work trước close data channels/dispose engine. Không để scheduler
  chờ lock trong lúc shutdown giữ lock và join scheduler. Scheduler/task chết
  phải được phản ánh nhất quán trong readiness; cập nhật contract nếu thêm
  điều kiện readiness cleanup.

**Checks:** busy skip, không duplicate task, trên 8 replica, nhóm lỗi đầu không
starve nhóm sau, DOWN/disabled/re-enable, retry sau ack loss/DB lỗi, FAILED giữ
error_code, startup DELETING chưa xong/đã xong, AVAILABLE PENDING không bị xóa;
health vẫn poll khi cleanup chậm, lifespan drain/shutdown không deadlock/leak.

**Gate:** dọn bytes tự tiếp tục từ trạng thái DB; kiểm tra process restart thật ở P6.

## 7. P4 — Repair một chunk

1. Chỉ file AVAILABLE. Probe mọi known replica eligible ACTIVE/SUSPECTED và
   enabled/configured, bỏ DELETED/cleanup_pending. Không chỉ chọn cached
   under-replicated; PENDING/MISSING/CORRUPTED cũng cần reconcile.
2. Get/hash/size/id đúng → VERIFIED/last_verified_at; NOT_FOUND → MISSING;
   bytes/hash sai/DATA_LOSS → CORRUPTED. Timeout/unreachable giữ status và ghi
   last_error, không suy ra MISSING. Không gọi DOWN/disabled, giữ mapping.
3. Không có source đúng → UNAVAILABLE và không Store/Delete. Source SUSPECTED
   có thể cung cấp bytes đúng, nhưng live RF chỉ tính enabled ACTIVE.
4. Nếu live RF còn thiếu, dùng placement + RF file, giữ một source đã verify
   trong RAM (một chunk), chọn destination ACTIVE đủ free snapshot. Upsert
   PENDING commit trước Store. Ack hợp lệ → VERIFIED; chỉ lúc đó tăng
   repaired_replicas. Thử node khác hữu hạn nếu destination lỗi.
5. MISSING có thể Store lại cùng node. Với destination CORRUPTED: chỉ xóa bytes
   lỗi khi đã có source khác đúng; trước destructive RPC phải giữ mapping
   attempted có thể recovery. Delete xác nhận rồi Store cùng chunk_id, không
   tự overwrite hoặc đặt cleanup_pending của file AVAILABLE. Nếu crash ở giữa,
   lượt sau Get/reconcile được; status không che mất attempt.
6. Đủ RF → không thêm replica; thiếu destination → NO_DESTINATION; lỗi ngoài
   dự kiến từng chunk → ERROR. Trường hợp có Store ack nhưng vẫn thiếu RF phải
   phản ánh số ack và outcome thiếu destination/lỗi, không báo đã đủ RF.

**Checks:** source corrupt/missing/timeout, tất cả source mất, node quay lại
volume trống, phục hồi MISSING/corrupt tại node cũ, Store conflict/full/ack loss,
Delete lỗi trước rewrite, DB fail trước PENDING và sau ack, RF snapshot khác
default, domain preference, SUSPECTED source không tính live, dư RF không prune.
Hash downloaded file sau repair phải bằng nguồn; fake chỉ dùng cho fault window,
success path dùng Storage production server.

**Gate:** tạo replica thật từ source đã verify, giữ mọi attempted mappings và
không xóa bản sao hợp lệ duy nhất.

## 8. P5 — REST repair, cursor và budget

- Thêm RepairRequest/Result và admin router. Optional file_id/node_id/after;
  max_chunks 1..8 và không vượt settings limit. UUID/index/body validation theo
  envelope. Unknown file → 404, unknown node → 422; explicit file ngoài
  AVAILABLE → 409 theo contract repair. Endpoint đọc DELETED vẫn trả 404;
  không dùng visibility của GET để tự đổi semantics của POST repair.
- Filter node chỉ chọn chunk có known mapping tới node đó, không giới hạn
  source/destination chỉ ở node đó. Kết hợp được file+node; disabled node còn
  row vẫn có scope nhưng không được gọi RPC.
- Scan `(file_id UUID ASC, chunk_index ASC)`, after exclusive; cursor không
  cần trỏ tới row còn tồn tại. File deleted giữa hai lượt ra khỏi scope.
  Mỗi chunk đã thử kể cả ERROR vẫn tiến cursor; muốn retry chạy vòng mới.
- Dùng monotonic budget 30 s, dừng bắt đầu chunk mới khi hết budget; chunk đang
  chạy được kết thúc với RPC hữu hạn. Không tạo job ID/queue/background repair.
  Khi chưa xử lý chunk nào nhưng còn scope, giữ cursor đầu vào và remaining
  thật; next_after có thể null khi input after=null. Contract đã làm rõ client
  phải dùng remaining_chunks=0 để xác nhận scan kết thúc.
- Results đúng checked_chunks phần tử; repaired_replicas chỉ ack Store,
  remaining_chunks đếm scope chưa scan sau cursor, next_after null khi hết.
  Summary dùng counters chung, không đếm VERIFIED trên node DOWN thành live.
- Cleanup ưu tiên như mục 2; DELETE/repair/upload cạnh tranh trả OPERATION_BUSY.
  Request không bắt đầu được trả 409/503; lượt có UNAVAILABLE/NO_DESTINATION/
  ERROR từng chunk vẫn 200 summary. Không che DB outage toàn lượt bằng success.

**Checks:** default/filter/combined/invalid input, empty/rỗng, UUID ordering,
max_chunks, 2–3 pages, cursor row biến mất, ERROR vẫn tiến, budget bằng clock
điều khiển, boundary hết scope, partial repair/ack counts, cleanup priority,
request cạnh tranh/cancel và GET snapshots không cần data lock.

**Gate:** client scan hết scope không bỏ sót, summary không hứa durability từ
health/cache. HTTP client smoke dành khoảng 180 s cho repair theo contract;
đo thời gian xấu nhất của một chunk nếu cho thấy cần chỉnh contract.

## 9. P6 — Fault lifecycle và smoke thật

**Isolated Docker Linux/PostgreSQL schemas/temp DATA_DIR:**

1. SIGKILL sau DELETING commit trước Delete, và sau Delete thật trước DB ack:
   process mới tiếp tục pending, idempotent cleanup hoàn tất, giữ tombstone.
2. Upload interrupted/FAILED có bytes thật → restart tự cleanup; FAILED/error
   còn nguyên. DELETING offline qua nhiều restart vẫn không download/repair.
3. Kill repair sau PENDING commit/Store thật trước VERIFIED; restart không dọn
   AVAILABLE attempt, repair probe xác nhận bytes và không đếm thành Store mới.
4. Kill giữa Delete corrupted replica và Store; vẫn còn source đúng, restart
   repair được. Node volume trống/corruption chỉ tạo trong fixtures riêng.
5. Cạnh tranh delete/repair và shutdown khi cleanup/repair RPC đang chạy: không
   hồi sinh, leak thread/session/lock hay đóng DB trước final commit.

**Smoke Compose hiện có, dùng build hiện hành:**

- Upload nhiều chunk RF=2, lưu ID/hash/manifest của fixture do smoke sở hữu.
- Stop một node có mapping, chờ DOWN; download vẫn SHA đúng. Repair sang node
  thứ ba, kiểm tra Store ack, mapping/chunk bytes và RF=2. Local `dev_host`
  chỉ chứng minh process failure/RF, không chứng minh hai failure domains.
- Bật node cũ, scoped repair probe; nếu dư RF giữ nguyên và báo counter đúng.
- Stop một node có replica của file sẽ xóa; DELETE → 202; restart Metadata,
  detail vẫn DELETING/pending, download bị chặn. Bật node, worker hoàn tất;
  repeat DELETE → 200, read → 404, bytes của fixture mất trên các mappings.
- Chạy cleanup cho fixture M3 bàn giao qua endpoint; giữ manifest/evidence
  trước và sau. Restore node trong finally, không reset volumes/unrelated data.
- Check services healthy, pending của fixtures bằng 0, API GET/health và
  upload/download cơ bản sau worker wiring; không cần rerun toàn bộ M1–M3.

Fixture M3 bàn giao (đã cleanup qua API ở P6, xem evidence bên dưới):
`f26d076f-0ec6-423d-a161-6e18d5799d2a`, 6 chunks/12 mappings,
manifest `.runtime/m3-smoke-43cc9c856bfd40f8bcc537209a0b143c.json`. Xác minh
manifest/ID hiện còn trước khi dùng; nếu không còn thì ghi trạng thái thực và
dùng fixture mới. Review ingress M3 đã có focused evidence nhưng chưa deploy
fix ở lần bàn giao; build/deploy P6 phải bao gồm source hiện hành đó.

**Gate:** có evidence actual bytes, persisted mappings, process restart và
live RF repair/delete recovery. In-process checks không thay evidence này.

## 10. Commands và chọn test gate khi triển khai

Các lệnh dưới đây là gate theo phase; kết quả thực tế ở phần evidence.
Trước mỗi lượt ghi tier, exact command/phạm vi và lý do; sau lượt ghi environment,
passed/failed/skipped. Không cộng regression/rerun thành full-suite count.

**Targeted native** (từ `backend/`): pure selection/DTO/budget helpers dùng
existing pytest; chọn names thật khi có. Lint/format chỉ paths đã sửa:

```powershell
.\.venv\Scripts\python.exe -m ruff check <affected-paths>
.\.venv\Scripts\python.exe -m ruff format --check <affected-paths>
```

**Focused integration preflight** (repo root):

```powershell
docker info
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
```

DB checks cần PostgreSQL healthy/TEST_DATABASE_URL do tests service cấp;
in-process Storage checks không cần live Storage containers. P6 smoke cần
Metadata/PostgreSQL/Storage services healthy với current build. Native gRPC
loopback đã có host timeout; dùng Docker Linux cho boundary gRPC.

Rebuild tests image khi code được include thay đổi, sau preflight:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
```

P1 mở rộng files hiện có (narrow `-k` theo names mới khi triển khai):

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_storage_client.py tests/test_metadata_operations.py -k delete
```

P2–P6 dự kiến theo từng affected file, không chạy chung mặc định:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_delete.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_cleanup.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_repair.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_repair_api.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_failure_lifecycle.py
```

Chọn thêm regression names trực tiếp liên quan trong worker/bootstrap/operations/
cluster/files/download/lifecycle khi sửa wiring; không rerun passing phase khi
code/config/environment liên quan chưa đổi. P6 viết smoke helper theo pattern
`smoke_files.py/.ps1`, ghi exact invocation và manifest trong evidence sau đó.
Không dùng smoke M3 nguyên trạng để tuyên bố DELETE/repair đã qua.

Nếu thiếu prerequisite, dừng dependent check, báo lỗi đầu tiên và recovery:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait
```

Nêu rõ thiếu env/current build nếu có. Test-only không tự start/restart/migrate;
chờ readiness được xác nhận, rerun intended command một lần. Khi deployment/
environment repair đã được cho phép, làm trong scope đó; không xóa volume/data.
Full backend validation chỉ khi user yêu cầu, merge/release readiness hoặc
thay đổi thực sự cross-cutting theo root AGENTS; frontend không nằm trong M4.

## 11. Definition of Done và evidence

- [x] P1: Delete RPC verified/idempotent, bounded retry/drain (04/10/2026).
- [x] P2: DELETE tombstone trước RPC, đúng 200/202, rows/pending giữ qua lỗi (04/10/2026).
- [x] P3: cleanup FAILED/DELETING bounded/fair, health độc lập, lifespan an toàn (04/10/2026).
- [x] P4: repair source verified/RF file/PENDING trước Store, reconcile corrupt/missing (04/10/2026).
- [x] P5: REST filters/cursor/budget/counts/validation và operation serialization đúng (04/10/2026).
- [x] P6: crash windows + Compose offline/restart/recovery + repair RF + delete bytes thật qua (04/10/2026).
- [x] P7: contracts/docs phản ánh code, fixtures/evidence rõ, bàn giao M5 (04/10/2026).

Mỗi phase append evidence ngày chạy, paths đã đổi, exact commands/counts,
environment, failures đã sửa, warnings/skips và việc chưa chạy. Không đánh dấu
checkbox từ kế hoạch hoặc lint/build. Giữ ghi chú M3 fixture đến khi cleanup
được xác nhận thật; worker không thể dọn node bị tháo vĩnh viễn thì vẫn pending.

## 12. Bàn giao M5 — UI tối thiểu

Kế hoạch triển khai tiếp theo: [M5 — UI tối thiểu, P1–P7](M5_IMPLEMENTATION_PLAN.md)
(lập 04/10/2026, P1 đã hoàn thành; evidence hiện hành nằm trong plan M5). Các ghi nhận P7 bên
dưới vẫn là lịch sử tại thời điểm bàn giao M4.

### DoD M4 đã đối chiếu

| Điều kiện | Implementation | Evidence đã có |
|---|---|---|
| Delete xác minh ack, idempotent, finite retry/cancel, RPC ngoài transaction | [storage_client.py](../backend/metadata/storage_client.py), [operations.py](../backend/metadata/operations.py), [Storage guard](../backend/storage/service.py) | P1 client/RPC checks; P4 cancel/deadline guard; P6 worker active Delete shutdown |
| Tombstone commit trước Delete; 200/202 và history giữ qua lỗi | [delete.py](../backend/metadata/delete.py), [files route](../backend/metadata/routes/files.py) | P2 HTTP/DB/RPC/ack-loss; P6 SIGKILL trước Delete và sau unlink/trước DB ack |
| FAILED/DELETING cleanup bounded/fair, health độc lập, startup/drain an toàn | [cleanup.py](../backend/metadata/cleanup.py), [worker.py](../backend/metadata/worker.py), [bootstrap.py](../backend/metadata/bootstrap.py), [main.py](../backend/metadata/main.py) | P3 fairness/busy/DB failure/lifespan; P6 interrupted upload, hai restarts offline, SIGTERM |
| Repair chỉ từ bytes size/hash đúng, RF file, PENDING trước Delete/Store | [repair.py](../backend/metadata/repair.py), [placement.py](../backend/metadata/placement.py) | P4 missing/corrupt/partial ack; P6 PENDING/Store/corrupt Delete crash windows |
| Filters/cursor/budget/counts và errors đúng REST schema | [schemas.py](../backend/metadata/schemas.py), [admin route](../backend/metadata/routes/admin.py) | P5 HTTP validation/zero-progress/error cursor/DB outage/disconnect; P6 live cursor pages |
| Node quay lại reconcile known mappings, không prune dư RF | [repair.py](../backend/metadata/repair.py), [cached counters](../backend/metadata/replica_health.py) | P4 source/domain/RF checks; P6 returned node checked=4/repaired=0, over_replicated=4 |
| Runtime local có evidence actual bytes và pending recovery | [smoke_failure.py](../backend/scripts/smoke_failure.py), [orchestrator](../backend/scripts/smoke_failure.ps1) | P6 deploy Metadata/3 Storage, RF=2 repair, offline DELETE/Metadata restart; 40 mappings NOT_FOUND |
| Không mở rộng scope/schema/wire; fixtures có ownership | [API_CONTRACTS](API_CONTRACTS.md), [ARCHITECTURE](ARCHITECTURE.md), [storage.proto](../backend/contracts/storage.proto) | 4 tables/4 unary RPC giữ nguyên; hai bản proto cùng hash; P6 API-only cleanup/volumes giữ nguyên |

Các gate hoàn thành theo evidence của từng phase; không phải một full-suite
run và không cộng 86 P1–P3, 89 P4–P5, 16 P6 thành tổng tests milestone vì có
regressions trùng. P7 chỉ đọc source/contracts và host manifests P6, không
kiểm tra lại trạng thái cluster live ở thời điểm P7.

### Contract cần giữ khi nối UI

REST base `/api/v1`; dùng [API_CONTRACTS](API_CONTRACTS.md) cho DTO/error codes.
Frontend hiện vẫn là starter; M5 mới triển khai các màn hình sau:

| Luồng | Hành vi UI cần thể hiện |
|---|---|
| Upload/list/download | Upload multipart đúng field `file`; 100% gửi HTTP body vẫn là đang chờ commit, chỉ 201 mới thành công. Refresh list/cluster sau mutation. Download binary blob và Content-Disposition; hiển thị đang chuẩn bị, xử lý JSON lỗi riêng với body binary. |
| File detail/placement | Detail gồm FileSummary + health fields, list chỉ có FileSummary. Hiển thị chunk/node/replica/cleanup/error và timestamps. `known_readable`, counters và ACTIVE là observations/cache, không là integrity guarantee. Node capacity/free có thể cùng filesystem, không cộng thành cluster capacity. |
| Delete/cleanup | 202: file đã xóa logic, đang dọn bản sao; không báo xóa vật lý xong. Theo dõi detail/inactive list trong DELETING; read 404 khi DELETED thì dừng polling và đóng/refresh detail. 200/DELETED/pending=0 xác nhận hoàn tất. FAILED vẫn giữ error_code dù worker đã dọn bytes. |
| Cluster/nodes | Poll GET snapshots; ready vẫn có thể 200 khi node DOWN. Hiển thị ACTIVE/SUSPECTED/DOWN/disabled và pending/degraded. `operation_busy` chỉ là snapshot; 409 OPERATION_BUSY vẫn phải xử lý ở mutation. |
| Manual repair | Gửi JSON với file/node filter, max_chunks integer 1..8, cursor từ response. Hiển thị checked, Store ack count và outcomes từng chunk; 200 có ERROR/UNAVAILABLE/NO_DESTINATION vẫn chưa đạt mục tiêu. Cho phép tiếp tục vòng scan, không giả định server có background repair/job ID. |

List mặc định chỉ AVAILABLE; `include_inactive=true` thêm UPLOADING/FAILED/
DELETING, vẫn không trả DELETED. Đọc DELETED trả 404; explicit repair file đó
trả 409 FILE_DELETING. FAILED/UPLOADING repair trả FILE_NOT_READY. File không
tồn tại repair 404; node filter không tồn tại/invalid body 422.

Giữ nguyên filters trong một vòng repair; thay filters thì reset `after=null`.
Kết thúc scan khi `remaining_chunks=0`. Nếu checked=0/remaining>0, giữ cursor
trả về, thông báo chưa có tiến triển và cho người dùng tiếp tục; không hot-loop.
ERROR đã tiến cursor, muốn kiểm tra lại chunk đó phải bắt đầu vòng scan mới.
`remaining_chunks` là unscanned count, không phải số chunks thiếu RF. Có Store
ack rồi vẫn có thể NO_DESTINATION/ERROR; probe replica cũ đúng không tính Store.

Không tự retry upload/POST repair khi timeout/disconnect: outcome có thể đã
commit. Refresh list/detail/placement trước khi người dùng chọn làm tiếp;
upload error có `details.file_id` thì giữ ID để tra FAILED/cleanup. DELETE là
idempotent nhưng polling chỉ dùng GET, không lặp DELETE như cơ chế dọn bytes.
Client timeout repair khoảng 180 s theo contract là khuyến nghị, không SLA;
budget 30 s chỉ ngừng bắt đầu chunk mới, RPC/DB đang chạy vẫn cần drain.

### Điểm tích hợp với frontend starter

Giữ React/Vite, TanStack Query cho server state, Zustand cho client UI state;
theo [frontend/AGENTS](../frontend/AGENTS.md), [BASE_B](../frontend/docs/BASE_B.md)
và [STRUCTURE](../frontend/docs/STRUCTURE.md). API HTTP đặt trong `src/api/`,
feature controllers/components/hooks colocate theo convention; không thêm
framework test hoặc shared abstraction chỉ có một consumer.

- `src/api/rootApi.js` hiện `withCredentials=true` và tự gửi Authorization từ
  localStorage. Metadata V1 không auth/cookies, CORS hiện chỉ allow Content-Type
  và không enable credentials. M5 cần client storage không gửi credentials/
  Authorization (client riêng hoặc opt-out rõ), base URL có `/api/v1`; không
  đổi auth starter thành yêu cầu backend auth.
- Interceptor hiện đọc `data.detail/data.message`; backend trả
  `{error:{code,message,details}}`. M5 cần adapter đọc envelope này, giữ code/
  details, có fallback network/non-JSON; không chỉ hiển thị Axios message.
- Các routes storage cần truy cập không qua login guard/getMe của starter;
  không dùng auth init làm readiness API. Root public route table hiện rỗng,
  M5 phải nối route/layout phù hợp; P7 chưa sửa frontend.
- Query keys chứa file ID/list paging/filters; invalidate list/detail/chunks/
  cluster sau upload/delete/repair kể cả outcome không rõ, GET refresh để
  reconcile. Tách progress/local action state khỏi cache server; mutation
  đặt retry=false và không tự enqueue lượt repair tiếp theo.

M5 bắt đầu bằng một luồng upload → list → detail/placement → download thật,
rồi nối Delete 202/cleanup, dashboard nodes/cluster và manual repair. Gate M5
cần UI/API runtime evidence cho các flows này, gồm pending/offline/busy và
repair không tiến; lint/build chỉ chứng minh compilation/static checks.
Package frontend hiện không có dedicated test/E2E script; chọn checks theo
AGENTS, không giả định đã có UI tests. M6 mới triển khai/demo hai host.

### Chạy local và smoke sau bàn giao

Hướng dẫn setup/env chung ở [README repo](../README.md). Khi cần cập nhật build,
chạy từ repo root và giữ named volumes:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml build metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --no-deps --wait metadata storage-node-1 storage-node-2 storage-node-3
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec -T metadata python -O scripts/smoke_metadata.py baseline
```

Baseline trên chỉ đọc health/nodes/cluster. Nếu cả cluster chưa được setup thì
theo lệnh `up -d --build --wait` trong README, không dùng `--no-deps` để giả định
PostgreSQL đã sẵn sàng. Đọc preflight/check gate trong AGENTS trước integration.
Giữ một Metadata worker/process; không tăng Uvicorn workers để mở concurrency.

Khi chủ động cần lặp live fault smoke, script tự tạo fixtures mới và dọn qua API:

```powershell
& .\backend\scripts\smoke_failure.ps1
```

Script stop/start node-2 và restart Metadata, vì vậy chỉ chạy khi có scope
lifecycle/deployment hoặc được yêu cầu. Không truyền lại M3 legacy manifest
P6 vào lượt mới: file M3 đã DELETED, precheck AVAILABLE sẽ fail đúng. Checkpoints
host nằm trong `.runtime/m4-smoke-<uuid>.json`; nếu fail, giữ manifest để xử lý
đúng IDs, không raw-delete DB/chunks hoặc reset volumes. P6 manifest và deployment
evidence được giữ; một file AVAILABLE ngoài ownership được để nguyên.

Nếu `create` bị ngắt, chạy riêng chế độ `finish` với manifest đã lưu, sau khi
Metadata/PostgreSQL và các Storage cần dọn đã sẵn sàng. Chế độ này không cần đủ
cả fixture `repair` và `delete`: nó xử lý các ID đã checkpoint, kể cả
`upload_failure.error.error.details.file_id` nếu API báo file đã được tạo.
Identity (ID và tên fixture) được kiểm tra từ durable metadata trước DELETE;
mapping còn thiếu được đọc và checkpoint trước mutation, kể cả file đã DELETED.
FAILED có thể chưa tạo chunk hoặc attempted mapping; không suy diễn có bytes
trên Storage nếu chưa từng ghi nhận mapping. Chỉ xác nhận mappings đã ghi nhận
biến mất khi GetChunk trả NOT_FOUND; lỗi transport vẫn làm smoke fail.

Native frontend dev từ `frontend/`: `npm ci`, `npm run dev`; cấu hình client
storage trỏ Metadata local khi làm M5. P7 không đổi env/frontend hoặc chạy dev
server. Hai host/domain khác nhau, DB restart mới, worst-case timeout benchmark,
full backend suite và UI runtime chưa là evidence của M4 P7.

**Evidence lập kế hoạch — 04/10/2026:** đọc contracts, bàn giao M3 và source
Metadata hiện có; thêm kế hoạch P1–P7 và links. Tier no execution vì docs-only;
không chạy pytest/Ruff/build/smoke, không preflight/start/deploy hay dọn fixture.

### Evidence P1–P3 — 04/10/2026

**Source:** thêm `DeleteAck`/StorageClient/DataOperation Delete wrapper;
`cleanup.py` dùng pending rows và cursor `(chunk_id,node_id)` luân phiên, tối
đa 8 replica/lượt; `delete.py` commit tombstone/flags trước RPC, finalize không
xóa rows; DELETE route drain owning thread khi disconnect/cancel. Worker có
cleanup executor riêng, không overlap/backlog và skip busy; hint DB bỏ qua tick
rỗng trước khi lấy lock. Startup dùng bootstrap hiện có, shutdown dừng/drain
worker trước close data client rồi drain request operations/dispose DB.

**Tier:** focused integration cho HTTP/PostgreSQL/gRPC và worker/lifespan;
Ruff targeted cho 12 Python paths. Preflight `docker info --format
'{{.ServerVersion}}'` qua (Docker 29.3.1), Compose `ps` báo PostgreSQL và 4
services còn lại healthy. Tests image rebuild từ source hiện hành, live services
không recreate. Môi trường Docker Linux, Python 3.12 (`python:3.12-slim`),
PostgreSQL 16 với schema riêng từng test, production Storage server/temp dirs.
Lệnh phụ đọc patch version Python bằng `docker run --rm --entrypoint python
dfs-python-tests:dev --version` sau các checks bị từ chối quyền Docker API;
không coi lệnh đó passed và không suy ra patch version từ evidence M3.

Các lệnh từ repo root:

```powershell
docker info --format '{{.ServerVersion}}'
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_storage_client.py tests/test_metadata_operations.py -k delete
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_delete.py tests/test_metadata_cleanup.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_cleanup.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_worker.py tests/test_metadata_operations.py tests/test_bootstrap.py -k 'not delete'
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_delete.py -k asgi
```

- P1: **15 passed, 60 deselected**. Production Delete/idempotency/node isolation;
  same-request lost-ack retry; transient attempt limits; NOT_FOUND compatibility;
  invalid ack; deadline/cancel/close; transaction/thread/lifetime guards.
- P2/P3 lượt đầu: **20 passed, 6 failed**. 17 P2 qua; 3 P3 qua. Sáu lỗi là
  upload fixture bị OPERATION_BUSY vì cleanup lấy lock ngay cả khi không có
  việc ở interval 0.03 s. Sửa worker thêm hint DB chỉ đọc trước lấy lock,
  cập nhật fatal-task test tạo việc thật; rebuild tests image.
- P3 affected file sau sửa: **9 passed**, không skipped/deselected. FAILED giữ
  nguyên error, AVAILABLE PENDING không bị dọn; disabled/re-enable; busy skip;
  fairness qua 8 lỗi đầu; retry DB; health tiếp tục khi cleanup bị block;
  startup/restart lifespan reconstruct flags, zero-pending finalize;
  fatal task → not ready; shutdown chờ final DB work trước đóng client/engine.
- Regression trực tiếp worker/operation/bootstrap: **42 passed, 2 deselected**.
- P2 bổ sung ASGI boundary: **3 passed, 17 deselected**. Disconnect, cancellation
  và repeated raw cancellation giữ tombstone/pending, chờ owning thread rồi
  thả lock; cleanup tiếp theo hoàn tất. Chỉ thêm tests, rebuild image và chạy
  names mới, không rerun 17 P2 đã pass.

Tổng **86 ca khác nhau passed** qua các lượt focused checks, không cộng rerun
3 ca P3 vào count; đây không là full-suite run. Mỗi lượt có một warning
Starlette/TestClient deprecation; không skipped PostgreSQL tests. Lượt lint đầu
phát hiện import/line length, sửa import + format rồi checks qua.

Ruff từ `backend/` (check/format đã qua, chỉ rerun paths sửa tiếp ở lượt sau):

```powershell
.\.venv\Scripts\python.exe -m ruff check metadata/storage_client.py metadata/operations.py metadata/schemas.py metadata/cleanup.py metadata/delete.py metadata/routes/files.py metadata/worker.py metadata/main.py tests/test_metadata_storage_client.py tests/test_metadata_operations.py tests/test_metadata_delete.py tests/test_metadata_cleanup.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/storage_client.py metadata/operations.py metadata/schemas.py metadata/cleanup.py metadata/delete.py metadata/routes/files.py metadata/worker.py metadata/main.py tests/test_metadata_storage_client.py tests/test_metadata_operations.py tests/test_metadata_delete.py tests/test_metadata_cleanup.py
```

**Chưa chạy:** full suite/frontend, process SIGKILL/Storage container restart,
live Compose smoke DELETE/cleanup, repair hoặc hai host. Restart evidence P3
là TestClient lifespans dùng cùng DB/Storage fixture, không là crash process.
Chưa deploy/restart live services hoặc xử lý manifest M3; fixture live vẫn để
P6 dùng endpoint cleanup. P4 nhận Delete wrapper/cleanup engine/operation lock;
cleanup-priority và repair semantics chưa triển khai.

### Evidence P4–P5 — 04/10/2026

**Source:** `metadata/repair.py` probe mọi known mapping eligible, giữ một source
qua Get/hash, dùng RF file và placement/domain để copy phần thiếu. PENDING commit
trước Store hoặc Delete corrupted destination; VERIFIED chỉ sau ack. Không có
source thì UNAVAILABLE và không destructive RPC; SUSPECTED source không tính live,
DOWN/disabled/endpoint lệch không được gọi. Mapping timeout/ack loss vẫn để
probe/retry lượt sau; đủ RF không auto prune/đổi domain. Chunk progress giữ
count ack đã xác nhận ngay cả khi chunk lỗi sau một Store thành công.

`routes/admin.py`, Repair DTOs và router wiring thêm POST `/api/v1/admin/repair`.
File/node filters kết hợp, cursor UUID/index exclusive, scope AVAILABLE, max
1..8/settings limit, budget monotonic gồm cleanup priority dưới cùng lock.
ERROR vẫn tiến cursor, remaining là unscanned count, cancellation drain owning
thread trước thả lock. DB outage trả 503, không biến thành 200 ERROR summary.
Contract làm rõ cursor khi checked=0/budget hết trước chunk đầu và live RF từ
replicas được xác nhận trong lượt; response không thêm fields/job ID.

**Finding sửa trong P4:** Storage Delete trước đây không check RPC activity sau
đợi mutex. Request timeout/cancel có thể unlink trễ trong lúc một lượt repair
mới đã ghi lại replica. `ChunkStore.delete` nhận optional activity callback,
check sau lock và trước unlink; service truyền context, trả CANCELLED nếu hết
hạn. Giữ idempotency/accounting; không đổi proto/schema. Hai test hooks Delete
được cập nhật theo callback mới.

**Tier:** focused integration vì HTTP/PostgreSQL/gRPC/filesystem, targeted Ruff.
Read-only preflight Docker 29.3.1 và PostgreSQL healthy qua; Storage production
server dùng temp dirs trong process, DB schema riêng. Docker Linux/Python 3.12
(`python:3.12-slim`)/PostgreSQL 16. Rebuild tests image mỗi khi included source
thay đổi; không rebuild/deploy/restart runtime services, không dọn live fixture.

Lệnh từ repo root:

```powershell
docker info --format '{{.ServerVersion}}'
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps postgres
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_repair.py tests/test_metadata_repair_api.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_storage_p3.py tests/test_storage_p4.py -k delete
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_repair.py tests/test_metadata_repair_api.py -k 'missing_corrupt or failed_attempt or down_node or partial_ack or default_limit'
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_cleanup.py tests/test_metadata_operations.py tests/test_metadata_storage_client.py -k 'cleanup or lifespan or delete or start_failure'
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_repair.py tests/test_metadata_repair_api.py::test_repair_unexpected_error_after_partial_ack_keeps_confirmed_count tests/test_metadata_repair_api.py::test_repair_error_result_advances_cursor_and_remaining_is_unscanned tests/test_metadata_repair_api.py::test_repair_partial_ack_without_full_rf_is_no_destination_with_exact_count
```

- P4–P5 lượt đầu: **44 passed**, không skips/deselections (16 core + 28 API).
  Healthy vẫn probe; PENDING/MISSING/corruption; source mất/corrupt/timeout;
  down → third-node copy; over-RF giữ history; SUSPECTED source; endpoint guard;
  RF snapshot/domain degraded; Delete/Store/ack-loss attempts; DB interruption.
  API validation/status/scope, zero-byte/empty, UUID ordering/pages/filters,
  cursor row deleted/missing, budget/zero-progress, ERROR cursor, cleanup
  priority, busy/readiness/config limit, DB outage và ASGI disconnect/cancel.
- Sau finding Storage Delete guard, affected checks: **15 passed, 17 deselected**
  (4 mới cancel/deadline lúc đợi mutex/trước unlink + 11 direct regressions).
- Repair paths bị guard ảnh hưởng + 2 API mới: **9 passed, 37 deselected**.
  Hai ca mới kiểm tra partial ack/NO_DESTINATION và default 8 với 9 chunks,
  UNAVAILABLE vẫn trả 200 summary. Bảy ca core là rerun, không cộng mới.
- Cleanup/client/operation/lifespan regressions: **27 passed, 57 deselected**.
- Sau bổ sung progress giữ ack khi lỗi bất ngờ: **19 passed**, không skips/
  deselections (16 core rerun + 2 API regression + 1 API mới partial ack/ERROR).

Tổng **89 ca khác nhau passed trong lượt P4–P5**, gồm **47 repair checks**
(16 core + 31 API), **4 Storage guard mới** và **38 direct regressions**.
Không cộng 25 rerun thành full-suite evidence; không cộng với 86 P1–P3 vì có
regression trùng nhau. Pytest không có assertion/environment failure ở lượt
này; có warning Starlette/TestClient deprecation. Lượt Ruff đầu phát hiện
import/line length, đã fix/format rồi checks qua.

Ruff targeted **10 paths** qua (6 paths đầu và 5 paths bổ sung có một path
trùng; chỉ rerun paths sửa tiếp). Các lệnh check/format từ `backend/`:

```powershell
.\.venv\Scripts\python.exe -m ruff check metadata/repair.py metadata/routes/admin.py metadata/main.py metadata/schemas.py tests/test_metadata_repair.py tests/test_metadata_repair_api.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/repair.py metadata/routes/admin.py metadata/main.py metadata/schemas.py tests/test_metadata_repair.py tests/test_metadata_repair_api.py
.\.venv\Scripts\python.exe -m ruff check storage/chunk_store.py storage/service.py tests/test_storage_p4.py tests/test_metadata_cleanup.py tests/test_metadata_repair_api.py
.\.venv\Scripts\python.exe -m ruff format --check storage/chunk_store.py storage/service.py tests/test_storage_p4.py tests/test_metadata_cleanup.py tests/test_metadata_repair_api.py
```

**Chưa chạy:** full suite/frontend, process SIGKILL/restart hoặc live Compose
repair/delete smoke, deployment/hai host. Success repair evidence là thật trên
isolated production Storage servers/DB, không là live cluster failover evidence.
HTTP 180 s là khuyến nghị, chưa đo worst-case live deadline/DB stalls như SLA.
Manifest M3/volumes live giữ nguyên. **P1–P5 hoàn thành**, bước tiếp theo P6:
crash windows sau PENDING/Store/Delete, cleanup offline qua process restart,
deploy current build Metadata/Storage (gồm Delete guard) rồi smoke có ownership
fixture/manifest; P7 sau khi P6 đạt gate.

### Evidence P6 — 04/10/2026

**Source:** thêm `tests/test_metadata_failure_lifecycle.py`,
`scripts/smoke_failure.py/.ps1` và 5 guards trong `tests/test_smoke_failure.py`.
Subprocess helper M3 nhận optional harness/env, default giữ nguyên. Hooks chỉ
ở tests: pipe barrier sau commit/RPC, parent kiểm tra DB/disk rồi SIGKILL;
không thêm fault injection vào runtime. P6 không sửa service logic/proto/schema.
Smoke chạy `python -O`, kiểm tra bằng `require`, lưu IDs trước checks/next upload,
checkpoint host mỗi phase, xác minh ownership trước DELETE và restore node
trong finally. Query DB cuối chỉ đọc để kiểm tra tombstone/history sau read 404.

**Tier/environment:** focused integration/E2E vì process/HTTP/PostgreSQL/gRPC,
filesystem và deployment; targeted native guards/Ruff/PowerShell parser.
Preflight sandbox ban đầu bị Docker pipe permission denied; preflight ngoài
sandbox qua, Docker Linux và PostgreSQL/Metadata/3 Storage healthy. Không có
dependency unavailable hoặc shared-infrastructure flake. Isolated tests dùng
schema PostgreSQL riêng/temp DATA_DIR, child Uvicorn thật và production Storage
servers. Runtime Compose Python **3.12.15**, PostgreSQL 16; 3 Storage domains
đều `dev_host`, nên evidence là process failure/RF, chưa là host HA.

Lệnh từ repo root (chờ build hoàn tất trước dependent checks):

```powershell
docker info --format '{{.OSType}}'
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps --format json
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_failure_lifecycle.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_failure_lifecycle.py -k sigterm
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_failure_lifecycle.py::test_sigterm_waits_for_active_repair_store_rpc tests/test_metadata_files_lifecycle.py::test_multichunk_domains_real_disk_faults_and_normal_restart
docker compose --env-file deploy/.env -f deploy/compose.local.yml build metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --no-deps --force-recreate --wait metadata storage-node-1 storage-node-2 storage-node-3
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec -T metadata python --version
& .\backend\scripts\smoke_failure.ps1 -LegacyManifest .runtime/m3-smoke-43cc9c856bfd40f8bcc537209a0b143c.json
```

Native commands từ `backend/`:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_smoke_failure.py
.\.venv\Scripts\python.exe -m ruff check scripts/smoke_failure.py tests/test_smoke_failure.py tests/test_metadata_failure_lifecycle.py tests/test_metadata_files_lifecycle.py
.\.venv\Scripts\python.exe -m ruff format --check scripts/smoke_failure.py tests/test_smoke_failure.py tests/test_metadata_failure_lifecycle.py tests/test_metadata_files_lifecycle.py
```

PowerShell parser dùng `System.Management.Automation.Language.Parser.ParseFile`
cho `backend/scripts/smoke_failure.ps1`; không có parse errors.

**Test results:** tổng **16 ca khác nhau passed**: 10 M4 process lifecycle,
5 native guards và 1 M3 lifecycle regression trực tiếp của shared helper.
Không cộng reruns hoặc counts P1–P5 thành full-suite evidence.

- Lượt lifecycle đầu: **7 passed, 2 failed**. Sáu SIGKILL windows xác minh:
  DELETING trước Delete; unlink trước DB ack; repair PENDING trước Store;
  bytes Store trước VERIFIED; corrupt Delete trước Store; UPLOADING bytes
  trước ack. Process mới giữ mappings và tombstone; FAILED/error giữ nguyên
  sau worker cleanup. AVAILABLE PENDING không bị cleanup, Store đã ghi được
  probe lại với repaired=0; source đúng còn qua corrupt replacement crash.
- Offline DELETING qua **hai Metadata restarts** vẫn detail pending,
  download/repair 409; bật production Storage server trên cùng directory/port
  rồi worker hoàn tất, DELETE 200, bytes mất. Ca này nằm trong 7 passed.
- Hai failures là assertion test harness mong SIGTERM exit 0, trong khi
  Uvicorn 0.54.0 re-raise signal sau shutdown nên exit **-15**. Thêm dấu
  `SHUTDOWN_DRAINED` sau lifespan để chứng minh drain; sửa assertion và rerun
  đúng hai ca: **2 passed, 7 deselected**. HTTP DELETE/repair cạnh tranh nhận
  OPERATION_BUSY, health vẫn ready, admitted repair commit xong trước exit;
  worker stop làm active Delete context inactive trước unlink, pending bytes
  được cleanup sau process restart. Không sửa runtime vì failures này.
- Bổ sung actual active Store RPC shutdown và M3 multichunk/disk/restart
  regression: **2 passed**. Parent giữ handler `put` đang chạy, SIGTERM chờ
  request, release cho Store ack/VERIFIED commit rồi lifespan drain; bytes
  đúng, không Store temp. Lần gọi đầu trước build image mới hoàn tất có
  **no tests ran / test not found** do stale image; đã dừng và chờ build exit 0,
  chạy lại đúng hai names một lần. Không tính collection failure là test pass.
- Native **5 passed**, một PytestCacheWarning do host `.pytest_cache` không
  ghi được; không phải assertion failure. Guards chặn unowned file trước HTTP
  mutation, không xem UNAVAILABLE/DEADLINE_EXCEEDED là bytes đã mất, và không
  xem cursor null/remaining>0/checked=0 là scan complete.
- Ruff check/format **4 Python paths** và PowerShell syntax qua;
  `git diff --check` qua. Lượt đầu có import/line length đã format/fix.

**Deployment:** Metadata và 3 Storage dùng chung runtime image
`sha256:35f6f3639213a7184645593ffcb8c2a25e43a7d97d7608629a3b4c1917dad7b2`.
Build bao gồm M3 multipart ingress fixes và M4 Delete activity guard/worker/
manual repair. Chỉ recreate 4 Python services, PostgreSQL container giữ nguyên;
so sánh named mounts trước/sau deploy và sau smoke đều không đổi:
`distributed-file-storage_postgres-data`, `distributed-file-storage_node-1-data`,
`distributed-file-storage_node-2-data`, `distributed-file-storage_node-3-data`.
Host deployment evidence: `.runtime/m4-p6-deployment.json`.

**Live smoke:** hai fixture mới, mỗi file **10,485,777 bytes**, 6 chunks/RF=2;
full SHA `c4a9068afee819811d78887024edcabee99f03f85e40e8319078206d2d547321`.
Manifest host **`.runtime/m4-smoke-2b91f03de4f04338a62721cb76f53b0b.json`**
ghi summaries, chunks, RPC bytes, cursor pages, pending trước/sau restart,
terminal delete và volumes. M3 manifest cũ giữ nguyên làm snapshot bàn giao.

| Fixture | File ID | Terminal chunks/mappings | Replica bytes đã xác minh mất |
|---|---|---|---|
| M3 bàn giao | `f26d076f-0ec6-423d-a161-6e18d5799d2a` | 6 / 12 | 20,971,554 |
| M4 repair | `542fdee9-e8b4-434a-826b-bdd5be77422b` | 6 / 16 | 27,263,027 |
| M4 delete offline | `37e63fac-7b30-41de-bb0b-12bc6c780042` | 6 / 12 | 20,971,554 |

- Node-2 DOWN: fallback download SHA/bytes đúng. Repair max_chunks=2 dùng
  **3 cursor pages, checked=6/repaired=4/remaining=0**, khôi phục RF=2 trên
  node-1/node-3 cho 4 chunks bị ảnh hưởng. GetChunk trực tiếp xác minh 12 live
  mappings/20,971,554 replica bytes; không suy ra integrity từ health.
- Node-2 trở lại: scoped scan **2 pages, checked=4/repaired=0**, probe giữ
  nguyên mappings, 4 chunks có 3 live replicas và over_replicated=4. GetChunk
  xác minh cả **16 mappings/27,263,027 bytes**, download full SHA đúng.
- Stop node-2 lần hai: DELETE **202/DELETING/pending=4**; restart Metadata
  vẫn DELETING/pending=4 và download/repair bị chặn. Bật node-2, worker hoàn
  tất; repeat DELETE **200/DELETED/pending=0**, GET/detail/download **404**.
- Cleanup cả 3 owned fixtures qua API; **40 mappings trả GetChunk NOT_FOUND**,
  tổng **69,206,135 replica bytes** mất. DB giữ đủ 18 chunks/40 replica rows,
  status DELETED/cleanup_pending=false và 3 tombstones có deleted_at.
  Baseline cuối live/ready 200, 3 nodes ACTIVE, cluster pending/under/unavailable/
  over/domain_degraded bằng 0, operation_busy=false; cả 5 services healthy.
  Kiểm tra cuối bằng Compose `ps` và đọc `/tmp/dfs-downloads` xác nhận 5
  services running/healthy và download temp rỗng. Một file AVAILABLE có từ
  trước nằm ngoài manifest vẫn được giữ nguyên.

**Chủ động chưa chạy:** full backend suite, frontend, DB restart, hai host,
worst-case timeout benchmark/SLA. Không reset volumes/raw-delete chunk hoặc
DB rows, không cleanup fixture ngoài ownership. **P1–P6 hoàn thành; P7 còn lại**
để rà DoD/contracts/evidence và chốt bàn giao M5.

### Evidence P7 — 04/10/2026

**Tier: no execution**, vì chỉ sửa Markdown. Rà source Delete/cleanup/worker/
startup/shutdown, repair DTO/REST/cursor/budget, cached counters, Storage guard,
tests và evidence P1–P6; đọc host smoke/deployment manifests P6. Không sửa
runtime/tests/config/dependencies/frontend, không chạy pytest/Ruff/build/smoke,
không preflight/restart/deploy hoặc query cluster live ở P7.

**DoD:** đã đối chiếu các gate P1–P6 với source và evidence trong bảng mục 12;
P7 hoàn tất khi contracts/hướng dẫn/fixtures và bàn giao khớp implementation.
Counts 86 P1–P3, 89 P4–P5 và 16 P6 là các lượt focused có regression trùng,
không cộng thành full-suite total. Không suy ra UI runtime, hai host hoặc
worst-case timeout SLA từ các gate backend/local này.

**Docs chỉnh:** `API_CONTRACTS.md` sửa mẫu repair thành checked=1/repaired=1,
1 result và cursor chunk_index=0 cho nhất quán; chỉ sửa ví dụ, không đổi DTO/
behavior. `ARCHITECTURE.md` ghi đúng summary fields và per-chunk outcomes,
cleanup chỉ enqueue attempted replicas chưa DELETED. `README.md`, docs index,
plan tổng và plan M4 chốt P1–P7 hoàn thành, phân biệt historical evidence với
runtime M4 đã deploy, thêm hướng dẫn live smoke mới không tái dùng fixture M3
đã xóa. Mục 12 bàn giao M5 đầy đủ endpoint/state/cursor/error/cache semantics,
client credentials/token/error-envelope và public storage routes theo starter.
Những thay đổi frontend này là việc M5, chưa được triển khai hoặc kiểm tra ở P7.

**Fixtures/deployment:** host manifest P6 vẫn có 3 terminal DELETED/pending=0,
40 mappings NOT_FOUND và 69,206,135 replica bytes đã mất; deployment manifest
ghi build/Python/volumes preserved. M3 manifest giữ snapshot trước cleanup,
M4 manifest giữ kết quả terminal sau cleanup. Một AVAILABLE file ngoài
ownership được giữ theo evidence P6. Hai bản `docs/storage.proto` và
`backend/contracts/storage.proto` có cùng SHA-256 khi đọc file ở P7; không
đổi wire/schema hoặc thêm migration. Baseline 5 healthy/3 ACTIVE/pending=0
là quan sát cuối P6, không tuyên bố vừa chạy lại ở P7.

**Kiểm tra tài liệu:** `git diff --check` qua; các local Markdown links của
6 files đã chỉnh được đối chiếu với files/headings có thật, không có link gãy.
Không có runtime assertion/environment failure vì không chạy runtime checks.
Các warning/failure đã xử lý ở P1–P6 giữ nguyên trong evidence lịch sử.

**M4 hoàn thành P1–P7. Milestone tiếp theo M5 UI tối thiểu**, dùng contract và
bàn giao mục 12; M6 dành cho hai host/rehearsal. Chưa tạo phase plan M5 hoặc
triển khai UI trong lượt P7.

## Sửa findings sau code review — 04/10/2026

Đã sửa hai bug và hai điểm maintainability được review sau P7:

- `operations_available` dùng chung cho readiness và admission; kiểm tra
  scheduler/client trước và sau khi lấy operation lock. Scheduler chết thì
  upload/download/delete/repair mới trả 503; snapshot GET vẫn dùng được và
  thao tác đã nhận vẫn drain trước khi đóng resources.
- Smoke `finish` đọc các records đã checkpoint, không truy cập cả `repair`
  và `delete` trước dispatch. ID từ lỗi upload được đưa vào manifest và không
  nhân đôi. Trước mutation, đọc durable identity/history, xác minh tên fixture
  và checkpoint mapping thiếu. Hỗ trợ DELETED, FAILED chưa có chunks/mappings;
  giữ tất cả mapping đã ghi nhận và chỉ xác nhận absence bằng NOT_FOUND.
- Repair dùng public `worker.cleanup_pass` với cùng cursor và operation lock.
  Một boundary xử lý lỗi chunk ngoài dự kiến; log file/chunk/node/phase và
  exception class, không log raw exception message. Ack counts đã xác nhận
  được giữ, SQL/operation errors vẫn đi theo request-level failure.
- Snapshot tests dùng `tests/health_control.py` để drain/pause background work
  và giữ scheduler sống; không giả trạng thái worker.running sau `stop()`.

**Tier:** focused integration vì thay admission/lifecycle và recovery từ DB/API/gRPC.
Preflight read-only đạt: Docker Linux và PostgreSQL healthy. Rebuild riêng
image `tests`, chờ build exit 0 trước khi chạy. Services live không bị thay đổi.
Tests dùng PostgreSQL schema riêng và Storage gRPC thật với thư mục tạm.

Lượt chính từ repository root:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q --tb=short tests/test_metadata_operations.py tests/test_metadata_cleanup.py tests/test_metadata_delete.py tests/test_metadata_repair.py tests/test_metadata_repair_api.py tests/test_smoke_failure.py tests/test_smoke_failure_recovery.py tests/test_metadata_upload.py::test_empty_needs_no_active_nodes_and_repeated_post_creates_new_uuid tests/test_metadata_upload.py::test_pending_committed_before_every_store_with_no_coordinator_transaction tests/test_metadata_upload.py::test_database_failure_after_store_keeps_committed_attempt_and_recovery tests/test_metadata_upload.py::test_commit_uses_file_snapshots_and_acknowledgements_despite_health_change tests/test_metadata_upload.py::test_database_failure_before_file_creation_has_no_file_id tests/test_metadata_download.py::test_suspected_before_down_and_down_is_usable_fallback tests/test_metadata_download.py::test_database_failure_does_not_fail_file_or_leak_tmp
```

**112 passed, 0 skipped, 35.69 s**, image config
`sha256:03b79b8bc8f6e0813234767c2b4f6e8dc83e12d78cc2aa93933a1a4358ed125a`.
Sau đó bổ sung hai case FAILED chưa tạo chunk/mapping và điều chỉnh smoke
history check cho mapping rỗng hợp lệ; rebuild image tests và chạy lại riêng:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q --tb=short tests/test_smoke_failure_recovery.py
```

**7 passed, 0 skipped, 2.86 s**, image config
`sha256:0bdcde71d3a43feca8a30d33ff5a2cbff57e2206b91d85b55178465afc74b659`.
Tổng 114 test cases khác nhau được kiểm tra; 5 recovery cases được chạy lại vì
source nhánh đó thay đổi. Mỗi lượt Docker có một Starlette/TestClient deprecation
warning đã có; không có assertion hoặc prerequisite failure trong Docker.

Native `.venv/Scripts/python.exe -m pytest -q tests/test_smoke_failure.py`:
7 passed, 2 setup errors do WinError 5 tại
`C:/Users/thanh/AppData/Local/Temp/pytest-of-thanh`, kèm pytest-cache permission
warnings. Dừng lần chạy native đó; cả 9 guards đã pass trong lượt Docker trên.
Không sửa quyền/xóa cache host hoặc retry native suite. Ruff check và format
check đạt trên 15 file Python đã sửa; sau thay đổi cuối kiểm tra lại riêng
`scripts/smoke_failure.py` và `tests/test_smoke_failure_recovery.py`, đều đạt.

Không chạy full backend/frontend suite, live fault smoke hoặc deploy bản sửa
vào Metadata/Storage đang chạy. Evidence P6 vẫn mô tả build P6 trước bản sửa;
regression hiện tại chứng minh source trong các image tests mới ở trên.
