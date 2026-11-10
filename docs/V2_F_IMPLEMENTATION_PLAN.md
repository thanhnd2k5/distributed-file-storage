# Mốc F — automatic repair, rejoin và scrub

Cập nhật 10/11/2026. F1 và F2 đã xác minh baseline trên PostgreSQL/gRPC. F3–F5, C5/browser
và file 1–10 GiB vẫn tạm gác. E1–E4 đã xác minh baseline ngày 06/11/2026.

Đọc cùng [thiết kế §6–7](V2_DESIGN.md), [schema](V2_METADATA_SCHEMA.md),
[wire/API](V2_API_CONTRACTS.md) và [correctness](V2_CORRECTNESS.md).

## Hiện trạng và thứ tự triển khai

Schema foundation đã có health/observation generation, incarnation, allocation
ledger và ba kind HEALTH/REPAIR/SCRUB. Storage có HealthCheck, ListAllocations,
InspectAllocation và streaming Get/Store. Đây là nền tảng, chưa phải runtime F.

Worker dispatch bốn kind CLEANUP/RECONCILE/FINALIZE/REPAIR. F1 dùng lịch poll riêng theo
`node_id` để giữ đúng một bản ghi bền vững mỗi node qua nhiều vòng poll. Kind
HEALTH và SCRUB trong schema vẫn để dành; không ánh xạ `node_id` dạng chuỗi sang UUID
của job hoặc tạo lịch sử job theo từng nhịp poll, và chưa bật scheduler SCRUB ở F2.

| Phase | Phạm vi | Gate nhỏ nhất đủ |
|---|---|---|
| F1 | Health polling có deadline/hysteresis, observation fencing và eligibility | Policy thuần; PostgreSQL/gRPC thật cho hai poller, late response, timeout và sai scope |
| F2 | Phát hiện thiếu replica, enqueue repair có dedup, reserve đích mới và copy xác minh | PostgreSQL + ba Storage/failure domain trong fixture; race, mất ACK, capacity và bytes/hash |
| F3 | Rejoin/inventory theo trang, đối soát incarnation/epoch và orphan | Restart cùng volume, volume mới, late inventory và tombstone; không tự release charge |
| F4 | Scrub có ngân sách, observation missing/corrupt và repair handoff | Bytes hỏng thật, bounded reads, stale result và nguồn fallback |
| F5 | Gate kết hợp node failure/rejoin, Worker crash và tải người dùng | Focused nhiều process, đo budget/fairness và cleanup; không suy thành HA nhiều host |

## F1 — lát triển khai đầu tiên

### Contract poll và chuyển trạng thái

| Kết quả | Hành vi |
|---|---|
| Health hợp lệ khi ACTIVE | Giữ ACTIVE và đặt lại số lỗi liên tiếp |
| Timeout/lỗi kết nối | Lỗi đầu đưa ACTIVE sang SUSPECTED. Lỗi liên tiếp thứ ba đưa SUSPECTED sang DOWN |
| Health hợp lệ khi SUSPECTED | Hai lần hợp lệ liên tiếp đưa node về ACTIVE. Một lỗi ngắt chuỗi thành công. Một thành công ngắt chuỗi lỗi |
| Health hợp lệ khi DOWN | Ghi nhận node đã trả lời và giữ DOWN. Đối soát rejoin ở F3 hoặc quy trình activation quản trị hiện có quyết định khi nào node được phục vụ lại |
| Sai scope, failure domain hoặc node yêu cầu recovery | Đưa node vào QUARANTINED khi kết quả thuộc ticket hiện hành. QUARANTINED chỉ mở bằng quy trình đối soát và activation |

Chỉ poll endpoint đã đăng ký của node enabled có incarnation ACTIVE cùng epoch.
Gửi namespace, epoch, node ID và incarnation trong HealthCheck. Xác minh scope
và failure domain trong response. Với `FAILED_PRECONDITION` từ HealthCheck, giữ
node ngoài phục vụ bằng QUARANTINED vì endpoint không chấp nhận scope đang đăng
ký. Cancellation do shutdown hoặc lỗi DB chưa áp dụng kết quả không tính là
node failure. Health thành công chỉ xác nhận node phản hồi, kể cả khi
`writable=false`. Ghi writable/free bytes để quan sát. Placement vẫn dùng
eligibility ACTIVE và ledger hiện có, Storage vẫn kiểm dung lượng khi ghi.
Health không đổi replica health, allocation state, node budget hoặc charge.

Mặc định mỗi node đến hạn sau 10 giây kể từ lần áp dụng kết quả, deadline RPC
2 giây, ticket hết hạn sau 5 giây theo DB clock và mỗi Worker tối đa một RPC
health đang chạy. Các Worker trong cùng namespace dùng cùng interval và
ngưỡng. Tách một slot health khỏi executor job E3 để poll chậm không giữ slot
cleanup/reconcile/finalize. Tính thêm slot health khi cộng budget deployment.
Không xếp hàng RPC trong RAM. Mất Worker giữ ticket đến hạn, Worker khác có thể
claim lại sau expiry. Quá hạn ticket chỉ loại kết quả, không tự suy node DOWN.

### Ownership và lưu trạng thái

1. Lưu một lịch poll theo `node_id` gồm thời điểm đến hạn, hạn ticket và số lần
   thành công/lỗi liên tiếp. Ticket mang incarnation và endpoint đã chọn.
   Chọn node đến hạn theo thứ tự
   ổn định và khóa hàng với `SKIP LOCKED`. Tăng `health_generation` khi cấp
   ticket, commit trước RPC. Node mới active phải được đăng ký vào lịch trong
   cùng transaction activation, còn migration backfill node hiện có.
2. Khi áp dụng kết quả, khóa và đối chiếu ticket còn hạn, generation, scope,
   endpoint, domain, enabled và incarnation ACTIVE. Thay đổi cấu hình hoặc
   activation phải fence ticket cũ kể cả khi cấu hình đổi rồi quay lại giá trị
   cũ. Khóa node trước lịch poll ở cả claim/apply/activation để giữ thứ tự khóa.
   Cập nhật counter, state, `health_observed_at` và hạn poll kế tiếp cùng commit.
3. Thêm migration `0002` cho lịch poll và nâng runner để xác minh checksum
   foundation trước khi nâng cấp. Migration phải kiểm namespace/epoch trước
   khi sửa schema hiện hữu, áp dụng upgrade trong transaction và giữ checksum
   `0001`. Kiểm tra fresh install, upgrade từ schema V2 version 1 có dữ liệu và chạy migrate
   lần hai. Kết nối health poller từ cả Worker CLI và Worker runtime HTTP.

### Thứ tự thay đổi dự kiến

1. Thêm migration và kiểm tra khả năng nâng cấp trên schema fixture sở hữu riêng.
2. Thêm policy thuần và repository transaction claim/apply observation.
3. Thêm HealthCheck client validation và bounded poll scheduling, renew/cancel/
   retry/shutdown; không bật REPAIR/SCRUB khi chưa có handler tương ứng.
4. Chạy targeted policy và focused PostgreSQL/gRPC checks dưới đây; ghi evidence
   thật trước khi bàn giao F2. Chưa mở thêm endpoint UI ở F1.

### Gate F1 và hồi quy trực tiếp

| Case | Kết quả bắt buộc |
|---|---|
| Policy threshold/recovery | Chuyển state đúng ngưỡng; một timeout không được hiểu là mất bytes vĩnh viễn |
| Hai poller, ticket hết hạn và response tới muộn | Chỉ ticket mới còn hạn được ghi, state và counter giữ đúng khi restart |
| Scope/incarnation/config đổi trong RPC | Kết quả cũ bị từ chối; node thay volume vẫn bị chặn cho tới đối soát/activation |
| Health thành công sau observation corrupt | Allocation vẫn CORRUPT; charged bytes và allocatable budget giữ đúng |
| Node không eligible | Placement mới loại node theo policy; không làm hỏng admission/accounting đang có |
| Poll chậm, retry và shutdown | Concurrency/lease/deadline hữu hạn; user progress và ba kind E3 vẫn tiến triển |
| Upgrade từ schema V2 version 1 có dữ liệu | Backfill lịch cho node hiện có, giữ nguyên ledger và checksum foundation, chạy migrate lần hai trả no-op |

Tier **targeted + focused integration**. Gate PostgreSQL/gRPC dùng fixture V2
riêng trên Docker Linux. Preflight read-only theo [local tests](V2_LOCAL_TESTS.md).
Khi PostgreSQL healthy, build lại image rồi chạy từ repo root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_f1_integration.py tests/test_v2_metadata.py -k 'test_v2_f1_integration or migration or strict_domains'
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_c2.py::test_bounded_loop_cleanup_progress_and_shutdown tests/test_v2_c2.py::test_worker_entrypoint_sigterm_cancels_real_grpc_and_can_restart tests/test_v2_e3_integration.py::test_each_kind_progresses_while_an_older_flood_holds_its_io_lane
```

Database V2 hiện hữu cần chạy `python scripts/migrate_v2_metadata.py` từ
`backend/` với cấu hình database/namespace/epoch V2 trước khi khởi động Worker
bản F1. Migration runner giữ lại checksum của từng version đã áp dụng.

## Bất biến cho F2–F5

Repair dùng allocation UUID và reservation mới; nguồn phải qua checksum,
confirmation chỉ nhận ACK đúng scope/spec/generation và job token còn hiệu lực.
Manifest version/part bất biến; cập nhật replica projection qua allocations.
Tombstone/read lease và repair/delete phải được serialize để late repair không
hồi sinh file hoặc làm rò charge. Không còn nguồn hợp lệ thì báo thiếu replica,
giữ bằng chứng và retry policy; không công bố dữ liệu chưa kiểm chứng.

Node offline, corrupt hoặc inventory NOT_FOUND không làm charge biến mất.
Chỉ durable Delete ACK hợp lệ hoặc retire volume có quy trình xác nhận mới trả
charge. Online inventory giữ upper sequence và recheck/CAS từng observation vì
không có snapshot state qua các trang; bytes chưa rõ chủ phải quarantine/đối soát.

Repair/scrub thêm vào tổng execution/resident/RPC/buffer/I/O budget; đo lại gate
bị ảnh hưởng khi tăng kind hoặc process count. Muốn phục hồi RF=2/distinct domain
khi mất một node cần domain đích còn hoạt động và đủ dung lượng. Fixture nhiều
process cùng host chỉ chứng minh logic này, chưa chứng minh chịu mất host.

## F2 — discovery và enqueue

Metadata có hai transaction cho bước đầu F2. `repair_scan_page` đọc tối đa
tám manifest part theo cursor `(version_id,part_index)`. Cursor đi qua cả
part đủ replica và file terminal để một trang không có deficit vẫn tiếp tục
được. Chỉ báo deficit của file AVAILABLE theo RF và số failure domain của
session, dùng cùng tiêu chí replica hợp lệ với finalize.

`enqueue_repair` khóa file rồi kiểm lại version, chunk, deficit và ít nhất
một nguồn hợp lệ. Job dùng chunk ID làm dedup key. Hai transaction enqueue
đồng thời nhận cùng job ID. Discovery báo cả chunk không còn nguồn để vận hành
nhận biết tình trạng đó. Enqueue chưa cấp reservation.

Worker chưa chạy discovery định kỳ hay dispatch REPAIR. Chỉ bật scheduler
sau khi có đường copy, reconcile mất ACK và cleanup đích thất bại.

Focused integration trên Docker Linux/Python 3.12/PostgreSQL 16 đạt
**11 passed (2.67s)**, gồm 3 ca discovery/dedup và 8 hồi quy C1 về replica
policy, finalize và manifest. Ruff đạt trên hai file Python thay đổi.

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_f2_discovery.py tests/test_v2_c1.py::test_finalize_rechecks_replica_manifest_and_revision tests/test_v2_c1.py::test_confirm_and_complete_enforce_current_replica_policy
```

## F2 — reservation và write intent

`reserve_repair` khóa file/session/part, kiểm lại đích theo incarnation và
failure domain, rồi ghi allocation, charge và write attempt cùng transaction
trước RPC. Khi cần khôi phục số domain, đích phải thêm một domain còn thiếu.
Hiện chỉ chọn node chưa giữ allocation chưa release của chunk đó.

Payload của job giữ allocation ID và fencing token đã cấp intent. Cùng lease
trả lại cùng intent. Lease mới giữ allocation và charge cũ, tăng generation
để Worker tiếp theo có thể fence writer cũ ở Storage trước khi tiếp tục.
`confirm_repair` kiểm file còn AVAILABLE, job lease, incarnation, generation
và nội dung ACK trước khi xác nhận allocation STORED và đóng job. Nếu file
bị tombstone, cleanup hiện có tiếp quản allocation và chỉ trả charge sau
durable Delete ACK.

Focused integration Docker Linux/Python 3.12/PostgreSQL 16 đạt
**10 passed (2.37s)**, gồm 4 ca intent mới, 3 ca discovery và 3 hồi quy C1.
Gate này dùng ACK fixture để kiểm transaction. Chưa chạy copy qua gRPC hoặc
reconcile mất ACK. Ruff check và format đạt trên hai file Python thay đổi.

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_f2_intent.py tests/test_v2_f2_discovery.py tests/test_v2_c1.py::test_confirm_and_complete_enforce_current_replica_policy
```

## F2 — thực thi copy, recovery và cleanup

`StorageClient.copy` stream trực tiếp `GetChunk` → `StoreChunk` theo từng block
giới hạn `<= 262144` byte dưới giới hạn tốc độ của `ReadBudget`, giữ lại block
cuối trong RAM cho tới khi nhận `footer` khớp size/SHA-256 của manifest và
`StopIteration` sạch (không đóng/commit `StoreChunk` nếu stream nguồn đứt trước
EOF). Lỗi từ nguồn được tách riêng qua `RepairSourceError(store_opened=...)` để
`Worker._copy_chunk` phân biệt lỗi nguồn và lỗi đích.

Trong `Worker.repair`:

1. `reserve_repair` chốt/tái sử dụng một allocation đích (`allocation_id`) và
   tăng generation nếu job đổi lease; khi số replica/domain đạt lại policy thì bỏ
   allocation dư (nếu có) qua `CLEANUP` và đóng job `REPAIR`.
2. `PrepareAllocation` trên node đích cho phép tái sử dụng ngay allocation đã
   `STORED` ở các trường hợp mất ACK sau `StoreChunk` mà không cần copy lại bytes.
3. Khi gặp `RepairSourceError`, Worker chuyển sang nguồn kế tiếp (và gọi lại
   `PrepareAllocation` nếu `StoreChunk` đã mở). Khi đích trả lỗi xác định
   (`ENOSPC`/`RESOURCE_EXHAUSTED`, `FAILED_PRECONDITION`, ACK/stream không hợp lệ)
   hoặc lỗi mạng/timeout có thể tiếp tục sang đích khác (`repair_can_continue`),
   `abandon_repair_destination` chuyển allocation đích lỗi sang `DELETE_PENDING`
   kèm job `CLEANUP` có `dedup_key = allocation:<allocation_id>`. Nếu đích còn
   truy cập được thì `DeleteAllocation` chạy inline giải phóng charge ngay; nếu
   đích đang rớt mạng thì charge vẫn giữ và `Worker.cleanup_allocation` thu hồi
   khi node quay lại.
4. `Worker._scan_repairs` quét định kỳ theo cursor `page_size` (log warning nếu
   `eligible_nodes <= 0`, không tạo job rỗng). Job `DEAD` không cho `enqueue_repair`
   tạo trùng backlog và tự wake `DEAD → RETRY` qua `_requeue_dead_repairs` khi
   `activate_node`, node hồi `ACTIVE` trong `apply_health_poll`, hoặc tăng
   `allocatable_bytes` trong `set_budget`. File `AVAILABLE` không bị chuyển thành
   `FAILED` nếu repair cạn retry; tombstone (`DELETING`) vẫn thu hồi mọi allocation.
5. `KINDS` mở rộng thành `("CLEANUP", "RECONCILE", "FINALIZE", "REPAIR")` và giới
   hạn `jobs_per_kind <= 24 // len(KINDS)` (`6`), giữ nguyên ràng buộc loại trừ
   tối đa 24 job đang giữ của E3.

## Evidence hiện tại

Targeted Windows/Python 3.12 (`.venv`):

- `pytest -q tests/test_v2_f1.py tests/test_v2_e3.py` đạt **25 passed (0.12s)**
  cho policy health F1 và logic thuần `WorkerConfig`/`ReadBudget`/`Work`/`Worker.close`.
- `ruff check` và `ruff format --check` đạt trên cả 6 file Python thay đổi
  (`repository.py`, `storage_client.py`, `worker.py`, `test_v2_e3.py`,
  `test_v2_e3_integration.py`, `test_v2_f2_integration.py`).

Focused integration trên Docker Linux/Python 3.12/PostgreSQL 16 + StorageService
gRPC/SQLite thật (container `dfs-v2-tests-postgres-1` trạng thái healthy, truy
vấn kiểm tra `dfs_v2_test|v2_test|1`, image `dfs-python-tests:dev` build lại từ
source hiện tại):

- **Gate F2 (discovery, intent, execution, recovery, cleanup, crash/cancel) & C1:**
  ```powershell
  docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_f2_integration.py tests/test_v2_f2_discovery.py tests/test_v2_f2_intent.py tests/test_v2_c1.py::test_confirm_and_complete_enforce_current_replica_policy
  ```
  Đạt **23 passed (17.08s)**, bao phủ health→`DOWN` discovery có phân trang và đọc
  lại byte-for-byte/SHA-256 chỉ từ replica mới, mất ACK + reclaim generation không
  copy lại, `ENOSPC`/unreachable cleanup (`DELETE_PENDING` → `RELEASED`) và xoay
  đích, truncate stream nhiều block (`262144 + 777` B) trước `footer` + `ReadBudget`
  + nguồn lỗi cạn retry về `DEAD` / tự wake khi activate node mới, tombstone
  trong lúc repair, `SIGKILL` ở 6 ranh giới crash, và graceful cancel khi tắt `Worker`.
- **Gate hồi quy E3 4-kind fairness, F1 health & C2 lifecycle/SIGTERM:**
  ```powershell
  docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_e3_integration.py tests/test_v2_f1_integration.py tests/test_v2_c2.py::test_bounded_loop_cleanup_progress_and_shutdown tests/test_v2_c2.py::test_worker_entrypoint_sigterm_cancels_real_grpc_and_can_restart tests/test_v2_c2.py::test_exhausted_job_rechecks_lease_after_failure_domain_effects
  ```
  Đạt **21 passed (16.60s)**.

F1 và F2 hoàn tất baseline. Giới hạn đã kiểm: fixture dùng nhiều process cùng host
trên Docker Linux, chưa chứng minh failover nhiều host vật lý; chưa chạy full
backend suite, cluster smoke, F3 rejoin/inventory reconciliation, F4 scrub, F5 hay
frontend/C5 checks.
