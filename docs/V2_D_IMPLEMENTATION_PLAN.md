# Mốc D — Download streaming, Range và read/delete lease

Trạng thái 29/10/2026: **D1–D5 hoàn tất trong phạm vi baseline đã ghi evidence**.
GET/Range, read/delete lease và tải hỗn hợp đã kiểm qua Nginx, hai API/hai Transfer
độc lập và Storage thật trên một host. Chưa có gate HA/nhiều host hoặc file 1–10 GiB.
Theo chỉ đạo người dùng, tạm gác gate browser/file lớn C5 để triển khai phần D
độc lập. Không coi C5 hoặc mốc C đã hoàn tất, không suy ra nghiệm thu download
từ unit tests. Các gate còn thiếu của C5 giữ ở
[kế hoạch C](V2_C_IMPLEMENTATION_PLAN.md#14-evidence-c5-bước-đầu--26102026).

Đọc cùng [thiết kế](V2_DESIGN.md#5-download-và-xóa),
[API](V2_API_CONTRACTS.md#3-catalog-download-và-delete),
[schema](V2_METADATA_SCHEMA.md#3-transaction-và-thứ-tự-khóa) và
[crash/race matrix D01–D04](V2_CORRECTNESS.md#4-ma-trận-crashrace-phải-chuyển-thành-test-bd).
Giữ nguyên contract V2 và runtime/volume V1.

## 1. Thứ tự triển khai

| Phase | Phạm vi | Gate nhỏ nhất đủ chứng minh |
|---|---|---|
| D1 — hoàn tất | Pure selection cho GET/HEAD, Range, conditional ETag và attachment header | Unit tests status/header/byte slices, file rỗng, precondition priority, invalid input và số rất lớn |
| D2 — hoàn tất | Domain acquire/renew/release read lease, immutable manifest paging và file tombstone/cleanup | PostgreSQL thật, hai repository/connection; acquire/renew/delete/cleanup races, DB-clock expiry, absolute limit và stale epoch |
| D3 — hoàn tất | API metadata/catalog cần cho download, internal read commands và DELETE file | Focused HTTP/PostgreSQL; DTO/status/headers/envelope, không lộ token/endpoint nội bộ; HEAD không tạo lease |
| D4 — hoàn tất | Transfer đọc/verify tối đa một chunk, fallback bounded, Range slicing và streaming response | HTTP/gRPC/filesystem thật; corrupt replica, missing footer/EOF, disconnect, renew fail và lỗi sau headers |
| D5 — hoàn tất baseline | Nginx/nhiều instance, read/delete race và budget tải hỗn hợp | D01–D04 với bytes thật, read amplification/TTFB/RSS, slow client/replica cùng finalize; lưu topology và command |

D2 tái sử dụng bảng `download_leases` và guard read lease đã có trong cleanup;
kiểm behavior hiện hữu trước khi sửa migration. Tách phần catalog tối thiểu cho
luồng download khỏi dashboard/health tổng hợp F–G.

## 2. Bất biến khi nối các tầng

- Resolve đúng AVAILABLE version trước khi đánh giá preconditions. UPLOADING/
  VERIFYING/FAILED trả FILE_NOT_READY; tombstone trả FILE_NOT_FOUND kể cả khi
  conditional header khớp. Không dùng helper D1 để bỏ qua state/epoch gate.
- If-Match so strong, If-None-Match so weak, rồi mới If-Range/Range. HEAD bỏ qua
  Range/If-Range; không đọc Storage và không tạo read lease. Không phát
  Last-Modified/date validator trong baseline.
- GET thành công kể cả file rỗng phải có lease trên cùng version; HEAD, 304,
  400, 412 và 416 không tạo lease. Resolve/plan trước acquire không thay cho
  acquire transaction recheck state: DELETE có thể xảy ra ở giữa.
- Acquire/renew và authorize physical delete khóa file theo thứ tự contract;
  dùng DB clock sau khi sở hữu lock. Renew được tiếp tục sau tombstone chỉ
  khi lease cũ vẫn ACTIVE, còn hạn và cùng version. Không hồi sinh lease cũ.
- TTL baseline 30s, renew mỗi 10s, absolute limit 24h. Mất renew phải dừng
  phát bytes trước khi lease hết hạn; release best effort, GC dựa expiry.
- Manifest phân trang có giới hạn. Verify đầy đủ size/hash/EOF của tối đa
  một chunk trước khi phát bất kỳ bytes nào của chunk đó; Range nhỏ vẫn có
  thể đọc cả chunk liên quan. Không spool cả file, không prefetch chunk kế.
- Cấp admission/buffer/RPC budget trước I/O. Fallback và tổng thời gian hữu
  hạn; cancellation phải drain stream/task. Không giữ transaction DB qua I/O.
- Trước binary headers, lỗi dùng envelope V2. Sau headers, lỗi ngắt response,
  không gửi JSON hoặc padding cho đủ Content-Length. HEAD không có error body.
- DELETE tombstone/enqueue cleanup cùng commit, chặn lượt đọc mới; read lease
  cũ được giữ tới giới hạn hợp lệ. Chỉ durable Delete ACK mới giải phóng charge.

## 3. Evidence D1 — 28/10/2026

Thêm [`download_http.py`](../backend/transfer/v2/download_http.py) với
`plan_download()` trả status, ETag, binary headers, khoảng `[start, stop)` và
`requires_lease`. Không nối route, không tạo lease hay đọc Storage. Adapter sau
này phải dùng V2 envelope cho `error_code`, chặn mọi body của HEAD và chỉ lấy
Content-Length binary từ plan khi status 200/206; helper cố ý không gán chiều
dài representation cho body lỗi hoặc 304.

Hành vi được kiểm theo contract:

- Single closed/open/suffix range, clamp end, inclusive HTTP end chuyển thành
  exclusive stop; range hợp lệ vẫn trả 206 khi phủ cả representation.
- Multi-range hợp lệ và unit khác bỏ qua trả full 200; malformed byte range
  kể cả trong multi-range trả 400 INVALID_RANGE; unsatisfiable trả 416 và size.
- File rỗng: GET thường và suffix dương 200/length=0, suffix=0 hoặc start range
  416. HEAD trả full metadata, bỏ qua cả Range sai cú pháp và không cần lease.
- If-Match/If-None-Match theo strong/weak, wildcard, list có dấu phẩy trong tag,
  malformed list và priority trước Range. If-Range chỉ áp dụng Range khi exact
  strong ETag khớp; weak, date hoặc không khớp chọn full response.
- Numeral 5000 chữ số không overflow hoặc vướng giới hạn `int()` của Python;
  so sánh decimal chuẩn hóa trước khi chuyển offset đã bounded. End<start vẫn
  là invalid kể cả hai số đều rất lớn; offset representation tới 2^63−1 chính xác.
- Attachment UTF-8 percent-encoded giữ tên Unicode/quote/semicolon/percent trong
  một parameter; từ chối basename có slash/backslash/control hoặc surrogate.
  Content type cố định application/octet-stream, không thêm Content-Encoding.

Tier **targeted tests/checks**, native Windows, Python 3.12.14 trong `backend/.venv`.
Từ `backend/`:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_v2_download_http.py
.\.venv\Scripts\python.exe -m ruff check transfer/v2/download_http.py tests/test_v2_download_http.py
.\.venv\Scripts\python.exe -m ruff format --check transfer/v2/download_http.py tests/test_v2_download_http.py
```

**121 passed** (0.34s), không skipped/deselected/failure. Ruff check/format
**2 files passed**. Các ca Range đối chiếu byte slices từ buffer fixture,
không phải bytes truyền qua HTTP/Storage. Không cần infrastructure preflight
vì không có dependency runtime bên ngoài. Không chạy Docker, browser, integration,
full suites, frontend lint/build, C5 hoặc benchmark. Chỉ thêm tài liệu sau checks;
không rerun checks đã pass cho thay đổi docs.

D1 chuẩn bị semantics của D03; **D03 và mốc D chưa được nghiệm thu end-to-end**.
Tiếp theo D2 read/delete lease và transaction races trước khi nối GET download.

Tham chiếu chuẩn khi hiện thực:
[RFC 9110 — preconditions](https://www.rfc-editor.org/rfc/rfc9110.html#section-13.2),
[byte ranges](https://www.rfc-editor.org/rfc/rfc9110.html#section-14.1.2).

## 4. Evidence D2 — 28/10/2026

Repository đã có `resolve_download`, `acquire_read`, `renew_read`, `release_read`,
`read_manifest` và `delete_file`; tái sử dụng schema foundation, không thêm migration.
Acquire kiểm AVAILABLE/version dưới file lock; renew và manifest lấy file rồi lease
lock, kiểm DB clock sau khi có khóa. TTL 30s, absolute limit 24h; lease còn hạn
được renew/đọc manifest sau tombstone, lease hết/released không hồi sinh.
Manifest/spec bất biến, paging tối đa 1000 part và tối đa RF destination mỗi part;
eligible allocation lọc identity/epoch/incarnation, ACK spec và health hiện tại.

DELETE file tombstone/enqueue cùng transaction, abort OPEN/FINALIZING để fence
confirm/retry/publication và trả admission đúng một lần; giữ charge/attempt tới
durable Delete ACK. Cleanup nhận cả session COMPLETED của file đã tombstone,
chặn authorize và finish khi còn read lease, kể cả manifest rỗng. Sau khi mọi
allocation RELEASED/attempt CLOSED và không còn lease hợp lệ, commit DELETED
cùng job DONE. Việc chờ read lease retry sau 10s và không tiêu retry budget lỗi.

Tier **focused integration**, Docker Linux/Python 3.12, PostgreSQL 16 fixture
`dfs_v2_test`, schema sở hữu riêng cho từng ca. Hai ca dùng Worker với gRPC,
SQLite/bytes thật; không cần API/Transfer, Storage containers hoặc volumes V1.
Ruff check/format hai file qua native Windows trong `backend/.venv`.

Preflight sau lượt gián đoạn trả Docker `29.8.2` nhưng
`service "postgres" is not running`. Đã dừng gate và hỏi người dùng theo
`AGENTS.md`; sau khi được phép, khởi động đúng fixture bằng lệnh dưới. Container
healthy và psql trả `dfs_v2_test|v2_test|1`; không migrate public hoặc reset dữ liệu.

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml up -d --wait postgres
```

Build tests image từ source hiện tại, rồi chạy từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_d2.py
```

**57 passed (15.94s)**, không skipped/deselected/failure. Các nhóm chính:

- Acquire/resolve kiểm state và version, kể cả empty file; plan metadata không tạo
  lease. TTL đúng 30s/absolute 24h theo DB; request tới sau tombstone bị chặn.
- Hai repository/connection cạnh tranh acquire–DELETE, renew–cleanup,
  release–renew và DELETE–confirm/publication. Ca acquire–DELETE dùng file lock
  có điều phối và `pg_blocking_pids` xác nhận connection còn lại thực sự chờ khóa.
- Renew/manifest chờ file lock hoặc lease lock tới khi TTL hết phải thất bại;
  acquire bắt đầu TTL sau lock wait; cleanup đọc clock sau khi lấy file lock.
- Lease expired/released không hồi sinh; token khác/read ID khác không dùng được.
  Renew sau tombstone chỉ lease còn hạn, clamp absolute deadline trong fixture.
- Manifest phân trang đúng thứ tự, giữ spec/version sau DELETE, bound cursor/limit;
  lọc replica sai ACK size/hash, state, epoch, health hoặc incarnation eligibility.
  Không yêu cầu đủ RF để lấy metadata; Transfer sau này chịu trách nhiệm fallback
  và báo CHUNK_UNAVAILABLE khi không lấy được bytes hợp lệ.
- DELETE idempotent và release admission một lần, giữ charge tới durable ACK;
  stale job/failed enqueue rollback domain effects, không commit DELETED sớm.
- Worker thật chờ hai read lease trên file rỗng và file nhiều part, qua nhiều lượt
  retry dù `max_attempts=1`; release một lease chưa cho xóa, expiry của lease còn
  lại cho cleanup tiến hành. Bytes/ledger trên cả hai replica được dọn, charge về
  0, job DONE và file DELETED; đây là evidence cleanup, chưa phải HTTP download.

Direct regressions, cùng image/DB, từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_metadata.py::test_cleanup_requires_durable_exact_ack_and_releases_once tests/test_v2_metadata.py::test_cleanup_authorization_pages_and_retries_do_not_release_charge tests/test_v2_metadata.py::test_stale_cleanup_job_cannot_publish_capacity_release tests/test_v2_c1.py::test_stale_finalize_job_rolls_back_failure_cleanup_and_admission tests/test_v2_c1.py::test_abort_races_never_resurrect_terminal_upload tests/test_v2_c1.py::test_retry_and_cleanup_recheck_job_lease_after_waiting_for_job_lock tests/test_v2_c2.py::test_cleanup_pages_and_lost_delete_ack_hold_charge_until_retry tests/test_v2_c2.py::test_cleanup_cannot_finish_with_earlier_unresolved_page tests/test_v2_c2.py::test_retry_budget_keeps_cleanup_obligation tests/test_v2_c2.py::test_full_hash_mismatch_fails_and_cleanup_runs
```

**20 passed (8.23s)**, không skipped/deselected/failure. Không cộng collect-only
hoặc lượt kiểm cũ thành full suite. Ruff check/format **2 files passed** với
`metadata/v2/repository.py` và `tests/test_v2_d2.py`. Chỉ sửa docs sau gate;
không rerun checks đã pass cho thay đổi tài liệu. PostgreSQL test giữ healthy.

Preflight/gate tái lập ở [V2 local tests](V2_LOCAL_TESTS.md#readdelete-lease-d2).
Không chạy full backend suite, frontend lint/build, browser C5, benchmark hoặc
gate D3–D5. Chưa có route catalog/read commands/DELETE file, HTTP streaming,
Range qua Storage, mixed-load/RSS hoặc D01–D04 end-to-end.

Tiếp theo **D3**: API metadata/catalog tối thiểu, internal acquire/manifest/renew/
release, public DELETE; kiểm DTO/status/envelope và không lộ token/endpoint nội bộ.
HEAD không tạo lease/đọc Storage. D4 mới nối binary download, D5 mới kiểm topology
nhiều instance và budgets. C5 vẫn tạm gác, mốc C và D chưa hoàn tất.

## 5. Evidence D3 — 28/10/2026

Thêm `metadata/v2/download_api.py`, đăng ký route vào hai listener API hiện có:

- Public GET `/files`, `/files/{id}`, `/files/{id}/chunks` và DELETE `/files/{id}`.
  Catalog AVAILABLE dùng keyset `(created_at,file_id)` giảm dần, limit 1..100;
  cursor opaque gắn version/filter/scope namespace–epoch, vẫn dùng được khi anchor
  đã tombstone. Không hứa snapshot giữa các trang. DTO whitelist không chứa
  claim/read token, Storage endpoint/incarnation hoặc allocation identity.
- Metadata theo ID hiển thị UPLOADING/VERIFYING/FAILED với checksum null; chunks
  chỉ cho AVAILABLE. Tombstone ẩn khỏi cả catalog/detail/chunks. Public replicas
  chỉ là cached observations, bound theo RF, không phải kiểm bytes/readability.
- Internal POST `/downloads`, GET manifest và POST renew/release nối domain D2.
  Strict DTO/UUID/epoch, GET dùng một `X-Recovery-Epoch` và một
  `X-Read-Lease-Token`, không dùng token trong URL. Acquire có version guard
  tùy chọn. Release lặp cùng token trả 204 không body; JsonClient hỗ trợ header
  epoch/token, body rỗng 204 và giữ socket/call budget như trước.
- Transfer HEAD `/files/{id}/download` tra public metadata, kiểm state trước
  conditional ETag, dùng helper D1; luôn không body, không lease/Storage I/O,
  bỏ qua Range/If-Range và kết hợp repeated ETag list fields. Nginx route download
  tới Transfer, metadata/DELETE tới API; internal listener không có route public.
  GET binary vẫn chưa có, chưa triển khai Range bytes hoặc tạo download stream.

Wire fields/status/bounds chốt tại [API contract](V2_API_CONTRACTS.md#3-catalog-download-và-delete).
Read ID không tồn tại → internal 404 READ_LEASE_NOT_FOUND, token cũ/sai hoặc
lease expired/released → 409 STALE_READ_LEASE cho renew/manifest. Error envelope,
Retry-After và request ID dùng handler chung; HEAD bỏ toàn bộ body kể cả lỗi.
Không thay migration, schema, proto, role lifecycle hoặc volumes V1.

Tier **focused integration**, Docker Linux/Python 3.12 + PostgreSQL 16 fixture
`dfs_v2_test`, mỗi test sở hữu schema riêng. Preflight read-only: Engine `29.8.2`,
PostgreSQL healthy, psql trả `dfs_v2_test|v2_test|1`; không cần start/reset/migrate
public. Image `dfs-v2-http-tests:dev` build mới từ source, có Nginx thật; các
HTTP listener/gRPC/bytes trong test container, không publish thêm port host.

Lệnh từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_d3.py
```

**55 passed (43.57s)**, không skipped/deselected/failure. Bao gồm:

- Cursor ties, deleted anchor và insert giữa các trang; checksum/state visibility,
  manifest paging và public projection; invalid/unknown/repeated query/cursor.
- Internal acquire commit thấy được trên connection khác; JsonClient GET
  manifest/renew và 204 release replay; DTO, stale token/version, epoch/header
  thiếu/lặp, DB recovery gate và listener isolation.
- HEAD empty/nonempty, tên Unicode/quote, 200/304/400/412, ETag priority và repeated
  fields, malformed Range bị bỏ qua; state/tombstone thắng conditional header.
  DB/recovery errors có Retry-After/request ID và HEAD không body, không lộ lỗi DB.
- Hai ca HTTP upload/finalize rồi acquire/read–DELETE thật: 202/idempotent/ẩn ngay,
  Worker chờ read lease mà giữ charge, renew/manifest tiếp tục sau tombstone;
  release rồi durable gRPC Delete ACK trên hai replica → bytes dọn/DELETED/200.
  Một ca acquire–DELETE đồng thời qua hai HTTP connection. Đây là control-plane
  và cleanup evidence, chưa phải download bytes qua HTTP.
- Nginx thật route HEAD tới Transfer (API trực tiếp không có HEAD download),
  metadata/DELETE tới API; chặn internal path; HEAD sau DELETE 404, không tạo lease.

Direct regressions do sửa shared JsonClient/handlers/ingress, cùng image/DB:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c3.py::test_upload_complete_publish_real_http_bytes tests/test_v2_c3.py::test_create_replay_strict_json_and_pagination tests/test_v2_c3.py::test_content_length_busy_epoch_and_internal_dto tests/test_v2_c3.py::test_actual_bytes_fail_then_synchronous_retry_handoff tests/test_v2_c3.py::test_error_request_id_and_invalid_confirmation_evidence tests/test_v2_c3.py::test_independent_readiness_and_recovery_gate tests/test_v2_c3.py::test_limits_and_control_admission tests/test_v2_c3.py::test_real_ingress_streams_before_eof_and_blocks_internal
```

**13 passed (13.75s)**, không skipped/deselected/failure. Ruff check/format **6 files
passed** trên native Windows trong `backend/.venv`:

```powershell
.\.venv\Scripts\python.exe -m ruff check metadata/v2/api.py metadata/v2/http.py metadata/v2/download_api.py metadata/v2/repository.py transfer/v2/app.py tests/test_v2_d3.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/v2/api.py metadata/v2/http.py metadata/v2/download_api.py metadata/v2/repository.py transfer/v2/app.py tests/test_v2_d3.py
```

Không chạy full suite, nguyên gate D1/D2/C3, frontend lint/build, browser C5,
benchmark 1–10 GiB/RSS hoặc hai instance D5. Chỉ cập nhật docs sau checks, không
rerun checks đã pass. PostgreSQL fixture giữ healthy. Mốc C và D chưa hoàn tất.

Tiếp theo **D4**: GET binary qua Transfer; acquire cùng version trước headers,
read lease renew mỗi 10s và dừng phát bytes khi mất lease; bounded one-chunk verify
size/hash/footer/EOF trước khi phát, fallback/cancellation hữu hạn, Range slicing
và lỗi trước/sau headers. D5 mới đo mixed load/budgets và kiểm D01–D04 nhiều instance.

## 6. Evidence D4 — 29/10/2026

Thêm `transfer/v2/download.py`, nối GET `/api/v2/files/{id}/download` và bổ sung
download budgets trong `Limits`. Không đổi schema/migration/proto hoặc volume V1.

- Resolve AVAILABLE trước conditional/Range; acquire cùng `version_id` trước
  binary headers, kể cả empty file. Acquire recheck thắng race DELETE giữa resolve
  và acquire. 304/400/412/416 không acquire hay đọc Storage; HEAD giữ semantics D3.
- Một slot dành một chunk buffer và một GetChunk RPC đồng thời; không queue chờ
  admission, spool cả file hoặc prefetch. Manifest một item/trang, bắt đầu tại
  chunk của Range. Verify scope/allocation/generation/spec, offset, actual size/hash,
  footer và successful EOF rồi mới gửi slice. Mỗi replica thử một lần, tuần tự,
  bound `max_replicas` và deadline RPC; không trộn bytes giữa hai attempt.
- Renew mỗi 10s mặc định; TTL/absolute deadline dựa DB clock và monotonic clock
  local sau khi trừ toàn round trip + 100ms dự phòng. Renewal bị block vẫn bị hủy
  trước deadline cũ. Mất renew/expiry/total deadline/shutdown dừng cả send đang block
  và RPC; client disconnect không kích hoạt fallback mới. Release best effort 2s
  sau drain watcher, DB expiry là fallback khi release hoặc acquire response mất.
- Hết replica trước headers → 503 CHUNK_UNAVAILABLE/envelope/Retry-After; sau headers
  → ngắt transport, không JSON/padding cho Content-Length. Không expose read token
  hay Storage endpoints trong lỗi public. Error lease nội bộ được map ra lỗi public.

Tier **focused integration**, Docker Linux/Python 3.12 + PostgreSQL 16 fixture
`dfs_v2_test`, HTTP Uvicorn, Nginx, gRPC, SQLite và bytes trên filesystem thật.
Mỗi ca có schema và thư mục tạm riêng; hai Storage node RF=2 chạy trong tests
container. Fault readers đọc ledger/bytes thật rồi inject frame/footer/EOF/deadline.
Các ca blocked-send dùng ASGI có điều phối để kiểm cancellation/backpressure chính
xác, không coi đó là benchmark TCP slow client hay tải hỗn hợp D5.

Preflight read-only: Docker Engine **29.8.2**, PostgreSQL healthy, psql trả
`dfs_v2_test|v2_test|1`. Lượt sandbox đầu bị từ chối truy cập named pipe Docker;
chạy lại với quyền được cấp thành công. Không start/restart/reset/migrate fixture.
Build image mới mỗi khi source test/runtime thay đổi; từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d4.py
```

**60 ca D4 riêng biệt đã pass qua các lượt chọn lọc**, không cộng các lần chạy
lặp thành ca mới và không tuyên bố một lượt nguyên file đã pass:

1. Sau sửa runtime release lease, lượt `-x` có **42 passed**, dừng ở assertion
   test cleanup lấy `state` từ return `None` của Worker; đây là lỗi test, không phải
   lỗi infrastructure. Đổi assertion sang job/file state, ledger/charge và bytes.
2. Chạy phần còn lại bằng lệnh dưới: **18 passed, 42 deselected (14.69s)**.
3. Hoàn thiện admission thành bốn HTTP download thật và đổi readiness negative test
   sang lifespan để không tạo warning SystemExit từ thread Uvicorn. Chỉ hai ca bị
   đổi chạy lại: **2 passed (3.05s)**, không warning/skipped/failure. Production
   source không đổi giữa các lượt này; các ca còn lại không rerun.

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d4.py -k 'slow_send_renews or download_admission or invalid_download_budgets or ingress_get or readiness_rejects or multiple_wire_blocks or control_failures or state_gate'
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d4.py::test_download_admission_precedes_acquire_or_storage tests/test_v2_d4.py::test_readiness_rejects_chunk_policy_larger_than_reservation
```

Phạm vi đã exercise:

- Empty/boundary/multiple chunks; full GET và Range closed/open/suffix/clamp,
  multi-range/unit khác/If-Range; 400/412/416/304 và state gate thắng conditional.
  Status, Content-Length/Content-Range/ETag/UTF-8 attachment và actual HTTP bytes
  đúng. Nginx thật trả Range và If-Range; không chỉ gọi helper D1.
- Corrupt physical replica chuyển ledger CORRUPT, fallback sang replica còn đúng.
  Fault stream: sai identity/spec/generation, duplicate header, corrupt bytes,
  offset/empty/extra block, missing/wrong footer, trailing frame, error sau footer,
  missing successful EOF và slow replica. Bounded fallback không phát chunk chưa
  verify; lỗi ở chunk thứ hai chỉ để prefix `abc` rồi HTTP incomplete.
- Chunk 262147 bytes vượt wire block 256 KiB; full file **524544 bytes**, ba chunk,
  tái sử dụng đúng một buffer; Range băng qua wire/chunk boundary trả đúng slice.
  Đây là correctness fixture, không phải benchmark 1–10 GiB.
- TCP disconnect hủy/drain RPC và admission. ASGI blocked-send không fetch chunk
  kế; renew fail/stall, expiry, total timeout và shutdown đều hủy send hữu hạn.
  TTL fixture được rút ngắn; acquire response chậm hơn TTL không phát headers/bytes.
  Release lỗi không biến binary success thành JSON; lease còn ACTIVE chờ DB expiry.
- Người đọc đang gửi gặp DELETE vẫn renew/đọc cùng version; Worker giữ bytes và
  charge khi lease ACTIVE. Sau response/release, cleanup durable gRPC Delete ACK
  trên hai replica → job DONE/file DELETED, charge=0 và filesystem không còn chunks.
  Bốn GET chậm thật giữ bốn lease/RPC; lượt thứ năm 429 không thêm lease/RPC.
- Startup từ chối policy chunk vượt reservation; từ chối config buffer thiếu,
  renew/deadline/stream budget vượt bound hoặc duration không hữu hạn.

Lượt đầu gate còn bắt được race thực: Uvicorn phát disconnect sau final body,
watcher hủy release làm lease còn ACTIVE dù HTTP bytes đúng. Đã đưa release ra sau
drain watcher; success/empty/disconnect và cleanup tests trên đều kiểm release DB.

Direct regressions vì sửa shared Limits/HTTP status và Transfer startup/shutdown,
cùng production source, image/DB fixture; từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d3.py::test_head_status_headers_and_preconditions_without_lease_or_storage tests/test_v2_d3.py::test_head_combines_repeated_etag_lists_and_handles_state_before_conditionals tests/test_v2_d3.py::test_d3_errors_use_envelope_retry_after_and_head_suppresses_body tests/test_v2_c3.py::test_upload_complete_publish_real_http_bytes tests/test_v2_c3.py::test_independent_readiness_and_recovery_gate tests/test_v2_c3.py::test_limits_and_control_admission tests/test_v2_c3.py::test_error_request_id_and_invalid_confirmation_evidence
```

**23 passed (20.37s)**, không skipped/deselected/failure. Ruff check và format
**5 files passed** trên native Windows/Python 3.12 trong `backend/.venv`; test file
được kiểm lại riêng sau hai sửa đổi cuối, các file không đổi giữ kết quả đã pass.
Từ `backend/`:

```powershell
.\.venv\Scripts\python.exe -m ruff check transfer/v2/download.py transfer/v2/app.py metadata/v2/runtime.py metadata/v2/http.py tests/test_v2_d4.py
.\.venv\Scripts\python.exe -m ruff format --check transfer/v2/download.py transfer/v2/app.py metadata/v2/runtime.py metadata/v2/http.py tests/test_v2_d4.py
```

Chỉ cập nhật docs sau gate, không rerun checks cho documentation. Không chạy full
backend, nguyên D1/D2/D3/C3, frontend lint/build, browser C5, benchmark RSS/TTFB/read
amplification hoặc topology nhiều instance D5. **Mốc C và D chưa hoàn tất**;
tiếp theo D5 read/delete race qua ingress/nhiều instance và mixed-load budgets.

## 7. Evidence D5 — 29/10/2026

Thêm `tests/test_v2_d5.py`, test-only probes `tests/v2_d5_service.py` và override
`deploy/compose.v2-d5-tests.yml`. Tái sử dụng process/ingress/sampler helpers C5,
không chạy browser/C5 gate và không đổi production runtime/schema/proto/frontend.

Tier **focused integration/E2E**. Docker 29.8.2/Python 3.12/PostgreSQL 16, Linux
WSL2, tests container cap 3 GiB. Mỗi ca có 9 PID dịch vụ độc lập (hai API, hai
internal, hai Transfer, hai Storage, một Worker) và Nginx riêng; RF=2/two domains,
chunk 8 MiB, không sticky/retry upstream. API/internal/Transfer audit thực sự tới
cả hai endpoint. TTL/renew read mặc định 30s/10s; fixture đặt RPC deadline 20s và
2 download slots/Transfer. Không coi nhiều process cùng host là HA.

Preflight ban đầu: Docker hoạt động nhưng `service "postgres" is not running`.
Dừng dependent gate theo AGENTS.md; sau khi người dùng cho phép, chạy đúng fixture:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml up -d --wait postgres
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -Atc 'SELECT current_database(),current_user,1'
```

PostgreSQL healthy, psql trả `dfs_v2_test|v2_test|1`. Không reset/delete volumes,
không migrate public/V1. Build source hiện tại rồi chạy từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-d5-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-d5-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d5.py
```

**6 passed (121.23s)**, không skipped/deselected/failure. Sau đó siết assertion
hai ca read/delete để bắt buộc Worker thực sự ghi `READ_LEASE_ACTIVE`, sửa test
reschedule dùng đúng job states READY/RETRY, và bỏ biến local không dùng. Chỉ hai
ca bị thay behavior được chạy lại, sau build image mới:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-d5-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d5.py::test_reader_delete_cleanup_across_instances
```

**2 passed (49.08s)**, không warning/skipped/deselected/failure; không cộng thành
8 ca khác nhau. Ruff check/format **2 files passed** trên native Windows, từ backend:

```powershell
.\.venv\Scripts\python.exe -m ruff check tests/test_v2_d5.py tests/v2_d5_service.py
.\.venv\Scripts\python.exe -m ruff format --check tests/test_v2_d5.py tests/v2_d5_service.py
```

Đã exercise D01–D04: acquire/delete qua process khác; người đọc hợp lệ tiếp tục
sau tombstone và renew 10s thật; expiry không hồi sinh; cleanup giữ charge/bytes
cho tới khi lease kết thúc rồi durable delete cả hai replica. Range/full/empty qua
ingress đúng bytes, fallback khi mất footer, lỗi chunk kế chỉ để prefix verified
và HTTP incomplete. Bốn TCP download 64 MiB với slow replica cùng upload/finalize
vẫn tiến triển; 16 GET vượt slot trả 429 không thêm lease. Buffer/stream high-water
đúng 2/process, sau flow task/buffer/lease drain hết.

TTFB tới block client đầu của full 8 MiB +17 khoảng **59.5–61.7ms**; Range 1 byte
đọc cả chunk 8 MiB, amplification payload **8,388,608×**. Mixed phase RSS sampled
Transfer **104.6/103.9 MiB**, upload/finalize đồng thời **0.844s**, send bị backpressure
khoảng **9.7s** nhưng renew vẫn hoạt động. Định nghĩa phép đo, topology, config,
giới hạn, vị trí raw JSON/log và bảng kết quả ở [D5 evidence](V2_D5_EVIDENCE.md).
Các số đo là fixture đã nóng cache, không phải latency/throughput SLO hoặc physical
disk read amplification. Raw artifacts trong `backend/.runtime/v2-d5/` (gitignored).

Chỉ docs/static formatting sau gates; không rerun checks đã pass. Không chạy full
backend, D1–D4 regressions vì production source không đổi, frontend lint/build,
browser C5, file 1–10 GiB, 16 lượt tải thành công đồng thời hoặc nhiều host/HA.
Mốc **D hoàn tất baseline** với các giới hạn trên; **C5 và mốc C chưa hoàn tất**.
Mốc tiếp theo theo thiết kế là E: mở rộng admission/capacity/fairness và overload.

## 8. Sửa findings review trước E — 29/10/2026

Đã sửa bốn findings về Storage shared-reader, deadline tổng/cancel control HTTP,
duration float qua environment và request ID ở admission 429/503. Thêm regression
permanent, giữ fencing/drain Delete và unknown outcome của mutation. h11 vẫn ở
version 0.16.0 đã khóa, chỉ khai báo thêm direct dependency.

**32 ca mới +44 hồi quy trực tiếp +6 D5 =82 ca Linux riêng biệt pass**; Ruff
check/format bảy file Python pass. D5 mixed test hiện dùng bốn reader **cùng file**
64 MiB/RF=2 trên hai Transfer, không còn bỏ sót giới hạn reader theo số replica.
Xem [fixes, commands và môi trường](V2_PRE_E_FIXES.md) và
[số đo D5 sau sửa](V2_D5_EVIDENCE.md#sau-review-trước-e--29102026).

Native Windows I/O gặp socketpair/Proactor, đã dừng phần I/O native và kiểm đủ
regression trên Docker Linux; không cộng native vào tổng hoặc suy ra native gate
đã pass. Không chạy full suite/frontend/C5/file 1–10 GiB/HA nhiều host. Chỉ cập
nhật comment/docs sau gates, không rerun checks đã pass. Mốc E chưa bắt đầu.
