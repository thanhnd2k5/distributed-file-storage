# Sửa findings review trước E — 29/10/2026

Đã sửa cả bốn findings. Phạm vi là Storage read ownership, control HTTP transport,
runtime config và request tracing; không thay schema/migration/proto hay frontend.
Không bắt đầu mốc E hoặc mở lại gate browser/file lớn C5 trong lượt này.

## Thay đổi và regression

| Finding | Sửa | Evidence mới |
|---|---|---|
| P1: ba reader cùng file/RF=2 làm lượt thứ ba 503 dù còn download slot | Reader counter/idle event theo allocation; đọc dùng chung ownership, writer giữ exclusive slot. Prepare/Delete chặn reader mới và chờ mọi reader cũ trong một drain budget; Delete commit fence trước khi chờ, ACK sau cleanup bền vững | Bốn GET/Range đồng thời cùng physical chunk trên node đầu, body/ETag đúng, bốn lease cùng ACTIVE rồi release. Ba read handles giữ bytes qua Delete fence; đóng một handle chưa cho unlink; writer/replay/Prepare không chạy chồng reader; late read bị fence |
| P2: socket timeout không giới hạn cả exchange; caller cancel để HTTP worker giữ slot | JsonClient dùng asyncio streams + h11, deadline tổng bao connect/TLS/header/body, response ≤1 MiB và header/event incomplete ≤16 KiB. Timeout/cancel abort transport và drain trước khi trả slot. Không retry/pool; mutation cancel vẫn unknown outcome | Header/body gửi nhỏ giọt, body stall và TLS handshake đều bị timeout; cancellation đóng socket, slot dùng được ngay sau drain, POST chỉ được nhận đúng một lần. Kiểm chunked + 100 Continue, 204, error envelope, truncated EOF và response quá lớn |
| P2: duration float trong environment bị parse int | Parse theo kiểu khai báo dataclass field cho Limits và WorkerConfig; giữ float khi truyền Uvicorn graceful timeout | Bảy duration Limits và bốn Worker durations nhận 0.5; nan/inf/zero/negative/bad vẫn bị từ chối, integer counters không nhận 1.5 |
| P3: 429/503 admission thiếu X-Request-ID | Admission được bọc RequestIds; dùng lại ID trong scope và chuẩn hóa đúng một response header qua các wrapper lồng nhau | Normal response, request overload, health overload và closing; GET/HEAD, HEAD không body, ID canonical duy nhất và khớp ID handler |

`h11` đã có trong lock ở **0.16.0**, nay khai báo direct dependency vì client dùng
parser này; không đổi version dependency. Test TLS có self-signed certificate/key
chỉ dành cho fixture local; production vẫn kiểm system trust store và hostname.

## Môi trường và test gate

Tier **focused integration** cộng targeted config/tracing checks. Read-only preflight
Docker Engine **29.8.2**, PostgreSQL 16 fixture healthy; psql trả
`dfs_v2_test|v2_test|1`. Dùng schema/thư mục fixture riêng; không start/reset services,
không migrate public/V1 trong lượt sửa này. Build tests image mới từ source hiện tại:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
```

Image đã chạy các gates Linux:
`sha256:3bb999d7c11358ff0dd00a56590aeb4f9ca733b01ba7cd5a2c8ee349086b6f47`.
Môi trường Docker Linux/Python 3.12 là evidence I/O chính, đúng backend/AGENTS.md.

### Regression mới

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_pre_e.py tests/test_v2_pre_e_integration.py
```

**32 passed (3.33s)**, không skipped/deselected/failure. Hai ca Storage ownership
giữ read handles thật trên filesystem; ca HTTP dùng API/Transfer/PostgreSQL/gRPC
thật. Các ca control HTTP/TLS dùng async TCP peers cục bộ và trust store fixture,
không mock kết quả deadline/cancellation/byte framing.

### Hồi quy trực tiếp

Từ root, cùng image và PostgreSQL fixture:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_streaming.py::test_store_replay_validates_bytes tests/test_v2_streaming.py::test_interrupted_stream_retry tests/test_v2_streaming.py::test_slow_writer_fence_and_other_allocation_progress tests/test_v2_streaming.py::test_actual_grpc_all_seven_rpcs tests/test_v2_c3.py::test_upload_complete_publish_real_http_bytes tests/test_v2_c3.py::test_lost_confirmation_response_reconcile_and_body_replay tests/test_v2_c3.py::test_actual_bytes_fail_then_synchronous_retry_handoff tests/test_v2_c3.py::test_verified_replay_drains_checks_body_without_new_allocations tests/test_v2_c3.py::test_limits_and_control_admission tests/test_v2_c3.py::test_transfer_runtime_sigterm_drain_and_resume tests/test_v2_c3.py::test_error_request_id_and_invalid_confirmation_evidence tests/test_v2_c3.py::test_multiple_grpc_blocks_real_http_hash_and_finalize tests/test_v2_c3.py::test_stream_admission_deadline_disconnect_and_renew_failure tests/test_v2_d3.py::test_internal_read_commands_commit_page_renew_and_release_via_json_client tests/test_v2_d3.py::test_head_status_headers_and_preconditions_without_lease_or_storage tests/test_v2_d4.py::test_blocked_send_stops_and_drains_on_lease_or_request_end tests/test_v2_d4.py::test_control_failures_before_headers_and_best_effort_release tests/test_v2_d4.py::test_client_disconnect_cancels_grpc_and_releases_admission tests/test_v2_d4.py::test_exhausted_replicas_before_and_after_headers
```

**44 passed (43.28s)**, không skipped/deselected/failure. Phạm vi gồm Store replay,
interruption/fence, cả bảy Storage RPC, upload/finalize/unknown confirmation, consumer
retry, SIGTERM, read acquire/manifest/renew/release 204, HEAD và error request ID,
download disconnect/renew stall/expiry/total timeout/shutdown và lỗi sau headers.

### Nhiều process và tải hỗn hợp

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-d5-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d5.py
```

**6 passed (114.64s)**, không skipped/deselected/failure. Mixed test đã đổi thành
bốn reader cùng một file 64 MiB thay vì bốn file riêng, qua hai Transfer và cùng
RF=2. Cả bốn full SHA-256 đúng, mỗi process tối đa hai buffer/stream; renew qua
backpressure vẫn tiến triển. 16 GET vượt slot trả 429 không thêm lease; upload/
finalize vẫn tiến hành; sau flow không còn task/buffer/lease đang giữ.

Lượt này sampled RSS Transfer **100.3/101.8 MiB**, send bị backpressure tối đa
**9.63/9.67s**, upload/finalize đồng thời **0.839s**. Đây là số đo fixture nóng cache,
không phải SLO; xem [bảng cập nhật D5](V2_D5_EVIDENCE.md#sau-review-trước-e--29102026).
Raw report/log dưới `backend/.runtime/v2-d5/` đã được thay bằng lượt hiện tại.

### Native Windows và Ruff

Native collect ban đầu thiếu `v2_storage_pb2`; đã sinh stubs bằng lệnh có sẵn,
không sửa proto:

```powershell
.\.venv\Scripts\python.exe scripts/generate_v2_proto.py
.\.venv\Scripts\python.exe -m pytest -q -x tests/test_v2_pre_e.py
```

Trong sandbox, Windows asyncio treo ngay ở `socket._fallback_socketpair` trước
logic ASGI. Đã lấy traceback, dừng đúng các PID pytest và chạy lại ngoài sandbox.
Lượt đó có **26 passed**, sau đó TCP I/O gặp Proactor `WinError 64/10022`; dừng phần
I/O native, không retry aggregate. Không coi lượt native là gate hoàn tất và không
cộng các ca đó vào tổng Linux. Đã chuyển toàn bộ file sang Linux, nơi 32 ca mới
đều pass. Không có assertion/runtime failure còn mở trên môi trường Linux đã chọn.

Ruff check/format **7 Python files passed** trên Windows trong `backend/.venv`:

```powershell
.\.venv\Scripts\python.exe -m ruff check storage/v2/streaming.py storage/v2/service.py metadata/v2/http.py metadata/v2/runtime.py tests/test_v2_pre_e.py tests/test_v2_pre_e_integration.py tests/test_v2_d5.py
.\.venv\Scripts\python.exe -m ruff format --check storage/v2/streaming.py storage/v2/service.py metadata/v2/http.py metadata/v2/runtime.py tests/test_v2_pre_e.py tests/test_v2_pre_e_integration.py tests/test_v2_d5.py
```

Sau khi thêm timeout cho probe ASGI trong `test_v2_pre_e.py`, chỉ file đó được
Ruff check/format lại và pass; các file không đổi giữ kết quả đã pass. Chỉ sửa
comment/docs sau gates, không rerun checks cho thay đổi không có runtime behavior.

Tổng **82 ca Linux riêng biệt pass**, không cộng probe/native hoặc các lượt lặp.
Không chạy full backend, nguyên D1–D4, frontend lint/build, browser C5, file 1–10 GiB
hoặc HA nhiều host. C5 vẫn tạm gác; bốn findings review đã có fixes và regression.
