# M3 — Upload/download thật với RF=2

**Ngày lập:** 04/10/2026  
**Trạng thái:** P1–P7 hoàn thành (04/10/2026); M3 hoàn thành, bàn giao M4.  
**Đầu vào:** M0–M2 hoàn thành, Storage RPC M1 và registry/health/API M2.  
**Đầu ra:** upload nhiều chunk có RF=2, quan sát file/placement qua REST,
download đúng SHA-256 và vẫn đọc được khi một Storage Node ngừng hoạt động.

## 1. Mục tiêu và ranh giới

Thực hiện M3 trong [kế hoạch tổng](IMPLEMENTATION_PLAN.md), theo
[ARCHITECTURE.md, mục 2–7 và 8](ARCHITECTURE.md),
[API_CONTRACTS.md, mục 1, 2.1–2.4 và 3](API_CONTRACTS.md),
[storage.proto](storage.proto) và [bàn giao M2](M2_IMPLEMENTATION_PLAN.md#bàn-giao-m3--uploaddownload-rf2).

M3 gồm:

- Validation multipart, size thật, tên file; đọc từng chunk và hash toàn file.
- Placement greedy: node enabled ACTIVE, đủ free-space snapshot, ưu tiên khác
  failure domain; RF snapshot vào mỗi file, demo chuẩn RF=2.
- Client data RPC với deadline/retry hữu hạn, xác minh Store ack/Get bytes.
- Upload UPLOADING → AVAILABLE chỉ sau khi mọi chunk đủ RF ack hợp lệ.
- Upload lỗi/cancel trước commit → FAILED, giữ mọi attempted mapping và đặt
  cleanup_pending; tận dụng startup recovery đã có nếu process/DB gián đoạn.
- POST `/api/v1/files`; GET list, detail, chunks và download theo contract.
- Download assemble tempfile, checksum từng chunk/toàn file, fallback replica,
  quản lý tempfile khi lỗi, response kết thúc, disconnect và restart.
- Focused integration và smoke thật cho RF=2, checksum và lỗi một node.

**Ranh giới M4:** DELETE/tombstone flow, worker xử lý pending cleanup, manual
repair/cursor/budget và kiểm tra cleanup hoàn tất qua restart. M3 phải lưu trạng
thái lỗi/pending bền vững nhưng chưa tuyên bố bytes của FAILED đã được dọn.
Startup recovery UPLOADING → FAILED đã có từ M0–M2; M3 kiểm tra nó với upload thật.
UI thuộc M5; demo hai host thật thuộc M6. Không đổi proto/schema/API semantics,
thêm streaming, background repair, auth hoặc distributed transaction.

## 2. Điểm xuất phát thực tế

| Thành phần hiện có | Cách dùng trong M3 |
|---|---|
| `metadata/models.py`, migration `0001_initial.py` | Đã có files/chunks/chunk_replicas/storage_nodes và states; dùng lại, chưa có lý do thêm migration |
| `metadata/bootstrap.py` | Reset registry, disable node vắng config, UPLOADING → FAILED và cleanup flags; mở rộng startup cho download tmp |
| `metadata/main.py` | `app.state` có settings/session_factory/operation_lock/initialized/health_worker; thêm data client, file router và lifecycle tmp |
| `metadata/config.py`, `common/config.py` | Đã có file/chunk/message limits, RF, deadline 5 s, tối đa 2 attempts, download_temp_dir |
| `metadata/worker.py`, `health.py` | Health độc lập theo node; không dùng health để xác nhận replica, không nối cleanup trong M3 |
| `metadata/cluster.py`, `schemas.py`, `routes/cluster.py` | Dùng quy tắc live replica/domain counters hiện có cho file detail/chunks, giữ GET chỉ đọc |
| `metadata/errors.py` | Envelope/validation đã có; bổ sung ánh xạ lỗi nghiệp vụ file đúng API contract |
| Storage M1 + generated stubs | Store atomic/idempotent, Get actual bytes/hash, Delete idempotent; dùng production server factory |
| `tests/conftest.py` | PostgreSQL schema riêng có sẵn; fixture nhiều Storage Node cần subdirectory riêng cho từng node |
| `deploy/compose.local.yml` | PostgreSQL, Metadata một worker, ba node cùng dev_host, volumes riêng; giữ topology |
| Dependency locks | python-multipart, grpcio, SQLAlchemy, pytest/httpx đã có; không thêm framework chỉ để làm M3 |

P1 đã có `metadata/file_input.py`, `metadata/placement.py` và hai targeted test
files tương ứng. P2 đã có `metadata/storage_client.py`, `metadata/operations.py`,
lifespan/readiness wiring và hai focused integration test files. P3 đã có
`metadata/upload.py`, POST trong `metadata/routes/files.py`, FileSummary và
`tests/test_metadata_upload.py`. P4 đã có GET list/detail/chunks,
`metadata/files.py`, helper `metadata/replica_health.py` dùng chung với cluster
và `tests/test_metadata_files.py`. P5 đã có `metadata/download.py`,
`metadata/download_temp.py`, `metadata/download_response.py`, helper ownership
`metadata/transfers.py` và `tests/test_metadata_download.py`. P6 đã có
`scripts/smoke_files.py`, `scripts/smoke_files.ps1`,
`tests/test_metadata_files_lifecycle.py` và `tests/test_smoke_files.py`.
Metadata build M3 đã deploy vào Compose local; P7 đã rà và bàn giao M4.

## 3. Chia phase và thứ tự

| Phase | Nội dung | Phụ thuộc | Gate chuyển bước |
|---|---|---|---|
| P1 | Validation, chunking và placement | M2 | Size/count/hash đúng; chọn RF node khác nhau, ưu tiên domain và loại node không đủ điều kiện |
| P2 | Data RPC client và operation boundary | P1 | Deadline/retry/ack đúng; lock nonblocking; Session/transaction không kéo dài qua RPC |
| P3 | Upload coordinator và POST files | P1–P2 | PENDING trước Store, đủ RF mới 201; failure/cancel giữ attempted mappings và cleanup flags |
| P4 | GET list/detail/chunks | P3 | Đúng schema/filter/counters; GET đọc được khi transfer busy, không RPC |
| P5 | Download assembly và fallback | P2–P4 | Chunk/file integrity trước 200; fallback đúng; lock/tmp được giải phóng mọi nhánh |
| P6 | Lifecycle và smoke RF=2 thật | P1–P5 | Nhiều chunk đủ RF, SHA đúng, tắt một node vẫn đọc; restart giữ file/mapping |
| P7 | Rà DoD, evidence và bàn giao M4 | P6 | Docs đúng code; gate có bằng chứng; ghi rõ cleanup/repair chưa làm |

Làm tuần tự, kiểm tra hành vi trong phase sở hữu nó. Phân bổ dự kiến 3 buổi
tập trung theo ước lượng M3 trong kế hoạch tổng: buổi đầu P1–P2, buổi hai P3–P4,
buổi ba P5–P7. Fault injection/lifecycle có thể cần buffer; không cam kết ngày
hoàn thành. Không chuyển phase khi gate còn thiếu hoặc chỉ có lint/build.

## 4. P1 — Validation, chunking và placement

### Đầu việc

1. Tách logic file input/chunking có thể kiểm tra không cần DB/gRPC.
   Multipart phải có đúng một field file bắt buộc, reject nhiều file và input
   sai theo contract; client không được chọn RF/chunk size bằng request.
2. Chuẩn hóa basename cả slash/backslash, loại control characters, validate
   tên cuối 1–255 ký tự. Tên không dùng làm storage path. Content type chỉ là
   hint, có default hợp lệ và giới hạn phù hợp cột hiện có.
3. Đo size thật từ spool bằng thao tác file có giới hạn; không tin Content-Length.
   Vượt MAX_FILE_SIZE_BYTES trả 413 trước file/chunk rows và Store RPC. Đưa con
   trỏ về đầu trước chunking; đóng UploadFile cả khi validation thất bại.
4. Iterator đọc tối đa chunk_size mỗi lượt, index liên tiếp từ 0, chunk cuối
   có thể ngắn; không tạo chunk rỗng cuối. SHA-256 từng chunk và toàn file lấy
   từ bytes thật; size/count đối chiếu khi hoàn tất. File rỗng có 0 chunk và
   SHA-256 của bytes rỗng, không cần Storage Node.
5. Placement nhận node snapshots và allocation của thao tác. Lọc enabled,
   ACTIVE, available_bytes đủ chunk; metrics null không được coi là đủ disk.
   ACTIVE là kết quả health identity/writable hợp lệ; schema chưa có cột writable.
6. Với từng replica: loại node đã có replica đúng của chunk và node đã lỗi trong
   thao tác; ưu tiên domain chưa dùng, rồi chọn tuple
   `(allocated_bytes_this_operation, used_bytes, node_id)` nhỏ nhất. Allocation
   tăng sau placement thành công; node khác nhau cho cùng chunk. Khi chỉ có một
   domain vẫn cho đủ RF trên node khác nhau và log fallback cùng domain.
7. Free-space là snapshot, không reservation; Store có thể báo đầy. Khi không
   đủ destination phải fail hữu hạn; không vòng lặp chọn lại node đã lỗi.

### Kiểm tra và gate

**Tier targeted**, dự kiến `tests/test_metadata_placement.py` và
`tests/test_metadata_file_input.py`: size 0, 1, C−1, C, C+1, 2C, MAX, MAX+1;
hash/count/last chunk đúng; tên path/control/Unicode; không read toàn-file.
Placement kiểm tra ba node một domain, A/B/B, tie-break, RF khác nhau, disabled/
SUSPECTED/DOWN, free null/thiếu, node đã chọn/đã lỗi, thiếu node và allocation
qua nhiều chunk. Không cần Docker cho logic thuần. Validation HTTP thật thuộc P3.

**Gate:** input/placement có kết quả xác định và đúng contracts; không thêm
migration hoặc sửa Storage để phục vụ thuật toán placement.

## 5. P2 — Data RPC client và operation boundary

### Đầu việc

1. Dự kiến `metadata/storage_client.py`: quản lý channel/stub theo node cấu hình,
   dùng settings.grpc_options() ở client, không truy cập private fields của
   health worker. Health giữ lifecycle và deadline riêng.
2. Mỗi Store/Get dùng CHUNK_RPC_TIMEOUT_SECONDS; RPC_MAX_ATTEMPTS gồm lần đầu,
   tối đa 2. Chỉ retry UNAVAILABLE/DEADLINE_EXCEEDED, một backoff 0.2 s; cùng
   chunk_id/payload trên Store retry, không bật gRPC retry chồng wrapper.
3. Store ack phải đúng chunk_id/size/checksum, kể cả already_existed=true;
   response sai không được VERIFIED. Get kiểm tra chunk_id, len(data), hash
   thực, hash response và hash DB; không tin checksum field node một mình.
4. Giữ phân loại lỗi gRPC theo API_CONTRACTS: NOT_FOUND → MISSING, DATA_LOSS/
   bytes không đúng → CORRUPTED; timeout/unreachable không phải MISSING.
   RESOURCE_EXHAUSTED/FAILED_PRECONDITION/conflict không retry cùng destination.
   CANCELLED dừng thao tác; INTERNAL log/fallback hữu hạn theo flow.
5. Lấy operation_lock bằng acquire nonblocking; busy trả 409. Giữ lock trong
   upload và chuẩn bị download; release bằng finally. GET và health vẫn chạy.
   Data RPC/DB/filesystem blocking chạy trong threadpool, không trong event loop.
6. Session riêng cho từng thao tác/thread, transaction ngắn trước/sau RPC. Đọc
   snapshots thành DTO/value trước khi đóng transaction; tránh ORM lazy-load
   mở transaction trong đoạn mạng. Không giữ Session có transaction active qua RPC.
7. Khởi tạo/đóng data channels theo lifespan; shutdown phải chờ hoặc hủy đúng
   các thao tác đang dùng client trước khi dispose DB/channels. Không tạo task
   hoặc network side effect lúc import.

### Kiểm tra và gate

**Tier focused integration**, dự kiến `tests/test_metadata_storage_client.py`
trong Docker Linux: production Storage server với dirs riêng, transient fail,
ack loss/idempotent retry, ack sai, Get size/hash/id sai, nonretryable errors,
deadline/attempt count hữu hạn và đóng channel. Fake/fault service chỉ dùng ở
điểm inject lỗi; baseline Store/Get phải qua production factory.

Lock/session/lifespan được kiểm tra thêm trong P3/P5 khi có flow thật: không
thay bằng test chỉ gọi acquire/release. Không cần live Storage containers cho
in-process gRPC; chỉ cần Docker để tránh loopback issue native Windows đã biết.

**Gate:** wrapper phân loại outcome đúng; không biến timeout thành bằng chứng
chunk chưa ghi, không lấy health success thay data integrity.

## 6. P3 — Upload coordinator và POST files

### Luồng triển khai

1. Validate multipart/name/actual spool size trước ghi metadata. Lấy lock;
   kiểm tra startup/DB. File không rỗng thiếu eligible node cho RF trả 503
   INSUFFICIENT_NODES trước khi tạo file; file rỗng không phụ thuộc node ACTIVE.
2. Transaction tạo File UPLOADING, UUID mới, snapshot chunk_size/RF, size/count
   dự kiến từ spool; checksum nullable. POST cùng tên/bytes vẫn tạo ID khác.
3. Đọc từng chunk. Transaction tạo Chunk và attempted replica PENDING **commit
   trước Store**; destination thay thế cũng phải có mapping trước RPC. Không
   dựa vào transaction chưa commit để bảo vệ crash window.
4. Store lần lượt; ack đúng mới cập nhật VERIFIED/last_verified_at. Node RPC
   vừa lỗi bị loại trong operation hiện tại, không sửa health detector từ data
   RPC hoặc chờ poll mới để quyết định thử destination khác.
5. Tiếp tục đến RF ack đúng trên node khác nhau cho từng chunk. Mapping timeout
   hoặc ack sai giữ PENDING/last_error; không xóa nó khi destination khác thành
   công. File thành công có thể còn attempted PENDING, cần download/repair
   xác minh sau; không đặt cleanup_pending cho replica của AVAILABLE chỉ vì
   ack chưa biết. Replica trở lại có thể làm dư RF; V1 không tự prune.
6. Transaction cuối đối chiếu count/index/size/hash và RF acknowledged của
   mọi chunk, rồi AVAILABLE. RF commit dựa trên Store ack hợp lệ của thao tác,
   không yêu cầu mọi node vẫn ACTIVE tại đúng thời điểm commit; health có thể
   thay đổi ngay sau ack. HTTP 201 FileSummary + Location chỉ sau commit.
7. Khi lỗi/cancel trước commit: stop bắt đầu RPC mới, chờ outcome hữu hạn của
   call đang có, transaction FAILED/error_code + cleanup_pending cho mọi
   attempted replica. Trả envelope có file_id nếu đã tạo; thiếu RF dùng
   UPLOAD_REPLICATION_FAILED. Cleanup thực tế thuộc M4.
8. Nếu DB mất nên chưa lưu được FAILED/ack, giữ mapping đã commit trước RPC,
   không trả 201; trả METADATA_UNAVAILABLE khi còn gửi response được. Startup
   recovery sẽ đánh dấu UPLOADING còn lại. Không hứa transaction thành công
   khi DB đang unavailable. Lỗi sau commit không được đổi AVAILABLE thành FAILED.
9. Đóng spool và release lock mọi nhánh. Xác định rõ cách nhận cancellation/
   disconnect giữa các chunk và khi await threadpool; không coi hủy coroutine
   là bằng chứng thread/RPC đã dừng. Không chạy cleanup song song Store còn sống.
   Nếu client mất response sau commit, file giữ AVAILABLE.

### Kiểm tra và gate

**Tier focused integration**, dự kiến `tests/test_metadata_upload.py`, qua
FastAPI multipart + PostgreSQL schema riêng + ba production Storage Nodes:

- Empty/single/multi/exact-boundary/max file đúng count/hash/RF/Location;
  thiếu field, nhiều file, UUID/schema validation và file quá lớn đúng envelope.
- Mỗi Store entry quan sát được PENDING đã commit từ Session khác; mỗi RPC
  entry kiểm tra không còn DB transaction của coordinator đang mở.
- Ack loss retry cùng ID; node đầy/lỗi thử node khác; không đủ RF → FAILED;
  mọi attempted mapping còn, flags đúng và list mặc định không public FAILED.
- Ack sai không counted, final commit không chạy trước chunk cuối đủ RF;
  DB lỗi giữa Store/ack commit giữ recovery path, không báo thành công giả.
- Upload cạnh tranh/upload với download chuẩn bị (bổ sung P5) trả 409; health
  và GET nodes/cluster vẫn đáp ứng trong transfer; lỗi không để lock kẹt.
- Cancellation trước commit giữ FAILED/pending; response loss sau commit
  giữ AVAILABLE. Process crash trước commit kiểm tra thật ở P6.

**Gate:** POST chạy được end-to-end RF=2 và failure persistence đúng; chưa có
worker dọn replica không được diễn giải thành cleanup đã hoàn tất.

## 7. P4 — GET list, detail và chunks

### Đầu việc

- Dự kiến `metadata/files.py`, `metadata/routes/files.py`; mở rộng schemas
  với FileSummary/FileList/FileDetail/ChunkList, UTC Z, nullable checksum/errors.
- List limit 1..100, offset >=0; default AVAILABLE; include_inactive thêm
  UPLOADING/FAILED/DELETING, vẫn ẩn DELETED. Order created_at DESC/file_id DESC;
  total cùng filter, không tính replica statistics cho từng list item.
- Detail/chunks không tồn tại hoặc DELETED trả 404 FILE_NOT_FOUND; trạng thái
  khác vẫn quan sát được. Detail có đầy đủ FileSummary và counters/error_code.
- Chunks order chunk_index ASC; replica list giữ mapping DOWN/disabled/PENDING/
  DELETED, ổn định thứ tự node_id để quan sát. Không thêm persisted chunk state.
- Live chỉ VERIFIED + enabled ACTIVE + không cleanup_pending; RF của file,
  domain target min(file RF, số domain enabled), over_replicated khi live > RF.
  FAILED/DELETING → INACTIVE; UPLOADING → PENDING. known_readable chỉ true cho
  AVAILABLE có mỗi chunk >=1 live; empty AVAILABLE là true.
- Dùng chung quy tắc counters với cluster hiện có, tránh hai phiên bản tính
  khác nhau. Query bounded, không N+1 từng replica/chunk; response một snapshot
  nhất quán. GET không acquire operation_lock, không gọi Storage RPC.

### Kiểm tra và gate

**Tier focused integration**, dự kiến `tests/test_metadata_files.py`: HTTP/DB
shape, validation, sorting/pagination/status visibility, RF snapshot, null/UTC,
empty file, mixed replica states, disabled history, domain degradation và dư RF.
Đối chiếu detail/chunks/cluster cùng fixture; GET vẫn chạy khi lock busy.
Rerun regression cluster chỉ khi chia sẻ/sửa query logic của M2.

**Gate:** API quan sát đúng dữ liệu upload và lỗi; cached known_readable/counters
không được diễn giải thành đã đọc/verify bytes trong request GET.

## 8. P5 — Download assemble, fallback và tempfile lifecycle

### Luồng triển khai

1. Lấy lock, đọc file/mapping snapshot và đóng transaction. AVAILABLE mới đọc;
   UPLOADING/FAILED → FILE_NOT_READY, DELETING → FILE_DELETING, DELETED/missing
   → FILE_NOT_FOUND. Kiểm tra index liên tiếp/count/size để tránh assemble thiếu.
2. Tạo tempfile riêng trong DOWNLOAD_TEMP_DIR. Chỉ dùng file do Metadata quản
   lý; startup dọn stale download files theo naming convention trong thư mục
   riêng, không recursive delete một path tính toán hoặc thư mục không thuộc app.
3. Get từng chunk theo index: ACTIVE trước, SUSPECTED sau, enabled DOWN fallback
   cuối; disabled/node vắng config không gọi. Bỏ replica DELETED/cleanup_pending;
   không chỉ đọc cached VERIFIED. PENDING/MISSING/CORRUPTED có thể probe lại
   theo mapping để tìm bytes đúng, không tự overwrite hoặc repair lúc download.
4. Xác minh ID/size/hash như P2; đúng mới VERIFIED/last_verified_at. NOT_FOUND
   → MISSING; DATA_LOSS/checksum/bytes sai → CORRUPTED; timeout/unreachable chỉ
   ghi last_error, không MISSING. Persist qua transaction ngắn dưới lock.
5. Fallback hữu hạn qua known replicas, giữ RAM O(chunk size). Ghi chunk hợp
   lệ đúng thứ tự vào tmp và cập nhật hash toàn file; hết source trả 503
   CHUNK_UNAVAILABLE với file_id/chunk_index, xóa tmp, chưa gửi binary 200.
6. Kiểm tra full size/hash: sai → INTEGRITY_CHECK_FAILED. Disk/probe/write
   lỗi ở Metadata → TEMP_STORAGE_UNAVAILABLE. Không gửi file thiếu, không đổi
   file AVAILABLE sang FAILED do một lượt download lỗi.
7. Hoàn tất tempfile rồi release lock, sau đó trả HTTP binary response từ disk:
   application/octet-stream, Content-Length, safe filename*=UTF-8'',
   X-File-Checksum-SHA256. Không read toàn tempfile vào RAM; file rỗng vẫn 200.
8. Response sở hữu tempfile sau bàn giao. Xóa sau success, send error hoặc
   disconnect bằng finalization được kiểm tra thật; không chỉ giả định callback
   background luôn chạy khi response bị hủy. Crash để lại tmp được startup dọn.
   Lock không giữ suốt HTTP send; file snapshot đang gửi độc lập storage gốc.

### Kiểm tra và gate

**Tier focused integration**, dự kiến `tests/test_metadata_download.py`:
normal/empty/multi download SHA/size/headers đúng; Unicode/path-safe filename;
fallback timeout, NOT_FOUND, DATA_LOSS, corrupted bytes/hash/id; PENDING probe
thành VERIFIED; DOWN fallback và disabled không gọi; cuối file hết replica
trả JSON 503 trước response file; whole-file checksum sai trả integrity error.
Fault injection disk/full/write lỗi, DB lỗi, cạnh tranh lock và tmp cleanup.

Dùng ASGI send/disconnect harness hoặc HTTP server thật ở case cancellation
để chứng minh finalizer; TestClient nhận trọn body không đủ evidence disconnect.
Giữ regression health/cluster liên quan nếu lifecycle/query bị thay đổi.

**Gate:** mọi chunk và file checksum đúng trước 200; lỗi chunk cuối không lọt
partial response; không rò tempfile hoặc giữ lock sau lỗi/cancel/send kết thúc.

## 9. P6 — Lifecycle và smoke RF=2 thật

### Điều kiện và kiểm tra

Integration dùng Docker Linux, PostgreSQL schema riêng, ba production Storage
Nodes có dirs riêng. Lifecycle dùng Metadata child process/HTTP server thật và
manifest lưu file/chunk IDs fixture. Không sửa/xóa dữ liệu ngoài fixture.

1. File 10–20 MiB: POST 201, mỗi chunk có ít nhất hai VERIFIED ack trên node
   khác nhau, indexes/last size/full hash đúng; happy path không inject lỗi thì
   đúng hai replica/chunk. Download bytes/SHA trùng nguồn; list/detail/chunks/
   cluster nhất quán. Chứng minh local placement dùng nhiều node/volumes riêng.
2. Fixture A/B/B xác minh placement khác domain bằng config của test servers.
   Đây là evidence policy, không thay evidence hai host thật M6. Compose local
   vẫn dùng dev_host, chỉ chứng minh chịu lỗi process.
3. Stop một node đang chứa replica của fixture; download vẫn đủ bytes/hash,
   counters phản ánh health rồi recovery. Khôi phục node và chờ ACTIVE trong
   finally; không sửa last_success_at/status bằng SQL để giả lập smoke thật.
4. Corrupt/xóa một chunk fixture trong isolated Storage dir, download fallback
   đúng và ghi CORRUPTED/MISSING. Hết tất cả source của chunk cuối trả 503 JSON.
   Không corrupt volume live chứa dữ liệu người dùng.
5. Kill Metadata sau PENDING commit trước upload commit: process mới recovery
   FAILED/cleanup_pending, giữ attempted mappings/Storage bytes; file không
   download được. Kill sau AVAILABLE commit trước response: restart vẫn list/
   download đúng. Dùng synchronization có giới hạn, không sleep đoán crash window.
6. Restart Metadata bình thường: AVAILABLE/hash/mapping/RF không đổi; node reset
   DOWN rồi ACTIVE qua health; tmp stale được dọn, download lại SHA đúng. Không
   cần rerun PostgreSQL volume lifecycle M2 nếu không sửa persistence/config đó.

### Smoke trên Compose hiện có

Rebuild runtime hiện tại cho Metadata và tests; chỉ cập nhật Storage nếu
code/config Storage thực sự đổi. `scripts/smoke_files.py` và `smoke_files.ps1`
đã triển khai: manifest, guards không phụ thuộc Python assert dưới -O, request
timeout 300 s. Node bị stop được restore và chờ ACTIVE trong finally; live file
giữ manifest vì chưa có DELETE. Tham số `-Manifest` tiếp tục fixture đã tạo,
không upload thêm khi sửa một smoke check.

Smoke chạy upload multi-chunk → REST placement → download SHA → stop một node
→ fallback download → restore ACTIVE → restart Metadata → download lại. Trước
khi chạy phải ghi rõ node bị stop, API URL và fixture được tạo. Đây là thay đổi
lifecycle có chủ đích; P6 đã được yêu cầu và đã chạy, xem evidence bên dưới.

HTTP client/proxy demo dành tối thiểu 300 s cho upload/download theo architecture;
RPC vẫn deadline 5 s. Kiểm tra timeout đúng tầng khi viết smoke: Uvicorn
`--timeout-keep-alive 300` hiện có chỉ quản lý keep-alive, không phải deadline
xử lý một request upload/download. Không dùng giá trị đó làm evidence request
có đủ thời gian xử lý.

M3 chưa có DELETE: fixture live phải lưu manifest/ID và ghi rõ dữ liệu còn lại
để xử lý qua M4; không raw-delete DB rows trước khi dọn bytes hoặc giả lập endpoint
DELETE. Prefer isolated fixtures cho fault/crash checks; smoke live giữ số file
và bytes nhỏ, không lẫn với dữ liệu người dùng. CLI/modes smoke cụ thể chỉ được
ghi thành lệnh chạy sau khi script đã triển khai và kiểm tra.

**Gate:** evidence có lệnh, môi trường/build, IDs/hashes, replica mapping và
down/recovery thật. In-process integration không thay bằng chứng live Compose;
lint/build không thay bằng chứng checksum/fallback/persistence.

## 10. Commands và test gate khi triển khai

Lượt lập kế hoạch ban đầu là **no execution**; P1 đã chạy targeted tests, P2–P6 đã
chạy focused integration và lint/format, xem evidence bên dưới. Các command
dưới đây dành cho phase implementation; P1–P6 test files đã có.
Chạy từ thư mục ghi rõ. Trước mỗi check nêu tier, command
và lý do; sau check ghi pass/fail/skipped/deselected, môi trường và phần không chạy.

### P1 targeted — từ backend/

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_metadata_file_input.py tests/test_metadata_placement.py
```

Ruff check/format dùng `.venv` và liệt kê **các path thật đã thay đổi** của phase,
không chạy cả repo chỉ vì đã chuyển phase. Không cần build frontend.

### Preflight integration — từ repo root

```powershell
docker info
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
```

P2 storage-client tests cần Docker và tests image hiện tại, không cần live
Storage/Postgres. P2 operation/lifespan tests và P3–P6 HTTP/DB/lifecycle tests
cần postgres healthy và TEST_DATABASE_URL từ tests
service; schema riêng do fixture tạo. Live smoke cần Metadata/Postgres/ba Storage
Node dùng build/config liên quan hiện tại, ready và baseline ACTIVE.

Thiếu dependency: dừng check phụ thuộc, báo đúng blocker. Không start/restart/
migrate/reset data trong task test-only. Recovery command có sẵn (chỉ chạy khi
setup/environment repair đã được cho phép):

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait
```

Nếu code/dependency/config trong tests image thay đổi, rebuild trước check:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
```

### Focused integration — từ repo root

```powershell
# P2: data RPC + operation/HTTP/DB/lifespan, cần PostgreSQL healthy; Storage tạo trong process
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_storage_client.py tests/test_metadata_operations.py
# P3: multipart → DB → production Storage RPC
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_upload.py
# P4: file read APIs và counters
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_files.py
# P5: download integrity/fallback/response lifecycle
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_download.py
# P6: Metadata process crash/restart và fixture persistence
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_files_lifecycle.py
```

Chỉ thêm regression test file/name trực tiếp bị ảnh hưởng. Không chạy full suite
do kết thúc M3/P7; full validation chỉ khi người dùng yêu cầu merge/release hoặc
có thay đổi cross-cutting thực sự theo [AGENTS.md](../AGENTS.md). Passing checks
không rerun nếu code/dependency/config/environment không đổi và không có failure
mới. Shared-infrastructure failure dừng tại lỗi đầu có thể hành động, không retry
aggregate suite. PostgreSQL tests skipped vì thiếu URL không phải database pass.

## 11. Definition of Done và evidence

- [x] P1: validation/chunking/placement có targeted evidence, RF/domain policy đúng (04/10/2026).
- [x] P2: data RPC deadline/retry/ack/Get verification, operation boundary và lifecycle qua focused gate (04/10/2026).
- [x] P3: POST multi-chunk RF=2, PENDING trước RPC, AVAILABLE trước 201;
  failed/cancel/DB interruption giữ recovery/cleanup flags (04/10/2026).
- [x] P4: list/detail/chunks đúng contract, GET không lock/RPC và counters thống nhất (04/10/2026).
- [x] P5: download verify trước 200, fallback đúng, lỗi cuối file rõ, tmp/lock sạch (04/10/2026).
- [x] P6: production boundary/lifecycle và Compose smoke thật có hashes/mappings;
  stop một node vẫn đọc; restart giữ file và phục hồi health (04/10/2026).
- [x] P7: README/plan/evidence phản ánh code; ranh giới M4 và fixture còn lại rõ (04/10/2026).

Mỗi phase cập nhật evidence tại đây: ngày, thay đổi, command đã chạy, môi trường
native/Docker/Compose, kết quả counts/skips, build đã deploy hay chỉ tests image,
fixture đã dọn hoặc manifest còn lại, hạn chế và bước tiếp. Không cộng counts
phase có regression trùng nhau thành một full-suite run. P1 đã qua targeted
gate, P2–P6 đã qua focused integration và P6 có live Compose smoke; P7 đã rà
source/contracts/evidence và chốt bàn giao. M3 hoàn thành trong phạm vi milestone;
DELETE/cleanup/repair, UI và demo hai host vẫn thuộc M4–M6.

### Bằng chứng P1 — 04/10/2026

Đã triển khai:

- `backend/metadata/file_input.py`: `prepare_upload` nhận parsed multipart qua
  `multi_items()` để không mất duplicate fields; chỉ nhận một UploadFile ở field
  file. Chuẩn hóa basename slash/backslash/control/Unicode và content-type hint,
  đo actual spool size bằng seek/tell, rewind; reject vượt limit trước consumer.
  Context đóng mọi spool đã nhận, kể cả duplicate/extra parts và nhánh lỗi.
- `ChunkReader` đọc có giới hạn, ghép short reads thành chunk đúng kích thước,
  index từ 0, SHA-256 chunk/toàn file. Summary chỉ có sau EOF và đối chiếu size;
  file rỗng 0 chunk, không có chunk rỗng cuối, lỗi I/O được sanitize.
- `backend/metadata/placement.py`: immutable node snapshots, chọn một destination
  thiếu RF, ưu tiên domain chưa dùng rồi allocation/used_bytes/node_id. Chỉ chọn
  enabled ACTIVE với metrics đủ; loại confirmed replica nodes và failed nodes
  trong operation, fallback cùng domain có warning, thiếu destination trả None.
  Không mutate snapshots/allocation hoặc tăng allocation trước Store ack.
- `backend/tests/test_metadata_file_input.py`, `test_metadata_placement.py`:
  106 cases về input/spool/chunk boundaries và placement. Tests size/hash chạy
  cả file MAX=64 MiB, reject MAX+1; có spool disk-backed, read-size guard, short
  reads, thay đổi size, partial consumption, I/O errors và closure khi lỗi.
  Policy checks có RF=1/2/3, A/B/B, cùng domain, tie-break theo mọi thứ tự node,
  allocation qua nhiều chunk, null metrics, failed/disabled/SUSPECTED/DOWN và
  thiếu node. Simulated successful acks chỉ chứng minh selection policy.

Tier **targeted**, từ `backend/`, native Windows với Python 3.12.14 `.venv`:

```powershell
.\.venv\Scripts\python.exe -m ruff format metadata/file_input.py metadata/placement.py tests/test_metadata_file_input.py tests/test_metadata_placement.py
.\.venv\Scripts\python.exe -m pytest -q tests/test_metadata_file_input.py tests/test_metadata_placement.py
.\.venv\Scripts\python.exe -m ruff check metadata/file_input.py metadata/placement.py tests/test_metadata_file_input.py tests/test_metadata_placement.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/file_input.py metadata/placement.py tests/test_metadata_file_input.py tests/test_metadata_placement.py
```

Kết quả: **106 passed**, không skipped/deselected; Ruff check passed và 4 files
formatted. Có một PytestCacheWarning: sandbox từ chối tạo/ghi cache dưới
`backend/.pytest_cache`; không phải assertion failure. Không rerun passing checks.

Không chạy Docker/preflight/DB/gRPC/HTTP integration, full suite, smoke, build,
deploy/restart services hoặc frontend. Chưa sửa schema/proto/dependencies,
main/lifespan/health worker và chưa nối router. Multipart validation ở đây dùng
parsed FormData/UploadFile; HTTP parsing/error envelope/cancellation thực được
nối và kiểm tra ở P3. Closure trong context chưa là evidence disconnect HTTP.
Chưa có evidence replication, download/failover hoặc cleanup thật.

Bàn giao: P2 triển khai data RPC client/operation boundary. P3 giữ toàn bộ upload
consumer trong scope `prepare_upload`, dùng size đo thật làm expected size của
ChunkReader và chỉ commit từ summary sau EOF. Coordinator chụp PlacementNode
trong transaction ngắn, lặp select_destination cho RF của file; chỉ thêm vào
verified_replicas và tăng allocated_bytes sau ack đúng, failed_node_ids giữ
suốt operation. None khi chưa đạt RF phải dẫn tới failure path, không commit giả.

### Bằng chứng P2 — 04/10/2026

Đã triển khai:

- `backend/metadata/storage_client.py`: data channels/stubs riêng theo config,
  IPv6 target đúng, send/receive limit và tắt automatic retry/proxy từ settings.
  Constructor không mở channel; start/close theo lifespan, close idempotent,
  ngừng nhận call mới và drain call đã nhận trước khi đóng channels.
- Store/Get unary có deadline từng attempt, tối đa RPC_MAX_ATTEMPTS=2, chỉ retry
  UNAVAILABLE/DEADLINE_EXCEEDED với backoff 0.2 s. Store retry dùng cùng request
  bytes/ID/hash; ack phải khớp ID/size/hash cả khi already_existed=true. Get đối
  chiếu ID/len/hash actual bytes, checksum response và expected checksum metadata.
  Không dùng current default chunk size để reject chunk cũ lớn hơn khi node và
  message limit vẫn hỗ trợ.
- `StorageRpcError` giữ method/node/status/attempts/reason đã sanitize. Get
  NOT_FOUND → MISSING; DATA_LOSS hoặc invalid bytes/response → CORRUPTED; Store
  lỗi và timeout/unreachable không suy ra MISSING. CANCELLED dừng retry; cancel
  token có thể hủy unary future đang chờ hoặc ngừng trong backoff. Coordinator
  P3/P5 chịu trách nhiệm persist states/fallback, wrapper không tự sửa DB/health.
- `backend/metadata/operations.py`: admission nonblocking, OperationError →
  envelope 409 OPERATION_BUSY hoặc 503 METADATA_UNAVAILABLE qua handler hiện có.
  Scope giữ lock đến hết DB/RPC work, Sessions riêng/transaction ngắn có DB
  statement timeout 5 s; chặn nested transaction/RPC trong transaction và dùng
  operation ngoài owning thread/scope. Snapshot phải được chụp trong transaction,
  không giữ lazy ORM objects qua RPC.
- `main.py`: khởi tạo data client sau health worker, readiness kiểm tra client
  còn running. Startup client lỗi dừng worker và đóng client; shutdown đóng
  admission, đợi operation hoàn tất cả DB work rồi đóng data channels, dừng
  health worker trước dispose engine. GET snapshots và health không lấy data lock.

Tier **focused integration**, Docker Desktop Linux + Python **3.12.15**, production
Storage factory với directories riêng và PostgreSQL isolated-schema fixture.
Read-only `docker info`/Compose `ps` qua; PostgreSQL healthy. Chỉ rebuild tests
image; image cuối `sha256:d8714a1f6af1b312355238e9e5402b7321ffb39bb877687afdcdfdb9285c6b85`.

Từ repo root, commands đã chạy:

```powershell
docker info
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_storage_client.py tests/test_metadata_operations.py
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_base.py::test_metadata_db_down_still_live_but_not_ready tests/test_bootstrap.py::test_app_startup_holds_operation_lock_and_gates_readiness tests/test_metadata_worker.py::test_lifespans_start_fresh_worker_and_stop_before_engine_disposal tests/test_metadata_worker.py::test_partial_worker_start_failure_cleans_resources_and_keeps_not_ready tests/test_metadata_worker.py::test_all_storage_down_still_ready_after_successful_initialization tests/test_metadata_worker.py::test_dead_scheduler_keeps_live_and_snapshots_but_not_ready tests/test_metadata_worker.py::test_stopped_worker_is_not_ready_but_still_live tests/test_metadata_worker.py::test_health_ignores_operation_lock_and_preserves_replica_rows tests/test_metadata_cluster.py::test_gets_do_not_acquire_busy_lock_or_call_health_rpc
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python --version
docker image inspect dfs-python-tests:dev --format '{{.Id}}'
```

Kết quả: **60 P2 tests passed + 10 selected regressions passed**, không skipped/
deselected. Hai lượt là focused checks, không phải full-suite run. Có warning
Starlette/TestClient deprecation về httpx trong dependency lock hiện có; không
thay dependency để xử lý warning ngoài scope.

Native Windows `.venv` dùng Ruff cho 6 Python files thay đổi (từ backend/):

```powershell
.\.venv\Scripts\python.exe -m ruff format metadata/storage_client.py metadata/operations.py metadata/main.py metadata/errors.py tests/test_metadata_storage_client.py tests/test_metadata_operations.py
.\.venv\Scripts\python.exe -m ruff check metadata/storage_client.py metadata/operations.py metadata/main.py metadata/errors.py tests/test_metadata_storage_client.py tests/test_metadata_operations.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/storage_client.py metadata/operations.py metadata/main.py metadata/errors.py tests/test_metadata_storage_client.py tests/test_metadata_operations.py
```

Lint lần đầu phát hiện một unused import trong test; đã bỏ, rebuild tests image
vì source thay đổi trước pytest và lint lại qua. Format-check 6 files qua; sau
repair import chỉ check lại format file đó, qua. Không rerun passing tests.

Evidence gồm: Store/Get 2 MiB production thật, duplicate/idempotency và node
directory isolation; actual Store commit rồi mất ack/deadline → retry xác nhận
already_existed; retry budgets/nonretryable codes; ack sai kể cả duplicate;
corrupt actual bytes, dishonest response hash/ID/size; actual deadlines,
cancellation và channel draining. Operation tests qua real gRPC/PostgreSQL và
test-only HTTP route: transaction đã đóng trước RPC, transaction sau RPC dùng
Session mới, lock busy trả 409 đúng envelope, health/GET vẫn chạy, shutdown
đợi final DB work trước channels/engine closure; startup failures và fresh
lifespan/readiness có checks thật.

Không deploy/restart live Metadata/Storage/PostgreSQL, chạy migrations, schema
drift check, smoke, full suite, P1 tests hoặc frontend checks. Không sửa proto,
models/migration, dependency lock, topology hoặc Storage implementation. Live
services vẫn dùng build trước P2. Test-only route không phải POST /files thật;
chưa có upload commit/FAILED cleanup flags hoặc download assembly/fallback.
Session/lock/cancel semantics còn phải được kiểm tra trong flow P3/P5, không
coi client roundtrip là evidence replication RF=2 hoặc failover Metadata.

Bàn giao P3: dùng `app.state.storage_client` qua `data_operation` trong một
threadpool invocation; persist UPLOADING/chunk/attempted PENDING trong transaction
**trước** gọi Store. Chỉ VERIFIED/allocation sau StoreAck đúng; timeout/ack sai
giữ mapping để recovery/cleanup. Truyền cancel token từ flow khi phát hiện hủy,
đợi thread kết thúc trước nhả lock/đóng upload spool; mất response sau AVAILABLE
không được đổi file thành FAILED. P5 dùng expected chunk size/hash snapshot từ
DB, persist Get observations trong transaction riêng sau RPC và dựng tmp trước 200.

### Bằng chứng P3 — 04/10/2026

Đã triển khai:

- `backend/metadata/upload.py`: UUID mới mỗi POST, snapshot size/chunk size/RF;
  UPLOADING và từng attempted PENDING commit trước Store. Ack đúng mới VERIFIED,
  allocation chỉ tăng sau ack; node lỗi bị loại trong operation, fallback
  tiếp tục đến RF. Commit cuối đối chiếu chunk count/index/size/hash và RF;
  không yêu cầu health còn ACTIVE sau acknowledgement.
- Upload lỗi giữ mapping, FAILED/error_code và cleanup_pending cho mọi attempted
  replica. Timeout/ack sai còn PENDING; upload thành công qua fallback giữ
  attempted PENDING mà không đặt cleanup flag. DB lỗi trả METADATA_UNAVAILABLE;
  nếu không ghi được FAILED, startup recovery giữ đường phục hồi từ UPLOADING.
  Guard cập nhật FAILED chỉ áp dụng UPLOADING, không đổi AVAILABLE khi mất ack
  của commit cuối hoặc mất HTTP response.
- `backend/metadata/routes/files.py`: multipart đúng một field file, actual
  spool size, 201 FileSummary/UTC Z và Location sau commit; requestBody trong
  OpenAPI. Parser kiểm tra multipart kết thúc đầy đủ và đóng cả unfinished
  spools khi malformed/disconnect/disk error. Sau parse, monitor nhận ASGI
  disconnect, truyền Event vào RPC; coroutine bị hủy vẫn drain transfer thread
  trước kết thúc handler/đóng FormData. Toàn bộ DB/RPC nằm trong một thread scope.
- `backend/metadata/schemas.py`, `main.py`: FileSummary và include file router.
  Không thêm GET file endpoints trước P4.

Tier **focused integration**: Docker Desktop Linux engine 29.3.1,
Python 3.12.15; PostgreSQL healthy từ Compose và TEST_DATABASE_URL của tests
service; manifest list của tests build cuối
`sha256:6670b0c41358c3c568ebc09cdec5b16acc641d2d6b645d6c598ee0f2806dc3af`
(quan sát từ output build thành công, trước lượt 10 selected tests).
Mỗi test tạo schema UUID riêng, ba production Storage server trong
process Linux, mỗi node thư mục riêng. Fault injection bao quanh production
Store handler; atomic filesystem và gRPC thật được sử dụng. Fixture dọn schema,
temp directories, channels/server/executors; không tạo file demo trên live volumes.

Preflight `docker info` và Compose `ps` qua; chỉ rebuild **tests image** sau thay
đổi code. Commands từ repo root:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_upload.py
```

Lượt suite sau sửa ASGI harness/parser: **32 passed**, không skipped/deselected,
1 warning Starlette/TestClient về httpx deprecation. Sau đó bổ sung 5 ca cuối mà
không sửa runtime, rebuild tests image và chỉ chạy các node mới:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_upload.py::test_pending_commit_failure_prevents_store_and_fails_file tests/test_metadata_upload.py::test_final_commit_ack_loss_does_not_change_available_to_failed tests/test_metadata_upload.py::test_commit_uses_file_snapshots_and_acknowledgements_despite_health_change tests/test_metadata_upload.py::test_final_commit_rejects_inconsistent_persisted_chunks_or_rf
```

**5 passed**, không skipped/deselected, cùng warning deprecation. Rà cuối sửa
error details: DB lỗi trước bước tạo File trả details rỗng; chỉ giữ file_id khi
đã bắt đầu creation (commit có thể mất ack). Bổ sung một ca mới và chỉ rerun
nhánh failure/cancel bị ảnh hưởng sau rebuild:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_upload.py -k "empty_needs or insufficient_rf_marks or database_failure or http_disconnect_and_coroutine or pending_commit_failure or final_commit_ack_loss or final_commit_rejects"
```

**10 passed, 28 deselected**, không skipped, cùng warning deprecation; 9 ca
regression và 1 ca mới. Tổng **38 ca P3 khác nhau** đã qua các lượt tập trung;
không cộng counts thành một full-suite run. Lượt ban đầu có ba
ASGI harness failures vì header names chưa lowercase; đã sửa harness. Lượt chạy
trong khi rebuild chưa hoàn tất dùng image cũ, không dùng làm evidence code mới.
Sau các lượt tests đã pass, read-only `docker image inspect dfs-python-tests:dev
--format '{{.Id}}'` bị permission denied tại `dockerDesktopLinuxEngine` named
pipe. Dừng tại lỗi đầu, không retry/restart hoặc chạy check phụ thuộc tiếp;
giữ build manifest đã quan sát, không coi inspection cuối đã pass. Các integration
results ở trên đã hoàn tất trước lỗi đọc này.

Native Ruff check/format qua cho 5 files P3; sau sửa router/tests chỉ chạy lại
paths thay đổi. Commands từ backend/ (path list thu hẹp tương ứng mỗi lần):

```powershell
.\.venv\Scripts\python.exe -m ruff check metadata/upload.py metadata/routes/files.py metadata/main.py metadata/schemas.py tests/test_metadata_upload.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/upload.py metadata/routes/files.py metadata/main.py metadata/schemas.py tests/test_metadata_upload.py
```

Evidence gồm empty/single/multi/exact-boundary/max-config, file đúng 64 MiB
(chunk 4 MiB, RF2, 16 chunks, 32 replicas và full hash), oversized/invalid parts,
actual size bất chấp Content-Length giả; quan sát PENDING từ Session riêng tại
mọi Store entry và không có coordinator transaction/checked-out connection.
Ack loss retry cùng ID/duplicate, node đầy, ack sai, timeout sau durable Store
đều kiểm tra mapping/fallback; node lỗi không được chọn cho chunk tiếp theo.
Final chunk chưa đủ RF vẫn UPLOADING; upload cạnh tranh 409, GET cluster/nodes và
health tiếp tục. DB lỗi trước PENDING commit không Store; lỗi sau Store giữ
UPLOADING/PENDING và bootstrap recovery thành FAILED/cleanup. Final commit
ack loss trả 503 nhưng giữ AVAILABLE; lost HTTP response cũng giữ AVAILABLE.
ASGI disconnect và task cancellation giữ FAILED/UPLOAD_INTERRUPTED, retained
attempt, cleanup flag, spool đóng và lock/client scope sạch. Persisted hash/RF
bị thay đổi trước commit bị chặn, không trả 201.

Không chạy lại P1/P2 tests, full backend/frontend suite, migrations/schema drift,
live Compose smoke, deploy/restart services hoặc sửa volume. Live Metadata vẫn
dùng build trước P3. Chưa có GET files/list/detail/chunks hoặc download/failover,
process crash thật (P6), DELETE/cleanup worker/repair (M4). Cleanup flags chỉ
là pending intent; test FAILED cố ý vẫn có bytes trên node.

Bàn giao P4: tái dùng FileSummary và UUID file trong error details; bổ sung
GET list mặc định chỉ AVAILABLE, detail/chunks cho states theo contract. GET
chỉ đọc snapshots, không operation lock hoặc RPC; đừng biến attempted PENDING
của AVAILABLE thành cleanup. Download và tempfile lifecycle tiếp tục ở P5.

### Bằng chứng P4 — 04/10/2026

Đã triển khai:

- `backend/metadata/files.py`: list default AVAILABLE, include_inactive thêm
  UPLOADING/FAILED/DELETING, luôn ẩn DELETED; total cùng filter, pagination
  limit 1..100/offset >=0 và order created_at DESC/file_id DESC. List chỉ đọc
  FileSummary, không query replica counters.
- Detail trả FileSummary/error_code, known_readable, counters và cleanup count;
  missing/DELETED trả 404 FILE_NOT_FOUND. Chunks theo chunk_index ASC, replicas
  theo node_id; giữ cả PENDING/MISSING/CORRUPTED/DELETED và node DOWN/disabled.
  Chunk state suy ra: UPLOADING/PENDING, FAILED hoặc DELETING/INACTIVE; AVAILABLE
  theo live count và RF của file. Empty AVAILABLE known_readable=true.
- `backend/metadata/replica_health.py`: CTE live eligibility và health flags
  dùng chung trong `cluster.py` và file views; VERIFIED + enabled ACTIVE + không
  cleanup_pending. Domain target min(file RF, domain enabled), under-replicated
  chỉ 1..RF-1, unavailable=0 live, over-replicated khi live > RF. Cluster chỉ
  tính chunks của AVAILABLE; detail tính chunks đã tạo ở mọi status được xem.
- `backend/metadata/routes/files.py`, `schemas.py`: GET files/detail/chunks và
  FileList/FileDetail/ChunkList/ChunkSummary/ReplicaSummary đúng envelope/schema,
  UUID/query validation 422, nullable errors/checksum, timestamps UTC Z.
- Các file GET dùng transaction PostgreSQL **REPEATABLE READ, READ ONLY**, timeout
  statement 5 s, Session riêng trong threadpool. Không operation lock hoặc RPC;
  snapshot tiếp tục đọc sau startup dù health scheduler/data client đã dừng.
  Query count cố định: list 2 SELECT, detail 3 SELECT, chunks 2 SELECT, thêm 2
  SET cho scope; không N+1 và không trộn data từ concurrent commit trong request.

Tier **focused integration**, vì thay HTTP/DB views và chia sẻ query rules của
cluster. Read-only preflight `docker info`/Compose `ps` qua: Docker Desktop Linux
engine 29.3.1, postgres healthy. Chỉ rebuild tests image; manifest list quan sát
từ build thành công:
`sha256:bd6d27fa1a2f9efe9b883cf52b6078cd5be4a4818d03db4bebdbf94cb3bc46ac`.
Python 3.12.15 theo runtime image cùng parent digest đã xác minh ở P3. Tests
dùng TEST_DATABASE_URL và schema UUID riêng; freeze health poll cho snapshot
fixtures. Một test dùng ba production Storage server Linux với thư mục riêng,
POST multi-chunk RF2 thật rồi GET và quan sát UPLOADING khi Store tiếp theo bị giữ.

Commands từ repo root:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_files.py tests/test_metadata_cluster.py
```

**77 passed**, không skipped/deselected: 46 P4 checks và 31 cluster regressions.
1 warning Starlette/TestClient về httpx deprecation; không thay dependency lock.
Không assertion failures hoặc infrastructure blockers trong P4. Fixture đã dọn
schema/temp directories/server/channels; không có file demo trên live volumes.

Native lint/format cho 6 paths P4 (từ backend/):

```powershell
.\.venv\Scripts\python.exe -m ruff check metadata/files.py metadata/replica_health.py metadata/cluster.py metadata/schemas.py metadata/routes/files.py tests/test_metadata_files.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/files.py metadata/replica_health.py metadata/cluster.py metadata/schemas.py metadata/routes/files.py tests/test_metadata_files.py
```

Ruff check/format qua; sau sửa fixture alias chỉ check/format lại test file.
`git diff --check` qua. Không rerun tests sau các cập nhật tài liệu vì không đổi
runtime. Evidence gồm pagination/sorting/status visibility/UTC/null, UUID/query
errors, mixed history và domain degradation/over-replication, RF snapshot khác
setting hiện tại, empty/inactive views, DB outage/startup gating, không RPC/lock
khi clients dừng. Fixture 1 và 256 chunks với 3 replica/chunk có cùng query count;
concurrent writer đổi health hoặc insert file giữa SELECT vẫn trả snapshot cũ
nhất quán, request kế tiếp thấy commit mới. Cluster regressions tiếp tục giữ
aggregate một statement/snapshot và các rule cũ.

Không deploy/restart live services, migrations/schema drift, smoke, full backend
suite, frontend hoặc chạy lại toàn P1–P3. Một POST trong P4 kiểm tra read view sau
upload thật, không thay P3 gate. Chưa download bytes/failover hoặc process crash
thật (P5/P6); cleanup worker/DELETE/repair thuộc M4. known_readable/counters là
cached metadata/health, không chứng minh bytes vừa được verify qua GET.

Bàn giao P5: implement GET download riêng dưới data operation lock, dùng size,
chunk size/RF/hash snapshot của file trong DB; validate metadata/chunk order,
probe/fallback qua verified Get bytes và assemble tmp trước HTTP 200. Đóng read
transaction trước RPC, cập nhật observations bằng transaction riêng; giữ các
mapping attempted PENDING từ upload fallback để reconcile, không tự cleanup.

### Bằng chứng P5 — 04/10/2026

Đã triển khai:

- GET `/api/v1/files/{file_id}/download` chuẩn bị dưới data operation lock.
  Đọc metadata bằng snapshot REPEATABLE READ, đóng transaction trước Get RPC;
  dùng size/chunk size/count/hash của file trong DB, kể cả file từ config cũ.
  Kiểm tra state, contiguous indexes, kích thước và checksum metadata trước RPC.
- Fallback hữu hạn qua enabled/configured mappings không DELETED/cleanup_pending:
  ưu tiên ACTIVE → SUSPECTED → DOWN, VERIFIED trước trong cùng health tier.
  Vẫn probe PENDING/MISSING/CORRUPTED; Get thành công chuyển VERIFIED,
  NOT_FOUND chuyển MISSING, data integrity lỗi chuyển CORRUPTED. Lỗi transport
  giữ replica status; observations dùng transaction riêng, không sửa node health,
  File state hoặc cleanup flags và không tạo replica mới.
- Assemble actual verified bytes vào tempfile độc quyền, flush rồi đọc lại chính
  tempfile theo block để xác minh size/SHA-256 toàn file trước HTTP 200. Lỗi ở
  chunk cuối, metadata, DB hoặc disk vẫn trả JSON error trước binary headers.
- Response sở hữu disk snapshot độc lập; lock được thả trước gửi HTTP. Headers
  có Content-Length, checksum và tên UTF-8 đã chuẩn hóa. Đọc response 64 KiB/block;
  success, send failure, disconnect và cancellation đều drain thread đang đọc
  trước close/unlink, với ASGI spec 2.0 và 2.4. Upload mới có thể chạy trong lúc
  response cũ đang gửi; thay đổi metadata/storage không đổi snapshot đã chuẩn bị.
- Startup khởi tạo download_temp_dir dưới lock trước readiness. Janitor chỉ xóa
  regular files có tên chính xác `.download-<32 lowercase UUID hex>.tmp` ở trực
  tiếp thư mục đó; giữ symlinks, subdirectories và foreign names. Lỗi khởi tạo
  giữ ready 503/live 200. Cleanup runtime thất bại được log để startup sau xử lý.
- Tách shared thread ownership/drain sang `metadata/transfers.py`; upload dùng
  lại helper drain. Fixture Storage cho phép fault injection Get trên production
  gRPC server; các node dùng filesystem riêng trong temporary directories.

**Tier: focused integration**, vì thay đổi HTTP → PostgreSQL → production gRPC
→ filesystem cùng startup/response lifecycle. Preflight `docker info` và
`docker compose --env-file deploy/.env -f deploy/compose.local.yml ps` passed;
PostgreSQL healthy. Môi trường Docker Desktop Linux engine 29.3.1, Python 3.12.15,
TEST_DATABASE_URL từ tests service và schema UUID riêng. Rebuild tests image hoàn
tất trước mỗi lượt cần code mới; không deploy hoặc restart live services.

Các lượt từ repo root:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_download.py
```

Lượt đầu **58 passed**, không skipped/deselected, 19.23 s. Bao phủ empty/boundary/
multi-chunk, legacy config, state/UUID, fallback/priority, wire integrity,
replica exclusions/observations, metadata/full-file integrity,
disk/DB faults, busy admission, transaction boundary và cancellation/disconnect.
Các ca bổ sung sau lượt này dùng selection riêng:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_download.py -k "stream_owns or cancel_during_threaded or real_corrupted or legacy_metadata_filename or app_startup_cleans"
```

Kết quả **4 passed, 1 failed, 58 deselected**, không skipped. Lỗi nằm ở ASGI test
harness: chưa hỗ trợ external cancellation trong mode success; các assertions
drain/open-handle trước release đã qua. Sửa harness bằng mode read_cancel, không
đổi runtime; Ruff test file passed, rebuild xong rồi chạy riêng ca lỗi:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_download.py::test_cancel_during_threaded_response_read_drains_before_closing_temp
```

**1 passed**, không skipped/deselected, 2.07 s. Manifest list từ output build cuối:
`sha256:f54894cf0238416595163b0878f73108ed23c2a42b0cd1ddb5c6969c62dc3925`.
Năm ca bổ sung xác minh response snapshot/lock release với upload mới, cancel
giữa threaded read, corruption trên disk thật, legacy filename và startup janitor
được nối vào lifespan thật. Tổng **63 ca P5 khác nhau passed** qua các lượt trên;
không coi là một lần chạy full suite hoặc cộng lần rerun thành ca mới.

Regressions trực tiếp cho startup/lifespan wiring và shared upload drain:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_bootstrap.py::test_app_startup_holds_operation_lock_and_gates_readiness tests/test_metadata_worker.py::test_lifespans_start_fresh_worker_and_stop_before_engine_disposal tests/test_metadata_worker.py::test_partial_worker_start_failure_cleans_resources_and_keeps_not_ready tests/test_metadata_worker.py::test_all_storage_down_still_ready_after_successful_initialization tests/test_metadata_operations.py::test_lifespan_waits_for_operation_final_db_work_before_closing_clients_engine tests/test_metadata_upload.py::test_http_disconnect_and_coroutine_cancel_wait_for_failure_persistence
```

**8 passed**, không skipped/deselected, 4.16 s. Các lượt có HTTP TestClient đều
có một Starlette deprecation warning liên quan httpx. Không có infrastructure
blocker; fixtures dọn schema/tempdirs, không dùng hoặc reset live storage volumes.

Native Windows/Python 3.12.14, Ruff check/format 8 paths passed, từ backend/:

```powershell
.\.venv\Scripts\python.exe -m ruff check metadata/download.py metadata/download_temp.py metadata/download_response.py metadata/transfers.py metadata/routes/files.py metadata/main.py tests/test_metadata_download.py tests/test_metadata_upload.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/download.py metadata/download_temp.py metadata/download_response.py metadata/transfers.py metadata/routes/files.py metadata/main.py tests/test_metadata_download.py tests/test_metadata_upload.py
```

Sau khi thêm/sửa harness, chỉ Ruff check/format test_metadata_download.py được
chạy lại. Chốt docs dùng **no execution**; không rerun runtime checks đã pass.
`git diff --check` kiểm tra whitespace riêng. Không chạy full backend suite,
frontend, migrations, deploy, live Compose smoke, stop node thật hoặc Metadata
process crash/restart. Các gate đó còn ở P6; fault injection/health snapshot
trong P5 không thay bằng chứng live failover/persistence. Cleanup/DELETE/repair
vẫn thuộc M4. Bước tiếp theo **P6 lifecycle và smoke RF=2 thật**.

### Bằng chứng P6 — 04/10/2026

Đã thêm production-boundary lifecycle harness và smoke scripts; không sửa
service runtime, schema, dependencies hoặc Storage/config trong phase này.
Các hooks chỉ nằm trong child harness của test, sau commit thật; parent đọc
bounded pipe barriers để SIGKILL đúng window, không sleep đoán thời điểm.

**Tier focused integration** cho HTTP child process, PostgreSQL và production
gRPC servers với dirs riêng. Ban đầu Docker Linux daemon chưa chạy; dừng checks
phụ thuộc, tiếp tục implementation và chờ người dùng xác nhận đã bật Docker.
Preflight sau xác nhận passed: Docker Linux engine 29.3.1, PostgreSQL healthy,
TEST_DATABASE_URL từ tests service; mỗi test dùng schema UUID riêng.

Từ repo root, build hoàn tất trước test:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_files_lifecycle.py
```

**3 passed**, không skipped/deselected hoặc warnings, 10.51 s, Docker Python 3.12.15:

- 10 MiB + 17 byte, 6 chunks, exact RF=2, dùng cả ba Storage dirs và config A/B/B;
  mọi chunk có A+B. POST 201/Location, REST list/detail/chunks/cluster và downloaded
  bytes/SHA được đối chiếu nguồn. Corrupt/xóa fixture bytes ở hai chunk riêng:
  download fallback đúng, observations CORRUPTED/MISSING. Xóa cả source chunk
  cuối trả 503 JSON; chỉ restore bytes của fixture. Normal Metadata restart giữ
  file hash/RF/IDs/mappings/observations; startup barrier chứng minh nodes DOWN
  trước poll, sau đó ACTIVE. Stale managed tmp bị dọn, foreign file giữ nguyên.
- SIGKILL sau real Store nhưng trước ghi VERIFIED: DB đã có PENDING commit,
  bytes đã nằm trên disk. Process mới recovery FAILED/UPLOAD_INTERRUPTED và
  cleanup_pending, giữ attempted mappings/bytes; list mặc định ẩn, inactive list
  có fixture, download 409 FILE_NOT_READY.
- SIGKILL sau AVAILABLE commit nhưng trước HTTP response: request mất kết nối;
  process mới vẫn giữ AVAILABLE/hash/RF/chunk/mapping, không cleanup_pending,
  list/download đúng. Hai crash cases dùng file 2 MiB + 17 byte/2 chunks.

Harness ghi manifest file/chunk IDs trong tmp_path từng case. Fixture teardown
dọn schema/temporary dirs và stop owned servers; không chạm live DB rows/volumes.

**Targeted tests/checks** cho guard smoke và lint, từ backend/:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_smoke_files.py
.\.venv\Scripts\python.exe -m ruff check scripts/smoke_files.py tests/test_metadata_files_lifecycle.py
.\.venv\Scripts\python.exe -m ruff format --check scripts/smoke_files.py tests/test_metadata_files_lifecycle.py
# Sau sửa guard, chỉ paths bị thay đổi/thêm mới:
.\.venv\Scripts\python.exe -m ruff check scripts/smoke_files.py tests/test_smoke_files.py
.\.venv\Scripts\python.exe -m ruff format --check scripts/smoke_files.py tests/test_smoke_files.py
```

Ruff 3 Python paths passed qua checks tập trung; PowerShell Parser.ParseFile
cho `backend/scripts/smoke_files.ps1` passed. Native Python 3.12.14: **2 passed**,
không skipped/deselected, 0.08 s; một PytestCacheWarning do sandbox không ghi được
cache. Không xử lý/xóa cache directory ngoài scope.

**Live Compose smoke:** build/deploy Metadata hiện tại, giữ Storage containers
và PostgreSQL. Lệnh tạo fixture chạy script dưới `python -O` để guards vẫn có
hiệu lực khi Python assert bị bỏ:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml build metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --no-deps --wait metadata
& ./backend/scripts/smoke_files.ps1
```

Script lần đầu bị Docker pipe permission denied ngay preflight, chưa tạo fixture
hoặc stop node; chạy với quyền Docker phù hợp mới tới baseline/upload. Upload
201 đã tạo một file AVAILABLE nhưng smoke fail ở guard domain count: script
so với RF=2 thay vì min(RF, configured enabled domains). Sửa guard đúng contract,
thêm hai regressions topology một domain (baseline/down), bổ sung `-Manifest`
để tiếp tục đúng fixture. Không sửa domain counters service để chiều smoke.

Rebuild tests/Metadata; riêng ca integration nhiều chunk dùng helper verify đã
thay đổi được chạy lại **1 passed**, 6.11 s, không skipped/deselected/warnings:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_files_lifecycle.py::test_multichunk_domains_real_disk_faults_and_normal_restart
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --no-deps --wait metadata
& ./backend/scripts/smoke_files.ps1 -Manifest ./.runtime/m3-smoke-43cc9c856bfd40f8bcc537209a0b143c.json
```

Smoke resume **passed**, không upload file thứ hai. Tổng 3 lifecycle cases khác
nhau + 2 targeted guard cases passed; không cộng integration rerun thành ca mới
hoặc coi là full-suite run. Build cuối, manifest-list digests từ build output:

- tests: `sha256:c50cbdd5a35baa091e9163e9eb711e7337be55fd545653142a04b4854640da0a`.
- Metadata deploy: `sha256:d6f3d9149deb44314d136034984b1817447e1544ab1a8d74a0a6f63dd2082380`;
  `docker inspect --format '{{.Image}}' distributed-file-storage-metadata-1`
  xác nhận container dùng digest này.
- Storage giữ image `sha256:18c80476b1ff7bd02675f2106983df4f94692f401a1c18a5e5b235e27189a401`;
  ba named volumes node-1-data/node-2-data/node-3-data khác nhau và không đổi
  qua smoke (prefix `distributed-file-storage_`).

Fixture live tại API `http://127.0.0.1:8000/api/v1`:

- File ID `f26d076f-0ec6-423d-a161-6e18d5799d2a`, size 10,485,777 bytes,
  chunk size 2,097,152, RF=2, SHA-256
  `c4a9068afee819811d78887024edcabee99f03f85e40e8319078206d2d547321`.
- Manifest host `.runtime/m3-smoke-43cc9c856bfd40f8bcc537209a0b143c.json`;
  copy trong Metadata `/tmp/m3-smoke-43cc9c856bfd40f8bcc537209a0b143c.json`.
  Host manifest giữ ngoài container để không mất khi recreate; `.runtime` gitignored.

| Index | Chunk ID | Bytes | VERIFIED mappings |
|---|---|---:|---|
| 0 | a28a6862-72d4-4fb4-b086-f37d9acb3076 | 2097152 | node-1, node-2 |
| 1 | c48f3871-2463-45fb-ab0e-137eb9711f4e | 2097152 | node-1, node-3 |
| 2 | 3c10e80f-cb47-4337-bcba-fa75e5558c17 | 2097152 | node-2, node-3 |
| 3 | 0794522c-c036-4b6a-9e33-7507b60eca57 | 2097152 | node-1, node-2 |
| 4 | 182cc2ae-647c-434d-b09d-4160165bf762 | 2097152 | node-1, node-3 |
| 5 | d7fc6c2d-40dc-4f61-8a08-5bd1351c46ea | 17 | node-2, node-3 |

Baseline list/detail/chunks/cluster và downloaded bytes/hash/headers đều đúng.
Stop **storage-node-2** thật: REST quan sát ACTIVE → SUSPECTED → DOWN, reason
DEADLINE_EXCEEDED, last-success age 13.11 s khi DOWN. Hai node khác ACTIVE,
live/ready 200; 4/6 chunks under-replicated, unavailable=0, download vẫn đúng
bytes/SHA. Recovery start existing node-2 rồi chờ ACTIVE: under-replicated=0,
last_error cleared. Restart Metadata thật: fresh health snapshots, 3 ACTIVE,
file summary/chunk IDs/mappings/RF và downloaded SHA giữ nguyên. Compose local
chỉ có dev_host nên domain_target=1 và domain_degraded=0; A/B/B evidence trong
test không thay bằng chứng hai host M6.

Sau smoke `docker compose ... ps` cho cả 5 services healthy; kiểm tra trực tiếp
`/tmp/dfs-downloads` trả `[]`. used_bytes: node-1 8,388,608; node-2/node-3 mỗi
6,291,473; tổng fixture replicas 20,971,554 bytes. Giữ đúng **một live file** và
12 mappings để M4 DELETE/cleanup; không raw-delete DB rows/chunks hoặc reset
volumes. Metadata đã deploy API M3; không rerun full backend/P1–P5, frontend,
PostgreSQL restart/lifecycle M2, hoặc stop các node khác. Không có migration
source mới; Compose Metadata vẫn chạy lệnh existing `alembic upgrade head`.
Docs chốt bằng no execution và `git diff --check`, không rerun passing checks.
Tiếp theo **P7 rà DoD/evidence và bàn giao M4**; M3 chưa đánh dấu hoàn thành.

## 12. P7 — Rà và bàn giao M4

Rà implementation với contracts và checklist. Cập nhật README repo, docs/README,
milestone tổng và evidence M3; không sửa lịch sử evidence M1/M2 hoặc chạy lại
tests chỉ để có counts mới. Gate còn thiếu giữ unchecked.

M4 nhận coordinator/client, file read APIs, verified bytes/fallback, operation
lock và attempted mappings/cleanup flags bền vững. Tiếp tục DELETE tombstone,
cleanup bounded dưới lock, repair dựa RF của file và source đã xác minh,
reconcile mappings khi node trở lại. Không coi M3 download fallback là repair:
nó chưa tạo replica mới. Đặc biệt kiểm tra pending từ timeout/ack loss, FAILED
fixtures và node disabled/offline; không xóa mapping trước Delete ack.

### Bằng chứng P7 — 04/10/2026

Đã đọc lại source validation/chunk reader, placement, data client/operation
boundary, upload commit/failure, read snapshots/counters, download preparation/
response/temp janitor và lifespan; đối chiếu API_CONTRACTS §§1–2.4,
ARCHITECTURE §§4–8, PROJECT_OVERVIEW và gates/evidence P1–P6. Rà tên/coverage tests
và manifest P6 trên host. Không phát hiện lệch contract cần sửa runtime trong
phạm vi M3; cập nhật README repo, docs/README và kế hoạch tổng để ghi M3 hoàn thành.
Các ghi nhận phase trước giữ nguyên như evidence lịch sử, không đổi counts.

| Gate M3 | Source chính | Evidence đã có | Kết luận |
|---|---|---|---|
| Input/actual size, empty/boundary, bounded chunk reads/hash | file_input.py | [P1](#bằng-chứng-p1--04102026), HTTP validation/64 MiB [P3](#bằng-chứng-p3--04102026) | Đạt |
| RF node khác nhau, ưu tiên domain, snapshot size/RF vào file | placement.py, upload.py | P1 policy; [P6](#bằng-chứng-p6--04102026) A/B/B và 6 chunks local | Đạt |
| PENDING commit trước Store; đủ ack mới AVAILABLE/201; không tx qua RPC | upload.py, operations.py, storage_client.py | [P2](#bằng-chứng-p2--04102026), P3 ack loss/failure/DB/cancel, P6 commit-window SIGKILL | Đạt |
| FAILED/cleanup flags giữ attempted mapping; mất response sau commit không fail AVAILABLE | upload.py, bootstrap.py | P3, P6 crash trước/sau commit | Đạt; dọn bytes thuộc M4 |
| List/detail/chunks, cached counters/visibility, GET không lock/RPC | files.py, replica_health.py, schemas.py | [P4](#bằng-chứng-p4--04102026), P6 HTTP views | Đạt |
| Verify Get bytes và toàn tempfile trước 200; lỗi chunk cuối JSON; fallback observations | download.py, storage_client.py | [P5](#bằng-chứng-p5--04102026), P6 real disk faults/Compose download | Đạt |
| Lock thả trước response; tmp cleanup/send failure/disconnect/cancel; startup/drain | download_response.py, download_temp.py, transfers.py, main.py | P2/P3/P5 lifecycle, P6 restart/stale tmp | Đạt |
| File nhiều chunk RF=2, stop replica node vẫn đọc, recovery/restart giữ SHA/mappings | smoke_files.py/.ps1, test_metadata_files_lifecycle.py | P6 live Compose node-2 DOWN/recovery và Metadata restart | Đạt local; hai host thuộc M6 |

Counts là evidence từng phase, không phải một full-suite run: P1 106 targeted;
P2 60 + 10 regressions; P3 38 ca khác nhau; P4 46 + 31 cluster regressions;
P5 63 ca khác nhau + 8 regressions; P6 3 lifecycle + 2 guard cases và live smoke.
Giữ các failures/harness fixes, deselections, warnings và build digests ở evidence
tương ứng. Không cộng các regression/rerun trùng nhau thành tổng test mới.

**Tier P7: no execution.** Chỉ chỉnh tài liệu sau read-only source/evidence review;
không đổi runtime/tests/config/dependencies, nên không chạy pytest/Ruff/build,
frontend, Docker preflight, deploy/restart, migrations hoặc smoke mới. Kiểm tra
patch bằng `git diff --check` từ repo root; passing test/runtime evidence vẫn là
các lượt P1–P6, không phải kết quả chạy mới ở P7. Trạng thái 5 services healthy và
tmp rỗng là quan sát cuối P6; P7 không tuyên bố một health check live mới.

### Bàn giao M4 — failure, DELETE, cleanup và repair

M3 hoàn thành; M4 nhận các thành phần và invariants sau:

- Một Metadata process/worker, một nonblocking operation_lock. DataOperation
  sở hữu thread/session; transaction ngắn, đóng trước RPC, statement timeout 5 s.
  GET snapshots và health không chờ data lock. Giữ admission/drain/shutdown đúng
  khi thêm cleanup lifecycle, không đóng channels/engine trước final DB work.
- Storage đã có DeleteChunk idempotent từ M1; Metadata StorageClient và
  DataOperation hiện chỉ có Store/Get. M4 thêm Delete wrapper/deadline/error mapping
  và coordinator, không coi endpoint DELETE đã có trong M3.
- File snapshot chunk_size/RF/hash làm nguồn cho download/repair; dùng RF của
  file thay vì default hiện tại. Store ack/Get actual bytes được verify. Timeout
  không chứng minh bytes chưa tồn tại; attempted PENDING vẫn phải reconcile/cleanup.
- Upload failure/recovery chỉ ghi FAILED và cleanup_pending, chưa dọn disk.
  Không xóa replica mapping hoặc node row trước khi Delete được xác nhận. Cleanup
  bounded, node DOWN giữ pending bền vững; retry sau recovery/restart.
- DELETE AVAILABLE/FAILED: tombstone DELETING trước RPC; cấm download/repair,
  completion DELETED chỉ khi pending=0; repeat DELETE theo API 200/202. Giữ
  metadata để quan sát cleanup, không raw-delete/tự GC tombstones.
- Repair dưới lock theo cursor/max_chunks/time budget của contract. Source phải
  Get/hash đúng; destination theo placement/domain, PENDING trước Store, VERIFIED
  sau ack. Fallback download chỉ cập nhật observations, không tạo replica mới.
  Health ACTIVE không chứng minh chunk còn nguyên; xét node quay lại volume trống,
  MISSING/CORRUPTED, node disabled/offline, timeout/ack loss và over-replication.

Fixture live bàn giao: **một AVAILABLE file**
`f26d076f-0ec6-423d-a161-6e18d5799d2a`, 10,485,777 bytes, 6 chunks/12 mappings,
20,971,554 bytes replicas; host manifest
`.runtime/m3-smoke-43cc9c856bfd40f8bcc537209a0b143c.json`. IDs/hash/mapping và
named volumes ở evidence P6. Giữ manifest host khi recreate container; không
corrupt/xóa live volume để test faults. Khi M4 DELETE đã triển khai và được chọn
làm fixture, dùng endpoint để dọn, giữ evidence pending/completion; không dọn
fixture hoặc reset unrelated data trong P7.

Chưa làm DELETE/background cleanup/manual repair (M4), UI (M5), hai host/rehearsal
(M6), streaming/resumable upload, auth hoặc distributed transactions. P7 không
tạo kế hoạch chi tiết M4 hay bắt đầu implementation M4. **P1–P7 và M3 hoàn thành**
ngày 04/10/2026; bước tiếp theo là lập kế hoạch M4 theo contracts hiện có.

### Review bổ sung M3 — 04/10/2026

Review sau P7 tái hiện 3 findings P2 bằng native ASGI probes trước sửa:

- Raw task cancellation trong parser spool write: request kết thúc và spool đóng
  khi writer còn chạy, writer gặp ValueError khi được release. Coordinator chưa
  bắt đầu nên các test cancel giữa Store/Get trước đó không bao phủ window này.
- Limit chỉ kiểm tra sau parse: với file limit 4 bytes, toàn bộ stream 8,192 bytes
  vẫn được spool trước 413. Không tin Content-Length nhưng vẫn cần bảo vệ disk
  tạm trong chính giai đoạn nhận HTTP file.
- `charset=not-a-codec` trả 500 text Internal Server Error thay vì JSON 400.
  Các boundary/header hỏng thông thường vẫn trả 400 qua pinned Starlette parser.

Đã sửa `backend/metadata/routes/files.py`:

- CompleteMultipartParser nhận max_file_size_bytes, reset counter cho từng part,
  đếm actual file data trước khi queue spool write; vượt limit raise
  FILE_TOO_LARGE/413 và dừng đọc stream. Giữ measure_spool cuối và các rule field/
  filename/content type. Không dùng Content-Length hoặc multipart overhead làm
  size của riêng file, không đổi unary RPC/transfer architecture.
- Validate text codec bằng input không rỗng trước decode fields/tạo spool; convert
  codec lookup/Unicode decode errors thành MultiPartException để route trả JSON
  INVALID_REQUEST/400. Valid UTF-8/Latin-1, empty/exact-limit vẫn được parse.
- OwnedUploadFile dùng owned task + shield + finish_cancelled cho write/seek/close;
  repeated raw cancellation chờ thread kết thúc trước parser cleanup. Spool I/O
  chạy ngoài event loop; reuse existing ownership/drain helpers, không thêm deps.

Thêm `tests/test_metadata_multipart.py` với 13 cases: unknown/non-text/unusable
codecs, stream không Content-Length hoặc header giả, dừng trước EOF và spool
không vượt limit, fragmented boundaries, valid charsets, empty/exact-limit,
repeated cancel giữa rolled-file write/seek và cancel trong owned close. Các ca
reject/cancel kiểm tra coordinator không được gọi; không cần DB/RPC cho tier này.

**Targeted tests/checks**, native Windows/Python 3.12.14, từ backend/:

```powershell
.\.venv\Scripts\python.exe -m ruff check metadata/routes/files.py tests/test_metadata_multipart.py
.\.venv\Scripts\python.exe -m ruff format --check metadata/routes/files.py tests/test_metadata_multipart.py
.\.venv\Scripts\python.exe -m pytest -q tests/test_metadata_multipart.py
.\.venv\Scripts\python.exe -m pytest -q tests/test_metadata_multipart.py -k invalid_multipart_charset
```

Ruff check/format 2 paths passed. Lượt native đầu **9 passed, 3 failed**: validator
dùng empty bytes bị Python bỏ qua codec lookup. Sửa bằng input không rỗng;
selection **3 passed, 9 deselected**. Sau bổ sung codec undefined và decode error
conversion, selection cuối **4 passed, 9 deselected**, 0.40 s. Các lượt có
PytestCacheWarning do sandbox chặn ghi cache; không xóa/reset cache directories.
Không coi các lượt rerun là cases mới. Source cuối được kiểm tra cả 13 cases
trong Docker ở lượt dưới.

**Focused integration** cho parser mới nối vào coordinator/HTTP/DB/gRPC. Read-only
preflight `docker info --format '{{.OSType}}'` và Compose ps passed; PostgreSQL
healthy, Docker Linux engine 29.3.1. Build tests image hoàn tất theo source cuối
trước chạy; schema UUID/production gRPC servers và dirs fixture riêng.

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_metadata_multipart.py tests/test_metadata_upload.py -k "metadata_multipart or http_upload_size_hash_rf_location or default_64_mib or invalid_http_input or empty_needs or parse_failures or actual_file_size or http_disconnect_and_coroutine_cancel"
```

**36 passed, 15 deselected**, không skipped, 7.79 s, Docker Python 3.12.15:
13 ingress cases mới + 23 directly affected upload regressions (empty/boundary/
multi-chunk, 64 MiB/RF=2, invalid input, parse/truncated/disconnect/write failure,
Content-Length giả và cancellation sau Store). Một Starlette/TestClient
deprecation warning. Tests image manifest-list từ build output:
`sha256:ad8d09ca34e36e4c1cc2b3eb0eda5226cd50e845fc86a1ab983eebdc860d6dac`.

Docs phản ánh ingress behavior mới; `git diff --check` qua. **Chưa deploy fix**:
Metadata live vẫn là P6 build, cần build/deploy khi cập nhật runtime. Không đổi
schema/proto/dependencies, không start/restart services hoặc thêm/xóa live
fixture. Không rerun full backend/frontend, download/failover smoke hoặc crash/
restart P6; các boundaries đó không thay đổi. Manifest fixture bàn giao M4 giữ
nguyên. Ba findings đã sửa và có regression evidence; M3 vẫn hoàn thành trong
phạm vi milestone, bản ingress mới hiện ở source/tests image.
