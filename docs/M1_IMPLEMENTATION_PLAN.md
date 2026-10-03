# M1 — Một Storage Node lưu dữ liệu thật

**Ngày lập:** 03/10/2026  
**Trạng thái:** P1 hoàn thành; P2–P6 chưa triển khai  
**Đầu vào:** base M0 đã chạy và được kiểm tra bằng Docker Linux  
**Đầu ra:** Store → Get → restart → Get → Delete hai lần hoạt động qua gRPC.

## 1. Mục tiêu và ranh giới

M1 hoàn thiện một Storage Node độc lập: lưu chunk immutable, đọc bytes thật,
xóa idempotent, báo health/capacity và giữ dữ liệu qua restart. Cùng codebase
vẫn chạy được thành ba node bằng env và volumes hiện có.

Giữ nguyên bốn unary RPC và field trong [proto](../backend/contracts/storage.proto).
Hành vi theo [API_CONTRACTS.md, mục 3](API_CONTRACTS.md) và
[ARCHITECTURE.md, mục 2, 6–8](ARCHITECTURE.md). Tài liệu này chia công việc để
code; không thay scope hay contract.

Metadata không tham gia đường truyền thử nghiệm M1: script/test đóng vai client
gRPC. Upload/download REST, placement/RF, health detector, replica status trong
DB và repair/cleanup toàn hệ thống thuộc M2–M4. M1 chỉ chứng minh persistence và
integrity của một node, chưa chứng minh replication/failover.

## 2. Điểm xuất phát và phần sẽ sửa

| Thành phần hiện có | Việc M1 cần làm |
|---|---|
| `backend/storage/service.py` | Implement Store/Get/Delete; giữ HealthCheck và hoàn thiện metrics |
| `backend/storage/server.py` | Giữ threadpool, bind từ env, message limits và shutdown; nối lifecycle nếu cần |
| `backend/storage/config.py` | Giữ config; bổ sung kiểm tra startup cho data directory/chunk limit nếu cần |
| `backend/contracts/`, `backend/generated/` | Giữ wire contract và stubs hiện tại |
| `backend/tests/test_base.py` | Thay assertion data RPC UNIMPLEMENTED bằng hành vi đúng của M1; giữ các checks M0 khác |
| `deploy/compose.local.yml` | Tận dụng ba volume độc lập; chỉ chỉnh khi kiểm tra M1 cần |
| `README.md`, docs | Ghi lệnh, kết quả thực tế và milestone đã hoàn thành |

Các file mới dự kiến: `backend/storage/validation.py`, `backend/storage/chunk_store.py`,
`backend/tests/test_storage.py`, `backend/tests/test_storage_failures.py`,
`backend/scripts/smoke_storage.py`. Có thể gộp/tách helper khi code nếu hành vi giữ nguyên;
không dựng repository framework hay abstraction cho nhiều storage backend.

## 3. Chia phase và thứ tự

| Phase | Nội dung | Phụ thuộc | Điều kiện chuyển bước |
|---|---|---|---|
| P1 | Validation, layout và startup filesystem | M0 | Input sai bị chặn; chỉ chunk đã commit được nhận diện |
| P2 | StoreChunk atomic và idempotent | P1 | Lưu/ack đúng; retry đúng; conflict không overwrite |
| P3 | GetChunk và DeleteChunk | P2 | Đọc đúng bytes/hash; delete lặp lại thành công |
| P4 | Metrics, lỗi và concurrency/cancel | P2–P3 | Health không bị transfer lock chặn; failure checks qua |
| P5 | gRPC integration, restart và volume | P1–P4 | Smoke qua server thật; restart giữ bytes; node khác không có chunk |
| P6 | Tổng kiểm tra và bàn giao M2 | P5 | DoD M1 đủ bằng chứng, docs cập nhật |

Làm tuần tự. Test của hành vi nào viết/chạy trong phase đó; P5 tổng hợp kiểm tra
qua process/container. Ước lượng vẫn nằm trong khoảng 2 buổi của milestone gốc,
có thể cần thêm thời gian cho các lỗi concurrency. Không gắn từng phase với một
ngày hoàn thành cố định.

## 4. P1 — Validation, layout và startup

### Đầu việc

- Validate `chunk_id` bằng parse UUID và so lại chuỗi canonical lowercase;
  reject UUID viết hoa, dạng rút gọn, path traversal và chuỗi không phải UUID.
- Store validate checksum là lowercase hex 64 ký tự và bằng SHA-256(data).
- Store chỉ nhận `1 <= len(data) <= CHUNK_SIZE_BYTES`; hỗ trợ chunk cuối nhỏ.
  File rỗng được Metadata xử lý ở M3, không gửi chunk rỗng cho Storage.
- Quy ước path committed: `DATA_DIR/<canonical_uuid>.chunk`.
  File tạm dùng prefix riêng, ví dụ `.store-`, trong chính DATA_DIR.
- Một Storage process sở hữu một DATA_DIR. Không chạy hai process cùng volume;
  mutex trong process không thay thế lock giữa nhiều process.
- Startup tạo/kiểm tra directory, dọn đúng tempfiles Store thuộc prefix đã chọn
  khi process trước đã dừng; không xóa committed chunks hoặc file lạ.
- Startup quét committed files một lần để khởi tạo `used_bytes` và kiểm tra
  chunk lớn nhất đã lưu không vượt storage limit hiện tại. Nếu giảm limit dưới
  chunk đã có, fail rõ thay vì âm thầm làm dữ liệu cũ không phục vụ được.

### Kiểm tra và gate

- Input sai trả `INVALID_ARGUMENT`, không tạo chunk hay tempfile sót lại.
- Payload 1 byte, đúng 2 MiB và vượt limit 1 byte cho kết quả đúng.
- Tempfile cũ được dọn; committed file còn nguyên.
- Config/data directory không dùng được phải báo lỗi rõ; không nhận storage
  path từ filename của người dùng.

## 5. P2 — StoreChunk atomic và idempotent

### Luồng thực hiện

1. Validate UUID, payload và hash trước khi đụng filesystem.
2. Lấy một mutex bảo vệ chunk operations của node; kiểm tra context còn active.
3. Nếu committed path đã tồn tại, đọc actual bytes và tính hash. Chỉ trả OK với
   `already_existed=true` khi bytes trùng payload; khác bytes trả `ALREADY_EXISTS`.
   Không chỉ tin checksum field hay sidecar và không overwrite replica corrupt.
4. Nếu chưa có, ghi tempfile cùng filesystem; flush và fsync hoàn tất.
5. Ngay trước atomic replace, kiểm tra context còn active. Request đã hết
   deadline/cancel trước điểm này không được tiếp tục commit. Dọn tempfile.
6. Atomic replace dưới cùng mutex; cập nhật cached used_bytes theo chunk mới.
7. Trả ack khớp chunk_id, actual size, actual checksum và `already_existed=false`
   sau commit. Dọn file tạm ở nhánh lỗi; không rollback committed bytes chỉ vì
   response sau commit bị mất.

Giữ mutex qua kiểm tra tồn tại và commit để hai Store cùng ID không ghi đè nhau.
Get/Delete dùng cùng mutex ở P3. Không giữ lock riêng cho từng loại RPC.

Deadline có thể hết ngay sau lần kiểm tra context hoặc sau commit; outcome vẫn
có thể chưa biết ở client. Đây là lý do retry phải cùng ID/payload và coordinator
ở M3 phải ghi PENDING trước RPC. Không hứa timeout đồng nghĩa chưa ghi dữ liệu.

### Kiểm tra và gate

- Store lần đầu rồi đọc file committed: bytes/hash/size đúng; ack đúng fields.
- Store cùng ID/cùng bytes: OK, already_existed=true, không nhân đôi used_bytes.
- Store cùng ID/bytes khác với checksum hợp lệ của payload mới: ALREADY_EXISTS;
  bytes cũ giữ nguyên.
- Gây lỗi write/fsync/replace: không ack OK và không public file ghi dở.
- Hai Store cạnh tranh cùng ID: chỉ một nội dung committed; nội dung khác conflict.

## 6. P3 — GetChunk và DeleteChunk

### GetChunk

- Validate UUID; lấy mutex rồi đọc snapshot của committed bytes; không phục vụ
  tempfile. Nhả mutex sau khi có snapshot, trước phần trả response nếu phù hợp.
- Trả chunk_id, actual data và SHA-256 tính từ bytes đọc được.
- Path không tồn tại: NOT_FOUND. Local bytes không đọc được/lỗi dữ liệu có thể
  nhận biết: DATA_LOSS theo contract; không trả response bytes thiếu như thành công.
- Node không biết expected hash/size trong DB. Nếu file bị sửa nhưng vẫn đọc
  được hợp lệ, Get có thể OK kèm hash mới; Metadata ở M3 phát hiện mismatch.
  Không phát minh full integrity validation mà node không có nguồn so sánh.

### DeleteChunk

- Validate UUID; dùng cùng mutex với Store/Get.
- Có chunk và unlink thành công: OK, existed=true; cập nhật used_bytes sau xóa.
- Không tồn tại: OK, existed=false; gọi lần hai không là lỗi NOT_FOUND.
- Lỗi I/O khi xóa: không ack success, không trừ used_bytes như đã hoàn tất.
- Node không xem file state hay metadata DB để quyết định xóa.

### Kiểm tra và gate

- Store → Get trả đúng byte-for-byte và SHA-256, kể cả payload 2 MiB.
- Get ID chưa tồn tại trả NOT_FOUND; input sai trả INVALID_ARGUMENT.
- Sửa bytes trên disk: Get trả bytes/hash thực tế, không hash của lần Store trước.
- Delete hai lần: existed=true rồi false; Get sau Delete trả NOT_FOUND.
- Gây lỗi đọc/xóa: status đúng, metrics không báo thao tác đã hoàn tất.

## 7. P4 — Health, metrics và failure/concurrency checks

### Health và metrics

- Giữ identity/domain từ env và probe writable bằng tempfile nhỏ được dọn ngay.
- capacity/available lấy filesystem snapshot; used_bytes tính committed chunks
  lúc startup rồi cập nhật sau Store mới/Delete thành công dưới synchronization.
- Duplicate Store và Delete absent không đổi count; tempfile không được tính.
- Health đọc cached stats qua critical section ngắn hoặc snapshot an toàn;
  không đợi mutex đang bao toàn bộ Store/Get/Delete và không walk directory
  ở mỗi request. Health không xác minh checksum tất cả chunks.

### Error mapping

| Trường hợp | gRPC status/hành vi |
|---|---|
| ID/hash/payload sai hoặc Store vượt chunk limit | INVALID_ARGUMENT |
| ID đã có bytes khác | ALREADY_EXISTS |
| Get không có committed chunk | NOT_FOUND |
| Local data không đọc được/lỗi dữ liệu node nhận biết | DATA_LOSS |
| Hết disk/quota khi ghi | RESOURCE_EXHAUSTED |
| Storage directory không writable/không ready | FAILED_PRECONDITION |
| Lỗi I/O khác hoặc lỗi ngoài dự kiến | INTERNAL; log nội bộ, không lộ raw path/stack trace qua RPC |
| Context đã cancel/deadline hết trước commit | Dừng ghi và dọn tempfile; transport có thể báo CANCELLED/DEADLINE_EXCEEDED |

Không bắt broad exception rồi luôn trả INTERNAL cho mọi tình huống. Abort của
gRPC cũng không được bị catch và đổi thành lỗi khác. Storage không tự retry RPC;
retry wrapper của Metadata thuộc milestone sau.

### Failure checks bắt buộc

- Giữ mutex bằng barrier/event, để Store đợi rồi cancel/expire context: sau khi
  nhả mutex không có commit trễ; mô phỏng thêm cancel sau fsync trước replace.
- Store đã commit nhưng client mất ack: retry cùng payload nhận already_existed;
  Delete tiếp theo thành công. M1 kiểm tra ở node; PENDING/cleanup DB thuộc M3–M4.
- Giữ Store ở điểm trước commit, gọi Get/Delete: hai RPC chỉ quan sát một thứ tự
  hợp lệ, không thấy partial file. Không bắt test vào một thứ tự thread cụ thể.
- Health vẫn phản hồi trong khi chunk mutex đang bị giữ.
- Fault injection lỗi disk dùng monkeypatch filesystem tại ranh giới thao tác;
  test directory permission bằng chmod đơn thuần không đáng tin khi container
  chạy root. Không thêm endpoint/env fault injection vào production server.

Gate: tests xác nhận trạng thái filesystem, metrics và response sau lỗi; dùng
barrier/event và deadline hữu hạn thay sleep ngẫu nhiên để tránh test chập chờn.

## 8. P5 — Integration qua gRPC, restart và volume

### Script smoke dự kiến

`backend/scripts/smoke_storage.py` sẽ nhận target, chunk ID và mode thao tác từ CLI.
Client dùng stubs/message limits hiện có, disable automatic retry, đặt deadline
hữu hạn. Fixture bytes sinh xác định, mặc định 2 MiB; hash do client tính.

Tách các bước để có thể restart server giữa chúng:

1. **store:** gửi chunk mới, kiểm tra ack, ghi lại ID/checksum; cùng ID retry đúng.
2. **verify:** Get và so byte-for-byte/hash với fixture.
3. Restart storage-node-1, giữ volume; chờ container healthy.
4. **verify** lần nữa với ID cũ; bytes/hash giữ nguyên, used_bytes được khôi phục.
5. Get ID đó trên storage-node-2 trả NOT_FOUND, chứng minh hai volume độc lập.
6. **delete:** xóa node-1 hai lần, xác nhận Get NOT_FOUND và metrics trở về baseline.

Chỉ xóa chunk UUID do lượt smoke tạo; không dọn cả volume hay dùng `down -v`.
Chạy client trong network Compose để tránh vấn đề gRPC loopback Windows đã ghi
trong README. Docker Linux là môi trường kiểm chứng chính; native Windows có thể
được điều tra riêng, không làm điều kiện bắt buộc mới cho M1.

### Startup recovery của Storage

- Test restart bình thường với chunk đã commit còn đọc được.
- Test process kill lúc đã fsync tempfile nhưng chưa replace bằng test harness:
  committed ID chưa xuất hiện, tempfile được dọn khi server khởi động lại.
- Khi lỗi/crash ở thời điểm commit chưa biết, file committed nếu có phải là một
  chunk hoàn chỉnh; retry/verify cùng ID xử lý outcome. Không tuyên bố bảo đảm
  trước mọi dạng mất điện hoặc hỏng phần cứng.

Gate: cả RPC smoke và restart thật qua; kiểm tra identity, named volume, checksum
và cleanup fixture. Không suy ra persistence chỉ từ việc tạo lại service object.

## 9. P6 — Tổng kiểm tra và cập nhật tài liệu

Giữ nguyên các checks M0 và PostgreSQL bootstrap hiện tại. Khi M1 implement,
thay test UNIMPLEMENTED bằng Get missing NOT_FOUND và các tests data RPC mới;
không giữ assertion của trạng thái skeleton.

Các lệnh hiện có dùng để kiểm tra sau khi code M1:

```powershell
Push-Location backend
.\.venv\Scripts\python.exe -m ruff check common metadata storage scripts tests
.\.venv\Scripts\python.exe -m ruff format --check common metadata storage scripts tests
Pop-Location
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec metadata python scripts/smoke_base.py
```

Lệnh cụ thể cho smoke Storage/restart được chốt và ghi vào README khi script
P5 tồn tại. Không ghi kết quả pass hoặc đánh dấu milestone trước khi chạy thật.
Nếu sửa code sau một failure, chạy lại check liên quan rồi bộ tích hợp cần thiết.

## 10. Definition of Done và tiến độ

- [x] **P1:** validation, layout và startup cleanup hoạt động (03/10/2026).
- [ ] **P2:** Store atomic, checksum, idempotency và conflict đúng.
- [ ] **P3:** Get snapshot/hash actual bytes, Delete idempotent đúng.
- [ ] **P4:** error mapping, metrics, cancel và concurrency checks qua.
- [ ] **P5:** 2 MiB RPC roundtrip → restart thật → đọc lại → Delete hai lần qua.
- [ ] **P5:** volume node-1/node-2 độc lập; tempfile sau process crash được dọn.
- [ ] **P6:** checks M0 còn qua; lint/format qua; setup/smoke guide ghi bằng chứng.
- [ ] **M1 hoàn thành:** mọi gate trên đạt, không còn data RPC UNIMPLEMENTED.

Khi bàn giao M2, Storage có bốn RPC chạy thật, stable node identity/data path,
message limits thống nhất và failure semantics đủ cho Metadata coordinator.
M2 tiếp tục health polling/trạng thái node trên base DB/Compose đã có; không cần
viết lại kế hoạch kiến trúc trước mỗi RPC.

### Bằng chứng P1 — 03/10/2026

`backend/storage/validation.py` kiểm tra UUID/hash/payload; `backend/storage/chunk_store.py`
quản lý layout và startup; ba data RPC trả INVALID_ARGUMENT cho request sai,
request hợp lệ vẫn UNIMPLEMENTED để giữ phạm vi P1.

Layout committed: `<canonical_uuid>.chunk`. Tempfile Store dành riêng:
`.store-<32 lowercase hex>.tmp`, tạo tên trong cùng DATA_DIR; startup chỉ dọn
regular files khớp chính xác mẫu này. Không theo symlink, không dọn directory
hay tên lạ. Một process sở hữu mỗi DATA_DIR; process trước phải dừng trước startup.

58 tests qua trong Docker Linux (bao gồm checks M0 và 51 cases P1); lint/format
qua. Các cases P1 kiểm tra biên 1 byte/2 MiB/limit+1, hash/UUID sai qua RPC,
không có filesystem artifacts sau reject, temp cleanup, accounting startup,
giảm chunk limit và filesystem không dùng được. M1 chưa hoàn thành.
