# API contracts V1

**Ngày chốt:** 02/10/2026 · REST cho React, gRPC cho Storage · Đọc cùng `ARCHITECTURE.md`.

Đây là contract trước triển khai. Các JSON dưới đây là mẫu shape; ID, timestamp và checksum là ví dụ. API thực tế phải được kiểm tra bằng integration test, không coi tài liệu này là bằng chứng đã có server chạy.

## 1. Quy ước chung

- REST prefix `/api/v1`; UTF-8 JSON, trừ multipart upload và binary download.
- file_id/chunk_id: UUID chuẩn lowercase; node_id: cấu hình ổn định, 1–64 ký tự `[a-zA-Z0-9_-]`.
- Timestamp: RFC 3339 UTC, ví dụ `2026-10-02T16:00:00Z`.
- Size: integer bytes không âm. SHA-256: lowercase hex 64 ký tự.
- Enum state dùng UPPERCASE. Success không có wrapper `success: true`.
- Tên file trùng nhau được phép. Tên basename 1–255 ký tự, loại control characters; bỏ path và chuẩn hóa cả slash/backslash. Không dùng tên này làm storage path. Content-Disposition encode an toàn, không ghép chuỗi header từ input thô.
- Content type là hint từ upload, không chứng minh nội dung. Mặc định download `application/octet-stream`.
- Không có auth/account trong V1. Các endpoint admin chỉ nằm trong LAN demo.

Error JSON chuẩn cho lỗi ứng dụng, kể cả request validation do FastAPI:

```json
{
  "error": {
    "code": "CHUNK_UNAVAILABLE",
    "message": "Không đọc được một phần của file. Hãy kiểm tra các Storage Node.",
    "details": {"file_id":"11111111-1111-4111-8111-111111111111", "chunk_index":2}
  }
}
```

message phục vụ người dùng; code ổn định cho frontend; details có thể `{}` và không chứa stack trace/password. Nếu reverse proxy hoặc kết nối mạng lỗi trước app, response có thể không theo envelope; frontend vẫn phải xử lý lỗi network/non-JSON.

| HTTP | Error code | Ý nghĩa |
|---:|---|---|
| 400 | INVALID_REQUEST | Multipart sai hoặc input logic không hợp lệ |
| 404 | FILE_NOT_FOUND | Không có file, hoặc tombstone DELETED với endpoint đọc |
| 409 | FILE_NOT_READY | File đang UPLOADING hoặc FAILED |
| 409 | FILE_DELETING | File đang xóa |
| 409 | OPERATION_BUSY | Một thao tác transfer/delete/repair khác đang chạy |
| 413 | FILE_TOO_LARGE | Size thật vượt MAX_FILE_SIZE_BYTES |
| 422 | VALIDATION_ERROR | UUID/query/body không đạt schema |
| 503 | INSUFFICIENT_NODES | Không đủ node cho RF |
| 503 | UPLOAD_REPLICATION_FAILED | Một hoặc nhiều chunk không đạt RF; có file_id nếu đã tạo |
| 503 | CHUNK_UNAVAILABLE | Không có replica đọc hợp lệ của một chunk |
| 503 | INTEGRITY_CHECK_FAILED | Full-file size/hash không khớp sau assemble |
| 503 | TEMP_STORAGE_UNAVAILABLE | Metadata không đủ disk tạm |
| 503 | METADATA_UNAVAILABLE | App còn phản hồi nhưng DB không dùng được |
| 500 | INTERNAL_ERROR | Lỗi ngoài dự kiến; log nội bộ theo request_id nếu có |

Không dùng 200 kèm error JSON cho request thất bại. Repair trả 200 summary ngay cả khi một số chunk không sửa được vì đó là kết quả một lượt kiểm tra; request không bắt đầu được vẫn trả 409/503.

## 2. REST endpoints

| Method | Path | Mục đích | Success |
|---|---|---|---|
| POST | `/files` | Upload multipart một file | 201 FileSummary |
| GET | `/files` | List file | 200 FileList |
| GET | `/files/{file_id}` | Metadata file và trạng thái quan sát | 200 FileDetail |
| GET | `/files/{file_id}/chunks` | Chunk, replica và placement | 200 ChunkList |
| GET | `/files/{file_id}/download` | Assemble rồi trả file | 200 binary |
| DELETE | `/files/{file_id}` | Xóa logic và bắt đầu cleanup | 200 hoặc 202 DeleteResult |
| GET | `/nodes` | Node status/capacity | 200 NodeList |
| GET | `/cluster` | Summary cluster và cấu hình demo | 200 ClusterSummary |
| POST | `/admin/repair` | Một lượt repair có giới hạn | 200 RepairResult |
| GET | `/health/live` | Metadata process đang phản hồi | 200 |
| GET | `/health/ready` | DB/config/khởi tạo xong (gồm download temp directory), health scheduler và data RPC client running | 200 hoặc 503 |

Paths trong bảng nối sau `/api/v1`. Không tạo REST endpoints trên Storage Nodes. Không cần node registration API; registry lấy từ config.

### 2.1 FileSummary và upload

`POST /files`: `multipart/form-data`, **một field `file` bắt buộc**; không kèm JSON body. Không nhận chunk size/RF từ browser. Reject nhiều file trong cùng request. Đếm actual file bytes khi parse multipart để dừng và trả 413 FILE_TOO_LARGE khi vượt limit, rồi kiểm tra actual spool size trước coordinator; không tin Content-Length là size của riêng file. Charset không tồn tại hoặc không dùng được để decode multipart trả 400 INVALID_REQUEST. Cancel khi spool I/O đang chạy phải drain I/O trước khi đóng spool.

201 trả sau commit AVAILABLE; `Location: /api/v1/files/{id}`:

```json
{
  "file_id":"11111111-1111-4111-8111-111111111111",
  "original_name":"report.pdf",
  "content_type":"application/pdf",
  "size_bytes":5242880,
  "chunk_size_bytes":2097152,
  "total_chunks":3,
  "replication_factor":2,
  "checksum_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "status":"AVAILABLE",
  "created_at":"2026-10-02T16:00:00Z"
}
```

FileSummary cũng áp dụng cho list, nhưng checksum nullable cho file chưa commit. `created_at` là lúc tạo file record, không phải lúc request body bắt đầu qua mạng.

Upload thất bại sau khi tạo metadata: details có file_id để tra GET detail. Không expose partial file để download. POST không idempotent; frontend không tự retry toàn POST khi timeout. Dùng list để kiểm tra trước khi người dùng thử lại.

Browser progress qua XHR/Axios chỉ đo bytes gửi HTTP, chưa phản ánh replication commit. Khi upload progress 100%, UI đổi sang "Đang lưu các bản sao"; chỉ báo hoàn tất sau 201. File rỗng: 201, size=0, total_chunks=0, full SHA-256 bytes rỗng.

### 2.2 List và detail

`GET /files?limit=50&offset=0&include_inactive=false`:

- limit 1..100; offset >=0; mặc định chỉ AVAILABLE.
- include_inactive=true thêm UPLOADING/FAILED/DELETING, vẫn ẩn DELETED.
- Sắp xếp created_at DESC, file_id DESC. Offset pagination có thể thay đổi khi có upload/delete; frontend refresh list sau thao tác, không dùng nó làm snapshot.

```json
{"items":[], "total":0, "limit":50, "offset":0}
```

items là FileSummary. List không tính replica statistics cho từng file để tránh query thừa.

`GET /files/{file_id}` trả mọi FileSummary fields và:

```json
{
  "known_readable":true,
  "under_replicated_chunks":1,
  "unavailable_chunks":0,
  "domain_degraded_chunks":1,
  "over_replicated_chunks":0,
  "cleanup_pending_replicas":0,
  "error_code":null
}
```

Đoạn trên là fields bổ sung, không phải response thay cho FileSummary. known_readable chỉ true cho AVAILABLE và mọi chunk có >=1 cached VERIFIED trên node ACTIVE; file AVAILABLE rỗng là true. Nếu false vẫn có thể đọc được sau probing replica; nếu true request sau vẫn có thể thất bại. Counters tính trên chunk đã tạo; với UPLOADING không diễn giải thành durability đã commit. DELETING vẫn được xem detail để theo dõi cleanup, nhưng không download. DELETED trả 404 ở endpoint đọc.

### 2.3 Chunk placement

`GET /files/{file_id}/chunks`: trả theo chunk_index tăng dần; tối đa số chunk trong file. Limit 64 MiB và min chunk 256 KiB giới hạn file ở 256 chunk. Nếu tăng file limit sau V1, xem xét pagination.

```json
{
  "file_id":"11111111-1111-4111-8111-111111111111",
  "items":[{
    "chunk_id":"22222222-2222-4222-8222-222222222222",
    "chunk_index":0,
    "size_bytes":2097152,
    "checksum_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "state":"UNDER_REPLICATED",
    "live_replica_count":1,
    "live_failure_domain_count":1,
    "domain_degraded":true,
    "over_replicated":false,
    "replicas":[{
      "node_id":"node-1",
      "failure_domain":"machine_A",
      "node_status":"ACTIVE",
      "status":"VERIFIED",
      "cleanup_pending":false,
      "last_verified_at":"2026-10-02T16:00:00Z",
      "last_error":null
    }]
  }]
}
```

Replica list bao gồm mapping trên node DOWN, PENDING hoặc DELETED để quan sát, không lọc chúng thành giả như chưa từng được ghi. Các count chỉ tính VERIFIED + enabled + ACTIVE, không cleanup_pending. Không gọi chunk LOST khi mọi node tạm unreachable. Chunk state cho FAILED/DELETING là `INACTIVE`; trạng thái không phục vụ download. Với UPLOADING là PENDING. Định nghĩa domain_degraded theo ARCHITECTURE.

### 2.4 Download

`GET /files/{file_id}/download`: không cần body. Chỉ AVAILABLE; file khác trả code ở bảng lỗi. Assemble vào tempfile, verify mọi chunk và full-file hash trước khi trả 200. Response headers:

```http
Content-Type: application/octet-stream
Content-Length: 5242880
Content-Disposition: attachment; filename*=UTF-8''report.pdf
X-File-Checksum-SHA256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

Header checksum để client/script verify; V1 không cần ETag, Range, resumable download. Nếu origin khác frontend, CORS expose Content-Disposition và X-File-Checksum-SHA256. HTTP connection có thể đứt sau khi 200; client vẫn kiểm tra bytes nhận đủ. Không hứa một response không bao giờ bị network failure.

Frontend có thể dùng binary blob để save, vì file đã giới hạn 64 MiB; không nhầm blob phía browser với cách Metadata xử lý toàn file. UI hiển thị giai đoạn chuẩn bị trước khi nhận body. Không cần tự chế upload/download server percentage endpoint.

### 2.5 Delete

`DELETE /files/{file_id}` không body. AVAILABLE hoặc FAILED được xóa. UPLOADING trả 409 FILE_NOT_READY (hoặc OPERATION_BUSY nếu lock đang có). File DELETING lặp lại trả 202 trạng thái hiện tại, file DELETED lặp lại trả 200; ID chưa tồn tại trả 404. Delete idempotent theo file_id.

```json
{
  "file_id":"11111111-1111-4111-8111-111111111111",
  "status":"DELETING",
  "cleanup_pending_replicas":2
}
```

202 khi còn pending; 200 khi DELETED và pending=0. Xóa logic trước khi RPC; file ẩn và không download/repair ngay. Đừng báo "Đã xóa mọi bản sao" khi mới có 202. UI có thể nói "Đã xóa file, đang dọn các bản sao còn lại" và tra detail nếu cần. Metadata của cleanup giữ qua restart.

### 2.6 Nodes, cluster và health

`GET /nodes`:

```json
{
  "items":[{
    "node_id":"node-1",
    "host":"storage-node-1",
    "port":50051,
    "failure_domain":"machine_A",
    "enabled":true,
    "status":"ACTIVE",
    "last_success_at":"2026-10-02T16:00:00Z",
    "last_error":null,
    "capacity_bytes":107374182400,
    "available_bytes":53687091200,
    "used_bytes":2097152
  }]
}
```

Capacity/free là filesystem snapshot; used_bytes là committed bytes trong DATA_DIR. Hai node cùng disk có thể báo trùng capacity/free; **không cộng capacity của các node thành dung lượng cluster**. Metrics chưa có health success trả null ở REST (proto không có null). Status lấy từ detector, không gọi health RPC đồng bộ cho từng GET.

`GET /cluster`:

```json
{
  "chunk_size_bytes":2097152,
  "default_replication_factor":2,
  "max_file_size_bytes":67108864,
  "nodes":{"active":3,"suspected":0,"down":0,"disabled":0},
  "configured_failure_domains":2,
  "active_failure_domains":2,
  "files_available":1,
  "under_replicated_chunks":0,
  "unavailable_chunks":0,
  "domain_degraded_chunks":0,
  "over_replicated_chunks":0,
  "cleanup_pending_replicas":0,
  "operation_busy":false
}
```

Node state counts active/suspected/down chỉ tính enabled; disabled tính riêng. Chunk counters cluster chỉ tính files AVAILABLE; cleanup counter tính mọi file chưa dọn hết. operation_busy là snapshot của lock.

`GET /health/live` → `{"status":"LIVE"}`. `/health/ready` → `{"status":"READY"}` khi DB ping thành công, startup recovery và khởi tạo download temp directory/dọn stale temp hoàn tất, health scheduler và data RPC client đang running. DB/config/temp directory chưa ready hoặc scheduler/client đã dừng/lỗi trả 503 METADATA_UNAVAILABLE. Client running nghĩa channels đã khởi tạo và chưa closing/closed, không yêu cầu kết nối thành công tới mọi node. Ready không yêu cầu tất cả node ACTIVE: data plane degraded vẫn cần API để xem và điều khiển. GET nodes/cluster và files/list/detail/chunks vẫn cho đọc snapshot sau startup khi scheduler/client lỗi, miễn DB còn dùng được.

### 2.7 Repair

`POST /admin/repair`, JSON body:

```json
{
  "file_id":null,
  "node_id":null,
  "max_chunks":8,
  "after":null
}
```

file_id/node_id optional null; max_chunks 1..8, mặc định 8. file_id có giá trị chỉ quét file đó; node_id chỉ chọn chunk có known mapping tới node đó, dùng cho kiểm tra khi node trở lại. Có thể phối hợp cả hai filter. ID không tồn tại trả 404 FILE_NOT_FOUND hoặc 422 VALIDATION_ERROR cho node_id. File ngoài AVAILABLE trả 409.

Không chỉ quét under-replicated cached state: mỗi selected chunk phải được probe để tìm replica MISSING/corrupt trên node vừa trở lại. Source có checksum hợp lệ là điều kiện repair. Không có source → UNAVAILABLE; không đủ destination → NO_DESTINATION. Khi mapping tới node DOWN, không tính live nhưng không xóa row. Node vắng config không được gọi RPC.

Scan order: file_id ASC (UUID), chunk_index ASC. `after` là object `{ "file_id":"...", "chunk_index":7 }` của chunk cuối đã kiểm tra. `next_after` trả cursor đó nếu còn chunk trong scope, null khi kết thúc. Cursor không là job ID. Client giữ nguyên filter trong một vòng scan; thay filter thì reset after. Khi file bị delete giữa hai request, chunk đó tự ra khỏi scope.

```json
{
  "checked_chunks":8,
  "repaired_replicas":3,
  "remaining_chunks":12,
  "next_after":{"file_id":"11111111-1111-4111-8111-111111111111","chunk_index":7},
  "results":[{
    "file_id":"11111111-1111-4111-8111-111111111111",
    "chunk_id":"22222222-2222-4222-8222-222222222222",
    "chunk_index":0,
    "outcome":"REPAIRED",
    "live_replica_count":2,
    "domain_degraded":false,
    "message":null
  }]
}
```

outcome: HEALTHY, REPAIRED, NO_DESTINATION, UNAVAILABLE, ERROR. Results có đúng checked_chunks phần tử. `remaining_chunks` là số chunk còn **chưa quét sau cursor**, không là số under-replicated đã đo. Khi một chunk đã thử nhưng lỗi, cursor vẫn tiến; chạy vòng scan mới để retry chunk đó. `repaired_replicas` chỉ đếm Store được ack xác nhận. Probe phát hiện replica cũ còn đúng không được tính thành một bản sao mới.

Lượt ngừng bắt đầu chunk mới sau budget 30 giây, có thể trả ít hơn max_chunks. Số lần RPC/node/chunk hữu hạn, timeout 5 giây, tối đa 2 attempts. HTTP client dành khoảng 180 giây cho lượt repair. UI không retry tự động POST khi timeout: có thể đã ghi thành công; refresh placement trước rồi scan lại.

## 3. gRPC contract

`storage.proto` là wire contract duy nhất; package `dfs.storage.v1`, service `StorageService`. Bốn method đều unary. Mọi thất bại dùng gRPC status chuẩn, không trả object `success=false` với status OK. Không thêm ReplicateChunk/Heartbeat/RegisterNode trong V1.

### StoreChunk

Request: chunk_id, data, checksum_sha256. Validation: UUID canonical, hash đúng format và SHA-256(data) khớp, 1 <= len(data) <= configured CHUNK_SIZE_BYTES. Metadata file rỗng không gọi Store. Chunks của file cũ nhỏ/lớn hơn current default chỉ được phục vụ nếu nằm trong node limit đã cấu hình; không giảm storage limit dưới chunk lớn nhất đã lưu.

Response: chunk_id, size_bytes, checksum_sha256, already_existed. Node không cần file_id/chunk_index; coordinator giữ global mapping. Ack chỉ sau atomic commit file local. Same ID+same data → OK; same ID+different bytes → ALREADY_EXISTS. Không overwrite. Không lưu checksum sidecar làm nguồn duy nhất: Get và duplicate Store tính hash từ actual bytes.

### GetChunk

Request chunk_id. Response chunk_id, data, checksum_sha256 computed from bytes. Metadata so sánh hash/size với DB, không chỉ so hash với field node trả. Node không biết expected hash của metadata nên có thể trả OK cho bytes đã bị sửa; Metadata phát hiện CORRUPTED và fallback.

Node chụp bytes dưới mutex bảo vệ chunk operation, không đọc file đang ghi dở. NOT_FOUND khi path không tồn tại; DATA_LOSS cho file local unreadable/truncated theo lỗi filesystem mà node có thể nhận biết; lỗi bytes hash khác DB thường do Metadata phát hiện.

### DeleteChunk

Request chunk_id. Response chunk_id, existed. Không có path → OK existed=false. Có path và xóa thành công → OK existed=true. Nếu xóa lỗi I/O, trả lỗi, không ack hoàn tất. Không có điều kiện file status ở node; Metadata chịu trách nhiệm chỉ gọi Delete đúng luồng.

### HealthCheck

Request empty. Response node_id/failure_domain/storage_writable/capacity_bytes/available_bytes/used_bytes. Không trả timestamp để Metadata dùng quyết định timeout. Custom method của dự án, không tuyên bố đây là service `grpc.health.v1.Health` chuẩn.

Check writable bằng probe tempfile nhỏ trong DATA_DIR (hoặc cached probe hợp lệ trong chu kỳ health), cleanup probe ngay. used_bytes có thể cache; không walk toàn filesystem ở mỗi health request. Health không verify toàn bộ chunk.

### Error mapping và retry

| gRPC status | Tình huống | Coordinator |
|---|---|---|
| INVALID_ARGUMENT | ID/hash/bytes sai, chunk vượt limit | Không retry; lỗi input/code/config |
| ALREADY_EXISTS | Same chunk ID có bytes khác | Không retry Store; lỗi conflict |
| NOT_FOUND | Get không có chunk | MISSING, dùng replica khác |
| DATA_LOSS | Node nhận biết dữ liệu local không đọc đúng | CORRUPTED, dùng replica khác |
| RESOURCE_EXHAUSTED | Hết disk hoặc giới hạn message do runtime | Không retry cùng destination; thử node khác nếu phù hợp |
| FAILED_PRECONDITION | Storage directory không writable/không ready | Không retry ngay; dùng node khác |
| UNAVAILABLE | Không kết nối được node | Tối đa 2 attempts, sau đó fallback |
| DEADLINE_EXCEEDED | Quá timeout; outcome có thể đã xảy ra | Retry cùng ID tối đa 2 attempts; giữ PENDING khi chưa xác nhận |
| INTERNAL | Lỗi I/O/không dự kiến | Log; không retry mù; fallback nếu có |
| CANCELLED | Caller hủy | Dừng thao tác; upload chưa commit → FAILED/cleanup |

Chỉ retry UNAVAILABLE/DEADLINE_EXCEEDED, một backoff 0.2 giây, không bật thêm gRPC automatic retry chồng lên wrapper. Delete NOT_FOUND nếu implementation lỡ trả về thì coordinator có thể xem là đã xóa, nhưng implementation chuẩn phải trả OK existed=false. Health không retry trong cùng lượt.

### Message size và stub generation

Cả gRPC server và client channel phải cấu hình:

```python
options = [
    ("grpc.max_send_message_length", 8 * 1024 * 1024),
    ("grpc.max_receive_message_length", 8 * 1024 * 1024),
]
```

Limit là toàn protobuf message, không chỉ bytes field. Mặc định chunk 2 MiB, cấu hình cho phép đến 4 MiB, message limit 8 MiB có phần dư cho fields. Không tăng chunk size tùy tiện chỉ vì đã tăng file limit.

Contract implementation nằm ở `backend/contracts/storage.proto`. Từ `backend/`,
tạo `generated/`, rồi:

```bash
python -m grpc_tools.protoc -I contracts --python_out=generated --grpc_python_out=generated contracts/storage.proto
```

Đặt `storage.proto` của bộ tài liệu vào `backend/contracts/storage.proto`.
Thêm `backend/` và `backend/generated/` vào PYTHONPATH cho cả Metadata và Storage;
generated module dùng absolute import `storage_pb2`, không sửa code sinh tự động
bằng tay. Lock các phiên bản grpcio/grpcio-tools/protobuf tương thích trong môi
trường dự án sau lần compile/import thành công; không chọn mỗi service một version riêng.

## 4. Điều kiện thay đổi contract

Proto, REST schema và implementation phải đổi cùng nhau. Không tái sử dụng field number đã xóa; reserve field khi cần. Thêm RPC, thay state, đổi transfer mode hoặc đổi commit semantics phải cập nhật tài liệu trước khi triển khai. Đổi tên biến nội bộ hoặc tách helper không cần xin phép lại nếu contract/behavior giữ nguyên.

Tham chiếu: [Python gRPC](https://grpc.io/docs/languages/python/basics/), [deadlines](https://grpc.io/docs/guides/deadlines/), [status codes](https://grpc.io/docs/guides/status-codes/), [FastAPI UploadFile](https://fastapi.tiangolo.com/tutorial/request-files/).
