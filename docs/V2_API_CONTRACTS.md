# Contract API V2 — mốc A

Ngày 22/10/2026. Đặc tả để triển khai, chưa phải endpoint đang hoạt động.
**Cập nhật C3 (24/10/2026):** phần upload public/internal và Transfer đã được
hiện thực riêng, có gate HTTP/PostgreSQL/gRPC và ingress thật. Các route
catalog/download/delete và read lease dưới đây vẫn là contract cho các mốc sau.
**Cập nhật D3 (28/10/2026):** catalog/metadata/chunks, DELETE, internal read
commands và Transfer HEAD download đã có implementation/evidence. GET binary
download/Range còn thuộc D4, dashboard nodes/cluster thuộc F–G; không suy ra các
route này đã hoạt động từ gate upload hoặc HEAD. Xem
[evidence D3](V2_D_IMPLEMENTATION_PLAN.md#5-evidence-d3--28102026).
**Cập nhật D4 (29/10/2026):** GET binary/Range đã nối Transfer–Storage, kiểm từng
chunk trước headers/bytes, fallback hữu hạn và renew read lease. Lỗi hết replica
trước binary headers trả 503 CHUNK_UNAVAILABLE với envelope/Retry-After; sau headers
ngắt response. Các lỗi lease nội bộ không expose qua GET public. Evidence tại
[D4](V2_D_IMPLEMENTATION_PLAN.md#6-evidence-d4--29102026); chưa có gate D5.
**Cập nhật E1 (29/10/2026):** HTTP/control admission dành slot hai chiều cho
new work/progress trong cùng tổng cap. 429 có thể xảy ra khi một nhóm đầy dù
phần dành riêng của nhóm kia còn trống; không queue hoặc tự retry mutation.
Xem [contract và evidence E1](V2_E_IMPLEMENTATION_PLAN.md) và
[baseline nhiều process D5](V2_D5_EVIDENCE.md); phạm vi E1 là admission theo process.
**Cập nhật E2 (30/10/2026):** audit session/claim/node accounting qua hai process,
commit crash, mất ACK và Storage restart có 22 integration pass; không đổi wire
contract/schema. Download/job vẫn cap theo process trong topology hữu hạn, chưa
có hard cap concurrency cố định toàn cluster. E3–E4 chưa nghiệm thu; xem
[quyết định và evidence E2](V2_E_IMPLEMENTATION_PLAN.md#contract-và-audit-e2--30102026).
Xem [runtime C3](V2_C3_RUNTIME.md) và [evidence C](V2_C_IMPLEMENTATION_PLAN.md).
Đọc cùng [thiết kế](V2_DESIGN.md), [schema](V2_METADATA_SCHEMA.md) và
[quyết định tính đúng](V2_CORRECTNESS.md). Wire source duy nhất cho V2 là
[`v2_storage.proto`](../backend/contracts/v2_storage.proto); không đổi proto V1.

## 1. Quy ước và giới hạn

Public prefix `/api/v2`, internal prefix `/internal/v2`. Một ingress cùng origin
route JSON tới API và `/api/v2/uploads/{id}/parts/{index}` cùng đường download
tới Transfer. Không sticky session. Internal routes và Storage không được ingress
public chuyển tiếp; network service boundary tách biệt với quyền truy cập kho chung.

UUID canonical lowercase; UTC RFC3339; SHA-256 lowercase hex 64 ký tự. JSON size
và index là integer chính xác trong 0..2^53-1, từ chối bool/float/string. Wire/DB
generation và byte counter trong 0..2^63-1; generation bắt đầu 1. Tên file basename
1..255 ký tự, loại control characters và cả slash/backslash; tên trùng được phép.
Content type chỉ là hint; download mặc định `application/octet-stream`.
JSON từ chối unknown fields; body lệnh tối đa 16 KiB; binary body đếm bytes thực.

Baseline cấu hình công bố qua `GET /config`: chunk 8388608 bytes, block tối đa
262144 bytes, RF=2, distinct domains=2, strict placement; tối đa 2 part/phiên
đang giữ claim; file tối đa 64 GiB và tối đa 131072 part (đều cấu hình được).
`part_count = ceil(size/chunk_size)`; file rỗng có 0 part. Server snapshot policy
vào session; browser không chọn RF/chunk size. Admission active session/stream,
buffered bytes và queue phải có giới hạn hữu hạn cấu hình theo deployment trước
readiness. Những con số này là baseline kỹ thuật, chưa có benchmark.

Error JSON dùng cho mọi lỗi app trước headers:

```json
{"error":{"code":"PART_BUSY","message":"Phần tệp đang được xử lý.","retryable":true,"details":{}}}
```

| HTTP | Code | Hành vi client |
|---|---|---|
| 400 | INVALID_REQUEST / INVALID_RANGE | Sửa request |
| 404 | UPLOAD_NOT_FOUND / FILE_NOT_FOUND | Không retry mù |
| 409 | IDEMPOTENCY_CONFLICT / PART_CONFLICT / UPLOAD_STATE_CONFLICT | Tra trạng thái, không thay bytes của part |
| 409 | FILE_NOT_READY | File chưa AVAILABLE, hỏi trạng thái upload |
| 409 | PART_BUSY / PARTS_INCOMPLETE / OUTCOME_PENDING | Retry sau `Retry-After`, hỏi trạng thái |
| 410 | UPLOAD_EXPIRED / UPLOAD_ABORTED | Tạo phiên mới nếu muốn gửi lại |
| 413 | FILE_TOO_LARGE / PART_TOO_LARGE | Sửa input |
| 412 | PRECONDITION_FAILED | ETag không đáp ứng If-Match |
| 416 | RANGE_NOT_SATISFIABLE | Range ngoài representation, dùng Content-Range để tra size |
| 422 | VALIDATION_ERROR / CHECKSUM_MISMATCH / PART_SIZE_MISMATCH | Sửa input; part đã claim vẫn cố định size/hash |
| 429 | RESOURCE_LIMIT | Backoff theo `Retry-After` |
| 503 | CAPACITY_UNAVAILABLE / INSUFFICIENT_DOMAINS / STORAGE_UNAVAILABLE | Có thể retry cùng session |
| 503 | METADATA_UNAVAILABLE / RECOVERY_REQUIRED | Dừng mutation, tra kết quả sau phục hồi |
| 503 | CHUNK_UNAVAILABLE | Download có thể thử lại cùng version/ETag |
| 500 | INTERNAL_ERROR | Log request ID, không suy ra request chưa commit |

429, lỗi busy/pending và 503 có `Retry-After` giây nguyên dương. Lỗi mạng/proxy
có thể ngoài envelope. Không trả JSON lỗi sau khi binary headers đã gửi: ngắt
response, không phát đủ Content-Length giả và không báo download thành công.

Admission E1 theo process có hai nhóm: progress gồm POST upload complete/abort,
DELETE file, internal upload part renew/confirm/uncertain và internal read GET
manifest/POST renew/release. Create upload, acquire read, claim mới, binary GET/
PUT và queries/polls khác là new work. Tổng mặc định 32, mỗi nhóm được dành tối
thiểu 8, mỗi nhóm tối đa 24; tổng vẫn tối đa 32. Hai budget HTTP/control cấu hình
riêng; không thay active-session/capacity counter PostgreSQL. Rejection trước
body/I/O không tạo thêm lease/claim. Route classification không thay kiểm DTO,
token hoặc recovery epoch. Request ID có cả trên rejection; HEAD không body.

## 2. Upload công khai

| Method | Path sau prefix | Kết quả |
|---|---|---|
| GET | `/config` | 200 policy/limits hiện tại |
| POST | `/uploads` | 201 tạo session; 200 replay |
| GET | `/uploads/{id}` | 200 UploadSession |
| GET | `/uploads/{id}/parts?after_index=-1&limit=100` | 200 trang các part đã có record |
| PUT | `/uploads/{id}/parts/{index}` | 200 PartResult sau metadata confirm commit |
| POST | `/uploads/{id}/complete` | 202 đang finalize; 200 nếu COMPLETED |
| POST | `/uploads/{id}/abort` | 202 đang cleanup; 200 nếu cleanup hoàn tất |

Tạo phiên bắt buộc header `Idempotency-Key` là UUID do client sinh duy nhất cho
operation tạo phiên. Scope `(namespace, CREATE_UPLOAD, key)`; giữ record suốt
namespace, không tự hết hạn rồi cho cùng key tạo file mới. Payload canonical là
bộ field đã validate/normalize, không phụ thuộc thứ tự JSON. Cùng key khác payload
trả 409, cùng payload trả cùng session/file ID và trạng thái hiện tại; kiểm replay
trước kiểm capacity mới. File và version ID được cấp khi tạo session, catalog
mặc định chỉ hiện AVAILABLE.

```json
{"original_name":"sample.bin","content_type":"application/octet-stream","size_bytes":8388609,"expected_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
```

UploadSession luôn có các field sau; timestamp/ID/hash trong ví dụ là minh họa:

```json
{"upload_id":"11111111-1111-4111-8111-111111111111","file_id":"22222222-2222-4222-8222-222222222222","version_id":"33333333-3333-4333-8333-333333333333","state":"OPEN","original_name":"sample.bin","content_type":"application/octet-stream","size_bytes":8388609,"expected_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","chunk_size_bytes":8388608,"part_count":2,"replication_factor":2,"min_distinct_domains":2,"max_parallel_parts":2,"confirmed_parts":0,"confirmed_bytes":0,"expires_at":"2026-10-23T00:00:00Z","created_at":"2026-10-22T00:00:00Z","completed_at":null,"error_code":null,"cleanup_pending":false}
```

Expiration baseline 24h từ create; không trượt theo GET/retry, không đổi khi replay.
FINALIZING không bị expiry dành cho OPEN; worker có retry/backlog policy riêng.
POST complete/abort dùng body `{}`; không nhận lại hash để thay identity nguồn.
`Location` của create trỏ GET upload; 202 complete cũng trỏ cùng resource.

### 2.1. Part và truyền lại

PUT dùng `application/octet-stream`, bắt buộc `X-Part-SHA256`. Chấp nhận HTTP body
không Content-Length nhưng vẫn giới hạn actual bytes; nếu có Content-Length phải
khớp expected part size. Index 0..part_count-1, kích thước bằng chunk size trừ part
cuối; không nhận part cho file rỗng. Claim đầu tiên cố định part hash, không sửa
hash bằng retry. Sai actual bytes trả 422, cleanup attempt và có thể gửi lại đúng
bytes với hash cũ. Hash header khác record trả 409 trước khi ghi.

PartResult có `index`, `size_bytes`, `checksum_sha256`, `state` và
`confirmed_at` (nullable). State là CLAIMED, RECONCILING, VERIFIED; chưa có record
thì part chưa gửi. GET parts phân trang index tăng dần, limit 1..1000, response
`{items,next_after_index,has_more}`; không tạo danh sách trăm nghìn missing index.
GET là snapshot quan sát từng request, không khóa toàn bộ phiên.

Chỉ VERIFIED sau đủ RF/domain, đúng ACK và transaction confirm. Request lặp trên
VERIFIED vẫn đọc/kiểm body hữu hạn và so actual hash trước khi trả 200, không cần
ghi Storage lại. Lượt song song cùng index đang claim nhận 409 PART_BUSY. Claim
hết lease chuyển reconcile, không tự cho write mới chồng lên attempt cũ.
Nếu đã STORED nhưng ACK/HTTP response mất, inspect có thể phục hồi confirmation;
client hỏi trạng thái trước khi gửi lại. Hết capacity giữa phiên giữ part đã xác
nhận, không bảo đảm đã reserve toàn file chỉ vì tạo session thành công.

### 2.2. Complete và abort

Complete chỉ đổi OPEN→FINALIZING khi toàn bộ part VERIFIED, không còn attempt
chưa rõ outcome, đầy đủ size và manifest đúng thứ tự. Nếu thiếu trả 409
PARTS_INCOMPLETE; nếu attempt chưa rõ trả OUTCOME_PENDING; không đổi state.
Đóng nhận part, cố định manifest và enqueue finalize trong cùng transaction.
Complete lặp trên FINALIZING trả cùng phiên 202; COMPLETED trả 200 và cùng file ID.
Worker kiểm full size/hash rồi commit AVAILABLE/COMPLETED trong một transaction.
Checksum mismatch làm FAILED, không công bố bytes, enqueue cleanup cùng commit.

Abort OPEN hoặc FINALIZING→ABORTED và file→FAILED; tăng session revision, hủy
khả năng finalize/confirm, tạo cleanup job cùng transaction. Abort đã ABORTED
idempotent; EXPIRED trả 410; FAILED trả trạng thái lỗi; COMPLETED trả 409 và
client dùng DELETE file. Race complete/abort lấy khóa cùng session: nếu finalize
đã công bố trước thì abort không được xóa một file COMPLETED bằng đường này.

## 3. Catalog, download và delete

GET `/files?limit=50&cursor=...` trả `{items,next_cursor}`; sort created_at DESC,
file_id DESC; limit 1..100. Cursor keyset opaque bind filter/version, không hứa
snapshot qua nhiều trang. GET `/files/{id}` trả FileSummary với `file_id`,
`version_id`, `original_name`, `content_type`, `size_bytes`, `checksum_sha256`,
`state`, `created_at`, `available_at`; checksum chỉ công bố sau finalize.
Baseline D3: catalog chỉ liệt kê AVAILABLE, không nhận filter tùy ý. `next_cursor`
null ở trang cuối; cursor tối đa 1024 ký tự, gắn version 1, filter AVAILABLE và
scope namespace/epoch, không phụ thuộc anchor row còn hiển thị. Cursor là điểm
keyset, không phải quyền truy cập. GET theo ID cho UPLOADING/VERIFYING/FAILED trả
summary với `checksum_sha256=null`; DELETING/DELETED luôn 404 FILE_NOT_FOUND.
GET `/files/{id}/chunks?after_index=-1&limit=100` trả trang manifest/replicas có
node ID, domain và trạng thái quan sát, không có token/endpoint nội bộ.
Route chunks chỉ nhận AVAILABLE; trạng thái khác như download. Response gồm
`file_id,version_id,items,has_more,next_after_index`; mỗi item có
`index,chunk_id,size_bytes,checksum_sha256,replicas`. Mỗi replica có
`node_id,failure_domain,state,replica_health,observed_at,node_health`, là thông tin
quan sát cached, không phải bằng chứng hiện còn đọc được bytes. Limit 1..1000,
after_index từ -1..2^53-1; next_after_index null khi hết trang. Mỗi part trả tối đa
RF record STORED, một record/node; không expose allocation history toàn bộ.
Query parameters lạ hoặc lặp bị từ chối 422 VALIDATION_ERROR trên các route D3.
GET `/nodes`, `/cluster` trả health/counters theo schema, có thời điểm quan sát;
không coi counters là bằng chứng bytes tồn tại. Shape chi tiết dashboard chốt ở F–G.

GET/HEAD `/files/{id}/download` chỉ nhận AVAILABLE; UPLOADING/VERIFYING/FAILED trả
409 FILE_NOT_READY; DELETING/DELETED trả 404. Strong ETag là
`"sha256-<full-file-hex>"`; file ID/version bất biến, không overwrite bằng tên.
Trả `Accept-Ranges: bytes`, Content-Length đúng representation, Content-Disposition
attachment encode an toàn; không nén nội dung binary động. GET tạo download lease
và giữ nguyên version cho cả response. HEAD không tạo lease và không đọc bytes.

Quy tắc Range theo [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-range-requests):

- HEAD bỏ qua Range/If-Range, trả metadata 200 với full Content-Length.
- GET hỗ trợ một `bytes=start-end`, `bytes=start-` hoặc `bytes=-suffix_length`.
  Clamp end theo size; start>=size hoặc suffix=0 là unsatisfiable 416 với
  `Content-Range: bytes */<size>`. File rỗng: GET thường 200 length=0; suffix
  dương được bỏ qua để trả full 200 rỗng; dạng start-end/start- trả 416.
- Range sai cú pháp hoặc end<start trả 400 INVALID_RANGE. Range unit khác hoặc nhiều range hợp
  lệ được bỏ qua và trả full 200; không triển khai multipart/byteranges.
- If-Range chỉ dùng khi có Range: strong ETag khớp mới áp dụng range; weak ETag,
  HTTP-date hoặc giá trị không khớp đều bỏ qua Range và trả full 200. Parse Range
  chỉ sau quyết định có áp dụng nó. Không dùng Last-Modified làm If-Range validator.
- 206 có `Content-Range: bytes <start>-<end>/<size>` và length chính xác.
- GET/HEAD xử lý If-Match trước If-None-Match, trước Range: If-Match so strong
  hoặc `*`, không khớp trả 412; If-None-Match so weak hoặc `*`, khớp trả 304
  không body, có ETag. HEAD không trả error body. Header ETag sai cú pháp trả 400.
  Baseline không phát Last-Modified và không dùng date validators; bỏ qua
  If-Modified-Since/If-Unmodified-Since. D kiểm chứng thứ tự precondition này.

DELETE `/files/{id}` transaction tombstone DELETING ngay và enqueue cleanup.
Command DELETE không có body hoặc query parameters; body khác rỗng trả
400 INVALID_REQUEST, đọc EOF có timeout 10s.
Trả 202 `{file_id,state,cleanup_pending:true}` tới khi mọi allocation đóng/dọn
và lease đã hết; sau đó 200 với DELETED/false. Lặp DELETING/DELETED là idempotent;
ID chưa từng tồn tại trả 404. Nếu file còn upload/finalize thì đồng thời abort
session và fence attempt. GET file/catalog/download mới ẩn ngay sau tombstone;
chỉ DELETE dùng được ID để xem tiến độ xóa. Lượt đọc có lease được tiếp tục tới
giới hạn lease, không làm file hiện lại.

## 4. Giao thức API nội bộ

Internal request bắt buộc `recovery_epoch` đúng cấu hình đang active. API/Worker
dùng chung domain transaction; Worker không gọi public endpoint để sửa state.

| Route sau `/internal/v2` | Request bắt buộc | Response/điểm commit |
|---|---|---|
| POST `/uploads/{id}/parts/{index}/claim` | size_bytes, checksum_sha256, request_id | part revision, claim_token, claim_expires_at, destinations; reservation/attempt đã commit |
| POST `/uploads/{id}/parts/{index}/renew` | claim_token, revision | expires_at mới nếu OPEN và token còn hạn |
| POST `/uploads/{id}/parts/{index}/confirm` | claim_token, revision, allocation ACKs | VERIFIED nếu đủ RF/domain; 409 token cũ/state đổi; replay cùng ACK trả cùng kết quả |
| POST `/uploads/{id}/parts/{index}/uncertain` | claim_token, revision, allocation_ids, reason_code | enqueue reconcile và giữ charge, idempotent; crash trước lời gọi được lease sweeper phát hiện |
| POST `/downloads` | file_id; version_id tùy chọn | read_id, lease_token, version_id, expires_at, absolute_expires_at, size/hash |
| GET `/downloads/{read_id}/manifest?after_index=-1&limit=100` | X-Recovery-Epoch và X-Read-Lease-Token | immutable manifest page gồm spec và eligible allocations; không ghép full manifest vào RAM |
| POST `/downloads/{read_id}/renew` | lease_token | expires_at mới nếu lease chưa hết và chưa quá absolute limit |
| POST `/downloads/{read_id}/release` | lease_token | 204, lặp vẫn 204 |

Các POST read command có `recovery_epoch` trong JSON như upload commands; field
lạ, UUID không canonical, token thiếu/sai kiểu trả 422. GET manifest dùng đúng
một header mỗi loại ở bảng; không nhận token/epoch trong URL hoặc body GET.
Epoch request sai hoặc DB không ACTIVE/đúng scope trả 503 RECOVERY_REQUIRED.
Read ID chưa từng tồn tại trả 404 READ_LEASE_NOT_FOUND; token sai hoặc lease
expired/released trả 409 STALE_READ_LEASE cho renew/manifest. Release cùng token
vẫn 204 sau expiry/release. Các code lease chỉ xuất hiện ở internal listener.
Acquire với version_id khác representation hiện tại trả 412 PRECONDITION_FAILED.
Acquire trả thêm `file_id,created_at,db_now,size_bytes,checksum_sha256,original_name,
chunk_size_bytes,part_count`; renew giữ các field identity/time của lease. `db_now`
là thời điểm DB kiểm lease, hỗ trợ Transfer tính phần TTL còn lại theo monotonic
clock sau khi trừ thời gian round trip; không so wall clock hai host. Manifest
giữ shape D2 `version_id,items,has_more,next_after_index,expires_at,db_now`, item
có `part_index,chunk_id,size_bytes,checksum_sha256` và eligible `destinations`;
chỉ internal DTO này chứa identity/generation/endpoint Storage.

Claim token UUID ngẫu nhiên, không log/expose browser; part revision tăng khi
thay claim. Destination có scope, allocation_id, generation, ChunkSpec và endpoint
gRPC từ registry. ACK phải khớp toàn bộ identity, size/hash và STORED; chọn RF
allocation trên distinct nodes/domains theo policy snapshot. ACK không phải bằng
chứng node còn sống vĩnh viễn. Confirm còn phải kiểm claim/session state trong DB.

Claim TTL 60s, renew mỗi 20s; part request có deadline hữu hạn cấu hình được,
baseline 120s. Storage RPC deadline không dài hơn phần còn lại của request.
Mất lease thì ngừng I/O và không confirm; cleanup vẫn reconcile outcome thật.
Read lease TTL 30s, renew mỗi 10s, absolute tối đa 24h; khi hết hạn/renew không
thành công, Transfer dừng gửi bytes. Release best effort, không dựa vào nó để GC.
Thời gian lease dùng DB clock, không so timestamp của hai host. Renew read sau
tombstone được phép chỉ với lease đã tồn tại và vẫn còn hạn; lease hết không hồi sinh.

## 5. Wire contract Storage V2

Service có 7 RPC trong package `dfs.storage.v2`. Store client-streaming;
Get server-streaming; 5 RPC còn lại unary. Request thiếu scope/spec, enum zero,
UUID/hash sai, generation zero, size zero/vượt chunk limit đều INVALID_ARGUMENT.
Empty file không tạo allocation. Không tái sử dụng field number hoặc enum value
đã phát hành; xóa field phải reserve theo [protobuf guide](https://protobuf.dev/programming-guides/proto3/).

Store có đúng header đầu, tiếp theo block 1..256 KiB, offset liên tục từ 0,
tổng bytes đúng spec và clean EOF. Không chấp nhận frame rỗng/header thứ hai;
Store dùng allocation đã Prepare. Nếu bytes đã STORED, Store retry vẫn drain và
validate stream trước ACK `already_stored=true`; client nên Inspect trước để
tránh truyền lại. Get có header STORED, block liên tục, footer actual size/hash,
successful EOF. Client kiểm identity, hash bytes thực và footer; footer hoặc
EOF mất thì chưa hoàn tất chunk. Transfer chỉ phát bytes chunk sau kiểm chứng.

Inspect không mutate state/generation; generation thấp bị FAILED_PRECONDITION,
generation cao hơn ledger cũng bị FAILED_PRECONDITION (chưa prepare). Prepare
generation lớn hơn có thể fence writer cũ nhưng spec luôn bất biến; cùng generation
idempotent. Delete nhận generation >= high-water và lưu max generation; đóng cả
allocation chưa tồn tại, chặn mọi Prepare/Store sau đó. Delete lặp generation thấp
vẫn được tra bằng Inspect ở generation hiện hành, không ghi đè high-water.
Delete thành công chỉ sau `state=DELETED,cleanup_complete=true`; timeout trước đó
giữ DELETE_PENDING trong DB. Inspect tombstone còn đang dọn có cleanup_complete=false.

ListAllocations: limit 1..1000; trang đầu chụp upper sequence, các trang sau giữ
nguyên bound; inventory sequence tăng theo lần insert record kể cả tombstone.
Không bỏ tombstone; không hứa snapshot state qua các trang. Reconcile online dùng
CAS/recheck; full recovery chỉ scan sau khi drain mutation. Endpoint quản trị đổi
epoch/adopt allocation không thuộc public wire và chỉ xây trong chế độ maintenance B/H.

| gRPC status | Ý nghĩa |
|---|---|
| INVALID_ARGUMENT | Sai shape/spec/offset/limit |
| FAILED_PRECONDITION | Scope/incarnation/epoch/generation sai, DELETED hoặc recovery gate |
| ALREADY_EXISTS | Cùng allocation nhưng khác immutable spec |
| ABORTED | Writer khác đang giữ allocation |
| NOT_FOUND | Inspect/Get chưa có allocation; không là delete fence |
| DATA_LOSS | Actual bytes/hash sai hoặc chunk STORED thiếu file |
| RESOURCE_EXHAUSTED | Hết local disk/buffer/concurrency budget |
| UNAVAILABLE / DEADLINE_EXCEEDED / CANCELLED | Outcome mutation chưa biết; phải inspect/reconcile |

Health sai expected scope trả FAILED_PRECONDITION; registry/provisioning khám phá
incarnation qua cấu hình quản trị, không tự tin node_id cũ sau thay volume.
Health counters chỉ có ý nghĩa quan sát, không cấp capacity credit.

## 6. Phạm vi kiểm chứng A

Compile/import proto, kiểm RPC direction, field presence và serialize block giới
hạn chỉ chứng minh wire shape. REST, lease, SQL constraints và crash matrix phải
được exercise khi hiện thực B–D. A không có evidence HTTP/DB/filesystem runtime.
