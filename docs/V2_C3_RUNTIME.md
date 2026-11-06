# Runtime HTTP V2 — C3

C3 nối upload control plane và byte plane với Worker C2. Client tối thiểu thuộc
C4; download/Range/read lease và catalog/delete API thuộc D trở đi. Runtime V1
không được gắn thêm route V2.

**Bổ sung D3 (28/10/2026):** đã có catalog/metadata/chunks/DELETE, internal read
commands và Transfer HEAD download. GET binary streaming/Range còn thuộc D4;
[evidence D3](V2_D_IMPLEMENTATION_PLAN.md#5-evidence-d3--28102026) không thay thế
gate C5/D5 về file lớn/tài nguyên/nhiều instance.

**Bổ sung D4 (29/10/2026):** GET binary/Range qua Transfer–Storage đã hoạt động
với one-chunk verify, fallback và read lease; xem
[evidence D4](V2_D_IMPLEMENTATION_PLAN.md#6-evidence-d4--29102026).

**Bổ sung D5 (29/10/2026):** đã có gate download/read-delete và tải hỗn hợp qua
hai API/hai Transfer cùng Nginx, Storage/Worker process độc lập; xem
[số đo và giới hạn D5](V2_D5_EVIDENCE.md). Đây là topology một host trong tests,
không phải deployment production/HA. D5 không thay runtime hay các mặc định dưới đây.

**Review trước E (29/10/2026):** đã sửa shared-reader ownership tại Storage,
deadline tổng cho control HTTP, parse duration thập phân và tracing qua admission.
Xem [evidence sửa review](V2_PRE_E_FIXES.md). Các mặc định giữ nguyên.

**Bổ sung E1 (29/10/2026):** incoming HTTP và outgoing control có reservation
hai chiều cho new work/progress trong cùng tổng cap, không queue. Xem
[contract, gate và giới hạn E1](V2_E_IMPLEMENTATION_PLAN.md). Admission này theo
process; ngân sách bền vững toàn cluster và Worker fairness thuộc E2–E4.

**Bổ sung E2 (30/10/2026):** audit durable session/claim/node accounting có
22 integration pass qua process/connection riêng và POSIX Storage restart.
Không đổi runtime; cap download/job vẫn theo process, phải cộng cả internal
retry lane khi tính deployment envelope. Chưa có grant concurrency cố định toàn
cluster; xem [quyết định E2 và phần còn lại E3–E4](V2_E_IMPLEMENTATION_PLAN.md#contract-và-audit-e2--30102026).

**Bổ sung E3 (30/10/2026):** Worker claim/dispatch luân phiên ba kind, nhường
lượt sau chunk/allocation/RPC, giữ continuation có cap và dùng chung read credit
trước GetChunk. **77 ca liên quan pass**, gồm 16 policy, 11 integration mới và
50 Worker/job/HTTP handoff regressions. Xem
[contract, cấu hình và evidence E3](V2_E_IMPLEMENTATION_PLAN.md#contract-e3--worker-fairness-và-io).
E4 chưa nghiệm thu; chưa có benchmark RSS/load mở rộng cho scheduler mới.

## Entry point và điều kiện khởi động

Trong image Python runtime đã build từ `deploy/Dockerfile.python`, chạy cùng lệnh
cho từng process, cấu hình role và port riêng:

```text
python -m metadata.v2.runtime
```

| `V2_SERVICE_ROLE` | Listener | Cấu hình bắt buộc |
|---|---|---|
| `api` | Public JSON `/api/v2/config`, upload commands/status/parts và catalog/file/chunks/DELETE | `V2_DATABASE_URL`, `V2_NAMESPACE_ID`, `V2_RECOVERY_EPOCH` |
| `internal` | Upload claim/renew/confirm/uncertain và download acquire/manifest/renew/release `/internal/v2` | Cùng DB/schema/namespace/epoch của API |
| `transfer` | PUT `/api/v2/uploads/{id}/parts/{index}` và GET/HEAD `/api/v2/files/{id}/download` | `V2_API_URL`, `V2_INTERNAL_URL`, `V2_RECOVERY_EPOCH` |
| `worker` | Health HTTP; background reconcile/cleanup/finalize | Cùng DB/schema/namespace/epoch của API |

`V2_PORT` mặc định 8080, `V2_BIND_HOST` mặc định `0.0.0.0`;
`V2_DATABASE_SCHEMA` mặc định `public` **trong database V2 riêng đã provision**.
Không trỏ vào DB/schema V1. Khi chạy native cần `PYTHONPATH` gồm `backend/` và
`backend/generated/`; integration đã kiểm trên Docker Linux/Python 3.12.

Startup không migrate DB, provision/activate node, thay epoch hay start Storage.
Trước khi chạy cần migration V2 hiện có, cluster control ACTIVE đúng identity,
session admission counter đã cấu hình và registry Storage đã provision đúng
incarnation/domain/budget. API sẵn sàng độc lập Worker; Transfer cần cả hai
listener API; Worker readiness cần loop còn chạy và DB gate hợp lệ.
`/health/live` chỉ xác nhận HTTP process; `/health/ready` kiểm dependency của role.

Public/internal là **hai listener riêng**, không chỉ hai prefix trên cùng app.
Internal listener, Worker health và gRPC chỉ nằm trong mạng dịch vụ riêng; chỉ
ingress nhận browser traffic. C3 không thêm auth vào kho chung. Claim token,
allocation và endpoint gRPC không xuất hiện trong response public.

## Budget hữu hạn trước readiness

| Biến môi trường | Mặc định | Ý nghĩa |
|---|---:|---|
| `V2_REQUESTS` | 32 | Tổng request đang xử lý/process; hết total/class cap trả 429, không queue chờ |
| `V2_REQUEST_RESERVE` | 8 | Slot dành tối thiểu cho mỗi nhóm new work/progress trong tổng request cap |
| `V2_STREAMS` | 8 | Part stream/Transfer; cấp trước đọc body |
| `V2_BLOCK_BYTES` | 262144 | Block gửi gRPC, không vượt wire limit |
| `V2_QUEUE_BLOCKS` | 2 | Queue/block mỗi replica, tối đa 2 |
| `V2_MAX_REPLICAS` | 16 | Giới hạn ACK/destination; policy vượt mức bị chặn |
| `V2_BUFFERED_BYTES` | 134217728 | Budget cấu hình cho các buffer stream ứng dụng |
| `V2_CONTROL_CALLS` | 32 | Tổng control calls đang hoạt động; cancellation đóng transport rồi trả slot |
| `V2_CONTROL_RESERVE` | 8 | Slot dành tối thiểu cho mỗi nhóm new work/progress trong tổng control cap |
| `V2_REQUEST_SECONDS` | 120 | Deadline toàn PUT, RPC không vượt thời gian còn lại |
| `V2_CONTROL_SECONDS` | 20 | Deadline tổng cho một control exchange, gồm connect/TLS/header/body |
| `V2_RENEW_SECONDS` | 20 | Renew claim bằng DB clock; không vượt 20s |
| `V2_SHUTDOWN_SECONDS` | 10 | Grace/drain HTTP; best-effort uncertain tối đa min(2s, grace/2) |
| `V2_JOB_BACKLOG_LIMIT` | 1024 | Chặn create mới khi backlog job chưa DONE đạt ngưỡng, kể cả DEAD |
| `V2_DOWNLOAD_STREAMS` | 4 | Lượt GET đang xử lý; hết slot trả 429 trước acquire/Storage |
| `V2_DOWNLOAD_CHUNK_BYTES` | 8388608 | Reservation một chunk/GET; readiness chặn policy lớn hơn |
| `V2_DOWNLOAD_BUFFERED_BYTES` | 37748736 | Budget riêng cho chunk buffers và hai wire blocks/GET |
| `V2_DOWNLOAD_RPC_SECONDS` | 5 | Deadline một replica; tối đa 120s, mỗi replica thử một lần |
| `V2_DOWNLOAD_SECONDS` | 86400 | Deadline toàn GET, tối đa 24h; không kéo dài absolute read lease |
| `V2_DOWNLOAD_RENEW_SECONDS` | 10 | Renew read lease riêng, không vượt 10s |

Mỗi health-ready có admission riêng tối đa 2, không xếp queue không giới hạn.
Mỗi budget phải thỏa `0 <= 2*reserve <= limit`; mỗi nhóm tối đa `limit-reserve`,
tổng tối đa `limit`. Với mặc định 32/8, tối đa 24 request/call một nhóm để còn
ít nhất 8 slot cho nhóm kia; shared capacity 16 slot được dùng linh hoạt. Khi
giảm tổng cap phải cấu hình reserve tương ứng, nếu không hợp lệ startup báo lỗi.
Reserve=0 tắt bảo đảm riêng; constructor trực tiếp không chỉ rõ reserve dùng
`min(8, limit//4)`. Đây là reservation khả năng nhận việc, không phải FIFO hoặc
bảo đảm mọi renew thành công khi chính nhóm progress cũng đầy.
Progress gồm upload complete/abort, DELETE file, internal part renew/confirm/
uncertain và internal read manifest/renew/release. Create/acquire/claim mới,
binary GET/PUT và các query/poll khác dùng nhóm new work. Classifier chỉ dựa
method/route shape; validation/token/epoch và budget stream/DB vẫn áp dụng.
Duration được parse theo kiểu field `float`, ví dụ `V2_DOWNLOAD_RPC_SECONDS=0.5`
và `V2_WORKER_SHUTDOWN_SECONDS=0.5`; counter vẫn bắt buộc integer, duration phải
dương/hữu hạn và trong bound của từng field. Request ID có mặt cả ở response
429/503 từ admission, chỉ một header/cùng ID qua các middleware lồng nhau.
DB pool mỗi process tối đa 8, overflow=0, pool wait=5s, connect timeout=5s;
repository giữ lock timeout=3s và statement timeout=10s. Session admission vẫn
là counter bền vững trong DB, do migration/provisioning cấu hình; không dùng
counter RAM thay thế. Create replay được kiểm trước admission/backlog/policy mới.
Backlog gate đóng nhận phiên mới; confirm/abort/cleanup vẫn có đường tiến triển.

Buffer reservation kiểm trước readiness:
`streams * block_bytes * (max_replicas * (queue_blocks + 1) + 1) <= buffered_bytes`.
Đây là bound cho queue/producer/serialization blocks ứng dụng; không phải số đo
RSS hay giới hạn memory cứng của gRPC, socket và Python. C5 đo các phần đó.
Download có reservation riêng:
`download_streams * (download_chunk_bytes + 2 * 262144) <= download_buffered_bytes`.
Mỗi slot chỉ một GetChunk RPC đang chạy, tuần tự tối đa `max_replicas` đích/chunk;
không prefetch, spool file hay tạo queue chờ slot. Manifest lấy đúng một item/trang,
bắt đầu từ chunk chứa Range start. Đây là bound ứng dụng, chưa phải số đo RSS D5.

Policy API/Worker có thể cấu hình bằng `V2_POLICY_<FIELD>` (tên field viết hoa
trong `Policy`, ví dụ `V2_POLICY_CHUNK_SIZE_BYTES`); session snapshot policy khi
create. Worker nhận `V2_WORKER_<FIELD>` theo `WorkerConfig`, gồm slots/finalize
slots, page size, retry, renew, job/shutdown và read-rate budget. Consumer retry
ở internal listener dùng **Worker riêng không chạy loop**, cùng retry policy,
job budget bằng 0.75 lần control timeout; chỉ một consumer nhận handoff/process.

Từ E3, `V2_WORKER_JOBS_PER_KIND=2` (1–8) giới hạn continuation mỗi kind;
mặc định ba kind giữ tối đa 6 job leases/heartbeat/page states, nhưng chỉ
`V2_WORKER_SLOTS=2` quantum chạy cùng lúc. Cleanup/reconcile mỗi kind tối đa
slots-1; finalize bị giới hạn thêm bởi FINALIZE_SLOTS. READ_BYTES_PER_SECOND
mặc định 64 MiB/s là **tổng cho một Worker**, trả credit expected chunk trước
GetChunk, không còn một rate độc lập cho mỗi finalize. Không refund khi fallback/
cancel; RPC vẫn có thể burst trong chunk. Paused lease vẫn renew, job deadline
không reset; process chết phải hash lại từ đầu. Cộng resident state và internal
retry lanes khi tính deployment envelope, không chỉ số execution slots.

## Ingress và semantics

[`deploy/v2-ingress.conf`](../deploy/v2-ingress.conf) là cấu hình Nginx standalone:
upstream `api:8080` và `transfer:8080` cần resolve trên mạng deployment.
Ingress route JSON tới API, đường part và download tới Transfer. Không có route tới
internal/Storage. Request/response buffering tắt, HTTP/1.1, không tự retry
upstream mutation, timeout proxy 125s cho baseline PUT 120s. Khi thay deadline
deployment phải cập nhật timeout proxy tương ứng. Không mount volumes V1.

JSON command tối đa 16 KiB, có deadline đọc body 10s; từ chối unknown/duplicate
fields, bool/float/string thay integer, ID/hash không canonical và control
characters. Binary PUT đếm actual bytes, hash tăng dần, không spool file/part.
HTTP response có `X-Request-ID`; lỗi 500 ghi cùng ID trong log, không ghi claim
token hoặc bytes body.
Control client dùng asyncio streams và h11 0.16.0 đã khóa sẵn, một connection/call,
không pool/retry; max response body 1 MiB và max incomplete HTTP event/header 16 KiB.
Peer gửi nhỏ giọt vẫn chịu deadline tổng; timeout/cancel đóng transport trước khi
trả control slot. Mutation bị hủy vẫn là unknown outcome, tra trạng thái/lease
recovery như trước. HTTPS dùng system trust store và hostname verification.
Storage cho nhiều reader của cùng allocation dùng chung read ownership; writer
không chạy cùng reader. Prepare/Delete giữ writer slot chặn reader mới, chờ reader
cũ drain trong cùng một drain budget; Delete vẫn commit fence trước khi chờ và
chỉ ACK cleanup_complete sau khi mọi reader đóng handle và bytes được dọn/fsync.
Read commands D3 cũng bị giới hạn admission/control calls hiện có. Internal GET
manifest cần `X-Recovery-Epoch` và `X-Read-Lease-Token`; POST dùng epoch/token
trong JSON. Read token và Storage endpoints chỉ trả ở internal listener, không
đưa vào public summary/chunks hoặc URL. HEAD chỉ tra metadata, không cấp stream/
read lease hay gọi Storage. `V2_RENEW_SECONDS=20` chỉ dành cho upload claim;
GET dùng `V2_DOWNLOAD_RENEW_SECONDS=10`. Transfer lấy TTL từ `expires_at`,
`absolute_expires_at`, `db_now`, trừ toàn bộ round trip và 100ms dự phòng rồi
theo monotonic clock. Watchdog hủy cả RPC/send bị block nếu renew lỗi/hết hạn;
release tối đa 2s, sau khi drain watcher disconnect (kể cả EOF thành công).
Mất response acquire hoặc release không thành công vẫn dựa DB expiry để GC.
GET acquire đúng version trước binary headers, verify identity/spec/generation,
offset/size/hash/footer và successful EOF của cả chunk trước khi phát slice.
Lỗi trước headers dùng envelope; lỗi sau headers ngắt transport, không JSON/padding.
Queue hữu hạn trên từng replica áp dụng backpressure. Chỉ clean EOF gRPC sau
HTTP EOF, đủ size và đúng hash. ACK phải đúng scope/spec/generation và STORED;
200 chỉ sau transaction confirm. VERIFIED replay vẫn đọc và hash toàn body
part, không tạo allocation mới.

Sai body/mất ACK/cancel/deadline: hủy và drain RPC; best-effort uncertain ghi
reconcile. Nếu Transfer chết hoặc không nhận được phản hồi claim, lease vẫn ở
DB; sweeper sau expiry mới cho reconcile, không chiếm lại claim đang sống.
Retry đồng bộ resolve STORED hoặc commit generation intent → Prepare fence/drain
→ handoff token cho đúng request consumer. Không tự gửi lại stream đã tiêu thụ.

Complete trả 202 và Location upload khi FINALIZING; Worker hash full bytes đúng
thứ tự rồi publish AVAILABLE/COMPLETED. Abort chặn confirm/publish và enqueue
cleanup; chỉ trả 200 abort khi cleanup hết pending. SIGTERM đóng admission/
readiness, drain trong grace; token không bàn giao được phục hồi bằng lease.
Uvicorn trên Unix có thể kết thúc với exit status SIGTERM sau graceful shutdown.

## Gate C3

Tier **focused integration**, prerequisites chỉ Docker và PostgreSQL V2 test
healthy. Xem [V2 local tests](V2_LOCAL_TESTS.md) và chính sách blocker trong
[`AGENTS.md`](../AGENTS.md). Chạy từ root:

```powershell
docker info --format '{{.ServerVersion}}'
docker compose -f deploy/compose.v2-metadata-tests.yml ps
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -c 'SELECT current_database(),current_user,1 AS ready'

docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c3.py
```

Overlay HTTP dùng image riêng `dfs-v2-http-tests:dev`, thêm Nginx chỉ trong test
stage. Các fixture sở hữu schema PostgreSQL, HTTP loopback ports và hai Storage
gRPC/SQLite/filesystem tạm, RF=2/domain=2. Nginx dùng chính cấu hình ingress trên,
chỉ thay upstream/port/pid theo fixture. Không cần live Storage containers.
Evidence/kết quả thực tế ghi ở [mốc C](V2_C_IMPLEMENTATION_PLAN.md).
