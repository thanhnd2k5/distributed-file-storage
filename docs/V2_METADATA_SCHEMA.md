# Schema metadata V2 — mốc A

Ngày 22/10/2026. Schema logic để viết migration B; chưa chạy DDL vào database.
Cập nhật 23/10/2026: baseline này đã được hiện thực bởi migration V2 độc lập
`backend/metadata/v2/migrations/0001_foundation.sql`, áp dụng vào `dfs_v2_test`.
Evidence/giới hạn: [B3](V2_B_IMPLEMENTATION_PLAN.md#evidence-b3--23102026).
Nguồn hành vi: [API](V2_API_CONTRACTS.md), [ADR và state machines](V2_CORRECTNESS.md).
PostgreSQL V2 dùng database riêng; migration V1 không nạp các bảng này. Mỗi database
phục vụ một namespace V2; recovery epoch active được đối chiếu với cấu hình ngoài DB.

## 1. Kiểu và quy tắc chung

ID dùng UUID; node_id varchar(64), domain varchar(255); checksum varchar(64) với
CHECK `^[0-9a-f]{64}$`; bytes/generation/revision dùng bigint CHECK >=0 hoặc >0
theo cột. Timestamp timestamptz; clock lease dùng `clock_timestamp()` trong DB,
không dùng thời điểm bắt đầu transaction dài. JSONB chỉ cho snapshot policy,
job payload và error details; key/FK/counter không giấu trong JSON. State dùng
text + CHECK enum để dễ migration. Cột không ghi nullable bên dưới là NOT NULL.

FK mặc định RESTRICT, không cascade xóa attempt/allocation/cleanup history.
Không hard-delete file/session/allocation/idempotency trong namespace active.
CHECK không dùng subquery; bất biến qua nhiều bảng được giữ bằng transaction
và kiểm chứng integration B–D, không tuyên bố FK/CHECK tự bảo đảm toàn bộ nghiệp vụ.

## 2. Danh mục bảng PostgreSQL

| Bảng | Cột chính và quan hệ | Constraint/index bắt buộc |
|---|---|---|
| cluster_control | singleton PK, namespace_id UUID UNIQUE, recovery_epoch UUID, mode ACTIVE/RECOVERING, updated_at | CHECK singleton=1; epoch DB phải khớp cấu hình trước readiness |
| files | file_id PK, version_id UUID UNIQUE, original_name, content_type, size_bytes, expected_sha256, actual_sha256 nullable, state, revision, created_at, available_at/deleted_at nullable | state UPLOADING/VERIFYING/AVAILABLE/FAILED/DELETING/DELETED; size>=0; revision>0; AVAILABLE bắt buộc actual_sha256 IS NOT NULL và actual=expected, available_at khác null; index (created_at DESC,file_id DESC) WHERE state='AVAILABLE' |
| upload_sessions | upload_id PK, file_id UNIQUE FK files, state, revision, chunk_size_bytes, part_count, replication_factor, min_distinct_domains, max_parallel_parts, policy JSONB, active_claims, expires_at, completed_at/admission_released_at nullable, error_code nullable | state OPEN/FINALIZING/COMPLETED/ABORTED/EXPIRED/FAILED; chunk_size>0, part_count>=0, RF>=1, 1<=domains<=RF, 0<=active_claims<=max_parallel_parts; index (expires_at,upload_id) WHERE state='OPEN' |
| upload_parts | chunk_id PK, upload_id FK sessions, part_index, size_bytes, checksum_sha256, state, revision, claim_held boolean, claim_token/claim_owner/claim_expires_at nullable, confirmed_at nullable | UNIQUE(upload_id,part_index); state CLAIMED/RECONCILING/VERIFIED; index>=0,size>0,revision>0; VERIFIED requires confirmed_at; partial index claim_expires_at for CLAIMED/RECONCILING |
| manifest_parts | version_id FK files.version_id, part_index, chunk_id UNIQUE FK upload_parts, size_bytes, checksum_sha256 | PK(version_id,part_index), size>0,index>=0; immutable sau khi session FINALIZING; empty manifest hợp lệ |
| storage_nodes | node_id PK, host, port, failure_domain, active_incarnation UUID, enabled, health_state, health_generation, health_observed_at nullable, available_bytes nullable | port 1..65535; state ACTIVE/SUSPECTED/DOWN/QUARANTINED; health gen>=0; startup không reset node của instance khác |
| node_incarnations | node_id FK nodes, incarnation UUID, recovery_epoch UUID, state, registered_at, retired_at nullable | PK(node_id,incarnation); state JOINING/ACTIVE/QUARANTINED/RETIRED; tối đa một ACTIVE mỗi node (partial unique index) |
| node_budgets | node_id/incarnation FK node_incarnations, allocatable_bytes, charged_bytes, revision | PK(node_id,incarnation), 0<=charged<=allocatable, revision>0; giảm budget dưới charged phải từ chối và quarantine qua node state |
| allocations | allocation_id PK, chunk_id FK upload_parts, node_id/incarnation FK node_incarnations, recovery_epoch, generation, failure_domain_snapshot, charged_bytes, state, replica_health, observation_generation, observed_at nullable, ack_size/ack_hash/acked_at nullable, cleanup_complete_at nullable | state RESERVED/WRITING/UNKNOWN/STORED/DELETE_PENDING/RELEASED; replica_health UNKNOWN/VERIFIED/MISSING/CORRUPT; generation>0,charged_bytes>0; STORED yêu cầu ACK đủ; RELEASED yêu cầu cleanup_complete_at; index (chunk_id,state), (node_id,incarnation,state) |
| write_attempts | attempt_id PK, allocation_id FK allocations, generation, owner_token UUID, state, lease_expires_at, started_at, finished_at/error_code nullable | UNIQUE(allocation_id,generation); state INTENDED/IN_FLIGHT/ACKED/UNKNOWN/CLOSED; index (state,lease_expires_at); mọi destination có row trước RPC |
| jobs | job_id PK, kind, dedup_key, target_id UUID, payload JSONB, state, run_after, attempts, lease_owner/lease_expires_at nullable, fencing_token, last_error nullable, created_at | kind FINALIZE/RECONCILE/CLEANUP/REPAIR/SCRUB/HEALTH; state READY/RUNNING/RETRY/DONE/DEAD; partial UNIQUE(kind,dedup_key) WHERE state IN ('READY','RUNNING','RETRY'); index (run_after,job_id) cho READY/RETRY; index lease_expires_at cho RUNNING |
| idempotency_records | operation, key UUID, request_sha256, upload_id UNIQUE FK sessions, created_at | PK(operation,key); operation CREATE_UPLOAD; canonical normalized request hash; không expires tự động |
| download_leases | read_id PK, file_id/version_id FK files, lease_token UUID UNIQUE, state, created_at, expires_at, absolute_expires_at, released_at nullable | state ACTIVE/RELEASED; created_at<expires_at<=absolute_expires_at; composite FK(file_id,version_id) yêu cầu UNIQUE tương ứng ở files; index (file_id,expires_at) WHERE state='ACTIVE' |
| admission_counters | resource_key PK, limit_count, held_count, revision | 0<=held<=limit; resource cho OPEN/FINALIZING sessions và các grant bền vững cần chia cluster; reconciliation với owner rows |

Allocation identity/spec được suy từ FK chunk và không được đổi node/incarnation,
epoch, charged_bytes hay chunk khi đã tạo. Một chunk có thể có nhiều allocation
history trên cùng node; chỉ chọn một allocation hợp lệ mỗi node cho RF. Replica
là projection của allocation STORED + ACK + replica_health VERIFIED + node/incarnation
eligibility, không tạo thêm bảng replica có thể lệch với sổ allocation. Rejoin/health không tự đổi
UNKNOWN thành STORED. charged_bytes không giảm về 0 khi RELEASED: tổng budget
chỉ SUM các state khác RELEASED để giữ lịch sử kiểm toán được.

ACK đã kiểm chứng đặt replica_health=VERIFIED. Observation về chunk thiếu/hỏng
đặt MISSING/CORRUPT với generation/thời điểm để loại kết quả cũ; không giảm charge.
Health node thành công chỉ cho biết node đáp ứng, không tự xóa observation chunk
hỏng. node.active_incarnation phải tham chiếu node_incarnations qua composite FK
được tạo sau hai bảng, DEFERRABLE để bootstrap cùng transaction; node JOINING
không được chọn placement trước khi incarnation ACTIVE được xác minh.

Cross-row bắt buộc: part_index<part_count, size đúng công thức part cuối;
part_count=ceil(file.size/chunk_size); manifest thuộc đúng session/file/version;
tổng manifest size bằng file size; toàn bộ session snapshot bất biến; ACK spec
khớp part; file AVAILABLE chỉ nếu job finalize token/revision còn hợp lệ và đủ
RF/domain. Khóa domain nghiệp vụ giữ các điều kiện này theo mục 3.

## 3. Transaction và thứ tự khóa

Dùng READ COMMITTED với khóa tường minh/CAS. Thứ tự khóa cho mutation có nhiều
đối tượng: file → session → part theo index → node budget theo (node_id,incarnation)
→ allocation theo ID → attempt. Admission counter nếu cần luôn lấy trước file.
Job claim/renew riêng không giữ job lock khi bắt đầu I/O; publication kiểm token
trong cùng transaction với domain mutation và khóa job sau các domain row.
Không có network/filesystem I/O trong transaction PostgreSQL. Lock timeout hữu
hạn; deadlock/serialization failure chỉ retry transaction thuần DB trước I/O.

| Transaction | Phải commit nguyên tử |
|---|---|
| Create upload | Kiểm idempotency, cấp admission, files+session+record; unique conflict đọc lại cùng payload; duplicate không tạo file thừa |
| Claim part | Khóa file/session OPEN chưa expired, part; cố định spec, tăng revision/token, active_claims; chọn domain, khóa budget, thêm allocations+attempts, tăng charge |
| Confirm part | Kiểm epoch/session/claim revision+token chưa hết hạn; ACKs khớp; đủ RF/domain; đánh dấu allocations STORED/attempt ACKED/part VERIFIED; giảm active_claims đúng một lần |
| Uncertain/sweeper | CAS attempt/claim, enqueue RECONCILE, giữ charge; không cấp claim mới khi attempt cũ chưa được resolve |
| Complete | Khóa file/session, kiểm VERIFIED và không UNKNOWN; insert immutable manifest, OPEN→FINALIZING/file VERIFYING, tăng revision, enqueue FINALIZE |
| Publish finalize | Kiểm job token+lease, file/session revision, manifest và replica evidence còn đủ; actual hash=expected; FILE AVAILABLE/session COMPLETED/job DONE, release admission cùng commit |
| Abort/expiry/delete | Tăng revision và đổi terminal/tombstone, enqueue CLEANUP; không release node budget tại đây; release session admission nếu chưa release |
| Allocation cleanup | Sau durable Delete ACK: CAS allocation còn DELETE_PENDING cùng identity, đổi RELEASED và trừ charged đúng một lần; attempt CLOSED; retry không trừ thêm |
| Read lease acquire/renew | Khóa file, kiểm AVAILABLE để acquire; renew chỉ ACTIVE chưa hết hạn, cùng version, kể cả DELETING; clamp tuyệt đối 24h |
| Authorize physical delete | Khóa file, kiểm không ACTIVE read lease còn hạn, terminal session và không tác vụ ghi hợp lệ; allocations→DELETE_PENDING/job tồn tại; commit rồi mới RPC |

Không để cleanup release capacity vì inspect NOT_FOUND. Job DEAD vẫn giữ nghĩa
vụ cleanup và charge; có lỗi hiển thị để vận hành requeue, không mất row công việc.
Các attempt thừa đã STORED nhưng không được chọn vào manifest phải enqueue cleanup;
chỉ complete khi không còn outcome chưa biết, không yêu cầu mọi cleanup thừa đã xong.

Claim quota chỉ tính token còn giữ slot. Sweeper khi lease hết hạn chuyển part
RECONCILING và bỏ slot đúng một lần; part đó vẫn không nhận writer mới tới khi
attempt được resolve/fence. active_claims kiểm đối soát từ owner rows; không tăng
slot vì request retry, không âm vì sweeper và confirm đụng nhau.
`claim_held` CAS true→false cùng decrement là bằng chứng đã trả slot;
`admission_released_at` được ghi cùng decrement admission của session terminal.

Implementation B3 sau review: sweeper chỉ scan tối đa 8 candidate, rồi xử lý
từng session/part trong transaction riêng. Không giữ file/session locks của
candidate trước khi sang candidate sau vì thứ tự expiry của session và claim
có thể khác nhau. Mỗi transaction recheck epoch/state/expiry/owner; lỗi ở
candidate sau không rollback tiến độ đã commit ở candidate trước. Lượt tiếp
theo đối soát owner rows, không trả slot/admission lần thứ hai.

Job claim: `SELECT ... FOR UPDATE SKIP LOCKED` theo run_after/job_id, batch tối đa
8, cập nhật owner/expiry và tăng fencing_token rồi commit. Renew/finish cần cùng
owner+token và lease còn hạn theo DB clock. RUNNING hết lease được claim lại với
token mới; token cũ không commit domain effect. Dedup key cố định theo kind và
đối tượng; mutation và enqueue cùng transaction, UNIQUE bảo vệ race hai instance.
Các quy tắc này dựa trên [khóa PostgreSQL](https://www.postgresql.org/docs/current/explicit-locking.html);
SQL/migration và kiểm concurrent transactions được hiện thực, kiểm chứng ở B–C.

## 4. Ledger SQLite tại Storage

Một process duy nhất sở hữu một volume; startup giữ exclusive process/volume
lock suốt đời process. Không dùng SQLite qua NFS hoặc chia volume cho nhiều
Storage process. Các allocation khác nhau có thể stream đồng thời; SQLite chỉ
giữ transaction ghi ngắn. Chọn `journal_mode=WAL`, `synchronous=FULL`,
`foreign_keys=ON`, busy_timeout hữu hạn; đọc lại PRAGMA và fail readiness nếu
không áp dụng được. Cấu hình theo [SQLite synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous).
Backup ledger dùng SQLite backup/checkpoint đúng quy trình, không copy riêng db
file khi WAL đang hoạt động. B kiểm crash thật trên Docker Linux/filesystem local.

| Bảng local | Cột/constraint |
|---|---|
| node_identity | singleton=1 PK, namespace_id, active_epoch, node_id, incarnation; UUID dạng canonical text |
| allocations | inventory_sequence INTEGER PRIMARY KEY AUTOINCREMENT, allocation_id UNIQUE, epoch, generation>0, chunk_id/size/hash nullable chỉ cho tombstone tạo trước Prepare, state PREPARED/WRITING/STORED/DELETED/CORRUPT, temp_name nullable, actual_size/hash nullable, cleanup_complete boolean |
| maintenance | singleton PK, mode ACTIVE/RECOVERING, pending_epoch nullable; epoch adoption chỉ qua quản trị đã cô lập writer cũ |

`allocation_id` giữ UNIQUE xuyên các epoch trong volume; adoption không tạo bản
sao identity mới để né tombstone. inventory_sequence không tái sử dụng; PK UUID
không phải đường dẫn nhập từ client. Layout sinh nội bộ:
`chunks/<allocation_id>` và `tmp/<allocation_id>.<generation>.part` sau validate;
chỉ một writer/tempfile mỗi allocation; ledger ghi temp_name trước khi tạo file.
Node incarnation mới không tự adopt ledger/bytes cũ.

Thứ tự durable write/delete và recovery theo [ADR](V2_CORRECTNESS.md). SQLite
transaction không bao trùm chunk file. `STORED` yêu cầu actual size/hash, file
được xác minh khi phục hồi; `DELETED` chặn mọi generation dù cleanup chưa xong.
Giữ tombstone đến retire namespace có kiểm soát. Disk đầy phải vẫn có headroom
để ghi tombstone/ledger; không báo cleanup_complete khi fsync/unlink thất bại.

## 5. Gate schema

A chốt bảng, key, constraint, transaction và ownership. B tạo migration V2,
kiểm FK/CHECK/unique/counter trên PostgreSQL thật và kiểm ledger trên filesystem
thật. Không áp dụng schema này bằng migration V1 hoặc reset volume hiện có.
