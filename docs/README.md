# Distributed File Storage — bộ đặc tả V1

**Đặc tả 02/10/2026 · Một người code · Implementation M0–M2 hoàn thành 03/10/2026.**

## Đọc theo thứ tự

1. `PROJECT_OVERVIEW.md`: phạm vi, mục tiêu và giới hạn môn học.
2. `ARCHITECTURE.md`: thuật toán, trạng thái, failure handling và triển khai.
3. `API_CONTRACTS.md`: REST schema, gRPC semantics và error codes.
4. `storage.proto`: wire contract có thể generate stub Python.
5. `IMPLEMENTATION_PLAN.md`: thứ tự code và checks trước demo.

Kế hoạch milestone: [M1 — Storage Node thật](M1_IMPLEMENTATION_PLAN.md) đã hoàn
thành; [M2 — Metadata quan sát ba node](M2_IMPLEMENTATION_PLAN.md) đã hoàn thành
P1–P6. Tiếp theo M3 upload/download RF=2; data replication/fallback chưa triển khai.

## Quyết định mới so với bản V2 trước

| Chủ đề | Chốt hiện tại |
|---|---|
| Người triển khai | Một người; backend trước, UI sau |
| Health | Pull HealthCheck định kỳ từ Metadata, thay mô tả push heartbeat |
| Repair | Manual, qua Metadata, bốn RPC hiện có đủ |
| Upload commit | Chỉ AVAILABLE khi mọi chunk có RF replica đã ack đúng |
| Download | Assemble tempfile rồi mới HTTP 200; checksum cả chunk và file |
| Xóa/failed upload | Tombstone và pending cleanup lưu DB; không bỏ attempted replica khi timeout |
| Đồng thời | Một Metadata process/worker; operation lock cho thao tác dữ liệu |
| Giới hạn demo | File mặc định tối đa 64 MiB; chunk 2 MiB; unary gRPC |
| Hai máy | Tắt B có thể vẫn đọc; chỉ còn một node thì chưa thể repair đủ RF |

Các lựa chọn này cụ thể hóa phần còn mở của overview. Không thay stack, không thêm infrastructure. Overview trong bộ này đã cập nhật cách tổ chức một người và health/repair cho nhất quán.

## Quy tắc cho AI coding agent

Đọc cả overview, architecture và API trước khi viết service. `storage.proto` quyết định field number/type trên wire; API_CONTRACTS quyết định validation và REST shape; ARCHITECTURE quyết định thuật toán và trạng thái; overview quyết định scope. Nếu có mâu thuẫn thực, báo và sửa docs thay vì tự chọn một kiến trúc khác.

Giữ React → REST → Metadata/FastAPI/PostgreSQL → unary gRPC → ba Storage Nodes với volumes riêng. Không tự thêm gateway, Kafka, Redis, Kubernetes, streaming, Raft, auth, file edit hoặc client gọi storage trực tiếp. Giữ RF=2 cho demo, failure-domain placement, checksum và cleanup bền vững.

Lựa chọn tên helper, tách file Python hay refactor nội bộ được tự quyết nếu giữ nguyên contract và hành vi. Thay scope, persistence model, transfer mode hoặc commit/failure semantics cần cập nhật đặc tả và trao đổi với người phụ trách trước khi code.

## Trạng thái kiểm tra

Đã kiểm tra ngày 02/10/2026: protoc sinh stub Python thành công; import client/server modules thành công; descriptor có đúng bốn unary RPC; StoreChunk payload 2 MiB serialize/deserialize đúng và dưới message limit; 11 JSON examples parse hợp lệ. Môi trường kiểm tra: Python 3.12.14, grpcio 1.84.0, protobuf 7.36.2. Đây là phiên bản dùng kiểm tra tài liệu, chưa phải dependency lock của implementation.

Việc compile proto không chứng minh upload, failover hay repair đã hoạt động.
Evidence implementation M0–M2 và ranh giới chưa làm nằm ở phần tiến độ bên dưới;
M3–M6 chỉ hoàn thành khi có code và kiểm tra thật theo gate của từng milestone.

## Tiến độ implementation — 03/10/2026

**M1 đã hoàn thành P1–P6.** Full backend suite 106 passed, không skipped/deselected;
lint/format 29 files, schema drift check và base smoke HTTP/DB/registry/ba node qua.
Tại gate M1, Metadata và cả ba node dùng build M1 mới, volumes giữ nguyên. Persistence/crash
và volume isolation đã có bằng chứng P5; chưa có replication/failover. Tiếp theo
lúc đó là M2: health polling và REST nodes/cluster.

**M2 đã hoàn thành P1–P6.** Registry, monotonic detector, poll worker và REST
nodes/cluster đã qua các gate tập trung P1–P4; P5 đã deploy Metadata M2 và kiểm
tra down/recovery, restart process/DB thật, persistence và volumes độc lập.
P6 rà source/contracts/evidence, chốt DoD và bàn giao M3; không sửa runtime hay
rerun tests/lint/build/smoke. Counts từng phase có regression trùng nhau, không
coi là full-suite run. Tiếp theo M3 upload/download RF=2.
Chi tiết: [evidence P6 và bàn giao](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p6--03102026).

Review M2 sau P6 đã sửa ba findings: guard/validation smoke bị bỏ dưới `-O`,
interval không hữu hạn làm scheduler chết nhưng ready vẫn 200, smoke từ chối
disabled history hợp lệ. 54 native targeted + 53 Docker focused integration
passed, lint/format 8 files qua. Metadata đã cập nhật bản sửa, baseline read-only
`-O` passed, tất cả services healthy; readiness hiện kiểm tra scheduler đang chạy.

Các ghi nhận bên dưới là lịch sử tại thời điểm từng phase.

**M2 P1 đã hoàn thành:** registry reset DOWN giữ lịch sử cùng endpoint, vô hiệu
snapshot khi endpoint/domain đổi; startup recovery giữ operation_lock. Focused
PostgreSQL/lifespan checks 16 passed, không skipped/deselected; lint/format 3 files
qua. Chỉ rebuild tests image, chưa deploy/restart live services. Tiếp theo P2;
poll worker, nodes/cluster API và restart thật vẫn chưa qua gate. Chi tiết trong
[evidence P1 của M2](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p1--03102026).

**M2 P2 đã hoàn thành:** health state machine dùng monotonic age, kiểm tra
identity/writable, giữ hard-DOWN đến success và trả immutable snapshot mới.
32 targeted tests native Windows qua, không skipped/deselected; lint/format
2 files qua. Một warning pytest cache bị sandbox chặn ghi. Chưa nối worker/RPC/DB,
không build/deploy/restart services trong P2. Tiếp theo P3; chi tiết trong
[evidence P2 của M2](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p2--03102026).

**M2 P3 đã hoàn thành:** lifespan poll worker, RPC độc lập mỗi node, transaction
DB ngắn và publish snapshot sau commit, shutdown đóng threads/channels trước
engine. 32 focused integration/regression tests Docker Linux qua, không skipped/
deselected; lint/format 5 files qua. Chỉ rebuild tests image, chưa deploy/restart
services. Tiếp theo P4 REST nodes/cluster; Compose smoke/restart thật còn ở P5.
Chi tiết: [evidence P3 của M2](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p3--03102026).

**M2 P4 đã hoàn thành:** GET nodes/cluster đọc snapshots, DTO null/UTC, counters
theo RF của file và live replica eligibility. Cluster aggregates trong một SQL,
GET không gọi RPC/acquire lock. 32 HTTP/PostgreSQL checks Docker Linux passed,
không skipped/deselected; lint/format 6 files qua. Chỉ rebuild tests image, chưa
deploy/restart services. Tiếp theo P5 lifecycle/Compose smoke; M2 chưa hoàn thành.
Chi tiết: [evidence P4 của M2](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p4--03102026).

**M2 P5 đã hoàn thành:** 1 lifecycle test passed với hai Metadata child processes
thật, PostgreSQL schema riêng và gRPC thật; startup reset/recovery và metadata
preservation qua restart đúng. Metadata đã deploy build M2; Compose smoke
node-2 ACTIVE → SUSPECTED → DOWN → ACTIVE, live/ready 200 và hai node khác ACTIVE.
Restart PostgreSQL giữ nguyên toàn bộ fixture rows/columns; named volumes không
đổi, ba Storage volumes độc lập, schema fixture đã dọn. Lint/format 3 Python
files, PowerShell syntax passed; tất cả services healthy. Không rerun full suite,
không coi replication/failover đã có. Còn P6 rà DoD/bàn giao M3.
Chi tiết: [evidence P5 của M2](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p5--03102026).

Review sau P6 đã sửa hai bug Health worker starvation và accounting khi Store lại
chunk mất trên disk. Focused Storage/lifecycle regression: 105 passed, 4 deselected;
lint/format qua. Năm cases mới và fixtures dùng production server factory đã bổ sung.

Base M0 đã được thiết lập trong repo: Python 3.12 venv/dependency lock, proto
generation/import, config/logging, Metadata health API, migration bốn bảng và
Storage HealthCheck. Compose local chạy PostgreSQL, Metadata và ba node với
volumes riêng; 7 checks qua trong Docker Linux, schema không drift và smoke qua
sau Metadata restart. Hướng dẫn thực tế: [README repo](../README.md).

M1 P1 đã có validation đầu vào data RPC, layout chunk/tempfile và startup
filesystem cleanup/used_bytes/limit checks. M1 P2 đã có StoreChunk atomic,
idempotency, conflict handling và metrics sau commit. Focused integration P2:
70 passed, 4 deselected trong Docker Linux; lint/format qua.

M1 P3 đã có Get snapshot/hash actual bytes và Delete idempotent dưới cùng mutex
với Store. Focused integration P3: 82 passed, 4 deselected trong Docker Linux,
gồm 12 cases mới và hồi quy Storage; lint/format qua. Chỉ image tests được rebuild;
không chạy DB tests, cluster smoke hay restart. P4–P6 chưa hoàn thành.

M1 P4 đã có stats snapshot dưới lock riêng, Health không chờ operation mutex,
probe short-write check và sanitize lỗi bất ngờ. Focused integration: 98 passed,
4 deselected, gồm 16 cases P4 về health/concurrency/cancel/deadline/ack loss và
hồi quy Storage; lint/format qua. Chỉ rebuild image tests, không restart runtime
containers hay chạy DB/cluster smoke. P5–P6 và M1 chưa hoàn thành.

M1 P5 đã xác minh 2 MiB qua restart container thật, node-2 NOT_FOUND với chunk
node-1 và Delete lặp đưa metrics về baseline. Hai lifecycle scenarios qua process
thật kiểm tra restart và SIGKILL sau fsync trước commit, startup dọn tempfile và
giữ chunk cũ. Có smoke client theo mode và manifest; hướng dẫn trong README repo.
Node-1/node-2 đã cập nhật build, Metadata/node-3 chưa recreate. Không rerun suite
cũ/DB/Metadata smoke; P6 và M1 chưa hoàn thành.

Luồng file REST chưa được triển khai; health polling và GET nodes/cluster đã có
ở M2. Cleanup worker, repair và UI dự án thuộc các milestone tiếp theo. Không coi việc
container healthy là bằng chứng replication hoặc failover đã hoạt động.

Bằng chứng và bàn giao M1: [M1 — Một Storage Node thật, phase P1–P6](M1_IMPLEMENTATION_PLAN.md).
Bằng chứng và bàn giao M3: [M2 — Metadata quan sát ba node, phase P1–P6](M2_IMPLEMENTATION_PLAN.md).

Mã nguồn Python, contract implementation, tests/scripts và dependency lock đã
gom vào `backend/`, song song với `frontend/`. `docs/` và `deploy/` dùng chung
ở repo root. Setup/Docker và các đường dẫn trong tài liệu đã cập nhật; 58 tests
và smoke cluster vẫn qua với cấu trúc mới.
