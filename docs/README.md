# Distributed File Storage — bộ đặc tả V1

**02/10/2026 · Một người code · Chưa có implementation trong bộ này.**

## Đọc theo thứ tự

1. `PROJECT_OVERVIEW.md`: phạm vi, mục tiêu và giới hạn môn học.
2. `ARCHITECTURE.md`: thuật toán, trạng thái, failure handling và triển khai.
3. `API_CONTRACTS.md`: REST schema, gRPC semantics và error codes.
4. `storage.proto`: wire contract có thể generate stub Python.
5. `IMPLEMENTATION_PLAN.md`: thứ tự code và checks trước demo.

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

Việc compile proto không chứng minh upload, failover hay repair đã hoạt động. Các milestone và DoD trong plan vẫn chưa được đánh dấu hoàn thành cho đến khi có code và chạy kiểm tra thật.

Có thể bắt đầu code theo M0/M1 ngay; không cần thêm vòng tài liệu trước triển khai.

## Tiến độ implementation — 03/10/2026

Base M0 đã được thiết lập trong repo: Python 3.12 venv/dependency lock, proto
generation/import, config/logging, Metadata health API, migration bốn bảng và
Storage HealthCheck. Compose local chạy PostgreSQL, Metadata và ba node với
volumes riêng; 7 checks qua trong Docker Linux, schema không drift và smoke qua
sau Metadata restart. Hướng dẫn thực tế: [README repo](../README.md).

M1 P1 đã có validation đầu vào data RPC, layout chunk/tempfile và startup
filesystem cleanup/used_bytes/limit checks. 58 tests qua trong Docker Linux;
request sai trả INVALID_ARGUMENT, request hợp lệ vẫn UNIMPLEMENTED.

Store/Get/Delete xử lý dữ liệu và luồng file chưa được triển khai; health polling, cleanup
worker, repair và UI dự án vẫn thuộc các milestone tiếp theo. Không coi việc
container healthy là bằng chứng replication hoặc failover đã hoạt động.

Kế hoạch bước tiếp theo: [M1 — Một Storage Node thật, phase P1–P6](M1_IMPLEMENTATION_PLAN.md).

Mã nguồn Python, contract implementation, tests/scripts và dependency lock đã
gom vào `backend/`, song song với `frontend/`. `docs/` và `deploy/` dùng chung
ở repo root. Setup/Docker và các đường dẫn trong tài liệu đã cập nhật; 58 tests
và smoke cluster vẫn qua với cấu trúc mới.
