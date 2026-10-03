# Kế hoạch triển khai — một người code

**Ngày:** 02/10/2026 · **Mục tiêu:** hoàn thành V1 đã chốt; làm tuần tự theo milestone.

Mốc ngày là ước lượng 14–18 buổi tập trung, không phải cam kết xong trong 2–3 tuần dù thời gian mỗi ngày khác nhau. Với một người code, không giao module cho ba thành viên như overview cũ. Các thành viên khác nếu có có thể giúp chạy máy B, chuẩn bị slide và học demo; không tính đó là code song song.

## 1. Điểm bắt đầu

Đọc `README.md` → `PROJECT_OVERVIEW.md` → `ARCHITECTURE.md` → `API_CONTRACTS.md` → `storage.proto`. Không cần thêm SRS/UML/WBS trước khi code. Khi code, cập nhật docs nếu hành vi thay đổi; không viết thêm tài liệu chỉ để tăng số file.

Repo dự kiến:

| Đường dẫn | Nội dung |
|---|---|
| `docs/` | Overview, kiến trúc, API, plan |
| `backend/contracts/storage.proto` | Contract gRPC |
| `backend/generated/` | Stub Python sinh tự động |
| `backend/metadata/` | FastAPI, coordinator, DB, migration, health/cleanup |
| `backend/storage/` | Một Storage server dùng env để chạy nhiều node |
| `backend/common/` | Config/logging dùng chung giữa hai service Python |
| `frontend/` | React UI |
| `deploy/` | Compose local, machine A, machine B; env examples |
| `backend/tests/` | Các integration/failure checks cần thiết |
| `backend/scripts/` | Generate proto, smoke/demo helpers nếu cần |

Base M0 đã có các service skeleton và Compose local chạy được. Hướng dẫn thực tế
nằm trong [README repo](../README.md); các luồng dữ liệu vẫn cần triển khai theo
milestone bên dưới.

## 2. Milestone và điều kiện chuyển bước

| Mốc | Ước lượng | Công việc | Phải chứng minh trước khi chuyển |
|---|---|---|---|
| M0: skeleton và contract | Buổi 1 | Python 3.12 venv, dependency lock, proto generation/import, logging/config, folder layout | Stub import được ở cả client/server; config sai fail rõ |
| M1: một Storage Node thật | Buổi 2–3 | Store/Get/Delete/Health, checksum, atomic write, idempotency, volume | Store 2 MiB → Get bytes đúng → restart vẫn đọc → Delete hai lần thành công |
| M2: metadata và ba node | Buổi 4–5 | Migration 4 bảng, registry env, health polling, local Compose | Ba node identity đúng, volumes khác; node down được phát hiện; DB giữ qua restart |
| M3: upload/download RF=2 | Buổi 6–8 | Placement, PENDING trước RPC, commit AVAILABLE, assemble tempfile, fallback | File nhiều chunk có 2 replica/chunk; download SHA đúng; tắt một node vẫn đọc |
| M4: failure/delete/repair | Buổi 9–11 | FAILED cleanup, DELETE tombstone, background cleanup, repair cursor/budget, startup recovery | Delete khi node tắt không hồi sinh; repair khôi phục RF; timeout không mất mapping |
| M5: UI tối thiểu | Buổi 12–13 | Upload/list/download/delete, cluster, placement, repair summary | UI gọi đúng contract, 100% upload không báo xong sớm; đọc lỗi và pending cleanup rõ |
| M6: hai máy và rehearsal | Buổi 14–16 | Compose A/B, env LAN, port check, failure-domain demo, setup/demo guide | Build giống nhau trên 2 host; ngắt B vẫn đọc; bật lại B kiểm tra mapping |
| Buffer | Buổi 17–18 | Sửa lỗi tích hợp và luyện giải thích | Không thêm tính năng lớn |

M3 đã dùng RF=2; không đợi cuối dự án mới thêm replication vào happy path. Checksum làm từ M1. Không làm UI đẹp trong lúc backend chưa đọc file đúng. Với một người, ưu tiên một luồng end-to-end chạy thật rồi mở rộng xử lý lỗi.

M0 và M1 đã hoàn thành ngày 03/10/2026. Bằng chứng M1:
[Storage Node, phase P1–P6](M1_IMPLEMENTATION_PLAN.md): 106 backend tests qua,
lint/format, schema check và cluster smoke qua; persistence/restart/crash và
volume độc lập đã được kiểm tra thật. Chưa coi replication/failover hay REST file
flows là hoàn thành. Milestone tiếp theo là M2: health polling, node snapshots,
API nodes/cluster và kiểm tra down/recovery/DB restart.

## 3. Kiểm tra có ý nghĩa

Các checks dưới đây kiểm tra ranh giới và lỗi thực, không cần tạo test cho từng getter.

| Check | Kết quả cần đạt |
|---|---|
| File 0 byte, nhỏ hơn chunk, đúng bội chunk, vượt chunk 1 byte | Count/index/size đúng; full SHA-256 giống nguồn |
| File 10–20 MiB | Nhiều chunk, mapping phân tán; không dùng toàn-file read vào RAM |
| Store cùng ID cùng bytes; cùng ID bytes khác | Retry thành công; conflict bị từ chối |
| Store đã ghi nhưng ack bị timeout (fault injection ở test) | Retry không tạo version khác; PENDING mapping còn để cleanup |
| Upload thiếu RF hoặc node đầy giữa upload | Không AVAILABLE; FAILED và cleanup attempted replicas |
| Sửa bytes một replica trong DATA_DIR | Download nhận ra checksum sai, lấy replica khác; UI CORRUPTED |
| Tắt tất cả replica của một chunk | 503 trước file response; không nhận file thiếu |
| Delete khi node DOWN, restart Metadata rồi bật node | Pending tồn tại; cleanup sau đó hoàn tất; file không xuất hiện lại |
| Crash Metadata khi file UPLOADING | Restart FAILED; cleanup attempted replicas; không resume giả |
| Repair sau tắt node-2, node-3 còn hoạt động | Source đúng → destination mới → RF=2; same chunk ID |
| Node quay lại với volume trống | Health không được coi là integrity; Get/repair phát hiện MISSING |
| Delete/repair hoặc hai upload đồng thời | Một thao tác chạy; request cạnh tranh trả 409; không hồi sinh file |
| Một máy thật ngắt LAN | Health DOWN; mọi chunk còn ở A đọc được; thiếu destination không báo repair thành công |

Có thể bắt đầu bằng script Python/curl và vài integration tests; khi service đã ổn dùng pytest cho các lỗi khó tái hiện. PowerShell dùng `curl.exe` hoặc script Python để tránh alias khác hành vi. Ghi lại lệnh và kết quả đã chạy trong setup/demo guide khi triển khai, không tự đánh dấu tests pass từ bộ đặc tả này.

## 4. UI chỉ cần hai màn hình

- Trang chính: upload, bảng files, cluster nodes, thông báo thao tác.
- File detail: chunk_index → replicas, node/domain/status, checksum, button repair và summary.

Polling status 3–5 giây đủ cho demo; không cần WebSocket. Disable action buttons trong lúc frontend chờ thao tác, nhưng backend vẫn enforce lock. Không làm rename/version/search/auth khi V1 chưa qua DoD. Không có phần trăm server replication nếu chưa có API đo thật.

## 5. Demo cuối kỳ

1. Khởi động A/B, show ba node ACTIVE và hai domain. Upload file 10–20 MiB, show một replica mỗi chunk ở A và B; download hash đúng.
2. Tắt node-2 riêng, chờ detector, download vẫn đúng; repair sang node-3, show RF khôi phục khác domain.
3. Bật node-2 lại, giải thích có thể dư replica; V1 không auto prune.
4. Ngắt toàn máy B, show node-2/3 DOWN, download từ node-1; repair trả NO_DESTINATION nếu cần, không hứa restore RF trên một node.
5. Bật lại B; health phục hồi, probe/repair kiểm tra các mapping. Demo delete khi một node tắt nếu còn thời gian.
6. Giải thích Metadata/DB trên A là SPOF; tắt A không có HA. Local dev một host chỉ chứng minh lỗi process.

Không chạy kịch bản host failure lần đầu vào ngày nộp. Trước demo cần kiểm tra Wi-Fi isolation/firewall/IP, Docker volumes và checksum file mẫu. Giữ một kịch bản local dự phòng cho sự cố mạng lớp học; mô tả đúng hạn chế nếu phải dùng nó.

## 6. Definition of Done

- [x] Proto generate/import được với dependency lock của dự án (M0, 03/10/2026).
- [ ] Upload/list/detail/download/delete theo contract.
- [ ] Chunk/RF/hash thực sự đúng và có dữ liệu ở volumes độc lập.
- [ ] Health detector, fallback read và manual repair hoạt động.
- [ ] Delete/failed upload cleanup giữ qua Metadata restart.
- [ ] Một Metadata worker; serialization và timeout/retry theo đặc tả.
- [ ] React hiện rõ node/domain/chunk và outcome repair.
- [ ] Cùng code/build chạy được dev local và demo hai máy.
- [ ] Demo physical host failure không làm mất khả năng đọc file còn đủ replica ở A.
- [ ] Có hướng dẫn setup thực tế và kịch bản demo đã chạy thử.
- [ ] Docs phản ánh code cuối, không hứa metadata HA hoặc recovery khi không còn source.

## 7. Nếu tiến độ chậm

Cắt UI polish, tìm kiếm, rename/version và mọi extension trước. Giữ manual repair; không nâng automatic repair hoặc streaming RPC. Giảm file demo, số lượt thao tác và độ phức tạp UI. Đừng giảm RF xuống 1 hoặc bỏ demo hai máy để gọi là cùng V1; đó là thay scope cần bàn lại.

Nếu sau M3 đã tốn hơn khoảng 8 buổi, cập nhật tiến độ theo tình trạng thật. Chưa có deadline cụ thể nên không gắn kế hoạch với một ngày nộp tự suy đoán. Một người code có thể triển khai nhanh hơn về phối hợp, nhưng tổng công việc vẫn cần thời gian.
