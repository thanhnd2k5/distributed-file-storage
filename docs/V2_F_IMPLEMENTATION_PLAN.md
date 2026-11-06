# Mốc F — automatic repair, rejoin và scrub

Ngày 01/11/2026. Đây là **kế hoạch**, chưa có implementation hoặc evidence F.
E1–E3 hoàn tất baseline; E4 còn chờ xác minh lại gate 1/4/16 session sau thay
đổi fixture. Chỉ bắt đầu runtime F sau khi chốt E4. C5/browser/file 1–10 GiB
vẫn tạm gác, không được suy là hoàn tất từ E hoặc F.

Đọc cùng [thiết kế §6–7](V2_DESIGN.md), [schema](V2_METADATA_SCHEMA.md),
[wire/API](V2_API_CONTRACTS.md) và [correctness](V2_CORRECTNESS.md).

## Hiện trạng và thứ tự triển khai

Schema foundation đã có health/observation generation, incarnation, allocation
ledger và ba kind HEALTH/REPAIR/SCRUB. Storage có HealthCheck, ListAllocations,
InspectAllocation và streaming Get/Store. Đây là nền tảng, chưa phải runtime F.

Worker hiện chỉ dispatch CLEANUP/RECONCILE/FINALIZE. Snapshot và xử lý terminal
session đang phục vụ upload; không đưa health/repair của file AVAILABLE qua
nhánh bỏ qua session COMPLETED. Job target hiện là UUID, còn node_id là chuỗi:
F1 phải chốt mapping target/payload trước khi enqueue HEALTH.

| Phase | Phạm vi | Gate nhỏ nhất đủ |
|---|---|---|
| F1 | Health polling có deadline/hysteresis, observation fencing và eligibility | Policy thuần; PostgreSQL/gRPC thật cho hai poller, late response, timeout và sai scope |
| F2 | Phát hiện thiếu replica, enqueue repair có dedup, reserve đích mới và copy xác minh | PostgreSQL + ba Storage/failure domain trong fixture; race, mất ACK, capacity và bytes/hash |
| F3 | Rejoin/inventory theo trang, đối soát incarnation/epoch và orphan | Restart cùng volume, volume mới, late inventory và tombstone; không tự release charge |
| F4 | Scrub có ngân sách, observation missing/corrupt và repair handoff | Bytes hỏng thật, bounded reads, stale result và nguồn fallback |
| F5 | Gate kết hợp node failure/rejoin, Worker crash và tải người dùng | Focused nhiều process, đo budget/fairness và cleanup; không suy thành HA nhiều host |

## F1 — lát triển khai đầu tiên

### Contract cần chốt trước code

- Poll pull HealthCheck theo scope đã đăng ký: namespace, epoch, node ID và
  incarnation. Không khám phá hoặc tự kích hoạt disk mới bằng node ID cũ.
- Cấu hình hữu hạn cho interval, RPC deadline, số poll đồng thời, ngưỡng failure/
  recovery và độ cũ observation được chấp nhận. Ghi rõ các giá trị mặc định và
  state transitions trong contract; policy dùng trạng thái bền vững đủ để hai
  process/restart không đặt lại hysteresis hoặc tranh nhau promote node.
- Cấp observation generation trước RPC trong transaction ngắn. Khi ghi kết quả,
  CAS cả generation và identity/epoch/config còn hiệu lực; response cũ không
  ghi đè quan sát mới. Không giữ DB lock trong network I/O.
- Timeout chỉ là tín hiệu health; không đặt allocation MISSING/CORRUPT, xóa
  bytes hoặc trả capacity. Health thành công không tự bỏ quarantine/rejoin gate
  hoặc biến replica UNKNOWN/CORRUPT thành VERIFIED.
- Placement/read eligibility tiếp tục dùng trạng thái node/incarnation theo
  contract. Health filesystem_available_bytes là quan sát bổ sung, không tăng
  node budget hoặc trừ reservation lần nữa.
- Chốt ownership/dedup và mapping job HEALTH target/payload; xử lý HEALTH độc
  lập upload session. Dùng lease/token và bounded scheduling hiện có khi phù
  hợp; bảo toàn lượt cleanup/reconcile/finalize và shutdown budget của E3.
- Nếu cần lưu thêm hysteresis/cursor, thêm migration kế tiếp theo cơ chế version/
  checksum hiện có và cập nhật schema/ADR. Không sửa migration đã phát hành.

### Thứ tự thay đổi dự kiến

1. Chốt contract state transitions, defaults, observation CAS, job identity và
   persistence ở tài liệu API/schema/correctness liên quan.
2. Thêm policy thuần và repository transaction snapshot/apply observation;
   chọn test tên/file thực tế khi implementation tồn tại.
3. Thêm HealthCheck client validation và bounded poll scheduling, renew/cancel/
   retry/shutdown; không bật REPAIR/SCRUB khi chưa có handler tương ứng.
4. Chạy targeted policy và focused PostgreSQL/gRPC checks dưới đây; ghi evidence
   thật trước khi bàn giao F2. Chưa mở thêm endpoint UI ở F1.

### Gate F1 và hồi quy trực tiếp

| Case | Kết quả bắt buộc |
|---|---|
| Policy threshold/recovery | Chuyển state đúng ngưỡng; một timeout không được hiểu là mất bytes vĩnh viễn |
| Hai poller, response đảo thứ tự | Chỉ observation còn hợp lệ được ghi; không reset state/counter khi restart |
| Scope/incarnation/config đổi trong RPC | Kết quả cũ bị từ chối; node thay volume vẫn bị chặn cho tới đối soát/activation |
| Health thành công sau observation corrupt | Allocation vẫn CORRUPT; charged bytes và allocatable budget giữ đúng |
| Node không eligible | Placement mới loại node theo policy; không làm hỏng admission/accounting đang có |
| Poll chậm, retry và shutdown | Concurrency/lease/deadline hữu hạn; user progress và ba kind E3 vẫn tiến triển |

Tier dự kiến **targeted + focused integration** trên Docker Linux/PostgreSQL V2
test riêng và gRPC Storage fixture. Preflight read-only theo
[local tests](V2_LOCAL_TESTS.md). Chỉ ghi lệnh pytest cụ thể sau khi có file/test
thật; không thêm framework hoặc dùng lệnh giả làm evidence. Chọn trực tiếp
regression placement/incarnation, Worker fairness và lifecycle bị ảnh hưởng.

## Bất biến cho F2–F5

Repair dùng allocation UUID và reservation mới; nguồn phải qua checksum,
confirmation chỉ nhận ACK đúng scope/spec/generation và job token còn hiệu lực.
Manifest version/part bất biến; cập nhật replica projection qua allocations.
Tombstone/read lease và repair/delete phải được serialize để late repair không
hồi sinh file hoặc làm rò charge. Không còn nguồn hợp lệ thì báo thiếu replica,
giữ bằng chứng và retry policy; không công bố dữ liệu chưa kiểm chứng.

Node offline, corrupt hoặc inventory NOT_FOUND không làm charge biến mất.
Chỉ durable Delete ACK hợp lệ hoặc retire volume có quy trình xác nhận mới trả
charge. Online inventory giữ upper sequence và recheck/CAS từng observation vì
không có snapshot state qua các trang; bytes chưa rõ chủ phải quarantine/đối soát.

Repair/scrub thêm vào tổng execution/resident/RPC/buffer/I/O budget; đo lại gate
bị ảnh hưởng khi tăng kind hoặc process count. Muốn phục hồi RF=2/distinct domain
khi mất một node cần domain đích còn hoạt động và đủ dung lượng. Fixture nhiều
process cùng host chỉ chứng minh logic này, chưa chứng minh chịu mất host.

## Evidence hiện tại

Chỉ rà soát tài liệu, schema, Storage RPC và Worker dispatch ngày 01/11/2026.
Không thay runtime/schema, không chạy test cho kế hoạch tài liệu này. F1–F5
chưa bắt đầu; E4 vẫn chờ dependency và kết quả pytest xác nhận.
