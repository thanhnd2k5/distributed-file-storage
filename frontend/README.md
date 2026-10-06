# Distributed File Storage — frontend

React/Vite, JavaScript/JSX, TanStack Query và UI primitives từ starter.
Kế hoạch: [M5 P1–P7](../docs/M5_IMPLEMENTATION_PLAN.md).

## Chạy local

Từ `frontend/`, cài dependencies theo lockfile và tạo env local nếu chưa có:

```powershell
npm ci
Copy-Item .env.example .env.local
npm run dev -- --host localhost --port 5173 --strictPort
```

Không ghi đè `.env.local` đang dùng. `VITE_STORAGE_API_URL` trỏ Metadata,
mặc định `http://localhost:8000/api/v1`; URL có hoặc không có trailing slash
đều được chuẩn hóa. Origin HTTP(S) chưa có prefix sẽ được nối `/api/v1`.
Không chứa query/fragment/credentials trong URL. Đổi env phải restart Vite.
Nếu `METADATA_PORT` khác 8000, dùng port thực trong env frontend.

Mở `http://localhost:5173/`. Compose local hiện cho CORS origin này; `127.0.0.1`
hoặc port khác là origin khác. `--strictPort` tránh tự chuyển port ngoài CORS.
Backend setup/prerequisites nằm trong [README repo](../README.md).

## Routes và client

Storage `/`, `/cluster` và `/files/:fileId` là public, không gọi auth
initialization/getMe và không gửi token/cookies. Auth bootstrap của starter chỉ
mount trong nhánh login/dashboard/settings. Storage dùng client riêng trong
`src/api/storage/`, không dùng `api/rootApi.js` hoặc media/S3 helper của starter.

- `/` — upload và danh sách file
- `/cluster` — tình trạng cụm, nodes và repair toàn cụm
- `/files/:fileId` — chi tiết, placement và repair theo file

Nav Files / Cụm nằm trong `StorageLayout`; `TransferNotice` ở layout để giữ
tiến trình khi đổi trang.

P1 có shell/client public. P2 đã nối list/paging/inactive filter, detail/
placement và nodes/cluster bằng GET; gate đã hoàn thành, gồm reload/404/unmount
sau phục hồi crash và visual check sidebar hẹp.
P3 upload/download, P4 delete/pending cleanup, P5 manual repair và P6 UI/API
runtime/failure flows (fallback download, repair khi node DOWN, offline DELETE,
OPERATION_BUSY) đã qua gate trên Compose local một `dev_host`.
**M5 hoàn thành P1–P7 (05/10/2026).** P7 docs-only bàn giao M6; local process
failure không chứng minh hai host/LAN CORS. Xem
[evidence P6](../docs/M5_IMPLEMENTATION_PLAN.md#12-evidence-p6--05102026),
[bàn giao M6](../docs/M5_IMPLEMENTATION_PLAN.md#13-bàn-giao-m6--hai-máy-và-rehearsal)
và [evidence P7](../docs/M5_IMPLEMENTATION_PLAN.md#14-evidence-p7--05102026).
READY là readiness Metadata,
không bảo đảm mọi chunk đọc được. Nút làm mới chỉ gửi GET.

Snapshots cập nhật mỗi 4 s khi trang visible, dừng khi hidden/unmounted;
GET retry tối đa một lần, 400/404/422 không retry. File 404 dừng polling
tương ứng; ID sai format không gọi file API. Dữ liệu lần đọc thành công vẫn
được giữ qua lỗi refresh và có nhãn “Dữ liệu cũ”/timestamp. Đổi paging/filter
không mang data trang cũ sang query mới; tổng giảm thì về trang hợp lệ.
Readiness 503 không chặn GET snapshots. Replica history giữ DOWN/PENDING/
DELETED; metrics null khác 0 và không cộng capacity/free các node.

Upload gửi đúng một multipart `file`, kiểm size theo cluster nếu có và cho phép
0 byte. Progress chỉ là body gửi; 100% chuyển sang chờ lưu các bản sao, chỉ
201/AVAILABLE mới báo success và mở detail. Upload wrapper giữ `httpStatus`.
Provider giữ action state xuyên list/detail và khóa thao tác cạnh tranh, không
tự retry POST. Lỗi giữ code/details.file_id; outcome chưa rõ khóa chọn/retry
đến khi GET list reconcile thành công, bật inactive để kiểm tra file theo tên/
thời gian. Reconcile GET lỗi vẫn giữ khóa; không tự upload lại sau reconcile.

Download chỉ AVAILABLE, không dùng known_readable để cấm probing. Hiện chuẩn bị
trước body, chỉ có percent khi có loaded/total thực; GET lỗi Blob/non-JSON không
save. Kiểm 200/Blob/size đầy đủ và loại JSON/HTML response, chọn tên UTF-8
filename* rồi filename thường rồi metadata, bỏ path/control. Object URL được
revoke sau 30 s. UI chỉ báo đã chuyển cho trình duyệt lưu, không xác nhận file
trên đĩa. Gate P3 xác minh SHA-256 Blob nhận thật bằng nguồn, xem
[evidence P3](../docs/M5_IMPLEMENTATION_PLAN.md#9-evidence-p3--04102026).

Delete chỉ AVAILABLE/FAILED, dialog xác nhận tên/ID và kiểm lại eligibility
trước gửi. Provider khóa cạnh tranh upload/download/delete khi request chạy.
202/DELETING hiển thị pending và invalidate list/detail/chunks/cluster/nodes;
GET detail 4 s theo dõi cả khi quay về list, không poll DELETE. Detail nhường
interval cùng key cho provider, tránh hai pollers. GET 503 giữ stale pending;
404 dừng file/chunk polling và chỉ báo không còn file trong API đọc. Chỉ
DELETE 200/DELETED/pending=0 mới xác nhận đã dọn xong các bản sao.

Timeout/network/5xx xóa giữ ID và yêu cầu GET reconcile trước retry; GET lỗi
giữ khóa, DELETING tiếp tục theo dõi, 404 kết thúc, AVAILABLE/FAILED cho phép
xác nhận mới. Không tự retry DELETE. FAILED giữ error_code khi pending=0.
Gate P4 đã xóa ba fixture owned P3 sau user xác nhận; offline cleanup/recovery
thật nằm [evidence P6](../docs/M5_IMPLEMENTATION_PLAN.md#12-evidence-p6--05102026).
Xem [evidence P4](../docs/M5_IMPLEMENTATION_PLAN.md#10-evidence-p4--05102026).

Manual repair ở root quét toàn cluster hoặc theo node; detail cố định file ID
theo route, optional node filter. Node DISABLED vẫn chọn làm filter được.
Max chunks là integer 1..8. Một click gửi một POST, giữ filter/max_chunks và
cursor response khi tiếp tục. Đổi filter, kể cả chọn lại filter cũ, hoặc chuyển
route/reload bỏ scan cũ; scan mới dùng after=null. Chuyển trang khi đang POST
vẫn khóa thao tác, không coi server đã rollback, không ghép response vào scope mới.

Hiện summary từng lượt/tổng scan và outcome/domain_degraded từng chunk.
remaining_chunks=0 chỉ là quét hết scope; repaired_replicas chỉ đếm Store ack.
ERROR/UNAVAILABLE/NO_DESTINATION trong HTTP 200 vẫn có cảnh báo; retry chunk đã
qua cursor bằng scan mới. Zero-progress/remaining>0 vẫn cho tiếp tục với cursor
response, kể cả null. Timeout/network/5xx/DTO sai không tự retry POST hay đoán
cursor; phải GET placement/cluster thành công rồi tự bắt đầu scan mới. Reconcile
GET lỗi giữ khóa. Mọi response/outcome chưa rõ invalidate snapshots liên quan.
Gate P5 scan thật fixture owned 6 chunks qua ba lượt max_chunks=2; RF repair/node
recovery thật nằm [evidence P6](../docs/M5_IMPLEMENTATION_PLAN.md#12-evidence-p6--05102026).
Xem [evidence P5](../docs/M5_IMPLEMENTATION_PLAN.md#11-evidence-p5--05102026).

Client giữ status/code/details của error envelope V1, xử lý cả JSON lỗi trong
Blob download và fallback network/non-JSON. Timeout read 15 s, DELETE/repair
180 s, upload/download 900 s; transfer timeout cho phép replication/assembly
tuần tự với cấu hình demo mặc định, không là SLA cho mọi chunk-size cấu hình.
Network/timeout của mutation có outcome chưa rõ; UI phải reconcile trước user
retry. HTTP wrappers không retry; Query mutation dùng keys `storageKeys` được
đặt `retry:false`. P2 chỉ poll GET snapshot; không tự POST hoặc repair.

## Kiểm tra

Chọn tier theo [frontend AGENTS](AGENTS.md). Targeted ESLint chỉ cho files bị
ảnh hưởng; `npm run build` cho routing/import/bundle. Package chưa có dedicated
test/E2E script; lint/build không thay browser/API runtime evidence.
