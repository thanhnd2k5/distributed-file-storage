# Client tối thiểu V2 — C4

Ngày 25/10/2026. Route riêng **`/uploads-v2`**, không gắn V2 vào runtime/API V1.
C4 cung cấp pre-hash/create/resume/parts/complete/abort; catalog/download/UI đầy
đủ và thử tải nhiều instance vẫn thuộc các mốc sau.

## Chạy client

Trong `frontend/`:

```powershell
npm run dev
```

Mở `http://127.0.0.1:5173/uploads-v2`. Client gọi cùng origin `/api/v2/`.
Dev proxy chỉ chuyển `/api/v2` tới `http://127.0.0.1:18080` mặc định; đổi bằng
environment `V2_DEV_PROXY_TARGET` trước khởi động Vite nếu dùng ingress khác.
Production cần ingress cùng origin route JSON/bytes như C3 và phục vụ frontend.
Client cần HTTPS hoặc localhost với Web Worker, Web Locks và `crypto.randomUUID`.

API adapter riêng ở `frontend/src/api/storageV2`, không dùng auth client V1,
không tự retry Axios mutation và không forward Authorization mặc định. Server
response/body là source của trạng thái xác nhận; HTTP upload progress chỉ là
bytes đã gửi từ browser. Một controller giữ tối đa một request part; policy
server tối đa hai part không bắt buộc client phải chạy song song.

## Chuẩn bị và giữ identity nguồn

`hash.worker.js` dùng **`@noble/hashes` 2.4.0**, exact version/integrity trong
`package.json`/`package-lock.json`. Incremental API theo
[README chính thức](https://github.com/paulmillr/noble-hashes#implementations).
Worker đọc `blob.slice()` từng block tối đa 262144 byte; không đọc cả file vào
`arrayBuffer()` hay dùng Web Crypto digest cho đường hash nguồn. Thư viện và
Worker được bundle riêng; terminate Worker khi người dùng dừng chuẩn bị.

Full SHA-256 hoàn thành trước create, và được tính lại khi resume phần upload
OPEN. Chỉ size/hash quyết định nguồn có khớp phiên; tên và thời gian sửa file
không thay thế content identity. Mỗi part được hash riêng trước PUT; không thay
hash đã cố định của part cũ. Nguồn đổi sau pre-hash vẫn bị full-file finalize
kiểm lại; không công bố chỉ vì part đã VERIFIED.

## Journal và phục hồi

Một journal localStorage mỗi API base URL/origin, format version 1:

- UUID Idempotency-Key và canonical payload tạo phiên được lưu **trước POST**.
- Upload ID, server state gần nhất, cleanup pending, abort intent và part đang
  chờ đối soát. Không lưu claim token, bytes, Blob hoặc File handle.
- Thời điểm `next_retry_at` để giữ Retry-After qua reload. Journal đọc tối đa
  16 KiB; bản ghi hỏng không bị tự ghi đè. Không lưu được journal thì chặn mutation
  tiếp theo, không chuyển ngầm sang state chỉ trong RAM.

Web Locks chặn hai controller/tab cùng origin ghi đè journal; thao tác trong
controller cũng có admission. Đây không phải kiểm chứng hai API/Transfer C5.
Sau reload người dùng chọn lại nguồn nếu còn phần phải gửi. FINALIZING có thể
tiếp tục chờ không cần file; abort/cleanup cũng không cần chọn lại nguồn.

Mất phản hồi create: giữ payload/key; lần tiếp tục rõ ràng của người dùng replay
đúng operation sau xác minh nguồn, không tạo key mới. Abort khi chưa nhận upload
ID có thể resolve đúng create intent đã gửi bằng key cũ rồi abort phiên đó.

Resume đọc parts theo trang, giữ tối đa một page 100 record; bỏ các part VERIFIED.
GET sau PUT lỗi là bắt buộc trước quyết định gửi lại. VERIFIED sau mất response
không resend; CLAIMED không bị chiếm lại bằng write mới. RECONCILING có thể được
gửi lại đúng hash sau status/backoff để dùng synchronous handoff C3.
Mỗi part tối đa ba lượt trong một thao tác; hết budget giữ phiên để tiếp tục sau.

GET tạm lỗi có tối đa ba lượt. PUT timeout 125s, control POST 20s, GET 15s.
Retry-After được lưu và tôn trọng trước retry; delay hơn 30s làm dừng tại client
để tiếp tục sau, không giảm Retry-After của server. Poll finalize/cleanup mặc
định 120 lượt, cách tối thiểu 1s; hết budget là **paused**, không tự coi FAILED.

Complete chỉ POST sau các part, rồi chờ trạng thái upload. Mất complete response
thì GET; FINALIZING/COMPLETED không POST lại mù. Chỉ COMPLETED là thành công.
Abort lưu intent trước mutation, kiểm trạng thái và theo dõi cleanup; không tự
DELETE một file đã COMPLETED. Nút phiên mới chỉ mở khi terminal và hết pending.

**Dừng trên máy này** terminate hash/abort HTTP wait và giữ journal; không tự gửi
abort server. Điều hướng/unmount cũng dừng xử lý tại client. **Hủy phiên trên
server** là thao tác riêng. Sau reload UI hiện trạng thái cuối đã lưu, còn counters
ghi chưa cập nhật cho tới GET, không giả định zero bytes hoặc file mới.

## Kiểm chứng tái lập

Tier **targeted checks** (từ `frontend/`):

```powershell
.\node_modules\.bin\eslint.cmd src/api/storageV2 src/app/pages/storageV2 src/app/router/router.jsx checks
npm run build
```

Tier **focused browser/API integration** cần Docker và PostgreSQL V2 healthy.
Nếu dependency chưa sẵn sàng, tuân thủ blocker policy trong `AGENTS.md`; không
tự start/reset DB cho một yêu cầu chỉ kiểm thử. Cấu hình fixture C4:

```powershell
docker info --format '{{.ServerVersion}}'
docker compose -f deploy/compose.v2-metadata-tests.yml ps
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -c 'SELECT current_database(),current_user,1 AS ready'

docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-browser-tests.yml run --rm --no-deps -p 127.0.0.1:18080:18080 --name dfs-v2-c4-fixture tests python tests/v2_c4_fixture.py
```

Fixture sở hữu schema `test_v2_<uuid>`, hai Storage gRPC/SQLite/bytes tạm, API,
internal API, Transfer và Nginx thật. Worker bật/tắt qua route chỉ có trong test.
Overlay browser thêm một mạng riêng **chỉ cho tests service**, không sửa mạng
internal/PostgreSQL và chỉ publish ingress vào localhost. Fault hooks nằm trong
`backend/tests/v2_c4_fixture.py`, không trong production app.

Chạy Vite rồi mở **`/checks/v2-upload.html`**, bấm Run C4 checks. Chờ
RELOAD_READY, reload trang thật, bấm Run C4 checks lần nữa. Harness dùng chính
controller, journal, Axios adapter và hash Worker của feature. Có inject mất
response sau commit, 503 trước PUT và hasher fault cho cửa sổ nguồn đổi. API,
transaction, gRPC, bytes và Worker finalize/cleanup đều thật. Web Crypto digest
chỉ dùng làm reference trên fixture nhỏ, không phải thuật toán hash của app.

`/api/v2/_c4/status` trả report và bằng chứng Python đọc lại bytes của **cả hai
replica**, kiểm chunk/full hash, size và distinct domains cho từng AVAILABLE file.
Các route `_c4` chỉ có trong fixture test. Sau lưu evidence, dừng graceful bằng:

```powershell
Invoke-RestMethod -Uri http://127.0.0.1:18080/api/v2/_c4/control -Method Post -ContentType application/json -Body '{"finish":true}'
```

Cuối context fixture tự drop schema và dọn thư mục tạm; không đụng volume V1.
Network browser có thể dọn sau khi fixture container đã hết attachment.

Trang **`/checks/v2-upload-ui.html`** là fixture bổ sung để chọn File tổng hợp qua
input change event và thao tác trên React page thật. Không dùng nó để giả là
đã kiểm hộp thoại chọn file hệ điều hành. Không đưa checks HTML vào bundle
production; build chỉ dùng entry `index.html` hiện có.

Kết quả và giới hạn lần chạy 25/10/2026 ở
[evidence C4](V2_C_IMPLEMENTATION_PLAN.md#12-evidence-c4--25102026).
