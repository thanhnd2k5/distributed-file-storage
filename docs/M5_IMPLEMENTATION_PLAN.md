# M5 — UI tối thiểu cho Distributed File Storage

**Ngày lập:** 04/10/2026  
**Trạng thái:** M5 hoàn thành P1–P7 ngày 05/10/2026; bàn giao M6 hai máy và rehearsal.
**Đầu vào:** M4 hoàn thành P1–P7; [bàn giao M5](M4_IMPLEMENTATION_PLAN.md#12-bàn-giao-m5--ui-tối-thiểu).  
**Đầu ra:** hai màn hình dùng API thật để upload/list/download/delete, quan sát
cluster/placement và chạy manual repair với trạng thái, lỗi và cursor đúng contract.

## 1. Phạm vi và điểm xuất phát

Đọc [kế hoạch tổng](IMPLEMENTATION_PLAN.md), [overview](PROJECT_OVERVIEW.md),
[kiến trúc](ARCHITECTURE.md), [API contracts](API_CONTRACTS.md),
[frontend AGENTS](../frontend/AGENTS.md), [BASE_B](../frontend/docs/BASE_B.md)
và [STRUCTURE](../frontend/docs/STRUCTURE.md) trước implementation.

Giữ React/Vite, JavaScript/JSX, TanStack Query và các UI primitives đang có.
Ngôn ngữ chính tiếng Việt. Server state nằm trong Query; tiến trình thao tác,
file browser đã chọn và repair cursor là client state. Không thêm auth,
WebSocket, job queue, automatic repair, rename/version/search hoặc framework test.
M6 dành cho deployment hai host và rehearsal.

Snapshot dưới đây là điểm xuất phát trước P1; implementation/evidence hiện
hành được ghi ở cuối tài liệu.

| Hiện trạng đọc từ source | Việc cần làm ở M5 |
|---|---|
| `api/rootApi.js`: credentials và Authorization; adapter lỗi starter | Client storage riêng, không cookies/token, hiểu error envelope V1 |
| `.env.example`: API cũ ở port 3456 | Biến `VITE_STORAGE_API_URL=http://localhost:8000/api/v1`, tài liệu local rõ |
| `App.jsx` bọc AuthBootstrap; `Root.jsx` chờ auth initialized | Storage routes/layout không chờ auth init hoặc gọi getMe |
| Public routes rỗng; protected index redirect dashboards | Đặt index public cho storage, tránh route index cạnh tranh/redirect login |
| QueryClient có GET retry=1; chỉ có auth keys | Storage keys, polling có giới hạn, mutation retry=false rõ ràng |
| Có Button/Table/Badge/Progress/ConfirmModal và dropzone | Tái dùng khi hợp semantics; không nối S3/media upload starter vào DFS |
| Backend CORS cho `http://localhost:5173`, GET/POST/DELETE, Content-Type | Giữ đúng origin; download headers đã expose, không cần thêm CORS mặc định |
| Compose local: ba node đều `dev_host` | UI hiển thị domain thật; local evidence chỉ là process failure, chưa hai host |

**Source và live build:** evidence M4 P6 là build đã deploy lúc P6. Các fixes
sau review M4 có focused regression evidence trong image tests nhưng chưa
deploy live theo [ghi nhận review](M4_IMPLEMENTATION_PLAN.md#sửa-findings-sau-code-review--04102026).
Không coi live cluster hiện tại tự động chứa source mới. Trước gate UI thật,
đối chiếu build; nếu cần setup/deploy, xử lý trong phạm vi đã được cho phép.
Lượt lập kế hoạch này không deploy hoặc xác nhận health live.

## 2. Hai màn hình và cách tổ chức code

### Trang chính `/`

- Upload một file: chọn file, giới hạn cấu hình, progress HTTP và chờ commit.
- Bảng files: tên, size, thời gian, status; mặc định AVAILABLE, tùy chọn
  `include_inactive`, phân trang limit/offset, refresh và đường dẫn detail.
- Download/Delete theo state; thông báo thao tác và pending cleanup.
- Cluster/nodes: counts, node status/domain/metrics, trạng thái API readiness,
  operation busy và thời điểm snapshot gần nhất.
- Manual repair toàn cluster hoặc theo node, dùng chung phần UI repair với detail.

### File detail `/files/:fileId`

- FileSummary + checksum, RF/chunk size snapshot, error_code và health counters.
- Bảng chunks theo index; replica node/domain/status, cleanup_pending,
  last_verified_at/last_error; giữ cả DOWN/PENDING/DELETED history.
- Download/Delete và repair scoped theo file, optional node filter.
- Trạng thái loading/error/404 và cleanup DELETING; về trang chính rõ ràng.

Tổ chức dự kiến, có thể điều chỉnh tên khi implementation:

```text
frontend/src/
  api/storage/
    client.js                  # Axios riêng, base URL, timeout/error adapter
    index.js                   # HTTP files/nodes/cluster/ready/repair
    queryKeys.js               # keys bao gồm paging/filters/file ID
  app/layouts/StorageLayout.jsx # shell public, không phụ thuộc session
  app/pages/storage/
    index.jsx                  # trang chính, giữ thin
    detail.jsx                 # route detail
    components/                # FileTable, UploadPanel, PlacementTable,
                               # ClusterPanel, RepairPanel, Delete dialog...
    hooks/                     # queries, page controllers, actions, repair scan
    utils/                     # status labels, download/error helpers theo DFS
  app/router/                  # public routes + entry phù hợp
```

HTTP luôn ở `src/api/`; không gọi trực tiếp từ components. Các phần dùng trong
cả hai màn hình nhưng vẫn thuộc DFS ở cùng feature `storage/`. Tái dùng generic
components hiện có; không tự promote một helper chỉ có một consumer. Nếu phát
sinh ứng viên shared mới, đề xuất path/lý do theo frontend AGENTS trước khi
extract, trừ khi người dùng đã cho phép. Không mirror server cache vào Zustand.

## 3. Phase, thứ tự và gate

Làm tuần tự **P1 → P2 → P3 → P4 → P5 → P6 → P7**; không chuyển phase dựa trên
lint/build khi gate đòi runtime. P1–P5 kiểm tra increment theo luồng bị thay đổi;
P6 bổ sung các tình huống tích hợp còn thiếu, không chạy lại toàn bộ evidence
đã pass nếu code/environment không đổi. P7 chỉ rà và bàn giao.

| Phase | Đầu ra chính | Điều kiện chuyển |
|---|---|---|
| P1 — Client và shell public | Storage HTTP client, lỗi V1, routes/env | Không auth/getMe/token; GET API thật, lỗi network/envelope đúng |
| P2 — Snapshot UI | Files/list/detail/chunks + nodes/cluster/ready | Paging/inactive/404 và observations hiển thị đúng DTO |
| P3 — Upload/download | Luồng file end-to-end | 100% body chưa hoàn tất; 201/bytes/checksum và binary error đúng |
| P4 — Delete/cleanup | Delete 200/202 + theo dõi cleanup | DELETING không download/repair; 404 terminal dừng poll |
| P5 — Manual repair | Filters/cursor/summary/outcomes | Scan đúng remaining/cursor; zero-progress/timeout không tự retry |
| P6 — Gate UI runtime | Evidence browser/API cho failure/busy/reconcile | Affected flows thật pass, fixtures dọn đúng ownership |
| P7 — DoD/bàn giao M6 | Docs trạng thái/evidence/setup | Phân biệt static/runtime/local/hai host; không còn blocker M5 |

Ước lượng cũ M5 là 2 buổi, dùng như mục tiêu happy path. Dành khoảng **3–4 buổi
tập trung + buffer** cho public-shell integration, pending cleanup, cursor và
runtime evidence; cập nhật theo kết quả P1. Đây không là cam kết ngày hoàn thành.

### P1 — Client, lỗi và shell không auth

1. Client riêng lấy `VITE_STORAGE_API_URL`, mặc định cấu hình local có `/api/v1`;
   nối endpoint không lặp/mất prefix. `withCredentials=false`, không interceptor
   token, không Bearer dù localStorage có authToken từ starter.
2. Adapter giữ HTTP status, `error.code/message/details`, network/timeout;
   fallback dễ đọc cho HTML/non-JSON. Không đưa raw HTML/stack vào toast.
   Hỗ trợ error response dạng Blob cho download ở P3.
3. Storage route tree chạy độc lập AuthBootstrap và Root auth splash; xử lý
   index protected đang tồn tại. Giữ một router và các providers theme/locale/
   Query cần thiết. Shell/nav storage không đọc user profile/getMe.
4. Định nghĩa HTTP wrappers/query keys; `retry:false` cho mọi mutation. Timeout
   repair 180 s, không hiểu budget server 30 s là hard HTTP deadline. Upload/
   download cần timeout cho phép replication/assembly, chốt theo flow thực tế.
5. Cập nhật frontend env example và local run instructions; dùng port 5173
   đúng CORS, tránh Vite tự đổi port rồi báo lỗi như backend unavailable.

**Gate:** load và refresh deep link không login/splash vô hạn; DevTools không
gọi auth API hoặc gửi Authorization/credentials tới Metadata. GET health/
cluster thật qua client. Kiểm tra envelope và network/non-JSON fallback bằng
available browser tooling; response mô phỏng phải ghi rõ là adapter evidence.
Targeted ESLint các JS/JSX đã sửa; build một lần khi đổi routing/import/env.

### P2 — List, detail, placement và cluster chỉ đọc

1. List keys gồm limit/offset/include_inactive; mặc định chỉ AVAILABLE.
   Reset offset khi đổi filter; về trang hợp lệ khi tổng giảm sau delete.
   Có empty/loading/error/retry-by-user; không hiển thị mock data như API thật.
2. Detail/chunks keys theo file ID, query enabled khi ID hợp lệ. Invalid route
   ID có lỗi rõ; 404 dừng retry/poll tương ứng và cho về list.
3. Poll nodes/cluster/ready và list/detail cần thiết mỗi 3–5 s khi trang visible;
   không tiếp tục polling unmounted/hidden, không nhân interval theo component.
   Không poll binary download hoặc POST. GET retry hữu hạn, 404 không retry.
4. Hiển thị số 0 khác null/chưa có metrics; ACTIVE/SUSPECTED/DOWN/disabled,
   counters under/unavailable/domain_degraded/over/pending. Không cộng free/
   capacity node thành capacity cluster khi có thể dùng chung filesystem.
5. Ghi rõ observations theo metadata/health: known_readable không bảo đảm lần
   download tiếp theo; health không chứng minh integrity. Stale snapshot sau
   lỗi fetch phải có nhãn/thời điểm, không giả thành healthy hoặc list rỗng.
6. ready 503 vẫn cho xem snapshots GET còn hoạt động. Khi ready/busy cho biết
   không thể mutation, disable hợp lý; backend vẫn có thể trả 409/503 do race.

**Gate:** UI đối chiếu GET list/detail/chunks/nodes/cluster thật; paging/filter,
refresh deep link, file rỗng và 404. Snapshot shapes/status hiếm có thể dùng
response kiểm soát để kiểm tra rendering, ghi rõ không là failure evidence.
Targeted lint; build nếu mới thêm route/import. Không cần Docker cho lint/build.

### P3 — Upload và download end-to-end

1. Một field multipart `file`, một file/request; không gửi RF/chunk size hay
   ép Content-Type làm mất browser boundary. Kiểm size theo cluster config
   khi có; backend vẫn quyết định actual-byte limit. Cho phép file 0 byte.
2. Upload states: chọn → gửi body (progress nếu có total) → chờ lưu bản sao
   → thành công **chỉ sau 201**. 100% gửi HTTP chỉ chuyển nhãn “Đang lưu các
   bản sao”; không toast success/refetch giả trước response.
3. Sau 201 invalidate list/cluster, mở detail theo ID được trả. Lỗi có
   details.file_id giữ link tra trạng thái; upload timeout/network outcome
   chưa biết thì refresh list trước khi người dùng chọn thử lại.
4. Download dùng Blob, “Đang chuẩn bị file” trước khi có body; chỉ có transfer
   progress thực thì hiển thị percent. Không tạo % assembly/replication giả.
5. Đọc Content-Disposition UTF-8 filename*, fallback tên từ metadata; lưu qua
   object URL rồi revoke phù hợp. Lỗi JSON dạng Blob phải qua adapter, không
   save error JSON/HTML thành file. Đứt body không báo hoàn tất.
6. Giữ action state độc lập Query, disable thao tác cạnh tranh trong cùng UI;
   hiển thị OPERATION_BUSY từ request khác. Không tự retry POST/cancel-and-retry.

**Gate:** browser upload → list → detail → download một file nhiều chunk
10 MiB + 17 byte và một file rỗng; SHA-256 bytes download bằng nguồn (hash bằng
tool/browser phù hợp và ghi evidence). Kiểm upload đang chờ response dù body
100%, tên Unicode, giới hạn vượt size và download error không lưu Blob lỗi.
Response delay có kiểm soát chỉ chứng minh UI state, không là backend fault gate.
Targeted lint; chỉ build lại nếu code đã đổi từ lần passing trước.

### P4 — Delete và pending cleanup

1. Dialog xác nhận tên file; AVAILABLE/FAILED có thể delete. UPLOADING/DELETING
   không hiện action gây hiểu lầm; backend errors vẫn là nguồn quyết định.
2. 202 hiển thị “Đã xóa file, đang dọn các bản sao còn lại”, pending count;
   invalidate list/detail/chunks/cluster. List mặc định ẩn file, inactive view
   và detail tiếp tục theo dõi DELETING. Không coi 202 là physical cleanup done.
3. Poll bằng GET trong DELETING; terminal read 404 dừng polling/đóng detail
   hoặc chuyển về list. Phân biệt 404 terminal sau delete với network/503.
   DELETE 200/DELETED/pending=0 là xác nhận dọn xong; không lặp DELETE tự động.
4. FAILED vẫn hiển thị error_code dù worker đã cleanup bytes. Đừng xóa lỗi
   chỉ vì cleanup_pending_replicas về 0.
5. Timeout delete giữ ID và reconcile GET; idempotency không biến mutation
   thành polling. Read 404 tổng quát không tự chứng minh mọi bytes đã mất.

**Gate:** delete file nhiều chunk có thể 202 cả khi nodes online; pending tới
terminal, inactive/detail/cache cập nhật và dừng poll. Offline pending/recovery
thật nằm P6. Targeted lint; backend suite chỉ cần khi sửa runtime backend.

### P5 — Manual repair theo lượt

1. Trang chính cho full scan/node filter; detail khóa file filter theo route,
   optional node filter. max_chunks integer 1..8. Registry disabled vẫn là
   filter hợp lệ, hiển thị disabled; server không RPC tới node đó.
2. Một click = một POST; giữ bộ filters/max_chunks trong lượt scan. Lưu
   after/next_after và summaries nhận được. Đổi scope/reset bắt đầu scan mới
   từ after=null; không mang cursor cũ sang file/node khác.
3. Summary checked_chunks/repaired_replicas/remaining_chunks, outcomes từng
   chunk và domain_degraded. repaired chỉ là Store ack, không là số chunks
   HEALTHY hay số probe OK. remaining là chưa quét, không là thiếu replica.
4. Hoàn tất scan khi remaining_chunks=0; không suy ra mọi chunk đã healthy.
   ERROR/UNAVAILABLE/NO_DESTINATION vẫn hiển thị cảnh báo trong HTTP 200.
   Chunk lỗi đã qua cursor; bấm scan mới để retry, không tự lùi cursor.
5. checked=0/remaining>0 giữ cursor response (có thể null), báo chưa tiến
   triển và để nút “Tiếp tục”; không hot-loop/auto POST. Không có job ID,
   background repair hoặc percent hoàn thành bền vững qua reload.
6. Timeout/disconnect: không auto retry hay advance cursor phỏng đoán;
   refresh placement/cluster trước, yêu cầu người dùng bắt đầu scan mới sau
   reconcile. Unmount không coi request server đã rollback.
7. Invalidate list/detail/chunks/cluster sau response hoặc outcome chưa rõ;
   stale response của scope cũ không được nhập vào summary scope mới.

**Gate:** real scoped scan max_chunks=2 qua nhiều cursor pages; end remaining=0,
filters/cursor và summary bằng responses. Kiểm zero-progress/null cursor,
partial outcome và timeout không retry bằng response/network kiểm soát, ghi
rõ nguồn. RF repair/node recovery thật thuộc P6. Targeted lint cho affected code.

### P6 — UI/API runtime và failure flows

Trước chạy, read-only preflight đúng dependencies: frontend/browser accessible,
Docker Linux, PostgreSQL/Metadata và nodes cần cho selected flow. Đối chiếu live
build với source review M4. Chưa sẵn sàng thì dừng flow phụ thuộc; không retry
aggregate hoặc tự start/restart infrastructure cho task chỉ kiểm tra.

Khi bước vào implementation/gate có setup/deployment scope, dùng lệnh existing
README/M4 để cập nhật services cần thiết, giữ named volumes và một Metadata
worker. Không làm deployment trong lượt lập plan. Lint/build không cần preflight
Docker. Không chạy fault script M4 chỉ để đánh dấu UI đã qua.

| Flow chọn theo phần evidence còn thiếu | Bằng chứng cần ghi |
|---|---|
| Happy path sau tích hợp mới | UI state + REST responses + SHA nguồn/download; tái dùng gate P3 nếu còn hợp lệ |
| Node DOWN → fallback download | UI detector/cached placement, bytes/hash thật; không suy ra từ lint |
| Repair khi node DOWN → node recovery | UI cursor pages/outcomes/ack count; RF restored, returned mapping/over-replicated sau scoped scan |
| DELETE khi node offline → recovery | UI 202/pending/inactive, không download/repair; node trở lại, terminal read 404, poll dừng |
| Request cạnh tranh từ tab thứ hai | 409 OPERATION_BUSY hiển thị đúng; không tự retry POST |
| API/network lỗi hoặc timeout | Thông báo/outcome chưa biết, stale data rõ; refresh trước user retry |
| Zero-progress repair | checked=0/remaining>0/null cursor không bị coi done và không hot-loop; ghi rõ nếu mô phỏng response |
| Public routes và cleanup client | Không auth calls, deep link refresh; không leak interval/object URL hoặc update scope cũ |

Chỉ stop/start node được chọn trong scope fault integration đã cho phép; lưu
trạng thái đầu và restore trong finally. Không restart DB/Metadata để lặp lại
M4 lifecycle nếu UI changes không đòi regression đó. Local `dev_host` không
chứng minh failure-domain giữa hai máy.

Fixtures mới tên `m5-<run-id>-...`; ghi IDs, nguồn/hash, build, commands và responses
trong `.runtime/` khi chạy. Chỉ delete fixtures owned qua API; tiếp tục pending
cleanup khi nodes trở lại, không sửa DB/raw-delete chunk/reset volumes. Không
dùng M3/M4 fixtures đã DELETED, không đụng file AVAILABLE ngoài ownership.

**Gate:** mọi required flow có evidence phù hợp; phân biệt lỗi assertion/code
với dependency failure, response mô phỏng với backend thật. Sửa code failure
và rerun riêng affected flow. Nếu tooling không quan sát được bytes download
hoặc state cần thiết, ghi blocked/unverified thay vì tự chốt pass.

### P7 — DoD và bàn giao M6

Đối chiếu source/API/UI/evidence; cập nhật README/index/plan tổng và tài liệu
chạy frontend với env/API origin. Ghi phases hoàn thành, warnings/blockers,
build đang live, fixtures/cleanup pending và checks chủ động không chạy.
Không đổi runtime ở P7 thì tier no execution, không rerun checks đã pass.
Không tạo `M6_IMPLEMENTATION_PLAN.md` trong P7.

Làm tuần tự **P7.1 → P7.2 → P7.3 → P7.4**:

| Sub-phase | Đầu ra | Gate |
|---|---|---|
| P7.1 Inventory DoD | Bảng đối chiếu điều kiện ↔ source ↔ evidence; phân biệt static/controlled/live/fault local / không hai host | Mọi checkbox DoD P1–P6 có dòng đối chiếu; fixtures/live build ghi rõ |
| P7.2 Bàn giao M6 | Section handoff: UI đã có, semantics giữ, local limits, việc M6, setup UI | M6 đọc một section biết điểm xuất phát; không suy ra hai host đã pass |
| P7.3 Sync nav docs | README repo, docs index, plan tổng, frontend README, header/DoD plan M5 | Không còn “P6/P7 chưa triển khai” mâu thuẫn evidence |
| P7.4 Evidence và chốt | Evidence P7 docs-only; `git diff --check`; checkbox P7 | M5 hoàn thành; tiếp theo M6 (chưa lập phase plan M6) |

**Cảnh báo download (agent / browser automation):** `saveBlob` đã gọi
`<a download>` + click; Cursor/IAB thường không tự ghi vào thư mục Downloads
của user hoặc cần hộp thoại chấp nhận. Nếu gate/bàn giao cần file trên đĩa
hoặc dialog download hiện ra: **dừng ngay, kêu user ấn/chấp nhận**, rồi tiếp
tục quan sát. Không coi đây là bug app; không cố bypass bằng CDP download API,
đổi `saveBlob`, tắt dialog, hay “sửa” tooling chỉ để khỏi nhờ user. SHA từ Blob
HTTP (như P3/P6) vẫn là evidence bytes nhận được khi disk-save chưa quan sát
được — ghi rõ nguồn bằng chứng, không suy ra file đã nằm trong Downloads.

M6 nhận frontend đã dùng API thật, local flows và giới hạn rõ; việc còn lại là
Compose A/B, LAN URL/CORS, ports, builds giống nhau, hai failure domains thật
và rehearsal. Không suy ra hai host đã pass từ M5 local UI gate.
Chi tiết đối chiếu và bàn giao: [mục 13](#13-bàn-giao-m6--hai-máy-và-rehearsal).

## 4. Chính sách kiểm tra và lệnh dự kiến

Theo [AGENTS](../AGENTS.md): trước mỗi execution nêu tier, **exact command** và
lý do; sau đó ghi pass/fail/skipped, môi trường, intentional omissions.

- **Lượt lập plan/P7 docs-only:** no execution; `git diff --check` là kiểm tra
  tài liệu, không runtime evidence.
- **Logic/component/client hẹp:** targeted check, từ `frontend/` chạy
  `npm exec -- eslint` kèm đúng affected paths có thật. Không lint cả starter
  mỗi phase. `npm run build` khi cần routing/import/bundle evidence.
- **UI/API wiring:** focused integration bằng available browser tooling với
  selected flow; ghi thao tác/requests/observations thực tế. Package hiện
  không có dedicated test/E2E script, không invent `npm test` hoặc thêm runner.
- **Full validation:** chỉ khi user yêu cầu hoặc change cross-cutting/build/
  dependency đòi; backend unchanged không tự động chạy full backend suite.

Read-only preflight từ repo root khi flow cần Compose:

```powershell
docker info
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
curl.exe --fail-with-body http://localhost:8000/api/v1/health/ready
```

API URL dùng port thực trong deploy env; flow snapshots có thể vẫn kiểm riêng
khi readiness 503, nhưng flow dữ liệu đòi ready và nodes phù hợp. Nếu prerequisite
fail, báo dependency/lệnh setup hiện có từ repo root:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait
```

Nếu env/build chưa có, dùng setup `up -d --build --wait` trong README. Không
tự chạy recovery trong task test-only; tiếp tục independent work, chờ readiness
được xác nhận rồi rerun riêng command intended một lần.

Frontend dev sau khi cấu hình env đúng, từ `frontend/`:

```powershell
npm ci
npm run dev -- --host localhost --port 5173 --strictPort
```

Chỉ `npm ci` khi chưa cài deps hoặc lock thay đổi. Không đổi dependencies để
làm plan; không chạy commands trên ở lượt docs-only này.

## 5. Definition of Done M5

- [x] P1 public routes dùng client V1, không auth/token/credentials/getMe.
- [x] P2 list/paging/inactive/detail/placement/nodes/cluster/ready đúng DTO;
      null/stale/cache observations được diễn giải đúng.
- [x] P3 upload chỉ success sau 201; download bytes/hash đúng; Blob error
      không được save thành file; network outcome chưa biết không auto POST retry.
- [x] P4 delete 200/202, pending/terminal và FAILED error đúng;
      không polling DELETE hoặc khẳng định cleanup bytes từ read 404 tổng quát.
      Offline cleanup/recovery thật thuộc gate P6.
- [x] P5 manual repair filters/cursor/outcomes/counts đúng, không auto scan;
      zero-progress và timeout không làm mất scope/cursor hoặc báo done sai.
- [x] P6 required browser/API flows có evidence; static/mô phỏng/backend/local
      và hai host được phân biệt; owned fixtures dọn hoặc ghi pending rõ.
- [x] P7 setup/env/evidence/bàn giao M6 khớp source/live build thực tế
      (05/10/2026; docs-only, xem [evidence P7](#14-evidence-p7--05102026)).

## 6. Evidence lập kế hoạch — 04/10/2026

Đã đọc kế hoạch tổng, contracts/kiến trúc/overview, M4 handoff và review evidence,
frontend/backend AGENTS, BASE_B/STRUCTURE, source router/auth/client/Query và
Compose CORS/domain hiện hành. Tạo plan P1–P7 và liên kết từ tài liệu điều hướng.

**Tier: no execution**, chỉ Markdown. Không chạy npm/pytest/Ruff/build/browser
runtime, không preflight/start/deploy/restart hoặc tạo/dọn fixture.
`git diff --check` qua; 101 local Markdown links trong 5 tài liệu đã đối chiếu
với files/headings có thật, không có link gãy. Môi trường kiểm tra tài liệu:
PowerShell/native Python trong backend venv trên Windows; không có runtime
assertion hoặc infrastructure check. Không coi M5 đã triển khai.

## 7. Evidence P1 — 04/10/2026

**Source:** `src/api/storage/` có HTTP wrappers/keys/client riêng, chuẩn hóa URL
V1, không auth/credentials và adapter `StorageApiError` giữ status/code/details,
JSON trong Blob, network/non-JSON/timeout/cancel. Repair/DELETE timeout 180 s;
transfer 900 s là client policy cho cấu hình demo mặc định, không SLA. Mutation
defaults dưới storage keys là retry=false; HTTP wrappers không có retry.

`App`/`Root` không mount/chờ auth init toàn cục; `StarterLayout` giữ bootstrap
và splash riêng cho nhánh login/dashboard/settings. Public `StorageLayout`
có `/` và `/files/:fileId`; protected root index đã bỏ để không cạnh tranh.
ConnectionPanel chỉ đọc readiness/cluster, có refresh/lỗi/stale snapshot; P1
không poll hay nối actions. Detail P1 chỉ có ID/kết nối, chưa xác minh file
tồn tại hoặc hiển thị metadata/placement. Env example/README frontend đã
hướng dẫn URL/port/CORS, `index.html` có title dự án. Không đổi lockfile/deps,
backend runtime/contracts hoặc deploy services.

**Tier:** targeted lint + build cho routing/import và focused browser/API cho
HTTP boundary/public routes. Môi trường Windows, Node **26.5.1**, npm **11.17.0**,
Vite **6.3.5**; browser Codex
IAB origin `http://localhost:5173`; Metadata Docker Linux/PostgreSQL 16. P1
read-only preflight API ready=200, cluster 3 ACTIVE; Docker `ps` có 5 healthy
services. Live Metadata image **`35f6f3639213`** vẫn là build P6, chưa deploy
fixes review M4; P1 chỉ đọc health/cluster và 404, không dùng evidence này để
chứng minh admission fixes hoặc failure flows mới.

Dependency setup `npm ci --cache .npm-cache --no-audit --no-fund` bị ENOTCACHED
do sandbox chỉ dùng cache. Lượt `--prefer-online` được phép ngoài sandbox,
cài 667 packages theo lockfile, không audit/update. Warnings deprecated
lodash.isequal/eslint-define-config và pending install scripts của esbuild/
tailwind oxide; build thực tế dùng binaries đã có và qua, không approve thêm
scripts hoặc thay package versions. Cache workspace được ignore.

Commands từ `frontend/`:

```powershell
npm exec -- eslint src/App.jsx src/app/layouts/Root.jsx src/app/layouts/StarterLayout.jsx src/app/layouts/StorageLayout.jsx src/app/router/router.jsx src/app/router/public.jsx src/app/router/protected.jsx src/app/queryClient.js src/api/storage src/app/pages/storage
npm run build
npm run dev -- --host localhost --port 5173 --strictPort
```

Targeted ESLint **qua, không warnings**. Build bị `spawn EPERM` trong sandbox,
chạy ngoài sandbox qua; Docker preflight cũng cần quyền đọc pipe ngoài sandbox.
Sau đổi title `index.html`, build lại một lần: **qua, 11.07 s**; không rerun
lint/browser checks vì runtime JS không đổi. Lượt build đầu qua trong 27.47 s.
Warnings build ở starter: Table/PreviewImg lint, Accordion circular chunks,
CSS `:is()` rỗng, bundle >500 kB và Node module.register deprecation. Browser
có warning React DevTools shim/Fast Refresh; không có app console error.
Không sửa những phần starter này để mở rộng scope P1.

**Focused browser checks:** harness tạm
`frontend/.runtime/m5-p1-checks.html`, chạy qua Vite/IAB, **19 checks passed**.
GET readiness/cluster thật; XHR URLs giữ một `/api/v1`, Authorization=false/
withCredentials=false dù localStorage có synthetic authToken. GET missing
file/detail và download thật trả 404 FILE_NOT_FOUND qua adapter JSON/Blob;
port local không lắng nghe cho NETWORK_ERROR thật. Public shell render và giữ
synthetic token không bị auth.initialize xóa, không có XHR `/api/auth/`.
Token/XHR adapter/instrumentation được restore sau harness.

Các check **mô phỏng** dùng Axios adapter: HTML 503/malformed Blob không lộ
raw body; error code/details còn; mutation timeout/cancel outcomeUnknown;
Query repair timeout chỉ execute một lần; multipart đúng một field file,
không ép multipart boundary; DELETE giữ 202/pending; repair giữ filter/cursor/
timeout 180 s. Không gửi POST/DELETE tới backend thật, không upload/download
bytes thành công hoặc repair/cleanup runtime ở P1.

**Route regression thật:** `/files/11111111-1111-4111-8111-111111111111` direct
navigation + reload hiện detail ID/kết nối READY, không login/splash vô hạn;
link về `/` hiện trang file READY. `/dashboards/home` vẫn redirect
`/login?redirect=%2Fdashboards%2Fhome`, form username/password hiện đúng.
Đây không là file-detail existence check hoặc authenticated login evidence.
Screenshot public shell: `.runtime/m5-p1-public-shell.jpg`; dev server đang
chạy tại `http://localhost:5173/`. `git diff --check` và local Markdown links
được kiểm tra trước bàn giao; cảnh báo LF→CRLF là conversion của Git trên Windows.

**Chủ động chưa chạy:** full frontend lint, backend pytest/Ruff/full suite,
deploy/restart/fault smoke, upload/delete/repair thật, hai host. Không tạo/
dọn fixture DB hoặc thay named volumes. Không có ứng viên generic shared mới:
ConnectionPanel/hook/error adapter vẫn thuộc storage feature, tái dùng primitives
có sẵn. P1 hoàn thành; tiếp theo P2 list/detail/placement và nodes/cluster.

## 8. Evidence P2 — 04/10/2026

**Implementation đã lưu:** FilesPanel có GET list, limit/offset/include_inactive,
loading/empty/error, reset offset khi đổi filter/limit và về trang hợp lệ khi
total giảm; query keys/AbortSignal tránh áp data scope cũ. FileMetadata và
PlacementPanel đọc detail/chunks theo UUID lowercase, hiển thị FileSummary,
checksum/RF file/counters/error_code và mọi replica history. Invalid ID không
gọi file API; read 404 ẩn stale detail và dừng file polling. Query 404 vẫn có
thể được refresh thủ công để người dùng reconcile.

MetadataStatus/ClusterPanel/NodesPanel đọc readiness/cluster/nodes độc lập;
ready 503 không chặn các GET còn dùng được. Hiển thị busy/counters/domains,
ACTIVE/SUSPECTED/DOWN/disabled, metrics null khác zero, không cộng capacity/
free node. Mỗi snapshot có timestamp; GET lỗi giữ data cũ kèm error/stale label.
`useSnapshotQuery` poll 4 s khi visible, bỏ interval khi hidden/unmounted/404;
GET retry tối đa 1 lần, không retry 400/404/422. Detail không fetch nodes vì
không có node table ở màn hình đó. Không thêm mutation UI hoặc sửa backend.

**Tier/environment:** targeted ESLint + build và focused browser/API cho flow
GET/routing/state. Windows Node 26.5.1/npm 11.17.0/Vite 6.3.5, Codex IAB và
Metadata Docker Linux/PostgreSQL; live build vẫn M4 P6. Commands từ frontend:

```powershell
npm exec -- eslint src/app/pages/storage
npm run build
```

Lint **qua, không warnings**; build **qua 9.99 s**. Warnings giữ từ starter:
Table/PreviewImg lint, Accordion circular chunks, CSS `:is()` rỗng, chunk size
và module.register deprecation. Không rerun P1 checks/full lint/backend suite.

**API/UI thật trước crash:** list/inactive/limit và nodes/cluster đọc được.
File live ngoài ownership `94c8a2d8-d935-4c1a-b127-835ff507c6ef` chỉ GET:
10,485,777 bytes, AVAILABLE, RF=2, 6 chunks, checksum
`c4a9068afee819811d78887024edcabee99f03f85e40e8319078206d2d547321`.
Browser detail hiển thị các fields trên và đúng 6 headings chunks. Có một
locator wait ngắn hết hạn trong lúc SPA navigation/queries đang cập nhật;
state sau xác nhận route đúng, không có app console error hoặc code fix vì
lần wait này. Không download bytes, không mutation fixture hoặc coi observations
này là integrity/failover/replication proof mới.

**Responses kiểm soát, không backend fault:** harness tạm
`frontend/.runtime/m5-p2-checks.html` thay Axios adapter chỉ trong test page.
Tool outputs xác nhận **9 automated checks passed trước crash**: zero-byte list,
null/zero/all node states, cluster busy/domains, visible polling theo key,
simulated hidden document không polling, zero-byte detail không chunk,
replica history đủ VERIFIED/PENDING/DELETED/CORRUPTED/MISSING, known_readable
false là observation, terminal 404 không poll file/chunks trong cửa sổ 4.4 s.
Tên check 404 có “focus retry”; chưa có explicit focus-event assertion riêng,
chỉ ghi nhận polling evidence thực tế.

Các bước browser đã quan sát thêm: page offset=50 hiển thị 51–51/51; bật
inactive reset offset=0 và total=54, trang cuối có UPLOADING/FAILED/DELETING;
shrink total=2 tự về 1–2/2. Ready 503 vẫn giữ metadata file/placement còn đọc
được. Khi snapshots GET trả 503, data trước lỗi giữ nguyên cùng timestamp và
nhãn “Dữ liệu cũ”; recover bỏ lỗi. File 404 thay detail bằng trạng thái terminal.
Các quan sát này là UI với fixture responses mô phỏng, không phải dịch vụ
thật DOWN, cleanup hoặc repair thật.

**Crash và phục hồi:** người dùng báo máy crash trước khi kiểm tra cuối và ghi
evidence xong. Source/harness còn nguyên. Docker tự lên lại 5 services healthy,
API ready=200; dev server 5173 đã dừng. Đã khởi động lại theo yêu cầu tiếp tục:

```powershell
npm run dev -- --host localhost --port 5173 --strictPort
```

Không restart/deploy backend, đổi dependencies hoặc reset volumes. Lint/build
đã pass trên cùng source giữ làm evidence; environment đổi nên runtime checks
còn lại cần chạy trên phiên mới. Read-only API sau crash vẫn trả AVAILABLE/
6 chunks/12 replica mappings và checksum trên; đây chỉ là metadata snapshot.

**Gate sau phục hồi đã qua:** unmount check trước crash chưa quan sát completion,
không tính pass. Ban đầu browser từ chối nối tab HTTP với lỗi protocol; không
đổi URL/browser hoặc dùng CDP vượt chặn. Sau khi người dùng mở lại tab và xác
nhận trang tải được, kết nối bình thường thành công. Live detail reload hiện
đúng 6 chunks; `/files/not-a-uuid` hiện alert ID không hợp lệ; UUID không tồn
tại trả trạng thái 404 terminal qua API thật. Không dùng quan sát alert làm
network assertion cho invalid ID; guard không gọi file API được đối chiếu source.
Harness chạy riêng check unmount **passed**: không thêm request trong 4.4 s sau
unmount. Tổng cộng **10 automated checks mô phỏng được quan sát passed**, cùng
các bước UI/API thật và manual state checks trên. Không rerun 9 checks đã qua.

Visual check trong sidebar 338 px phát hiện grid starter gây page overflow.
StorageLayout thêm `min-w-0` và header `flex-wrap`; reload xác nhận list và
detail có document scrollWidth=323 px, main=323.2 px (viewport trừ scrollbar),
bảng cuộn trong panel. Đây là chỉnh static styling: chỉ visual check, không
rerun lint/build đã qua vì không đổi logic/import. Screenshot list tại
`.runtime/m5-p2-list.jpg`; tổng hợp evidence tại `.runtime/m5-p2-evidence.json`.
Dev server giữ chạy tại `http://localhost:5173/`. **P2 hoàn thành; tiếp theo P3.**

**Chủ động chưa chạy:** full frontend lint/backend tests, live upload/download/
delete/repair, fault smoke, hai host. Không tạo/dọn fixture thật. Helpers byte/
timestamp đang colocate; có thể đề xuất promote sang `src/utils/` nếu tái dùng
ngoài storage, chưa extract shared mới.

## 9. Evidence P3 — 04/10/2026

**Implementation:** UploadPanel trên list, DownloadPanel trên detail;
StorageTransferProvider giữ state/ref guard qua điều hướng hai màn hình, độc
lập snapshot Query và không tự retry transfer. POST multipart đúng một field
file, không ép boundary/RF/chunk size; kiểm file.size theo snapshot cluster,
cho phép 0 byte. Upload wrapper giữ httpStatus; 100% body chỉ chuyển “Đang lưu
các bản sao”, chỉ 201 + AVAILABLE + UUID hợp lệ mới success/invalidate files/
cluster/nodes và mở detail. Không invalidate success giả trước response.

Lỗi giữ code/details.file_id. Timeout/network outcome chưa rõ hoặc success HTTP
ngoài hợp đồng khóa upload/download cạnh tranh; GET list include_inactive để
reconcile trước user retry. GET lỗi giữ khóa; GET thành công bật inactive,
nhắc kiểm tên/thời gian và không tự POST lại. Danh sách không thể khẳng định
request cũ đã kết thúc vì V1 không có upload idempotency key.

Download chuẩn bị trước body, percent chỉ từ loaded/total thực. Chỉ AVAILABLE,
không cấm download khi known_readable=false. HTTP 200/Blob/size đầy đủ và
không JSON/HTML mới được save; Blob error qua adapter P1. Filename* UTF-8 ưu
tiên, rồi filename thường/metadata; strip path/control và Windows reserved
names. Object URL revoke sau 30 s, cả khi điều hướng. UI báo chuyển file cho
trình duyệt lưu, không khẳng định file đã ghi trên đĩa.

**Tier/environment:** targeted ESLint + build và focused UI/API. Windows Node
26.5.1/npm 11.17.0/Vite 6.3.5, Codex IAB; Docker Linux, Metadata/PostgreSQL và
3 Storage nodes healthy/ACTIVE, cùng domain dev_host. Read-only preflight
docker info/Compose ps cần quyền đọc pipe ngoài sandbox; API ready=200,
cluster operation_busy=false/max size=64 MiB, frontend=200. Không restart,
deploy hay reset services/volumes; live Metadata vẫn M4 P6.

Commands từ frontend:

```powershell
npm exec -- eslint src/api/storage/index.js src/app/layouts/StorageLayout.jsx src/app/pages/storage
npm exec -- eslint src/app/pages/storage/utils/transfers.js
npm run build
```

Lint lượt đầu có đúng một no-control-regex error ở transfers.js; các file khác
qua, không warnings. Sửa sanitize bằng Unicode property Cc, rerun riêng helper
**qua không warnings**. Build đầu thất bại cùng lint error, rerun sau fix
**qua 17.64 s**; warnings starter Table/PreviewImg, Accordion circular chunks,
CSS empty :is(), chunk >500 kB và Node module.register deprecation giữ nguyên.
Không đổi dependencies/test framework. Lint/build không là runtime evidence.

**Live UI/API gate:** fixture tạo riêng, không chạm fixture cũ ngoài ownership.
Fixture đầu chọn qua file chooser có 10 MiB + 18 byte do phần đuôi generator;
upload 201/list/detail thành công nhưng không tính làm gate exact-size. Giữ
ID `756d6315-f07c-46f1-ae53-852bb6af483b` để P4 dọn cùng fixtures của lượt này.
Sửa nguồn thành đúng 17 byte, hash source bằng Python. Picker có một timeout
và mất page selection trong tab đang mở; chuyển tab riêng/harness tạo cùng
bytes trong browser, đặt vào input file qua change event. **App và Axios
adapter thật giữ nguyên**, Tải lên/Tải xuống do browser click; không mock API.

| Fixture | File ID | Bytes/chunks | SHA-256 nguồn = Blob download thật = metadata |
|---|---|---|---|
| m5-p3-thu-nghiem-Đặng-数据.bin | 79548558-9634-4201-8b8b-c8227c4d93b8 | 10,485,777 / 6 (RF=2, 12 mappings) | 8da665b1fe4a896c6b4e12c76c53fd03f0e6d990a447efe47d7598659d09120e |
| m5-p3-empty.bin | 807c0c11-db27-410d-aac8-218b329a8551 | 0 / 0 (không replica) | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 |

Cả hai upload báo 201, xuất hiện trong list, detail AVAILABLE/chunks đúng,
download gọi API thật. Instrument URL.createObjectURL đọc **Blob nhận thật**
bằng crypto.subtle SHA-256; Python đối chiếu nguồn tại host, report browser và
GET metadata/chunks **qua**. Header filename* trả tên Unicode đúng, không mất
đuôi; cả hai object URLs đã revoke. IAB waitForEvent(download) hết hạn 20 s
cho file nhiều chunk dù Blob đã nhận/save dispatch; không dùng lần wait này
làm disk-save proof. Live report posts/downloads=0 là counters của mock adapter
không bật ở live mode, không có nghĩa không gửi request.

**Controlled responses:** harness `frontend/.runtime/m5-p3-checks.html` không
gọi backend ở chế độ mặc định, dùng App/hooks/components thật và adapter mô
phỏng; anchor click chỉ ghi tên, không tạo downloads thật trong mode này.
**18 automated checks passed**: 5 filename/progress helper cases, oversized
64 MiB+1 không POST, body 100% vẫn chờ response, multipart một file/POST một
lần khi pending, JSON Blob error, HTML proxy error không lộ raw body, body
đứt/thiếu không tạo object URL, preparing không % giả, receiving 1/3 có 33%,
Blob success dùng UTF-8 header filename, OPERATION_BUSY giữ code/link ID,
upload timeout không retry và khóa lựa chọn, object URL revoke sau grace.

Các quan sát manual thêm: điều hướng khi upload pending vẫn khóa download;
download loaded không total bỏ progress; reconcile GET 503 giữ khóa lựa chọn,
GET recover mở lựa chọn và checkbox inactive=true; upload 202 hiện
UNEXPECTED_UPLOAD_RESPONSE, không success và yêu cầu reconcile. Đây là UI
delay/error evidence có kiểm soát, không là backend fault/failover gate.

Artifacts ignored tại `.runtime/m5-p3/`: sources.json, controlled-report.json,
live-report.json, evidence.json và hai nguồn bytes; screenshot UI thật
`.runtime/m5-p3-detail.jpg`. Các tab harness đã đóng, tab frontend thật giữ mở;
sidebar 371 px có scrollWidth=356 px (trừ scrollbar), không page overflow.

**Chủ động chưa chạy:** full frontend lint/backend tests, delete/repair UI,
deploy/restart/fault smoke, hai host. Không DELETE các fixture tại P3; cả ba
ID do lượt này tạo đã ghi ownership để P4 tái dùng. Fixture cũ
94c8a2d8-d935-4c1a-b127-835ff507c6ef giữ nguyên. Helpers filename/progress còn
colocate; có thể promote sang src/utils khi có domain thứ hai cần, chưa tạo
shared abstraction. Docs-only update không chạy runtime checks; git diff
whitespace được kiểm tra khi bàn giao. **P3 hoàn thành; tiếp theo P4.**

## 10. Evidence P4 — 05/10/2026

**Implementation:** DeletePanel chỉ hiện action cho AVAILABLE/FAILED. Dialog
Headless UI hiển thị tên Unicode và ID; focus Hủy, confirm kiểm lại trạng
thái/kết nối và khóa cạnh tranh bằng ref dùng chung upload/download/delete.
Không dùng shared ConfirmModal của starter vì default copy/Retry không phù
hợp hợp đồng cleanup; ghép từ Dialog/Button có sẵn, giữ UI trong feature.

DeleteResult kiểm identity, HTTP status, status và pending integer không âm:
200/DELETED/pending=0 mới xác nhận đã dọn xong; 202/DELETING giữ pending,
không coi pending=0 riêng lẻ là hoàn tất. Response khác yêu cầu GET reconcile.
Sau response invalidate list/detail/chunks/cluster/nodes, 202 cập nhật cache
detail thành tombstone để khóa download ngay. FAILED error_code giữ nguyên
khi cleanup pending=0. Không tự retry DELETE dù API idempotent.

Provider theo dõi GET detail 4 s trong cleanup, cả khi quay về list. Detail
nhường interval cùng key cho provider để không tạo hai pollers; chunks vẫn
GET riêng. GET 503 giữ pending/stale timestamp và polling có giới hạn retry,
không biến lỗi mạng thành terminal. Read 404 dừng file/chunk polling và hiển
thị kết thúc theo dõi, không khẳng định từng replica đã xóa trên đĩa. 200
DELETE giữ acknowledgment riêng và ẩn stale metadata/action ngay.

Timeout/network hoặc DELETE >=500 giữ ID/khóa mutation, nút Đọc lại trạng
thái xóa chỉ GET detail: AVAILABLE/FAILED cho phép xác nhận mới, DELETING tiếp
tục theo dõi, 404 kết thúc trung tính. GET reconcile lỗi giữ khóa. Notification
đặt ngoài upload/download panels để trạng thái xóa có vùng riêng trên cả routes.

**Tier/environment:** targeted lint/build + focused UI/API. Windows Node
26.5.1/npm 11.17.0/Vite 6.3.5, Codex IAB; read-only Docker Linux/Compose
preflight xác nhận 5 services healthy, 3 nodes ACTIVE cùng dev_host, Metadata
ready=200/lock trống. Live Metadata vẫn M4 P6. Không deploy/restart/reset data.
Commands từ frontend:

```powershell
npm exec -- eslint src/app/pages/storage
npm run build
```

Lint **qua không warnings**, build **qua 15.53 s**. Warnings starter
Table/PreviewImg, Accordion circular chunks, CSS :is() rỗng, chunk >500 kB và
Node module.register giữ nguyên. Không đổi dependencies/framework. Không rerun
full frontend lint/backend suite. Vì controller/notice dùng chung đã đổi,
chạy **3 selected P3 controlled regressions passed**: upload body 100% chờ
201, đúng một multipart file/POST khi pending, download success filename UTF-8.
Upload 201 mở detail vẫn được quan sát; không tạo thêm fixture thật để hồi quy.

**Controlled gate:** harness `frontend/.runtime/m5-p4-checks.html` dùng
App/components/hooks thật nhưng Axios adapter mô phỏng, không backend mutation.
**14 automated checks passed**: action states, validation 200/202/ID/status/
pending (5 helper checks), FAILED giữ mã upload error khi pending=0, cancel
không DELETE, timeout giữ ID/không retry, 202 giữ pending và không báo dọn xong,
DELETING ẩn delete/khóa download, GET 503 giữ stale pending, 404 dừng poll và
không lặp DELETE, 404 không physical cleanup claim, 200/DELETED/zero xác nhận.

Các bước manual đã quan sát: dialog đúng tên/ID và focus Hủy; UPLOADING/
DELETING không action; reconcile GET 503 giữ khóa rồi GET AVAILABLE cho phép
confirm mới; OPERATION_BUSY giữ code; 200/pending=1 bị từ chối và yêu cầu đọc
lại; DELETE pending khóa upload/download cả khi đổi route; pending 2→1 theo
GET; default list ẩn file, inactive hiển thị DELETING. Controlled 503/timeout
không phải fault thật hoặc offline recovery evidence; phần đó vẫn ở P6.

**Live gate sau xác nhận user:** Browser yêu cầu xác nhận tại thời điểm xóa
không có undo. Sau khi code/lint/build/controlled gate đã review được, user
xác nhận xóa cả ba ID fixture P3. Harness `frontend/.runtime/m5-p4-live.html`
giữ adapter/backend thật, chỉ log requests/responses và guard DELETE theo ba
ID owned. Click dialog/confirm qua Browser UI; **đúng một DELETE mỗi ID**.

| Fixture P3 | DELETE thật | Quan sát GET/UI sau đó |
|---|---|---|
| 79548558-9634-4201-8b8b-c8227c4d93b8, 10 MiB + 17 byte | 202/DELETING/pending=4 | GET DELETING/4 rồi 404, trạng thái kết thúc, file/chunk request count dừng |
| 756d6315-f07c-46f1-ae53-852bb6af483b, 10 MiB + 18 byte | 202/DELETING/pending=4 | Inactive UI hiện DELETING khi còn pending; provider GET đến 404 cả trên list |
| 807c0c11-db27-410d-aac8-218b329a8551, file rỗng | 200/DELETED/pending=0 | UI xác nhận đã dọn xong, metadata/action cũ ẩn, GET 404 và polling dừng |

File nhiều chunk 12 mappings trả 202 với 4 pending đúng cleanup budget 8, dù
nodes online. Chỉ response DELETE 200 của file rỗng được tính acknowledgment
physical cleanup; 404 của hai file khác là read terminal, không kiểm tra riêng
disk bytes. Request log có GET REQUEST_CANCELLED khi StrictMode/đổi route:
AbortSignal lifecycle dự kiến, không phải assertion/environment failure.

Postcondition read-only audit **qua**: include_inactive list còn 1 file,
không có ba owned IDs; files_available=1, cluster cleanup_pending=0. Fixture
cũ 94c8a2d8-d935-4c1a-b127-835ff507c6ef vẫn AVAILABLE/10,485,777 bytes/hash
c4a9068afee819811d78887024edcabee99f03f85e40e8319078206d2d547321. Không DELETE
ngoài ownership, xóa volume hoặc reset DB. Ba fixture P3 đã dọn bằng workflow
API, không còn fixture owned pending của P3/P4.

Artifacts ignored `.runtime/m5-p4/`: controlled-report.json, live-report.json,
p3-regression-report.json, evidence.json; screenshots `.runtime/m5-p4-confirm.jpg`
và `.runtime/m5-p4-complete.jpg`. Browser thể hiện layout desktop 1280 px;
dialog/notification đã kiểm tra trực quan. Harness tabs đóng khi bàn giao,
frontend thật giữ mở tại `http://localhost:5173/`.

**Chủ động chưa chạy:** full frontend lint/backend tests, deploy/restart,
live offline/recovery/fault smoke hoặc hai host. Không có generic shared mới
cần promote ở P4: dialog/notice/helpers biết contract/storage state; primitives
Dialog/Button được tái dùng. Docs-only updates không chạy runtime checks,
git diff whitespace kiểm tra trước bàn giao. **P4 hoàn thành; tiếp theo P5.**

## 11. Evidence P5 — 05/10/2026

**P5 hoàn thành.** Root có full scan/node filter; detail cố định file theo route
và đọc registry để chọn node. DISABLED vẫn là filter hợp lệ. Max chunks integer
1..8; client không tự chạy các lượt tiếp theo. Repair dùng khóa thao tác chung
với upload/download/delete, giữ request/cursor và response history trong provider.

Scope gồm filters và phiên form; đổi filter, kể cả đổi lại filter trước, bỏ cursor
cũ. Route mới không nhập summary của request đang chạy ở route cũ. Chuyển trang
không hủy POST hoặc suy ra server đã rollback. Mỗi lượt giữ summary riêng và tổng
checked/Store ack; chỉ remaining=0 là quét hết scope. HTTP 200 có partial outcomes
vẫn cảnh báo, domain_degraded của HEALTHY vẫn hiện. Zero-progress giữ nguyên cursor
response, kể cả null; tiếp tục hoàn toàn thủ công.

Timeout/network/5xx hoặc DTO sai scope/count/outcome khóa mutation, không đoán
cursor/auto retry. Reconcile dùng GET cluster và file/chunks cho scoped scan,
hoặc cluster/list cho full scan, retry=false; lỗi giữ khóa. Sau snapshot thành
công chỉ cho scan mới after=null. Invalidate files (bao gồm detail/chunks), cluster
và nodes sau response/outcome chưa rõ. Upload reconcile vẫn bật inactive filter;
repair reconcile không tự đổi filter danh sách.

**Tier/commands:** targeted `npm exec -- eslint src/app/pages/storage` passed,
không warning/error; `npm run build` passed (40,73 s). Node 26.5.1/npm 11.17.0,
Vite 6.3.5 trên Windows. Build còn warnings đã biết của starter: Table fast
refresh, PreviewImg unused disable, Accordion circular chunks, CSS empty `:is()`,
chunk >500 kB và Node module.register deprecation. Không gọi lint/build là runtime.

**Read-only preflight:** Docker 29.3.1, Compose Metadata/PostgreSQL/3 Storage
healthy; GET ready=READY và frontend HTTP 200. Không restart/deploy/migrate/reset.
Live backend vẫn là build M4 P6; các fixes review M4 chưa deploy như handoff đã
ghi. Ba nodes ACTIVE, cùng dev_host (1 configured/active domain); đây là local
Docker Linux/PostgreSQL qua browser localhost, không có evidence hai host.

**Controlled Browser evidence:** chạy React App thật với Axios adapter mô phỏng
trong harness ignored `frontend/.runtime/m5-p5-checks.html`, không gọi backend:

- 18 assertion khác nhau cuối cùng passed: summaries bằng DTO; body chỉ bốn
  contract fields; ba cursor pages giữ scope/max_chunks; remaining=0 và totals;
  zero-progress/null cursor không auto POST; filter reset; partial warnings/
  domain degraded/Store ack; khóa cạnh tranh; response route cũ không ghép summary;
  timeout không retry; GET lỗi giữ khóa/reconcile thành công; 409 OPERATION_BUSY;
  wrong-scope DTO bị từ chối; validation count/outcome/cursor identity.
- Một assertion GET lỗi đọc UI khi GET còn chạy thất bại lần đầu. Kiểm lại đúng
  trạng thái sau phản hồi passed; raw report giữ cả hai attempts. Không sửa code
  hoặc chạy lại aggregate vì timing của harness.
- Các bước DOM trực tiếp thêm: max_chunks=1.5 khóa submit; root request
  file_id=null/node-disabled/max_chunks=2/after=null; đổi lại filter cũ không
  phục hồi cursor; zero-progress sau cursor non-null giữ đúng response cursor.
- Hồi quy tập trung controller chung: 3 P3 assertions passed (100% body chờ ack,
  single multipart/khóa pending, unknown upload không retry), navigation sau 201
  và upload reconcile bật inactive đã đọc trên DOM. 3 P4 assertions passed
  (cancel không DELETE, 202 giữ pending, DELETING khóa download/ẩn delete);
  thêm repair disabled trong DELETING và notice read 404 trung tính. Không gọi
  đây là chạy lại toàn bộ gate P3/P4. Không DELETE thật trong hồi quy này.

**Live gate:** harness ignored `frontend/.runtime/m5-p5-live.html` giữ real Axios
adapter, chỉ instrument request/response. Tạo bytes fixture bằng test control
để tránh file chooser lớn; upload vẫn đi qua form/controller/multipart thật.
Guard chặn mọi DELETE và repair ngoài fixture vừa tạo.

- Fixture owned P5: `96729595-61ba-47ad-8441-b4e7b711d0ee`, tên
  `m5-p5-repair-Đặng-数据.bin`; upload 201/AVAILABLE, 10 MiB + 17 byte,
  6 chunks, RF=2. SHA-256 response khớp nguồn pattern 0..255 lặp 10 MiB và
  tail ASCII `M5-P3-end-to-end!`:
  `8da665b1fe4a896c6b4e12c76c53fd03f0e6d990a447efe47d7598659d09120e`.
- UI scoped file này, node=null, max_chunks=2; đúng ba click/ba POST 200:

| Lượt | after gửi | checked | repaired | remaining | next_after |
|---|---|---:|---:|---:|---|
| 1 | null | 2 | 0 | 4 | fixture / chunk 1 |
| 2 | fixture / chunk 1 | 2 | 0 | 2 | fixture / chunk 3 |
| 3 | fixture / chunk 3 | 2 | 0 | 0 | null |

10 live assertions passed: summary/filter/cursor/outcomes của từng lượt bằng
responses và scan cuối total checked=6, không còn continuation. Sáu chunk đều
HEALTHY/live_replica_count=2/domain_degraded=false; repaired=0 đúng vì không
Store thêm replica. Không coi kết quả này là RF recovery hoặc node failover.
Không repair file cũ `94c8a2d8-d935-4c1a-b127-835ff507c6ef` ngoài ownership.

**Fixture pending cho P6:** giữ nguyên fixture P5 trên để thử failure flows;
chưa xóa, không dùng lại quyền xóa ba fixture P3 cho ID mới. Không xóa volumes
hoặc reset DB. Bàn giao frontend thật tại detail fixture P5, dev server giữ chạy.
Harness tabs đã đóng. Khi cập nhật frontend README, Vite reload các harness về
URL App hiện hành; reports và screenshot đã lưu trước reload.

Artifacts ignored `.runtime/m5-p5/`: controlled-report.json, live-report.json,
p3-regression-report.json, p4-regression-report.json, evidence.json; screenshot
`.runtime/m5-p5-complete.jpg` thể hiện remaining=0/tổng 6/HEALTHY trong App.

**Chủ động chưa chạy:** full frontend lint/backend tests, live fault/offline/
recovery/restart, deploy review fixes M4, hai host; không cần các checks đó cho
scope P5. Docs updates không chạy runtime checks; git diff whitespace kiểm tra
trước bàn giao. Không có generic mới cần promote: RepairPanel/helpers biết DTO,
scope và API storage; giữ feature-local, tái dùng Button hiện có.
**Tiếp theo P6 UI/API runtime và failure flows.**

## 12. Evidence P6 — 05/10/2026

**P6 hoàn thành.** Gate UI/API runtime trên local Docker Linux/PostgreSQL +
browser Vite `localhost:5173`. Không sửa runtime frontend/backend trong phase
này; chỉ fault orchestration, harness tạm và evidence. Live Metadata vẫn image
`35f6f3639213` (M4 P6); ba node cùng `dev_host` — process failure only, **không**
chứng minh hai host/failure domain (M6).

**Tier:** focused integration/E2E (browser + REST + Compose stop/start
`storage-node-2`). Không lint/build vì không đổi source JS/CSS. Không chạy
`smoke_failure.ps1` aggregate; không full frontend lint/backend suite; không
deploy/rebuild Metadata; không reset volumes/DB.

**Read-only preflight:** Docker 29.3.1; Compose postgres/metadata/3 storage
healthy; `GET /health/ready` → READY; fixture P5
`96729595-61ba-47ad-8441-b4e7b711d0ee` còn AVAILABLE/10,485,777 bytes/SHA
`8da665b1fe4a896c6b4e12c76c53fd03f0e6d990a447efe47d7598659d09120e`, 6 chunks,
4 chunks có mapping node-2.

| Flow | Evidence |
|---|---|
| Happy path download | API + UI Blob SHA khớp nguồn; filename UTF-8 Content-Disposition |
| Node DOWN → fallback download | stop `storage-node-2` → detector DOWN; UI thấy DOWN; download SHA khớp; API under_replicated=4 |
| Repair while DOWN → recovery | UI 3 lượt max_chunks=2: repaired Store ack 1+2+1=4, remaining 4→2→0; start node-2 → over_replicated=4, under=0; download SHA vẫn khớp |
| Offline DELETE → recovery | fixture mới `1c283813-2e0f-4e68-8fa1-537bf4c7557c`; DELETE lúc node-2 DOWN → 202/DELETING/pending=1; UI khóa download/repair + “Đã xóa file, đang dọn…”; start node-2 → GET 404, UI terminal, cleanup_pending=0 |
| OPERATION_BUSY | hai POST repair chồng: 200 + 409; UI hiện `OPERATION_BUSY` |
| Zero-progress / timeout | tái dùng evidence controlled P5 (frontend code không đổi); ghi rõ không phải fault mới |
| Public routes / cleanup | không auth XHR; deep link detail; sau cleanup deep link 404; không interval leak quan sát được |

Owned fixtures đã dọn qua API: delete fixture → 404; repair fixture DELETE 202
→ pending=8 → GET 404. Postcondition: `files_available=1` (chỉ file ngoài
ownership `94c8a2d8-…`), cluster cleanup_pending=0, nodes 3 ACTIVE, lock trống.
Không đụng file ngoài ownership; không raw-delete chunk/DB; node-2 restore trong
finally/`ensure-up`.

Artifacts ignored `.runtime/m5-p6/`: `evidence.json`, `orchestrate.py`,
`ui-happy.json`, `ui-fault-repair.json`, `terminal-404.png`. Harness tạm
`m5-p6-live.html` (Vite entry; gitignore frontend). Helper API không phải
M4 smoke script.

**Chủ động chưa chạy / không claim:** full lint/build/backend suite, deploy
review fixes M4, hai host/CORS LAN, automatic repair. Phân biệt: controlled P5
≠ live fault; local `dev_host` ≠ hai failure domain. Download SHA lấy từ Blob
HTTP (automation không tự lưu Downloads); nếu cần disk-save thì dừng nhờ
user ấn — xem cảnh báo ở mục P7. **P6 hoàn thành; P7 DoD/bàn giao M6 đã chốt
05/10/2026** — xem [mục 13](#13-bàn-giao-m6--hai-máy-và-rehearsal) và
[evidence P7](#14-evidence-p7--05102026).

## 13. Bàn giao M6 — hai máy và rehearsal

Kế hoạch tổng: [IMPLEMENTATION_PLAN](IMPLEMENTATION_PLAN.md) mục M6 (Compose A/B,
env LAN, failure-domain demo). P7 không tạo phase plan M6 riêng; evidence hiện
hành nằm trong mục 7–12 và [evidence P7](#14-evidence-p7--05102026).

### DoD M5 đã đối chiếu

| Điều kiện | Implementation | Evidence / loại bằng chứng |
|---|---|---|
| P1 public routes, client V1 không auth/token/credentials/getMe | [client.js](../frontend/src/api/storage/client.js), [errors.js](../frontend/src/api/storage/errors.js), [index.js](../frontend/src/api/storage/index.js), [queryKeys.js](../frontend/src/api/storage/queryKeys.js), [StorageLayout.jsx](../frontend/src/app/layouts/StorageLayout.jsx), [public.jsx](../frontend/src/app/router/public.jsx) | [P1](#7-evidence-p1--04102026): lint/build + browser GET thật / adapter mô phỏng |
| P2 list/paging/inactive/detail/placement/nodes/cluster/ready; null/stale đúng | [index.jsx](../frontend/src/app/pages/storage/index.jsx), [detail.jsx](../frontend/src/app/pages/storage/detail.jsx), [useFilesPage.js](../frontend/src/app/pages/storage/hooks/useFilesPage.js), [useFileDetailPage.js](../frontend/src/app/pages/storage/hooks/useFileDetailPage.js), [useSnapshotQuery.js](../frontend/src/app/pages/storage/hooks/useSnapshotQuery.js), Files/Placement/Cluster/Nodes panels | [P2](#8-evidence-p2--04102026): lint/build + controlled + UI GET thật |
| P3 upload success chỉ sau 201; download SHA Blob; lỗi Blob không save; không auto POST retry | [UploadPanel.jsx](../frontend/src/app/pages/storage/components/UploadPanel.jsx), [DownloadPanel.jsx](../frontend/src/app/pages/storage/components/DownloadPanel.jsx), [useStorageTransfers.js](../frontend/src/app/pages/storage/hooks/useStorageTransfers.js), [transfers.js](../frontend/src/app/pages/storage/utils/transfers.js) | [P3](#9-evidence-p3--04102026): controlled + UI/API thật 10 MiB+17B / rỗng; SHA Blob = nguồn |
| P4 delete 200/202, pending/terminal, không poll DELETE; read 404 ≠ disk proof | [DeletePanel.jsx](../frontend/src/app/pages/storage/components/DeletePanel.jsx), [DeletionNotice.jsx](../frontend/src/app/pages/storage/components/DeletionNotice.jsx), [deletions.js](../frontend/src/app/pages/storage/utils/deletions.js), StorageTransferProvider | [P4](#10-evidence-p4--05102026): controlled + delete thật 3 fixture; offline thuộc P6 |
| P5 repair filters/cursor/outcomes; zero-progress/timeout không hot-loop | [RepairPanel.jsx](../frontend/src/app/pages/storage/components/RepairPanel.jsx), [repairs.js](../frontend/src/app/pages/storage/utils/repairs.js) | [P5](#11-evidence-p5--05102026): controlled + scan scoped max_chunks=2; RF recovery thuộc P6 |
| P6 UI/API fault local: fallback download, repair DOWN→recovery, offline DELETE, OPERATION_BUSY | cùng UI ở trên; harness tạm `.runtime/m5-p6/` (ignored) | [P6](#12-evidence-p6--05102026): focused E2E Compose stop/start node-2; **local `dev_host` only** |
| P7 docs/env/evidence/bàn giao khớp; phân biệt static/runtime/local/hai host | plan này + README/index/frontend README | [P7](#14-evidence-p7--05102026): docs-only; không rerun runtime |

Phân biệt bằng chứng (không cộng thành full-suite hay hai host):

| Loại | Phạm vi M5 | Không suy ra |
|---|---|---|
| Static lint/build | compilation/import các phase P1–P5 | runtime/API/fault |
| Controlled / mô phỏng | adapter UI state, zero-progress, timeout UI | backend fault thật |
| Live backend local | GET/POST/DELETE qua Metadata Docker + UI Vite 5173 | failure-domain hai máy |
| Fault process local | stop/start `storage-node-2` cùng `dev_host` | Compose A/B, LAN CORS, hai host |
| Disk-save Downloads | chưa quan sát tự động; SHA từ Blob HTTP | file đã nằm trong thư mục Downloads |

**Live build tại gate UI:** Metadata image `35f6f3639213` (M4 P6). Các fixes
sau [review M4](M4_IMPLEMENTATION_PLAN.md#sửa-findings-sau-code-review--04102026)
có focused tests nhưng **chưa deploy live** — warning bàn giao, không blocker
các flow UI local đã pass trên build đó. P7 không deploy/rebuild.

**Fixtures:** owned P5 (`96729595-…`) và P6 delete (`1c283813-…`) đã dọn qua API;
postcondition P6: `files_available=1` ngoài ownership (`94c8a2d8-…`),
`cleanup_pending=0`, 3 nodes ACTIVE, lock trống. Không truyền fixture M5 sang
M6 như AVAILABLE bắt buộc; M6 tạo fixture mới theo ownership riêng.

### UI đã có và semantics giữ

Ba route public: `/` (files), `/cluster` (nodes/repair cụm), `/files/:fileId`
(detail/placement/repair file). Client `VITE_STORAGE_API_URL`
(mặc định `http://localhost:8000/api/v1`), `withCredentials=false`, không Bearer.
Nav Files/Cụm và `TransferNotice` nằm trong `StorageLayout`.
Chi tiết chạy local: [frontend/README.md](../frontend/README.md).

| Luồng | Hành vi đã triển khai |
|---|---|
| Upload/list/download | Multipart field `file`; 100% body = chờ commit; success chỉ sau 201. Download Blob + Content-Disposition; lỗi JSON/Blob không save. SHA evidence từ Blob HTTP. |
| Detail/placement/cluster | Poll GET ~4 s khi visible; stale có nhãn; null ≠ 0; không cộng capacity node. Observations (`known_readable`, ACTIVE) không phải integrity guarantee. |
| Delete/cleanup | 202 = đã xóa logic, đang dọn; theo dõi DELETING bằng GET; 404 terminal dừng poll; chỉ 200/DELETED/pending=0 xác nhận dọn xong. FAILED giữ `error_code`. |
| Manual repair | Filters/max_chunks 1..8/cursor; summary checked/repaired(Store ack)/remaining(unscanned); zero-progress không hot-loop; timeout không auto retry/advance cursor. |
| Busy / lỗi | Snapshot `operation_busy` + mutation 409 `OPERATION_BUSY`; network/timeout → reconcile GET trước user retry; mutation `retry:false`. |

### Giới hạn local và việc còn lại cho M6

Local Compose: ba node cùng `dev_host` — process failure only. **Không** chứng
minh hai failure domains, LAN URL/CORS giữa hai máy, hay build giống nhau trên
hai host.

M6 nhận frontend đã dùng API thật và local flows; còn lại:

1. Compose A/B và env LAN (ports, Metadata URL, CORS origins thật).
2. Cùng code/build trên hai host; port/check sẵn sàng trước demo.
3. Rehearsal: ngắt máy B vẫn đọc từ domain còn lại; bật lại B kiểm tra mapping/
   repair/over-replication theo contract.
4. Setup/demo guide phản ánh lệnh và quan sát thật — không suy ra từ M5 local.

Không mở rộng ở handoff: auth, WebSocket, automatic repair, job queue,
rename/version/search, framework test/E2E runner mới.

### Chạy UI local sau bàn giao

Backend prerequisites: [README repo](../README.md). Frontend từ `frontend/`:

```powershell
npm ci
Copy-Item .env.example .env.local
npm run dev -- --host localhost --port 5173 --strictPort
```

Mở `http://localhost:5173/`. Giữ port 5173 đúng CORS Compose local; không dùng
`127.0.0.1` trừ khi CORS cho phép origin đó. Đổi env phải restart Vite.

## 14. Evidence P7 — 05/10/2026

**P7 hoàn thành.** Tier **no execution**: chỉ Markdown. Rà DoD P1–P6 với source
`frontend/src/api/storage/` và `frontend/src/app/pages/storage/`, evidence mục
7–12, env/CORS trong frontend README. Không sửa runtime/tests/config/dependencies,
không lint/build/browser/Compose/deploy, không preflight/restart hoặc query
cluster live ở P7, không tạo `M6_IMPLEMENTATION_PLAN.md`.

**DoD:** bảng mục 13 đối chiếu đủ checkbox P1–P6; P7 chốt khi docs/setup/evidence
và bàn giao khớp implementation. Live Metadata vẫn image `35f6f3639213` theo
evidence P6 (không kiểm tra lại ở P7). Owned fixtures đã terminal ở P6; file
ngoài ownership giữ nguyên theo ownership rules. Không suy ra hai host, deploy
review M4, full lint/build/backend suite, hoặc disk-save Downloads từ lượt này.

**Docs chỉnh:** `M5_IMPLEMENTATION_PLAN.md` (header, P7 sub-phases, DoD checkbox,
mục 13 bàn giao M6, mục 14 evidence); `README.md`; `docs/README.md`;
`docs/IMPLEMENTATION_PLAN.md`; `frontend/README.md`.

**Kiểm tra tài liệu:** `git diff --check` trên các Markdown đã chỉnh; local
links/headings đối chiếu với files có thật. Không có runtime assertion vì không
chạy runtime checks.

**M5 hoàn thành P1–P7. Milestone tiếp theo M6 — hai máy và rehearsal**, dùng
bàn giao mục 13. Phase plan M6: [M6_IMPLEMENTATION_PLAN.md](M6_IMPLEMENTATION_PLAN.md)
(P1 Compose/env đã có 05/10/2026).
