# Chạy kiểm tra V2 tại máy phát triển

## Worker fairness/shared read budget E3

Tier **targeted + focused integration**, không cần frontend/Nginx. Native policy
từ `backend/`:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_v2_e3.py
```

Integration cần Docker/PostgreSQL V2 fixture healthy; preflight read-only như D2.
Fixture tự mở gRPC và Storage POSIX/schema riêng. Từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_e3_integration.py
```

Ngày 30/10/2026: **16 native riêng biệt + 11 integration mới + 50 direct
regressions pass**, không skip; Ruff năm file pass. Kiểm flood mỗi kind vẫn
cho hai kind khác tiến triển, chunk/allocation interleave giữa file lớn/nhỏ,
shared rate trước GetChunk, paused lease/deadline/reclaim và consumer shutdown.
Lệnh regression C2/job/C3 handoff và chi tiết các lượt tại
[evidence E3](V2_E_IMPLEMENTATION_PLAN.md#evidence-e3--30102026).
E4 load/RSS, browser C5 và file lớn chưa được nghiệm thu.

## Durable accounting nhiều process E2

Tier **focused integration**, cần Docker/PostgreSQL V2 fixture healthy; preflight
read-only như D2. Không cần HTTP/gRPC/Nginx hoặc service runtime bên ngoài.
Fixture dùng hai process với engine/connection riêng, lock barrier tại DB,
crash trước/sau commit, POSIX Storage volumes tạm và schema PostgreSQL riêng.
Từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_e2.py
```

**22 passed / 60.85s**, không skip, ngày 30/10/2026. Kiểm active sessions,
per-session claims, node charge, idempotency/retry, mất ACK/restart và release
chỉ sau durable Delete ACK; counter đối chiếu owner rows cùng SQL snapshot.
Ruff check/format hai file mới pass. Xem
[audit, quyết định grant và evidence E2](V2_E_IMPLEMENTATION_PLAN.md#contract-và-audit-e2--30102026).
Không thay runtime/schema; chưa thêm cap download/job cố định toàn cluster.
E3 fairness, E4 deployment/load, C5/browser/file lớn chưa nghiệm thu.

## Admission hai nhóm E1

Tier **targeted + focused integration**. Policy thuần chạy từ `backend/`:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_v2_e1.py
```

Phần TCP/HTTP thật và read lease cần Docker/PostgreSQL fixture healthy, preflight
read-only như D2. Rebuild để không dùng code cũ, từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_e1_integration.py
```

Kiểm flood cả hai chiều new work/progress, total/class cap, cancellation trả
slot, reject trước I/O/lease, recovery epoch và request ID/Retry-After. Các
hồi quy HTTP/deadline/upload/download và mixed load nhiều process được chọn
riêng; xem [commands và evidence E1](V2_E_IMPLEMENTATION_PLAN.md).
Không phải gate E2–E4, browser C5 hoặc file 1–10 GiB.

## Fixes sau review trước E

Tier focused integration, Docker Linux/Python 3.12, PostgreSQL fixture cho phần
HTTP/Storage thật. Dùng preflight D2, không dùng image cũ cho code mới. Từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_pre_e.py tests/test_v2_pre_e_integration.py
```

Regression mới kiểm nhiều GET/Range đồng thời cùng file vượt RF; reader cancel,
Prepare/Delete drain và durable fence; control header/body nhỏ giọt, cancellation,
TLS verification/handshake, size/EOF và 204; parse duration và request ID cho
429/503/HEAD qua admission lồng nhau. Fixture TLS dùng self-signed credentials
trong `backend/tests/fixtures/`, không thay trust store production. Commands và
evidence hồi quy chọn lọc ở [fixes trước E](V2_PRE_E_FIXES.md).

## Nhiều process và tải hỗn hợp D5

Tier **focused integration/E2E**; chỉ cần Docker/PostgreSQL V2 test healthy.
Preflight read-only như mục D2; không tự start fixture nếu thiếu dependency trong
tác vụ kiểm thử. D5 đã được người dùng cho phép start PostgreSQL ngày 29/10/2026.
Các API/internal/Transfer/Storage/Worker/Nginx process được fixture tạo và đóng
trong tests container, schema/bytes tạm riêng. Từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-d5-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-d5-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d5.py
```

Override cap memory 3 GiB, bind `backend/.runtime/v2-d5/` làm `/evidence` để lưu
report/log/nginx config. **6 ca pass**, hai ca read/delete siết assertion chạy lại
riêng pass; Ruff hai Python files pass. Xem
[evidence và giới hạn D5](V2_D5_EVIDENCE.md) và
[commands chọn lọc](V2_D_IMPLEMENTATION_PLAN.md#7-evidence-d5--29102026).
Không phải browser C5, file 1–10 GiB, full suite hoặc bằng chứng HA nhiều host.

## GET streaming/Range D4

Tier **focused integration**; Docker/PostgreSQL V2 test healthy, dùng preflight
read-only ở mục D2. Không cần start API/Transfer/Storage containers bên ngoài:
fixture tự mở HTTP/Nginx/gRPC, SQLite/bytes tạm và schema DB riêng. Không dùng V1
volumes, không reset dữ liệu. Build source hiện tại rồi chạy từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d4.py
```

60 ca D4 riêng biệt đã pass qua các lượt chọn lọc ngày 29/10/2026, cùng 23 direct
regressions; không có một lượt nguyên file pass để suy ra full-suite evidence.
Lệnh chọn lọc, lỗi đã sửa và topology tại
[evidence D4](V2_D_IMPLEMENTATION_PLAN.md#6-evidence-d4--29102026).
Gate kiểm bytes/status/headers/fallback và lease lifecycle; blocked-send có điều
phối qua ASGI. Chưa đo TTFB/read amplification/RSS/file lớn hoặc nhiều instance D5.

## Catalog/read commands/HEAD D3

Tier **focused integration**, cần Docker và PostgreSQL V2 test healthy. Dùng
preflight read-only ở mục D2 bên dưới; không cần API/Transfer/Storage containers
chạy sẵn hoặc browser. Fixture tự mở các HTTP listener, Nginx và Storage gRPC/
SQLite/bytes tạm cần cho từng ca; mỗi ca sở hữu schema DB riêng.

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_d3.py
```

55 ca D3 và 13 selected C3 regressions pass ngày 28/10/2026; lệnh regression,
topology và giới hạn ở [evidence D3](V2_D_IMPLEMENTATION_PLAN.md#5-evidence-d3--28102026).
Không dùng image cũ cho runtime mới, không rerun gate đã pass nếu không bị ảnh
hưởng. Gate chỉ kiểm metadata/control-plane/HEAD và DELETE cleanup, chưa có
GET binary streaming/Range, benchmark hoặc nhiều instance D5.

## Read/delete lease D2

Tier **focused integration**, PostgreSQL V2 test thật, mỗi ca sở hữu schema riêng.
Không cần API/Transfer đang chạy, browser hoặc Storage containers; ca Worker dùng
hai gRPC Storage với SQLite/bytes tạm trong test container. 57 ca D2 và 20 direct
regressions đã pass ngày 28/10/2026; xem
[evidence D2](V2_D_IMPLEMENTATION_PLAN.md#4-evidence-d2--28102026). Chỉ chạy lại
gate bị ảnh hưởng khi code/config/dependency/environment thay đổi.

Preflight chỉ đọc từ root:

```powershell
docker info --format '{{.ServerVersion}}'
docker compose -f deploy/compose.v2-metadata-tests.yml ps
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -Atc 'SELECT current_database(),current_user,1 AS ready'
```

Nếu dependency chưa sẵn sàng, dừng theo blocker policy trong `AGENTS.md`; không
chạy test để thay lỗi prerequisite bằng lỗi fixture. Khi được phép setup:
`docker compose -f deploy/compose.v2-metadata-tests.yml up -d --wait postgres`.

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_d2.py
```

Build sau thay đổi runtime/config/dependency; không dùng image cũ làm evidence
code mới. Với thay đổi chỉ ở test harness sau build, có thể mount file test hiện
tại read-only vào `/app/tests/test_v2_d2.py`. Gate này không chứng minh route
GET/HEAD/DELETE, streaming/Range HTTP, RSS hoặc browser C5.

## Download HTTP D1 — logic thuần

Tier **targeted tests/checks**, không cần Docker, DB, API hoặc browser. Từ
`backend/` dùng Python của `.venv`:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_v2_download_http.py
.\.venv\Scripts\python.exe -m ruff check transfer/v2/download_http.py tests/test_v2_download_http.py
.\.venv\Scripts\python.exe -m ruff format --check transfer/v2/download_http.py tests/test_v2_download_http.py
```

Gate chỉ kiểm status/header/byte selection, không phải HTTP streaming hoặc
read/delete lease evidence. Xem [evidence D1 và bước D2](V2_D_IMPLEMENTATION_PLAN.md).

## Browser/tệp lớn C5 — harness chưa nghiệm thu

Harness `/checks/v2-c5.html` dùng chính controller/journal/hash Worker C4.
Fixture và lệnh Compose/overlay 3 GiB ở
[evidence lượt chuẩn bị](V2_C_IMPLEMENTATION_PLAN.md#15-chuẩn-bị-gate-browsertệp-lớn--26102026-chưa-hoàn-tất).
Chỉ empty case đã qua; IAB fetch bị ngắt và Computer Use Windows dừng vì không
xác định được URL đủ tin cậy. Đừng coi các nhánh crash/1–10 GiB/slow đã pass.
Sau khi browser localhost sẵn sàng, xác nhận focused harness trước khi benchmark;
không retry aggregate suite vì flake môi trường. Source OPFS được tạo trong
thư mục riêng `dfs-c5-sources`, không dùng tệp cá nhân.

## Nhiều instance C5 — gate file nhỏ

Tier **focused integration**, chỉ cần Docker và PostgreSQL V2 test đang healthy.
Preflight read-only từ root:

```powershell
docker info --format '{{.ServerVersion}}'
docker compose -f deploy/compose.v2-metadata-tests.yml ps
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -Atc 'SELECT current_database(),current_user,1 AS ready'
```

Nếu prerequisite lỗi, dừng gate và theo blocker policy trong `AGENTS.md`.
Khi DB chưa chạy, lệnh setup hiện có là
`docker compose -f deploy/compose.v2-metadata-tests.yml up -d --wait postgres`;
test-only không tự chạy lệnh này. Không cần migrate public schema hoặc Storage
containers: mỗi test sở hữu schema và thư mục ledger/bytes tạm riêng.

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c5.py
```

Fixture chạy hai process API public, hai internal listener, hai Transfer và một
Worker riêng. Nginx thật dùng upstream round robin, không sticky session hoặc
replay mutation; access log/header test ghi upstream để kiểm traffic thực sự qua
cả hai instance. Tất cả HTTP/gRPC chỉ ở loopback trong test container, không
publish port Windows hay dùng volumes V1.

Ba ca kiểm empty/biên chunk 64 KiB/multipart, restart API/Transfer sau part đã
confirm và SIGKILL Transfer khi cả hai Storage đang WRITING. Ca cuối đợi lease DB
60 giây thật, rồi retry generation-fenced và abort/cleanup; không sửa timestamp
DB. Dữ liệu được đọc lại trên cả hai replica và so full/chunk SHA, size và domain.
Gate dùng HTTP client Python; chưa kiểm C4 browser trên topology này, file
1–10 GiB, RSS/queue/socket/backpressure hoặc toàn crash matrix C5.

Tùy chọn lưu JSON topology/request/bytes và service logs bằng bind mount thư mục
evidence, đặt `V2_C5_REPORT_DIR` tới đường dẫn đó trong container. Ví dụ đúng
workspace hiện tại (tạo `.runtime/v2-c5` trước):

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps -v D:/distributed-file-storage/.runtime/v2-c5:/evidence -v D:/distributed-file-storage/backend/tests/test_v2_c5.py:/app/tests/test_v2_c5.py:ro -e V2_C5_REPORT_DIR=/evidence tests python -m pytest -q tests/test_v2_c5.py
```

Mount file test read-only cho phép dùng harness hiện tại trên image đã build;
nếu runtime/dependency/config thay đổi vẫn phải rebuild image trước gate.
JSON có thể được ghi cả khi test lỗi; kết luận passed dựa trên output pytest,
không dựa trên sự tồn tại của report. Chỉ rerun ca bị ảnh hưởng khi sửa harness;
không chạy lại các gate C1–C4 hoặc full suite chỉ vì đang làm C5.

## Browser client C4

Client `/uploads-v2`, fixture HTTP/gRPC/DB riêng và gate browser hai giai đoạn
(trước/sau reload) ở [V2 C4 client](V2_C4_CLIENT.md#kiểm-chứng-tái-lập).
Browser overlay riêng chỉ publish ingress fixture vào localhost; PostgreSQL vẫn
trên mạng internal. Control/fault routes chỉ nằm trong test process. Xem
[evidence C4](V2_C_IMPLEMENTATION_PLAN.md#12-evidence-c4--25102026) để phân biệt
18 nhóm controller/API pass với native picker/UI click-flow chưa xác nhận.

## HTTP/Transfer C3

Tier **focused integration** với cùng PostgreSQL V2 test healthy bên dưới.
Overlay riêng thêm Nginx vào tests image để kiểm ingress không buffer toàn body:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c3.py
```

Không start Storage containers; fixture dùng hai server gRPC với SQLite/bytes
tạm và schema PostgreSQL riêng. Runtime/role/env/budgets, ingress và prerequisites
ở [runtime C3](V2_C3_RUNTIME.md); kết quả thực tế ở
[evidence C3](V2_C_IMPLEMENTATION_PLAN.md#10-evidence-c3--24102026).
Đây không phải client C4, nhiều instance/tệp 1–10 GiB/RSS C5 hoặc download D.

## Port Docker: cách đọc

`8000:8000` nghĩa là port Windows 8000 chuyển vào port container 8000.
Phần bên trái phải khác giữa các ứng dụng chạy cùng lúc. Các container khác
nhau có thể cùng dùng port bên phải 5432; không có xung đột nếu không publish
cùng port Windows. Tên service được phân giải trong mạng của từng Compose project.

V1 local hiện publish Metadata 8000 và Storage 50051–50053 theo mặc định;
giá trị có thể override trong `deploy/.env`. PostgreSQL V1 không publish port.
DB test V2 cũng không publish port: không cần dành 5433, 5434… trên Windows.

## Storage V2: không cần PostgreSQL

Từ root repository:

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml -f deploy/compose.v2-tests.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml -f deploy/compose.v2-tests.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_v2_ledger.py tests/test_v2_streaming.py
```

Override thêm tmpfs 8 MiB để kiểm ENOSPC thực; không gắn volume V1. Các ca
SIGKILL dùng thư mục tạm trong container Linux. Không suy ra chịu mất điện
hoặc mất host từ các ca này. Native Windows bị chặn cho ledger V2 vì chưa có
implementation directory fsync tương đương; V1 vẫn dùng entry point hiện có.

## DB test V2 riêng

`deploy/compose.v2-metadata-tests.yml` dùng Compose project `dfs-v2-tests`,
mạng internal riêng, database `dfs_v2_test`, dữ liệu tmpfs riêng. Password là
fixture test local trong file Compose; không dùng cho deployment. Không publish
port, không nạp migration V1, không mount dữ liệu ứng dụng hiện có.

Đã được người vận hành cho phép dựng ngày 23/10/2026. PostgreSQL hiện healthy;
`SELECT current_database(), current_user, 1 AS ready` trả
`dfs_v2_test | v2_test | 1`. Đây là evidence setup/kết nối, chưa phải evidence
migration hoặc transactions B3. Lệnh dựng/kiểm trạng thái:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml up -d --wait postgres
docker compose -f deploy/compose.v2-metadata-tests.yml ps
```

Gate metadata chỉ chạy khi PostgreSQL healthy và implementation/tests B3 có
sẵn. `TEST_DATABASE_URL` trong tests trỏ tới hostname `postgres` trong mạng riêng,
không phải localhost Windows. Không chạy các test V1 để thay evidence V2.

### Migration và kiểm tra B3

Đã có implementation B3. Evidence migration vào `public` ngày 23/10/2026 ghi
nhận 14 bảng nghiệp vụ, một bảng phiên bản, `cluster_control` ACTIVE và admission
counter `0/128`. Đây là trạng thái lịch sử: DB fixture dùng tmpfs; lượt review
sau khi DB dừng đã ghi nhận public_tables=0 và không migrate lại public. Test B3/B4
tự migrate schema sở hữu riêng, không cần migration public. Xem
[evidence](V2_B_IMPLEMENTATION_PLAN.md#evidence-b3--23102026).
Từ root repository, các lệnh tái lập khi cần (không cần chạy lại gate đang pass):

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_metadata.py
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python scripts/migrate_v2_metadata.py
```

Tests chỉ tạo/dọn schema `test_v2_<uuid>` sở hữu riêng; không chạy migration V1.
CLI migration dùng `V2_DATABASE_URL`, `V2_NAMESPACE_ID`, `V2_RECOVERY_EPOCH`
được khai báo trong tests service. Namespace/epoch này là fixture V2 bên ngoài
DB. Không đổi epoch để né lỗi checksum/schema; không tự promote DB restore.
Migration có version/checksum, khóa migration trong transaction và từ chối schema
đã có bảng ngoài chuỗi V2; không có lệnh downgrade/reset. Repository readiness
chỉ mở khi namespace/epoch/mode khớp. Cấu hình test không phải deploy API V2.

Foundation đã có confirm/retry/complete/finalize publication transaction C1;
chưa có HTTP routes hoặc read lease runtime. C2 đã thêm Worker riêng (gate bên dưới).
Cleanup dùng `JobLease`
còn hiệu lực trong cùng transaction với authorize/
release, mỗi trang tối đa 8 part; caller phải thực hiện Storage I/O ngoài
transaction và chỉ gửi lại durable Delete ACK có đúng scope/identity/generation.
ACK chưa có hoặc NOT_FOUND không giải phóng node budget. Không finish cleanup
job trước khi đối soát hết các trang/outcome; orchestration C2 đã kiểm chứng.

### Transaction gate C1

Dùng cùng preflight PostgreSQL của B4 bên dưới; không cần tmpfs ENOSPC override.
Từ root repository, chỉ tái lập gate khi có thay đổi hoặc cần evidence mới:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c1.py
```

Đợi build kết thúc thành công trước khi chạy để tránh dùng image cũ. Tests dùng
schema riêng, DB clock và connections cạnh tranh; một ca inspect/reconcile/finalize
dùng bytes Storage thật trong container Linux. Không suy ra HTTP/Worker lifecycle,
streaming end-to-end hoặc benchmark từ kết quả này. Xem
[evidence C1](V2_C_IMPLEMENTATION_PLAN.md#6-evidence-c1--23102026) cho các lượt
affected checks và số case khác nhau; không chạy lại các gate đã pass chỉ vì
chuyển phase.

### Worker gate C2 và entrypoint

Dùng preflight PostgreSQL ở mục B4 bên dưới. Không cần Storage container, tmpfs
ENOSPC hoặc API V1; tests tự dựng hai gRPC Storage/ledger tạm, schema DB riêng.
Từ root, khi code/config đổi hoặc cần tái lập evidence:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml build tests

docker compose -f deploy/compose.v2-metadata-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_c2.py
```

Xem [evidence C2](V2_C_IMPLEMENTATION_PLAN.md#8-evidence-c2--24102026) cho các lượt
affected đã chạy và giới hạn evidence. Các ca bao gồm mất ACK, generation fence
và drain writer, cleanup paging/retry, full byte hash, stale lease/abort/epoch,
malformed stream/failed EOF, SIGKILL tại commit boundaries và SIGTERM entrypoint.
Không chạy lại gate đã pass chỉ vì chuyển phase.

Entrypoint riêng: `python -m metadata.v2.worker` từ `backend/` với generated V2
stubs trong PYTHONPATH như runtime image. Bắt buộc `V2_DATABASE_URL`,
`V2_NAMESPACE_ID`, `V2_RECOVERY_EPOCH`; `V2_DATABASE_SCHEMA` mặc định `public`.
DB phải được migrate V2 và Storage endpoints đã provision/activate rõ ràng;
Worker không tự migrate/provision/start DB hoặc cài epoch. Không chạy entrypoint
với DB/volume V1. Gate entrypoint tự truyền schema fixture riêng, không cần migrate
public hoặc triển khai service lâu dài.

Mặc định WorkerConfig: 2 job slots, 1 finalize, page ≤8 part, RPC deadline 5s,
renew 15s/lease 60s, retry 8 claims/backoff 5–300s, job budget 3600s,
shutdown budget 30s; finalize đọc block ≤256 KiB, budget 64 MiB/s mỗi finalize.
`Worker.ready()` dành cho adapter chỉ true khi loop đang chạy và DB gate hợp lệ;
CLI không expose HTTP health. Điều chỉnh budget qua WorkerConfig khi tích hợp
deployment C3. Retry cần consumer dùng `retry_for_consumer` đồng bộ trên instance
Worker riêng; background chỉ reconcile STORED, không tự giữ claim chờ Transfer.
Job CLEANUP DEAD giữ charge/nghĩa vụ để vận hành đối soát, không tự reset data.

### Gate liên kết B4

Chỉ chạy khi gate bị ảnh hưởng hoặc cần tái lập evidence; không cần chạy lại một
gate đã pass khi code/config/environment không đổi. Preflight read-only:

```powershell
docker info
docker compose -f deploy/compose.v2-metadata-tests.yml ps
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -Atc 'SELECT current_database(), current_user, 1 AS ready'
```

Khi Docker truy cập được và PostgreSQL healthy, từ root repository:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-tests.yml run --rm --no-deps tests python -m pytest -q tests/test_v2_b4.py
```

Gate cần cả PostgreSQL và tmpfs override; không chạy native Windows. Bốn case
kiểm incarnation/ACK identity, disk-full/flush/charge và offline epoch CLI với
stream đang WRITING. Schema/volume tạm riêng, không mount V1; CLI maintenance
chỉ chạy trên volume fixture. Không dùng gate này làm bằng chứng restore cluster
hay HTTP upload E2E. Xem [evidence và bàn giao C](V2_B_IMPLEMENTATION_PLAN.md#evidence-b4-và-bàn-giao-c--23102026).

Khi không cần DB fixture:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml down
```

Lệnh trên chỉ nhắm project `dfs-v2-tests`. DB test là dữ liệu tạm, mất khi
container bị recreate/dừng; không dùng làm nơi lưu file hoặc metadata cần giữ.

## Provision và maintenance Storage

V2 chạy bằng `python -m storage.v2.server`; lần đầu cần `--provision` và
volume **mới, rỗng, riêng V2**. Biến bắt buộc: `V2_NAMESPACE_ID`,
`V2_RECOVERY_EPOCH`, `V2_NODE_INCARNATION` (UUID canonical), `NODE_ID`,
`FAILURE_DOMAIN`, `V2_DATA_DIR`. `V2_GRPC_BIND` mặc định `127.0.0.1:50061`.
RPC client phải đặt deadline tối đa 120s. Generate stubs native bằng
`backend/.venv/Scripts/python.exe backend/scripts/generate_v2_proto.py`;
Docker image tự generate V1 và V2. Không dùng V1 volume cho V2.

Maintenance chạy khi đã đóng ingress, cô lập API/Transfer/Worker và dừng Storage.
`python scripts/v2_storage_maintenance.py begin --epoch <new-uuid>` giữ mode
RECOVERING; `adopt --epoch <new-uuid> --allocation <uuid>` chỉ nhận STORED có
bytes/hash đúng. `close` ghi quyết định đóng allocation; không unlink trong
maintenance. `activate --epoch <new-uuid>` mở epoch đã đối soát; sau đó đổi
cấu hình bên ngoài DB sang epoch mới trước khi restart. Các lệnh dùng scope cũ
trong env cho đến activate. Volume lock chỉ chứng minh process Storage local
đã dừng, không tự chứng minh writer trên các host khác đã được cô lập.

Allocation không được adopt giữ nguyên ledger/bytes và bị chặn phục vụ/cleanup
ở epoch mới. Tombstone không thể adopt thành STORED. Quy trình này là hook local;
nghiệm thu restore toàn cluster, DB recovery gate và rebuild budget còn thuộc
B3/B4–H, chưa có evidence chỉ từ Storage tests.
