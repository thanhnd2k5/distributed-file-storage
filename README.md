# Distributed File Storage

React → REST → Python 3.12/FastAPI + PostgreSQL → unary gRPC → ba Storage Nodes.
Đặc tả V1: [docs/README.md](docs/README.md).

## Cấu trúc repo

```text
backend/
  metadata/       # FastAPI, coordinator, DB/migrations
  storage/        # gRPC Storage Node
  common/         # Config/logging dùng chung cho hai service
  contracts/      # Proto gRPC
  generated/      # Python stubs
  tests/
  scripts/
  requirements*.txt, pyproject.toml, alembic.ini
frontend/         # React/Vite
deploy/           # Dockerfile, Compose, env triển khai
docs/             # Đặc tả và kế hoạch toàn dự án
```

`backend/` phân biệt phần Python với `frontend/`; `metadata/` và `storage/`
là tên các service theo trách nhiệm. Lệnh Docker chạy từ repo root; lệnh Python
native chạy từ `backend/`. Import Python vẫn dùng `metadata`, `storage`, `common`.

## Trạng thái

Base M0: dependency lock, config/logging, generated proto, FastAPI live/ready,
SQLAlchemy và migration bốn bảng, registry/startup recovery, gRPC HealthCheck,
Compose local với PostgreSQL và ba volume storage riêng. Frontend starter đã có
trong `frontend/`; frontend chạy bằng Vite trên máy dev.

**M1 đã hoàn thành (03/10/2026).** Full backend suite: 106 passed, không skipped;
lint/format, schema drift check và smoke cluster qua. Metadata và cả ba Storage
node dùng build M1 tại thời điểm gate.

**M2 đã hoàn thành P1–P6 (03/10/2026).** Metadata đã chạy health polling và
GET nodes/cluster trên build M2. Các gate registry/detector/worker/API có focused
tests và lint/format; P5 đã quan sát down/recovery, restart Metadata/PostgreSQL
giữ metadata và volumes. P6 rà source/contracts/evidence và chốt bàn giao M3,
không sửa runtime hoặc chạy lại tests. Counts P1–P5 có regression trùng nhau,
không coi là một full-suite run. Tiếp theo **M3: upload/download RF=2**.
Chi tiết: [DoD, evidence và bàn giao M3](docs/M2_IMPLEMENTATION_PLAN.md).

Review bổ sung M2 đã sửa guard schema/validation của smoke khi chạy Python `-O`,
thời gian inf/nan và readiness khi scheduler dừng, smoke có disabled history.
Regression: 54 targeted checks native + 53 focused integration Docker Linux
passed; lint/format 8 files qua. Metadata đã cập nhật bản sửa, baseline `-O`
read-only qua và ba node ACTIVE; không rerun full suite hoặc kịch bản DB/node restart.

Các ghi nhận P1–P5 dưới đây là lịch sử tại thời điểm từng phase.

**M2 P1 đã hoàn thành (03/10/2026).** Registry/startup reset DOWN, giữ mapping
và lịch sử health cùng endpoint, xóa snapshot khi endpoint/domain đổi; recovery
chạy dưới operation_lock. Focused PostgreSQL/lifespan checks: 16 passed, không
skipped/deselected; lint/format 3 files qua. Chỉ image tests được rebuild, chưa
deploy/restart services. Tiếp theo P2 health state machine; M2 chưa hoàn thành.
Chi tiết: [kế hoạch và evidence M2](docs/M2_IMPLEMENTATION_PLAN.md).

**M2 P2 đã hoàn thành (03/10/2026).** Health state machine xử lý
ACTIVE/SUSPECTED/DOWN bằng monotonic clock, identity/domain, unwritable và
recovery. 32 targeted tests native Windows passed, không skipped/deselected;
lint/format 2 files qua. Một warning pytest cache bị sandbox chặn ghi.
Chưa nối RPC/persistence hoặc deploy/restart dịch vụ; tiếp theo P3 worker/lifespan.

**M2 P3 đã hoàn thành (03/10/2026).** Health worker chạy theo lifespan, poll
độc lập từng node, dùng Session/transaction riêng trước và sau RPC, publish
snapshot sau commit và dừng threads/channels trước dispose DB engine.
Focused integration/regression: 32 passed trong Docker Linux với gRPC thật và
PostgreSQL schema riêng; lint/format 5 files qua. Chỉ rebuild tests image, chưa
deploy/restart services. Tiếp theo P4 API nodes/cluster; M2 chưa hoàn thành.

**M2 P4 đã hoàn thành (03/10/2026).** GET `/api/v1/nodes` và `/api/v1/cluster`
đọc snapshot DB, trả đúng contract null/UTC và counters theo RF từng file.
Cluster dùng một SQL statement; GET vẫn đọc được khi operation_lock busy và
không gọi RPC. HTTP/PostgreSQL checks: 32 passed trong Docker Linux, không
skipped/deselected; lint/format 6 files qua. Chỉ rebuild tests image, chưa deploy
API mới vào Metadata đang chạy. Tiếp theo P5 lifecycle/Compose smoke; M2 chưa xong.

**M2 P5 đã hoàn thành (03/10/2026).** Metadata đã deploy build M2. Lifecycle
test với hai process thật và schema PostgreSQL riêng: 1 passed; Compose smoke
quan sát node-2 ACTIVE → SUSPECTED → DOWN → ACTIVE, hai node khác vẫn ACTIVE,
live/ready vẫn 200. Restart Metadata/PostgreSQL giữ metadata fixture và named
volumes; ba Storage volumes khác nhau, fixture đã dọn, tất cả services healthy.
Lint/format 3 Python files và PowerShell syntax qua. Còn P6 rà DoD/bàn giao M3;
M2 chưa đánh dấu hoàn thành. Chi tiết trong evidence P5 của phase plan.

Sau review M1, đã sửa Health bị starvation khi bốn transfer worker đợi lock
(executor riêng cho Health) và used_bytes cộng dư khi Store lại chunk mất trên
disk (cập nhật theo chênh lệch size đã accounted). Focused regression:
105 passed, 4 deselected; lint/format qua. Fixtures Storage dùng chung production
server factory. Chi tiết trong bằng chứng review của M1 phase plan.

M1 P1 đã có validation UUID/SHA-256/payload và startup filesystem. Chunk committed
dùng `<canonical_uuid>.chunk`; tempfile Store dùng `.store-<32 lowercase hex>.tmp`
trong cùng DATA_DIR. Startup chỉ dọn regular tempfiles đúng mẫu, giữ file lạ,
khởi tạo used_bytes từ committed chunks và fail nếu limit nhỏ hơn chunk đã lưu.
Mỗi DATA_DIR chỉ dành cho một Storage process; dừng process cũ trước khi chạy mới.

M1 P2 đã implement StoreChunk atomic: write/flush/fsync/replace, mutex và context
checks trước commit. Retry cùng ID/bytes trả already_existed=true; khác bytes
trả ALREADY_EXISTS, không overwrite. used_bytes tăng sau commit mới, không tăng
ở retry. Lỗi disk không ack thành công và tempfile được dọn khi có thể.

M1 P3 đã có GetChunk đọc snapshot dưới cùng mutex, trả bytes và SHA-256 thực tế;
chunk thiếu trả NOT_FOUND, lỗi đọc/size local nhận biết trả DATA_LOSS. DeleteChunk
xóa idempotent: existed=true rồi false; chỉ giảm cached used_bytes sau unlink
thành công. Lỗi xóa không ack success hay thay đổi accounting.

Các data RPC trả INVALID_ARGUMENT khi input sai. Chưa có upload,
download, delete qua REST hay cleanup worker/repair. Health polling đã qua
integration P3; nodes/cluster APIs đã qua P4 và đang chạy trên Metadata build M2
sau Compose smoke P5. Startup recovery chỉ
đánh dấu upload gián đoạn FAILED và giữ cleanup_pending trong DB; worker xử lý
chúng thuộc các milestone sau. Node registry khởi tạo DOWN đến health success
hợp lệ đầu tiên của process hiện tại. M2 đã hoàn thành; luồng file thuộc M3.

M1 P4 đã tách stats lock ngắn cho used_bytes: Health đọc cached snapshot mà
không chờ mutex chunk hay scan directory. Probe phát hiện short write; lỗi
bất ngờ được sanitize. Các checks gRPC cancel/deadline trước commit, concurrency
Store/Get/Delete và mất ack → retry → Delete đã qua. Restart/crash process và
volume isolation còn thuộc P5.

M1 P5 đã xác minh persistence sau restart process/container thật, cleanup tempfile
sau SIGKILL trước commit và volume node-1/node-2 độc lập. P6 đã qua tổng kiểm tra
và hoàn tất bàn giao M1; bằng chứng chi tiết trong phase plan.

## Chạy backend bằng Docker (không cần Python cài trên host)

Từ repo root, với Docker Desktop đang chạy Linux containers:

```powershell
Copy-Item deploy/.env.example deploy/.env
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --build --wait
docker compose --env-file deploy/.env -f deploy/compose.local.yml ps
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec metadata python scripts/smoke_base.py
```

Chỉ copy `.env` lần đầu; giữ nguyên cấu hình riêng khi đã có file. Mật khẩu mẫu
chỉ dùng local. Nếu đổi password, dùng ký tự URL-safe hoặc encode trong DATABASE_URL.
Compose tự chạy `alembic upgrade head` trước khi khởi động một Uvicorn worker.
PostgreSQL không publish port ra host/LAN; Metadata/gRPC local bind 127.0.0.1.

- OpenAPI: http://localhost:8000/docs
- Live: http://localhost:8000/api/v1/health/live
- Ready: http://localhost:8000/api/v1/health/ready
- Nodes: http://localhost:8000/api/v1/nodes
- Cluster: http://localhost:8000/api/v1/cluster
- gRPC node-1/2/3: localhost:50051/50052/50053.

`ready` kiểm tra DB, startup initialization và health scheduler đang chạy;
không yêu cầu mọi node ACTIVE. Scheduler lỗi/dừng trả ready 503; live và GET
nodes/cluster vẫn cho xem snapshot nếu DB dùng được.
`live` vẫn hoạt động nếu DB mất kết nối sau startup. Cấu hình sai fail ngay khi
khởi động; lỗi DB/schema lúc initialize giữ readiness 503, cần restart sau khi sửa.

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml logs --tail 100 metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml stop
docker compose --env-file deploy/.env -f deploy/compose.local.yml start --wait
```

Stop/start giữ nguyên bốn named volumes. Compose local dùng cùng `dev_host`;
demo hai máy và các file Compose A/B sẽ làm ở M6.

Smoke M2 mặc định chỉ đọc REST. Kịch bản PowerShell bên dưới dành cho local dev:
stop/start node-2, restart Metadata/PostgreSQL, giữ volumes và chỉ dọn schema
fixture riêng do lượt chạy tạo.

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec -T metadata python scripts/smoke_metadata.py
powershell -ExecutionPolicy Bypass -File backend/scripts/smoke_metadata.ps1
```

## Môi trường Python để code

```powershell
powershell -ExecutionPolicy Bypass -File backend/scripts/setup.ps1
# Hoặc truyền đường dẫn python.exe 3.12 nếu py launcher chưa dùng được:
powershell -ExecutionPolicy Bypass -File backend/scripts/setup.ps1 -PythonPath 'C:\path\to\python.exe'
Set-Location backend
$env:PYTHONPATH = "$PWD;$PWD\generated"
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest -q
```

Script tạo `backend/.venv` riêng, cài `backend/requirements-dev.txt`, generate/import
proto, và tạo `deploy/.env` nếu chưa có. Runtime lock nằm trong
`backend/requirements.txt`; dev lock giữ cùng phiên bản runtime. Muốn cập nhật
lock, chạy từ `backend/`, dùng pip-tools với Python 3.12:

```powershell
.\.venv\Scripts\python.exe -m pip install pip-tools
.\.venv\Scripts\python.exe -m piptools compile --allow-unsafe --strip-extras -o requirements.txt requirements.in
.\.venv\Scripts\python.exe -m piptools compile --allow-unsafe --strip-extras -c requirements.txt -o requirements-dev.txt requirements-dev.in
.\.venv\Scripts\python.exe scripts/generate_proto.py
```

`backend/contracts/storage.proto` là wire contract dùng để generate. Không sửa generated
stubs bằng tay. `docs/storage.proto` giữ bản đi cùng đặc tả; khi đổi contract phải
đồng bộ hai bản và API docs. Cả client/server thêm `backend/generated/` vào PYTHONPATH.

Để chạy Metadata native, từ `backend/` copy `.env.example` thành `.env`, đặt DATABASE_URL tới
PostgreSQL truy cập được từ host, chạy migration rồi server:

```powershell
.\.venv\Scripts\python.exe -m alembic upgrade head
.\.venv\Scripts\python.exe -m uvicorn metadata.main:create_app --factory --port 8000 --workers 1
```

Compose mặc định không publish DB; native development cần PostgreSQL riêng hoặc
Compose override publish 5432 chỉ vào 127.0.0.1. Một storage process native:
`.\.venv\Scripts\python.exe -m storage.server` với NODE_ID, FAILURE_DOMAIN,
GRPC_PORT và DATA_DIR theo `.env.example`. Các process khác cần DATA_DIR/id/port riêng.

## Frontend

Từ repo root:

```powershell
Set-Location frontend
npm ci
npm run dev
```

Giữ base FE hiện có. Chưa gắn API files/cluster và chưa thay các màn hình/auth
mẫu của starter; V1 dự án không cần auth. Tích hợp API và UI thuộc M5.

## Kiểm tra bằng môi trường Linux giống deployment

Từ repo root; Dockerfile lấy mã nguồn trong `backend/` và giữ working directory
`/app` bên trong container, nên các lệnh container không thêm prefix `backend/`.

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec metadata alembic check
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec metadata python scripts/smoke_base.py
```

Tests kiểm tra config sai, duplicate identity/RF, payload proto 2 MiB, health RPC,
Store/Get/Delete thật, cùng live/ready khi DB không kết nối được.
Smoke kiểm tra schema bốn bảng, registry và identity/writable của cả ba node.
Các checks lưu chunk và process lifecycle M1 đã có; replication thuộc M3–M4.

Đã xác minh ngày 03/10/2026: 7 tests qua trong Docker Linux, lint/format qua,
`alembic check` không có schema drift, smoke cả ba node qua trước/sau restart
Metadata, và script setup chạy được với Python 3.12.14. Integration tests dùng
schema PostgreSQL riêng và tự dọn, không ghi vào bảng dữ liệu của ứng dụng.

Trong phiên thiết lập này, test gRPC loopback native Windows bị timeout; nguyên
nhân chưa xác định. Docker Linux là đường chạy đã được kiểm chứng. Bộ test hiện
có một deprecation warning từ Starlette khi dùng httpx, không ảnh hưởng kết quả.

Các kết quả P1–P5 dưới đây ghi nhận tại thời điểm từng phase; kết quả cuối M1 ở P6.

P1 đã xác minh ngày 03/10/2026: 58 tests qua trong Docker Linux (7 checks trước
đó và 51 cases P1), lint/format qua. Tiến độ chi tiết:
[M1 phase plan](docs/M1_IMPLEMENTATION_PLAN.md).

Sau khi gom Python vào `backend/`, đã kiểm tra lại setup/venv, proto generation,
Docker build, 58 tests, lint/format và smoke cluster: đều qua. Các named volumes
và tên service Compose giữ nguyên.

P2 đã xác minh ngày 03/10/2026: focused integration **70 passed, 4 deselected**
trong Docker Linux, lint/format qua. Không chạy DB tests/full suite/cluster smoke
cho thay đổi Store này. Chỉ image test được rebuild; muốn dùng P2 trong runtime
containers, chạy lệnh `up -d --build --wait` ở phần setup phía trên.

P3 đã xác minh ngày 03/10/2026: focused integration **82 passed, 4 deselected**
trong Docker Linux, gồm 12 cases P3 và hồi quy Storage; lint/format qua. Đã kiểm
tra roundtrip 1 byte/2 MiB, missing chunk, actual hash sau sửa disk, delete lặp,
accounting và lỗi đọc/xóa. Không chạy PostgreSQL bootstrap/full suite/cluster
smoke/restart ở P3; chỉ rebuild image tests. Runtime containers chưa được cập
nhật trong lượt này. P4–P6 còn lại; M1 chưa hoàn thành.

P4 đã xác minh ngày 03/10/2026: focused integration **98 passed, 4 deselected**
trong Docker Linux, gồm 16 cases P4 và hồi quy Storage; lint/format qua. Không
chạy DB tests/full suite/cluster smoke/restart. Runtime containers chưa cập nhật;
chỉ image tests được rebuild. P5–P6 còn lại; M1 chưa hoàn thành.

## Smoke Storage M1 qua restart thật

Từ repo root, Docker đang sẵn sàng. Chọn manifest mới cho mỗi lượt; giữ manifest
qua restart. Lệnh dưới chỉ cập nhật node-1/node-2, giữ named volumes và không chạy
migration hay restart Metadata. Client chạy trong network Compose.

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools build tests storage-node-1 storage-node-2
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --no-deps --wait storage-node-1 storage-node-2
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm --no-deps tests python -m pytest -q tests/test_storage_p5.py

$smokeManifest = "/tmp/dfs-smoke-$([guid]::NewGuid().ToString()).json"
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec -T storage-node-1 python scripts/smoke_storage.py store --state $smokeManifest --target storage-node-1:50051 --node-id node-1
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec -T storage-node-1 python scripts/smoke_storage.py verify --state $smokeManifest
docker compose --env-file deploy/.env -f deploy/compose.local.yml restart storage-node-1
docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --no-deps --no-recreate --wait storage-node-1
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec -T storage-node-1 python scripts/smoke_storage.py verify --state $smokeManifest
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec -T storage-node-1 python scripts/smoke_storage.py absent --state $smokeManifest --target storage-node-2:50052 --node-id node-2
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec -T storage-node-1 python scripts/smoke_storage.py delete --state $smokeManifest
```

Chạy từng bước và dừng khi lỗi; không tiếp tục restart/xóa khi verify thất bại.
Manifest chứa chunk UUID đã thử gửi để kiểm tra/cleanup đúng fixture. Không xóa
cả volume. Manifest ở `/tmp` giữ qua `restart`, không giữ khi recreate container;
lưu bản riêng nếu cần giữ qua recreate. Metrics baseline yêu cầu không có client
khác ghi/xóa trên node trong lượt smoke.

P5 đã xác minh ngày 03/10/2026: hai lifecycle scenarios pass (restart process
pass lần đầu; crash case pass sau sửa harness và rerun riêng), smoke 2 MiB trên
named volume qua toàn bộ luồng trên, lint/format qua. Fixture đã xóa và used_bytes
về baseline 0. Node-1/node-2 hiện dùng code mới; Metadata/node-3 chưa cập nhật.
Không rerun P1–P4/DB/full suite/Metadata smoke ở P5. Còn P6 trước khi hoàn thành M1.

P6 đã xác minh ngày 03/10/2026: **106 passed**, không skipped/deselected trong
Docker Linux, gồm PostgreSQL bootstrap và lifecycle tests. Ruff check/format
(29 files) qua; `alembic check` không drift; base smoke HTTP/DB/registry/cả ba node
qua. Metadata và ba Storage node đã cập nhật build, named volumes giữ nguyên.
Một warning Starlette/httpx đã biết. Không chạy frontend, replication/failover
hay demo hai máy trong M1. Bàn giao tiếp theo và giới hạn đã ghi trong
[M1 phase plan](docs/M1_IMPLEMENTATION_PLAN.md#bằng-chứng-p6-và-bàn-giao-m2--03102026).

