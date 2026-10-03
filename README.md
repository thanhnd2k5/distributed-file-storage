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

M1 P1 đã có validation UUID/SHA-256/payload và startup filesystem. Chunk committed
dùng `<canonical_uuid>.chunk`; tempfile Store dùng `.store-<32 lowercase hex>.tmp`
trong cùng DATA_DIR. Startup chỉ dọn regular tempfiles đúng mẫu, giữ file lạ,
khởi tạo used_bytes từ committed chunks và fail nếu limit nhỏ hơn chunk đã lưu.
Mỗi DATA_DIR chỉ dành cho một Storage process; dừng process cũ trước khi chạy mới.

StoreChunk/GetChunk/DeleteChunk trả INVALID_ARGUMENT khi input sai; request
hợp lệ vẫn trả gRPC UNIMPLEMENTED đến P2/P3. Chưa có upload,
download, delete, health polling hay cleanup worker/repair. Startup recovery chỉ
đánh dấu upload gián đoạn FAILED và giữ cleanup_pending trong DB; worker xử lý
chúng thuộc các milestone sau. Node registry khởi tạo DOWN đến khi health polling
được triển khai. Chưa đánh dấu M1/M2 hoàn thành.

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
- gRPC node-1/2/3: localhost:50051/50052/50053.

`ready` kiểm tra DB và startup initialization; không yêu cầu mọi node ACTIVE.
`live` vẫn hoạt động nếu DB mất kết nối sau startup. Cấu hình sai fail ngay khi
khởi động; lỗi DB/schema lúc initialize giữ readiness 503, cần restart sau khi sửa.

```powershell
docker compose --env-file deploy/.env -f deploy/compose.local.yml logs --tail 100 metadata
docker compose --env-file deploy/.env -f deploy/compose.local.yml stop
docker compose --env-file deploy/.env -f deploy/compose.local.yml start --wait
```

Stop/start giữ nguyên bốn named volumes. Compose local dùng cùng `dev_host`;
demo hai máy và các file Compose A/B sẽ làm ở M6.

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
docker compose --env-file deploy/.env -f deploy/compose.local.yml --profile tools run --rm tests
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec metadata alembic check
docker compose --env-file deploy/.env -f deploy/compose.local.yml exec metadata python scripts/smoke_base.py
```

Tests kiểm tra config sai, duplicate identity/RF, payload proto 2 MiB, health RPC
thật và data RPC chưa implement, cùng live/ready khi DB không kết nối được.
Smoke kiểm tra schema bốn bảng, registry và identity/writable của cả ba node.
Các failure checks cho lưu file/replication thuộc M1–M4.

Đã xác minh ngày 03/10/2026: 7 tests qua trong Docker Linux, lint/format qua,
`alembic check` không có schema drift, smoke cả ba node qua trước/sau restart
Metadata, và script setup chạy được với Python 3.12.14. Integration tests dùng
schema PostgreSQL riêng và tự dọn, không ghi vào bảng dữ liệu của ứng dụng.

Trong phiên thiết lập này, test gRPC loopback native Windows bị timeout; nguyên
nhân chưa xác định. Docker Linux là đường chạy đã được kiểm chứng. Bộ test hiện
có một deprecation warning từ Starlette khi dùng httpx, không ảnh hưởng kết quả.

P1 đã xác minh ngày 03/10/2026: 58 tests qua trong Docker Linux (7 checks trước
đó và 51 cases P1), lint/format qua. Tiến độ chi tiết:
[M1 phase plan](docs/M1_IMPLEMENTATION_PLAN.md).

Sau khi gom Python vào `backend/`, đã kiểm tra lại setup/venv, proto generation,
Docker build, 58 tests, lint/format và smoke cluster: đều qua. Các named volumes
và tên service Compose giữ nguyên.

