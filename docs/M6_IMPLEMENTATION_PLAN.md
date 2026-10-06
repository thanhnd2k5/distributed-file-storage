# M6 — Hai máy và rehearsal

**Ngày lập:** 05/10/2026  
**Trạng thái:** P1–P5 hoàn thành 06/10/2026. Bring-up + rehearsal hai host LAN
(commit `fa1c52b`, `MACHINE_B_HOST=172.16.11.255`) đã qua; docs/evidence khớp.  
**Đầu vào:** M5 hoàn thành P1–P7; [bàn giao M6](M5_IMPLEMENTATION_PLAN.md#13-bàn-giao-m6--hai-máy-và-rehearsal).  
**Đầu ra:** cùng code/build chạy trên hai host LAN; ngắt B vẫn đọc khi còn replica trên A;
repair khi còn đủ destination; setup/demo guide khớp lệnh thật.

## 1. Phạm vi

Đọc [ARCHITECTURE §10](ARCHITECTURE.md#10-triển-khai-hai-máy), [IMPLEMENTATION_PLAN](IMPLEMENTATION_PLAN.md)
mục M6, và bàn giao M5 mục 13 trước khi chạy.

| Có trong M6 | Không mở rộng |
|---|---|
| `compose.machine-a.yml` / `compose.machine-b.yml` | Auth, WebSocket, auto-repair |
| Env LAN, CORS, publish gRPC B | Queue/Redis/Kafka/K8s |
| Preflight IP/port/firewall | Đổi RF=1 hoặc bỏ demo hai máy |
| Rehearsal failure-domain + repair | Framework E2E mới |
| Demo guide / evidence | Metadata HA |

Topology cố định:

```text
Máy A (machine_A)          Máy B (machine_B)
├─ React :5173             ├─ Storage Node 2 :50052
├─ Metadata/API :8000      └─ Storage Node 3 :50053
├─ PostgreSQL (không LAN)
└─ Storage Node 1 :50051
```

Metadata trên A gọi node-1 bằng tên Docker; gọi node-2/3 bằng `MACHINE_B_HOST`
(LAN IP của B). Tắt A = mất API (SPOF đã chấp nhận). Tắt B vẫn có thể đọc nếu
mọi chunk còn replica trên node-1; lúc đó không restore đủ RF=2 cho đến khi có
destination khác.

## 2. Phase và gate

Làm tuần tự **P1 → P2 → P3 → P4 → P5**. P1–P2 làm một mình trước khi ngồi chung;
P3–P4 cần hai máy cùng LAN; P5 chốt docs/evidence.

| Phase | Đầu ra | Điều kiện chuyển | Ai / khi |
|---|---|---|---|
| P1 — Compose A/B + env | File compose/env example + plan | `docker compose … config` qua; không claim hai host | Một người, trước rehearsal |
| P2 — Preflight scripted | Checklist IP/port/firewall/git commit | Hai máy có cùng commit; lệnh copy-paste sẵn | Một người soạn; đối tác chỉ làm theo |
| P3 — Bring-up hai host | 3 node ACTIVE, 2 domain | ready=200; cluster hiện `machine_A` + `machine_B` | Ngồi chung |
| P4 — Rehearsal demo | 4 tình huống pass | Upload/placement → tắt B đọc được → repair 1 node B → bật lại B | Ngồi chung |
| P5 — Docs/evidence | README + evidence P3–P4 | Phân biệt local `dev_host` vs hai host; không suy ra từ M5 | Sau rehearsal |

Ước lượng: P1–P2 ≈ 1 buổi chuẩn bị; P3–P4 ≈ 1 buổi ngồi chung; P5 ngắn.

### P1 — Compose A/B và env (hoàn thành)

1. `deploy/compose.machine-a.yml`: postgres + metadata + node-1; domain `machine_A`.
2. `deploy/compose.machine-b.yml`: node-2 + node-3; domain `machine_B`; bind `0.0.0.0`.
3. `deploy/.env.machine-a.example` / `.env.machine-b.example`.
4. `STORAGE_NODES_JSON` dùng `${MACHINE_B_HOST}`; không hard-code localhost cho B.
5. Image tag chung `dfs-python:demo`; cùng Dockerfile local.
6. Validate: `docker compose … config` (không cần máy B thật).

### P2 — Preflight cho buổi ngồi chung

Script: [`deploy/preflight-m6.ps1`](../deploy/preflight-m6.ps1). Env thật
`deploy/.env.machine-a` / `.env.machine-b` nằm trong `.gitignore`.

**Một mình trước (Check):**

```powershell
powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role Check
```

In HEAD git, Docker Linux, `compose config` A/B từ example, liệt kê IPv4 LAN.

**Khi ngồi chung — Máy B:**

```powershell
powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role B
# Admin nếu cần mở firewall:
powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role B -EnsureFirewall
```

Gửi một IPv4 in ra cho Máy A. Rồi compose up machine-b.

**Khi ngồi chung — Máy A** (sau khi có IP B):

```powershell
powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role A -MachineBHost <IP_B>
# Sau khi B đã up:
powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role A -MachineBHost <IP_B> -ProbePorts
```

Checklist gate P2 (script + thao tác tay):

1. Hai máy cùng `git` HEAD (script in short SHA — đối chiếu miệng).
2. B: IPv4 LAN + inbound TCP 50052/50053 (`-EnsureFirewall` hoặc thủ công).
3. A: `MACHINE_B_HOST` trong `deploy/.env.machine-a` (không localhost).
4. Cùng Wi-Fi/hotspot; tắt AP isolation nếu có.
5. A→B: `-ProbePorts` thành công **sau** B compose up (thuộc ranh giới P2→P3).

P2 chốt khi script Check qua và lệnh A/B sẵn sàng copy-paste; probe LAN
thật có thể đợi phút đầu buổi ngồi chung.

### P3 — Bring-up

**Máy B trước** (để A health thấy node sớm):

```powershell
Copy-Item deploy/.env.machine-b.example deploy/.env.machine-b
docker compose --env-file deploy/.env.machine-b -f deploy/compose.machine-b.yml up -d --build --wait
docker compose --env-file deploy/.env.machine-b -f deploy/compose.machine-b.yml ps
```

**Máy A:**

```powershell
Copy-Item deploy/.env.machine-a.example deploy/.env.machine-a
# Sửa MACHINE_B_HOST = IPv4 thật của B
docker compose --env-file deploy/.env.machine-a -f deploy/compose.machine-a.yml up -d --build --wait
curl.exe --fail-with-body http://localhost:8000/api/v1/health/ready
curl.exe --fail-with-body http://localhost:8000/api/v1/cluster
curl.exe --fail-with-body http://localhost:8000/api/v1/nodes
```

**UI trên A:**

```powershell
Set-Location frontend
Copy-Item .env.example .env.local   # VITE_STORAGE_API_URL=http://localhost:8000/api/v1
npm ci
npm run dev -- --host localhost --port 5173 --strictPort
```

Gate P3: ready 200; 3 node ACTIVE; `active_failure_domains` / configured hiện 2 domain
(hoặc UI Cluster panel tương đương).

### P4 — Rehearsal checklist

Làm đúng thứ tự; mỗi bước ghi pass/fail + quan sát UI/API.

| # | Tình huống | Thao tác | Kỳ vọng |
|---|---|---|---|
| R1 | Placement khác domain | Upload file ~4–10 MiB trên UI A | Detail: mỗi chunk RF=2; replica trên `machine_A` và `machine_B` |
| R2 | Host B down vẫn đọc | Trên B: `docker compose … stop` (hoặc tắt Wi-Fi B) | Node 2/3 DOWN; download file R1 vẫn OK từ node-1 |
| R3 | Repair một node B | Bật lại B; stop **chỉ** node-2; xoá/làm hỏng replica nếu cần theo demo; repair scoped | Có destination node-3; repaired hoặc HEALTHY; không hot-loop |
| R4 | Bật lại / mapping | Start lại node-2; refresh cluster/detail | Node ACTIVE; có thể OVER_REPLICATED — giữ, không tự prune |

Sau R2 nói rõ: chỉ còn một node thì **không** hứa repair đủ RF=2.  
Sau cả buổi: dọn fixture upload qua DELETE API (ownership rehearsal).

### P5 — Docs và evidence

Cập nhật README lệnh hai máy, evidence P3–P4 thật, phân biệt M5 local
`dev_host` vs M6 hai domain. Không claim rehearsal pass nếu chưa chạy R1–R4.

## 3. DoD

- [x] P1: Compose A/B + env examples; `compose config` qua (05/10/2026)
- [x] P2: preflight script + checklist; Check role qua (05/10/2026); IP/firewall/probe LAN khi ngồi chung
- [x] P3: bring-up hai host, 3 ACTIVE, 2 failure domains (06/10/2026)
- [x] P4: R1–R4 rehearsal pass với quan sát ghi lại (06/10/2026)
- [x] P5: README/docs/evidence khớp; không suy ra từ local-only (06/10/2026)

## 4. Evidence P1 — 05/10/2026

**Tier:** no-execution cho docs + targeted `docker compose config` (không bring-up
hai host, không claim failure-domain thật).

**Files:**

- `deploy/compose.machine-a.yml`
- `deploy/compose.machine-b.yml`
- `deploy/.env.machine-a.example`
- `deploy/.env.machine-b.example`
- plan này

**Commands:**

```powershell
docker compose --env-file deploy/.env.machine-a.example -f deploy/compose.machine-a.yml config --quiet
docker compose --env-file deploy/.env.machine-b.example -f deploy/compose.machine-b.yml config --quiet
```

Cả hai exit 0. Interpolate kiểm tra: `MACHINE_B_HOST` → host node-2/3;
`failure_domain` `machine_A` / `machine_B`; CORS `http://localhost:5173`.

**Cố ý chưa chạy (tại P1):** `up` hai máy, port/firewall LAN, upload/repair
cross-host, UI rehearsal — đã chạy ở P3–P4 ngày 06/10/2026.

## 5. Evidence P2 — 05/10/2026

**Tier:** targeted script + compose config (không bring-up hai host, không claim
probe LAN thật giữa hai máy).

**Files:** `deploy/preflight-m6.ps1`; `.gitignore` thêm `.env.machine-a` /
`.env.machine-b`; plan P2 cập nhật.

**Commands:**

```powershell
powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role Check
powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role B
```

Check: git HEAD in ra, Docker Linux, compose config A/B example qua, liệt kê
IPv4 LAN trên host hiện tại. Role B: tạo `deploy/.env.machine-b` nếu thiếu,
compose config machine-b qua, in IP candidates; chưa `-EnsureFirewall` /
`-ProbePorts` (cần Admin + máy B thật khi ngồi chung).

**Cố ý chưa chạy (tại P2):** Role A với IP B thật, firewall Admin, ProbePorts,
compose up hai host, rehearsal R1–R4 — đã chạy ở P3–P4 ngày 06/10/2026.

## 6. Evidence P3 — 06/10/2026

**Tier:** focused integration hai host thật (Compose A/B LAN), không claim UI
Vite trong gate này; verify qua REST ready/cluster/nodes.

**Môi trường:** commit `fa1c52b` trên cả hai máy; Máy A Wi‑Fi `172.16.12.15`
(mask `/21`); Máy B `MACHINE_B_HOST=172.16.11.255`. Trước up A: dừng stack
local `compose.local` vì chiếm `127.0.0.1:50051` / `:8000`.

**Commands (rút gọn):**

```powershell
powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role A -MachineBHost 172.16.11.255 -ProbePorts
# OK  172.16.11.255:50052 / :50053 reachable
docker compose --env-file deploy/.env.machine-a -f deploy/compose.machine-a.yml up -d --wait --no-build
curl.exe --fail-with-body http://localhost:8000/api/v1/health/ready
curl.exe --fail-with-body http://localhost:8000/api/v1/cluster
curl.exe --fail-with-body http://localhost:8000/api/v1/nodes
```

**Quan sát gate:** `ready` → `READY`; nodes `active:3`;
`configured_failure_domains:2`, `active_failure_domains:2`; node-1
`machine_A`, node-2/3 host `172.16.11.255` `machine_B`, cả ba `ACTIVE`.

**Khác M5 local:** Compose local một `dev_host` (process failure only) không
thay bằng bằng chứng này. P3 chỉ claim topology hai domain LAN thật.

## 7. Evidence P4 — 06/10/2026

**Tier:** focused rehearsal R1–R4 trên cùng bring-up P3 (API trên Máy A;
stop/start container trên Máy B do đối tác).

| # | Thao tác | Quan sát |
|---|---|---|
| R1 | Upload 6 MiB `POST /files` | `file_id=59744f5e-…`, 3 chunks RF=2; mỗi chunk `live_failure_domain_count=2` (`machine_A`+`machine_B`) |
| R2 | B: `compose … stop` cả stack | node-2/3 `DOWN`; download SHA-256 khớp nguồn từ replica node-1; `under_replicated_chunks:3` |
| R3 | B start lại; `stop storage-node-2` | node-2 `DOWN`, node-3 `ACTIVE`; `POST /admin/repair` scoped `file_id`+`node_id=node-2`: `repaired_replicas:1`, `outcome=REPAIRED` → chunk 0 có VERIFIED trên node-3; `under_replicated=0` |
| R4 | B: `start storage-node-2` | 3 node `ACTIVE`; chunk 0 `live=3` / `over_replicated=true` (giữ, không prune); download SHA vẫn khớp |

Không claim: tắt cả B rồi repair đủ RF=2 (không có destination); auto-repair;
UI Vite trong lượt này (REST đủ gate).

## 8. Evidence P5 — 06/10/2026

**Tier:** no-execution docs. Cập nhật plan này, [README](../README.md) mục Demo
hai máy, [docs/README](README.md) trạng thái M6. Phân biệt rõ M5 `dev_host`
local vs M6 `machine_A`/`machine_B` LAN.

## 9. Cheat-sheet mang đi

### Preflight

1. Hai máy `git pull` / cùng commit (đối chiếu short SHA từ preflight).
2. Docker Desktop Linux containers chạy.
3. Cùng mạng; B chạy `preflight-m6.ps1 -Role B` và gửi IPv4.
4. A chạy `preflight-m6.ps1 -Role A -MachineBHost <IP>` rồi `-ProbePorts` sau khi B up.

### Thứ tự start

1. B: `-EnsureFirewall` (Admin) nếu cần → compose machine-b up  
2. A: `-ProbePorts` → compose machine-a up → ready + nodes  
3. A: Vite 5173 (tuỳ chọn demo UI)  
4. Cluster: 3 ACTIVE, 2 failure domains

### Demo nói gì

1. Client chỉ nói REST với Metadata — location transparency.  
2. Chunk RF=2, ưu tiên khác domain.  
3. Tắt B: vẫn đọc nếu còn replica A; Metadata trên A là SPOF.  
4. Repair thủ công qua Metadata, không node-to-node RPC.

### Lệnh dừng nhanh

```powershell
# Máy A
docker compose --env-file deploy/.env.machine-a -f deploy/compose.machine-a.yml stop
# Máy B
docker compose --env-file deploy/.env.machine-b -f deploy/compose.machine-b.yml stop
```
