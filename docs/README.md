# Distributed File Storage — bộ đặc tả V1

**Hướng phát triển mới (21/10/2026):** [Thiết kế V2](V2_DESIGN.md) đề xuất xây
lại kiến trúc cho tệp lớn, upload có thể tiếp tục, nhiều instance và phục hồi
sự cố. V2 là kho dùng chung, không tài khoản/phân quyền người dùng; trọng tâm
là luồng lưu trữ phân tán. Tài liệu V2 là thiết kế đích, chưa phải runtime đã triển khai; bộ V1
bên dưới vẫn mô tả hệ thống hiện tại.

**Cập nhật V2 (22/10/2026):** chốt hash nguồn trước upload/resume, streaming bytes,
fencing tại Storage, sổ dung lượng và chế độ phục hồi DB. Reconcile/cleanup cơ bản
và kiểm chứng hai instance được đưa vào B–C; xem các mục 2.1, 4.1–4.2, 6.1–6.2,
7.1 và 9.1 của thiết kế V2. Đây là quyết định thiết kế, chưa có evidence runtime.

**Mốc A V2 (22/10/2026):** đã có baseline [API](V2_API_CONTRACTS.md),
[schema logic](V2_METADATA_SCHEMA.md), [state machines/ADR và crash matrix](V2_CORRECTNESS.md)
và [protobuf V2](../backend/contracts/v2_storage.proto). Wire compile/import và
targeted Ruff đã qua trên native Windows; đây là evidence contract, chưa phải
runtime V2. Gate/evidence và bàn giao mốc B ở cuối [thiết kế V2](V2_DESIGN.md).

**Mốc B V2 (23/10/2026):** hoàn tất B1–B4 trong phạm vi foundation Storage
ledger/streaming/gRPC, metadata/session/capacity và hook maintenance/recovery.
Xem [phase/evidence B](V2_B_IMPLEMENTATION_PLAN.md); B4 bổ sung 4 case liên kết
PostgreSQL/gRPC/ledger cho incarnation, ENOSPC và epoch. Chưa có HTTP/Transfer
upload/resume/finalize hoặc restore backup toàn cluster. Tiếp theo
[mốc C — upload streaming/resume](V2_C_IMPLEMENTATION_PLAN.md). Cấu hình DB test
riêng không publish host port và cách chạy ở [V2 local tests](V2_LOCAL_TESTS.md).

**C1 V2 (23/10/2026):** hoàn tất transaction renew/confirm/reconcile/retry,
complete/manifest và finalize publication ở tầng domain. 59 ca C1 và 17
regression B3 khác nhau đã pass qua các lượt focused PostgreSQL; không cộng
rerun thành full suite. Đã sửa race job lease hết hạn trong lúc chờ row lock.
Xem [evidence và bàn giao C2](V2_C_IMPLEMENTATION_PLAN.md#6-evidence-c1--23102026).
Tiếp theo Worker reconcile/cleanup/finalize; C chưa có HTTP/Transfer E2E.

**C2 V2 (24/10/2026):** hoàn tất Worker riêng reconcile/cleanup/finalize với
bounded jobs/finalize/stream buffer, renew/retry và shutdown. 44 ca C2 khác nhau
qua các lượt focused PostgreSQL + gRPC/filesystem, gồm 10 SIGKILL commit boundaries
và entrypoint SIGTERM; 31 direct C1/B3 regressions pass. Không cộng rerun thành
full suite. Xem [evidence C2 và bàn giao C3](V2_C_IMPLEMENTATION_PLAN.md#8-evidence-c2--24102026).
Tiếp theo C3 API/Transfer; chưa có HTTP upload E2E hoặc benchmark tệp lớn.

**C3 V2 (24/10/2026):** hoàn tất API upload public/internal trên listener riêng,
Transfer streaming bounded queue/deadline/renew và lifecycle/readiness từng role.
28 ca C3 khác nhau + 2 direct create/admission regressions pass qua các lượt
focused HTTP/PostgreSQL/gRPC/filesystem; có Nginx thật, lost response, retry
handoff, SIGTERM/resume và complete tới publish/abort cleanup. Không cộng rerun
thành full suite. Xem [evidence C3 và bàn giao C4](V2_C_IMPLEMENTATION_PLAN.md#10-evidence-c3--24102026)
và [runtime/env/ingress](V2_C3_RUNTIME.md). Tiếp theo **C4 client pre-hash/resume**;
chưa benchmark 1–10 GiB/RSS hoặc chạy hai API/hai Transfer C5.

**C4 V2 (25/10/2026):** đã có [client tối thiểu `/uploads-v2`](V2_C4_CLIENT.md),
incremental SHA-256 trong Web Worker, journal/key bền vững trên browser,
resume đúng nguồn và parts paging, complete/poll cùng pause/abort/cleanup riêng.
18 nhóm kiểm browser/controller/API thật pass; PostgreSQL/gRPC/Nginx và đọc lại
bytes hai replica xác nhận 10 file AVAILABLE. ESLint/build/Ruff passed. Native
file picker/click-flow UI đầy đủ chưa xác nhận do browser automation lỗi; xem
[evidence và giới hạn C4](V2_C_IMPLEMENTATION_PLAN.md#12-evidence-c4--25102026).
Tiếp theo **C5 nhiều instance, crash và tài nguyên**; mốc C chưa hoàn tất.

**C5 bước đầu (26/10/2026):** gate file nhỏ trên hai process API public, hai
internal và hai Transfer qua Nginx round robin đã có evidence. Ba ca focused
integration qua hai lượt kiểm empty/biên chunk, restart/resume và SIGKILL giữa
stream với lease DB thật, generation fencing và abort/cleanup; sáu file AVAILABLE
được đọc lại bytes/hash trên cả hai replica. Ruff passed. Xem
[evidence C5](V2_C_IMPLEMENTATION_PLAN.md#14-evidence-c5-bước-đầu--26102026)
và [cách chạy](V2_LOCAL_TESTS.md#nhiều-instance-c5--gate-file-nhỏ).
Còn client C4 trên topology này, 1–10 GiB/RSS/backpressure và crash matrix bổ sung;
**C5 và mốc C chưa hoàn tất**.

**D1 V2 (28/10/2026):** theo chỉ đạo người dùng, tạm gác gate browser C5 để
làm phần download độc lập. Đã có logic HTTP thuần cho GET/HEAD, Range,
conditional ETag và attachment header an toàn; **121 unit tests** và Ruff
hai file passed trên Windows. Chưa nối route/streaming/read lease;
**D chưa hoàn tất**. Xem [kế hoạch và evidence D1](V2_D_IMPLEMENTATION_PLAN.md),
tiếp theo D2 read/delete lease. Gate C5 còn thiếu giữ nguyên trong kế hoạch C.

**D2 V2 (28/10/2026):** hoàn tất domain acquire/renew/release read lease,
immutable manifest paging và file tombstone/cleanup; không thay schema foundation.
57 ca D2 và 20 direct regressions pass trên Docker Linux/PostgreSQL, gồm hai
connection cạnh tranh và Worker/gRPC/bytes thật cho cleanup chờ người đọc;
Ruff check/format hai file qua native Windows. Xem
[evidence D2](V2_D_IMPLEMENTATION_PLAN.md#4-evidence-d2--28102026). Tiếp theo
**D3 API metadata/read commands/DELETE**; chưa có HTTP download streaming/Range,
benchmark hoặc D5. C5 vẫn tạm gác, mốc C và D chưa hoàn tất.

**D3 V2 (28/10/2026):** hoàn tất public catalog/metadata/chunks/DELETE và
internal acquire/manifest/renew/release; Transfer HEAD download qua ingress,
không read lease/Storage I/O. 55 ca D3 và 13 direct C3 regressions pass trên
HTTP/PostgreSQL/Nginx thật, gồm cleanup chờ người đọc với gRPC/bytes hai replica;
Ruff check/format sáu file qua. Xem
[evidence D3](V2_D_IMPLEMENTATION_PLAN.md#5-evidence-d3--28102026). Tiếp theo
**D4 GET download streaming/Range**; chưa có binary download HTTP, benchmark
hoặc D5. C5 vẫn tạm gác, mốc C và D chưa hoàn tất.

**D4 V2 (29/10/2026):** GET binary/Range qua Transfer–Storage đã có one-chunk
verify, fallback hữu hạn, renew read lease và cancellation/backpressure.
60 ca D4 riêng biệt qua các lượt chọn lọc trên Docker Linux/PostgreSQL,
HTTP/Nginx/gRPC/SQLite/bytes thật; blocked-send dùng ASGI có điều phối.
23 direct regressions và Ruff check/format năm file đã pass.
Xem [evidence D4](V2_D_IMPLEMENTATION_PLAN.md#6-evidence-d4--29102026).
Tiếp theo **D5 nhiều instance, read/delete race và budget tải hỗn hợp**;
chưa có benchmark TTFB/read amplification/RSS. C5 vẫn tạm gác, mốc C và D chưa hoàn tất.

**D5 V2 (29/10/2026):** baseline mốc D đã hoàn tất với Nginx, hai API/hai Transfer
và Storage/Worker process độc lập. Sáu ca D01–D04 pass; hai ca race được siết
assertion và chạy lại riêng pass, Ruff hai files pass. Range/read-delete/fallback
và bốn download 64 MiB cùng upload/finalize có evidence bytes/lease/buffer/RSS thật.
Transfer RSS mixed phase khoảng 104–105 MiB/process trong fixture cap 3 GiB.
Xem [D5 evidence](V2_D5_EVIDENCE.md) cho topology, lệnh và giới hạn phép đo.
Tiếp theo mốc **E admission/capacity/fairness**; C5/file 1–10 GiB vẫn tạm gác,
mốc C chưa hoàn tất. Nhiều process cùng host chưa phải HA.

**Fixes review trước E (29/10/2026):** đã sửa cả bốn findings: reader chung
allocation, control HTTP deadline/cancellation, duration float và tracing cho
admission. **82 ca Linux liên quan pass** (32 mới, 44 hồi quy, 6 D5), Ruff bảy file
Python pass. D5 đã đổi sang bốn reader cùng file 64 MiB, hai Transfer/RF=2; sampled
RSS khoảng 100–102 MiB/process trong fixture. Xem
[evidence fixes](V2_PRE_E_FIXES.md). E chưa bắt đầu; C5/file lớn vẫn tạm gác.

**E1 V2 (29/10/2026):** HTTP/control admission có reservation hai chiều cho
new work/progress trong cùng tổng cap; mặc định 32/8 cho phép tối đa 24 mỗi nhóm,
giữ ít nhất 8 slot cho nhóm kia, không queue. **90 ca liên quan pass**: 40 policy
trên Windows, 4 integration mới + 45 direct regressions + 1 mixed-load nhiều
process trên Docker Linux/PostgreSQL; Ruff check/format sáu file pass. Xem
[contract, commands, evidence và bàn giao E2](V2_E_IMPLEMENTATION_PLAN.md).
Phạm vi E1 là admission theo process; C5/file lớn vẫn tạm gác.

**E2 V2 (30/10/2026):** hoàn tất audit durable session/claim/node accounting.
**22 integration pass** trên Docker Linux/PostgreSQL/POSIX thật: hai process
tranh quota, crash trước/sau commit, mất Store/Delete ACK và restart; không
double grant/release, chỉ durable Delete ACK hợp lệ trả node charge. Ruff hai
file mới pass; không sửa runtime/schema. Chốt chưa thêm durable grant download/
job cho topology hữu hạn; khi tăng instance phải cộng các cap mỗi process và
internal retry lanes. Xem [audit, evidence và bàn giao E3](V2_E_IMPLEMENTATION_PLAN.md#contract-và-audit-e2--30102026).
Phạm vi E2 là durable accounting; C5/file lớn vẫn tạm gác.

**E3 V2 (30/10/2026):** Worker luân phiên cleanup/reconcile/finalize, nhường lượt
sau chunk/allocation/RPC và giữ tối đa 2 continuation/kind (mặc định 6 lease,
2 quantum chạy cùng lúc). Shared read budget 64 MiB/s/Worker được cấp trước
GetChunk; paused lease/deadline/fencing và bounded shutdown giữ đúng. **77 ca
liên quan pass**: 16 native, 11 integration mới, 50 Worker/job/HTTP handoff
regressions; Ruff năm file pass. Xem
[contract và evidence E3](V2_E_IMPLEMENTATION_PLAN.md#contract-e3--worker-fairness-và-io).
Trạng thái hiện tại: **E1–E3 hoàn tất baseline; E4 và mốc E chưa hoàn tất**;
C5/browser/file lớn vẫn tạm gác.

**E4 V2 (30/10/2026):** gate 1/4/16 active session và gate 16 stream đồng thời
đã pass trên topology hai API, hai internal, hai Transfer, hai Storage và một
Worker. Gate 16 stream kiểm 429, delete chờ 16 read lease và cleanup trả charge
về 0; mixed-load mặc định cũng pass. Xem [evidence và giới hạn E4](V2_E_IMPLEMENTATION_PLAN.md#e4-16-byte-stream-và-deletecleanup--30102026).
Các gate đã pass ở lượt trước; lượt 1/4/16 sau thay đổi fixture đang chờ xác minh
vì PostgreSQL test chưa chạy. Browser C5, file lớn và HA nhiều host chưa nghiệm thu.

**Đặc tả 02/10/2026 · Một người code · M0–M2 hoàn thành 03/10/2026, M3–M4 hoàn thành 04/10/2026.**

## Đọc theo thứ tự

1. `PROJECT_OVERVIEW.md`: phạm vi, mục tiêu và giới hạn môn học.
2. `ARCHITECTURE.md`: thuật toán, trạng thái, failure handling và triển khai.
3. `API_CONTRACTS.md`: REST schema, gRPC semantics và error codes.
4. `storage.proto`: wire contract có thể generate stub Python.
5. `IMPLEMENTATION_PLAN.md`: thứ tự code và checks trước demo.

Kế hoạch milestone: [M1 — Storage Node thật](M1_IMPLEMENTATION_PLAN.md) đã hoàn
thành; [M2 — Metadata quan sát ba node](M2_IMPLEMENTATION_PLAN.md) đã hoàn thành
P1–P6. [M3 — Upload/download RF=2](M3_IMPLEMENTATION_PLAN.md) đã hoàn thành
P1–P7 ngày 04/10/2026; Metadata M3 deploy local, lifecycle/crash và smoke RF=2
thật đã qua. [M4 — failure/DELETE/cleanup/repair](M4_IMPLEMENTATION_PLAN.md)
hoàn thành P1–P7; build local/repair/delete smoke đã qua và fixtures đã dọn đúng
ownership. [M5 — UI tối thiểu](M5_IMPLEMENTATION_PLAN.md) đã hoàn thành P1–P7
ngày 05/10/2026. **P1 hoàn thành**: client V1/
public shell, lint/build và 19 focused browser checks qua. **P2 hoàn thành**:
lint/build, 10 automated checks mô phỏng cùng UI/API thật và gate sau phục hồi
qua. **P3 hoàn thành**: upload/download file nhiều chunk và rỗng qua UI/API
thật, SHA-256 Blob trùng nguồn; 18 controlled checks qua. **P4 hoàn thành
05/10/2026:** lint/build, 14 controlled checks/3 selected P3 regressions qua;
delete thật ba fixture P3 với 202→read 404/200 zero pending. **P5 hoàn thành
05/10/2026:** manual repair filters/cursor/outcomes qua targeted lint/build,
controlled checks và scan scoped thật ba lượt max_chunks=2, remaining=4→2→0.
**P6 hoàn thành 05/10/2026:** gate UI/API runtime local — fallback download/
repair khi node-2 DOWN, offline DELETE→404, 409 OPERATION_BUSY; owned fixtures
đã dọn; không claim hai host.
**M5 hoàn thành P1–P7 (05/10/2026).** P7 docs-only đối chiếu DoD và chốt
[bàn giao M6](M5_IMPLEMENTATION_PLAN.md#13-bàn-giao-m6--hai-máy-và-rehearsal).
**M6 hoàn thành P1–P5 (06/10/2026):** [M6 — Hai máy và rehearsal](M6_IMPLEMENTATION_PLAN.md)
— Compose A/B, preflight, bring-up LAN 3 ACTIVE / 2 domain, rehearsal R1–R4
(tắt B vẫn đọc; repair scoped; OVER_REPLICATED giữ). Không suy từ M5
`dev_host` local. Chi tiết
[evidence P3](M6_IMPLEMENTATION_PLAN.md#6-evidence-p3--06102026),
[evidence P4](M6_IMPLEMENTATION_PLAN.md#7-evidence-p4--06102026),
[evidence P5](M6_IMPLEMENTATION_PLAN.md#8-evidence-p5--06102026).
Chi tiết [evidence P1](M5_IMPLEMENTATION_PLAN.md#7-evidence-p1--04102026).
Chi tiết: [evidence P2](M5_IMPLEMENTATION_PLAN.md#8-evidence-p2--04102026).
Chi tiết: [evidence P3](M5_IMPLEMENTATION_PLAN.md#9-evidence-p3--04102026).
Chi tiết: [evidence P4](M5_IMPLEMENTATION_PLAN.md#10-evidence-p4--05102026).
Chi tiết: [evidence P5](M5_IMPLEMENTATION_PLAN.md#11-evidence-p5--05102026).
Chi tiết: [evidence P6](M5_IMPLEMENTATION_PLAN.md#12-evidence-p6--05102026).
Chi tiết: [evidence P7](M5_IMPLEMENTATION_PLAN.md#14-evidence-p7--05102026).
Handoff M4 gốc:
[bàn giao M5](M4_IMPLEMENTATION_PLAN.md#12-bàn-giao-m5--ui-tối-thiểu),
[evidence P6 M4](M4_IMPLEMENTATION_PLAN.md#evidence-p6--04102026) và
[evidence P7 M4](M4_IMPLEMENTATION_PLAN.md#evidence-p7--04102026).

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

Việc compile proto không chứng minh upload, failover hay repair đã hoạt động.
Evidence implementation M0–M3 và ranh giới chưa làm nằm ở phần tiến độ bên dưới;
M4 đã hoàn thành theo evidence riêng; M5–M6 chỉ hoàn thành khi có code và
kiểm tra thật theo gate của từng milestone.

## Tiến độ implementation — 04/10/2026

**M4 P4–P5 hoàn thành (04/10/2026).** Manual repair source/RF/placement,
missing/corrupt reconcile và REST filter/cursor/budget/summary đã có code.
89 ca khác nhau passed trong lượt focused Docker Linux/PostgreSQL (47 repair,
4 Delete guard mới, 38 direct regressions), Ruff 10 paths qua; không cộng rerun
hoặc counts trùng P1–P3 thành full suite. Chỉ rebuild tests image, chưa deploy/
SIGKILL/live smoke hoặc dọn fixture M3. Tiếp theo P6 lifecycle/live smoke.
Chi tiết: [evidence P4–P5](M4_IMPLEMENTATION_PLAN.md#evidence-p4p5--04102026).

Ghi nhận P1–P3 dưới đây là lịch sử tại thời điểm bàn giao repair.

**M4 P1–P3 hoàn thành (04/10/2026).** Delete RPC, DELETE tombstone/200–202,
cleanup FAILED/DELETING bounded/fair và worker/lifespan đã có code. 86 ca khác
nhau passed qua focused integration Docker Linux/PostgreSQL; Ruff 12 paths qua.
Có warning TestClient; lỗi cleanup tick rỗng cạnh tranh upload đã sửa và checks
affected qua. Chỉ rebuild tests image; chưa deploy/live smoke/SIGKILL M4,
chưa repair/UI/hai host hoặc dọn fixture M3. Tiếp theo P4–P5 manual repair.
Chi tiết: [evidence P1–P3](M4_IMPLEMENTATION_PLAN.md#evidence-p1p3--04102026).

**M3 đã hoàn thành P1–P7 (04/10/2026).** Upload/GET files/download RF=2,
checksum/fallback và lifecycle có evidence; smoke Compose thật qua node-2 DOWN,
recovery và Metadata restart. P7 đã đối chiếu source/contracts/DoD và cập nhật
docs, không sửa runtime hoặc chạy lại checks đã pass. Một live fixture giữ
manifest cho M4 DELETE/cleanup; chưa repair/UI/demo hai host. Tiếp theo M4.
Chi tiết: [evidence P7](M3_IMPLEMENTATION_PLAN.md#bằng-chứng-p7--04102026)
và [bàn giao M4](M3_IMPLEMENTATION_PLAN.md#bàn-giao-m4--failure-delete-cleanup-và-repair).

Review bổ sung M3 đã sửa early file-byte limit, invalid charset envelope và
cancel/drain spool I/O. 36 focused Docker checks passed (13 mới + 23 regressions),
15 deselected; Ruff 2 paths qua. Chỉ rebuild tests image, chưa deploy fix hoặc
rerun live smoke. Chi tiết: [review M3](M3_IMPLEMENTATION_PLAN.md#review-bổ-sung-m3--04102026).

Các ghi nhận M3 bên dưới là lịch sử tại thời điểm từng phase.

**M3 P6 đã hoàn thành (04/10/2026).** 3 lifecycle cases Docker Linux/PostgreSQL
và 2 native smoke regressions passed; SIGKILL đúng PENDING/AVAILABLE, actual disk
faults và restart giữ dữ liệu. Metadata M3 đã deploy local; smoke file 10 MiB +
17 byte RF=2 qua node-2 DOWN/recovery và Metadata restart, SHA/mapping/volumes
giữ nguyên. 5 services healthy, download temp rỗng; một live fixture có manifest
cho M4 cleanup. Ruff 3 Python paths/PowerShell syntax qua; native cache warning.
Không full suite/frontend hoặc DB restart. Chi tiết:
[evidence P6 M3](M3_IMPLEMENTATION_PLAN.md#bằng-chứng-p6--04102026).
Tiếp theo P7; M3 chưa hoàn thành.

**M3 P5 đã hoàn thành (04/10/2026).** Download verify chunk/toàn tempfile trước
200, fallback/observations và response tempfile ownership đã có; lock thả trước
gửi, cancellation drain trước close/unlink. Startup janitor chỉ dọn đúng managed
regular tempfiles. 63 ca P5 khác nhau + 8 regressions passed qua các lượt Docker
Linux/PostgreSQL; Ruff check/format 8 paths qua. Một lỗi harness đã sửa/rerun
passed; có warning TestClient deprecation. Chỉ rebuild tests image, chưa deploy,
process crash/restart, live smoke/full suite.
Chi tiết: [evidence P5 M3](M3_IMPLEMENTATION_PLAN.md#bằng-chứng-p5--04102026).
Tiếp theo P6; M3 chưa hoàn thành. Cleanup/DELETE/repair thuộc M4.

**M3 P4 đã hoàn thành (04/10/2026).** GET list/detail/chunks đúng schema/filter,
RF snapshot, replica history/counters, DB snapshot chỉ đọc và không lock/RPC.
77 focused integration checks (46 P4 + 31 cluster regressions) passed trong
Docker Linux/PostgreSQL, không skipped/deselected; Ruff check/format 6 files qua.
Có warning TestClient deprecation; chỉ rebuild tests image, chưa deploy/restart
hoặc live smoke/full suite. known_readable là cached estimate; download chưa có.
Chi tiết: [evidence P4 M3](M3_IMPLEMENTATION_PLAN.md#bằng-chứng-p4--04102026).
Tiếp theo P5; M3 chưa hoàn thành.

**M3 P3 đã hoàn thành (04/10/2026).** POST files, commit RF=2, attempted mapping
và failure/cancel persistence qua 38 ca P3 khác nhau trong các lượt focused
integration Docker Linux/Python 3.12.15, không skipped; Ruff check/format
5 files qua. Kiểm tra 64 MiB, Store fallback/ack loss, DB interruption,
ASGI disconnect/task cancellation; warning Starlette/TestClient deprecation.
Chỉ rebuild tests image; chưa deploy/restart hoặc chạy live smoke/full suite.
GET files và download chưa có; cleanup thực tế/repair thuộc M4.
Chi tiết: [evidence P3 M3](M3_IMPLEMENTATION_PLAN.md#bằng-chứng-p3--04102026).
Tiếp theo P4; M3 chưa hoàn thành.

**M3 P2 đã hoàn thành (04/10/2026).** Data RPC client, deadline/retry,
Store ack/Get bytes verification, operation scopes và lifespan/readiness đã có
60 focused P2 tests + 10 selected regressions passed trong Docker Linux/Python
3.12.15, không skipped/deselected; Ruff check/format 6 files qua. Có warning
Starlette/TestClient deprecation. Chỉ rebuild tests image, chưa deploy/restart
live services hoặc chạy smoke; chưa có upload API/replication RF=2 thật.
Chi tiết: [evidence P2 M3](M3_IMPLEMENTATION_PLAN.md#bằng-chứng-p2--04102026).
Tiếp theo P3; M3 chưa hoàn thành.

**M3 P1 đã hoàn thành (04/10/2026).** Logic input/spool/chunk hash và placement
RF/domain có 106 targeted tests native Windows/Python 3.12.14 passed, không
skipped/deselected; Ruff check/format 4 files qua. Có một warning cache do sandbox
chặn ghi. Chưa nối HTTP/DB/gRPC hoặc deploy; chưa có replication/fallback thật.
Chi tiết: [evidence P1 M3](M3_IMPLEMENTATION_PLAN.md#bằng-chứng-p1--04102026).
Tiếp theo P2; M3 chưa hoàn thành. Các ghi nhận M0–M2 bên dưới giữ ngày lịch sử.

**M1 đã hoàn thành P1–P6.** Full backend suite 106 passed, không skipped/deselected;
lint/format 29 files, schema drift check và base smoke HTTP/DB/registry/ba node qua.
Tại gate M1, Metadata và cả ba node dùng build M1 mới, volumes giữ nguyên. Persistence/crash
và volume isolation đã có bằng chứng P5; chưa có replication/failover. Tiếp theo
lúc đó là M2: health polling và REST nodes/cluster.

**M2 đã hoàn thành P1–P6.** Registry, monotonic detector, poll worker và REST
nodes/cluster đã qua các gate tập trung P1–P4; P5 đã deploy Metadata M2 và kiểm
tra down/recovery, restart process/DB thật, persistence và volumes độc lập.
P6 rà source/contracts/evidence, chốt DoD và bàn giao M3; không sửa runtime hay
rerun tests/lint/build/smoke. Counts từng phase có regression trùng nhau, không
coi là full-suite run. Tiếp theo M3 upload/download RF=2.
Chi tiết: [evidence P6 và bàn giao](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p6--03102026).

Review M2 sau P6 đã sửa ba findings: guard/validation smoke bị bỏ dưới `-O`,
interval không hữu hạn làm scheduler chết nhưng ready vẫn 200, smoke từ chối
disabled history hợp lệ. 54 native targeted + 53 Docker focused integration
passed, lint/format 8 files qua. Metadata đã cập nhật bản sửa, baseline read-only
`-O` passed, tất cả services healthy; readiness hiện kiểm tra scheduler đang chạy.

Các ghi nhận bên dưới là lịch sử tại thời điểm từng phase.

**M2 P1 đã hoàn thành:** registry reset DOWN giữ lịch sử cùng endpoint, vô hiệu
snapshot khi endpoint/domain đổi; startup recovery giữ operation_lock. Focused
PostgreSQL/lifespan checks 16 passed, không skipped/deselected; lint/format 3 files
qua. Chỉ rebuild tests image, chưa deploy/restart live services. Tiếp theo P2;
poll worker, nodes/cluster API và restart thật vẫn chưa qua gate. Chi tiết trong
[evidence P1 của M2](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p1--03102026).

**M2 P2 đã hoàn thành:** health state machine dùng monotonic age, kiểm tra
identity/writable, giữ hard-DOWN đến success và trả immutable snapshot mới.
32 targeted tests native Windows qua, không skipped/deselected; lint/format
2 files qua. Một warning pytest cache bị sandbox chặn ghi. Chưa nối worker/RPC/DB,
không build/deploy/restart services trong P2. Tiếp theo P3; chi tiết trong
[evidence P2 của M2](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p2--03102026).

**M2 P3 đã hoàn thành:** lifespan poll worker, RPC độc lập mỗi node, transaction
DB ngắn và publish snapshot sau commit, shutdown đóng threads/channels trước
engine. 32 focused integration/regression tests Docker Linux qua, không skipped/
deselected; lint/format 5 files qua. Chỉ rebuild tests image, chưa deploy/restart
services. Tiếp theo P4 REST nodes/cluster; Compose smoke/restart thật còn ở P5.
Chi tiết: [evidence P3 của M2](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p3--03102026).

**M2 P4 đã hoàn thành:** GET nodes/cluster đọc snapshots, DTO null/UTC, counters
theo RF của file và live replica eligibility. Cluster aggregates trong một SQL,
GET không gọi RPC/acquire lock. 32 HTTP/PostgreSQL checks Docker Linux passed,
không skipped/deselected; lint/format 6 files qua. Chỉ rebuild tests image, chưa
deploy/restart services. Tiếp theo P5 lifecycle/Compose smoke; M2 chưa hoàn thành.
Chi tiết: [evidence P4 của M2](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p4--03102026).

**M2 P5 đã hoàn thành:** 1 lifecycle test passed với hai Metadata child processes
thật, PostgreSQL schema riêng và gRPC thật; startup reset/recovery và metadata
preservation qua restart đúng. Metadata đã deploy build M2; Compose smoke
node-2 ACTIVE → SUSPECTED → DOWN → ACTIVE, live/ready 200 và hai node khác ACTIVE.
Restart PostgreSQL giữ nguyên toàn bộ fixture rows/columns; named volumes không
đổi, ba Storage volumes độc lập, schema fixture đã dọn. Lint/format 3 Python
files, PowerShell syntax passed; tất cả services healthy. Không rerun full suite,
không coi replication/failover đã có. Còn P6 rà DoD/bàn giao M3.
Chi tiết: [evidence P5 của M2](M2_IMPLEMENTATION_PLAN.md#bằng-chứng-p5--03102026).

Review sau P6 đã sửa hai bug Health worker starvation và accounting khi Store lại
chunk mất trên disk. Focused Storage/lifecycle regression: 105 passed, 4 deselected;
lint/format qua. Năm cases mới và fixtures dùng production server factory đã bổ sung.

Base M0 đã được thiết lập trong repo: Python 3.12 venv/dependency lock, proto
generation/import, config/logging, Metadata health API, migration bốn bảng và
Storage HealthCheck. Compose local chạy PostgreSQL, Metadata và ba node với
volumes riêng; 7 checks qua trong Docker Linux, schema không drift và smoke qua
sau Metadata restart. Hướng dẫn thực tế: [README repo](../README.md).

M1 P1 đã có validation đầu vào data RPC, layout chunk/tempfile và startup
filesystem cleanup/used_bytes/limit checks. M1 P2 đã có StoreChunk atomic,
idempotency, conflict handling và metrics sau commit. Focused integration P2:
70 passed, 4 deselected trong Docker Linux; lint/format qua.

M1 P3 đã có Get snapshot/hash actual bytes và Delete idempotent dưới cùng mutex
với Store. Focused integration P3: 82 passed, 4 deselected trong Docker Linux,
gồm 12 cases mới và hồi quy Storage; lint/format qua. Chỉ image tests được rebuild;
không chạy DB tests, cluster smoke hay restart. P4–P6 chưa hoàn thành.

M1 P4 đã có stats snapshot dưới lock riêng, Health không chờ operation mutex,
probe short-write check và sanitize lỗi bất ngờ. Focused integration: 98 passed,
4 deselected, gồm 16 cases P4 về health/concurrency/cancel/deadline/ack loss và
hồi quy Storage; lint/format qua. Chỉ rebuild image tests, không restart runtime
containers hay chạy DB/cluster smoke. P5–P6 và M1 chưa hoàn thành.

M1 P5 đã xác minh 2 MiB qua restart container thật, node-2 NOT_FOUND với chunk
node-1 và Delete lặp đưa metrics về baseline. Hai lifecycle scenarios qua process
thật kiểm tra restart và SIGKILL sau fsync trước commit, startup dọn tempfile và
giữ chunk cũ. Có smoke client theo mode và manifest; hướng dẫn trong README repo.
Node-1/node-2 đã cập nhật build, Metadata/node-3 chưa recreate. Không rerun suite
cũ/DB/Metadata smoke; P6 và M1 chưa hoàn thành.

Luồng file REST chưa được triển khai; health polling và GET nodes/cluster đã có
ở M2. Cleanup worker, repair và UI dự án thuộc các milestone tiếp theo. Không coi việc
container healthy là bằng chứng replication hoặc failover đã hoạt động.

Bằng chứng và bàn giao M1: [M1 — Một Storage Node thật, phase P1–P6](M1_IMPLEMENTATION_PLAN.md).
Bằng chứng và bàn giao M3: [M2 — Metadata quan sát ba node, phase P1–P6](M2_IMPLEMENTATION_PLAN.md).

Mã nguồn Python, contract implementation, tests/scripts và dependency lock đã
gom vào `backend/`, song song với `frontend/`. `docs/` và `deploy/` dùng chung
ở repo root. Setup/Docker và các đường dẫn trong tài liệu đã cập nhật; 58 tests
và smoke cluster vẫn qua với cấu trúc mới.
