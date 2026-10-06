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
flows là hoàn thành. M2 health polling, node snapshots, API nodes/cluster và
down/recovery/DB restart cũng đã hoàn thành P1–P6 ngày 03/10/2026.

Kế hoạch chi tiết M2: [Metadata quan sát ba Storage Node, phase P1–P6](M2_IMPLEMENTATION_PLAN.md).
P1 registry/baseline → P2 health state machine → P3 worker/lifespan → P4 REST
nodes/cluster → P5 down/recovery/restart thật → P6 bàn giao M3. M2 P1 đã qua gate ngày 03/10/2026:
focused PostgreSQL/lifespan checks 16 passed, lint/format qua. M2 P2 state machine
đã qua 32 targeted tests native, lint/format qua. M2 P3 worker/lifespan đã qua
32 focused gRPC/PostgreSQL/bootstrap tests, lint/format qua. M2 P4 REST nodes/cluster
đã qua 32 HTTP/PostgreSQL checks, lint/format qua. M2 P5 đã qua 1 lifecycle test
với Metadata child processes thật và schema PostgreSQL riêng; Compose smoke đã
quan sát node down/recovery, restart Metadata/PostgreSQL giữ fixture và volumes,
ba Storage volumes độc lập. Metadata đã deploy M2, tất cả services healthy,
fixture schema đã dọn; lint/format và PowerShell syntax qua. Không rerun toàn bộ
suite M1/P1–P4 hoặc frontend. P6 đã rà source/contracts/evidence và hoàn tất
bàn giao M3; không sửa runtime hay rerun checks. M2 đã hoàn thành, các counts
từng phase có regression trùng nhau nên không là một full-suite run.

Review bổ sung M2 đã sửa smoke guards dưới `-O`, finite timing/scheduler readiness
và xử lý disabled history. 54 targeted checks native + 53 focused integration
Docker passed; lint/format 8 files và baseline REST read-only `-O` qua sau deploy
Metadata. Không rerun full suite hoặc DB/node lifecycle smoke P5.

**M3 đã hoàn thành P1–P7 ngày 04/10/2026.** Upload nhiều chunk đủ RF=2,
download SHA đúng trước 200 và fallback khi một node dừng đã qua gate; Metadata
M3 deploy local. P7 rà source/contracts/DoD/evidence và chốt bàn giao M4 bằng
docs, không chạy lại tests hoặc deploy. Counts dưới đây là lịch sử từng phase,
không cộng regression/rerun thành full-suite evidence.

Kế hoạch chi tiết: [M3 — Upload/download RF=2, phase P1–P7](M3_IMPLEMENTATION_PLAN.md).
P1 validation/chunking/placement → P2 data RPC và operation boundary → P3 upload
commit/failure → P4 GET list/detail/chunks → P5 download integrity/fallback/tempfile
→ P6 lifecycle và Compose smoke thật → P7 rà DoD/bàn giao M4. P1 hoàn thành
04/10/2026: 106 targeted tests native Windows/Python 3.12.14 passed, Ruff
check/format 4 files qua; một warning cache do sandbox chặn ghi. Chưa nối
HTTP/DB/gRPC, chưa deploy hoặc chạy smoke; replication/fallback chưa có evidence.
P2 cũng hoàn thành 04/10/2026: data RPC verification/retry, operation boundary
và lifespan qua 60 focused P2 tests + 10 selected regressions trong Docker Linux/
Python 3.12.15; Ruff check/format 6 files qua. Chỉ rebuild tests image, chưa
deploy/restart live services hoặc chạy smoke.
P3 hoàn thành 04/10/2026: POST files/RF=2, PENDING trước Store, commit AVAILABLE,
fallback và failed/cancel persistence có 38 ca P3 khác nhau passed qua các lượt
focused integration Docker Linux/Python 3.12.15; Ruff check/format 5 files qua.
Có evidence 64 MiB, DB interruption, ASGI disconnect/cancellation và lost ack;
chỉ rebuild tests image, chưa deploy/restart hoặc smoke.
P4 hoàn thành 04/10/2026: GET list/detail/chunks, pagination/visibility, replica
history và counters dùng chung cluster; snapshot DB chỉ đọc, không lock/RPC.
77 focused integration checks (46 P4 + 31 cluster regressions) passed Docker
Linux/PostgreSQL, Ruff check/format 6 files qua; một warning TestClient deprecation.
Chỉ rebuild tests image; chưa deploy/restart hoặc live smoke/full suite.
P5 hoàn thành 04/10/2026: download verify chunk/toàn tempfile trước 200,
fallback và observations, response ownership/drain và startup janitor.
63 ca P5 khác nhau + 8 regressions passed qua các lượt focused integration
Docker Linux/PostgreSQL; Ruff check/format 8 paths qua. Một lỗi harness đã sửa
và rerun passed; có warning TestClient deprecation. Chỉ rebuild tests image;
chưa deploy, process crash/restart hoặc live smoke/full suite.
P6 hoàn thành 04/10/2026: 3 Docker lifecycle cases và 2 native smoke guard
regressions passed; SIGKILL PENDING/AVAILABLE, disk faults và restart thật trong
isolated schema/dirs. Metadata M3 deploy local; Compose smoke RF=2 10 MiB +
17 byte qua node-2 DOWN/recovery và Metadata restart, downloaded SHA/mapping
giữ nguyên, volumes riêng không đổi, 5 services healthy/download temp rỗng.
Ruff 3 Python paths và PowerShell syntax qua; một live fixture lưu manifest
cho M4 cleanup. Chưa full suite/frontend hoặc PostgreSQL restart.
P7 hoàn thành 04/10/2026: DoD đã đối chiếu source/evidence, giữ ranh giới M4–M6
và ghi rõ fixture live/manifest. No execution cho docs-only, `git diff --check`
qua; không sửa runtime/tests/config/dependencies, không rerun checks đã pass.

Review bổ sung M3 đã sửa 3 upload ingress findings: early actual-byte limit,
charset lỗi trả 400 và cancellation drain trước close spool. 36 focused Docker
checks passed, 15 deselected; Ruff 2 paths qua. Tại thời điểm review chưa deploy
fix hoặc rerun live smoke/full suite; fixes được deploy ở M4 P6. Xem
[evidence review M3](M3_IMPLEMENTATION_PLAN.md#review-bổ-sung-m3--04102026).

**Bàn giao lịch sử M3 sang M4 failure/DELETE/cleanup/repair:** bổ sung Metadata
Delete RPC wrapper/coordinator, tombstone bền vững, cleanup bounded khi node
offline và repair theo RF snapshot/cursor/budget. Dùng lại data lock/transaction
boundary; fallback download không thay repair. Một AVAILABLE fixture P6 có 6
chunks/12 mappings giữ host manifest để xử lý qua DELETE M4, không raw-delete
DB/chunks/reset volumes. Chi tiết:
[bàn giao M4](M3_IMPLEMENTATION_PLAN.md#bàn-giao-m4--failure-delete-cleanup-và-repair).

Kế hoạch chi tiết M4: [Failure, DELETE, cleanup bền vững và manual repair, P1–P7](M4_IMPLEMENTATION_PLAN.md)
(lập 04/10/2026, đã hoàn thành P1–P7). P1 Delete RPC → P2 cleanup engine/DELETE
tombstone → P3 worker/recovery/drain → P4 repair một chunk → P5 REST cursor/budget
→ P6 fault lifecycle/smoke thật → P7 rà DoD/bàn giao M5. Mỗi phase có gate và
focused checks riêng; việc lập kế hoạch không là evidence cleanup/repair đã qua.

M4 P1–P3 hoàn thành 04/10/2026: 86 ca khác nhau passed qua focused integration
Docker Linux/PostgreSQL, Ruff 12 Python paths qua; không là full-suite run.
DELETE/cleanup bền vững có HTTP/gRPC/DB và restart lifespan evidence; chưa
process SIGKILL/live smoke/deploy M4 hoặc dọn fixture M3. Tiếp theo P4–P5 repair.
Chi tiết [evidence P1–P3](M4_IMPLEMENTATION_PLAN.md#evidence-p1p3--04102026).

M4 P4–P5 cũng hoàn thành 04/10/2026: repair verified source/RF snapshot,
missing/corrupt reconcile và POST admin repair với filter/cursor/budget. 89 ca
khác nhau passed trong lượt focused integration, Ruff 10 paths qua; có regression
trùng P1–P3 nên không cộng counts milestone. Storage Delete có guard active
trước unlink để tránh xóa trễ sau timeout/cancel. Chưa deploy/live smoke/SIGKILL;
tiếp theo P6 lifecycle và smoke thật, sau đó P7. Chi tiết
[evidence P4–P5](M4_IMPLEMENTATION_PLAN.md#evidence-p4p5--04102026).

M4 P6 hoàn thành 04/10/2026: 10 actual process lifecycle checks, 5 native guards
và 1 M3 shared-helper regression passed qua focused checks. Metadata/3 Storage
deploy build hiện hành; live repair RF=2, node recovery, offline DELETE qua
Metadata restart và cleanup bytes thật đều qua. Ba owned fixtures gồm M3 đã
DELETED; 40 mappings/69,206,135 replica bytes được xác minh mất bằng GetChunk
NOT_FOUND, DB giữ tombstones/history; named volumes không đổi. Cluster cuối
pending=0/3 ACTIVE và 5 services healthy, fixture ngoài manifest giữ nguyên.
Chưa full suite/frontend/hai host; tiếp theo P7 rà DoD và bàn giao M5.
Chi tiết [evidence P6](M4_IMPLEMENTATION_PLAN.md#evidence-p6--04102026).

M4 P7 hoàn thành 04/10/2026: đối chiếu DoD/source/contracts và host manifests
P6, sửa ví dụ repair/mô tả summary cho khớp schema, chốt trạng thái fixtures và
hướng dẫn chạy. No execution vì docs-only; không rerun tests/build/smoke/deploy
hoặc kiểm tra lại cluster live. **M4 hoàn thành; milestone tiếp theo M5 UI tối
thiểu**, gồm upload/list/download/delete, detail/placement, nodes/cluster và
manual repair. Frontend starter cần client không credentials/token cho API V1,
adapter error envelope, routes không login guard, mutation/cursor/pending UX
đúng contract; chưa triển khai các thay đổi này ở P7. Chi tiết
[bàn giao M5](M4_IMPLEMENTATION_PLAN.md#12-bàn-giao-m5--ui-tối-thiểu) và
[evidence P7](M4_IMPLEMENTATION_PLAN.md#evidence-p7--04102026).

**M5 P1–P3 hoàn thành ngày 04/10/2026.**
**P4 hoàn thành ngày 05/10/2026.**
Chi tiết [M5 — UI tối thiểu, P1–P7](M5_IMPLEMENTATION_PLAN.md): P1 client/shell
public → P2 snapshots/list/detail/cluster → P3 upload/download → P4 delete/
pending cleanup → P5 manual repair → P6 UI/API runtime/failure flows → P7 DoD/
bàn giao M6. Giữ hai màn hình và stack starter; 2 buổi là mục tiêu happy path,
dự trù 3–4 buổi + buffer cho integration/cursor/pending và evidence. Các fixes
sau review M4 đã có focused tests nhưng chưa deploy live theo evidence cuối;
đối chiếu build trước gate UI thật. P1 đã có client không auth/token/credentials,
shell public và scoped starter bootstrap; targeted ESLint/build qua, 19 browser
checks (API thật và adapter mô phỏng phân biệt rõ) cùng public deep-link/starter
guard regression qua. Live Metadata vẫn build P6; P1 chỉ GET, không deploy/
fault smoke hoặc file mutation thật. Xem [evidence P1](M5_IMPLEMENTATION_PLAN.md#7-evidence-p1--04102026).
P2 list/detail/placement/nodes/cluster hoàn thành: targeted lint/build qua,
10 automated checks mô phỏng được quan sát passed, UI GET thật và unmount/
live 404/invalid route/reload sau phục hồi qua. Static layout fix được kiểm
tra trực quan trong sidebar hẹp. Xem [evidence P2](M5_IMPLEMENTATION_PLAN.md#8-evidence-p2--04102026).
P3 upload/download qua targeted lint/build, 18 controlled checks và flow UI/API
thật cho 10 MiB + 17 byte/file rỗng: SHA-256 Blob download trùng nguồn, filename
Unicode đúng. IAB không trả download event; không có evidence file trên đĩa,
đã xác minh bytes nhận trong browser và anchor save dispatch. Xem
[evidence P3](M5_IMPLEMENTATION_PLAN.md#9-evidence-p3--04102026).
P4 dialog/delete/pending/terminal qua lint/build, 14 controlled checks và 3 P3
regressions chọn lọc. User xác nhận xóa ba fixture P3: hai file nhiều chunk
202/DELETING/pending=4→GET 404, file rỗng 200/DELETED/pending=0; mỗi ID đúng một
DELETE, terminal dừng poll, inactive/cache cập nhật, file ngoài ownership giữ
nguyên. Read 404 không là disk cleanup proof riêng; offline/recovery thật thuộc
P6. Xem [evidence P4](M5_IMPLEMENTATION_PLAN.md#10-evidence-p4--05102026).
P5 manual repair hoàn thành 05/10/2026: filter/root/detail, manual cursor pages,
zero-progress, partial outcomes và timeout/reconcile qua targeted lint/build,
18 controlled checks khác nhau cùng các bước UI trực tiếp. Scan thật scoped
max_chunks=2 ba lượt, checked=2/2/2, remaining=4/2/0, repaired=0; không suy ra
RF recovery từ HEALTHY. Fixture P5 giữ cho gate P6. Xem
[evidence P5](M5_IMPLEMENTATION_PLAN.md#11-evidence-p5--05102026).
P6 UI/API runtime hoàn thành 05/10/2026: stop/start `storage-node-2` với UI —
fallback download SHA khớp, repair Store ack 4/remaining=0 khi DOWN rồi
over_replicated=4 sau recovery, offline DELETE 202/pending→404, 409
OPERATION_BUSY hiện đúng; owned fixtures dọn; local `dev_host` không claim hai
host. Xem [evidence P6](M5_IMPLEMENTATION_PLAN.md#12-evidence-p6--05102026).
**M5 hoàn thành P1–P7 ngày 05/10/2026.** P7 docs-only: đối chiếu DoD/source/
evidence, chốt [bàn giao M6](M5_IMPLEMENTATION_PLAN.md#13-bàn-giao-m6--hai-máy-và-rehearsal),
đồng bộ README/index; không rerun runtime/deploy. Xem
[evidence P7](M5_IMPLEMENTATION_PLAN.md#14-evidence-p7--05102026).
Tiếp theo **M6 hai máy và rehearsal** (Compose A/B, LAN/CORS, failure-domain
demo). Kế hoạch chi tiết: [M6_IMPLEMENTATION_PLAN.md](M6_IMPLEMENTATION_PLAN.md).
**P1–P2 hoàn thành 05/10/2026:** Compose A/B + env examples +
`deploy/preflight-m6.ps1` (Check/B qua); chưa claim bring-up/rehearsal hai host
(P3–P4 khi ngồi chung).

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
