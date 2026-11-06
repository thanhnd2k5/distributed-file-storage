# D5 — baseline nhiều process và tải hỗn hợp, 29/10/2026

Phạm vi: D01–D04 qua HTTP/Nginx/gRPC/PostgreSQL/SQLite/filesystem thật. Đây là
fixture cùng một host; không phải HA hoặc benchmark production. Không thay runtime,
schema, proto hay volumes V1. C5/browser/1–10 GiB vẫn tạm gác.

## Topology và cách đo

- Docker Engine 29.8.2, Python 3.12, PostgreSQL 16; WSL2 Linux kernel
  `6.6.114.1-microsoft-standard-WSL2`, Docker thấy 12 CPU và 12,112,199,680 bytes RAM.
- Một tests container có memory cap 3 GiB. Mỗi ca có schema và directory riêng,
  9 PID dịch vụ độc lập: 2 API public, 2 internal, 2 Transfer, 2 Storage và 1 Worker;
  Nginx process riêng, round-robin, không sticky session, không upstream retry.
  Audit từng ca đã ghi nhận cả hai endpoint của mỗi nhóm API/internal/Transfer.
- Chunk 8 MiB, RF=2, hai domain. Fixture cấu hình 2 download slots/Transfer,
  read renew 10s thật và RPC deadline 20s để giữ EOF qua một lần renew. Mặc định
  runtime vẫn là 4 download slots và RPC 5s; D5 không đổi các mặc định đó.
- Probes chỉ ở `tests/v2_d5_service.py`: đọc/chèn lỗi quanh Storage GetChunk thật,
  theo dõi buffer/task, renew và thời gian ASGI send. Endpoint `/_d5/metrics`
  được đọc trực tiếp trên loopback, không được ingress công khai route.
- RSS lấy `/proc/<pid>/status` mỗi 100ms; VmHWM là high-water cả đời process,
  không phải riêng một phase. Socket memory là mẫu `ss -tmnpH` khoảng mỗi giây.
  Guard fixture cho Transfer VmHWM <256 MiB, tách khỏi reservation chunk ứng dụng.
- `ttfb_seconds` đo từ trước HTTP request tới block body đầu ở client, với iterator
  block 64 KiB (Range nhỏ chỉ có bytes thật của Range). Đây là proxy cho TTFB tới
  block đầu, không phải packet-level first-byte trace. Upload/finalize đã làm nóng
  dữ liệu/cache trước phép đo; chưa có cold-cache, percentile hoặc SLO.
- Read amplification ở đây là **payload Storage→Transfer / HTTP payload**, gồm
  retry/fallback. Không phải số bytes vật lý đọc từ disk: Storage còn verify file,
  filesystem/page cache và kernel có chi phí riêng chưa đo bằng block I/O counters.

## Kết quả đo của lượt D5 ban đầu, trước review

| Phép đo | Kết quả |
|---|---|
| GET full 8 MiB +17 bytes, hai lượt lần lượt qua hai Transfer | Body/full SHA-256 đúng; tới block đầu 59.5–61.7ms; tổng 88.9–89.9ms |
| Range `bytes=1-1` | Body 1 byte đúng; tới body khoảng 59.5ms; đọc 8,388,608 bytes payload Storage, amplification 8,388,608× |
| Replica đầu mất footer của chunk 8 MiB | Fallback body/full hash đúng; amplification 1.999998× cho file 8 MiB +17 |
| Một download đang chạy mỗi Transfer, RSS sampled | 93.0 và 92.5 MiB/process |
| Bốn download 64 MiB, hai mỗi Transfer, replica đầu chậm 3ms/block | Full SHA-256 đúng cả bốn; tới block đầu 195.3–211.4ms; tổng khoảng 15.49s gồm thời gian pause/throttle chủ động |
| RSS Transfer trong mixed phase | 104.6 và 103.9 MiB/process; baseline trước phase 78.6 và 77.3 MiB |
| Chunk buffer/task high-water trong mixed phase | 2 buffers và 2 download slots/process; mỗi stream tái sử dụng cùng buffer qua 8 chunks |
| Send bị backpressure lâu nhất | Khoảng 9.70s và 9.66s; renew vẫn có tiến triển |
| Socket memory used sampled peak, mỗi Transfer trong mixed phase | 8,496,512 bytes; socket capacity peak 17,568,768 bytes, không đồng nhất với RSS |
| Upload/finalize trong lúc bốn download chiếm hết slots | Finalize file 16 MiB và upload/finalize file 8 MiB +17 xong trong tổng 0.844s; download chưa kết thúc |
| 16 GET vượt mức | Cả 16 trả 429; không thêm lease, vẫn đúng 4 lease đã acquire |

Các số đo chỉ mô tả fixture này. Chưa đo 16 download thành công đồng thời,
throughput saturation, tải liên tục, nhiều host hoặc file 1–10 GiB. RSS sampled
không phải hard memory limit; guard VmHWM và cap container là hai kiểm tra khác.

## Tính đúng được kiểm

1. Full/Range đi qua hai Transfer và cùng manifest/version; suffix/If-Range,
   boundary qua chunk, multi-range fallback, HEAD và empty/416 đúng bytes/status.
   HEAD/416 không tạo lease, successful empty GET vẫn acquire/release.
2. Read đã acquire gặp DELETE: cả hai API/Transfer chặn người đọc mới. Worker ghi
   READ_LEASE_ACTIVE và giữ charge/bytes. Lease cũ renew qua mốc 10s, tiếp tục đúng
   version; response/release rồi cleanup → DELETED, charge=0, ledger durable tombstone
   và directory chunks rỗng trên cả hai Storage.
3. Lease bị làm hết hạn có kiểm soát trong DB fixture: renewal không hồi sinh lease;
   response chưa có binary headers trả 503, GetChunk bị hủy/drain, rồi cleanup được
   tiến hành. Đây là fault injection expiry, không phải thay TTL production.
4. DELETE trên process khác thắng giữa resolve và acquire: GET trả 404 trước binary
   headers, không tạo download lease. Coordination hooks chỉ có ở fixture.
5. Missing footer fallback đúng; cả hai replica lỗi ở chunk thứ hai thì client nhận
   đúng prefix 8 MiB và `RemoteProtocolError` do response incomplete. Không thêm JSON,
   padding hoặc phát chunk chưa verify. Metadata/ledger còn AVAILABLE/STORED vì các
   fault này chỉ sửa wire, không phá bytes lưu bền.
6. Slow TCP consumers + slow replica + upload/finalize: slot/buffer high-water hữu
   hạn, renew và các loại công việc có tiến triển. Mọi task/buffer/lease được drain
   sau khi client đọc xong; không lấy kết quả ASGI-only D4 làm evidence TCP này.

## Tái lập và raw artifacts

Tier focused integration/E2E. Preflight chỉ Docker và PostgreSQL fixture:

```powershell
docker info
docker compose -f deploy/compose.v2-metadata-tests.yml ps
docker compose -f deploy/compose.v2-metadata-tests.yml exec -T postgres psql -U v2_test -d dfs_v2_test -Atc 'SELECT current_database(),current_user,1'
```

Sau khi dependency sẵn sàng, từ root:

```powershell
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-d5-tests.yml build tests
docker compose -f deploy/compose.v2-metadata-tests.yml -f deploy/compose.v2-http-tests.yml -f deploy/compose.v2-d5-tests.yml run --rm --no-deps tests python -m pytest -q -x tests/test_v2_d5.py
```

Lượt nguyên file: **6 passed (121.23s)**. Raw artifacts tại
`backend/.runtime/v2-d5/<test-name>/`: report.json chứa PID/ports/config, metrics,
RSS/socket samples, audit requests và Storage/renew events; kèm nginx.conf và log
của mỗi service. Directory này được gitignore, bản tóm tắt hiện tại được giữ trong
docs. Rerun cùng case sẽ thay report/log của case đó; không gộp số liệu các lượt.

Sau khi siết assertion READ_LEASE_ACTIVE và sửa job retry selection, hai ca
read/delete được chạy lại riêng: **2 passed (49.08s)**. Không tính thành 8 ca khác
nhau. Production runtime không đổi; Ruff check/format hai Python files đều pass.
Image manifest cho lượt nguyên file là
`sha256:c326725e7ab5751b7e3e8769fb4e691bd8bac8b81ef86e2b336efbc0e83c9252`;
cho hai ca siết assertion là
`sha256:bf40d2e61f699034f4c9b2e2ea03471ad58c7cd8714db51d9fad644d79f40ebb`.
Chỉ static formatting/viết docs sau các gate, không rerun các ca đã pass.

## Sau review trước E — 29/10/2026

Review phát hiện gate ban đầu dùng bốn file riêng nên không bắt trường hợp nhiều
reader cùng allocation bị khóa writer độc quyền. Đã sửa shared-reader ownership,
control deadline/config/tracing và đổi mixed test thành **bốn reader cùng một file
64 MiB**, vẫn qua hai Transfer/RF=2. Chi tiết và commands tại
[fixes trước E](V2_PRE_E_FIXES.md).

Lượt D5 mới: **6 passed (114.64s)**, cùng **32 regression mới** và **44 hồi quy trực
tiếp** đã pass trên Linux; không cộng thành số ca mới của D5. Các raw reports trong
`backend/.runtime/v2-d5/` hiện thuộc lượt mới. Bảng trước giữ snapshot lịch sử;
hai workload khác nhau, không dùng để kết luận A/B hoặc cải thiện performance.

| Phép đo sau fixes | Kết quả |
|---|---|
| Full GET 8 MiB +17, lần lượt hai Transfer | Body/hash đúng, tới block client đầu 60.7–75.9ms |
| Range 1 byte | Khoảng 60.6ms, amplification payload vẫn 8,388,608× |
| Bốn reader cùng file 64 MiB | Hash đúng cả bốn, tới block đầu 221.3–266.9ms, tổng 15.57–15.60s gồm pause/throttle |
| RSS mixed phase, Transfer 0/1 | 100.3/101.8 MiB sampled; VmHWM khoảng 100.6/101.9 MiB cả đời process |
| Buffer/stream high-water | Hai/process, cùng buffer tái sử dụng qua tám chunks |
| ASGI send backpressure tối đa | 9.63/9.67s; renew vẫn hoạt động |
| Upload/finalize khi download slots đầy | 0.839s tổng cho hai công việc như fixture ban đầu |
| Vượt admission | 16 GET →429, không thêm lease; sau flow buffer/stream/lease drain hết |

Định nghĩa TTFB tới block client đầu 64 KiB, sampling RSS/socket, topology một host,
cap 3 GiB và các giới hạn ở trên vẫn giữ nguyên. Chưa nghiệm thu file 1–10 GiB,
16 download thành công đồng thời, cold-cache, percentile hay HA nhiều host.
