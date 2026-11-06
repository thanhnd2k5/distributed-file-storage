# ADR V2 — state machines, fencing và crash recovery

Ngày 22/10/2026. Quyết định baseline cho A; evidence runtime sẽ bổ sung ở B–H.
Tham chiếu [thiết kế](V2_DESIGN.md), [API](V2_API_CONTRACTS.md),
[schema](V2_METADATA_SCHEMA.md). Namespace/volume V2 tách V1.

## 1. State machines và quyền chuyển

| Đối tượng | Chuyển trạng thái | Chủ sở hữu/điều kiện |
|---|---|---|
| Session | OPEN→FINALIZING→COMPLETED | API complete rồi Worker publish sau full hash/RF validation |
| Session | OPEN→EXPIRED | Sweeper theo DB clock; chưa FINALIZING |
| Session | OPEN/FINALIZING→ABORTED | Abort/delete commit trước; token/revision cũ không publish |
| Session | OPEN/FINALIZING→FAILED | Lỗi không retry được hoặc retry budget hết; enqueue cleanup |
| Part | chưa có→CLAIMED→VERIFIED | API claim/confirm; immutable size/hash |
| Part | CLAIMED→RECONCILING→VERIFIED | Lease/ACK mất, Worker inspect được STORED đúng spec đủ policy |
| Part | RECONCILING→CLAIMED | Đã resolve/fence mọi attempt cũ, session còn OPEN; revision/token mới |
| File | UPLOADING→VERIFYING→AVAILABLE | Tương ứng complete/publish; immutable version |
| File | UPLOADING/VERIFYING→FAILED | Session terminal không thành công |
| File | mọi state chưa DELETED→DELETING→DELETED | Tombstone, rồi lease/attempt/bytes đã dọn |
| Allocation DB | RESERVED→WRITING→STORED | Intent trước RPC, ACK đúng spec |
| Allocation DB | RESERVED/WRITING→UNKNOWN→STORED hoặc DELETE_PENDING | Outcome chưa rõ được inspect; không free credit |
| Allocation DB | state khác RELEASED→DELETE_PENDING→RELEASED | Quyết định cleanup, durable Delete ACK, release ledger |
| Allocation Storage | chưa có→PREPARED→WRITING→STORED | Durable prepare, stream, verified durable commit |
| Allocation Storage | WRITING→PREPARED | Writer đã drain, temp đã dọn; chưa đủ bytes, có thể retry cùng spec |
| Allocation Storage | STORED→CORRUPT | Read/startup phát hiện thiếu/sai bytes; repair bằng ID mới |
| Allocation Storage | mọi state/chưa có→DELETED | Durable fence trước unlink, không có đường ra |

Session terminal không mở lại. COMPLETED giữ nguyên sau delete file; file
tombstone độc lập với lịch sử phiên. FAILED/ABORTED/EXPIRED giữ record part và
attempt để cleanup; không đổi VERIFIED thành “chưa gửi” hoặc reset startup.
Read lease hết hạn không renew lại; client tạo lượt đọc mới chỉ nếu AVAILABLE.

## 2. Identity và mức bảo đảm

Mỗi destination có allocation UUID riêng; không dùng content hash làm path hay
dedup key cho physical ownership. Scope chứa namespace, recovery epoch, node ID,
node incarnation; generation tăng theo allocation, job token tăng theo job và
part revision tăng theo claim. Ba bộ số này không thay thế nhau.

DB fence chặn stale confirmation; Storage fence chặn stale commit sau khi đã
quan sát generation/tombstone mới. Node bị partition có thể chưa biết fence:
write cũ có thể tạo orphan nhưng không được vào manifest, charge vẫn giữ tới khi
node nhận Delete. Không tuyên bố “không có bytes mồ côi tại mọi thời điểm”.
Delete fence được giữ bền vững kể cả chưa từng Prepare; NOT_FOUND không đóng được
request Prepare tới muộn. Generation cao hơn cũng không mở lại DELETED.

Prepare generation cao hơn chỉ thay chủ attempt; spec bất biến. Khi session còn
OPEN, Worker có thể tăng generation để fence writer đã mất lease trên allocation
PREPARED/WRITING rồi drain và retry; job phải giữ charge và không mở writer thứ hai
trước drain. Nếu không thể xác nhận fence, giữ UNKNOWN hoặc chọn destination mới
với charge mới. Abort/expiry dùng Delete terminal; không reuse allocation đã đóng.

## 3. Durable commit tại Storage

Atomic rename chỉ bảo vệ việc nhìn thấy file, không tự commit ledger. Trình tự:

1. Prepare: kiểm scope/epoch/spec, transaction SQLite ghi generation/PREPARED
   bền vững; response sau commit. Nếu DELETED hoặc spec khác phải từ chối.
2. Store: giữ owner lock allocation, transaction ngắn ghi WRITING/temp_name
   trước filesystem create. Đọc block hữu hạn, check offset/count/hash, deadline
   và cancellation. Không giữ SQLite transaction trong lúc đọc stream.
3. Đủ bytes/hash: fsync tempfile; dưới allocation commit lock kiểm lại epoch,
   generation và state, rename sang final trên cùng filesystem, fsync directory.
   Không overwrite file cuối không khớp; quarantine/CORRUPT khi phát hiện lệch.
4. Commit STORED với actual size/hash, xóa temp intent trong SQLite. ACK sau
   commit bền vững; không ACK sau riêng fsync hoặc riêng rename.
5. Delete: dưới cùng allocation lock, commit DELETED/cleanup_complete=false
   trước unlink. Chặn/drain writer, unlink final/temp, fsync directory, rồi
   commit cleanup_complete=true; chỉ lúc đó mới trả Delete ACK.

Prepare fence và Delete phải có thể đặt state khi writer đang chờ network;
không chờ giữ khóa stream vô hạn. Writer chỉ giữ commit lock quanh kiểm tra và
rename/commit, phải kiểm state sau mọi lần chờ. Writer slot riêng giữ tối đa một
tempfile; fencing đặt cancellation rồi drain writer slot trước khi tạo temp mới.
Get giữ read handle/reader ownership theo allocation, Delete drain readers hữu
hạn; read/delete lease trong DB quyết định quyền phát physical delete trước đó.

Khởi tạo directory/ledger mới cũng cần fsync theo khả năng filesystem. Hardware
phải thực hiện flush đúng; các guarantee cần kiểm chứng trên môi trường B, không
suy ra từ test mock. Theo [SQLite atomic commit](https://www.sqlite.org/atomiccommit.html),
độ bền phụ thuộc flush/filesystem; transaction local không bao trùm file chunk.

Startup giữ volume process lock, recovery gate đóng allocation chưa rõ. WRITING
có final hợp lệ thì xác minh actual bytes/spec rồi hoàn tất STORED; chỉ có temp
thì dọn/fsync và về PREPARED. STORED thiếu/sai final thành CORRUPT, không ACK replay
thành công; DELETED luôn tiếp tục unlink bất kể file/temp còn tồn tại. File không
có ledger intent được quarantine; không tự xóa vì một bản ledger có thể bị mất.
Reconcile metadata sau startup mới quyết định có dùng hoặc xóa allocation đó.

## 4. Ma trận crash/race phải chuyển thành test B–D

| ID / mốc | Điểm inject hoặc race | Kết quả bắt buộc |
|---|---|---|
| B01 | Prepare commit, response mất, process restart | Cùng generation/spec replay cùng allocation; khác spec conflict |
| B02 | Store đứt sau vài block | Không ACK; bounded temp; drain/dọn rồi retry; charge không tự mất |
| B03 | Kill sau fsync temp, trước rename | Startup dọn intent temp; không có STORED giả |
| B04 | Kill sau rename+directory fsync, trước SQLite STORED | Startup hash final và reconcile STORED; không ghi thêm/counter gấp đôi |
| B05 | STORED commit rồi ACK mất | Inspect trả actual spec; retry idempotent |
| B06 | Delete commit trước unlink, kill process | Tombstone còn, restart dọn bytes; Store/Prepare mọi generation bị reject |
| B07 | Delete allocation chưa Prepare; Prepare cũ tới sau | Tombstone chặn; NOT_FOUND chưa từng được dùng làm delete ACK |
| B08 | Writer gen1 chậm; Prepare gen2/Delete tới | Gen1 không rename/ACK sau fence đã commit; không hai tempfile chồng slot |
| B09 | Hai chunk khác allocation streaming đồng thời | Không operation lock toàn node; local DB transactions ngắn |
| B10 | Ledger mất/thay volume nhưng node ID cũ | Incarnation mới bị quarantine; không nhận ACK identity cũ |
| B11 | Disk đầy khi write hoặc ghi tombstone | Không ACK thành công; giữ charge/cleanup pending; headroom kiểm chứng |
| C01 | Hai API create cùng idempotency key | Một session/file; payload khác conflict; không mất admission slot |
| C02 | Hai Transfer claim cùng session/index | Một chủ; cùng spec busy, khác hash conflict; không reserve gấp đôi |
| C03 | Hai upload tranh budget cùng node | Tổng charge không vượt budget, destination mới có reserve riêng |
| C04 | Store ACK rồi Transfer chết trước confirm | Lease sweeper enqueue reconcile; inspect phục hồi VERIFIED hoặc cleanup |
| C05 | Confirm commit, HTTP response mất | GET parts thấy VERIFIED; retry không tạo allocation mới |
| C06 | Complete đụng part đang ghi/unknown | 409 chưa đổi FINALIZING; không manifest thiếu |
| C07 | Complete lặp hoặc hai Worker finalize | Một manifest/job active và một publish; token cũ không commit |
| C08 | Abort/expiry đụng late confirm/finalize | Revision/state fence; không AVAILABLE sau terminal commit |
| C09 | File B cùng tên/size được chọn khi resume A | Pre-hash không khớp bị chặn; nguồn đổi giữa hash/upload bị finalize mismatch |
| C10 | Health free đổi sau ACK; cleanup ACK mất rồi retry | Charge giữ đúng một lần; chỉ durable cleanup release đúng một lần |
| C11 | Empty/boundary/1 GiB/10 GiB | Size, count và full SHA trùng nguồn; RSS hữu hạn; đo finalize riêng |
| C12 | Worker chết giữa finalize; job lease hết | Retry từ đầu hữu hạn, không công bố sớm; job token cũ bị reject |
| D01 | Acquire/renew read lease đụng DELETE/cleanup | Acquire trước tombstone được tiếp tục, acquire sau bị chặn; lease hết không hồi sinh |
| D02 | Replica đọc hỏng hoặc stream mất footer/EOF | Không phát chunk chưa verified; fallback hữu hạn hoặc ngắt HTTP |
| D03 | Range suffix/open/boundary/empty/multi/If-Range | Status/header/bytes theo contract, không Blob toàn file |
| D04 | Client chậm và replica chậm, finalize đồng thời | Buffer/queue/deadline hữu hạn, mọi loại công việc có tiến triển |
| B/H01 | Restore DB cũ và writer/stream cũ còn chạy | Không mở ACTIVE; cô lập/drain, epoch mới, inventory/quarantine trước cleanup |
| H02 | Thiếu sync standby hoặc primary partition | Không ACK async; không hai primary writer; trace unknown outcome đúng |

Mỗi test phải lưu command/topology/failure point và kiểm cả metadata, ledger,
bytes thực; không chỉ kiểm response status. C11 và D04 là gate đo tải theo tài
nguyên môi trường, không thuộc lượt compile contract A.

## 5. Recovery epoch và backup

Recovery epoch là UUID trong cấu hình quản trị bên ngoài DB restore, được ghi
bền vững vào node và DB qua maintenance procedure. API/Worker/Transfer chỉ ready
khi khớp cấu hình. Cài epoch yêu cầu cô lập writer cũ, đóng stream/reader và
chặn mutation; không có RPC public để tự tăng epoch. Node offline không được
phục vụ lại trước rejoin và adoption tường minh. Tombstone DELETED cũ vẫn được
giữ và không adopt thành STORED.

Failover không mất commit đã ACK: dùng chính sách synchronous WAL theo thiết kế,
cô lập primary cũ và reconcile outcome chưa biết; không đổi epoch chỉ vì một
process restart. DB restore/PITR hoặc mất liên tục chưa chứng minh: RECOVERING,
đóng cả cleanup/repair/download mới, epoch mới, đối chiếu manifest/tombstone và
rebuild capacity trước ACTIVE. Không suy ra orphan là rác chỉ từ DB cũ.

Backup bao gồm manifest và bytes của cùng recovery point, retention bảo vệ
chunk còn được backup tham chiếu hoặc copy bytes sang backup độc lập. Restore
cũ có thể mất upload/delete sau recovery point; báo rõ mất mát, không tự công
bố file tìm thấy. Lựa chọn công cụ HA/topology và quy trình backup thực thi được
đặc tả ở H; policy fail-closed và các hook epoch phải được xây từ B.

## 6. Bàn giao A sang B

B bắt đầu bằng local ledger/Storage state machine và migration metadata riêng,
theo B01–B11, rồi session/placement/ledger trước luồng Transfer C. A chỉ có
contract kiểm tra được bằng protoc và ma trận test; HTTP, PostgreSQL, SQLite
crash recovery và guarantees trên đều chưa có evidence implementation V2.
