# PROJECT_OVERVIEW.md

# Distributed File Storage System

**Document status:** V1 locked; updated 2026-10-02 for one developer and finalized technical contracts  
**Implementation ownership:** one developer; sequential milestones  
**Planning estimate:** 14–18 focused sessions; actual deadline not yet specified

## 1. Purpose of This Document

This document defines the **V1 scope and architectural boundaries**. Read it together with `ARCHITECTURE.md`, `API_CONTRACTS.md`, `storage.proto`, and `IMPLEMENTATION_PLAN.md`. The technical documents finalize earlier suggestions; wire fields are defined by `storage.proto`. If a genuine contradiction remains, resolve it in the documents before changing implementation.

It is written so that:
- all team members understand the same system scope and architecture;
- future implementation decisions remain consistent;
- AI coding agents can read the document before generating code;
- the project does not gradually turn into a different system during development.

If an implementation detail conflicts with this document, the team should review and update the document first instead of silently changing the architecture.

---

# 2. Project Overview

## 2.1 Project Name

**Distributed File Storage System**

The project is a simplified distributed file-storage platform designed for an academic Distributed Systems course.

The goal is **not** to reproduce HDFS, Google Drive, Ceph, MinIO, or another production storage platform.

The goal is to build a system small enough for one developer to implement incrementally, but complete enough to demonstrate important distributed-system principles through a working product.

---

## 2.2 Problem Statement

A traditional file server stores files on one machine.

This is simple, but it creates several problems:

- storage capacity is limited by one machine;
- if the storage machine fails, files become unavailable;
- data is not distributed;
- there is no redundancy;
- scaling storage requires replacing or upgrading the same machine.

The proposed system distributes file data across several independent storage nodes.

A file uploaded by a client is divided into multiple chunks. Those chunks are stored on different nodes and replicated so that the failure of a single storage node does not necessarily make the file unavailable.

The system therefore separates:

- **metadata management**, which knows where chunks are stored;
- **actual file storage**, which is distributed across multiple Storage Nodes.

---

# 3. Academic Goal

The product should visibly demonstrate distributed-system principles instead of only being a CRUD web application split into multiple services.

The most important principles demonstrated by V1 are:

1. **Distribution of data**
2. **Replication**
3. **Fault tolerance**
4. **Failure detection**
5. **Location transparency**
6. **Communication between independent processes**
7. **Recovery / replica repair**
8. **Separation between metadata and stored data**
9. **RPC-based service communication using gRPC**

The system does not need to solve every problem that exists in a production distributed filesystem.

---

# 4. V1 Scope

V1 is the version that should be ready for the class presentation/demo.

V1 should be considered a **complete academic product**, not an unfinished prototype.

## 4.1 Required Functional Features

The system must support:

- Upload file
- List uploaded files
- Download file
- Delete file
- Display file information
- Display chunk placement
- Display status of Storage Nodes

Optional if time permits:

- Rename file
- File version history
- Search by filename

These optional features must not delay distributed-system features.

---

## 4.2 Required Distributed Features

### File Chunking

Uploaded files are split into fixed-size chunks.

Example:

```text
video.mp4
   |
   +-- chunk-0001
   +-- chunk-0002
   +-- chunk-0003
   +-- chunk-0004
```

Chunk size must be configurable.

A suggested initial value is between **1 MB and 8 MB** for classroom demonstration purposes.

The exact value may be changed later without changing the architecture.

---

### Distributed Placement

Chunks must not all be stored on one Storage Node.

The Metadata Service decides which Storage Nodes receive each chunk.

Example:

```text
chunk-01 -> Node A, Node B
chunk-02 -> Node B, Node C
chunk-03 -> Node A, Node C
```

The initial placement policy may use a simple algorithm such as:

- round-robin;
- node with lowest used capacity;
- node with fewest stored chunks.

However, replica placement should first try to avoid placing multiple replicas of the same chunk in the same `failure_domain`.

A complex placement algorithm is not required for V1.

---

### Failure Domains

Replication must consider not only **how many replicas exist**, but also **where those replicas are physically located**.

Each Storage Node has a configurable `failure_domain`, typically representing the physical host on which that node runs.

Example:

```text
Node 1 -> machine_A
Node 2 -> machine_B
Node 3 -> machine_B
```

Placement rule:

> When possible, replicas of the same chunk should be placed in different failure domains.

Example with replication factor 2:

```text
GOOD:
chunk-01 -> Node 1 (machine_A) + Node 2 (machine_B)

BAD when avoidable:
chunk-01 -> Node 2 (machine_B) + Node 3 (machine_B)
```

This allows the system to tolerate failure of an entire physical host when at least one valid replica remains on another host.

For local development, all nodes may temporarily use the same physical machine. In that mode, host-level fault tolerance is not being demonstrated.

---

### Replication

Default replication factor:

```text
Replication Factor = 2
```

Each logical chunk should normally exist on two different Storage Nodes.

Example:

```text
Chunk C1
 ├── Node A
 └── Node B
```

The system should avoid placing two replicas of the same chunk on the same node.

Replication factor should be represented in configuration so that the design is not permanently tied to the value 2.

---

### Fault-Tolerant Read

If one Storage Node containing a chunk replica is unavailable, the system should attempt to retrieve another valid replica.

Example:

```text
chunk-02 replicas:
- Node B: DOWN
- Node C: ACTIVE

Download uses Node C.
```

A file remains downloadable as long as every required chunk has at least one accessible valid replica.

---

### Node Health Detection

V1 uses **active health polling**: Metadata/API calls each Storage Node's unary `HealthCheck` method periodically. Storage Nodes do not push heartbeats to Metadata in V1.

Node states are `ACTIVE`, `SUSPECTED`, and `DOWN`. Poll every 3 seconds with a 1-second RPC deadline; mark DOWN after 10 seconds without successful health response. Health status is an observation, not proof that every stored chunk is intact. Exact transitions and identity checks are in `ARCHITECTURE.md`.

---

### Replica Repair

If the system detects that a chunk has fewer replicas than the required replication factor, it should be able to create another replica on an available Storage Node.

Example:

```text
Required replicas = 2

Before failure:
chunk-01 -> Node A, Node B

Node B goes down:
chunk-01 -> Node A

Repair:
Node A -> copy chunk-01 -> Node C

After repair:
chunk-01 -> Node A, Node C
```

For V1, repair is triggered **manually** through an administrator action. Metadata reads a verified source using `GetChunk` and writes a destination using `StoreChunk`. A bounded repair pass and cursor are defined in `API_CONTRACTS.md`. Background replica repair is post-V1; background pending-delete cleanup is required in V1.

---

### Checksum / Integrity Verification

Each stored chunk should have a checksum.

V1 uses SHA-256 for every chunk and the complete file. Checksums are verified from actual bytes; the full-file hash is checked before download success.

The checksum can be used to verify:

- upload integrity;
- transferred replica integrity;
- downloaded chunk integrity.

Full production-grade corruption recovery is outside V1 scope.

---

# 5. Important Design Constraints

These decisions are intentionally fixed for V1.

## 5.1 Centralized Metadata Service

V1 uses exactly:

```text
ONE active Metadata Service
```

The Metadata Service is responsible for:

- file metadata;
- chunk metadata;
- chunk locations;
- node registry;
- node health state;
- placement decisions;
- replica information.

### Known Limitation

The Metadata Service is a **Single Point of Failure (SPOF)**.

If the Metadata Service is down, clients may temporarily be unable to locate chunks even if the Storage Nodes still contain the data.

This is an accepted V1 limitation.

The project must not pretend that metadata is highly available.

### Future Direction

A later version may introduce:

- replicated metadata;
- leader election;
- consensus;
- Raft or another replication protocol.

These are explicitly **not required in V1**.

---

## 5.2 Immutable Files

Files are immutable after a successful upload.

V1 supports:

```text
Upload
Read
Delete
```

but does not support modifying bytes inside an existing file.

If a user wants to replace a file, the simplest solution is:

```text
delete old file
upload new file
```

or, in a future version:

```text
create a new file version
```

This decision avoids difficult distributed consistency problems involving concurrent writes.

---

## 5.3 Storage Nodes Do Not Own Global Metadata

Storage Nodes are responsible for storing and serving chunk data.

They should not decide the global location of files or replicas.

Global placement information belongs to the Metadata Service.

---

## 5.4 Storage Nodes Are Independent Processes

Each Storage Node must run independently.

For demo purposes they may run:

- as separate Docker containers;
- as separate processes on the same physical computer;
- or across multiple physical computers.

Running them on one laptop in different containers is acceptable for development and process-failure checks, provided the processes, storage locations, and communication paths are independent. The final V1 classroom demo requires at least two physical hosts to demonstrate a separate host failure domain.

---

# 6. High-Level Architecture

V1 deliberately avoids a separate API Gateway.

The **Metadata Service also acts as the Web API and coordinator**. It exposes REST/HTTP endpoints to the React frontend and uses gRPC to communicate with independent Storage Nodes.

```text
                         +----------------------+
                         |    React / Web UI    |
                         +----------+-----------+
                                    |
                                    | REST / HTTP
                                    v
                         +----------------------+
                         | Metadata/API Service |
                         | Python + FastAPI     |
                         |                      |
                         | File metadata        |
                         | Chunk metadata       |
                         | Replica locations    |
                         | Node status          |
                         | Upload/download      |
                         | coordination         |
                         +----------+-----------+
                                    |
                              PostgreSQL
                                    |
                    Internal service communication
                               via gRPC
                                    |
                 +------------------+------------------+
                 |                  |                  |
                 v                  v                  v
        +----------------+ +----------------+ +----------------+
        | Storage Node 1 | | Storage Node 2 | | Storage Node 3 |
        | Python + gRPC  | | Python + gRPC  | | Python + gRPC  |
        | local chunks   | | local chunks   | | local chunks   |
        +----------------+ +----------------+ +----------------+
                |                  |                  |
           local volume       local volume       local volume
```

## 6.1 Communication Boundaries

```text
Browser / React
      |
      | REST / HTTP
      v
Metadata/API Service
      |
      | gRPC
      v
Storage Nodes
```

Rules:

- React does **not** call Storage Nodes directly in V1.
- React does **not** use gRPC or gRPC-Web in V1.
- The Metadata/API Service receives file requests from the browser and coordinates Storage Nodes.
- Storage Nodes expose internal gRPC services.
- File bytes therefore pass through the Metadata/API Service in V1.
- This makes the Metadata/API Service a possible throughput bottleneck, which is an accepted V1 trade-off.

A future version may let clients transfer chunks directly to Storage Nodes after obtaining a placement plan from Metadata. That optimization is outside V1.

## 6.2 Deployment Topologies

The application code must not depend on all services running on the same physical machine.

All service locations must come from configuration/environment variables.

### Local Development Topology

During normal development, the entire system may run on one laptop:

```text
Developer Laptop

React
Metadata/API Service
PostgreSQL
Storage Node 1
Storage Node 2
Storage Node 3
```

Docker Compose is the preferred local setup.

Typical internal addressing may use Docker service names:

```text
storage-node-1:50051
storage-node-2:50052
storage-node-3:50053
```

This mode is convenient for development but does **not** protect against failure of the physical laptop.

### Two-Machine Demo Topology

For the classroom demo, V1 should support deployment across at least two physical machines connected to the same LAN/Wi-Fi/hotspot.

Suggested topology:

```text
Machine A
├─ React
├─ Metadata/API Service
├─ PostgreSQL
└─ Storage Node 1

Machine B
├─ Storage Node 2
└─ Storage Node 3
```

Example addresses:

```text
Node 1 -> 192.168.1.10:50051, failure_domain=machine_A
Node 2 -> 192.168.1.11:50052, failure_domain=machine_B
Node 3 -> 192.168.1.11:50053, failure_domain=machine_B
```

Only configuration changes between local development and two-machine deployment; application logic should remain unchanged.

### Configuration Rule

Never hard-code `localhost` for Storage Node discovery.

Node host/port must come from:

- database registration;
- environment variables;
- configuration files;
- or node-registration logic.

This allows the same code to run:

```text
on one machine during development
and
across multiple physical machines during demonstration
```

---

## 6.3 V1 RPC Style

V1 uses **unary gRPC** for chunk operations:

```text
one StoreChunk request  -> one StoreChunk response
one GetChunk request    -> one GetChunk response
```

The network still transmits bytes gradually, but at the application/gRPC level each operation is one request message and one response message.

V1 does **not** require gRPC streaming.

Default storage chunk size:

```text
DEFAULT_CHUNK_SIZE = 2 MiB
```

The value must remain configurable. If chunk size is increased later, gRPC message-size configuration must be reviewed.

### Future Extension: gRPC Streaming

A later version may use:

```proto
rpc UploadChunk(stream ChunkData) returns (UploadResult);
rpc DownloadChunk(ChunkRequest) returns (stream ChunkData);
```

Important distinction:

```text
Storage chunking
= how a logical file is divided for distributed storage.

gRPC streaming
= how multiple application-level messages are exchanged during one RPC.
```

The two concepts are related but are not the same.

---

# 7. Component Responsibilities

## 7.1 Client / Web UI

Technology:

```text
ReactJS
```

Responsibilities:

- choose a local file;
- upload file through REST/HTTP;
- list files;
- download file through REST/HTTP;
- delete file;
- show upload/download progress;
- show cluster/node status;
- show chunk placement for demonstration/debugging.

The frontend:

- does not choose Storage Nodes;
- does not maintain global metadata;
- does not call Storage Nodes directly;
- does not use gRPC-Web in V1.

---

## 7.2 Metadata/API Service

Technology:

```text
Python 3.12
FastAPI
PostgreSQL
SQLAlchemy
Alembic
grpcio / grpcio-tools / protobuf
```

This component combines the V1 Web API, metadata authority, and storage coordinator.

Responsibilities:

- expose REST/HTTP API to React;
- receive uploaded files from clients;
- split files into storage chunks;
- generate file IDs and chunk IDs;
- track file/chunk/replica metadata;
- choose Storage Nodes for new replicas;
- prefer placing replicas of the same chunk in different failure domains;
- maintain Storage Node state;
- call Storage Nodes through gRPC;
- coordinate upload and download;
- reassemble chunks in correct order;
- fall back to another replica when needed;
- identify under-replicated chunks;
- coordinate replica repair;
- expose cluster status for the UI.

The Metadata/API Service is the authoritative metadata source in V1.

It intentionally participates in both control flow and file-data flow in V1. This keeps the project understandable for a single developer but creates an accepted bottleneck.

---

## 7.3 Storage Node

Technology:

```text
Python 3.12
gRPC
local filesystem / Docker volume
```

Responsibilities:

- expose an internal gRPC `StorageService`;
- receive a complete V1 storage chunk through unary RPC;
- save chunk bytes to local storage;
- return chunk bytes;
- delete chunks;
- calculate/verify checksum;
- report health/status;
- report basic storage-capacity information.

A Storage Node must not:

- decide global chunk placement;
- store authoritative global metadata;
- communicate directly with the React frontend.

Suggested local storage:

```text
/storage
   /chunks
      chunk-<uuid>
      chunk-<uuid>
      chunk-<uuid>
```

---

## 7.4 Suggested gRPC Contract

The exact `.proto` file will be finalized in the API-contract document.

Conceptually:

```proto
service StorageService {
    rpc StoreChunk(StoreChunkRequest)
        returns (StoreChunkResponse);

    rpc GetChunk(GetChunkRequest)
        returns (GetChunkResponse);

    rpc DeleteChunk(DeleteChunkRequest)
        returns (DeleteChunkResponse);

    rpc HealthCheck(HealthCheckRequest)
        returns (HealthCheckResponse);
}
```

For V1, chunk data may be contained in protobuf `bytes` fields because the default storage chunk size is deliberately small.

Streaming RPC must not be introduced unless the team explicitly changes the V1 scope.

---

# 8. Metadata Model

The exact fields, constraints and state semantics are defined in `ARCHITECTURE.md`, section 4.

| Entity | Purpose |
|---|---|
| File | Immutable file metadata; chunk size/RF snapshots; full-file SHA-256; lifecycle state |
| Chunk | File mapping, ordered index, byte length and SHA-256 |
| ChunkReplica | One mapping per chunk/node; last verified state; persistent cleanup flag |
| StorageNode | Configured identity, address, failure domain, health and capacity snapshot |

File states: `UPLOADING`, `AVAILABLE`, `FAILED`, `DELETING`, `DELETED`.
Replica states: `PENDING`, `VERIFIED`, `MISSING`, `CORRUPTED`, `DELETED`.
Node states: `ACTIVE`, `SUSPECTED`, `DOWN`.
Chunk health is derived from replica/node state, not stored as a second authoritative status.

Metadata must record an attempted replica **before** `StoreChunk`, so an ambiguous timeout remains discoverable for repair/cleanup. Do not discard mappings on node failure or physically delete file metadata before pending cleanup finishes.

---

# 9. Main System Flows

# 9.1 Upload Flow

V1 upload data flows through the Metadata/API Service.

```text
React
  |
  | HTTP upload
  v
Metadata/API Service
  |
  | split file into fixed-size storage chunks
  | choose Storage Nodes
  |
  +---- gRPC StoreChunk(chunk-01) ----> Node A
  |                               \---> Node B
  |
  +---- gRPC StoreChunk(chunk-02) ----> Node B
  |                               \---> Node C
  |
  +---- gRPC StoreChunk(chunk-03) ----> Node A
                                  \---> Node C
```

Suggested steps:

1. User selects a file in React.
2. React uploads the file to the Metadata/API Service using REST/HTTP.
3. Metadata/API Service creates a file record with status `UPLOADING`.
4. Metadata/API Service processes the upload in configured storage chunks.
5. Metadata/API Service generates chunk IDs.
6. Metadata/API Service selects Storage Nodes for each replica.
7. Metadata/API Service calls `StoreChunk` through gRPC.
8. Storage Node calculates/verifies checksum and stores the chunk on its local volume.
9. Successful replica locations are recorded in PostgreSQL.
10. File becomes `AVAILABLE` only when every chunk has the configured RF of acknowledged valid replicas. Commit precedes HTTP 201; browser upload progress 100% is not success. File size defaults to a maximum of 64 MiB for the V1 demo.

### Upload Memory Rule

Do not read the entire uploaded file into RAM.

FastAPI should use file-like/temporary-file behavior such as `UploadFile` and process the upload chunk by chunk.

This is application file processing and is separate from the concept of gRPC streaming.

### Failure During Upload

If one replica upload fails:

- retry a limited number of times;
- select another healthy Storage Node when possible;
- mark the file `FAILED` if replication requirements cannot be satisfied; preserve attempted replica mappings for persistent cleanup;
- never falsely mark it `AVAILABLE`.

V1 does not require a distributed transaction protocol.

---

# 9.2 Download Flow

V1 download also flows through the Metadata/API Service.

```text
React
  |
  | GET /files/{id}/download
  v
Metadata/API Service
  |
  | metadata lookup
  |
  +---- gRPC GetChunk(chunk-01) ----> healthy replica
  +---- gRPC GetChunk(chunk-02) ----> healthy replica
  +---- gRPC GetChunk(chunk-03) ----> healthy replica
  |
  | verify checksum
  | restore chunk order
  v
HTTP file response
  |
  v
Browser
```

V1 first reconstructs into a temporary file on Metadata disk, checks the complete hash and size, and only then sends HTTP 200. This avoids returning a partial file when a later chunk is unavailable. RAM use remains bounded by chunk processing; the accepted cost is temporary disk and startup latency.

Rules:

- reconstruct chunks in `chunk_index` order;
- choose a healthy replica;
- if a selected replica fails, try another replica;
- verify checksum;
- if every replica of a required chunk is unavailable, fail clearly;
- avoid loading an unnecessarily large complete file into RAM.

Browser upload/download progress can still be shown even though Storage Node RPCs are unary. Browser/network progress and gRPC streaming are different concepts.

---

# 9.3 Delete Flow

1. Metadata marks the file `DELETING` and sets persistent cleanup flags before RPC.
2. The file is hidden from normal lists and cannot be downloaded or repaired.
3. Metadata sends idempotent `DeleteChunk` to every attempted/known replica.
4. Unreachable/ambiguous replicas remain pending in PostgreSQL, including across restart.
5. A small background cleanup loop retries reachable nodes; only confirmed cleanup yields `DELETED`.

Delete returns 202 when cleanup is pending and 200 when complete. Tombstone metadata remains in V1; no distributed transaction or automatic orphan inventory scan is added.

---

# 9.4 Health Polling Flow

Metadata/API calls Storage `HealthCheck` periodically through gRPC, validates node identity/domain, and uses its own monotonic clock to infer availability. This replaces the earlier suggested push-heartbeat flow. No Metadata gRPC heartbeat listener is required.

---

# 9.5 Replica Repair Flow

```text
Node B fails
    |
    v
Metadata detects missing replicas
    |
    v
Find healthy source replica
    |
    v
Choose new destination node
    |
    v
Copy chunk
    |
    v
Verify checksum
    |
    v
Update metadata
```

The repair process must not blindly create duplicate replicas on the same node.

---

# 10. Failure Scenarios

The project should explicitly handle and demonstrate selected failures.

## 10.1 Storage Node Failure

Expected behavior:

- periodic health calls fail;
- Metadata Service marks node unavailable;
- download uses another replica;
- system identifies chunks that are now under-replicated;
- repair may create replacement replicas.

---

## 10.2 Storage Node Returns

When a failed node becomes active again:

- it responds to health checks again;
- Metadata Service marks it active;
- stale/unneeded replicas may remain in V1 unless cleanup is implemented.

Advanced reconciliation is optional.

---

## 10.3 Metadata Service Failure

Expected V1 behavior:

- system control operations become unavailable;
- Storage Nodes may still physically hold chunks;
- no automatic metadata failover exists.

This limitation must be stated openly.

---

## 10.4 Chunk Corruption

Minimum V1 handling:

- compare stored/downloaded chunk checksum;
- report corruption;
- use another replica if available.

V1 manual repair may replace a corrupted replica only after confirming another valid source. Automatic corruption repair remains post-V1.

---

## 10.5 Network / Request Failure

Simple retry with a maximum retry count is acceptable.

The system should avoid infinite retry loops.

---

# 11. Consistency Model

V1 uses a deliberately simplified consistency model.

## Metadata

The centralized Metadata Service is the authoritative source for:

- file existence;
- chunk mapping;
- replica locations;
- node health state.

Because there is one active Metadata Service, metadata consistency is much easier than in a replicated metadata architecture.

## File Data

Files are immutable once committed.

This means clients do not need to reconcile multiple concurrent versions of the same file contents.

Replication focuses on availability and durability rather than concurrent-write consistency.

---

# 12. Distributed-System Concepts Demonstrated

The team should be able to explain each concept during the demo.

## Distribution

Chunks of one file exist on multiple Storage Nodes.

## Replication

A chunk has more than one physical copy.

## Fault Tolerance

One Storage Node may fail while file download continues using replicas.

## Failure Detection

Periodic health reporting/checking is used to infer node availability.

## Recovery

Missing replicas can be recreated.

## Transparency

The user interacts with one file abstraction without needing to know which machine stores each chunk.

## Communication

Metadata Service and Storage Nodes communicate through network APIs rather than direct in-process calls.

## Partial Failure

A node can fail while other parts of the system continue operating.

---

# 13. What V1 Does NOT Attempt to Solve

The following are intentionally out of scope unless the core system is already complete.

- Multi-leader metadata
- Consensus implementation
- Raft
- Paxos
- Distributed metadata database
- Strong consistency across metadata replicas
- Concurrent modification of the same file
- POSIX filesystem semantics
- Mounting as an operating-system filesystem
- Erasure coding
- Geographic replication
- Complex load balancing
- Automatic cluster-wide rebalancing
- Petabyte-scale storage
- Production-grade authentication
- Fine-grained authorization
- Encryption at rest
- CDN behavior
- Kubernetes
- Service mesh
- Complex observability stack

AI agents must **not** introduce these technologies unless the team explicitly changes the scope.

---

# 14. V1 Demo Scenario

The demo should focus on the distributed behavior, not only on UI operations.

## Demo 1 — Normal Upload

1. Show three active Storage Nodes.
2. Upload a file.
3. Show that the file is split into multiple chunks.
4. Show replica placement across nodes.
5. Download the file successfully.

---

## Demo 2 — Node Failure

1. Stop Storage Node B.
2. Wait for the configured failure-detection interval.
3. Dashboard shows Node B as DOWN.
4. Download the previously uploaded file.
5. File still downloads using replicas on other nodes.

Key concept:

```text
Fault tolerance through replication
```

---

## Demo 3 — Replica Repair

1. Keep Node B down.
2. Show an under-replicated chunk.
3. Trigger or wait for replica repair.
4. System copies the chunk from a healthy node to another healthy node.
5. Replication factor returns to the target value.

---

## Demo 4 — Physical Host Failure

This scenario is used when the demo is deployed across two physical machines.

1. Confirm that replicas are distributed across `machine_A` and `machine_B`.
2. Disconnect Machine B from the network or stop all Storage Nodes on Machine B.
3. Metadata/API Service detects Node 2 and Node 3 as unavailable.
4. Download a file whose required chunks still have replicas on Machine A.
5. The file remains readable.
6. Show any chunks that have become under-replicated.

Key concept:

```text
A physical machine is a failure domain.
Replication should cross failure domains when possible.
```

This demo is more convincing than stopping a single Docker container because it demonstrates a real host/network failure.

---

## Demo 5 — Node Recovery

1. Reconnect/restart Machine B.
2. Node 2 and Node 3 resume health reporting/responding.
3. Dashboard marks them ACTIVE again.
4. Optional: demonstrate repair/reconciliation.

---

## Demo 6 — Known Limitation

Optionally stop the Metadata Service.

Explain:

```text
Storage data still physically exists,
but the current architecture cannot locate/manage it
because metadata is centralized.
```

Then explain this as a deliberate V1 trade-off and a future direction toward metadata replication and consensus.

---

# 15. Suggested UI

The UI must be functional but should not dominate implementation time.

Possible main screen:

```text
------------------------------------------------
Distributed File Storage

[ Upload File ]

Files
------------------------------------------------
report.pdf     14.3 MB    AVAILABLE   [Download]
photo.jpg       3.2 MB    AVAILABLE   [Download]
------------------------------------------------

Cluster
Node A   ACTIVE   210 MB used
Node B   ACTIVE   178 MB used
Node C   ACTIVE   192 MB used
------------------------------------------------
```

File detail screen:

```text
report.pdf

Chunk 0
  Node A
  Node B

Chunk 1
  Node B
  Node C

Chunk 2
  Node A
  Node C
```

This view is valuable for demonstration because it makes the distribution visible.

---

# 16. Technology Decisions

The V1 technology stack is now **locked** unless the team explicitly revises this document.

## 16.1 Frontend

```text
ReactJS
```

Frontend communicates with the Metadata/API Service using REST/HTTP.

---

## 16.2 Backend Language

```text
Python 3.12
```

Python is used for:

- Metadata/API Service;
- Storage Nodes.

---

## 16.3 Web API

```text
FastAPI
```

FastAPI runs inside the Metadata/API Service.

There is **no separate API Gateway service in V1**.

---

## 16.4 Internal RPC

```text
gRPC
grpcio
grpcio-tools
protobuf
```

gRPC is used for internal service communication with Storage Nodes.

`.proto` files are authoritative RPC contracts.

### V1 Transfer Mode

```text
Unary RPC
```

V1 does not require client streaming, server streaming, or bidirectional streaming.

Default storage chunk size:

```text
2 MiB
```

gRPC streaming is reserved as a post-V1 optimization.

---

## 16.5 Metadata Database

```text
PostgreSQL
```

Suggested Python persistence tools:

```text
SQLAlchemy
Alembic
```

PostgreSQL stores:

- files;
- chunks;
- chunk replicas;
- Storage Node registry/status.

PostgreSQL does **not** store actual file chunk bytes.

---

## 16.6 Chunk Storage

Storage Nodes store chunk bytes directly on independent filesystem volumes.

```text
Storage Node 1 -> Docker volume 1
Storage Node 2 -> Docker volume 2
Storage Node 3 -> Docker volume 3
```

---

## 16.7 Deployment

```text
Docker Compose
```

Docker is used for packaging and repeatable deployment, but V1 must not assume that all containers run on the same physical host.

Two supported modes are required:

```text
Local development:
all containers on one machine

Classroom demo:
containers/processes distributed across at least two physical machines
```

A practical approach is to use separate Compose profiles/files, for example:

```text
Machine A:
frontend
metadata-service
postgres
storage-node-1

Machine B:
storage-node-2
storage-node-3
```

The exact Compose organization may change, but service code must remain the same.

Expected V1 services:

```text
frontend
metadata-service
postgres
storage-node-1
storage-node-2
storage-node-3
```

Example ports:

```text
frontend            3000
metadata REST       8000
storage-node-1      50051
storage-node-2      50052
storage-node-3      50053
postgres            5432
```

Exact ports remain configurable.

Example configuration for a two-machine demo:

```env
STORAGE_NODE_1_HOST=192.168.1.10
STORAGE_NODE_1_PORT=50051
STORAGE_NODE_1_FAILURE_DOMAIN=machine_A

STORAGE_NODE_2_HOST=192.168.1.11
STORAGE_NODE_2_PORT=50052
STORAGE_NODE_2_FAILURE_DOMAIN=machine_B

STORAGE_NODE_3_HOST=192.168.1.11
STORAGE_NODE_3_PORT=50053
STORAGE_NODE_3_FAILURE_DOMAIN=machine_B
```

Equivalent configuration may instead be stored through node registration in PostgreSQL.

---

## 16.8 Explicitly Rejected for V1

Do not add these simply to make the system appear more advanced:

```text
separate API Gateway
Kafka
RabbitMQ
Redis
Kubernetes
service mesh
Raft
Paxos
gRPC-Web
distributed metadata database
```

They may be reconsidered only after the V1 Definition of Done is satisfied.

---

# 17. Implementation Priorities

The team must prioritize correctness of distributed behavior over extra business features.

Priority order:

```text
1. Metadata model
2. Storage Node API
3. Chunk upload/download
4. Replication
5. File reassembly
6. Active health polling
7. Failure-aware reads
8. Replica repair
9. UI / dashboard
10. Optional improvements
```

If deadlines are close, optional UI polish must be cut before distributed-system functionality.

---

# 18. Implementation Ownership

One person implements V1 sequentially. Build Storage first, then Metadata/DB and RF=2 upload/download, then reliability/cleanup/repair, then React and two-machine deployment. Do not assume three developers will deliver modules in parallel.

Other group members, if available, may help with the second demo machine, presentation and rehearsal. No coding ownership is assigned to them here.

---

# 19. Development Plan

`IMPLEMENTATION_PLAN.md` defines milestones M0–M6 and meaningful failure checks. Budget approximately 14–18 focused sessions including integration/rehearsal; elapsed calendar time depends on actual availability. The earlier three-person 2–3-week estimate is not a delivery promise for one developer.

Backend end-to-end behavior must work before UI polish. The finalized V1 includes manual repair, active health polling, durable pending cleanup and one Metadata worker with serialized data operations.

---

# 20. Possible Post-Presentation Extensions

Only after V1 is stable.

Possible extensions include:

### Metadata High Availability

- metadata replicas;
- leader/follower model;
- Raft-based consensus.

### File Versioning

```text
report.pdf v1
report.pdf v2
report.pdf v3
```

### Rebalancing

Move chunks when a new Storage Node joins.

### Better Failure Recovery

Full inventory reconciliation and automatic orphan cleanup. V1 already retries known pending deletions and verifies known replicas during reads/manual repair.

### Access Control

Users, authentication, permissions.

### Monitoring

Metrics such as:

- node availability;
- replica health;
- used capacity;
- upload/download throughput.

### Advanced Storage

- compression;
- deduplication;
- erasure coding.

These features must remain extensions rather than V1 requirements.

---

# 21. Rules for AI Coding Agents

Before implementing any feature, an AI agent should read this document.

The following rules apply:

1. Read this scope document together with ARCHITECTURE.md, API_CONTRACTS.md, storage.proto and IMPLEMENTATION_PLAN.md; do not implement from the overview alone.
2. V1 stack is React + Python 3.12 + FastAPI + gRPC + PostgreSQL + filesystem storage + Docker Compose.
3. Do not create a separate API Gateway in V1.
4. Browser-facing communication is REST/HTTP; Storage Node communication is gRPC.
5. V1 chunk transfer uses unary gRPC. Do not add streaming RPC without explicit approval.
6. Default storage chunk size is 2 MiB and must remain configurable.
7. Do not change the project architecture without explicit instruction.
8. Do not add technologies simply because they are common in production systems.
9. Do not introduce Kafka, RabbitMQ, Kubernetes, Redis, Raft, Paxos, gRPC-Web, or another infrastructure component unless requested.
10. Keep the Metadata/API Service centralized in V1.
11. Keep files immutable in V1.
12. Keep replication factor configurable, default 2.
13. PostgreSQL stores metadata, not actual chunk bytes.
14. Storage Nodes store chunk bytes on independent filesystem volumes and do not control global metadata.
15. React must not communicate directly with Storage Nodes in V1.
16. Storage Node addresses must be configurable; do not hard-code `localhost`.
17. Each Storage Node must have a `failure_domain` identifier.
18. Replica placement should prefer different failure domains when possible.
19. The same application code must support one-machine development and multi-machine deployment.
20. Prefer simple, explainable algorithms over sophisticated optimizations.
21. All new behavior must preserve the demo scenarios described in this document.
22. First check the technical contracts for ambiguities. Routine implementation choices are allowed if behavior is preserved; discuss material scope/contract changes instead of inventing a new architecture.
23. Avoid premature optimization.
24. Avoid production-scale complexity that is not required for the academic goal.
25. Any proposal that significantly changes APIs, persistence, replication, transfer mode, or failure handling should be discussed before implementation.

---

# 22. Definition of Done for V1

V1 is considered complete when all conditions below are satisfied.

### Functional

- [ ] File can be uploaded.
- [ ] File is divided into chunks.
- [ ] Chunks are stored on multiple Storage Nodes.
- [ ] Replicas are created according to replication factor.
- [ ] File can be listed.
- [ ] File can be downloaded and reconstructed correctly.
- [ ] File can be deleted.

### Distributed Behavior

- [ ] At least three Storage Node processes can run independently.
- [ ] Storage Node addresses are configurable and not tied to localhost.
- [ ] Each Storage Node has a configured failure domain.
- [ ] The same build can run all nodes on one machine for development.
- [ ] The system can be deployed across at least two physical machines for demonstration.
- [ ] Replica placement prefers different failure domains when possible.
- [ ] Storage Nodes report health and availability.
- [ ] Metadata Service detects unavailable node.
- [ ] Download can fall back to another replica.
- [ ] Failure of one Storage Node does not make a sufficiently replicated file unavailable.
- [ ] Under-replicated chunks can be identified.
- [ ] Manual replica repair can be demonstrated.
- [ ] Failed-upload and delete cleanup is persistent across Metadata restart.
- [ ] One Metadata worker serializes data operations as specified in ARCHITECTURE.md.
- [ ] Chunk checksum exists.

### Demo

- [ ] Cluster status can be observed.
- [ ] Chunk placement can be shown.
- [ ] A Storage Node can be intentionally stopped.
- [ ] A second physical machine hosting Storage Nodes can be disconnected/stopped during the demo.
- [ ] The system continues reading sufficiently replicated data when valid replicas remain in another failure domain.
- [ ] The team can explain the Metadata Service SPOF.
- [ ] The project starts reliably for the demo.

### Documentation

- [ ] Architecture documented.
- [ ] API contracts documented.
- [ ] Setup instructions documented.
- [ ] Demo procedure documented.
- [ ] Known limitations documented.

---

# 23. Key Architectural Statement

The core idea of the project can be summarized as:

> A centralized Python Metadata/API Service exposes REST/HTTP to the React frontend, stores authoritative metadata in PostgreSQL, processes files as fixed-size chunks, and communicates with independent Python Storage Nodes through gRPC. Actual chunk bytes are stored and replicated on separate filesystem volumes. Storage Nodes carry a `failure_domain` identifier so that replicas can be placed across different physical hosts when possible. The same codebase supports one-machine Docker development and multi-machine classroom deployment through configuration only. V1 uses unary gRPC with a configurable default chunk size of 2 MiB, intentionally accepts the Metadata/API Service as a single point of failure and data-path bottleneck, and focuses on data distribution, replication, failure detection, host-aware placement, fallback reads, and replica repair.

This statement should remain true throughout V1 implementation.

## Implementation clarification — two-machine failure

In the A/B topology, host B failure can leave all chunks readable on node-1 of A. However, only one Storage Node remains, so RF=2 cannot be restored until another node is available. Host A failure also removes Metadata/API and PostgreSQL; V1 does not tolerate that control-plane failure. Repair demonstration should stop one B node while the other B node remains reachable.
