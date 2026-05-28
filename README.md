# Distributed Stream Processing System

A distributed, fault-tolerant stream processing framework built from scratch in Go. The system implements a full distributed computing stack — gossip-based failure detection, a consistent-hashing distributed file system, and a real-time stream processing engine with exactly-once semantics and dynamic autoscaling — all running across a 10-node cluster simulated locally via Docker Compose.

---

## Architecture

```
  Client
    │  submit job (TCP)
    ▼
┌──────────────────────────────────────────────────────────┐
│  Leader (node1)                                          │
│                                                          │
│  ┌──────────────┐   ┌──────────────┐                     │
│  │ RainStorm RM │   │ SWIM Member  │                     │
│  │ (scheduler)  │   │ (gossip/UDP) │                     │
│  └──────┬───────┘   └──────────────┘                     │
│         │ assign tasks                                   │
└─────────┼────────────────────────────────────────────────┘
          │
          ▼ (TCP, hash-partitioned tuples)
┌──────────────────────────────────────────────────────────┐
│  Workers (node2–node10)                                  │
│                                                          │
│   Stage 1 Tasks           Stage 2 Tasks                  │
│   ┌──────────┐            ┌──────────┐                   │
│   │ Operator │ ─tuples──► │ Operator │ ─► HyDFS output   │
│   │ (filter) │            │ (count)  │                   │
│   └──────────┘            └──────────┘                   │
│                                                          │
│   Each worker also runs: HyDFS node + SWIM member        │
└──────────────────────────────────────────────────────────┘
          │  ▲
          │  │  replicated file I/O (TCP)
          ▼  │
┌──────────────────────────────────────────────────────────┐
│  HyDFS — Distributed File System                         │
│  Consistent hashing ring, 3× replication                 │
│  Per-client append ordering + eventual consistency       │
└──────────────────────────────────────────────────────────┘
```

---

## Core Subsystems

### 1. Stream Processing Engine — RainStorm (`pkg/rainstorm/`)

- Leader-worker model: leader schedules tasks, workers execute operator binaries
- Up to 3 pipeline stages; parallel tasks per stage with hash partitioning
- Operator types: **Filter** (grep), **Transform** (field extraction), **AggregateByKey** (count)
- User-defined operators are standalone binaries, decoupled from the framework
- Two key features:
  - **Exactly-once semantics**: HyDFS-backed state logs; failed tasks recover by replaying log
  - **Dynamic autoscaling**: ResourceManager monitors per-task throughput and adds/removes tasks at runtime

### 2. Distributed File System — HyDFS (`pkg/fileops/`, `pkg/storage/`)

- Flat namespace, consistent hashing ring for O(1) file routing
- 3× replication across successor nodes in the ring
- Operations: `create`, `get`, `append`, `merge`
- Guarantees: per-client append ordering, read-your-writes, eventual consistency
- Automatic re-replication and re-balancing on node join/leave

### 3. Failure Detection — SWIM Gossip Protocol (`pkg/membership/`)

- Each node maintains a full membership list updated via UDP gossip
- Direct ping + indirect ping-request for failure detection
- Suspicion mechanism reduces false positives under packet loss
- Converges across 10 nodes within 3–6 seconds of a failure

### 4. Distributed Log Query (`pkg/logging/`)

- Parallel grep across all live cluster nodes
- Used for cluster-wide debugging and monitoring

---

## Running Locally

**Requirements:** Docker, Docker Compose, Go 1.26+

For a concise operator-focused walkthrough, see [docs/operations.md](docs/operations.md).

```bash
# 1. Build the Docker image (first time only, or after code changes)
make build-docker

# 2. Start the 10-node cluster
make up

# 3. View membership converging across all nodes
make logs

# 4. Attach to node1's interactive CLI
docker attach node1

# 5. Inside node1 — upload data and submit a streaming job:
create /app/data/dataset1.csv dataset1.csv
# (ctrl+a ctrl+d to detach without stopping, or open new terminal)

# 6. Submit via the CLI binary from your host terminal:
docker exec node1 ./rainstorm-cli \
  --stages=2 --tasks=3 \
  --op1=grep --op1-args="--pattern=SEVERE --column=4" \
  --op2=count \
  --src=dataset1.csv --dest=output.txt \
  --exactly-once=true --autoscale=false \
  --input-rate=100 --lw=10 --hw=50

# 7. Stop the cluster
make down
```

### Fault tolerance demo

```bash
# With the cluster running and a job submitted, kill a worker:
docker stop node5

# node5's tasks are detected as failed within ~6 seconds (SWIM)
# The ResourceManager reassigns them to surviving workers
# With exactly-once enabled, no tuples are lost or duplicated
```

---

## Project Structure

```
.
├── main.go                   # Node server entrypoint (SWIM + HyDFS + RainStorm)
├── cmd/cli/main.go           # Job submission CLI
├── pkg/
│   ├── membership/           # SWIM gossip failure detector
│   ├── fileops/              # HyDFS file coordinator + replica handler
│   ├── storage/              # Block-level storage engine
│   ├── hashing/              # Consistent hashing ring
│   ├── rainstorm/            # Stream processing (leader, worker, API)
│   ├── network/              # TCP client/server utilities
│   ├── logging/              # Distributed log query
│   └── common/               # Shared config and types
├── ops/                      # Stream operator binaries
│   ├── grep/                 # Filter operator
│   ├── count/                # AggregateByKey operator
│   ├── transform/            # Transform operator
│   ├── identity/             # Pass-through (baseline)
│   ├── echo/                 # Echo operator
│   └── output/               # Output writer
├── scripts/
│   ├── local/                # Docker cluster management scripts
│   ├── mp4/                  # End-to-end demo and verification scripts
│   └── experiments/          # RainStorm vs Spark benchmark scripts
├── spark_apps/               # Spark Streaming equivalents (for comparison)
├── data/                     # Sample datasets
├── experiment_results/       # Benchmark CSVs
├── plots/                    # Performance comparison charts
├── Dockerfile
├── docker-compose.yml        # 10-node cluster definition
└── Makefile
```

---

## Performance

Benchmarked RainStorm against Apache Spark Streaming (equal cluster, equal data, equal task counts):

| System          | 5K tuples       | 10K tuples      |
|-----------------|-----------------|-----------------|
| RainStorm       | ~107 tuples/sec | ~161 tuples/sec |
| Spark Streaming | ~352 tuples/sec | ~694 tuples/sec |

**Takeaway:** Spark is 2–4× faster due to JVM optimizations and in-memory execution. RainStorm trades raw throughput for stronger fault guarantees — exactly-once semantics require per-tuple checkpointing to the distributed file system, which introduces HyDFS I/O on the critical path.

---

## Fault Tolerance Model

- **Failure detection:** SWIM with suspicion — O(log n) convergence, tunable false-positive rate
- **Storage failures:** HyDFS re-replicates to 3 successors when a node leaves; re-balances on join
- **Task failures (exactly-once):** Each task appends processed tuple IDs and results to a HyDFS log before ACKing upstream. On recovery, the task replays its log to restore state without reprocessing.
- **Task failures (autoscaling mode):** At-least-once delivery; duplicates possible but throughput is maintained
- **Leader failure:** Not currently tolerated (single leader design; leader is assumed stable)

---

## Limitations

- Single leader is a bottleneck and single point of failure for the control plane
- Autoscaling and exactly-once are mutually exclusive (autoscaling uses at-least-once)
- Practical cluster size: tested up to 10 nodes; SWIM gossip scales to hundreds but the HyDFS coordinator is not distributed
- No encryption or authentication on inter-node communication
- The Spark comparison requires a separate Spark cluster (not included in Docker Compose setup)

---

## What I Learned

Building this system end-to-end reinforced several distributed systems principles that are easy to understand abstractly but hard to get right in practice:

- **Gossip is surprisingly robust** — SWIM's indirect ping-request dramatically reduces false positives under network congestion without adding much complexity.
- **Consistency is a spectrum** — HyDFS's per-client ordering guarantee is weaker than linearizability but sufficient for stream processing, and it's far easier to implement correctly.
- **Exactly-once is expensive** — the HyDFS checkpoint on every tuple is the dominant cost in RainStorm's throughput vs. Spark. Production systems like Kafka and Flink use epoch-based checkpointing to amortize this.
- **Autoscaling and fault tolerance interact** — adding tasks mid-stream invalidates in-flight tuple routing tables, which is why the two features cannot be combined safely without a global coordination step.
- **Operator isolation is valuable** — making operators standalone binaries (rather than plugins or goroutines) made the system much easier to debug and test independently.

---

## Tech Stack

- **Language:** Go 1.26+
- **Communication:** TCP (control plane, file ops, tuple routing), UDP (SWIM gossip)
- **Storage:** Custom block store with append-only log semantics
- **Infrastructure:** Docker, Docker Compose
- **Benchmarking:** Apache Spark Streaming (comparison), Python (data generation + plotting)
