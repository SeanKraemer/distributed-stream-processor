# Distributed Stream Processing System

[![CI](https://github.com/SeanKraemer/distributed-stream-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/SeanKraemer/distributed-stream-processor/actions/workflows/ci.yml)

A distributed, fault-tolerant stream processing framework built from scratch in Go. The system implements a full distributed computing stack — gossip-based failure detection, a consistent-hashing distributed file system, and a real-time stream processing engine with exactly-once semantics and dynamic autoscaling — all running across a 10-node cluster simulated locally via Docker Compose.

Originated as the cumulative project for UIUC's CS 425 Distributed Systems graduate course (Fall 2025); the stage-by-stage requirements are summarized in [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md).

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

# 3. Verify all 10 nodes joined (SWIM membership table)
make test

# 4. Run the end-to-end demo: upload a dataset to HyDFS, submit a
#    2-stage filter+count pipeline with exactly-once semantics, and
#    check the results against ground truth
make demo

# 5. Stop the cluster
make down
```

Exactly-once state persists in the HyDFS volumes between runs, so re-running the demo requires a clean slate:

```bash
make reset && make demo
```

To submit a job by hand (the CLI takes positional arguments — stages, tasks per stage, one operator + args per stage, then source, destination, exactly-once, autoscale, input rate, and low/high watermarks):

```bash
docker exec node1 ./rainstorm-cli \
  2 3 \
  grep --pattern=STOP --column=4 \
  count \
  dataset1.csv output.txt \
  true false 100 10 50
```

You can also attach to node1's interactive CLI with `make node1` (detach with `ctrl+a` then `ctrl+d`) — run `help` there for HyDFS and membership commands. Set `LOG_LEVEL=debug` before `make up` to enable per-tuple diagnostics in the node logs.

### Fault tolerance demo

```bash
# With the cluster running and a job submitted, kill a worker:
docker stop node5

# node5's tasks are detected as failed within ~6 seconds (SWIM)
# The ResourceManager reassigns them to surviving workers
# With exactly-once enabled, no tuples are lost or duplicated
```

---

## Testing

Unit tests cover the deterministic cores of each subsystem — the consistent-hash ring (including the minimal-movement redistribution property), SWIM membership state transitions, the HyDFS block store's per-client sequencing and merge ordering, RainStorm's hash partitioning, and the operator binaries:

```bash
go test ./... -race
```

End-to-end behavior is exercised against the live cluster:

```bash
./scripts/mp4/run_test.sh 1   # filter & count, verified against ground truth
./scripts/mp4/run_test.sh 2   # exactly-once under a mid-run task kill
./scripts/mp4/run_test.sh 3   # autoscaling under load
```

CI runs build, vet, gofmt, the race-enabled test suite, golangci-lint, and a Docker job that boots the full 10-node cluster and asserts membership converges.

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

Benchmarked RainStorm against Apache Spark Streaming on a 10-node cluster (equal data, equal task counts; 4 workloads × 3 runs each; mean throughput ± std dev):

| Workload                  | RainStorm          | Spark Streaming    | Ratio |
|---------------------------|--------------------|--------------------|-------|
| 5K tuples, filter+count   | 107.0 ± 0.6 t/s    | 346.5 ± 15.4 t/s   | 3.2×  |
| 5K tuples, filter+transform | 106.3 ± 0.6 t/s  | 357.0 ± 27.3 t/s   | 3.4×  |
| 10K tuples, filter+count  | 161.6 ± 0.3 t/s    | 688.9 ± 13.1 t/s   | 4.3×  |
| 10K tuples, filter+transform | 160.1 ± 1.4 t/s | 699.4 ± 43.6 t/s   | 4.4×  |

**Takeaways:** Spark is 3–4× faster due to JVM optimizations and micro-batching, and the gap widens with volume because RainStorm's exactly-once semantics put per-tuple HyDFS checkpointing on the critical path, which scales worse than amortized batch checkpoints. Two second-order observations worth noting: RainStorm's run-to-run variance is tiny (std 0.3–1.4) while Spark's is large (13–44, JVM GC), and both systems' throughput is insensitive to stateless vs stateful operators (±3%) because serialization and ACK overhead dominate at this key cardinality. Raw data lives in `experiment_results/`.

---

## Fault Tolerance Model

- **Failure detection:** SWIM with suspicion — O(log n) convergence, tunable false-positive rate
- **Storage failures:** HyDFS re-replicates to 3 successors when a node leaves; re-balances on join
- **Task failures (exactly-once):** Each task appends processed tuple IDs and results to a HyDFS log before ACKing upstream. On recovery, the task replays its log to restore state without reprocessing.
- **Task failures (autoscaling mode):** At-least-once delivery; duplicates possible but throughput is maintained. Rescaling is not epoch-coordinated, so a small number of in-flight tuples can be lost or duplicated at the moment tasks are added or drained — the autoscaling verifier allows a 0.5% tolerance for this
- **Leader failure:** Not currently tolerated (single leader design; leader is assumed stable)

---

## Design Notes & Limitations

- Single leader is a bottleneck and single point of failure for the control plane
- Autoscaling and exactly-once are mutually exclusive (autoscaling uses at-least-once)
- Practical cluster size: tested up to 10 nodes; SWIM gossip scales to hundreds but the HyDFS coordinator is not distributed
- Exactly-once state logs are keyed by task ID, so consecutive jobs share state — reset the cluster (`make reset`) between runs rather than relying on job isolation
- HyDFS create replication waits a fixed delay for the pipelined ACK instead of tracking per-request ACKs; failures surface later through re-replication and merge
- Final job output is collected from per-worker files out-of-band (`scripts/mp4/collect_output.sh`) rather than merged into HyDFS by the leader
- Two parallel ring implementations exist (`pkg/hashing` using SHA-256 with liveness filtering; `pkg/fileops` using SHA-1 without it) — drift from an earlier coordinator design rather than intent. Kept as-is and pinned by tests; unifying on the SHA-256 ring is the obvious refactor
- Autoscaling decisions require the watermark breach to be sustained for ~3 s, observe a cooldown between scaling events, and never scale the source stage; downscaled tasks drain via routing-table update and EOF markers before exiting
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
