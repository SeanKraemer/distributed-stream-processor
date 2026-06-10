# Project Requirements

This system was built as the cumulative project for UIUC's CS 425 Distributed
Systems graduate course (Fall 2025), delivered as four machine-programming
assignments that build on each other. The original course specs are not
redistributed here; this document summarizes, in my own words, what each
stage had to do. The whole stack was developed and demoed on a 10-node
cluster and is reproduced locally with Docker Compose.

## Stage 1 — Distributed Log Querier (`pkg/logging`)

A cluster-wide grep: from any node, run a query that fans out in parallel to
every machine in the cluster, greps that machine's local log file, and
aggregates the results (with per-machine line counts) back to the caller.

- Queries run concurrently against all nodes; one machine per log file.
- Must tolerate worker failures mid-query: results from reachable machines
  still come back.
- Built first because everything after it needs cluster-wide log debugging.

## Stage 2 — Distributed Group Membership (`pkg/membership`)

Every node maintains a full membership list of the group, kept current via a
failure-detection protocol with no leader. Two protocol variants, sharing a
common suspicion mechanism:

- **Gossip-style heartbeating** with suspicion.
- **Ping-ack (SWIM-style)** with suspicion — the variant this system runs.

Required properties:

- A failure must appear in at least one membership list within **3 seconds**,
  and in **all** lists within **6 seconds** (time-bounded completeness).
- Must tolerate up to **3 simultaneous failures**, with the next failure set
  at least ~15 seconds out (time to re-converge).
- Crash/fail-stop model: a rejoining machine comes back with a new versioned
  node ID (incarnation), distinguishable from its previous life.
- Joins go through a fixed introducer; detection itself is fully
  decentralized and bandwidth-efficient over UDP.

## Stage 3 — HyDFS, a Hybrid Distributed File System (`pkg/fileops`, `pkg/storage`, `pkg/hashing`)

A flat (no directories) replicated file system blending HDFS and Cassandra
ideas: **consistent hashing** places both nodes and files on a ring, each
file stored on its first *n* successor nodes (n = 3 here), routed in O(1)
using only the membership list — no central metadata service.

Operations: `create`, `get`, `append`, plus `merge` (force replicas of a file
to identical block order) and inspection commands (`ls`, `liststore`,
`getfromreplica`, `multiappend` for concurrent-append testing).

Required guarantees:

- Tolerates **2 simultaneous machine failures**; data is re-replicated after
  any failure and re-balanced when machines join, always keeping exactly the
  first *n* successors as replicas (no over-replication).
- **Per-client append ordering** — one client's appends to a file land in
  the order issued.
- **Eventual consistency** — once updates quiesce, all replicas of a file
  converge to identical contents.
- **Read-my-writes** — a client immediately sees its own completed appends.

## Stage 4 — RainStorm, a Stream-Processing Framework (`pkg/rainstorm`, `ops/`)

A leader/worker streaming engine in the spirit of Storm and Flink, built on
top of Stages 2 and 3: the source reads input from HyDFS, user-defined
operator executables transform tuples, and the final stage appends results
to a HyDFS output file.

- Jobs are pipelines of up to **3 processing stages**, each with N parallel
  tasks; streams are `<key, value>` tuples partitioned across the next
  stage's tasks by key hash.
- **No barriers** (unlike MapReduce): every tuple is forwarded downstream
  immediately.
- Three operator types: **Transform**, **Filter**, and stateful
  **AggregateByKey** (e.g. running counts).
- **Failure tolerance with exactly-once processing**: worker/task failures
  are detected via the Stage 2 membership protocol; the leader reschedules
  tasks, and each task persists its processed-tuple IDs and aggregate state
  to HyDFS state logs so a restarted task resumes without dropping or
  double-counting tuples.
- **Dynamic autoscaling** (operated as a separate mode from exactly-once):
  the leader monitors per-task throughput against low/high watermarks and
  adds or removes tasks at runtime, rebalancing the stream.
- Performance had to be compared experimentally against **Apache Spark
  Streaming** on the same workloads (see `docs/EXPERIMENT_GUIDE.md` and
  `plots/`).

## Demo applications

Two reference pipelines exercised in grading demos, both over a traffic-sign
dataset (CSV):

1. **Filter & count** — `grep <pattern>` → `count` by CSV column key
   (exactly-once mode, with a mid-run task kill to prove no loss or
   duplication).
2. **Filter & transform** — `grep <pattern>` → field extraction
   (autoscaling mode under varying input rates).
