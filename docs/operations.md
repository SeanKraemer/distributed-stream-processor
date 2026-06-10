# Local Operations Guide

This guide captures the fastest way to demonstrate the 10-node RainStorm/HyDFS cluster locally.

## Prerequisites

- Docker and Docker Compose
- Go 1.26 or newer for local binary builds
- `tmux` for the optional monitoring layout

## Quick Demo

```bash
make build-docker
make up
make test
make demo
make down
```

`make up` starts one leader/introducer node and nine worker nodes. Each node runs SWIM membership, HyDFS storage, and RainStorm stream processing services.

`make demo` uploads a dataset to HyDFS, submits a 2-stage filter+count pipeline with exactly-once semantics, waits for completion, and checks the aggregated results against ground truth (pattern `STOP`: 34 rows, 14 unique sign messages).

To re-run the demo, reset first — exactly-once dedup state persists in the HyDFS volumes and a stale-state re-run would (correctly) suppress all output:

```bash
make reset && make demo
```

## End-to-End Test Suite

```bash
./scripts/mp4/run_test.sh 1    # filter & count, verified against ground truth
./scripts/mp4/run_test.sh 2    # exactly-once under a mid-run task kill
./scripts/mp4/run_test.sh 3    # autoscaling under load
```

Each run resets the cluster, uploads data, submits the job, collects per-task outputs from all containers, and runs the matching `verify_test*.sh` script.

## Useful Commands

```bash
make logs      # stream all container logs
make status    # show container state and exposed ports
make tmux      # open node1 CLI, logs, and shell panes
make node1     # attach to the node1 interactive CLI
```

Detach from `docker attach node1` without stopping the container with `ctrl+a`, then `ctrl+d`.

## Fault Tolerance Check

With the cluster running:

```bash
docker stop node5
make logs
```

The leader should detect the failed worker through SWIM, reassign stream tasks, and continue processing with surviving nodes. HyDFS data is replicated across successors in the ring.

## Artifact Policy

Runtime logs, built binaries, and local Docker storage are intentionally ignored. Benchmark CSVs and plots may stay in the repository when they are small enough to explain system behavior, but new large raw datasets should be kept outside Git.
