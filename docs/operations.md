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
