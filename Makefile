.PHONY: build build-docker up down reset demo logs clean test tmux node1 status help

# ─── Local build ────────────────────────────────────────────────────────────

## build: Compile all binaries locally (requires Go 1.26+)
build:
	go build -o rainstorm .
	go build -o rainstorm-cli cmd/cli/main.go
	@for op in grep count transform identity echo output; do \
		cd ops/$$op && go build -o $$op . && cd ../..; \
	done
	@echo "Build complete: rainstorm, rainstorm-cli, and all operators"

## clean: Remove compiled binaries
clean:
	rm -f rainstorm rainstorm-cli
	@for op in grep count transform identity echo output; do \
		rm -f ops/$$op/$$op; \
	done
	@echo "Cleaned"

# ─── Docker cluster ──────────────────────────────────────────────────────────

## build-docker: Build the Docker image
build-docker:
	docker compose build

## up: Start the 10-node cluster (detached)
up:
	docker compose up -d
	@echo ""
	@echo "Cluster started. Wait ~5 seconds for SWIM membership to converge."
	@echo "  View logs:    make logs"
	@echo "  Run demo:     make demo"
	@echo "  Attach node1: docker attach node1"
	@echo "  Stop cluster: make down"

## down: Stop and remove all cluster containers (HyDFS volumes are kept)
down:
	docker compose down

## reset: Stop the cluster and wipe HyDFS state, then start fresh
## (exactly-once state persists in volumes; reset before re-running a demo)
reset:
	docker compose down -v
	docker compose up -d
	@echo ""
	@echo "Cluster reset with clean HyDFS state. Wait ~5 seconds for membership to converge."

## logs: Tail logs from all nodes
logs:
	docker compose logs -f

## restart: Rebuild image and restart cluster
restart: down build-docker up

# ─── Demo & testing ──────────────────────────────────────────────────────────

## demo: Run the end-to-end demo pipeline (filter + count)
demo:
	@./scripts/local/run_demo.sh

## test: Run smoke test — verify cluster is up and all 10 nodes joined
test:
	@echo "Checking cluster membership (via node1 client port)..."
	@docker exec node1 sh -c 'echo "list_mem" | nc -w 3 localhost 8003'
	@echo ""
	@echo "Checking container status:"
	@docker compose ps

# ─── Utilities ───────────────────────────────────────────────────────────────

## tmux: Open tmux session (node1 CLI + logs + shell)
tmux:
	@./scripts/local/tmux_cluster.sh

## node1: Attach to node1's interactive CLI
node1:
	docker attach node1

## status: Show running containers and their ports
status:
	docker compose ps

## help: Show this help
help:
	@grep -E '^## ' Makefile | sed 's/^## /  /'
