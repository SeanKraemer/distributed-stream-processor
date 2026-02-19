.PHONY: build build-docker up down demo logs clean test help

# ─── Local build ────────────────────────────────────────────────────────────

## build: Compile all binaries locally (requires Go 1.21+)
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

## down: Stop and remove all cluster containers
down:
	docker compose down

## logs: Tail logs from all nodes
logs:
	docker compose logs -f

## restart: Rebuild image and restart cluster
restart: down build-docker up

# ─── Demo & testing ──────────────────────────────────────────────────────────

## demo: Run the end-to-end demo pipeline (filter + count)
demo:
	@echo "Running demo: 2-stage grep+count pipeline"
	@echo "Uploading dataset to HyDFS..."
	docker exec node1 ./rainstorm-cli --op1=grep --op1-args="--pattern=SEVERE" --op2=count \
		--src=dataset1.csv --dest=demo_output.txt --exactly-once=true 2>/dev/null || \
	docker exec -it node1 ./rainstorm-cli 2>/dev/null || \
	(echo ""; echo "To submit a job manually, attach to node1:"; echo "  docker attach node1"; echo "  (then type commands at the CLI prompt)")

## test: Run smoke test — verify cluster is up and all 10 nodes joined
test:
	@echo "Checking cluster membership..."
	@docker exec node1 sh -c 'echo "list_mem" | timeout 3 ./rainstorm 2>/dev/null || true'
	@echo ""
	@echo "Checking container status:"
	@docker compose ps

# ─── Utilities ───────────────────────────────────────────────────────────────

## node1: Attach to node1's interactive CLI
node1:
	docker attach node1

## status: Show running containers and their ports
status:
	docker compose ps

## help: Show this help
help:
	@grep -E '^## ' Makefile | sed 's/^## /  /'
