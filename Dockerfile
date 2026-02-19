# Stage 1: Build all binaries
FROM golang:1.24-alpine AS builder

WORKDIR /build

# Cache dependencies first
COPY go.mod ./
RUN go mod download

# Copy source
COPY . .

# Build main node binary
RUN go build -o rainstorm .

# Build CLI job-submission tool
RUN go build -o rainstorm-cli cmd/cli/main.go

# Build all stream operators
RUN for op in grep count transform identity echo output; do \
      cd ops/$op && go build -o $op . && cd /build; \
    done


# Stage 2: Minimal runtime image
FROM alpine:3.19

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/rainstorm       ./rainstorm
COPY --from=builder /build/rainstorm-cli   ./rainstorm-cli

# Copy operators into expected directory structure
RUN mkdir -p ops/grep ops/count ops/transform ops/identity ops/echo ops/output
COPY --from=builder /build/ops/grep/grep           ops/grep/grep
COPY --from=builder /build/ops/count/count         ops/count/count
COPY --from=builder /build/ops/transform/transform ops/transform/transform
COPY --from=builder /build/ops/identity/identity   ops/identity/identity
COPY --from=builder /build/ops/echo/echo           ops/echo/echo
COPY --from=builder /build/ops/output/output       ops/output/output

# Copy config and data
COPY config.json ./
COPY data/       ./data/

# Create directories used at runtime
RUN mkdir -p logs rainstorm_outputs hydfs_storage

# Default: run the node server
CMD ["./rainstorm"]
