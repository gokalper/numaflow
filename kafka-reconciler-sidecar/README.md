# Kafka Reconciler Sidecar

Go-based sidecar for Kafka Admin API operations to support Phase 2 Enhanced MonoVertex reconciliation.

## Purpose

Provides Kafka transaction reconciliation capabilities using `franz-go` that are not available in `rdkafka`:
- `DescribeTransactions` - Query transaction state
- `AbortTransaction` - Clean up hanging transactions
- `ListConsumerGroupOffsets` - Verify offset alignment

## Architecture

- **Runtime**: Go 1.21+
- **Kafka Client**: franz-go v1.15+
- **IPC**: gRPC over Unix Domain Sockets
- **Resource Limits**: 64MB memory, 100m CPU

## Building

```bash
# Generate gRPC code
make generate

# Build binary
make build

# Build container
make docker-build
```

## Usage

The sidecar runs as a co-located container in the same Pod as the Rust numaflow core:

```yaml
containers:
- name: kafka-reconciler
  image: kafka-reconciler-sidecar:v1.0.0
  resources:
    limits:
      memory: "64Mi"
      cpu: "100m"
  volumeMounts:
  - name: reconciler-sock
    mountPath: /var/run/kafka-reconciler
```

The Rust core connects via UDS at `/var/run/kafka-reconciler/reconciler.sock`.

## API

See `api/reconciler/v1/reconciler.proto` for the full gRPC API definition.

## Development

```bash
# Run tests
make test

# Run locally
go run cmd/server/main.go
```

## License

Apache 2.0
