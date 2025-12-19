# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build binary (statically linked)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /kafka-reconciler \
    cmd/server/main.go

# Runtime stage (distroless)
FROM gcr.io/distroless/static:nonroot

# Copy binary
COPY --from=builder /kafka-reconciler /kafka-reconciler

# Run as non-root
USER nonroot:nonroot

# Expose health probe port
EXPOSE 8080

# Run
ENTRYPOINT ["/kafka-reconciler"]
