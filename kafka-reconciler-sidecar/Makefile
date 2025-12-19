.PHONY: generate build test docker-build clean

# Generate gRPC code from proto
generate:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/reconciler/v1/reconciler.proto

# Build binary
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" \
		-o bin/kafka-reconciler cmd/server/main.go

# Run tests
test:
	go test -v ./...

# Build Docker image
docker-build:
	docker build -t kafka-reconciler-sidecar:latest .

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f api/reconciler/v1/*.pb.go

# Run locally (development)
run:
	go run cmd/server/main.go

# Install dependencies
deps:
	go mod download
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
