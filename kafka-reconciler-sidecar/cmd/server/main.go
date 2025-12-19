package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	pb "github.com/numaproj/numaflow/kafka-reconciler-sidecar/api/reconciler/v1"
	grpcserver "github.com/numaproj/numaflow/kafka-reconciler-sidecar/internal/grpc"
	"github.com/numaproj/numaflow/kafka-reconciler-sidecar/internal/health"
)

func main() {
	log.Println("Starting Kafka Reconciler Sidecar v1.0.0")

	// Get listen address from environment (UDS or TCP)
	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = "unix:///var/run/kafka-reconciler/reconciler.sock"
	}

	// Health probe port
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8080"
	}

	// Start health probe
	healthProbe := health.NewProbe()
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", healthProbe.Handler())
		mux.HandleFunc("/readyz", healthProbe.Handler())

		addr := ":" + healthPort
		log.Printf("Health probe listening on %s\n", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatalf("Health probe failed: %v", err)
		}
	}()

	// Create gRPC server
	grpcSrv := grpcserver.NewServer()
	defer grpcSrv.Shutdown()

	// Parse listen address (unix:// or tcp://)
	var lis net.Listener
	var err error

	if len(listenAddr) > 7 && listenAddr[:7] == "unix://" {
		// Unix Domain Socket
		socketPath := listenAddr[7:]

		// Remove existing socket file
		os.Remove(socketPath)

		lis, err = net.Listen("unix", socketPath)
		if err != nil {
			log.Fatalf("Failed to listen on UDS: %v", err)
		}

		// Set socket permissions
		os.Chmod(socketPath, 0777)

		log.Printf("gRPC server listening on UDS: %s\n", socketPath)
	} else {
		// TCP
		lis, err = net.Listen("tcp", listenAddr)
		if err != nil {
			log.Fatalf("Failed to listen on TCP: %v", err)
		}
		log.Printf("gRPC server listening on TCP: %s\n", listenAddr)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.MaxConcurrentStreams(10), // Limit concurrent requests
	)
	pb.RegisterKafkaReconcilerServer(grpcServer, grpcSrv)

	// Graceful shutdown handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Printf("Received signal %v, shutting down gracefully...\n", sig)

		// Graceful stop with timeout
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
			log.Println("gRPC server stopped gracefully")
		case <-time.After(10 * time.Second):
			log.Println("Graceful stop timeout, forcing shutdown")
			grpcServer.Stop()
		}

		cancel()
	}()

	// Serve
	log.Println("Kafka Reconciler Sidecar ready")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server error: %v", err)
	}

	<-ctx.Done()
	log.Println("Shutdown complete")
}
