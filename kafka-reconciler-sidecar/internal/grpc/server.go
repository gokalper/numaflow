package grpc

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/numaproj/numaflow/kafka-reconciler-sidecar/api/reconciler/v1"
	"github.com/numaproj/numaflow/kafka-reconciler-sidecar/internal/reconciler"
)

// Server implements the KafkaReconciler gRPC service
type Server struct {
	pb.UnimplementedKafkaReconcilerServer

	// Client cache with idle timeout
	mu             sync.Mutex
	cachedClient   *reconciler.Client
	lastAccessTime time.Time
	idleTimeout    time.Duration

	// Server start time for uptime
	startTime time.Time
}

// NewServer creates a new gRPC server
func NewServer() *Server {
	return &Server{
		idleTimeout: 2 * time.Minute, // Close client after 2min idle
		startTime:   time.Now(),
	}
}

// getOrCreateClient returns cached client or creates new one
func (s *Server) getOrCreateClient(ctx context.Context, brokerList string, timeoutMs int32) (*reconciler.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if cached client exists and is still valid
	if s.cachedClient != nil {
		// Check idle timeout
		if time.Since(s.lastAccessTime) > s.idleTimeout {
			log.Println("Client idle timeout, closing...")
			s.cachedClient.Close()
			s.cachedClient = nil
		}
	}

	// Create new client if needed
	if s.cachedClient == nil {
		log.Printf("Creating new Kafka client for brokers: %s\n", brokerList)
		client, err := reconciler.NewClient(ctx, brokerList, int(timeoutMs/1000))
		if err != nil {
			return nil, err
		}
		s.cachedClient = client
	}

	s.lastAccessTime = time.Now()
	return s.cachedClient, nil
}

// DescribeTransaction implements the gRPC method
func (s *Server) DescribeTransaction(ctx context.Context, req *pb.DescribeTransactionRequest) (*pb.DescribeTransactionResponse, error) {
	log.Printf("DescribeTransaction: txn_id=%s, brokers=%s\n", req.TransactionalId, req.BrokerList)

	// Set timeout from request
	ctx, cancel := context.WithTimeout(ctx, time.Duration(req.TimeoutMs)*time.Millisecond)
	defer cancel()

	// Get or create client
	client, err := s.getOrCreateClient(ctx, req.BrokerList, req.TimeoutMs)
	if err != nil {
		return &pb.DescribeTransactionResponse{
			State: pb.TransactionState_TRANSACTION_STATE_UNKNOWN,
			Error: fmt.Sprintf("failed to create kafka client: %v", err),
		}, nil // Return gRPC success with error in response
	}

	// Query transaction state
	state, err := client.DescribeTransaction(ctx, req.TransactionalId)
	if err != nil {
		return &pb.DescribeTransactionResponse{
			State: pb.TransactionState_TRANSACTION_STATE_UNKNOWN,
			Error: fmt.Sprintf("failed to describe transaction: %v", err),
		}, nil
	}

	log.Printf("Transaction %s state: %v\n", req.TransactionalId, state)
	return &pb.DescribeTransactionResponse{
		State: state,
		Error: "",
	}, nil
}

// AbortTransaction implements the gRPC method
func (s *Server) AbortTransaction(ctx context.Context, req *pb.AbortTransactionRequest) (*pb.AbortTransactionResponse, error) {
	log.Printf("AbortTransaction: txn_id=%s, epoch=%d\n", req.TransactionalId, req.ProducerEpoch)

	// Set timeout
	ctx, cancel := context.WithTimeout(ctx, time.Duration(req.TimeoutMs)*time.Millisecond)
	defer cancel()

	// Get or create client
	client, err := s.getOrCreateClient(ctx, req.BrokerList, req.TimeoutMs)
	if err != nil {
		return &pb.AbortTransactionResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create kafka client: %v", err),
		}, nil
	}

	// Attempt abort
	err = client.AbortTransaction(ctx, req.TransactionalId, req.ProducerEpoch)
	if err != nil {
		return &pb.AbortTransactionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.AbortTransactionResponse{
		Success: true,
		Error:   "",
	}, nil
}

// ListConsumerGroupOffsets implements the gRPC method
func (s *Server) ListConsumerGroupOffsets(ctx context.Context, req *pb.ListConsumerGroupOffsetsRequest) (*pb.ListConsumerGroupOffsetsResponse, error) {
	log.Printf("ListConsumerGroupOffsets: group=%s, topics=%v\n", req.ConsumerGroup, req.Topics)

	// Set timeout
	ctx, cancel := context.WithTimeout(ctx, time.Duration(req.TimeoutMs)*time.Millisecond)
	defer cancel()

	// Get or create client
	client, err := s.getOrCreateClient(ctx, req.BrokerList, req.TimeoutMs)
	if err != nil {
		return &pb.ListConsumerGroupOffsetsResponse{
			PartitionOffsets: nil,
			Error:            fmt.Sprintf("failed to create kafka client: %v", err),
		}, nil
	}

	// List offsets
	offsets, err := client.ListConsumerGroupOffsets(ctx, req.ConsumerGroup, req.Topics)
	if err != nil {
		return &pb.ListConsumerGroupOffsetsResponse{
			PartitionOffsets: nil,
			Error:            err.Error(),
		}, nil
	}

	return &pb.ListConsumerGroupOffsetsResponse{
		PartitionOffsets: offsets,
		Error:            "",
	}, nil
}

// Health implements the gRPC method
func (s *Server) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	uptime := time.Since(s.startTime).Seconds()
	return &pb.HealthResponse{
		Healthy:       true,
		Version:       "v1.0.0",
		UptimeSeconds: int64(uptime),
	}, nil
}

// Shutdown cleans up resources
func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cachedClient != nil {
		log.Println("Shutting down, closing Kafka client...")
		s.cachedClient.Close()
		s.cachedClient = nil
	}
}
