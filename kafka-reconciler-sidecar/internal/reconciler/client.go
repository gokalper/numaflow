package reconciler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Client wraps franz-go for Kafka Admin operations
type Client struct {
	client *kgo.Client
	admin  *kadm.Client
}

// NewClient creates a franz-go client with Admin capabilities
// brokers: comma-separated list "broker1:9092,broker2:9092"
func NewClient(ctx context.Context, brokerList string, timeoutSec int) (*Client, error) {
	brokers := strings.Split(brokerList, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequestTimeoutOverhead(time.Duration(timeoutSec) * time.Second),
		kgo.ConnIdleTimeout(2 * time.Minute), // Idle connection timeout
	}

	kgoClient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	// Test connectivity
	if err := kgoClient.Ping(ctx); err != nil {
		kgoClient.Close()
		return nil, fmt.Errorf("failed to ping kafka brokers: %w", err)
	}

	return &Client{
		client: kgoClient,
		admin:  kadm.NewClient(kgoClient),
	}, nil
}

// Close releases resources
func (c *Client) Close() {
	if c.client != nil {
		c.client.Close()
	}
}
