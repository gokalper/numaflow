package reconciler

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
)

// ListConsumerGroupOffsets retrieves committed offsets for a consumer group
func (c *Client) ListConsumerGroupOffsets(ctx context.Context, group string, topics []string) (map[string]int64, error) {
	// Fetch offsets for the consumer group
	offsets, err := c.admin.FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch consumer group offsets: %w", err)
	}

	// Convert to map[string]int64 format: "topic-partition" -> offset
	result := make(map[string]int64)

	offsets.Each(func(o kadm.OffsetResponse) {
		// Format: topic-partition
		key := fmt.Sprintf("%s-%d", o.Topic, o.Partition)
		result[key] = o.Offset.At
	})

	return result, nil
}
