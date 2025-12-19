package reconciler

import (
	"context"
	"fmt"

	pb "github.com/numaproj/numaflow/kafka-reconciler-sidecar/api/reconciler/v1"
)

// DescribeTransaction queries Kafka for transaction state
func (c *Client) DescribeTransaction(ctx context.Context, transactionalID string) (pb.TransactionState, error) {
	// Use franz-go's DescribeTransactions
	result, err := c.admin.DescribeTransactions(ctx, transactionalID)
	if err != nil {
		return pb.TransactionState_TRANSACTION_STATE_UNKNOWN, fmt.Errorf("describe transactions failed: %w", err)
	}

	// Check if transaction exists
	txn, ok := result[transactionalID]
	if !ok {
		// Transaction not found - it may have auto-aborted or never existed
		return pb.TransactionState_TRANSACTION_STATE_ABORTED, nil
	}

	// Map franz-go state to proto enum
	// franz-go states: "Empty", "Ongoing", "PrepareCommit", "PrepareAbort", "CompleteCommit", "CompleteAbort"
	switch txn.State {
	case "CompleteCommit":
		return pb.TransactionState_TRANSACTION_STATE_COMMITTED, nil
	case "CompleteAbort", "Empty":
		return pb.TransactionState_TRANSACTION_STATE_ABORTED, nil
	case "Ongoing":
		return pb.TransactionState_TRANSACTION_STATE_ONGOING, nil
	case "PrepareCommit":
		return pb.TransactionState_TRANSACTION_STATE_PREPARE_COMMIT, nil
	case "PrepareAbort":
		return pb.TransactionState_TRANSACTION_STATE_PREPARE_ABORT, nil
	default:
		return pb.TransactionState_TRANSACTION_STATE_UNKNOWN, nil
	}
}

// AbortTransaction attempts to abort a transaction
// Note: This requires special privileges and may not be available in all Kafka versions
func (c *Client) AbortTransaction(ctx context.Context, transactionalID string, producerEpoch int32) error {
	// franz-go's EndTxn API requires producer context
	// For Phase 3B MVP: We rely on broker auto-abort (transaction.timeout.ms)
	// This is safe because we don't create new producers with same epoch

	// Return not-implemented for now
	// Future: Implement via producer fencing if needed
	return fmt.Errorf("abort transaction not implemented - relying on broker auto-abort after transaction.timeout.ms")
}
