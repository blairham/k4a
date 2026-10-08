// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProduceResult holds the outcome of producing a single message.
type ProduceResult struct {
	Err      error
	Key      string
	Sequence int
	Duration time.Duration
}

// ProduceAll writes all messages to Kafka one at a time, calling onResult after each.
func (c *Client) ProduceAll(
	ctx context.Context,
	messages []*kgo.Record,
	interval time.Duration,
	onResult func(ProduceResult),
) {
	for i, msg := range messages {
		if ctx.Err() != nil {
			return
		}

		if i > 0 && interval > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}

		start := time.Now()
		results := c.kgoClient.ProduceSync(ctx, msg)
		err := results.FirstErr()
		onResult(ProduceResult{
			Sequence: i + 1,
			Key:      string(msg.Key),
			Err:      err,
			Duration: time.Since(start),
		})
	}
}
