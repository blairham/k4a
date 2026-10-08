// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestMessage is the payload written to Kafka for connectivity testing.
type TestMessage struct {
	Timestamp time.Time `json:"timestamp"`
	ID        string    `json:"id"`
	Payload   string    `json:"payload"`
	Source    string    `json:"source"`
	Sequence  int       `json:"sequence"`
}

// GenerateMessages creates n test messages with random IDs and payloads.
func GenerateMessages(topic string, n int) ([]*kgo.Record, error) {
	messages := make([]*kgo.Record, 0, n)

	for i := range n {
		id, err := randomHex(16)
		if err != nil {
			return nil, fmt.Errorf("generating message ID: %w", err)
		}

		payload, err := randomHex(32)
		if err != nil {
			return nil, fmt.Errorf("generating payload: %w", err)
		}

		msg := TestMessage{
			ID:        id,
			Timestamp: time.Now().UTC(),
			Sequence:  i + 1,
			Payload:   payload,
			Source:    "k4a",
		}

		value, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("marshaling message: %w", err)
		}

		messages = append(messages, &kgo.Record{
			Topic: topic,
			Key:   []byte(id),
			Value: value,
		})
	}

	return messages, nil
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
