// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

// TopicSchema is a YAML file describing test messages for one topic: the
// records to cycle through, and which of their fields becomes the record key.
type TopicSchema struct {
	Topic   string           `yaml:"topic"`
	Key     string           `yaml:"key"`
	Samples []map[string]any `yaml:"samples"`
}

// LoadTopicSchema reads a YAML topic schema file.
func LoadTopicSchema(path string) (*TopicSchema, error) {
	data, err := os.ReadFile(path) //nolint:gosec // user-provided schema path
	if err != nil {
		return nil, fmt.Errorf("reading schema: %w", err)
	}
	return ParseTopicSchema(data)
}

// ParseTopicSchema parses and validates a YAML topic schema.
func ParseTopicSchema(data []byte) (*TopicSchema, error) {
	var schema TopicSchema
	if err := yaml.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("parsing schema: %w", err)
	}

	if len(schema.Samples) == 0 {
		return nil, fmt.Errorf("schema has no samples")
	}
	for i, s := range schema.Samples {
		// A bare `-` or `~` entry decodes to a nil map, which producing
		// would write seq into and panic.
		if s == nil {
			return nil, fmt.Errorf("samples[%d] is empty; each sample must be a mapping", i)
		}
		// Producing marshals every sample to JSON; a value JSON cannot hold
		// (a non-string mapping key, .nan, .inf) fails here rather than
		// after the run has started.
		if _, err := json.Marshal(s); err != nil {
			return nil, fmt.Errorf("samples[%d] cannot be encoded as JSON: %w", i, err)
		}
		if schema.Key != "" {
			if _, ok := s[schema.Key]; !ok {
				return nil, fmt.Errorf("samples[%d] has no %q field to use as the key", i, schema.Key)
			}
		}
	}

	return &schema, nil
}

// GenerateSchemaMessages creates n messages by cycling through the schema's
// samples. Each message is the sample plus a `seq` counter and a `producedAt`
// timestamp, so successive copies of one sample stay distinguishable.
func GenerateSchemaMessages(schema *TopicSchema, n int) ([]*kgo.Record, error) {
	messages := make([]*kgo.Record, 0, n)
	base := time.Now().UTC()

	for i := range n {
		sample := schema.Samples[i%len(schema.Samples)]
		msg := maps.Clone(sample)
		msg["seq"] = i
		msg["producedAt"] = base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano)

		value, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("marshaling message: %w", err)
		}

		rec := &kgo.Record{Topic: schema.Topic, Value: value}
		if schema.Key != "" {
			rec.Key = fmt.Append(nil, sample[schema.Key])
		}
		messages = append(messages, rec)
	}

	return messages, nil
}
