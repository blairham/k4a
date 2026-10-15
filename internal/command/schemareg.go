// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/schemareg"
)

// SchemaRegistryFlags overlay a context's schema-registry block, field by
// field (flag, then environment, then config).
type SchemaRegistryFlags struct {
	URL      string `long:"schema-registry-url"      env:"K4A_SCHEMA_REGISTRY_URL"      description:"Confluent Schema Registry URL; decodes Avro, Protobuf and JSON Schema payloads"`
	Username string `long:"schema-registry-username" env:"K4A_SCHEMA_REGISTRY_USERNAME" description:"Schema Registry basic-auth username"`
	Password string `long:"schema-registry-password" env:"K4A_SCHEMA_REGISTRY_PASSWORD" description:"Schema Registry basic-auth password"`
	Token    string `long:"schema-registry-token"    env:"K4A_SCHEMA_REGISTRY_TOKEN"    description:"Schema Registry bearer token"`
	CAFile   string `long:"schema-registry-ca"       env:"K4A_SCHEMA_REGISTRY_CA"       description:"CA certificate PEM file for the Schema Registry"`
}

func (f SchemaRegistryFlags) config() schemareg.Config {
	return schemareg.Config{
		URL:      f.URL,
		Username: f.Username,
		Password: f.Password,
		Token:    f.Token,
		CAFile:   f.CAFile,
	}
}

// registryConfig is the registry for a connection: the named (or current)
// context's block with the flags over it. A --brokers connection has no
// context, and an unreadable config degrades to the flags alone; the Kafka
// connection reports a broken config on its own.
func registryConfig(f SchemaRegistryFlags, configFile, contextName string, direct bool) schemareg.Config {
	if direct {
		return f.config()
	}
	if configFile == "" {
		configFile = config.DefaultConfigPath()
	}
	cfg, err := config.Load(configFile)
	if err != nil {
		return f.config()
	}
	ctx, err := cfg.Resolve(contextName)
	if err != nil {
		return f.config()
	}
	return ctx.SchemaRegistry.Config().Overlay(f.config())
}

// maxWarnings bounds the distinct "not decoded" reasons consume reports, so
// a topic of garbage naming a new schema ID per record cannot flood stderr.
const maxWarnings = 20

// consumeDecoder decodes `k4a consume` output and reports each distinct
// failure once on stderr, keeping stdout to the messages themselves.
type consumeDecoder struct {
	reg    *schemareg.Registry
	warned map[string]bool
	stderr io.Writer
}

func newConsumeDecoder(flags ConsumeFlags) (*consumeDecoder, error) {
	d := &consumeDecoder{warned: map[string]bool{}, stderr: os.Stderr}
	if flags.Decode == decodeRaw {
		return d, nil
	}
	cfg := registryConfig(flags.SchemaRegistry, flags.ConfigFile, flags.Context, flags.Brokers != "")
	reg, err := schemareg.NewIfSet(cfg)
	if err != nil {
		return nil, err
	}
	d.reg = reg
	return d, nil
}

const decodeRaw = "raw"

func (d *consumeDecoder) decode(ctx context.Context, m *kafka.ConsumedMessage) {
	if d.reg == nil {
		return
	}
	d.reg.DecodeMessage(ctx, m)
	for _, reason := range []string{m.KeyDecoding.Reason, m.ValueDecoding.Reason} {
		if reason == "" || d.warned[reason] || len(d.warned) >= maxWarnings {
			continue
		}
		d.warned[reason] = true
		fmt.Fprintf(d.stderr, "warning: not decoded, shown raw: %s\n", reason) //nolint:errcheck // best-effort stderr notice
	}
}

// schemaSuffix names the schemas that decoded a message, for --verbose.
func schemaSuffix(m kafka.ConsumedMessage) string {
	var s string
	if m.KeyDecoding.Decoded() {
		s += fmt.Sprintf(" key-schema=%q", m.KeyDecoding.Schema)
	}
	if m.ValueDecoding.Decoded() {
		s += fmt.Sprintf(" value-schema=%q", m.ValueDecoding.Schema)
	}
	return s
}
