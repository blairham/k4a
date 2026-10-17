// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/schemareg"
)

// registryFlags names the Schema Registry the daemon decodes framed keys and
// values with before indexing them, the same flags and environment the CLI
// reads. A client serves a search from this daemon only when it decodes with
// the same registry (docs/design/schema-registry.md § Search and the indexes).
type registryFlags struct {
	URL      string `long:"schema-registry-url"      env:"K4A_SCHEMA_REGISTRY_URL"      description:"Confluent Schema Registry URL: index Avro, Protobuf and JSON-Schema payloads as their decoded JSON. Clients must decode with the same registry to be served from the index."`
	Username string `long:"schema-registry-username" env:"K4A_SCHEMA_REGISTRY_USERNAME" description:"Schema Registry basic-auth username"`
	Password string `long:"schema-registry-password" env:"K4A_SCHEMA_REGISTRY_PASSWORD" description:"Schema Registry basic-auth password"`
	Token    string `long:"schema-registry-token"    env:"K4A_SCHEMA_REGISTRY_TOKEN"    description:"Schema Registry bearer token"`
	CAFile   string `long:"schema-registry-ca"       env:"K4A_SCHEMA_REGISTRY_CA"       description:"CA certificate PEM file for the Schema Registry"`
}

func (r registryFlags) config() schemareg.Config {
	return schemareg.Config{URL: r.URL, Username: r.Username, Password: r.Password, Token: r.Token, CAFile: r.CAFile}
}

// buildDecoder is the daemon's decoder: the context's schema-registry block
// with the flags over it, as the CLI resolves it, or nil when none is set. A
// --brokers connection has no context and takes the flags alone.
func buildDecoder(f *flags) (kafka.RecordDecoder, error) {
	reg, err := schemareg.NewIfSet(registryConfig(f))
	if err != nil {
		return nil, err
	}
	return reg.SearchDecoder(), nil
}

// registryConfig resolves the registry the same way resolveConnection resolves
// the cluster. A config that cannot be read here was already read there, so a
// failure degrades to the flags rather than failing a second time.
func registryConfig(f *flags) schemareg.Config {
	if f.Brokers != "" {
		return f.Registry.config()
	}
	path := f.ConfigFile
	if path == "" {
		path = config.DefaultConfigPath()
	}
	cfg, err := config.Load(path)
	if err != nil {
		return f.Registry.config()
	}
	cctx, err := cfg.Resolve(f.Context)
	if err != nil {
		return f.Registry.config()
	}
	return cctx.SchemaRegistry.Config().Overlay(f.Registry.config())
}
