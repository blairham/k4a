// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ---------------------------------------------------------------------------
// configSourceName
// ---------------------------------------------------------------------------

func TestConfigSourceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		src  kmsg.ConfigSource
	}{
		{name: "dynamic topic", src: kmsg.ConfigSourceDynamicTopicConfig, want: "DYNAMIC_TOPIC"},
		{name: "dynamic broker", src: kmsg.ConfigSourceDynamicBrokerConfig, want: "DYNAMIC_BROKER"},
		{name: "dynamic default broker", src: kmsg.ConfigSourceDynamicDefaultBrokerConfig, want: "DYNAMIC_DEFAULT_BROKER"},
		{name: "static broker", src: kmsg.ConfigSourceStaticBrokerConfig, want: "STATIC_BROKER"},
		{name: "default", src: kmsg.ConfigSourceDefaultConfig, want: "DEFAULT"},
		{name: "dynamic broker logger", src: kmsg.ConfigSourceDynamicBrokerLoggerConfig, want: "DYNAMIC_BROKER_LOGGER"},
		{name: "zero returns unknown", src: 0, want: "UNKNOWN"},
		{name: "out of range returns unknown", src: 99, want: "UNKNOWN"},
		{name: "negative returns unknown", src: -1, want: "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := configSourceName(tt.src)
			if got != tt.want {
				t.Errorf("configSourceName(%d) = %q, want %q", tt.src, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parseResourceType
// ---------------------------------------------------------------------------

func TestParseResourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    kmsg.ACLResourceType
		wantErr bool
	}{
		{"topic", "topic", kmsg.ACLResourceTypeTopic, false},
		{"group", "group", kmsg.ACLResourceTypeGroup, false},
		{"cluster", "cluster", kmsg.ACLResourceTypeCluster, false},
		{"transactionalid", "transactionalid", kmsg.ACLResourceTypeTransactionalId, false},
		{"invalid", "bad", 0, true},
		{"empty string", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseResourceType(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseResourceType(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseResourceType(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parsePatternType
// ---------------------------------------------------------------------------

func TestParsePatternType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    kadm.ACLPattern
		wantErr bool
	}{
		{"literal lowercase", "literal", kadm.ACLPatternLiteral, false},
		{"prefixed lowercase", "prefixed", kadm.ACLPatternPrefixed, false},
		{"LITERAL uppercase", "LITERAL", kadm.ACLPatternLiteral, false},
		{"Prefixed mixed case", "Prefixed", kadm.ACLPatternPrefixed, false},
		{"invalid", "bad", 0, true},
		{"empty string", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parsePatternType(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parsePatternType(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parsePatternType(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parseOperation
// ---------------------------------------------------------------------------

func TestParseOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    kadm.ACLOperation
		wantErr bool
	}{
		{"read", "read", kadm.OpRead, false},
		{"write", "write", kadm.OpWrite, false},
		{"all", "all", kadm.OpAll, false},
		{"create", "create", kadm.OpCreate, false},
		{"delete", "delete", kadm.OpDelete, false},
		{"alter", "alter", kadm.OpAlter, false},
		{"describe", "describe", kadm.OpDescribe, false},
		{"invalid", "bad", 0, true},
		{"empty string", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseOperation(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseOperation(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseOperation(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parsePermission
// ---------------------------------------------------------------------------

func TestParsePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    kmsg.ACLPermissionType
		wantErr bool
	}{
		{"allow", "allow", kmsg.ACLPermissionTypeAllow, false},
		{"deny", "deny", kmsg.ACLPermissionTypeDeny, false},
		{"invalid", "bad", 0, true},
		{"empty string", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parsePermission(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parsePermission(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parsePermission(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
