// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
)

// quotaDefault is how a default entity name is written: user=<default> is
// the quota every user without one of their own gets.
const quotaDefault = "<default>"

// quotaEntityTypes are the entity types Kafka accepts (KIP-546, KIP-612).
var quotaEntityTypes = []string{"user", "client-id", "ip"}

// QuotaEntity identifies who a client quota applies to, for example
// user=alice,client-id=billing or user=<default>.
type QuotaEntity []kadm.ClientQuotaEntityComponent

// String renders the entity as ParseQuotaEntity reads it, components in a
// fixed order so equal entities print equally.
func (e QuotaEntity) String() string {
	parts := make([]string, 0, len(e))
	for _, c := range e {
		name := quotaDefault
		if c.Name != nil {
			name = *c.Name
		}
		parts = append(parts, c.Type+"="+name)
	}
	return strings.Join(parts, ",")
}

// ParseQuotaEntity reads type=name[,type=name] (types user, client-id, ip;
// a name of <default> means the type's default entity).
func ParseQuotaEntity(s string) (QuotaEntity, error) {
	var e QuotaEntity
	for part := range strings.SplitSeq(s, ",") {
		typ, name, ok := strings.Cut(strings.TrimSpace(part), "=")
		typ, name = strings.TrimSpace(typ), strings.TrimSpace(name)
		if !ok || name == "" {
			return nil, fmt.Errorf("quota entity %q: want type=name, for example user=alice", part)
		}
		if !slices.Contains(quotaEntityTypes, typ) {
			return nil, fmt.Errorf("quota entity type %q: want one of %s", typ, strings.Join(quotaEntityTypes, ", "))
		}
		if slices.ContainsFunc(e, func(c kadm.ClientQuotaEntityComponent) bool { return c.Type == typ }) {
			return nil, fmt.Errorf("quota entity names %s twice", typ)
		}
		c := kadm.ClientQuotaEntityComponent{Type: typ}
		if name != quotaDefault {
			c.Name = &name
		}
		e = append(e, c)
	}
	sortQuotaEntity(e)
	return e, nil
}

func sortQuotaEntity(e QuotaEntity) {
	slices.SortFunc(e, func(a, b kadm.ClientQuotaEntityComponent) int {
		return slices.Index(quotaEntityTypes, a.Type) - slices.Index(quotaEntityTypes, b.Type)
	})
}

// ClientQuota is one quota value set on one entity.
type ClientQuota struct {
	Key    string
	Entity QuotaEntity
	Value  float64
}

// FormatQuotaValue prints a quota value without float noise: rates are
// whole bytes per second, request_percentage may be fractional.
func FormatQuotaValue(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// FetchClientQuotas returns every client quota on the cluster, one entry per
// entity and key, sorted by entity then key.
func (c *Client) FetchClientQuotas(ctx context.Context) ([]ClientQuota, error) {
	described, err := c.admin.DescribeClientQuotas(ctx, false, nil)
	if err != nil {
		return nil, fmt.Errorf("describing client quotas: %w", err)
	}
	var out []ClientQuota
	for _, d := range described {
		e := QuotaEntity(slices.Clone(d.Entity))
		sortQuotaEntity(e)
		for _, v := range d.Values {
			out = append(out, ClientQuota{Entity: e, Key: v.Key, Value: v.Value})
		}
	}
	slices.SortFunc(out, func(a, b ClientQuota) int {
		if n := strings.Compare(a.Entity.String(), b.Entity.String()); n != 0 {
			return n
		}
		return strings.Compare(a.Key, b.Key)
	})
	return out, nil
}

// AlterClientQuota sets key on entity, or with a nil value removes it.
func (c *Client) AlterClientQuota(ctx context.Context, entity QuotaEntity, key string, value *float64) error {
	if len(entity) == 0 {
		return errors.New("a client quota needs an entity, for example user=alice")
	}
	if key == "" {
		return errors.New("a client quota needs a key, for example producer_byte_rate")
	}
	op := kadm.AlterClientQuotaOp{Key: key, Remove: value == nil}
	if value != nil {
		op.Value = *value
	}
	results, err := c.admin.AlterClientQuotas(
		ctx,
		[]kadm.AlterClientQuotaEntry{{Entity: kadm.ClientQuotaEntity(entity), Ops: []kadm.AlterClientQuotaOp{op}}},
	)
	if err != nil {
		return fmt.Errorf("altering client quota: %w", err)
	}
	for _, r := range results {
		if r.Err != nil {
			if r.ErrMessage != "" {
				return fmt.Errorf("altering %s %s: %w: %s", entity, key, r.Err, r.ErrMessage)
			}
			return fmt.Errorf("altering %s %s: %w", entity, key, r.Err)
		}
	}
	return nil
}
