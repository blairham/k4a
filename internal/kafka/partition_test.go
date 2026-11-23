// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import "testing"

// The expected partitions come from an implementation outside this code: a
// Python murmur2 checked against Kafka's own UtilsTest values ("21" ->
// -973932308, "foobar" -> -790332482) and zlib's crc32. Each key is chosen so
// the two partitioners disagree, so a mix-up cannot pass.
func TestKeyPartition(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		partitioner, key string
		n                int
		want             int32
	}{
		{PartitionerMurmur2, "stale-key", 6, 3},
		{PartitionerCRC32, "stale-key", 6, 5},
		{PartitionerConsistentRandom, "stale-key", 6, 5},
		{PartitionerMurmur2, "order-42", 6, 0},
		{PartitionerCRC32, "order-42", 6, 4},
		{PartitionerMurmur2, "foobar", 6, 0},
		{PartitionerCRC32, "foobar", 6, 5},
		{PartitionerMurmur2, "k", 6, 2},
		{PartitionerCRC32, "k", 6, 1},
	} {
		got, err := KeyPartition(tc.partitioner, []byte(tc.key), tc.n)
		if err != nil || got != tc.want {
			t.Errorf("KeyPartition(%s, %q, %d) = %d, %v; want %d", tc.partitioner, tc.key, tc.n, got, err, tc.want)
		}
	}
	for name, tc := range map[string]struct {
		partitioner, key string
		n                int
	}{
		"empty key":     {PartitionerMurmur2, "", 6},
		"no partitions": {PartitionerCRC32, "k", 0},
		"unknown":       {"fnv", "k", 6},
	} {
		if _, err := KeyPartition(tc.partitioner, []byte(tc.key), tc.n); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
