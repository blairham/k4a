// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"fmt"
	"path/filepath"
	"strings"
)

// maxTopicNameLen is Kafka's own limit on a topic name.
const maxTopicNameLen = 249

// ValidateTopicName reports whether topic is a name Kafka itself would accept:
// 1-249 characters from [a-zA-Z0-9._-], and neither "." nor "..".
//
// It matters because a topic name becomes a directory under the index root.
// Kafka would refuse "..", but a k4a-index client reaches the daemon before any
// broker sees the name, and "*" in the --allow glob matches ".." — so without
// this check a client could aim index.Open (and a later eviction's RemoveAll)
// at the index root itself.
func ValidateTopicName(topic string) error {
	switch {
	case topic == "":
		return fmt.Errorf("topic is required")
	case topic == "." || topic == "..":
		return fmt.Errorf("illegal topic name %q", topic)
	case len(topic) > maxTopicNameLen:
		return fmt.Errorf("topic name is %d characters; Kafka allows at most %d", len(topic), maxTopicNameLen)
	}
	for _, r := range topic {
		if !isTopicNameRune(r) {
			return fmt.Errorf("illegal character %q in topic name %q", r, topic)
		}
	}
	return nil
}

func isTopicNameRune(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
		r == '.' || r == '_' || r == '-'
}

// containedChild reports whether path is a direct child of parent once both
// are cleaned — the shape every topic directory must have. It is the last
// check before deleting a directory, independent of how the name got there.
func containedChild(parent, path string) bool {
	rel, err := filepath.Rel(filepath.Clean(parent), filepath.Clean(path))
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) &&
		!strings.ContainsRune(rel, filepath.Separator)
}
