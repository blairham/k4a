// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/blairham/k4a/internal/state"
)

// ClearResult reports what a clear removed and what it had to leave.
type ClearResult struct {
	// Locked lists "<cluster>/<topic>" directories another process is
	// writing. They are left untouched.
	Locked  []string
	Bytes   int64
	Cleared int
}

// ClearTopic removes the local index for topic, in one cluster or, with an
// empty cluster, in every cluster that has one. Copies the indexer renamed
// aside after a schema bump or corruption (<topic>.bak.*) go with it.
func ClearTopic(root *state.Root, cluster, topic string) (ClearResult, error) {
	if err := checkPathElem("topic", topic); err != nil {
		return ClearResult{}, err
	}
	clusters := []string{cluster}
	if cluster == "" {
		var err error
		if clusters, err = clusterNames(root); err != nil {
			return ClearResult{}, err
		}
	} else if err := checkPathElem("cluster", cluster); err != nil {
		return ClearResult{}, err
	}

	var res ClearResult
	for _, c := range clusters {
		base := filepath.Join(root.File(state.SubsystemIndex), c)
		entries, err := os.ReadDir(base)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() && (e.Name() == topic || strings.HasPrefix(e.Name(), topic+".bak.")) {
				if err := res.clear(base, c, e.Name()); err != nil {
					return res, err
				}
			}
		}
		_ = os.Remove(base) //nolint:errcheck // only succeeds once the cluster dir is empty
	}
	return res, nil
}

// ClearAll removes every local topic index that no process is writing.
func ClearAll(root *state.Root) (ClearResult, error) {
	clusters, err := clusterNames(root)
	if err != nil {
		return ClearResult{}, err
	}
	var res ClearResult
	for _, c := range clusters {
		base := filepath.Join(root.File(state.SubsystemIndex), c)
		entries, err := os.ReadDir(base)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if err := res.clear(base, c, e.Name()); err != nil {
				return res, err
			}
		}
		_ = os.Remove(base) //nolint:errcheck // only succeeds once the cluster dir is empty
	}
	return res, nil
}

func (res *ClearResult) clear(base, cluster, name string) error {
	n, err := clearDir(filepath.Join(base, name))
	switch {
	case errors.Is(err, state.ErrLocked):
		res.Locked = append(res.Locked, cluster+"/"+name)
		return nil
	case err != nil:
		return err
	}
	res.Bytes += n
	res.Cleared++
	return nil
}

// clearDir deletes one topic index under its writer lock, so it can never
// pull files out from under an indexer that is running. Returns
// state.ErrLocked when one is.
func clearDir(dir string) (int64, error) {
	size := dirBytes(dir)
	lock, err := state.AcquireWriter(dir)
	if err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		_ = lock.Release() //nolint:errcheck // already failing
		return 0, fmt.Errorf("reading %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.Name() == "lock" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			_ = lock.Release() //nolint:errcheck // already failing
			return 0, fmt.Errorf("removing %s: %w", dir, err)
		}
	}
	// Unlink the lock while still holding it: nobody can then take the old
	// file after Release, and a writer that starts now creates a fresh lock
	// in a fresh index. Windows refuses to delete the open file, which
	// leaves an empty directory that List skips.
	_ = os.Remove(lock.Path()) //nolint:errcheck // see above
	_ = lock.Release()         //nolint:errcheck // closing releases anyway
	_ = os.Remove(dir)         //nolint:errcheck // fails only if a new writer already moved in
	return size, nil
}

func clusterNames(root *state.Root) ([]string, error) {
	entries, err := os.ReadDir(root.File(state.SubsystemIndex))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// checkPathElem refuses a name that would resolve outside its parent
// directory, so `k4a index clear ../..` cannot delete anything but an index.
func checkPathElem(what, name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("invalid %s name %q", what, name)
	}
	return nil
}
