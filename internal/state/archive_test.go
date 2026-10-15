// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func writeTestFile(t *testing.T, root, rel, body string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}
}

// populate fills a state root with one of everything, plus the leftovers
// an export must not carry: a lock, a crashed write's temp file and a
// renamed-aside backup.
func populate(t *testing.T) *Root {
	t.Helper()
	dir := t.TempDir()
	writeTestFile(t, dir, "state.json", `{"schema_version":1}`)
	writeTestFile(t, dir, "cache/searches/abc.json", "cached")
	writeTestFile(t, dir, "cache/searches/abc.tmp-123", "half written")
	writeTestFile(t, dir, "index/c1/t1/state.json", "bookkeeping")
	writeTestFile(t, dir, "index/c1/t1/bleve/store", "bleve bytes")
	writeTestFile(t, dir, "index/c1/t1/lock", "")
	writeTestFile(t, dir, "index/c1/.t2.bak.20260101/state.json", "old")
	return NewRoot(dir)
}

func exportTo(t *testing.T, r *Root, opts ExportOptions) (string, *ExportResult) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "out.tar.gz")
	f, err := os.Create(p) //nolint:gosec // test temp dir
	if err != nil {
		t.Fatal(err)
	}
	res, err := Export(r, f, opts)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return p, res
}

func TestExportImportRoundTrip(t *testing.T) {
	t.Parallel()

	src := populate(t)
	archive, res := exportTo(t, src, ExportOptions{
		Config: []byte("current-context: a\n"), Session: true, Cache: true, Index: true,
	})
	if want := []string{PartConfig, PartSession, PartCache, PartIndex}; !slices.Equal(res.Manifest.Parts, want) {
		t.Errorf("parts = %v, want %v", res.Manifest.Parts, want)
	}

	plan, err := InspectArchive(archive)
	if err != nil {
		t.Fatalf("InspectArchive: %v", err)
	}
	wantFiles := []string{
		"config.yaml", "state.json", "cache/searches/abc.json",
		"index/c1/t1/bleve/store", "index/c1/t1/state.json",
	}
	got := slices.Sorted(slices.Values(plan.Files))
	if !slices.Equal(got, slices.Sorted(slices.Values(wantFiles))) {
		t.Errorf("archived files = %v, want %v", got, wantFiles)
	}

	dst := NewRoot(filepath.Join(t.TempDir(), "k4a"))
	if c := plan.Conflicts(dst); len(c) != 0 {
		t.Errorf("conflicts into an empty root = %v", c)
	}
	if _, err := plan.Apply(dst); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, rel := range wantFiles {
		p := filepath.Join(dst.Path(), filepath.FromSlash(rel))
		info, err := os.Stat(p)
		if err != nil {
			t.Errorf("%s not imported: %v", rel, err)
			continue
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Errorf("%s mode = %o, want 600", rel, perm)
		}
	}
	body, _ := os.ReadFile(filepath.Join(dst.Path(), "index/c1/t1/bleve/store")) //nolint:errcheck // checked by compare
	if string(body) != "bleve bytes" {
		t.Errorf("index content = %q", body)
	}
	if c := plan.Conflicts(dst); len(c) != 4 || !slices.Contains(c, "index/c1/t1") {
		t.Errorf("conflicts after import = %v, want config, session, cache file and the topic", c)
	}
}

func TestExportLeavesIndexOutByDefault(t *testing.T) {
	t.Parallel()

	archive, res := exportTo(t, populate(t), ExportOptions{Session: true, Cache: true})
	if slices.Contains(res.Manifest.Parts, PartIndex) || slices.Contains(res.Manifest.Parts, PartConfig) {
		t.Errorf("parts = %v", res.Manifest.Parts)
	}
	plan, err := InspectArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Topics) != 0 {
		t.Errorf("topics = %v, want none", plan.Topics)
	}
}

func TestExportSkipsLockedIndex(t *testing.T) {
	t.Parallel()

	src := populate(t)
	lock, err := AcquireWriter(filepath.Join(src.Path(), "index", "c1", "t1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Release() })

	archive, res := exportTo(t, src, ExportOptions{Index: true})
	if !slices.Equal(res.Locked, []string{"c1/t1"}) {
		t.Errorf("locked = %v, want [c1/t1]", res.Locked)
	}
	plan, err := InspectArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Files) != 0 {
		t.Errorf("a locked index was archived: %v", plan.Files)
	}
}

func TestImportRefusesLockedTopic(t *testing.T) {
	t.Parallel()

	archive, _ := exportTo(t, populate(t), ExportOptions{Index: true})
	plan, err := InspectArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	dst := NewRoot(t.TempDir())
	writeTestFile(t, dst.Path(), "index/c1/t1/state.json", "live")
	lock, err := AcquireWriter(filepath.Join(dst.Path(), "index", "c1", "t1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Release() })

	if _, err := plan.Apply(dst); !errors.Is(err, ErrLocked) {
		t.Fatalf("Apply into a locked topic err = %v, want ErrLocked", err)
	}
	body, _ := os.ReadFile(filepath.Join(dst.Path(), "index/c1/t1/state.json")) //nolint:errcheck // compared
	if string(body) != "live" {
		t.Errorf("locked index was changed: %q", body)
	}
}

func TestImportReplacesTopicWhole(t *testing.T) {
	t.Parallel()

	archive, _ := exportTo(t, populate(t), ExportOptions{Index: true})
	plan, err := InspectArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	dst := NewRoot(t.TempDir())
	writeTestFile(t, dst.Path(), "index/c1/t1/bleve/stale-segment", "old")
	if _, err := plan.Apply(dst); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst.Path(), "index/c1/t1/bleve/stale-segment")); !os.IsNotExist(err) {
		t.Errorf("an old index file survived the import and would mix with the new index: %v", err)
	}
}

type tarEntry struct {
	name     string
	body     string
	linkname string
	typ      byte
}

func manifestEntry() tarEntry {
	m := fmt.Sprintf(`{"format":%q,"schema_version":%d}`, ArchiveFormat, archiveSchemaVersion)
	return tarEntry{name: manifestName, body: m, typ: tar.TypeReg}
}

func buildArchive(t *testing.T, entries ...tarEntry) string {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		hdr := &tar.Header{Name: e.name, Typeflag: e.typ, Linkname: e.linkname, Mode: 0o644, Size: int64(len(e.body))}
		if e.typ != tar.TypeReg {
			hdr.Size = 0
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if e.typ == tar.TypeReg {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "evil.tar.gz")
	if err := os.WriteFile(p, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestInspectRejectsMaliciousArchives feeds archives that try to write
// outside the state root, or to plant something an export never writes.
// Each must be refused before anything is written; the "valid" row proves
// the same builder produces an archive the inspector accepts.
func TestInspectRejectsMaliciousArchives(t *testing.T) {
	t.Parallel()

	reg := func(name string) tarEntry { return tarEntry{name: name, body: "x", typ: tar.TypeReg} }
	cases := []struct {
		name    string
		entries []tarEntry
		wantErr bool
	}{
		{"valid", []tarEntry{manifestEntry(), reg("cache/searches/a.json")}, false},
		{"dot-dot", []tarEntry{manifestEntry(), reg("../evil")}, true},
		{"nested dot-dot", []tarEntry{manifestEntry(), reg("cache/../../evil")}, true},
		{"absolute", []tarEntry{manifestEntry(), reg("/tmp/evil")}, true},
		{"unclean", []tarEntry{manifestEntry(), reg("cache//a.json")}, true},
		{"dot elem", []tarEntry{manifestEntry(), reg("./state.json")}, true},
		{"backslash", []tarEntry{manifestEntry(), reg(`cache\..\..\evil`)}, true},
		{"symlink", []tarEntry{
			manifestEntry(), {name: "cache/link", linkname: "/etc", typ: tar.TypeSymlink},
		}, true},
		{"relative symlink", []tarEntry{
			manifestEntry(), {name: "cache/link", linkname: "../../..", typ: tar.TypeSymlink},
		}, true},
		{"hardlink", []tarEntry{
			manifestEntry(), {name: "cache/link", linkname: "/etc/passwd", typ: tar.TypeLink},
		}, true},
		{"fifo", []tarEntry{manifestEntry(), {name: "cache/fifo", typ: tar.TypeFifo}}, true},
		{"unknown top level", []tarEntry{manifestEntry(), reg("bin/k4a")}, true},
		{"writer lock", []tarEntry{manifestEntry(), reg("index/c/t/lock")}, true},
		{"file at cluster level", []tarEntry{manifestEntry(), reg("index/c/stray")}, true},
		{"duplicate", []tarEntry{manifestEntry(), reg("state.json"), reg("state.json")}, true},
		{"no manifest first", []tarEntry{reg("state.json"), manifestEntry()}, true},
		{
			"wrong format",
			[]tarEntry{{name: manifestName, body: `{"format":"zip","schema_version":1}`, typ: tar.TypeReg}},
			true,
		},
		{
			"newer schema",
			[]tarEntry{{name: manifestName, body: `{"format":"k4a-state","schema_version":99}`, typ: tar.TypeReg}},
			true,
		},
		{"empty", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := InspectArchive(buildArchive(t, tc.entries...))
			if (err != nil) != tc.wantErr {
				t.Fatalf("InspectArchive err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrBadArchive) {
				t.Errorf("err = %v, want ErrBadArchive", err)
			}
		})
	}
}

// TestApplyDoesNotFollowSymlinkOutOfRoot covers the other way out: the
// archive is clean, but the destination already has a symlink pointing
// outside it. The write must not land at the link's target.
func TestApplyDoesNotFollowSymlinkOutOfRoot(t *testing.T) {
	t.Parallel()

	outside := t.TempDir()
	dst := NewRoot(t.TempDir())
	if err := os.Symlink(outside, filepath.Join(dst.Path(), "cache")); err != nil {
		t.Fatal(err)
	}
	plan, err := InspectArchive(buildArchive(t, manifestEntry(),
		tarEntry{name: "cache/searches/a.json", body: "x", typ: tar.TypeReg}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plan.Apply(dst); err == nil {
		t.Error("Apply wrote through a symlink that leaves the state root")
	}
	if _, err := os.Stat(filepath.Join(outside, "searches", "a.json")); !os.IsNotExist(err) {
		t.Errorf("file written outside the state root: %v", err)
	}
}
