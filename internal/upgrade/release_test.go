// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package upgrade

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNeedsUpdate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		current, latest string
		want            bool
	}{
		{"v0.0.0", "0.0.1", true},
		{"0.0.1", "0.0.1", false},
		{"v0.0.2", "0.0.1", false},
		{"v0.9.0", "0.10.0", true}, // numeric, not lexical
		{"v0.0.1-dirty", "0.0.1", false},
		{"dev", "0.0.1", true},
		{"", "0.0.1", true},
	}
	for _, tc := range cases {
		if got := needsUpdate(tc.current, tc.latest); got != tc.want {
			t.Errorf("needsUpdate(%q, %q) = %v, want %v", tc.current, tc.latest, got, tc.want)
		}
	}
}

func TestArchiveName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		goos, goarch, want string
		ok                 bool
	}{
		{"darwin", "arm64", "k4a_Darwin_arm64.tar.gz", true},
		{"darwin", "amd64", "k4a_Darwin_x86_64.tar.gz", true},
		{"linux", "arm64", "k4a_Linux_arm64.tar.gz", true},
		{"linux", "amd64", "k4a_Linux_x86_64.tar.gz", true},
		{"windows", "amd64", "k4a_Windows_x86_64.zip", true},
		{"windows", "arm64", "k4a_Windows_arm64.zip", true},
		{"freebsd", "amd64", "", false},
		{"linux", "386", "", false},
	}
	for _, tc := range cases {
		got, ok := archiveName(tc.goos, tc.goarch)
		if got != tc.want || ok != tc.ok {
			t.Errorf("archiveName(%q, %q) = (%q, %v), want (%q, %v)", tc.goos, tc.goarch, got, ok, tc.want, tc.ok)
		}
	}
}

func TestChecksumFor(t *testing.T) {
	t.Parallel()

	sums := []byte("aaaa  k4a_Linux_x86_64.tar.gz\nBBBB  k4a_Darwin_arm64.tar.gz\n")
	if got, err := checksumFor(sums, "k4a_Darwin_arm64.tar.gz"); err != nil || got != "bbbb" {
		t.Errorf("checksumFor = (%q, %v), want (bbbb, nil)", got, err)
	}
	// A name that is a suffix of another entry must not match it.
	if _, err := checksumFor(sums, "x86_64.tar.gz"); err == nil {
		t.Error("checksumFor matched a partial name")
	}
}

// fakeRelease serves a GitHub /releases/latest response plus its assets.
type fakeRelease struct {
	assets map[string][]byte
	tag    string
}

func (f fakeRelease) serve(t *testing.T) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/"+repo+"/releases/latest" {
			if f.tag == "" {
				http.NotFound(w, r)
				return
			}
			type asset struct {
				Name string `json:"name"`
				URL  string `json:"browser_download_url"`
			}
			var as []asset
			for name := range f.assets {
				as = append(as, asset{Name: name, URL: srv.URL + "/download/" + name})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tag_name": f.tag, "assets": as})
			return
		}
		if data, ok := f.assets[strings.TrimPrefix(r.URL.Path, "/download/")]; ok {
			_, _ = w.Write(data)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func tarGz(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, data := range files {
		if err := tw.WriteHeader(
			&tar.Header{Name: name, Mode: 0o755, Size: int64(len(data)), Typeflag: tar.TypeReg},
		); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func zipOf(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, data := range files {
		f, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func sha(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:])
}

// installed writes a stand-in for the running binary.
func installed(t *testing.T, name string) string {
	t.Helper()
	exe := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(exe, []byte("old binary"), 0o755); err != nil { //nolint:gosec // test executable
		t.Fatal(err)
	}
	return exe
}

func testClient(srv *httptest.Server) *client {
	return &client{http: srv.Client(), apiBase: srv.URL}
}

func TestRunReplacesBinary(t *testing.T) {
	t.Parallel()

	newBin := []byte("new binary")
	archive := tarGz(t, map[string][]byte{"README.md": []byte("readme"), "k4a": newBin})
	srv := fakeRelease{tag: "v0.0.2", assets: map[string][]byte{
		"k4a_Linux_x86_64.tar.gz": archive,
		"k4a_Darwin_arm64.tar.gz": []byte("not this one"),
		checksumsAsset:            []byte(sha(archive) + "  k4a_Linux_x86_64.tar.gz\n"),
	}}.serve(t)
	exe := installed(t, "k4a")

	ver, err := run(context.Background(), testClient(srv), "v0.0.1", exe, "linux", "amd64", io.Discard)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if ver != "0.0.2" {
		t.Errorf("version = %q, want 0.0.2", ver)
	}
	if got, _ := os.ReadFile(exe); !bytes.Equal(got, newBin) {
		t.Errorf("binary = %q, want %q", got, newBin)
	}
	if info, _ := os.Stat(exe); info.Mode().Perm() != 0o755 {
		t.Errorf("mode = %v, want 0755", info.Mode().Perm())
	}
}

func TestRunWindowsZip(t *testing.T) {
	t.Parallel()

	newBin := []byte("new exe")
	archive := zipOf(t, map[string][]byte{"LICENSE": []byte("license"), "k4a.exe": newBin})
	srv := fakeRelease{tag: "v0.0.2", assets: map[string][]byte{
		"k4a_Windows_x86_64.zip": archive,
		checksumsAsset:           []byte(sha(archive) + "  k4a_Windows_x86_64.zip\n"),
	}}.serve(t)
	exe := installed(t, "k4a.exe")

	if _, err := run(context.Background(), testClient(srv), "v0.0.1", exe, "windows", "amd64", io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got, _ := os.ReadFile(exe); !bytes.Equal(got, newBin) {
		t.Errorf("binary = %q, want %q", got, newBin)
	}
	if got, _ := os.ReadFile(exe + ".old"); string(got) != "old binary" {
		t.Errorf("the replaced binary should be kept as .old, got %q", got)
	}
}

// A tampered archive must not be installed.
func TestRunRejectsChecksumMismatch(t *testing.T) {
	t.Parallel()

	archive := tarGz(t, map[string][]byte{"k4a": []byte("tampered")})
	srv := fakeRelease{tag: "v0.0.2", assets: map[string][]byte{
		"k4a_Linux_x86_64.tar.gz": archive,
		checksumsAsset:            []byte(sha([]byte("the real archive")) + "  k4a_Linux_x86_64.tar.gz\n"),
	}}.serve(t)
	exe := installed(t, "k4a")

	_, err := run(context.Background(), testClient(srv), "v0.0.1", exe, "linux", "amd64", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("run error = %v, want a checksum mismatch", err)
	}
	if got, _ := os.ReadFile(exe); string(got) != "old binary" {
		t.Errorf("binary changed to %q after a failed verification", got)
	}
}

func TestRunMissingAssets(t *testing.T) {
	t.Parallel()

	archive := tarGz(t, map[string][]byte{"k4a": []byte("new")})
	cases := map[string]map[string][]byte{
		"no archive for this platform": {checksumsAsset: []byte(sha(archive) + "  k4a_Linux_x86_64.tar.gz\n")},
		"no checksums":                 {"k4a_Linux_x86_64.tar.gz": archive},
		"no checksum entry":            {"k4a_Linux_x86_64.tar.gz": archive, checksumsAsset: []byte("abc  other.tar.gz\n")},
		"no binary in archive": {
			"k4a_Linux_x86_64.tar.gz": tarGz(t, map[string][]byte{"README.md": []byte("x")}),
			checksumsAsset: []byte(
				sha(tarGz(t, map[string][]byte{"README.md": []byte("x")})) + "  k4a_Linux_x86_64.tar.gz\n",
			),
		},
	}
	for name, assets := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := fakeRelease{tag: "v0.0.2", assets: assets}.serve(t)
			exe := installed(t, "k4a")
			if _, err := run(context.Background(), testClient(srv), "v0.0.1", exe, "linux", "amd64", io.Discard); err == nil {
				t.Fatal("run succeeded, want an error")
			}
			if got, _ := os.ReadFile(exe); string(got) != "old binary" {
				t.Errorf("binary changed to %q", got)
			}
		})
	}
}

func TestRunAlreadyUpToDate(t *testing.T) {
	t.Parallel()

	srv := fakeRelease{tag: "v0.0.1", assets: map[string][]byte{}}.serve(t)
	_, err := run(context.Background(), testClient(srv), "v0.0.1", installed(t, "k4a"), "linux", "amd64", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "already up to date") {
		t.Fatalf("run error = %v, want already up to date", err)
	}
}

func TestCheck(t *testing.T) {
	t.Parallel()

	srv := fakeRelease{tag: "v0.0.2", assets: map[string][]byte{}}.serve(t)
	latest, available, err := check(context.Background(), testClient(srv), "v0.0.1")
	if err != nil || latest != "0.0.2" || !available {
		t.Errorf("check = (%q, %v, %v), want (0.0.2, true, nil)", latest, available, err)
	}

	empty := fakeRelease{}.serve(t)
	latest, available, err = check(context.Background(), testClient(empty), "v0.0.1")
	if err != nil || latest != "" || available {
		t.Errorf("check with no release = (%q, %v, %v), want (\"\", false, nil)", latest, available, err)
	}
}
