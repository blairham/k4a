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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// Size caps on what an upgrade downloads. A release archive is ~50MB today;
// the caps only stop a misbehaving server from filling memory.
const (
	maxAPIBytes       = 4 << 20
	maxChecksumsBytes = 1 << 20
	maxArchiveBytes   = 256 << 20
	maxBinaryBytes    = 512 << 20
)

const (
	checksumsAsset = "checksums.txt"
	goosWindows    = "windows"
)

// errNoRelease means the repository has no published, non-prerelease release.
var errNoRelease = errors.New("no release found")

// release is as much of a GitHub release as an upgrade needs.
type release struct {
	Assets map[string]string // asset name -> download URL
	Tag    string
}

// Version is the release tag without its "v".
func (r *release) Version() string { return strings.TrimPrefix(r.Tag, "v") }

// client talks to the GitHub releases API and downloads assets.
type client struct {
	http    *http.Client
	apiBase string // https://api.github.com, or a test server
	token   string // sent to the API only; may be empty
}

// newClient asks `gh` for a token under ctx, so a canceled upgrade does not
// wait out the token lookup.
func newClient(ctx context.Context) *client {
	return &client{
		http:    &http.Client{Timeout: 5 * time.Minute},
		apiBase: "https://api.github.com",
		token:   resolveGitHubToken(ctx),
	}
}

// latest returns the newest published release. GitHub's /releases/latest
// already skips drafts and prereleases.
func (c *client) latest(ctx context.Context) (*release, error) {
	url := c.apiBase + "/repos/" + repo + "/releases/latest"
	body, status, err := c.get(ctx, url, "application/vnd.github+json", true, maxAPIBytes)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, errNoRelease
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GitHub releases API returned %d", status)
	}

	var payload struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name string `json:"name"`
			URL  string `json:"browser_download_url"`
		} `json:"assets"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decoding release: %w", err)
	}
	if payload.TagName == "" {
		return nil, errNoRelease
	}

	rel := &release{Tag: payload.TagName, Assets: make(map[string]string, len(payload.Assets))}
	for _, a := range payload.Assets {
		rel.Assets[a.Name] = a.URL
	}
	return rel, nil
}

// download fetches a release asset, refusing anything larger than limit.
func (c *client) download(ctx context.Context, url string, limit int64) ([]byte, error) {
	body, status, err := c.get(ctx, url, "application/octet-stream", false, limit)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("downloading %s: HTTP %d", path.Base(url), status)
	}
	return body, nil
}

func (c *client) get(ctx context.Context, url, accept string, auth bool, limit int64) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", accept)
	if auth && c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close() //nolint:errcheck // read-only body

	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if int64(len(body)) > limit {
		return nil, resp.StatusCode, fmt.Errorf("%s is larger than %d bytes", path.Base(url), limit)
	}
	return body, resp.StatusCode, nil
}

// archiveName is the release archive for a platform, following the
// name_template in .goreleaser.yaml: k4a_<Title OS>_<uname arch>.
func archiveName(goos, goarch string) (string, bool) {
	osName, ok := map[string]string{"darwin": "Darwin", "linux": "Linux", goosWindows: "Windows"}[goos]
	if !ok {
		return "", false
	}
	arch, ok := map[string]string{"amd64": "x86_64", "arm64": "arm64"}[goarch]
	if !ok {
		return "", false
	}
	if goos == goosWindows {
		return "k4a_" + osName + "_" + arch + ".zip", true
	}
	return "k4a_" + osName + "_" + arch + ".tar.gz", true
}

// binaryName is the executable inside the archive.
func binaryName(goos string) string {
	if goos == goosWindows {
		return "k4a.exe"
	}
	return "k4a"
}

// checksumFor finds name's SHA-256 in a GoReleaser checksums.txt
// ("<hex>  <name>" per line).
func checksumFor(sums []byte, name string) (string, error) {
	for _, line := range strings.Split(string(sums), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == name {
			return strings.ToLower(fields[0]), nil
		}
	}
	return "", fmt.Errorf("%s has no entry for %s", checksumsAsset, name)
}

// verifyChecksum fails unless data hashes to want.
func verifyChecksum(data []byte, want string) error {
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != want {
		return fmt.Errorf("checksum mismatch: got %s, want %s", got, want)
	}
	return nil
}

// extractBinary returns the named executable from a .tar.gz or .zip archive.
func extractBinary(archive []byte, archiveName, binName string) ([]byte, error) {
	if strings.HasSuffix(archiveName, ".zip") {
		return extractZip(archive, binName)
	}
	return extractTarGz(archive, binName)
}

func extractTarGz(archive []byte, binName string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return nil, fmt.Errorf("reading archive: %w", err)
	}
	defer gz.Close() //nolint:errcheck // in-memory reader

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("archive has no %s", binName)
		}
		if err != nil {
			return nil, fmt.Errorf("reading archive: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg || path.Base(hdr.Name) != binName {
			continue
		}
		return readCapped(tr, binName)
	}
}

func extractZip(archive []byte, binName string) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return nil, fmt.Errorf("reading archive: %w", err)
	}
	for _, f := range zr.File {
		if f.FileInfo().IsDir() || path.Base(f.Name) != binName {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("reading archive: %w", err)
		}
		data, err := readCapped(rc, binName)
		rc.Close() //nolint:errcheck,gosec // in-memory reader
		return data, err
	}
	return nil, fmt.Errorf("archive has no %s", binName)
}

func readCapped(r io.Reader, name string) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBinaryBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", name, err)
	}
	if len(data) > maxBinaryBytes {
		return nil, fmt.Errorf("%s is larger than %d bytes", name, maxBinaryBytes)
	}
	return data, nil
}

// replaceExecutable swaps exe for data. The new file is written beside exe
// and renamed over it, so a failure leaves the old binary in place. Windows
// cannot replace a running executable, but can rename it: the old one moves
// to exe+".old" first.
func replaceExecutable(exe string, data []byte, goos string) error {
	info, err := os.Stat(exe)
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(exe), ".k4a-upgrade-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) //nolint:errcheck // gone after a successful rename

	if _, err := tmp.Write(data); err != nil {
		tmp.Close() //nolint:errcheck,gosec // already failing
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, info.Mode().Perm()); err != nil {
		return err
	}

	if goos != goosWindows {
		return os.Rename(tmpName, exe)
	}

	old := exe + ".old"
	// A leftover from the previous upgrade, which could not delete itself.
	if err := os.Remove(old); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.Rename(exe, old); err != nil {
		return err
	}
	if err := os.Rename(tmpName, exe); err != nil {
		// Put the running binary back.
		return errors.Join(err, os.Rename(old, exe))
	}
	return nil
}
