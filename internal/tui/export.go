// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/views"
)

// exportRecord is one line of an export, and the JSON a record copy puts on
// the clipboard. Key and value are as shown when it was taken.
type exportRecord struct {
	Value     json.RawMessage       `json:"value"`
	Decoding  *exportDecoding       `json:"decoding,omitempty"`
	Topic     string                `json:"topic"`
	Timestamp string                `json:"timestamp"`
	Key       string                `json:"key"`
	Headers   []kafka.MessageHeader `json:"headers,omitempty"`
	Offset    int64                 `json:"offset"`
	Partition int                   `json:"partition"`
}

// exportDecoding says what a schema registry did to the record, so a reader
// can tell decoded JSON from the bytes the producer wrote.
type exportDecoding struct {
	Key   *fieldDecoding `json:"key,omitempty"`
	Value *fieldDecoding `json:"value,omitempty"`
	// Shown is "decoded" or "raw": which of the two the key and value are.
	Shown string `json:"shown"`
}

type fieldDecoding struct {
	Schema string `json:"schema,omitempty"`
	Error  string `json:"error,omitempty"`
}

func newFieldDecoding(d kafka.Decoding) *fieldDecoding {
	if d.Schema == "" && d.Reason == "" {
		return nil
	}
	return &fieldDecoding{Schema: d.Schema, Error: d.Reason}
}

// newExportRecord renders m as shown: decoded, or raw when raw is set. A
// value that is JSON is embedded as JSON, anything else as a string, as the
// single-message save does.
func newExportRecord(m *kafka.ConsumedMessage, raw bool) exportRecord {
	value := m.ValueDecoding.Shown(m.Value, raw)
	rec := exportRecord{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Timestamp: m.Time.Format(time.RFC3339Nano),
		Key:       m.KeyDecoding.Shown(m.Key, raw),
		Headers:   m.Headers,
	}
	if json.Valid([]byte(value)) {
		rec.Value = json.RawMessage(value)
	} else if quoted, err := json.Marshal(value); err == nil {
		rec.Value = quoted
	} else {
		rec.Value = json.RawMessage("null")
	}
	key, val := newFieldDecoding(m.KeyDecoding), newFieldDecoding(m.ValueDecoding)
	if key != nil || val != nil {
		rec.Decoding = &exportDecoding{Key: key, Value: val, Shown: "decoded"}
		if raw {
			rec.Decoding.Shown = "raw"
		}
	}
	return rec
}

// writeExport writes msgs to path as JSONL, one record per line, and creates
// the parent directories. Without overwrite it refuses an existing file with
// an error that wraps fs.ErrExist.
func writeExport(path string, msgs []kafka.ConsumedMessage, raw, overwrite bool) (err error) {
	if dir := filepath.Dir(path); dir != "." {
		if mkErr := os.MkdirAll(dir, 0o700); mkErr != nil {
			return fmt.Errorf("creating directory: %w", mkErr)
		}
	}
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if overwrite {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o600) //nolint:gosec // the path is the one the user typed
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for i := range msgs {
		if encErr := enc.Encode(newExportRecord(&msgs[i], raw)); encErr != nil {
			return fmt.Errorf("encoding offset %d: %w", msgs[i].Offset, encErr)
		}
	}
	return w.Flush()
}

// exportSnapshot is what an export writes, taken when e was pressed so a
// live tail that moves on while the path is typed does not change it.
type exportSnapshot struct {
	topic string
	msgs  []kafka.ConsumedMessage
	raw   bool
}

// promptExport snapshots the active view's messages (Messages as filtered,
// or Search's matches) and asks where to write them. The suggested path is
// relative, so it lands in the working directory, never under ~/.k4a.
func (a *App) promptExport(topic string) (tea.Model, tea.Cmd) {
	snap := exportSnapshot{topic: topic}
	kind := "messages"
	switch v := a.activeView().(type) {
	case *views.MessagesView:
		snap.msgs, snap.raw = v.VisibleMessages(), v.Raw()
	case *views.SearchView:
		snap.msgs, kind = v.Matches(), "matches"
	}
	if len(snap.msgs) == 0 {
		a.errFlash = "nothing to export"
		return a, nil
	}
	name := fmt.Sprintf("%s-%s-%s.jsonl", sanitizeFilename(topic), kind, time.Now().Format("20060102-150405"))
	a.promptDispatch = func(input string) (string, tea.Cmd) {
		return a.exportTo(snap, input)
	}
	cmd := a.prompt.Open(name, chrome.OpenOpts{Prompt: "export:", SelectAll: true})
	a.resizeActiveView()
	return a, cmd
}

// exportTo writes snap to the typed path, asking before it replaces a file.
// A non-empty return keeps the prompt open with the reason.
func (a *App) exportTo(snap exportSnapshot, input string) (errMsg string, cmd tea.Cmd) {
	path := strings.TrimSpace(input)
	if a.inStateRoot(path) {
		return "exports do not go under " + a.stateRoot.Path() + ": choose another path", nil
	}
	err := writeExport(path, snap.msgs, snap.raw, false)
	if errors.Is(err, fs.ErrExist) {
		a.confirmDispatch = func(yes bool) (string, tea.Cmd) {
			if yes {
				a.finishExport(path, snap, writeExport(path, snap.msgs, snap.raw, true))
			}
			return "", nil
		}
		a.confirm.Open(fmt.Sprintf("%s exists: overwrite?", path))
		return "", nil
	}
	if err != nil {
		return err.Error(), nil
	}
	a.finishExport(path, snap, nil)
	return "", nil
}

// finishExport flashes how an export went.
func (a *App) finishExport(path string, snap exportSnapshot, err error) {
	if err != nil {
		a.errFlash = "export: " + err.Error()
		return
	}
	shown := ""
	for i := range snap.msgs {
		if snap.msgs[i].KeyDecoding.Decoded() || snap.msgs[i].ValueDecoding.Decoded() {
			shown = map[bool]string{true: " (raw)", false: " (decoded)"}[snap.raw]
			break
		}
	}
	a.flash = fmt.Sprintf("exported %d messages%s to %s", len(snap.msgs), shown, path)
}

// inStateRoot reports whether path is inside k4a's state directory, which
// holds caches and session state, not user exports.
func (a *App) inStateRoot(path string) bool {
	if a.stateRoot == nil {
		return false
	}
	root, err := filepath.Abs(a.stateRoot.Path())
	if err != nil {
		return false
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(root, abs)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
