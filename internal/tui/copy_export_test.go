// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

func TestOSC52(t *testing.T) {
	t.Parallel()
	if got, want := osc52("hi", false), "\x1b]52;c;aGk=\x07"; got != want {
		t.Errorf("osc52 = %q, want %q", got, want)
	}
	// tmux: the plain sequence for set-clipboard, then the DCS passthrough
	// with the inner ESC doubled for allow-passthrough.
	want := "\x1b]52;c;aGk=\x07" + "\x1bPtmux;\x1b\x1b]52;c;aGk=\x07\x1b\\"
	if got := osc52("hi", true); got != want {
		t.Errorf("osc52 under tmux = %q, want %q", got, want)
	}
	// Multi-line, non-ASCII text survives the round trip.
	text := "{\n  \"name\": \"Zoë ✓\"\n}"
	if got := decodeOSC52(t, osc52(text, false)); got != text {
		t.Errorf("round trip = %q, want %q", got, text)
	}
}

// decodeOSC52 returns the text an OSC 52 sequence (the first, under tmux)
// puts on the clipboard.
func decodeOSC52(t *testing.T, seq string) string {
	t.Helper()
	body, ok := strings.CutPrefix(seq, "\x1b]52;c;")
	if !ok {
		t.Fatalf("not an OSC 52 sequence: %q", seq)
	}
	end := strings.IndexByte(body, '\x07')
	if end < 0 {
		t.Fatalf("unterminated OSC 52 sequence: %q", seq)
	}
	data, err := base64.StdEncoding.DecodeString(body[:end])
	if err != nil {
		t.Fatalf("payload is not base64: %v", err)
	}
	return string(data)
}

// decodedMsg is a registry-decoded message: Value/Key decoded, the raw
// renderings kept in the Decoding.
func decodedMsg(off int64) kafka.ConsumedMessage {
	return kafka.ConsumedMessage{
		Topic: "orders", Partition: 1, Offset: off, Time: time.Unix(1_700_000_000, 0).UTC(),
		Key: "k-decoded", Value: `{"id":7}`,
		Headers:       []kafka.MessageHeader{{Key: "trace", Value: "abc"}},
		KeyDecoding:   kafka.Decoding{Schema: "orders-key v1 (id 1, AVRO)", Raw: "k-raw"},
		ValueDecoding: kafka.Decoding{Schema: "orders-value v3 (id 11, AVRO)", Raw: "\\x00raw-bytes"},
	}
}

func testApp(t *testing.T) *App {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("TMUX", "")
	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width, app.height = 120, 40
	app.stateRoot = state.NewRoot(filepath.Join(t.TempDir(), ".k4a"))
	return app
}

// copied runs a copy action and returns what it put on the clipboard.
func copied(t *testing.T, app *App, key string) string {
	t.Helper()
	_, cmd := app.handleAction(views.ActionCopy, key)
	if cmd == nil {
		t.Fatalf("copy %q: no command (errFlash %q)", key, app.errFlash)
	}
	raw, ok := cmd().(tea.RawMsg)
	if !ok {
		t.Fatalf("copy %q: command returned %T, want tea.RawMsg", key, cmd())
	}
	seq, ok := raw.Msg.(string)
	if !ok {
		t.Fatalf("copy %q: raw message is %T, want string", key, raw.Msg)
	}
	return decodeOSC52(t, seq)
}

func TestCopyFromMessageDetailFollowsRawToggle(t *testing.T) {
	app := testApp(t)
	m := decodedMsg(5)
	detail := views.NewMessageDetailView(&m)
	app.setView(style.ViewMessageDetail, detail)
	app.view = style.ViewMessageDetail
	app.resizeActiveView()

	if got := copied(t, app, views.KeyCopyValue); got != `{"id":7}` {
		t.Errorf("decoded value copy = %q", got)
	}
	if !strings.Contains(app.flash, "value (decoded)") {
		t.Errorf("flash %q does not say the value was decoded", app.flash)
	}
	if strings.Contains(app.flash, `"id"`) {
		t.Errorf("flash %q shows the copied content", app.flash)
	}
	if got := copied(t, app, views.KeyCopyKey); got != "k-decoded" {
		t.Errorf("decoded key copy = %q", got)
	}

	detail.HandleKey(views.KeyToggleRaw)
	if got := copied(t, app, views.KeyCopyValue); got != "\\x00raw-bytes" {
		t.Errorf("raw value copy = %q", got)
	}
	if !strings.Contains(app.flash, "value (raw)") {
		t.Errorf("flash %q does not say the value was raw", app.flash)
	}

	var rec map[string]any
	if err := json.Unmarshal([]byte(copied(t, app, views.KeyCopyRecord)), &rec); err != nil {
		t.Fatalf("record copy is not JSON: %v", err)
	}
	if rec["key"] != "k-raw" || rec["value"] != "\\x00raw-bytes" {
		t.Errorf("raw record copy has key %v, value %v", rec["key"], rec["value"])
	}
	if dec, _ := rec["decoding"].(map[string]any); dec["shown"] != "raw" {
		t.Errorf("record copy decoding = %v, want shown raw", rec["decoding"])
	}
}

func TestCopyEmptyKeyCopiesNothing(t *testing.T) {
	app := testApp(t)
	m := kafka.ConsumedMessage{Topic: "orders", Value: "v"}
	app.setView(style.ViewMessageDetail, views.NewMessageDetailView(&m))
	app.view = style.ViewMessageDetail

	// An empty OSC 52 payload would clear the clipboard instead.
	if _, cmd := app.handleAction(views.ActionCopy, views.KeyCopyKey); cmd != nil {
		t.Error("copying a null key returned a command")
	}
	if app.errFlash == "" {
		t.Error("copying a null key flashed nothing")
	}
}

// filteredMessages is a Messages view holding three messages, filtered down
// to the middle one, which is not where the cursor index would land in the
// unfiltered buffer.
func filteredMessages(app *App) *views.MessagesView {
	mv := views.NewMessagesView(nil, "orders")
	for i, v := range []string{"alpha", "bravo", "charlie"} {
		mv.Update(views.MessageMsg(kafka.ConsumedMessage{ //nolint:errcheck
			Topic: "orders", Offset: int64(i), Key: "k" + v, Value: v, Time: time.Unix(int64(i), 0),
		}))
	}
	mv.SetFilter("bravo")
	app.setView(style.ViewMessages, mv)
	app.view = style.ViewMessages
	app.resizeActiveView()
	return mv
}

func TestCopyAndOpenUseTheFilteredRow(t *testing.T) {
	app := testApp(t)
	mv := filteredMessages(app)

	if got := copied(t, app, views.KeyCopyValue); got != "bravo" {
		t.Errorf("copied %q from the filtered row, want bravo", got)
	}
	if strings.Contains(app.flash, "decoded") || strings.Contains(app.flash, "raw") {
		t.Errorf("flash %q mentions decoding for an undecoded message", app.flash)
	}

	// enter used to index the unfiltered buffer, opening the wrong message.
	action, param := mv.HandleKey(views.KeyEnter)
	app.handleAction(action, param)
	detail := typedView[*views.MessageDetailView](app, style.ViewMessageDetail)
	if detail == nil {
		t.Fatal("enter on the filtered row opened no detail view")
	}
	if got := detail.GetMessage().Value; got != "bravo" {
		t.Errorf("enter on the filtered row opened %q, want bravo", got)
	}
}

// object is v as a JSON object, or nil.
func object(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func readJSONL(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("line %d is not JSON: %v: %s", len(out)+1, err, sc.Text())
		}
		out = append(out, rec)
	}
	return out
}

func TestWriteExport(t *testing.T) {
	t.Parallel()
	plain := kafka.ConsumedMessage{
		Topic: "orders", Partition: 0, Offset: 9, Time: time.Unix(1_700_000_000, 0).UTC(), Value: "not json",
	}
	failed := plain
	failed.Offset = 10
	failed.ValueDecoding = kafka.Decoding{Reason: "schema 99 not found"}
	path := filepath.Join(t.TempDir(), "sub", "out.jsonl")

	if err := writeExport(path, []kafka.ConsumedMessage{decodedMsg(8), plain, failed}, false, false); err != nil {
		t.Fatal(err)
	}
	recs := readJSONL(t, path)
	if len(recs) != 3 {
		t.Fatalf("%d records, want 3", len(recs))
	}

	d := recs[0]
	if d["topic"] != "orders" || d["partition"] != 1.0 || d["offset"] != 8.0 ||
		d["timestamp"] != "2023-11-14T22:13:20Z" || d["key"] != "k-decoded" {
		t.Errorf("decoded record metadata = %v", d)
	}
	if v, _ := d["value"].(map[string]any); v["id"] != 7.0 {
		t.Errorf("JSON value not embedded as JSON: %v", d["value"])
	}
	if h, _ := d["headers"].([]any); len(h) != 1 {
		t.Errorf("headers = %v", d["headers"])
	}
	dec, _ := d["decoding"].(map[string]any)
	if dec["shown"] != "decoded" || object(dec["value"])["schema"] != "orders-value v3 (id 11, AVRO)" {
		t.Errorf("decoding = %v", d["decoding"])
	}

	if recs[1]["value"] != "not json" {
		t.Errorf("non-JSON value = %v, want a string", recs[1]["value"])
	}
	if _, ok := recs[1]["decoding"]; ok {
		t.Errorf("undecoded record carries decoding %v", recs[1]["decoding"])
	}
	if dec, _ := recs[2]["decoding"].(map[string]any); object(dec["value"])["error"] != "schema 99 not found" {
		t.Errorf("failed decode not recorded: %v", recs[2]["decoding"])
	}

	if err := writeExport(path, nil, false, false); !errors.Is(err, fs.ErrExist) {
		t.Errorf("second export without overwrite: err = %v, want fs.ErrExist", err)
	}
	if got := len(readJSONL(t, path)); got != 3 {
		t.Errorf("refused export changed the file: %d records", got)
	}
	if err := writeExport(path, []kafka.ConsumedMessage{plain}, false, true); err != nil {
		t.Fatal(err)
	}
	if got := len(readJSONL(t, path)); got != 1 {
		t.Errorf("overwrite left %d records, want 1", got)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("export mode = %v (%v), want 0600", fi.Mode().Perm(), err)
	}
}

func TestExportFromMessagesRespectsFilterAndAsksBeforeOverwrite(t *testing.T) {
	app := testApp(t)
	filteredMessages(app)
	path := filepath.Join(t.TempDir(), "out.jsonl")

	app.handleAction(views.ActionExport, "orders")
	if !app.prompt.Active() {
		t.Fatal("export did not open the path prompt")
	}
	if errMsg, _ := app.promptDispatch(path); errMsg != "" {
		t.Fatalf("export: %s", errMsg)
	}
	recs := readJSONL(t, path)
	if len(recs) != 1 || recs[0]["value"] != "bravo" {
		t.Fatalf("exported %v, want only the filtered bravo", recs)
	}
	if !strings.Contains(app.flash, "exported 1 messages") {
		t.Errorf("flash = %q", app.flash)
	}

	// Exporting again to the same path asks; no keeps the file.
	if err := os.WriteFile(path, []byte("keep\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	app.handleAction(views.ActionExport, "orders")
	app.promptDispatch(path) //nolint:errcheck
	if !app.confirm.Active() {
		t.Fatal("an existing file was not confirmed before overwrite")
	}
	app.confirmDispatch(false) //nolint:errcheck
	if data, _ := os.ReadFile(path); string(data) != "keep\n" {
		t.Errorf("declined overwrite changed the file to %q", data)
	}
	app.handleAction(views.ActionExport, "orders")
	app.promptDispatch(path)  //nolint:errcheck
	app.confirmDispatch(true) //nolint:errcheck
	if recs := readJSONL(t, path); len(recs) != 1 || recs[0]["value"] != "bravo" {
		t.Errorf("confirmed overwrite wrote %v", recs)
	}

	inState := filepath.Join(app.stateRoot.Path(), "out.jsonl")
	if errMsg, _ := app.promptDispatch(inState); errMsg == "" {
		t.Error("export into the state directory was not refused")
	}
	if _, err := os.Stat(inState); err == nil {
		t.Error("export wrote into the state directory")
	}
}

func TestExportFromSearch(t *testing.T) {
	app := testApp(t)
	sv := views.NewSearchView(nil, "orders", "a")
	for i := range 2 {
		sv.Update(
			views.SearchMatchMsg{Msg: kafka.ConsumedMessage{Topic: "orders", Offset: int64(i), Value: "a"}},
		) //nolint:errcheck
	}
	app.setView(style.ViewSearch, sv)
	app.view = style.ViewSearch
	action, param := sv.HandleKey(views.KeyExport)
	app.handleAction(action, param)

	path := filepath.Join(t.TempDir(), "search.jsonl")
	if errMsg, _ := app.promptDispatch(path); errMsg != "" {
		t.Fatalf("export: %s", errMsg)
	}
	if recs := readJSONL(t, path); len(recs) != 2 || recs[1]["offset"] != 1.0 {
		t.Errorf("exported %v, want both matches in order", recs)
	}
}

func TestMessageDetailNextPrevWalksTheListItCameFrom(t *testing.T) {
	app := testApp(t)
	sv := views.NewSearchView(nil, "orders", "a")
	for i := range 3 {
		sv.Update(views.SearchMatchMsg{Msg: kafka.ConsumedMessage{Topic: "orders", Offset: int64(10 + i)}}) //nolint:errcheck
	}
	app.setView(style.ViewSearch, sv)
	app.view = style.ViewSearch
	app.handleAction("search_message_detail", "1")

	detail := typedView[*views.MessageDetailView](app, style.ViewMessageDetail)
	if detail == nil {
		t.Fatal("no detail view")
	}
	steps := []struct {
		key  string
		want int64
	}{
		{views.KeyNextMessage, 12},
		{views.KeyNextMessage, 12}, // stays at the end
		{views.KeyPrevMessage, 11},
		{views.KeyPrevMessage, 10},
		{views.KeyPrevMessage, 10}, // and at the start
	}
	for _, s := range steps {
		detail.HandleKey(s.key)
		if got := detail.GetMessage().Offset; got != s.want {
			t.Fatalf("after %s: offset %d, want %d", s.key, got, s.want)
		}
	}
	if !strings.Contains(detail.View(), "1 of 3") {
		t.Error("detail does not show its position in the list")
	}
}
