// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/huh/v2"
	"charm.land/lipgloss/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// ProduceProgressMsg carries a single produce result.
type ProduceProgressMsg struct {
	Result kafka.ProduceResult
	Total  int
}

// ProduceDoneMsg signals that producing is complete.
type ProduceDoneMsg struct {
	Sent   int
	Failed int
}

// produceState tracks the produce view state machine.
type produceState int

const (
	produceStateForm produceState = iota
	produceStateRunning
	produceStateDone
)

// ProduceView handles message production with a form and progress display.
type ProduceView struct {
	cancel   context.CancelFunc
	resultCh chan kafka.ProduceResult
	form     *huh.Form
	client   *kafka.Client
	schema   string
	topic    string
	count    string
	interval string
	results  []kafka.ProduceResult
	table    table.Model
	state    produceState
	total    int
	sent     int
	failed   int
	width    int
	height   int
}

// NewProduceView creates a new produce view, optionally pre-filling the topic.
func NewProduceView(client *kafka.Client, topic string) *ProduceView {
	v := &ProduceView{
		client:   client,
		topic:    topic,
		count:    "10",
		interval: "1s",
		state:    produceStateForm,
	}

	schemaOptions := discoverSchemas()

	var schemaField huh.Field
	if len(schemaOptions) > 0 {
		opts := make([]huh.Option[string], 0, 1+len(schemaOptions))
		opts = append(opts, huh.NewOption("(random messages)", ""))
		for _, s := range schemaOptions {
			opts = append(opts, huh.NewOption(filepath.Base(s), s))
		}
		schemaField = huh.NewSelect[string]().
			Title("Schema").
			Key("schema").
			Options(opts...).
			Value(&v.schema)
	} else {
		schemaField = huh.NewInput().
			Title("Schema").
			Key("schema").
			Placeholder("path to schema YAML (blank for random)").
			Value(&v.schema)
	}

	v.form = huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Topic").
				Key("topic").
				Placeholder("topic name").
				Value(&v.topic),
			huh.NewInput().
				Title("Count").
				Key("count").
				Placeholder("number of messages").
				Value(&v.count),
			huh.NewInput().
				Title("Interval").
				Key("interval").
				Placeholder("e.g. 1s, 500ms").
				Value(&v.interval),
			schemaField,
		),
	).WithTheme(huh.ThemeFunc(huh.ThemeDracula)).
		WithWidth(60)

	cols := []table.Column{
		{Title: "#", Width: 5},
		{Title: "KEY", Width: 36},
		{Title: "STATUS", Width: 10},
		{Title: "LATENCY", Width: 12},
	}

	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)

	v.table = t
	return v
}

// Init returns the initial command — starts the huh form.
func (v *ProduceView) Init() tea.Cmd {
	return v.form.Init()
}

// Update handles messages.
func (v *ProduceView) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case ProduceProgressMsg:
		v.results = append(v.results, msg.Result)
		if msg.Result.Err != nil {
			v.failed++
		} else {
			v.sent++
		}
		v.rebuildRows()
		return v.waitForResult()
	case ProduceDoneMsg:
		v.state = produceStateDone
		v.sent = msg.Sent
		v.failed = msg.Failed
		return nil
	}

	// Delegate to huh form when in form state.
	if v.state == produceStateForm {
		model, cmd := v.form.Update(msg)
		if f, ok := model.(*huh.Form); ok {
			v.form = f
		}
		if v.form.State == huh.StateCompleted {
			return v.startProduce()
		}
		return cmd
	}

	return nil
}

// UpdateTable delegates to the bubbles table.
func (v *ProduceView) UpdateTable(msg tea.Msg) tea.Cmd {
	if v.state == produceStateForm {
		return nil
	}
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates dimensions.
func (v *ProduceView) Resize(width, height int) {
	v.width = width
	v.height = height
	v.form.WithWidth(width - 4) //nolint:errcheck // builder returns self
	v.table.SetWidth(width)
	v.table.SetHeight(height - 4) // Leave room for status line.
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the result count.
func (v *ProduceView) Count() int {
	if v.state == produceStateForm {
		return 0
	}
	return len(v.results)
}

// HandleKey processes keys for the produce view.
func (v *ProduceView) HandleKey(_ string) (string, string) {
	return "", ""
}

// HandleKeyMsg processes tea.KeyMsg for form interaction.
func (v *ProduceView) HandleKeyMsg(msg tea.KeyMsg) tea.Cmd {
	if v.state == produceStateForm {
		// Delegate to the huh form via Update.
		return v.Update(msg)
	}
	return nil
}

// Loading returns false — produce view tracks its own state.
func (v *ProduceView) Loading() bool { return false }

// SetFilter is a no-op.
func (v *ProduceView) SetFilter(_ string) {}

// Stop cancels any in-progress production.
func (v *ProduceView) Stop() {
	if v.cancel != nil {
		v.cancel()
	}
}

// View renders the produce view.
func (v *ProduceView) View() string {
	switch v.state {
	case produceStateForm:
		return v.form.View()
	case produceStateRunning, produceStateDone:
		return v.renderProgress()
	}
	return ""
}

// Refresh is a no-op for produce view.
func (v *ProduceView) Refresh() tea.Cmd { return nil }

func (v *ProduceView) renderProgress() string {
	var sb strings.Builder

	statusStyle := lipgloss.NewStyle().Padding(0, 1)

	if v.state == produceStateRunning {
		progress := fmt.Sprintf("  Producing to %s: [%d/%d] sent=%d failed=%d",
			v.topic, len(v.results), v.total, v.sent, v.failed)
		sb.WriteString(statusStyle.Foreground(style.ColorYellow).Render(progress))
	} else {
		summary := fmt.Sprintf("  Done: %d sent, %d failed → %s", v.sent, v.failed, v.topic)
		if v.failed > 0 {
			sb.WriteString(statusStyle.Foreground(style.ColorRed).Render(summary))
		} else {
			sb.WriteString(statusStyle.Foreground(style.ColorGreen).Render(summary))
		}
	}
	sb.WriteString("\n")
	sb.WriteString(fixSelectedRow(v.table.View()))

	return sb.String()
}

func (v *ProduceView) startProduce() tea.Cmd {
	if v.topic == "" {
		return nil
	}

	count, err := strconv.Atoi(v.count)
	if err != nil || count <= 0 {
		count = 10
	}

	interval, err := time.ParseDuration(v.interval)
	if err != nil {
		interval = time.Second
	}

	// Generate messages.
	var messages []*kgo.Record
	if v.schema != "" {
		schema, schemaErr := kafka.LoadTopicSchema(v.schema)
		if schemaErr != nil {
			return nil
		}
		messages, err = kafka.GenerateSchemaMessages(schema, count)
	} else {
		messages, err = kafka.GenerateMessages(v.topic, count)
	}
	if err != nil {
		return nil
	}

	v.state = produceStateRunning
	v.total = len(messages)
	v.results = nil
	v.sent = 0
	v.failed = 0

	ctx, cancel := context.WithCancel(context.Background())
	v.cancel = cancel
	v.resultCh = make(chan kafka.ProduceResult, len(messages))

	go func() {
		defer close(v.resultCh)
		v.client.ProduceAll(ctx, messages, interval, func(r kafka.ProduceResult) {
			v.resultCh <- r
		})
	}()

	return v.waitForResult()
}

func (v *ProduceView) waitForResult() tea.Cmd {
	ch := v.resultCh
	if ch == nil {
		return nil
	}
	total := v.total
	return func() tea.Msg {
		r, ok := <-ch
		if !ok {
			return ProduceDoneMsg{}
		}
		return ProduceProgressMsg{Result: r, Total: total}
	}
}

func (v *ProduceView) rebuildRows() {
	rows := make([]table.Row, 0, len(v.results))
	for _, r := range v.results {
		status := style.StatusRunning.Render("OK")
		if r.Err != nil {
			status = style.StatusError.Render("FAIL")
		}
		rows = append(rows, table.Row{
			fmt.Sprintf("%d", r.Sequence),
			r.Key,
			status,
			r.Duration.Round(time.Millisecond).String(),
		})
	}
	setTableRows(&v.table, rows)
}

// discoverSchemas finds YAML files in ~/.k4a/schemas/ and ./schemas/.
func discoverSchemas() []string {
	var paths []string

	dirs := []string{"schemas"}
	if home, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(home, ".k4a", "schemas"))
	}

	for _, dir := range dirs {
		matches, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
		if err == nil {
			paths = append(paths, matches...)
		}
		matches, err = filepath.Glob(filepath.Join(dir, "*.yml"))
		if err == nil {
			paths = append(paths, matches...)
		}
	}

	return paths
}
