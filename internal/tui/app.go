// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package tui is k4a's interactive terminal UI: the bubbletea App, its view
// stack, key and command dispatch, and the background refreshes that feed
// the views.
package tui

import (
	"context"
	"fmt"
	"sync"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/chrome"
	"github.com/blairham/tuikit/loading"
	tktheme "github.com/blairham/tuikit/theme"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/remoteindex"
	"github.com/blairham/k4a/internal/searchcache"
	"github.com/blairham/k4a/internal/state"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

const (
	// pollInterval is how often views auto-refresh.
	pollInterval = 2 * time.Second

	// splashDuration is how long the startup splash screen is shown before
	// transitioning to the main view.
	splashDuration = 3 * time.Second
)

// tickMsg triggers periodic refresh.
type tickMsg time.Time

// splashDoneMsg signals that the startup splash duration has elapsed.
type splashDoneMsg struct{}

// versionCheckMsg carries the result of a GitHub release version check.
type versionCheckMsg struct {
	latest string // empty if check failed or current is latest
}

// ConnInfo holds connection metadata displayed in the info panel.
type ConnInfo struct {
	Context string
	Auth    string
	Brokers string
	Region  string
}

// logoLines is the small header logo in the top-right corner. It uses
// the figlet "graffiti" font with k9s's hand-kerned K, so the two tools
// share a look. All lines are padded to equal width for alignment.
var logoLines = []string{
	` ____  __    _____         `,
	`|    |/  /  /  |  |_____   `,
	`|       /  /   |  |\__  \  `,
	`|    \   \/    ^   // __ \ `,
	`|____|\__ \____   |(____  /`,
	`         \/    |__|     \/ `,
}

// logoBigLines is the startup splash logo, mirroring k9s's LogoBig.
var logoBigLines = []string{
	` ____  __    _____          _______  ____     ___ `,
	`|    |/  /  /  |  |________/   ___ \|    |   |   |`,
	`|       /  /   |  |\__  \      \  \/|    |   |   |`,
	`|    \   \/    ^   // __ \      \___|    |___|   |`,
	`|____|\__ \____   |(____  /\______  /_______ \___|`,
	`         \/    |__|     \/        \/        \/    `,
}

// loadingTips are shown alongside the spinner while views load data. The
// first entry doubles as the startup splash caption — renderSplash shows the
// loader's current tip, which is index 0 until a tick rotates it.
var loadingTips = []string{
	"Connecting...",
	"Fetching metadata from brokers...",
	"Kafka was named after the Czech novelist Franz Kafka",
	"Tip: Press ? for keyboard shortcuts",
	"A Kafka cluster can handle millions of messages per second",
	"Tip: Use / to filter the current view",
	"Consumer groups coordinate via the __consumer_offsets topic",
	"Tip: Press i to toggle internal topics",
	"Kafka was originally developed at LinkedIn in 2011",
	"Tip: Navigate with j/k, drill in with Enter, back with Esc",
	"ISR = In-Sync Replicas: the set of replicas fully caught up with the leader",
	"Tip: Use :produce or p to produce messages to a topic",
	"Kafka topics are divided into partitions for parallel processing",
	"Tip: Press o for topic overview details",
	"The Kafka protocol uses a binary format over TCP",
	"Tip: Press r to refresh the current view",
}

// pendingConfirm holds a destructive action awaiting y/n confirmation.
type pendingConfirm struct {
	action string // e.g. "delete_topic", "delete_acl"
	param  string // opaque identifier handed back to the action handler
}

// indexerEntry pairs an open AsyncIndexer with the cancel func that
// stops its dedicated background tail consumer. The tail runs
// independently of any view so the index keeps advancing even when no
// MessagesView is open for the topic; cancel fires at app shutdown.
type indexerEntry struct {
	async  *index.AsyncIndexer
	cancel context.CancelFunc
}

// App is the root bubbletea model.
type App struct {
	filterBar        *chrome.FilterBar
	client           *kafka.Client
	viewMap          map[style.ViewType]views.View
	promptDispatch   chrome.Dispatch
	session          *state.Session
	searchCache      *searchcache.Cache
	remoteIndex      *remoteindex.Client
	commandBar       *chrome.CommandBar
	stateRoot        *state.Root
	cfg              *config.Config
	confirmDispatch  chrome.ConfirmDispatch
	prompt           *chrome.Prompt
	confirm          *chrome.Confirm
	indexers         map[string]*indexerEntry
	connInfo         ConnInfo
	errFlash         string
	version          string
	latestVersion    string
	filter           string
	flash            string
	cfgPath          string
	filterStack      []string
	viewStack        []style.ViewType
	loader           loading.Model
	chrome           chrome.Chrome
	height           int
	width            int
	view             style.ViewType
	initialLoad      bool
	loading          bool
	logoless         bool
	readonly         bool
	showHelp         bool
	skipContextView  bool
	awaitingContext  bool
	splashActive     bool
	upgradeRequested bool
}

// activeView returns the current view, or nil if not registered.
func (a *App) activeView() views.View { return a.viewMap[a.view] }

// setView registers a view in the map and returns it.
func (a *App) setView(vt style.ViewType, v views.View) {
	a.viewMap[vt] = v
}

// typedView returns a view cast to the given concrete type, or nil.
func typedView[T views.View](a *App, vt style.ViewType) T {
	if v, ok := a.viewMap[vt].(T); ok {
		return v
	}
	var zero T
	return zero
}

// NewApp creates the root app model.
func NewApp(client *kafka.Client, info ConnInfo, cfg *config.Config, cfgPath, version string) *App {
	// k4a uses a NoPaintBackground theme so its hand-rolled chrome
	// keeps owning the background — only the bar widgets need a theme
	// for their textinput styling.
	t := tktheme.NoPaintBackground()
	commandBar := chrome.NewCommandBar(t, chrome.CommandBarOpts{
		Prompt:    "🐶:",
		CharLimit: 128,
		SuggestFn: func(value string) []string {
			if best := fuzzyMatch(value); best != "" {
				return []string{best}
			}
			return nil
		},
	})
	filterBar := chrome.NewFilterBar(t, chrome.FilterBarOpts{
		Prompt:    "🐩/",
		CharLimit: 128,
	})
	prompt := chrome.NewPrompt(t, chrome.PromptOpts{CharLimit: 256})
	confirm := chrome.NewConfirm(t)
	logoless := cfg != nil && cfg.UI.Logoless
	chromeCfg := chrome.Config{
		Theme:         t,
		InfoPanelRows: 5, // Context, Auth, Region, Brokers, K4a Rev
		MinLogoWidth:  120,
	}
	if !logoless {
		chromeCfg.Logo = logoLines
	}
	ch := chrome.New(chromeCfg)

	// loading.New themes the spinner from t.Logo (orange under
	// NoPaintBackground) and the rotating tip from t.MutedStyle (gray).
	loader := loading.New(t, loadingTips)

	// Tier 1 state. Always succeeds — a missing or corrupt state.json
	// yields a fresh zero-value session.
	stateRoot := state.NewRoot(config.DefaultConfigDir())
	session := state.LoadSession(stateRoot)
	// Tier 2 cache. Shared across all callers (TUI search view, future
	// CLI bridging) so a query run anywhere benefits everywhere.
	cache := searchcache.New(stateRoot)

	// Optional shared index for the active context (docs/design/shared-index-
	// service.md). Best-effort: any error leaves it nil and search stays local.
	remote := dialRemoteIndex(cfg, info.Context)

	vm := make(map[style.ViewType]views.View)
	topicsView := views.NewTopicsView(client)
	topicsView.SetIndexLookup(indexLookup(client, stateRoot))
	vm[style.ViewTopics] = topicsView
	vm[style.ViewGroups] = views.NewGroupsView(client)
	vm[style.ViewCluster] = views.NewClusterView(client)

	return &App{
		client:       client,
		connInfo:     info,
		cfg:          cfg,
		cfgPath:      cfgPath,
		session:      session,
		searchCache:  cache,
		remoteIndex:  remote,
		stateRoot:    stateRoot,
		view:         style.ViewTopics,
		viewMap:      vm,
		commandBar:   commandBar,
		filterBar:    filterBar,
		prompt:       prompt,
		confirm:      confirm,
		chrome:       ch,
		loader:       loader,
		version:      version,
		initialLoad:  true,
		loading:      true,
		logoless:     logoless,
		readonly:     cfg != nil && cfg.UI.Readonly,
		splashActive: true,
	}
}

// dialRemoteIndex resolves the active context's shared-index endpoint and dials
// it. Best-effort: a missing config, no endpoint, or a dial error all yield nil,
// leaving search fully local. grpc.NewClient is lazy, so a dead endpoint costs
// nothing here — the per-search Coverage RPC times out fast and falls back.
func dialRemoteIndex(cfg *config.Config, ctxName string) *remoteindex.Client {
	if cfg == nil {
		return nil
	}
	cctx, err := cfg.Resolve(ctxName)
	if err != nil || cctx.IndexEndpoint == "" {
		return nil
	}
	client, err := remoteindex.Dial(cctx.IndexEndpoint, cctx.IndexInsecure)
	if err != nil {
		return nil
	}
	return client
}

// indexLookup returns a closure for the Topics view's INDEX column. The
// cluster ID is fetched asynchronously the first time the lookup runs
// so we never block the UI thread on a broker round-trip. Until it
// resolves, the column stays empty; once cached, every call is a single
// filesystem stat under ~/.k4a/index/<cluster>/<topic>/.
func indexLookup(client *kafka.Client, root *state.Root) func(topic string) bool {
	if client == nil || root == nil {
		return nil
	}
	var (
		mu        sync.Mutex
		clusterID string
		started   bool
	)
	return func(topic string) bool {
		mu.Lock()
		id := clusterID
		s := started
		if !s {
			started = true
		}
		mu.Unlock()
		if !s {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				cid, _ := client.ClusterID(ctx) //nolint:errcheck // best-effort; column stays empty on failure
				mu.Lock()
				clusterID = cid
				mu.Unlock()
			}()
			return false
		}
		if id == "" {
			return false
		}
		return index.HasIndex(root, id, topic)
	}
}

// SetAwaitingContext marks the app as started without a context: it holds on
// the picker until the user chooses one, because there is no client to serve
// any other view. Cleared by the first successful switch.
func (a *App) SetAwaitingContext() {
	a.awaitingContext = true
}

// SetSkipContextView tells the app to skip the context selector after splash
// and jump directly to the topics view.
func (a *App) SetSkipContextView() {
	a.skipContextView = true
}

// UpgradeRequested reports whether the user triggered an in-app upgrade.
func (a *App) UpgradeRequested() bool { return a.upgradeRequested }

// UpgradeContext returns the active context name so the caller can restart
// k4a in the same context after upgrading.
func (a *App) UpgradeContext() string { return a.connInfo.Context }

// Init initializes the app.
//
// The Topics view is only primed when there is a client to prime it with.
// Started with no context chosen, the app opens on the picker and has no
// connection yet — kicking off a topic fetch would dial a nil client. The
// data views get built for real in applySwitchContext once a context is
// picked.
func (a *App) Init() tea.Cmd {
	cmds := make([]tea.Cmd, 0, 4)
	if a.client != nil {
		cmds = append(cmds, a.viewMap[style.ViewTopics].Init())
	}
	cmds = append(cmds, a.loader.Tick(), a.splashTimer(), a.checkVersion())
	return tea.Batch(cmds...)
}

// splashTimer returns a command that fires after the splash duration.
func (a *App) splashTimer() tea.Cmd {
	return tea.Tick(splashDuration, func(_ time.Time) tea.Msg {
		return splashDoneMsg{}
	})
}

// Update handles messages.
func (a *App) Update(msg tea.Msg) (tea.Model, tea.Cmd) { //nolint:gocyclo,gocognit,funlen // flat message dispatch
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		a.width = msg.Width
		a.height = msg.Height
		a.resizeActiveView()
		return a, nil

	case tickMsg:
		var cmd tea.Cmd
		if !a.loading {
			cmd = a.refreshActiveView()
		}
		return a, tea.Batch(cmd, a.tick())

	case versionCheckMsg:
		if msg.latest != "" {
			a.latestVersion = msg.latest
		}
		return a, nil

	case splashDoneMsg:
		a.splashActive = false
		a.initialLoad = false
		a.loading = false

		// If the user specified --context or --brokers, skip the context
		// selector and jump straight to the topics view.
		if a.skipContextView {
			cmd := a.switchView(style.ViewTopics)
			return a, cmd
		}

		// Otherwise land on the context selection screen.
		// The client is already connected to the last-used context in the background.
		a.setView(style.ViewContext, views.NewContextView(a.cfg, a.connInfo.Context))
		a.view = style.ViewContext
		a.resizeActiveView()
		return a, tea.Batch(a.viewMap[style.ViewContext].Init(), a.tick())

	case loading.TickMsg:
		cmd := a.loader.Update(msg)
		return a, cmd

	case tea.KeyMsg:
		return a.handleKey(msg)

	case tea.PasteMsg:
		// Route paste events to whichever bar is currently focused so
		// the user can paste a topic name / filter / value etc.
		switch {
		case a.commandBar.Active():
			_, cmd := a.commandBar.Update(msg, a.dispatchCommand)
			return a, cmd
		case a.filterBar.Active():
			_, cmd := a.filterBar.Update(msg, a.onFilterChange)
			return a, cmd
		case a.prompt.Active():
			_, cmd := a.prompt.Update(msg, a.promptDispatch)
			return a, cmd
		}
		return a, nil

	case deleteTopicMsg:
		a.loading = false
		if msg.err != nil {
			a.errFlash = msg.err.Error()
			return a, nil
		}
		return a, a.viewMap[style.ViewTopics].Refresh()

	case deleteACLMsg:
		a.loading = false
		if msg.err != nil {
			a.errFlash = msg.err.Error()
			return a, nil
		}
		if v := a.viewMap[style.ViewACLs]; v != nil {
			return a, v.Refresh()
		}
		return a, nil

	case deleteGroupMsg:
		a.loading = false
		if msg.err != nil {
			a.errFlash = msg.err.Error()
			return a, nil
		}
		return a, a.viewMap[style.ViewGroups].Refresh()

	case deleteOffsetMsg:
		a.loading = false
		if msg.err != nil {
			a.errFlash = msg.err.Error()
			return a, nil
		}
		a.flash = "Offset deleted"
		if v := a.viewMap[style.ViewGroupDetail]; v != nil {
			return a, v.Refresh()
		}
		return a, nil

	case purgeTopicMsg:
		a.loading = false
		if msg.err != nil {
			a.errFlash = msg.err.Error()
			return a, nil
		}
		a.flash = fmt.Sprintf("purged messages from %s", msg.topic)
		if v := typedView[*views.TopicDetailView](a, style.ViewTopicDetail); v != nil && v.Topic() == msg.topic {
			return a, v.Refresh()
		}
		return a, nil

	case setRetentionMsg:
		a.loading = false
		if msg.err != nil {
			a.errFlash = msg.err.Error()
			return a, nil
		}
		a.flash = fmt.Sprintf("retention for %s set to %s permanently", msg.topic, formatRetentionMs(msg.retentionMs))
		if v := typedView[*views.TopicDetailView](a, style.ViewTopicDetail); v != nil && v.Topic() == msg.topic {
			return a, v.Refresh()
		}
		return a, nil

	case resetRetentionMsg:
		a.loading = false
		if msg.err != nil {
			a.errFlash = msg.err.Error()
			return a, nil
		}
		a.flash = fmt.Sprintf("retention override removed for %s (reverted to broker default)", msg.topic)
		if v := typedView[*views.TopicDetailView](a, style.ViewTopicDetail); v != nil && v.Topic() == msg.topic {
			return a, v.Refresh()
		}
		return a, nil

	case editTopicConfigMsg:
		a.loading = false
		if msg.err != nil {
			a.errFlash = msg.err.Error()
			return a, nil
		}
		if v := a.viewMap[style.ViewTopicConfig]; v != nil {
			return a, v.Refresh()
		}
		return a, nil

	case increasePartitionsMsg:
		a.loading = false
		if msg.err != nil {
			a.errFlash = msg.err.Error()
			return a, nil
		}
		a.flash = "partitions increased"
		if v := a.viewMap[style.ViewTopicDetail]; v != nil {
			return a, v.Refresh()
		}
		return a, nil

	case changeReplicationMsg:
		a.loading = false
		if msg.err != nil {
			a.errFlash = msg.err.Error()
			return a, nil
		}
		a.flash = "replication factor changed"
		if v := a.viewMap[style.ViewTopicDetail]; v != nil {
			return a, v.Refresh()
		}
		return a, nil

	case switchContextMsg:
		return a.applySwitchContext(msg)

	// Data refresh messages — mark loading done and delegate.
	case views.TopicsRefreshMsg, views.GroupsRefreshMsg, views.ClusterRefreshMsg,
		views.TopicCountsMsg, views.GroupEnrichmentMsg, views.TopicDetailRefreshMsg,
		views.GroupDetailRefreshMsg,
		views.ProduceProgressMsg, views.ProduceDoneMsg,
		views.CreateTopicResultMsg, views.CreateACLResultMsg,
		views.ContextRefreshMsg, views.ACLsRefreshMsg,
		views.TopicConfigRefreshMsg,
		views.ResetOffsetsTopicsMsg, views.ResetOffsetsResultMsg,
		views.BrokerDetailRefreshMsg,
		views.ConsumerErrorMsg:
		// On any fetch error, drop cached connections so the next
		// refresh dials fresh (picks up new creds, DNS, etc.).
		if refreshMsgHasError(msg) && a.client != nil {
			a.client.CloseIdleConnections()
		}
		switch {
		case a.splashActive:
			// Data loading in background during splash — don't change loading state.
		case a.initialLoad:
			// After context switch, clear loading when topics arrive.
			switch m := msg.(type) {
			case views.TopicsRefreshMsg:
				a.initialLoad = false
				a.loading = false
				if m.Err != nil && a.client != nil {
					a.client.CloseIdleConnections()
				}
			case views.TopicCountsMsg:
				a.initialLoad = false
				a.loading = false
			}
		case a.refreshMsgMatchesView(msg):
			a.loading = false
		}
		cmd := a.updateActiveView(msg)
		return a, cmd

	case views.SearchMatchMsg, views.SearchProgressMsg, views.SearchErrMsg,
		views.SearchChanClosedMsg, views.SearchTerminalMsg:
		// Route deep-search messages straight to the SearchView even
		// when it's not the active view, so the scan can stream in
		// matches while the user navigates into a message detail and
		// back.
		if v := a.viewMap[style.ViewSearch]; v != nil {
			return a, v.Update(msg)
		}
		return a, nil

	default:
		cmd := a.updateActiveView(msg)
		return a, cmd
	}
}
