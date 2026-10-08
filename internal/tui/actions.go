// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/remoteindex"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// openView pushes the current view onto the stack, sets the new view type,
// resizes, and returns the init command. Used by handleAction for drill-in views.
func (a *App) openView(v style.ViewType, init tea.Cmd) (tea.Model, tea.Cmd) {
	a.pushView(a.view)
	a.view = v
	a.resizeActiveView()
	return a, init
}

// startConfigEdit opens the prompt bar pre-filled with the current
// config value so the user can edit it inline.
func (a *App) startConfigEdit(configName string) (tea.Model, tea.Cmd) {
	v := typedView[*views.TopicConfigView](a, style.ViewTopicConfig)
	if v == nil {
		return a, nil
	}
	entry := v.SelectedConfig(configName)
	if entry == nil {
		return a, nil
	}
	topic := v.Topic()
	key := topic + "\x00" + configName
	cmd := a.openValuePrompt("alter_topic_config", key, entry.Value, "config:", "", true)
	a.resizeActiveView()
	return a, cmd
}

// promptDeleteOffset sets up a confirmation prompt for deleting a single offset.
func (a *App) promptDeleteOffset() (tea.Model, tea.Cmd) {
	v := typedView[*views.GroupDetailView](a, style.ViewGroupDetail)
	if a.view != style.ViewGroupDetail || v == nil {
		return a, nil
	}
	groupID := v.GroupID()
	info := v.GetSelectedOffset()
	if info == nil {
		return a, nil
	}
	param := fmt.Sprintf("%s\x00%s\x00%d", groupID, info.Topic, info.Partition)
	prompt := fmt.Sprintf("delete offset for %s:%d?", info.Topic, info.Partition)
	return a.promptConfirm("delete_offset", param, prompt)
}

// promptConfirm opens the y/n Confirm bar.
func (a *App) promptConfirm(action, param, prompt string) (tea.Model, tea.Cmd) {
	cmd := a.openConfirmPrompt(action, param, prompt)
	a.resizeActiveView()
	return a, cmd
}

// promptSaveMessage opens the prompt bar pre-filled with a suggested filename.
func (a *App) promptSaveMessage(param string) (tea.Model, tea.Cmd) {
	msg := a.resolveMessage(param)
	if msg == nil {
		return a, nil
	}
	filename := messageFilename(msg)
	cmd := a.openValuePrompt("save_message", param, filename, "save:", "", true)
	a.resizeActiveView()
	return a, cmd
}

// resolveMessage looks up a consumed message by param string (index for messages
// view, or "detail" for message detail view).
func (a *App) resolveMessage(param string) *kafka.ConsumedMessage {
	if a.view == style.ViewMessageDetail {
		if v := typedView[*views.MessageDetailView](a, style.ViewMessageDetail); v != nil {
			return v.GetMessage()
		}
	}
	if a.view == style.ViewMessages {
		if v := typedView[*views.MessagesView](a, style.ViewMessages); v != nil {
			idx := 0
			if n, err := fmt.Sscanf(param, "%d", &idx); n == 1 && err == nil {
				return v.GetMessage(idx)
			}
		}
	}
	return nil
}

// Action names a view, key or command dispatches through handleAction, for
// those spelled in more than one place.
const (
	actionProduce            = "produce"
	actionSearch             = "search"
	actionIncreasePartitions = "increase_partitions"
	actionChangeReplication  = "change_replication"
	actionCreateTopic        = "create_topic"
	actionCreateACL          = "create_acl"
	actionContextView        = "context_view"
)

// writeActions are actions that mutate Kafka state and are blocked in readonly mode.
var writeActions = map[string]bool{
	actionProduce:            true,
	actionCreateTopic:        true,
	actionCreateACL:          true,
	"reset_offsets":          true,
	"edit_topic_config":      true,
	actionIncreasePartitions: true,
	actionChangeReplication:  true,
	"confirm_purge_topic":    true,
	"confirm_delete_topic":   true,
	"confirm_delete_acl":     true,
	"confirm_delete_group":   true,
}

func (a *App) handleAction(action, param string) (tea.Model, tea.Cmd) { //nolint:gocyclo,funlen // flat action dispatch
	if a.readonly && writeActions[action] {
		a.errFlash = "readonly mode"
		return a, nil
	}

	switch action {
	case "topic_detail":
		a.setView(style.ViewTopicDetail, views.NewTopicDetailView(a.client, param))
		return a.openView(style.ViewTopicDetail, a.viewMap[style.ViewTopicDetail].Init())
	case "group_detail":
		a.setView(style.ViewGroupDetail, views.NewGroupDetailView(a.client, param))
		return a.openView(style.ViewGroupDetail, a.viewMap[style.ViewGroupDetail].Init())
	case "messages":
		a.stopStoppableViews()
		a.setView(style.ViewMessages, views.NewMessagesView(a.client, param))
		// Phase C-1: when indexing is enabled in config, start an
		// AsyncIndexer for this topic and tee live-tail records into
		// it. No-op when indexing is off, the cluster ID isn't
		// available, or another instance holds the writer lock.
		a.startIndexer(param)
		return a.openView(style.ViewMessages, a.viewMap[style.ViewMessages].Init())
	case actionProduce:
		a.stopStoppableViews()
		a.setView(style.ViewProduce, views.NewProduceView(a.client, param))
		return a.openView(style.ViewProduce, a.viewMap[style.ViewProduce].Init())
	case actionSearch:
		return a.openSearch(param)
	case actionCreateTopic:
		a.setView(style.ViewCreateTopic, views.NewCreateTopicView(a.client))
		return a.openView(style.ViewCreateTopic, a.viewMap[style.ViewCreateTopic].Init())
	case actionCreateACL:
		a.setView(style.ViewCreateACL, views.NewCreateACLView(a.client))
		return a.openView(style.ViewCreateACL, a.viewMap[style.ViewCreateACL].Init())
	case "topic_config":
		a.setView(style.ViewTopicConfig, views.NewTopicConfigView(a.client, param))
		return a.openView(style.ViewTopicConfig, a.viewMap[style.ViewTopicConfig].Init())
	case "broker_detail":
		return a.openBrokerDetail(param)
	case "reset_offsets":
		a.setView(style.ViewResetOffsets, views.NewResetOffsetsView(a.client, param))
		return a.openView(style.ViewResetOffsets, a.viewMap[style.ViewResetOffsets].Init())
	case "edit_topic_config":
		return a.startConfigEdit(param)
	case actionIncreasePartitions:
		return a.startIncreasePartitions(param)
	case actionChangeReplication:
		return a.startChangeReplication(param)
	case "confirm_purge_topic":
		return a.startPurgeTopic(param)
	case "confirm_delete_topic":
		return a.promptConfirm("delete_topic", param, fmt.Sprintf("delete topic %s?", param))
	case "confirm_delete_acl":
		label := aclConfirmLabel(typedView[*views.ACLsView](a, style.ViewACLs), param)
		return a.promptConfirm("delete_acl", param, fmt.Sprintf("delete ACL %s?", label))
	case "confirm_delete_group":
		return a.promptConfirm("delete_group", param, fmt.Sprintf("delete group %s?", param))
	case actionContextView:
		if a.cfg == nil {
			return a, nil
		}
		a.setView(style.ViewContext, views.NewContextView(a.cfg, a.connInfo.Context))
		return a.openView(style.ViewContext, a.viewMap[style.ViewContext].Init())
	case "switch_context":
		if a.cfg == nil {
			return a, nil
		}
		cmd := a.selectContext(param)
		return a, cmd
	case "message_detail":
		cmd := a.openMessageAt(param)
		return a, cmd
	case "search_message_detail":
		// Opens the detail view for a SearchView match. The search keeps
		// running in the background — popping back returns to streaming
		// results, no scan restart.
		cmd := a.openSearchMatchAt(param)
		return a, cmd
	case "save_message":
		return a.promptSaveMessage(param)
	}
	return a, nil
}

// openSearch handles the search action's two forms:
//
//	"topic"                 → open the search bar (no query yet)
//	"topic\x00query"        → run a search for topic+query
//
// The first form is dispatched by the `s` key in MessagesView and by the
// `:search` command. The second form is dispatched by the command-bar's Enter
// handler after the user types a query.
func (a *App) openSearch(param string) (tea.Model, tea.Cmd) {
	topic, query := splitSearchParam(param)
	if query == "" {
		return a.openSearchBar(topic)
	}
	runFn := a.searchRunner()
	a.setView(style.ViewSearch, views.NewSearchView(runFn, topic, query))
	return a.openView(style.ViewSearch, a.viewMap[style.ViewSearch].Init())
}

// openBrokerDetail opens the detail view for the broker whose ID is param,
// doing nothing when the cluster view does not know it.
func (a *App) openBrokerDetail(param string) (tea.Model, tea.Cmd) {
	brokerID := 0
	fmt.Sscanf(param, "%d", &brokerID) //nolint:errcheck // best-effort parse
	cv := typedView[*views.ClusterView](a, style.ViewCluster)
	if cv == nil {
		return a, nil
	}
	broker, isController := cv.BrokerByID(brokerID)
	if broker == nil {
		return a, nil
	}
	a.setView(style.ViewBrokerDetail, views.NewBrokerDetailView(a.client, *broker, isController))
	return a.openView(style.ViewBrokerDetail, a.viewMap[style.ViewBrokerDetail].Init())
}

// selectContext switches to the named context. If already connected to it, go
// straight to topics with a fresh view so the loading spinner shows until
// counts arrive.
func (a *App) selectContext(name string) tea.Cmd {
	a.loading = true
	if name == a.connInfo.Context {
		delete(a.viewMap, style.ViewContext)
		a.setView(style.ViewTopics, views.NewTopicsView(a.client))
		return a.switchView(style.ViewTopics)
	}
	return a.doSwitchContext(name)
}

// openMessageAt opens the detail view for the Messages row param indexes,
// doing nothing when there is no such row.
func (a *App) openMessageAt(param string) tea.Cmd {
	v := typedView[*views.MessagesView](a, style.ViewMessages)
	if v == nil {
		return nil
	}
	idx, ok := parseIndex(param)
	if !ok {
		return nil
	}
	msg := v.GetMessage(idx)
	if msg == nil {
		return nil
	}
	return a.openMessageDetail(msg)
}

// openSearchMatchAt opens the detail view for the search match param indexes,
// doing nothing when there is no such match.
func (a *App) openSearchMatchAt(param string) tea.Cmd {
	v := typedView[*views.SearchView](a, style.ViewSearch)
	if v == nil {
		return nil
	}
	idx, ok := parseIndex(param)
	if !ok {
		return nil
	}
	msg, ok := v.GetMatch(idx)
	if !ok {
		return nil
	}
	return a.openMessageDetail(&msg)
}

// openMessageDetail pushes the current view and shows msg in the detail view.
func (a *App) openMessageDetail(msg *kafka.ConsumedMessage) tea.Cmd {
	a.pushView(a.view)
	a.setView(style.ViewMessageDetail, views.NewMessageDetailView(msg))
	a.view = style.ViewMessageDetail
	a.resizeActiveView()
	return a.viewMap[style.ViewMessageDetail].Init()
}

// parseIndex reads a row index from an action param.
func parseIndex(param string) (int, bool) {
	idx := 0
	n, err := fmt.Sscanf(param, "%d", &idx)
	return idx, n == 1 && err == nil
}

// aclConfirmLabel returns a short human-readable label for the ACL identified
// by key, used in the confirmation prompt.
func aclConfirmLabel(v *views.ACLsView, key string) string {
	if v == nil {
		return ""
	}
	e := v.SelectedACL(key)
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s %s on %s:%s for %s", e.Permission, e.Operation, e.ResourceType, e.ResourceName, e.Principal)
}

// ── Async admin commands ────────────────────────────────────────────────────

// deleteTopicMsg carries the result of an async topic deletion.
type deleteTopicMsg struct {
	err   error
	topic string
}

// deleteACLMsg carries the result of an async ACL deletion.
type deleteACLMsg struct {
	err error
}

// deleteGroupMsg carries the result of an async consumer group deletion.
type deleteGroupMsg struct {
	err error
}

// deleteOffsetMsg carries the result of an async offset deletion.
type deleteOffsetMsg struct {
	err error
}

// purgeTopicMsg carries the result of a DeleteRecords purge.
type purgeTopicMsg struct {
	err   error
	topic string
}

// setRetentionMsg carries the result of a permanent retention change.
type setRetentionMsg struct {
	err         error
	topic       string
	retentionMs int64
}

// editTopicConfigMsg carries the result of an async topic config edit.
type editTopicConfigMsg struct {
	err error
}

// increasePartitionsMsg carries the result of an async partition increase.
type increasePartitionsMsg struct {
	err error
}

// changeReplicationMsg carries the result of an async replication factor change.
type changeReplicationMsg struct {
	err error
}

// resetRetentionMsg carries the result of removing the retention.ms override.
type resetRetentionMsg struct {
	err   error
	topic string
}

// doDeleteTopic deletes a topic asynchronously.
func (a *App) doDeleteTopic(topic string) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		ctx := context.Background()
		_, err := client.DeleteTopic(ctx, topic)
		return deleteTopicMsg{topic: topic, err: err}
	}
}

// doDeleteGroup deletes a consumer group asynchronously.
func (a *App) doDeleteGroup(groupID string) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		return deleteGroupMsg{err: client.DeleteGroup(context.Background(), groupID)}
	}
}

// doDeleteGroupOffset deletes committed offsets for a group+topic+partition asynchronously.
func (a *App) doDeleteGroupOffset(groupID, topic string, partition int) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		return deleteOffsetMsg{err: client.DeleteGroupOffsets(context.Background(), groupID, topic, partition)}
	}
}

// doAlterTopicConfig sets a topic config key asynchronously.
func (a *App) doAlterTopicConfig(topic, key, value string) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		return editTopicConfigMsg{err: client.AlterTopicConfig(context.Background(), topic, key, value)}
	}
}

// doIncreasePartitions increases the partition count for a topic.
func (a *App) doIncreasePartitions(topic string, newCount int) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		return increasePartitionsMsg{err: client.IncreasePartitions(context.Background(), topic, newCount)}
	}
}

// doChangeReplication changes the replication factor for a topic.
func (a *App) doChangeReplication(topic string, newRF int) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		return changeReplicationMsg{err: client.ChangeReplicationFactor(context.Background(), topic, newRF)}
	}
}

// doResetRetention removes the retention.ms override, reverting to the broker default.
func (a *App) doResetRetention(topic string) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		err := client.DeleteTopicConfigOverride(context.Background(), topic, "retention.ms")
		return resetRetentionMsg{topic: topic, err: err}
	}
}

// doSetRetention permanently sets retention.ms on a topic.
func (a *App) doSetRetention(topic string, retentionMs int64) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		err := client.AlterTopicConfig(
			context.Background(), topic, "retention.ms", fmt.Sprintf("%d", retentionMs),
		)
		return setRetentionMsg{topic: topic, retentionMs: retentionMs, err: err}
	}
}

// doPurgeTopic uses the DeleteRecords API to immediately remove messages.
func (a *App) doPurgeTopic(topic string, retentionMs int64) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		err := client.PurgeTopicMessages(context.Background(), topic, retentionMs)
		return purgeTopicMsg{topic: topic, err: err}
	}
}

// doDeleteACL deletes the ACL whose aclKey matches the given key.
func (a *App) doDeleteACL(key string) tea.Cmd {
	v := typedView[*views.ACLsView](a, style.ViewACLs)
	if v == nil {
		return func() tea.Msg { return deleteACLMsg{err: fmt.Errorf("ACL not found")} }
	}
	entry := v.SelectedACL(key)
	if entry == nil {
		return func() tea.Msg { return deleteACLMsg{err: fmt.Errorf("ACL not found")} }
	}
	client := a.client
	e := *entry
	return func() tea.Msg {
		return deleteACLMsg{err: client.DeleteACL(context.Background(), e)}
	}
}

// ── Context switching ───────────────────────────────────────────────────────

// switchContextMsg carries the result of an async context switch.
type switchContextMsg struct {
	client *kafka.Client
	info   ConnInfo
	err    error
	name   string
}

// doSwitchContext resolves brokers and builds a new client in a goroutine.
func (a *App) doSwitchContext(name string) tea.Cmd {
	cfg := a.cfg
	return func() tea.Msg {
		ctx, ok := cfg.Contexts[name]
		if !ok {
			return switchContextMsg{err: fmt.Errorf("context %q not found", name)}
		}

		brokers, err := ctx.ResolveBrokers()
		if err != nil {
			return switchContextMsg{err: fmt.Errorf("resolving brokers for %q: %w", name, err)}
		}

		authCfg := ctx.ToAuthConfig()
		client, err := kafka.NewClient(authCfg, brokers)
		if err != nil {
			return switchContextMsg{err: fmt.Errorf("client for %q: %w", name, err)}
		}
		return switchContextMsg{
			name:   name,
			client: client,
			info: ConnInfo{
				Context: name,
				Auth:    string(authCfg.Method),
				Brokers: brokers,
				Region:  authCfg.Region,
			},
		}
	}
}

// startPurgeTopic opens the prompt bar pre-filled with a default
// retention duration so the user can specify how much history to keep.
func (a *App) startPurgeTopic(topic string) (tea.Model, tea.Cmd) {
	cmd := a.openValuePrompt(
		"purge_topic", topic,
		"1h",
		"retention:",
		"e.g. 1h, 3h, 1d, 0 for all — add ! for permanent (1d!)",
		true,
	)
	a.resizeActiveView()
	return a, cmd
}

// startIncreasePartitions opens the prompt bar pre-filled with the
// current partition count so the user can type a new (higher) count.
func (a *App) startIncreasePartitions(topic string) (tea.Model, tea.Cmd) {
	v := typedView[*views.TopicDetailView](a, style.ViewTopicDetail)
	if v == nil {
		return a, nil
	}
	current := v.PartitionCount()
	if current == 0 {
		return a, nil
	}
	cmd := a.openValuePrompt(actionIncreasePartitions, topic, fmt.Sprintf("%d", current), "partitions:", "", true)
	a.resizeActiveView()
	return a, cmd
}

// startChangeReplication opens the prompt bar pre-filled with the
// current replication factor so the user can type a new value.
func (a *App) startChangeReplication(topic string) (tea.Model, tea.Cmd) {
	v := typedView[*views.TopicDetailView](a, style.ViewTopicDetail)
	if v == nil {
		return a, nil
	}
	current := v.ReplicationFactor()
	if current == 0 {
		return a, nil
	}
	cmd := a.openValuePrompt(actionChangeReplication, topic, fmt.Sprintf("%d", current), "replication:", "", true)
	a.resizeActiveView()
	return a, cmd
}

// reconnect rebuilds the Kafka client for the current context, re-resolving
// brokers and reloading AWS credentials. Use after refreshing tokens externally.
//
//nolint:unparam // (Model, Cmd) signature matches the other handler shape callers expect
func (a *App) reconnect() (tea.Model, tea.Cmd) {
	name := a.connInfo.Context
	if name == "" || a.cfg == nil {
		a.errFlash = "no context configured — use --brokers or set up a config file"
		return a, nil
	}
	a.loading = true
	a.flash = "reconnecting..."
	cmd := a.doSwitchContext(name)
	return a, cmd
}

// applySwitchContext swaps in the new client and resets all views.
func (a *App) applySwitchContext(msg switchContextMsg) (tea.Model, tea.Cmd) {
	if msg.err != nil {
		a.loading = false
		a.errFlash = msg.err.Error()
		// Stay on context view so the user can try again.
		return a, nil
	}

	a.client = msg.client
	a.connInfo = msg.info
	a.awaitingContext = false

	// Persist the context switch so the next launch starts here.
	if a.cfg != nil && a.cfgPath != "" {
		a.cfg.CurrentContext = msg.name
		config.Save(a.cfgPath, a.cfg) //nolint:errcheck // best-effort persistence
	}
	// Tier 1 session: remember the last context as a fast-path hint
	// independent of config.yaml (config may be read-only on some setups).
	if a.session != nil {
		_ = a.session.SetLastContext(msg.name) //nolint:errcheck // best-effort session persistence
	}

	// Rebuild all views with new client.
	a.viewMap = map[style.ViewType]views.View{
		style.ViewTopics:  views.NewTopicsView(a.client),
		style.ViewGroups:  views.NewGroupsView(a.client),
		style.ViewCluster: views.NewClusterView(a.client),
	}

	// Show loading spinner in topics view (not splash screen).
	a.loading = true
	cmd := a.switchView(style.ViewTopics)
	return a, cmd
}

// startIndexer ensures an AsyncIndexer is open for the given topic
// with a dedicated background tail consumer that feeds it. No-op when
// an indexer is already running for the topic — indexers are app-scoped
// and persist across view navigation per docs/design/local-index.md.
// Other no-op cases: indexing disabled in config, cluster ID can't be
// resolved (we won't risk cross-cluster collisions), or another process
// holds the writer lock. Errors are logged-and-swallowed; indexing is
// an optimization, never a hard dependency.
//
// The dedicated consumer is the indexer's source of truth — MessagesView
// no longer feeds the indexer. This keeps the index advancing for every
// topic that's been visited in the session, even ones the user has
// since navigated away from.
func (a *App) startIndexer(topic string) {
	if a.cfg == nil || !a.cfg.Index.Enabled {
		return
	}
	if topic == "" || a.client == nil || a.stateRoot == nil {
		return
	}
	if a.indexers == nil {
		a.indexers = make(map[string]*indexerEntry)
	}
	if _, ok := a.indexers[topic]; ok {
		return
	}
	cluster, err := a.client.ClusterID(context.Background())
	if err != nil || cluster == "" {
		// Without a cluster ID we'd risk shared-state collisions across
		// different clusters using the same context-name; skip the
		// indexer until we can identify the cluster.
		return
	}
	ix, err := index.Open(a.stateRoot, cluster, topic)
	if err != nil {
		// ErrLocked is the common case (another k4a is the writer for
		// this topic). Either way, fall through to scan-only.
		return
	}
	async := index.NewAsync(ix)
	//nolint:gosec // tailCancel is stored on indexerEntry and called from stopAllIndexers
	tailCtx, tailCancel := context.WithCancel(context.Background())
	a.indexers[topic] = &indexerEntry{async: async, cancel: tailCancel}

	// Dedicated tail: drains records from a background consumer into
	// async.Submit for the entire indexer lifetime. The consumer is
	// independent of any view, so closing MessagesView no longer
	// pauses indexing.
	go runIndexerTail(tailCtx, a.client, topic, async)

	// Phase C-2: if the indexer has prior state, fire a bounded
	// catch-up backfill in the background. Live tail starts immediately
	// — catch-up runs in parallel filling the offset gap.
	go func(c *kafka.Client, ax *index.AsyncIndexer, topic string) {
		_, _ = index.Catchup(context.Background(), c, ax, topic, index.CatchupOptions{}) //nolint:errcheck // best-effort
	}(a.client, async, topic)
}

// runIndexerTail drains records from a background consumer into the
// AsyncIndexer until ctx is canceled. Errors are dropped — indexing is
// an optimization, and the live UI is the surface where consumer
// failures get reported. Records have docID = (partition, offset) in
// Bleve, so any overlap with Catchup is idempotent.
func runIndexerTail(ctx context.Context, client *kafka.Client, topic string, async *index.AsyncIndexer) {
	msgCh, errCh, _ := client.Consume(ctx, topic)
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-msgCh:
			if !ok {
				return
			}
			async.Submit(m)
		case _, ok := <-errCh:
			if !ok {
				return
			}
			// Swallow — see comment above.
		}
	}
}

// stopAllIndexers cancels every dedicated tail, drains pending batches,
// and closes each AsyncIndexer. Called once at app shutdown (ctrl-c,
// :quit, upgrade) so writer locks are released and the bookkeeper
// flushes before the process exits.
func (a *App) stopAllIndexers() {
	if len(a.indexers) == 0 {
		return
	}
	for _, entry := range a.indexers {
		entry.cancel()         // stop the tail consumer
		_ = entry.async.Stop() //nolint:errcheck // best-effort indexer teardown
	}
	a.indexers = nil
}

// openSearchBar opens the Prompt widget in search mode against `topic`,
// pre-filling whatever the user is currently filtering by (if anything).
// This is the entry point reached from MessagesView's `s` key and from
// the `:search` command. Pressing Enter on the bar then dispatches the
// "search" action with a topic+query param, which runs the actual scan.
func (a *App) openSearchBar(topic string) (tea.Model, tea.Cmd) {
	if topic == "" {
		// No topic context — search is only meaningful from the
		// messages view; surface a quick flash and stay put.
		a.errFlash = "search is only available from the messages view"
		return a, nil
	}
	// Carry the in-memory filter (if any) into the search bar so the
	// user can refine from a filter that found nothing in the buffer.
	preset := ""
	if v := typedView[*views.MessagesView](a, style.ViewMessages); v != nil {
		preset = v.Filter()
	}
	a.promptDispatch = func(input string) (string, tea.Cmd) {
		_, c := a.executeSearch(input)
		return "", c
	}
	cmd := a.prompt.Open(preset, chrome.OpenOpts{Prompt: "search>"})
	a.resizeActiveView()
	return a, cmd
}

// searchRunner returns the SearchFunc that SearchView should use. It
// composes the three search sources in priority order:
//
//  1. searchcache.Cache — instant on repeat queries (HWM-validated).
//  2. index.AsyncIndexer — instant when the index covers the time range.
//  3. kafka.Client.DeepSearch — the correctness floor; broker scan.
//
// Every entry point into search (TUI Ctrl+F, MCP server, `k4a search`
// CLI) routes through this so they all pick up the same fast paths.
func (a *App) searchRunner() views.SearchFunc {
	base := a.localSearchRunner()
	if a.remoteIndex == nil {
		return base
	}
	// Shared index is the outermost source: a covered query is answered by the
	// warm daemon, everything else falls through to the local cache/index/scan.
	wrapped := a.remoteIndex.Wrap(remoteindex.SearchFunc(base))
	return views.SearchFunc(wrapped)
}

// localSearchRunner is the in-process source chain: cache → local index → scan.
func (a *App) localSearchRunner() views.SearchFunc {
	if a.searchCache == nil {
		return a.client.DeepSearch
	}
	cache := a.searchCache
	client := a.client
	return func(ctx context.Context, topic string, params kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		if ix := a.indexerFor(topic); ix != nil {
			if canServe, _ := ix.CanServe(indexParams(params)); canServe {
				return streamFromIndex(ctx, ix, params)
			}
		}
		return cache.Search(ctx, client, topic, params)
	}
}

// indexerFor returns the live AsyncIndexer's underlying Indexer for the
// topic, or nil if no indexer is open for it. The dispatcher consults
// this on every search so it auto-discovers indexing as topics open
// and close.
func (a *App) indexerFor(topic string) *index.Indexer {
	entry, ok := a.indexers[topic]
	if !ok {
		return nil
	}
	return entry.async.Inner()
}

// indexParams translates kafka.SearchParams to the index layer's
// QueryParams. Scope bitmask becomes a string slice; partition list
// passes through verbatim.
func indexParams(p kafka.SearchParams) index.QueryParams {
	out := index.QueryParams{
		Pattern:    p.Pattern,
		Since:      p.Since,
		Until:      p.Until,
		Partitions: p.Partitions,
		Limit:      p.Cap,
	}
	scope := p.Scope
	if scope == 0 {
		scope = kafka.ScopeKey | kafka.ScopeValue
	}
	if scope&kafka.ScopeKey != 0 {
		out.Scopes = append(out.Scopes, "key")
	}
	if scope&kafka.ScopeValue != 0 {
		out.Scopes = append(out.Scopes, "value")
	}
	if scope&kafka.ScopeHeaders != 0 {
		out.Scopes = append(out.Scopes, "headers")
	}
	return out
}

// streamFromIndex runs the query against the index and pipes the
// resulting matches through the same three-channel shape that DeepSearch
// uses, so SearchView consumes them identically. The terminal progress
// message carries Source=SourceIndex so the header reads "indexed · ..."
// instead of "scanned · ...".
func streamFromIndex(
	ctx context.Context,
	ix *index.Indexer,
	params kafka.SearchParams,
) (<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error) {
	matchCh := make(chan kafka.ConsumedMessage, 256)
	progCh := make(chan kafka.DeepSearchProgress, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(matchCh)
		defer close(progCh)
		defer close(errCh)

		start := time.Now()
		res, err := ix.Query(ctx, indexParams(params))
		if err != nil {
			select {
			case errCh <- err:
			default:
			}
			return
		}
		for _, m := range res.Matches {
			select {
			case matchCh <- m:
			case <-ctx.Done():
				return
			}
		}
		select {
		case progCh <- kafka.DeepSearchProgress{
			Elapsed:    time.Since(start),
			Matches:    len(res.Matches),
			Partitions: len(params.Partitions),
			Source:     kafka.SourceIndex,
			Capped:     res.Capped,
			Done:       true,
		}:
		default:
		}
	}()
	return matchCh, progCh, errCh
}

// splitSearchParam splits a "search" action param into (topic, initialPattern).
// The two halves are separated by a NUL byte so either may contain spaces or
// regex metacharacters without ambiguity. A param with no NUL is treated as
// topic-only (no initial pattern).
func splitSearchParam(param string) (topic, pattern string) {
	if before, after, ok := strings.Cut(param, "\x00"); ok {
		return before, after
	}
	return param, ""
}
