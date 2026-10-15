// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/kafka"
)

// ListGroupsCmdFlags defines flags for the list-groups command.
type ListGroupsCmdFlags struct {
	FormatFlag
	ConnectionFlags
	Lag bool `long:"lag" description:"Add total lag, topic count and coordinator (extra offset fetches)"`
}

// ListGroupsCmd implements the "list-groups" CLI command.
type ListGroupsCmd struct {
	Out io.Writer // stdout when nil
}

type groupRow struct {
	Lag         *int64 `json:"lag,omitempty"`
	Topics      *int   `json:"topics,omitempty"`
	Coordinator *int   `json:"coordinator,omitempty"`
	GroupID     string `json:"groupId"`
	State       string `json:"state"`
	Members     int    `json:"members"`
}

// Help returns the detailed help text.
func (c *ListGroupsCmd) Help() string {
	return `Usage: k4a list-groups [options]

  List consumer groups with their state and member count: the TUI's Groups
  view, headless. --lag adds total lag, topic count and coordinator, which
  costs an offset fetch per group.

Options:
      --lag                Add lag, topics and coordinator
      --format=FORMAT      Output format: text (default) or json

` + clusterFlagsHelp + `

Examples:
  k4a list-groups --context staging
  k4a list-groups --lag --format json | jq '.[] | select(.lag > 1000)'`
}

// Synopsis returns the one-line description.
func (c *ListGroupsCmd) Synopsis() string {
	return "List consumer groups"
}

// Run executes the list-groups command.
func (c *ListGroupsCmd) Run(args []string) int {
	var flags ListGroupsCmdFlags
	if _, code, done := parseCmdArgs(&flags, args); done {
		return code
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		rows, err := fetchGroupRows(ctx, client, flags.Lag)
		if err != nil {
			return err
		}
		out := orStdout(c.Out)
		if flags.JSON() {
			return writeJSON(out, rows)
		}
		return writeGroupRows(out, rows, flags.Lag)
	})
}

func fetchGroupRows(ctx context.Context, client *kafka.Client, lag bool) ([]groupRow, error) {
	groups, err := client.FetchGroups(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })

	var enrich map[string]kafka.GroupEnrichment
	if lag {
		ids := make([]string, len(groups))
		for i, g := range groups {
			ids[i] = g.GroupID
		}
		enrich = client.FetchGroupsEnrichment(ctx, ids)
	}

	rows := make([]groupRow, 0, len(groups))
	for _, g := range groups {
		r := groupRow{GroupID: g.GroupID, State: g.State, Members: g.Members}
		if e, ok := enrich[g.GroupID]; ok {
			r.Lag, r.Topics, r.Coordinator = &e.TotalLag, &e.Topics, &e.Coordinator
		}
		rows = append(rows, r)
	}
	return rows, nil
}

func writeGroupRows(w io.Writer, rows []groupRow, lag bool) error {
	p := newPrinter(w)
	if len(rows) == 0 {
		p.printf("No consumer groups found.\n")
		return p.done()
	}
	header := "GROUP\tSTATE\tMEMBERS"
	if lag {
		header += "\tTOPICS\tLAG\tCOORDINATOR"
	}
	p.printf("%s\n", header)
	for _, r := range rows {
		p.printf("%s\t%s\t%d", r.GroupID, r.State, r.Members)
		if r.Lag != nil {
			p.printf("\t%d\t%d\t%d", *r.Topics, *r.Lag, *r.Coordinator)
		} else if lag {
			p.printf("\t-\t-\t-")
		}
		p.printf("\n")
	}
	return p.done()
}

// DescribeGroupCmdFlags defines flags for the describe-group command.
type DescribeGroupCmdFlags struct {
	FormatFlag
	ConnectionFlags
}

// DescribeGroupCmd implements the "describe-group" CLI command.
type DescribeGroupCmd struct {
	Out io.Writer // stdout when nil
}

// Help returns the detailed help text.
func (c *DescribeGroupCmd) Help() string {
	return `Usage: k4a describe-group [options] GROUP...

  Show a consumer group's state, members and per-partition committed offset,
  high watermark and lag: the TUI's Group Detail view, headless.

Options:
      --format=FORMAT      Output format: text (default) or json (an array,
                           one object per GROUP)

` + clusterFlagsHelp + `

Examples:
  k4a describe-group --context staging orders-consumer
  k4a describe-group --format json orders-consumer | jq '.[0].totalLag'`
}

// Synopsis returns the one-line description.
func (c *DescribeGroupCmd) Synopsis() string {
	return "Show a consumer group's members, offsets and lag"
}

// Run executes the describe-group command.
func (c *DescribeGroupCmd) Run(args []string) int {
	var flags DescribeGroupCmdFlags
	groups, code, done := parseCmdArgs(&flags, args)
	if done {
		return code
	}
	if len(groups) == 0 {
		log.Error("at least one GROUP argument is required")
		return 1
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		details := make([]*kafka.GroupDetail, 0, len(groups))
		var errs []error
		for _, g := range groups {
			d, err := client.FetchGroupDetail(ctx, g)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			sortGroupDetail(d)
			details = append(details, d)
		}
		out := orStdout(c.Out)
		var err error
		if flags.JSON() {
			err = writeJSON(out, groupDetailsJSON(details))
		} else {
			err = writeGroupDetails(out, details)
		}
		return errors.Join(append(errs, err)...)
	})
}

func sortGroupDetail(d *kafka.GroupDetail) {
	sort.Slice(d.Offsets, func(i, j int) bool {
		if d.Offsets[i].Topic != d.Offsets[j].Topic {
			return d.Offsets[i].Topic < d.Offsets[j].Topic
		}
		return d.Offsets[i].Partition < d.Offsets[j].Partition
	})
	sort.Slice(d.Members, func(i, j int) bool { return d.Members[i].MemberID < d.Members[j].MemberID })
}

type groupMemberJSON struct {
	MemberID string   `json:"memberId"`
	ClientID string   `json:"clientId"`
	Host     string   `json:"host"`
	Topics   []string `json:"topics"`
}

type groupOffsetJSON struct {
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	Committed     int64  `json:"committed"`
	HighWatermark int64  `json:"highWatermark"`
	Lag           int64  `json:"lag"`
}

type groupDetailJSON struct {
	GroupID  string            `json:"groupId"`
	State    string            `json:"state"`
	Members  []groupMemberJSON `json:"members"`
	Offsets  []groupOffsetJSON `json:"offsets"`
	TotalLag int64             `json:"totalLag"`
}

// groupDetailsJSON gives the detail stable lower-camel keys and empty arrays
// rather than nulls, so a script can index into them unconditionally.
func groupDetailsJSON(details []*kafka.GroupDetail) []groupDetailJSON {
	out := make([]groupDetailJSON, 0, len(details))
	for _, d := range details {
		j := groupDetailJSON{
			GroupID:  d.GroupID,
			State:    d.State,
			TotalLag: d.TotalLag,
			Members:  make([]groupMemberJSON, 0, len(d.Members)),
			Offsets:  make([]groupOffsetJSON, 0, len(d.Offsets)),
		}
		for _, m := range d.Members {
			topics := m.Topics
			if topics == nil {
				topics = []string{}
			}
			j.Members = append(
				j.Members,
				groupMemberJSON{MemberID: m.MemberID, ClientID: m.ClientID, Host: m.Host, Topics: topics},
			)
		}
		for _, o := range d.Offsets {
			j.Offsets = append(j.Offsets, groupOffsetJSON{
				Topic: o.Topic, Partition: o.Partition, Committed: o.CommittedOffset,
				HighWatermark: o.HighWatermark, Lag: o.Lag,
			})
		}
		out = append(out, j)
	}
	return out
}

func writeGroupDetails(w io.Writer, details []*kafka.GroupDetail) error {
	p := newPrinter(w)
	for i, d := range details {
		if i > 0 {
			p.printf("\n")
		}
		p.printf("Group: %s  State: %s  Members: %d  Total lag: %d\n",
			d.GroupID, d.State, len(d.Members), d.TotalLag)
		if len(d.Offsets) == 0 {
			p.printf("No committed offsets.\n")
		} else {
			p.printf("\nTOPIC\tPARTITION\tCOMMITTED\tHIGH-WATERMARK\tLAG\n")
			for _, o := range d.Offsets {
				p.printf("%s\t%d\t%d\t%d\t%d\n", o.Topic, o.Partition, o.CommittedOffset, o.HighWatermark, o.Lag)
			}
		}
		if len(d.Members) > 0 {
			p.printf("\nMEMBER\tCLIENT-ID\tHOST\tTOPICS\n")
			for _, m := range d.Members {
				p.printf("%s\t%s\t%s\t%s\n", m.MemberID, m.ClientID, m.Host, strings.Join(m.Topics, ","))
			}
		}
	}
	return p.done()
}
