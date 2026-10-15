// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/kafka"
)

// DescribeBrokerConfigCmdFlags defines flags for the describe-broker-config command.
type DescribeBrokerConfigCmdFlags struct {
	ConfigKey string `long:"config-key" description:"Only report this config key (shown even when a default)"`
	FormatFlag
	ConnectionFlags
	All bool `long:"all"        description:"Include keys left at their defaults"`
}

// DescribeBrokerConfigCmd implements the "describe-broker-config" CLI command.
type DescribeBrokerConfigCmd struct {
	Out io.Writer // stdout when nil
}

type brokerConfigRow struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Source    string `json:"source"`
	Broker    int32  `json:"broker"`
	Sensitive bool   `json:"sensitive"`
}

// Help returns the detailed help text.
func (c *DescribeBrokerConfigCmd) Help() string {
	return `Usage: k4a describe-broker-config [options] [BROKER-ID...]

  Report broker configuration (DescribeConfigs): the TUI's Broker Detail
  view, headless. With no BROKER-ID, every broker is described.

  By default keys left at their DEFAULT source are hidden, the same split
  as describe-topic's overrides; --all shows every key, and --config-key
  reports one key even when it is a default. Sensitive values are never
  printed.

Options:
      --config-key=NAME    Only report this config key
      --all                Include keys left at their defaults
      --format=FORMAT      Output format: text (default) or json

` + clusterFlagsHelp + `

Examples:
  k4a describe-broker-config --context staging 1
  k4a describe-broker-config --config-key log.retention.hours`
}

// Synopsis returns the one-line description.
func (c *DescribeBrokerConfigCmd) Synopsis() string {
	return "Show broker configuration (non-defaults by default)"
}

// Run executes the describe-broker-config command.
func (c *DescribeBrokerConfigCmd) Run(args []string) int {
	var flags DescribeBrokerConfigCmdFlags
	brokerArgs, code, done := parseCmdArgs(&flags, args)
	if done {
		return code
	}
	ids, err := parseBrokerIDs(brokerArgs)
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		rows, err := fetchBrokerConfigRows(ctx, client, ids, flags.ConfigKey, flags.All)
		out := orStdout(c.Out)
		if flags.JSON() {
			return errors.Join(err, writeJSON(out, rows))
		}
		return errors.Join(err, writeBrokerConfigRows(out, rows))
	})
}

// fetchBrokerConfigRows describes ids (every broker when empty), sorted by
// broker then key. A broker that fails is reported in the error while the
// others' rows are still returned.
func fetchBrokerConfigRows(
	ctx context.Context, client *kafka.Client, ids []int32, configKey string, all bool,
) ([]brokerConfigRow, error) {
	if len(ids) == 0 {
		cluster, err := client.FetchCluster(ctx)
		if err != nil {
			return nil, err
		}
		for _, b := range cluster.Brokers {
			ids = append(ids, int32(b.ID)) //nolint:gosec // broker IDs are int32 on the wire
		}
	}
	rows := []brokerConfigRow{}
	var errs []error
	for _, id := range ids {
		entries, err := client.FetchBrokerConfigs(ctx, strconv.Itoa(int(id)))
		if err != nil {
			errs = append(errs, fmt.Errorf("broker %d: %w", id, err))
			continue
		}
		rows = append(rows, filterBrokerConfigs(id, entries, configKey, all)...)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Broker != rows[j].Broker {
			return rows[i].Broker < rows[j].Broker
		}
		return rows[i].Name < rows[j].Name
	})
	return rows, errors.Join(errs...)
}

func parseBrokerIDs(args []string) ([]int32, error) {
	ids := make([]int32, 0, len(args))
	for _, a := range args {
		id, err := strconv.ParseInt(a, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid broker ID %q", a)
		}
		ids = append(ids, int32(id))
	}
	return ids, nil
}

func filterBrokerConfigs(broker int32, entries []kafka.ConfigEntry, configKey string, all bool) []brokerConfigRow {
	rows := make([]brokerConfigRow, 0, len(entries))
	for _, e := range entries {
		switch {
		case configKey != "":
			if e.Name != configKey {
				continue
			}
		case !all && e.IsDefault:
			continue
		}
		value := e.Value
		if e.Sensitive {
			value = ""
		}
		rows = append(rows, brokerConfigRow{
			Broker: broker, Name: e.Name, Value: value, Source: e.Source, Sensitive: e.Sensitive,
		})
	}
	return rows
}

func writeBrokerConfigRows(w io.Writer, rows []brokerConfigRow) error {
	p := newPrinter(w)
	if len(rows) == 0 {
		p.printf("No non-default broker configs. Use --all to include defaults.\n")
		return p.done()
	}
	p.printf("BROKER\tCONFIG\tVALUE\tSOURCE\n")
	for _, r := range rows {
		value := r.Value
		if r.Sensitive {
			value = "(sensitive)"
		}
		p.printf("%d\t%s\t%s\t%s\n", r.Broker, r.Name, value, r.Source)
	}
	return p.done()
}
