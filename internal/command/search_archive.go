// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/blairham/k4a/internal/archive"
	"github.com/blairham/k4a/internal/awscreds"
	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/searchcli"
)

// archiveDeps is what an archive search reaches outside the process
// through, so tests can substitute every piece of it.
type archiveDeps struct {
	newClients func(ctx context.Context, profile, region string) (archive.AthenaAPI, archive.S3API, error)
	sleep      func(context.Context, time.Duration) error
	stdout     io.Writer
	stderr     io.Writer
	prompt     Prompt
	now        func() time.Time
}

func defaultArchiveDeps() archiveDeps {
	return archiveDeps{
		newClients: archive.NewClients,
		stdout:     os.Stdout,
		stderr:     os.Stderr,
		prompt:     Prompt{Out: os.Stderr},
		now:        time.Now,
	}
}

// archiveTarget is the resolved archive settings for one search.
type archiveTarget struct {
	cfg     archive.Config
	profile string
	region  string
	topic   archive.Topic
	limits  archive.Limits
}

// resolveArchive reads the context's archive block for the topic, applying
// --max-bytes and the profile/region precedence in archive-search.md.
func resolveArchive(flags *SearchFlags) (archiveTarget, error) {
	if flags.Brokers != "" {
		return archiveTarget{}, errors.New("--archive reads a context's archive block; use --context instead of --brokers")
	}
	cfgPath := flags.ConfigFile
	if cfgPath == "" {
		cfgPath = config.DefaultConfigPath()
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return archiveTarget{}, err
	}
	ctx, err := cfg.Resolve(flags.Context)
	if err != nil {
		return archiveTarget{}, err
	}
	a := ctx.Archive
	a.Profile = config.ExpandRef(a.Profile)
	a.Region = config.ExpandRef(a.Region)
	a.Workgroup = config.ExpandRef(a.Workgroup)
	a.OutputLocation = config.ExpandRef(a.OutputLocation)

	if strings.ContainsAny(flags.Topic, "*?[") {
		return archiveTarget{}, fmt.Errorf("--archive searches one topic's table; %q is a glob", flags.Topic)
	}
	t := archiveTarget{cfg: a}
	if t.topic, err = a.Topic(flags.Topic); err != nil {
		return archiveTarget{}, err
	}
	if t.limits, err = a.Limits(); err != nil {
		return archiveTarget{}, err
	}
	if flags.MaxBytes != "" {
		if t.limits.MaxBytes, err = archive.ParseSize(flags.MaxBytes); err != nil {
			return archiveTarget{}, fmt.Errorf("--max-bytes: %w", err)
		}
	}
	// The archive's own profile is the more specific statement; -p is the
	// cluster's, and an archive in a data-lake account is where they differ.
	t.profile = firstNonEmpty(a.Profile, flags.Profile, ctx.Profile)
	t.region = firstNonEmpty(a.Region, flags.Region, ctx.Region)
	return t, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// runArchiveSearch is `k4a search --archive`: one Athena query over the
// topic's archive, gated by the cost controls. It never connects to Kafka.
func runArchiveSearch(
	ctx context.Context,
	flags *SearchFlags,
	pattern string,
	params kafka.SearchParams,
	deps archiveDeps,
) int {
	fail := func(err error) int {
		fmt.Fprintln(deps.stderr, "archive search:", err) //nolint:errcheck // stderr
		return 1
	}
	if params.Path != nil {
		return fail(errors.New("--path cannot be used with --archive (it has no pushdown, so it would scan every row)"))
	}
	target, err := resolveArchive(flags)
	if err != nil {
		return fail(err)
	}
	if params.Until.IsZero() {
		params.Until = deps.now()
	}
	plan, err := archive.BuildPlan(target.cfg.CatalogOrDefault(), target.topic, archive.Request{
		Topic:      flags.Topic,
		Pattern:    params.Pattern,
		Scope:      params.Scope,
		Since:      params.Since,
		Until:      params.Until,
		Partitions: params.Partitions,
		Cap:        params.Cap,
	})
	if err != nil {
		return fail(err)
	}
	if flags.Explain {
		explainArchive(deps.stdout, plan, target)
		return 0
	}

	athenaAPI, s3API, err := deps.newClients(ctx, target.profile, target.region)
	if err != nil {
		return fail(err)
	}
	estimate, err := estimateArchive(ctx, s3API, plan, target.profile, deps.stderr)
	if err != nil {
		return fail(err)
	}
	if err := gateArchive(estimate, target.limits, flags.Yes, deps.prompt, flags.Topic); err != nil {
		return fail(err)
	}

	runner := archive.Runner{
		Athena:         athenaAPI,
		Workgroup:      target.cfg.WorkgroupOrDefault(),
		OutputLocation: target.cfg.OutputLocation,
		MaxBytes:       target.limits.MaxBytes,
		Sleep:          deps.sleep,
	}
	res, runErr := runner.Run(ctx, plan)
	if runErr != nil && kafka.IsAuthError(runErr) {
		runErr = awscreds.LoginHint(runErr, target.profile)
	}
	cov := archive.NewCoverage(plan, estimate, res)
	return emitArchive(flags, pattern, params, res, &cov, runErr, deps)
}

// estimateArchive sums the pruned partitions' S3 objects. nil means no
// estimate: no location is configured, or listing failed for a reason other
// than credentials (which the query would hit too, so it is fatal here).
func estimateArchive(ctx context.Context, api archive.S3API, plan archive.Plan, profile string, stderr io.Writer) (
	*int64, error,
) {
	if len(plan.Prefixes) == 0 {
		return nil, nil //nolint:nilnil // nil estimate is "unknown", not an error
	}
	n, err := archive.Estimate(ctx, api, plan.Prefixes)
	if err != nil {
		if kafka.IsAuthError(err) {
			return nil, awscreds.LoginHint(err, profile)
		}
		fmt.Fprintln(stderr, "warning: no size estimate:", err) //nolint:errcheck // stderr
		return nil, nil                                         //nolint:nilnil // as above
	}
	return &n, nil
}

// gateArchive applies the cost controls before anything is billed: refuse
// over max-bytes (even with --yes), ask over confirm-bytes or without an
// estimate, and run otherwise.
func gateArchive(estimate *int64, l archive.Limits, yes bool, p Prompt, topic string) error {
	if estimate != nil && l.MaxBytes > 0 && *estimate > l.MaxBytes {
		return fmt.Errorf("estimated scan %s is over max-bytes %s; narrow --since/--until or raise --max-bytes",
			archive.FormatSize(*estimate), archive.FormatSize(l.MaxBytes))
	}
	if yes || (estimate != nil && *estimate <= l.ConfirmBytes) {
		return nil
	}
	var q string
	if estimate != nil {
		q = fmt.Sprintf("Archive search of %s will scan up to %s (about $%.4f at Athena list price). Run it?",
			topic, archive.FormatSize(*estimate), archive.ListPriceUSD(*estimate))
	} else {
		q = fmt.Sprintf("Archive search of %s has no size estimate (set location to get one); "+
			"it is stopped at %s. Run it?", topic, archive.FormatSize(l.MaxBytes))
	}
	if err := p.ask(q); err != nil {
		return fmt.Errorf("not run: %w", err)
	}
	return nil
}

func explainArchive(w io.Writer, plan archive.Plan, t archiveTarget) {
	fmt.Fprintf(w, "-- workgroup %s, profile %q, region %q\n", //nolint:errcheck // stdout
		t.cfg.WorkgroupOrDefault(), t.profile, t.region)
	fmt.Fprintf(w, "-- %d partitions, confirm over %s, refuse over %s\n", //nolint:errcheck // stdout
		plan.PartitionCount, archive.FormatSize(t.limits.ConfirmBytes), archive.FormatSize(t.limits.MaxBytes))
	for _, p := range plan.Prefixes {
		fmt.Fprintln(w, "-- estimate from", p) //nolint:errcheck // stdout
	}
	fmt.Fprintln(w, plan.SQL) //nolint:errcheck // stdout
}

func emitArchive(
	flags *SearchFlags,
	pattern string,
	params kafka.SearchParams,
	res archive.Result,
	cov *archive.Coverage,
	runErr error,
	deps archiveDeps,
) int {
	format := strings.ToLower(strings.TrimSpace(flags.Format))
	if format == formatSummary {
		sum := searchcli.ArchiveSummary(flags.Topic, pattern, params, flags.CaseSensitive,
			res.Matches, cov, res.Elapsed, runErr)
		enc := json.NewEncoder(deps.stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(sum); err != nil || runErr != nil {
			return 1
		}
		return 0
	}
	if runErr != nil {
		fmt.Fprintln(deps.stderr, "archive search:", runErr) //nolint:errcheck // stderr
		return 1
	}
	out := newSearchOutputTo(format, deps.stdout)
	for _, m := range res.Matches {
		if err := out.emit(m); err != nil {
			fmt.Fprintln(deps.stderr, "writing match:", err) //nolint:errcheck // stderr
			return 1
		}
	}
	if !flags.Quiet {
		for _, l := range cov.Lines(len(res.Matches)) {
			fmt.Fprintln(deps.stderr, l) //nolint:errcheck // stderr
		}
	}
	return 0
}
