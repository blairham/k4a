// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/state"
	"github.com/blairham/k4a/internal/version"
)

type stateExportFlags struct {
	Output         string `short:"o" long:"output"          description:"Archive to write (default k4a-state-<time>.tar.gz here)"`
	NoConfig       bool   `          long:"no-config"       description:"Leave out config.yaml (contexts and UI settings)"`
	NoSession      bool   `          long:"no-session"      description:"Leave out session state (state.json)"`
	NoCache        bool   `          long:"no-cache"        description:"Leave out the search-result cache"`
	Index          bool   `          long:"index"           description:"Include local topic indexes (can be large)"`
	IncludeSecrets bool   `          long:"include-secrets" description:"Keep inline passwords and tokens in config.yaml"`
	Yes            bool   `short:"y" long:"yes"             description:"Replace an existing output file without asking"`
}

type stateImportFlags struct {
	Yes bool `short:"y" long:"yes" description:"Replace existing state without asking"`
}

// secretsWarning is printed whenever an archive holds inline credentials,
// on export and again on import, so neither end can miss it.
const secretsWarning = `WARNING: this archive contains inline secrets in cleartext (%s).
WARNING: anyone who can read it can use them. Keep it 0600, move it over a
WARNING: trusted channel, and delete it once imported.
`

// runStateExport writes the selected state parts to a tar.gz archive.
func (c *StateCommand) runStateExport(root *state.Root, args []string) error {
	var flags stateExportFlags
	parser := goflags.NewParser(&flags, goflags.HelpFlag|goflags.PassDoubleDash)
	parser.Usage = "export [options]"
	rest, err := parser.ParseArgs(args)
	if err != nil {
		return err
	}
	if len(rest) > 0 {
		return fmt.Errorf("unexpected argument %q", rest[0])
	}
	out, errOut := orStdout(c.Out), orStderr(c.Err)

	dest := flags.Output
	if dest == "" {
		dest = "k4a-state-" + time.Now().Format("20060102-150405") + ".tar.gz"
	}
	if insideDir(root.Path(), dest) {
		return fmt.Errorf("%s is inside the state directory %s; export somewhere else", dest, root.Path())
	}
	if _, serr := os.Lstat(dest); serr == nil && !flags.Yes {
		if aerr := c.Prompt.ask(dest + " exists. Replace it?"); aerr != nil {
			return fmt.Errorf("export not done: %w", aerr)
		}
	}

	opts := state.ExportOptions{
		Session:  !flags.NoSession,
		Cache:    !flags.NoCache,
		Index:    flags.Index,
		Manifest: state.Manifest{K4AVersion: version.String()},
	}
	if !flags.NoConfig {
		if cerr := exportConfig(root, &opts, flags.IncludeSecrets, errOut); cerr != nil {
			return cerr
		}
	}

	res, err := writeArchive(root, dest, opts)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Exported %s (%d files, %s) to %s.\n", //nolint:errcheck // stdout
		strings.Join(res.Manifest.Parts, ", "), res.Files, formatBytes(res.Bytes), dest)
	for _, t := range res.Locked {
		fmt.Fprintf(errOut, "Skipped index %s: another k4a process is writing it.\n", t) //nolint:errcheck // stderr
	}
	return nil
}

// exportConfig reads config.yaml into opts, stripping inline secrets unless
// includeSecrets. A $VAR reference is never a secret and always survives.
func exportConfig(root *state.Root, opts *state.ExportOptions, includeSecrets bool, errOut io.Writer) error {
	raw, err := os.ReadFile(filepath.Join(root.Path(), "config.yaml"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading config: %w", err)
	}
	stripped, names, err := config.StripInlineSecrets(raw)
	if err != nil {
		return err
	}
	if includeSecrets {
		// Verbatim, comments included, so it is marked as holding secrets
		// even when no field does: a comment can hold one too.
		opts.Config = raw
		opts.Manifest.SecretsIncluded = true
		what := "config.yaml verbatim"
		if len(names) > 0 {
			what = strings.Join(names, ", ")
		}
		fmt.Fprintf(errOut, secretsWarning, what) //nolint:errcheck // stderr
		return nil
	}
	opts.Config = stripped
	opts.Manifest.StrippedSecrets = names
	if len(names) > 0 {
		fmt.Fprintf(errOut, //nolint:errcheck // stderr
			"Left out inline secrets (%s); set them again after import, or write them as $VAR references.\n",
			strings.Join(names, ", "))
	}
	return nil
}

// writeArchive writes the archive to a 0600 temp file beside dest and
// renames it into place, so a failed export never leaves a truncated
// archive under the name the user asked for.
func writeArchive(root *state.Root, dest string, opts state.ExportOptions) (*state.ExportResult, error) {
	tmp, err := os.CreateTemp(filepath.Dir(dest), "."+filepath.Base(dest)+".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", dest, err)
	}
	defer os.Remove(tmp.Name()) //nolint:errcheck // gone after a successful Rename
	if cerr := tmp.Chmod(0o600); cerr != nil {
		tmp.Close() //nolint:errcheck // already failing
		return nil, cerr
	}
	res, err := state.Export(root, tmp, opts)
	if err != nil {
		tmp.Close() //nolint:errcheck // already failing
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp.Name(), dest); err != nil {
		return nil, fmt.Errorf("writing %s: %w", dest, err)
	}
	return res, nil
}

// runStateImport validates an archive, confirms any replacement, and
// writes it into the state root.
func (c *StateCommand) runStateImport(root *state.Root, args []string) error {
	var flags stateImportFlags
	parser := goflags.NewParser(&flags, goflags.HelpFlag|goflags.PassDoubleDash)
	parser.Usage = "import [options] ARCHIVE"
	rest, err := parser.ParseArgs(args)
	if err != nil {
		return err
	}
	if len(rest) != 1 {
		return errors.New("usage: k4a state import [--yes] ARCHIVE")
	}
	if werr := checkRootWritable(root, "state import"); werr != nil {
		return werr
	}
	out, errOut := orStdout(c.Out), orStderr(c.Err)

	plan, err := state.InspectArchive(rest[0])
	if err != nil {
		return err
	}
	if plan.Manifest.SecretsIncluded {
		fmt.Fprintf(errOut, secretsWarning, "config.yaml") //nolint:errcheck // stderr
	}
	if conflicts := plan.Conflicts(root); len(conflicts) > 0 && !flags.Yes {
		fmt.Fprintf(out, "Importing would replace:\n  %s\n", strings.Join(conflicts, "\n  ")) //nolint:errcheck // stdout
		if aerr := c.Prompt.ask(fmt.Sprintf("Replace %d item(s) in %s?", len(conflicts), root.Path())); aerr != nil {
			return fmt.Errorf("import not done: %w", aerr)
		}
	}
	res, err := plan.Apply(root)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Imported %s (%d files, %s) into %s.\n", //nolint:errcheck // stdout
		strings.Join(plan.Manifest.Parts, ", "), res.Files, formatBytes(res.Bytes), root.Path())
	if len(plan.Manifest.StrippedSecrets) > 0 {
		fmt.Fprintf(out, "Inline secrets were left out at export: %s.\n", //nolint:errcheck // stdout
			strings.Join(plan.Manifest.StrippedSecrets, ", "))
	}
	return nil
}

// insideDir reports whether p is dir or below it.
func insideDir(dir, p string) bool {
	d, err := filepath.Abs(dir)
	if err != nil {
		return false
	}
	a, err := filepath.Abs(p)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(d, a)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// orStderr is w, or stderr when a command was built without one.
func orStderr(w io.Writer) io.Writer {
	if w == nil {
		return os.Stderr
	}
	return w
}
