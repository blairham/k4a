// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package command implements the CLI commands for k4a.
package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui"
	"github.com/blairham/k4a/internal/upgrade"
)

// UIFlags defines the flags for the main TUI command.
type UIFlags struct {
	// Config-based connection.
	Context    string `short:"c" long:"context" description:"Named context from ~/.k4a/config.yaml"`
	ConfigFile string `          long:"config"  description:"Config file path"                      default:""`

	// Direct connection (overrides config).
	Brokers string `short:"b" long:"brokers" env:"KAFKA_BROKERS" description:"Bootstrap servers (comma-separated)"`
	Auth    string `short:"a" long:"auth"    env:"KAFKA_AUTH"    description:"Auth method: plaintext, tls, scram, mtls, iam" default:""`

	// SASL/SCRAM
	Username string `long:"username" env:"KAFKA_USERNAME" description:"SASL/SCRAM username"`
	Password string `long:"password" env:"KAFKA_PASSWORD" description:"SASL/SCRAM password"` // pragma: allowlist secret

	// mTLS
	CertFile string `long:"cert" env:"KAFKA_TLS_CERT" description:"Client certificate PEM file (mTLS)"`
	KeyFile  string `long:"key"  env:"KAFKA_TLS_KEY"  description:"Client private key PEM file (mTLS)"`
	CAFile   string `long:"ca"   env:"KAFKA_TLS_CA"   description:"CA certificate PEM file"`

	// IAM
	Region  string `short:"r" long:"region"  env:"AWS_REGION"  default:"us-east-1" description:"AWS region (IAM auth)"`
	Profile string `short:"p" long:"profile" env:"AWS_PROFILE"                     description:"AWS profile (IAM auth)"`

	Insecure bool `short:"k" long:"insecure" description:"Skip TLS certificate verification"`
	Logoless bool `          long:"logoless" description:"Hide the ASCII logo from the header"`
	Readonly bool `          long:"readonly" description:"Disable all mutating operations (read-only mode)"`
}

// UICommand implements the "ui" (default) CLI command.
type UICommand struct {
	Version string
}

// Help returns the help text.
func (c *UICommand) Help() string {
	return `Usage: k4a [options]

  Interactive TUI for exploring Kafka clusters — like k9s, but for Kafka.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
  -c, --context=NAME      Named context from config file
      --config=FILE       Config file path (default: ~/.k4a/config.yaml)

Direct Connection:
  -b, --brokers=BROKERS   Bootstrap servers, comma-separated
  -a, --auth=METHOD       Auth method: plaintext, tls, scram, mtls, iam
      --username=USER     SASL/SCRAM username
      --password=PASS     SASL/SCRAM password` + // pragma: allowlist secret
		`
      --cert=FILE         Client certificate PEM (mTLS)
      --key=FILE          Client private key PEM (mTLS)
      --ca=FILE           CA certificate PEM
  -r, --region=REGION     AWS region for IAM auth (default: us-east-1)
  -p, --profile=PROFILE   AWS profile for IAM auth
  -k, --insecure          Skip TLS certificate verification
      --logoless          Hide the ASCII logo from the header
      --readonly          Disable all mutating operations (read-only mode)

Navigation:
  1/2/3          Switch tabs (Topics, Groups, Cluster)
  j/k            Move up/down
  Enter          Messages (from topic view) / drill into detail
  o              Topic overview
  Esc            Go back
  /              Filter
  :              Command mode
  r              Refresh
  :q             Quit

Examples:
  # Use current-context from config
  k4a

  # Use a specific context
  k4a --context staging

  # Direct connection (no config)
  k4a -b broker:9098 --auth iam`
}

// Synopsis returns the one-line description.
func (c *UICommand) Synopsis() string {
	return "Interactive Kafka TUI"
}

// Run executes the TUI.
func (c *UICommand) Run(args []string) int {
	var flags UIFlags
	parser := goflags.NewParser(&flags, goflags.Default)
	if _, err := parser.ParseArgs(args); err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid flags", "err", err)
		return 1
	}

	expandEnvFlags(&flags)

	brokers, authCfg, ctxName, err := resolveConnection(flags)

	// No context chosen yet, but the config has some. Don't die: the TUI
	// already opens on the context picker by default, so start it with no
	// client and let the user pick one. Refusing to launch here made the
	// one screen that fixes this state unreachable from the state it fixes.
	pickContext := errors.Is(err, errPickContext)
	if err != nil && !pickContext {
		log.Error("connection failed", "err", err)
		return 1
	}

	var client *kafka.Client
	var connInfo tui.ConnInfo
	if !pickContext {
		client, err = kafka.NewClient(authCfg, brokers)
		if err != nil {
			log.Error("client setup failed", "err", err)
			return 1
		}
		connInfo = tui.ConnInfo{
			Context: ctxName,
			Auth:    string(authCfg.Method),
			Brokers: brokers,
			Region:  authCfg.Region,
		}
	}

	// Load config for in-TUI context switching.
	cfgPath := flags.ConfigFile
	if cfgPath == "" {
		cfgPath = config.DefaultConfigPath()
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		cfg = &config.Config{}
	}
	if flags.Logoless {
		cfg.UI.Logoless = true
	}
	if flags.Readonly {
		cfg.UI.Readonly = true
	}
	app := tui.NewApp(client, connInfo, cfg, cfgPath, c.Version)
	if pickContext {
		app.SetAwaitingContext()
	} else if flags.Context != "" || flags.Brokers != "" {
		app.SetSkipContextView()
	}

	// Set terminal + iTerm2 tab background to match k9s dark theme.
	setTerminalBg()
	defer resetTerminalBg()

	p := tea.NewProgram(app)
	model, err := p.Run()
	if err != nil {
		log.Error("TUI exited with error", "err", err)
		return 1
	}

	if appModel, ok := model.(*tui.App); ok && appModel.UpgradeRequested() {
		resetTerminalBg()
		return c.upgradeAndRestart(appModel.UpgradeContext())
	}

	return 0
}

func (c *UICommand) upgradeAndRestart(ctxName string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if _, err := upgrade.Run(ctx, c.Version, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	exe, err := os.Executable()
	if err != nil {
		fmt.Println("Upgraded successfully. Please restart k4a manually.")
		return 0
	}

	args := []string{"k4a"}
	if ctxName != "" && ctxName != "direct" {
		args = append(args, "--context", ctxName)
	}

	fmt.Println("Restarting...")
	if err := reexec(exe, args, os.Environ()); err != nil {
		fmt.Printf("Upgraded successfully but restart failed: %v\nPlease restart k4a manually.\n", err)
	}
	return 0
}

func resolveConnection(flags UIFlags) (string, kafka.AuthConfig, string, error) {
	// If --brokers is provided directly, use flags.
	if flags.Brokers != "" {
		auth := flags.Auth
		if auth == "" {
			auth = "scram"
		}
		return flags.Brokers, kafka.AuthConfig{
			Method:   kafka.AuthMethod(auth),
			Username: flags.Username,
			Password: flags.Password,
			CertFile: flags.CertFile,
			KeyFile:  flags.KeyFile,
			CAFile:   flags.CAFile,
			Region:   flags.Region,
			Profile:  flags.Profile,
			Insecure: flags.Insecure,
		}, "direct", nil
	}

	// Load config file.
	cfgPath := flags.ConfigFile
	if cfgPath == "" {
		cfgPath = config.DefaultConfigPath()
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return "", kafka.AuthConfig{}, "", err
	}

	ctxName := flags.Context
	if ctxName == "" {
		ctxName = cfg.CurrentContext
	}

	ctx, err := cfg.Resolve(flags.Context)
	if err != nil {
		// Nothing selected, but the file does hold contexts: the TUI can
		// recover on its own by opening the picker, so pass the sentinel
		// up bare and let the caller decide. Wrapping it in advice here
		// would just be advice the user never needed.
		if errors.Is(err, config.ErrNoCurrentContext) && len(cfg.Contexts) > 0 {
			return "", kafka.AuthConfig{}, "", errPickContext
		}
		return "", kafka.AuthConfig{}, "", connectionHelp(err, cfg, cfgPath)
	}

	brokers, err := ctx.ResolveBrokers()
	if err != nil {
		return "", kafka.AuthConfig{}, "", err
	}

	authCfg := ctx.ToAuthConfig()

	// Allow flags to override config values.
	if flags.Auth != "" {
		authCfg.Method = kafka.AuthMethod(flags.Auth)
	}
	if flags.Profile != "" {
		authCfg.Profile = flags.Profile
	}
	if flags.Region != "" && flags.Region != "us-east-1" {
		authCfg.Region = flags.Region
	}

	return brokers, authCfg, ctxName, nil
}

// errPickContext says "the config is fine, the user just hasn't chosen yet" —
// the one resolve failure the TUI recovers from by opening its picker. It is a
// signal, never printed. A distinct value rather than config.ErrNoCurrentContext
// because an EMPTY config raises that same sentinel, and there the honest answer
// really is "create a config" — matching on the shared one sent that case to an
// empty picker.
var errPickContext = errors.New("no context selected")

// connectionHelp turns a resolve failure into a message that names the actual
// remedy. The old text always ended "or create a config at <path>", which is
// wrong exactly when it is most confusing: the config already exists and is
// full of working contexts, and the user needs to pick one, not write a file.
func connectionHelp(err error, cfg *config.Config, cfgPath string) error {
	if len(cfg.Contexts) == 0 {
		return fmt.Errorf("%w\n\nUse --brokers to connect directly, or create a config at %s", err, cfgPath)
	}
	return fmt.Errorf(
		"%w\n\nPick one with `k4a use-context <name>`, or pass --context.\nConfig: %s",
		err, cfgPath,
	)
}
