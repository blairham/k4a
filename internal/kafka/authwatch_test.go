// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	refusalTopic = "orders"
	refusalUser  = "k4a"
	refusalPass  = "current"
	// refusalBackstop turns a consumer that never gives up into a failure
	// rather than a hung test. It is not a wait: a passing consumer returns
	// as soon as the broker has refused it authFailureLimit times.
	refusalBackstop = 30 * time.Second
)

// newRefusalFixture returns a client for a SASL cluster that answers a wrong
// PLAIN password the way MSK answers a rejected IAM signature, with
// SASL_AUTHENTICATION_FAILED and "Access denied"; kfake alone drops the
// connection, which says nothing about credentials. The client's admin
// connection authenticates with the right password, and the consumers it
// builds use consumerPass, as when credentials expire mid-run.
func newRefusalFixture(t *testing.T, consumerPass string) *Client {
	t.Helper()
	fake, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(blockingPartitions, refusalTopic),
		kfake.EnableSASL(),
		kfake.Superuser("PLAIN", refusalUser, refusalPass),
	)
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	fake.ControlKey(int16(kmsg.SASLAuthenticate), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		fake.KeepControl()
		req, ok := kreq.(*kmsg.SASLAuthenticateRequest)
		if !ok {
			return nil, nil, false //nolint:nilnil // kfake reads (nil, nil, false) as "not handled here"
		}
		if parts := bytes.SplitN(req.SASLAuthBytes, []byte{0}, 3); len(parts) == 3 && string(parts[2]) == refusalPass {
			return nil, nil, false //nolint:nilnil // kfake reads (nil, nil, false) as "let kfake answer"
		}
		resp := kmsg.NewPtrSASLAuthenticateResponse()
		resp.Version = req.Version
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		msg := "Access denied"
		resp.ErrorMessage = &msg
		return resp, nil, true
	})

	auth := AuthConfig{Method: AuthPlain, NoTLS: true, Username: refusalUser, Password: refusalPass}
	client, err := NewClient(auth, strings.Join(fake.ListenAddrs(), ","))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	t.Cleanup(client.Close)
	if _, err := client.admin.ListTopics(t.Context(), refusalTopic); err != nil {
		t.Fatalf("admin client did not authenticate: %v", err)
	}
	client.authCfg.Password = consumerPass
	return client
}

func discardMessage(ConsumedMessage) {}

// drainConsume reads a Consume-style stream to its end and returns the error
// it reported, if any.
func drainConsume(ch <-chan ConsumedMessage, errCh <-chan error, _ <-chan struct{}) error {
	for range ch {
	}
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// Every long-running poll loop must end with the broker's refusal once the
// credentials stop being accepted, rather than retry the handshake for as
// long as it runs (#153).
func TestConsumersStopOnRefusedCredentials(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		run  func(context.Context, *Client) error
		name string
	}{
		{name: "ConsumeBlocking", run: func(ctx context.Context, c *Client) error {
			return c.ConsumeBlocking(ctx, refusalTopic, "", AllPartitions, discardMessage)
		}},
		{name: "ConsumeBlocking one partition", run: func(ctx context.Context, c *Client) error {
			return c.ConsumeBlocking(ctx, refusalTopic, "", 0, discardMessage)
		}},
		{name: "ConsumeBlocking group", run: func(ctx context.Context, c *Client) error {
			return c.ConsumeBlocking(ctx, refusalTopic, "k4a", AllPartitions, discardMessage)
		}},
		{name: "ConsumeBlockingFrom", run: func(ctx context.Context, c *Client) error {
			return c.ConsumeBlockingFrom(ctx, refusalTopic, AllPartitions, Seek{}, discardMessage)
		}},
		{name: "ConsumeRange", run: func(ctx context.Context, c *Client) error {
			return c.ConsumeRange(ctx, refusalTopic, map[int32]int64{0: 0}, map[int32]int64{0: 1}, discardMessage)
		}},
		{name: "Consume", run: func(ctx context.Context, c *Client) error {
			return drainConsume(c.Consume(ctx, refusalTopic))
		}},
		{name: "ConsumeFrom", run: func(ctx context.Context, c *Client) error {
			return drainConsume(c.ConsumeFrom(ctx, refusalTopic, map[int32]int64{0: 0}))
		}},
		{name: "KeyedLog.Tail", run: func(ctx context.Context, c *Client) error {
			l, err := c.OpenKeyedLog(ctx, "follow-set")
			if err != nil {
				return fmt.Errorf("opening keyed log: %w", err)
			}
			defer l.Close()
			return l.Tail(ctx, map[int32]int64{0: 0}, func(string, []byte, int32, int64) {})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newRefusalFixture(t, "expired")
			ctx, cancel := context.WithTimeout(t.Context(), refusalBackstop)
			defer cancel()

			err := tc.run(ctx, client)
			if !errors.Is(err, kerr.SaslAuthenticationFailed) || !IsAuthError(err) {
				t.Fatalf("returned %v; want the broker's refusal (nil: still retrying when the backstop ended it)", err)
			}
		})
	}
}

// The refusal above comes from the password, not from the fixture: the same
// cluster serves a consumer whose credentials it accepts.
func TestConsumeReadsWithAcceptedCredentials(t *testing.T) {
	t.Parallel()
	client := newRefusalFixture(t, refusalPass)

	opts, err := NewClientOpts(client.authCfg, client.brokers)
	if err != nil {
		t.Fatal(err)
	}
	prod, err := kgo.NewClient(append(opts, kgo.DefaultProduceTopic(refusalTopic))...)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(t.Context(), refusalBackstop)
	defer cancel()
	if perr := prod.ProduceSync(ctx, &kgo.Record{Value: []byte("hello")}).FirstErr(); perr != nil {
		t.Fatalf("produce: %v", perr)
	}

	var got string
	err = client.ConsumeBlocking(ctx, refusalTopic, "", AllPartitions, func(m ConsumedMessage) {
		got = m.Value
		cancel()
	})
	if err != nil || got != "hello" {
		t.Fatalf("ConsumeBlocking = %v, read %q; want nil and %q", err, got, "hello")
	}
}

func TestAuthWatchCountsConsecutiveRefusals(t *testing.T) {
	t.Parallel()

	refused := fmt.Errorf("%w: Access denied", kerr.SaslAuthenticationFailed)
	ctx, cancel := context.WithCancelCause(t.Context())
	w := &authWatch{cancel: cancel}
	connect := func(err error) { w.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, err) }

	for range authFailureLimit - 1 {
		connect(refused)
	}
	connect(nil)
	for range authFailureLimit - 1 {
		connect(refused)
	}
	connect(io.EOF)
	if ctx.Err() != nil {
		t.Fatalf("gave up with %v; an authenticated connection resets the count and a dropped one adds nothing",
			context.Cause(ctx))
	}

	connect(refused)
	if err := authFailure(ctx); !errors.Is(err, kerr.SaslAuthenticationFailed) {
		t.Fatalf("authFailure = %v after %d refusals in a row; want the refusal", err, authFailureLimit)
	}
}

func TestAuthFailureIgnoresCallerCancel(t *testing.T) {
	t.Parallel()

	parent, cancel := context.WithCancel(t.Context())
	cl, watched, err := newWatchedClient(parent, kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	cancel()
	<-watched.Done()
	if err := authFailure(watched); err != nil {
		t.Fatalf("authFailure = %v for a context the caller canceled; want nil", err)
	}
}
