// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"strings"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/kafka"
)

func TestParseSeek(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	for _, in := range []string{"", "live", " LIVE "} {
		if s, err := ParseSeek(in, now); err != nil || s != nil {
			t.Errorf("ParseSeek(%q) = %+v, %v; want the live tail", in, s, err)
		}
	}
	if s, err := ParseSeek("b", now); err != nil || s == nil || s.Mode != kafka.SeekStart {
		t.Errorf("b = %+v, %v", s, err)
	}
	if s, err := ParseSeek("1200", now); err != nil || s.Mode != kafka.SeekOffset || s.Offset != 1200 {
		t.Errorf("1200 = %+v, %v", s, err)
	}
	if s, err := ParseSeek("0", now); err != nil || s.Mode != kafka.SeekOffset || s.Offset != 0 {
		t.Errorf("0 = %+v, %v; an offset, not the live tail", s, err)
	}
	if s, err := ParseSeek("2h", now); err != nil || s.Mode != kafka.SeekTime || !s.Time.Equal(now.Add(-2*time.Hour)) {
		t.Errorf("2h = %+v, %v", s, err)
	}
	if s, err := ParseSeek("2026-10-15T08:00:00Z", now); err != nil || s.Mode != kafka.SeekTime || s.Time.Hour() != 8 {
		t.Errorf("RFC3339 = %+v, %v", s, err)
	}
	for _, bad := range []string{"-5", "yesterday", "1x"} {
		if _, err := ParseSeek(bad, now); err == nil {
			t.Errorf("ParseSeek(%q) accepted", bad)
		}
	}
}

func consumed(offset int64) kafka.ConsumedMessage {
	return kafka.ConsumedMessage{Topic: testTopicTail, Offset: offset, Value: "v"}
}

// A record still in flight from the consumer a seek replaced must not land
// in the seeked buffer.
func TestMessagesSeekDropsStaleRecords(t *testing.T) {
	t.Parallel()
	v := NewMessagesView(nil, testTopicTail)
	v.gen = 1
	v.seek = &kafka.Seek{Mode: kafka.SeekOffset, Offset: 5}

	v.Update(genMessageMsg{msg: consumed(99), gen: 0}) //nolint:errcheck
	if len(v.messages) != 0 {
		t.Fatalf("a stale-generation record was kept: %v", v.messages)
	}
	v.Update(genMessageMsg{msg: consumed(5), gen: 1}) //nolint:errcheck
	if len(v.messages) != 1 || v.messages[0].Offset != 5 {
		t.Errorf("current-generation record = %v", v.messages)
	}
}

// After a seek the buffer keeps what follows the seek point; the live tail
// keeps the newest.
func TestMessagesSeekKeepsFirstRecords(t *testing.T) {
	t.Parallel()
	// One Update drains the rest from the channel, as a burst would.
	fill := func(v *MessagesView) {
		ch := make(chan kafka.ConsumedMessage, maxMessages+5)
		for i := 1; i < maxMessages+5; i++ {
			ch <- consumed(int64(i))
		}
		close(ch)
		v.msgCh = ch
		v.Update(MessageMsg(consumed(0))) //nolint:errcheck
	}

	seeked := NewMessagesView(nil, testTopicTail)
	seeked.seek = &kafka.Seek{Mode: kafka.SeekStart}
	fill(seeked)
	if n := len(seeked.messages); n != maxMessages || seeked.messages[0].Offset != 0 || !seeked.capped {
		t.Errorf("seeked: %d records from offset %d, capped=%t; want the first %d", n,
			seeked.messages[0].Offset, seeked.capped, maxMessages)
	}
	seeked.Resize(120, 20)
	if out := seeked.View(); !strings.Contains(out, "from the beginning") || !strings.Contains(out, "stopped") {
		t.Errorf("seeked view does not say where it started and that it stopped:\n%s", out)
	}

	live := NewMessagesView(nil, testTopicTail)
	fill(live)
	if n := len(live.messages); n != maxMessages || live.messages[0].Offset != 5 || live.capped {
		t.Errorf("live: %d records from offset %d, capped=%t; want the newest %d", n,
			live.messages[0].Offset, live.capped, maxMessages)
	}
}

func TestMessagesSeekKey(t *testing.T) {
	t.Parallel()
	v := NewMessagesView(nil, testTopicTail)
	if action, param := v.HandleKey(KeySeek); action != ActionSeek || param != testTopicTail {
		t.Errorf("HandleKey(t) = %q, %q", action, param)
	}
}
