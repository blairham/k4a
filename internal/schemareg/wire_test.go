// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"math/big"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
)

func mustRat(t *testing.T, s string) *big.Rat {
	t.Helper()
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		t.Fatalf("bad rat %q", s)
	}
	return r
}

func TestParseHeader(t *testing.T) {
	id, body, ok := ParseHeader([]byte{0, 0, 0, 1, 2, 'x'})
	if !ok || id != 258 || string(body) != "x" {
		t.Errorf("got %d %q %v", id, body, ok)
	}
	for _, b := range [][]byte{{}, {0, 0, 0, 1}, {1, 0, 0, 0, 1}, {0, 0, 0, 0, 0}, {0, 0x80, 0, 0, 0}} {
		if _, _, ok := ParseHeader(b); ok {
			t.Errorf("%x parsed as framed", b)
		}
	}
}

func TestParseMessageIndexes(t *testing.T) {
	for _, want := range [][]int{{0}, {1}, {1, 0}, {3, 2, 7}} {
		var h sr.ConfluentHeader
		b, err := h.AppendEncode(nil, 1, want)
		if err != nil {
			t.Fatal(err)
		}
		got, rest, err := ParseMessageIndexes(append(b[headerSize:], 'm'))
		if err != nil {
			t.Fatalf("%v: %v", want, err)
		}
		if !slices.Equal(got, want) || string(rest) != "m" {
			t.Errorf("%v: got %v rest %q", want, got, rest)
		}
	}
	for _, b := range [][]byte{{}, {0x80}, {0x02}, {0x01}, {0x7f}, {0x02, 0x01}} {
		if _, _, err := ParseMessageIndexes(b); err == nil {
			t.Errorf("%x parsed", b)
		}
	}
}
