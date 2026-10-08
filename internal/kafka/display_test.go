// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import "testing"

func TestBytesToDisplay(t *testing.T) {
	cases := []struct {
		name string
		want string
		in   []byte
	}{
		{name: "empty", want: "", in: nil},
		{name: "ascii", want: "hello", in: []byte("hello")},
		{name: "utf8 multibyte", want: "héllo", in: []byte("héllo")},
		{name: "json", want: `{"a":1}`, in: []byte(`{"a":1}`)},
		{name: "newline kept", want: "a\nb", in: []byte("a\nb")},
		{name: "tab kept", want: "a\tb", in: []byte("a\tb")},
		{name: "invalid utf8", want: "hex:fffefd", in: []byte{0xff, 0xfe, 0xfd}},
		{name: "embedded nul", want: "hex:00016162", in: []byte{0x00, 0x01, 'a', 'b'}},
		{name: "del byte", want: "hex:617f62", in: []byte{'a', 0x7f, 'b'}},
		{name: "control byte", want: "hex:610162", in: []byte{'a', 0x01, 'b'}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := bytesToDisplay(tc.in)
			if got != tc.want {
				t.Fatalf("bytesToDisplay(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
