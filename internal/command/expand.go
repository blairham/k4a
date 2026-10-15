// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"os"
	"reflect"

	"github.com/blairham/k4a/internal/config"
)

// expandEnvFlags resolves environment variable references in every string
// field of a flags struct, including embedded structs, by the same rule as a
// context: a value that is exactly $VAR or ${VAR} is replaced, anything else
// is literal, so a `$` inside a password, regex or message survives.
// --brokers alone also expands embedded references ($HOST:9092), matching the
// context's brokers field; a host list never holds a literal `$`.
func expandEnvFlags(flags any) {
	expandEnvFields(reflect.ValueOf(flags).Elem())
}

func expandEnvFields(v reflect.Value) {
	for i := range v.NumField() {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.String:
			if !f.CanSet() {
				continue
			}
			if v.Type().Field(i).Tag.Get("long") == "brokers" {
				f.SetString(os.ExpandEnv(f.String()))
			} else {
				f.SetString(config.ExpandRef(f.String()))
			}
		case reflect.Struct:
			expandEnvFields(f)
		}
	}
}
