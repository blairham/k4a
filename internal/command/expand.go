// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"os"
	"reflect"
)

// expandEnvFlags expands environment variable references (e.g. $VAR or ${VAR})
// in all string fields of a struct, including embedded structs.
func expandEnvFlags(flags any) {
	expandEnvFields(reflect.ValueOf(flags).Elem())
}

func expandEnvFields(v reflect.Value) {
	for i := range v.NumField() {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.String:
			if f.CanSet() {
				f.SetString(os.ExpandEnv(f.String()))
			}
		case reflect.Struct:
			expandEnvFields(f)
		}
	}
}
