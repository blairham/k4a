// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// Every function that builds a Kafka client must defer its Close: eight
// headless commands once exited with their connections still open. The check
// is structural because the commands cannot run without a broker.
func TestBuildClientIsAlwaysClosed(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var checked int
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			for _, v := range builtClients(fn.Body) {
				checked++
				if !defersClose(fn.Body, v) {
					t.Errorf("%s: %s builds a client into %q but never defers %s.Close()",
						fset.Position(fn.Pos()), fn.Name.Name, v, v)
				}
			}
		}
	}
	// A glob that matched nothing would pass vacuously.
	if checked < 10 {
		t.Fatalf("checked only %d BuildClient call sites; the scan is not seeing the commands", checked)
	}
}

// builtClients returns the variables assigned from a .BuildClient() call.
func builtClients(body *ast.BlockStmt) []string {
	var out []string
	ast.Inspect(body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 || len(as.Lhs) == 0 {
			return true
		}
		call, ok := as.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "BuildClient" {
			return true
		}
		if id, ok := as.Lhs[0].(*ast.Ident); ok {
			out = append(out, id.Name)
		}
		return true
	})
	return out
}

// defersClose reports whether body contains `defer v.Close()`.
func defersClose(body *ast.BlockStmt, v string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		d, ok := n.(*ast.DeferStmt)
		if !ok {
			return true
		}
		sel, ok := d.Call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Close" {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == v {
			found = true
		}
		return true
	})
	return found
}
