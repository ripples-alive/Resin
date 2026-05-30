package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestStartBackgroundServicesDoesNotForceRefreshSubscriptions(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "app_runtime.go", nil, 0)
	if err != nil {
		t.Fatalf("ParseFile: %v", err)
	}

	var fn *ast.FuncDecl
	for _, decl := range file.Decls {
		candidate, ok := decl.(*ast.FuncDecl)
		if ok && candidate.Name.Name == "startBackgroundServices" {
			fn = candidate
			break
		}
	}
	if fn == nil {
		t.Fatal("startBackgroundServices not found")
	}

	ast.Inspect(fn.Body, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "ForceRefreshAll", "ForceRefreshAllAsync":
			t.Fatalf("startBackgroundServices must not call %s; startup refresh is controlled by stored subscription timestamps", sel.Sel.Name)
		}
		return true
	})
}
