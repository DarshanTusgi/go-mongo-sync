package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// DeadCodeAnalyzer analyzes Go code for unused functions and variables
type DeadCodeAnalyzer struct {
	fileSet    *token.FileSet
	packages   map[string]*ast.Package
	functions  map[string]*FunctionInfo
	variables  map[string]*VariableInfo
	references map[string][]string
	exported   map[string]bool
	deadCode   []string
}

type FunctionInfo struct {
	Name     string
	File     string
	Line     int
	Package  string
	Exported bool
	Used     bool
}

type VariableInfo struct {
	Name     string
	File     string
	Line     int
	Package  string
	Exported bool
	Used     bool
}

func NewDeadCodeAnalyzer() *DeadCodeAnalyzer {
	return &DeadCodeAnalyzer{
		fileSet:    token.NewFileSet(),
		packages:   make(map[string]*ast.Package),
		functions:  make(map[string]*FunctionInfo),
		variables:  make(map[string]*VariableInfo),
		references: make(map[string][]string),
		exported:   make(map[string]bool),
		deadCode:   []string{},
	}
}

func (dca *DeadCodeAnalyzer) AnalyzeProject(rootDir string) error {
	fmt.Printf("🔍 DEAD CODE ANALYSIS: Scanning project at %s\n", rootDir)

	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip non-Go files and test files
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		// Skip vendor and .git directories
		if strings.Contains(path, "vendor/") || strings.Contains(path, ".git/") {
			return nil
		}

		return dca.analyzeFile(path)
	})

	if err != nil {
		return err
	}

	dca.findDeadCode()
	dca.printReport()

	return nil
}

func (dca *DeadCodeAnalyzer) analyzeFile(filename string) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	node, err := parser.ParseFile(dca.fileSet, filename, content, parser.ParseComments)
	if err != nil {
		return err
	}

	// Extract package name
	packageName := node.Name.Name

	// Analyze AST
	ast.Inspect(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncDecl:
			dca.analyzeFunction(x, filename, packageName)
		case *ast.GenDecl:
			dca.analyzeGenDecl(x, filename, packageName)
		case *ast.CallExpr:
			dca.analyzeCallExpr(x)
		case *ast.Ident:
			dca.analyzeIdent(x)
		}
		return true
	})

	return nil
}

func (dca *DeadCodeAnalyzer) analyzeFunction(fn *ast.FuncDecl, filename, packageName string) {
	if fn.Name == nil {
		return
	}

	funcName := fn.Name.Name
	exported := ast.IsExported(funcName)

	// Skip main functions and init functions
	if funcName == "main" || funcName == "init" {
		return
	}

	position := dca.fileSet.Position(fn.Pos())

	fullName := fmt.Sprintf("%s.%s", packageName, funcName)
	dca.functions[fullName] = &FunctionInfo{
		Name:     funcName,
		File:     filename,
		Line:     position.Line,
		Package:  packageName,
		Exported: exported,
		Used:     false,
	}

	if exported {
		dca.exported[fullName] = true
	}
}

func (dca *DeadCodeAnalyzer) analyzeGenDecl(gd *ast.GenDecl, filename, packageName string) {
	for _, spec := range gd.Specs {
		switch s := spec.(type) {
		case *ast.ValueSpec:
			for _, name := range s.Names {
				if name.Name == "_" {
					continue
				}

				varName := name.Name
				exported := ast.IsExported(varName)
				position := dca.fileSet.Position(name.Pos())

				fullName := fmt.Sprintf("%s.%s", packageName, varName)
				dca.variables[fullName] = &VariableInfo{
					Name:     varName,
					File:     filename,
					Line:     position.Line,
					Package:  packageName,
					Exported: exported,
					Used:     false,
				}

				if exported {
					dca.exported[fullName] = true
				}
			}
		}
	}
}

func (dca *DeadCodeAnalyzer) analyzeCallExpr(ce *ast.CallExpr) {
	if ident, ok := ce.Fun.(*ast.Ident); ok {
		// Mark function as used
		for fullName, fn := range dca.functions {
			if strings.HasSuffix(fullName, "."+ident.Name) {
				fn.Used = true
			}
		}
	}
}

func (dca *DeadCodeAnalyzer) analyzeIdent(ident *ast.Ident) {
	// Mark variables as used
	for fullName, variable := range dca.variables {
		if strings.HasSuffix(fullName, "."+ident.Name) {
			variable.Used = true
		}
	}
}

func (dca *DeadCodeAnalyzer) findDeadCode() {
	fmt.Printf("🧹 DEAD CODE DETECTION: Analyzing %d functions and %d variables\n",
		len(dca.functions), len(dca.variables))

	// Find unused functions
	for fullName, fn := range dca.functions {
		if !fn.Used && !fn.Exported {
			dca.deadCode = append(dca.deadCode, fmt.Sprintf("UNUSED FUNCTION: %s in %s:%d",
				fullName, fn.File, fn.Line))
		}
	}

	// Find unused variables
	for fullName, variable := range dca.variables {
		if !variable.Used && !variable.Exported {
			dca.deadCode = append(dca.deadCode, fmt.Sprintf("UNUSED VARIABLE: %s in %s:%d",
				fullName, variable.File, variable.Line))
		}
	}
}

func (dca *DeadCodeAnalyzer) printReport() {
	fmt.Printf("\n📊 DEAD CODE ANALYSIS REPORT\n")
	fmt.Printf("================================\n")
	fmt.Printf("Total Functions Analyzed: %d\n", len(dca.functions))
	fmt.Printf("Total Variables Analyzed: %d\n", len(dca.variables))
	fmt.Printf("Dead Code Items Found: %d\n", len(dca.deadCode))

	if len(dca.deadCode) == 0 {
		fmt.Printf("✅ NO DEAD CODE FOUND - Project is clean!\n")
		return
	}

	fmt.Printf("\n🔴 DEAD CODE IDENTIFIED:\n")
	for _, item := range dca.deadCode {
		fmt.Printf("  - %s\n", item)
	}

	fmt.Printf("\n💡 RECOMMENDATIONS:\n")
	fmt.Printf("  1. Review and remove unused functions and variables\n")
	fmt.Printf("  2. Consider if any functions should be exported\n")
	fmt.Printf("  3. Check for any missing test coverage\n")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run dead_code_analyzer.go <project_root>")
		os.Exit(1)
	}

	projectRoot := os.Args[1]
	analyzer := NewDeadCodeAnalyzer()

	if err := analyzer.AnalyzeProject(projectRoot); err != nil {
		log.Fatalf("Analysis failed: %v", err)
	}
}
